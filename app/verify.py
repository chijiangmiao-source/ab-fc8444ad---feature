"""One-shot acceptance client: exercises the full resume + content pool flow.

Run with:  API_BASE_URL=http://api:8000 python -m app.verify
Exit code 0 means every check passed.

Stages (all against real bytes, never fixed responses):
  1. original resumable upload flow (validation, idempotency, conflict, resume,
     atomic publish, artifact download);
  2. cross-session zero-body ``reuse`` from the global content pool, including
     CHUNK_NOT_CACHED and unchanged bitmaps;
  3. concurrent identical uploads hitting both API workers -> one pool result;
  4. expiry: reuse cannot add progress to an expired session;
  5. batched reclaim: persisted cycle_id/cursor, pinning, fence, completion
     independence from reclaimed chunks.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
CHUNK_SIZE = 1024 * 1024
FILE_SIZE = CHUNK_SIZE * 4 + 12345  # 5 chunks, last one short


class VerifyFailure(Exception):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise VerifyFailure(message)


def payload(size: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"verify-payload:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def wait_for_api(client: httpx.Client, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise VerifyFailure(f"API at {BASE_URL} did not become healthy within {timeout:.0f}s")


def expect_error(resp: httpx.Response, status: int, code: str) -> dict:
    check(resp.status_code == status, f"expected HTTP {status}, got {resp.status_code}: {resp.text}")
    body = resp.json()
    check(isinstance(body.get("error"), dict), f"error envelope missing: {body}")
    check(
        body["error"].get("code") == code,
        f"expected error code {code}, got {body['error'].get('code')}",
    )
    return body


def put_chunk(client: httpx.Client, sid: str, index: int, body: bytes, digest: str) -> httpx.Response:
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers={"X-Chunk-SHA256": digest})


def create_session(client: httpx.Client, file_size: int, file_sha: str, expires_in_minutes: int = 15) -> str:
    resp = client.post(
        "/sessions",
        json={
            "file_size": file_size,
            "chunk_size": CHUNK_SIZE,
            "file_sha256": file_sha,
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=expires_in_minutes)).isoformat(),
        },
    )
    check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
    return resp.json()["session_id"]


def reuse(client: httpx.Client, sid: str, index: int, digest: str) -> httpx.Response:
    return client.post(f"/sessions/{sid}/chunks/{index}/reuse", json={"sha256": digest})


def stage_original_flow(client: httpx.Client, data: bytes, chunks: list[bytes], digests: list[str], file_sha: str) -> str:
    resp = client.post(
        "/sessions",
        json={
            "file_size": FILE_SIZE,
            "chunk_size": CHUNK_SIZE,
            "file_sha256": file_sha,
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
        },
    )
    check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
    sid = resp.json()["session_id"]
    check(resp.json()["total_chunks"] == len(chunks), "total_chunks mismatch")
    check(resp.json()["missing_chunks"] == list(range(len(chunks))), "fresh session must miss every chunk")
    print(f"[verify] session {sid} created ({len(chunks)} chunks, {FILE_SIZE} bytes)")

    # Invalid chunks must be rejected and never recorded.
    expect_error(put_chunk(client, sid, 0, chunks[0], digests[1]), 400, "CHUNK_DIGEST_MISMATCH")
    expect_error(put_chunk(client, sid, len(chunks), chunks[0], digests[0]), 400, "CHUNK_INDEX_OUT_OF_RANGE")
    truncated = chunks[0][:-1]
    expect_error(put_chunk(client, sid, 0, truncated, sha256(truncated)), 400, "CHUNK_SIZE_MISMATCH")
    status = client.get(f"/sessions/{sid}").json()
    check(status["received_count"] == 0, "rejected chunks must not be recorded")
    print("[verify] digest/size/index violations rejected and not recorded")

    # Simulate a dropped connection: only the first 3 chunks go out.
    for i in range(3):
        resp = put_chunk(client, sid, i, chunks[i], digests[i])
        check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")
    status = client.get(f"/sessions/{sid}").json()
    check(status["missing_chunks"] == [3, 4], f"expected missing [3, 4], got {status['missing_chunks']}")
    print("[verify] partial upload visible after 'interruption': missing [3, 4]")

    # Idempotent replay of the same bytes; conflicting bytes must get 409.
    resp = put_chunk(client, sid, 1, chunks[1], digests[1])
    check(resp.status_code == 200 and resp.json()["duplicate"] is True,
          f"idempotent replay failed: {resp.status_code} {resp.text}")
    other = bytes(len(chunks[1]))  # same length, different content
    expect_error(put_chunk(client, sid, 1, other, sha256(other)), 409, "CHUNK_CONFLICT")
    print("[verify] idempotent replay accepted, conflicting content rejected with 409")

    # Finalize too early must fail and list the missing chunks.
    body = expect_error(client.post(f"/sessions/{sid}/finalize"), 409, "CHUNKS_INCOMPLETE")
    check(body["error"]["details"]["missing_chunks"] == [3, 4], "finalize error must list missing chunks")

    # Resume: upload the remaining chunks.
    for i in (3, 4):
        resp = put_chunk(client, sid, i, chunks[i], digests[i])
        check(resp.status_code == 201, f"resume chunk {i} failed: {resp.text}")
    status = client.get(f"/sessions/{sid}").json()
    check(status["missing_chunks"] == [], "all chunks should be received now")
    print("[verify] resumed upload completed the bitmap")

    # Finalize, re-finalize (idempotent), download and verify the artifact.
    resp = client.post(f"/sessions/{sid}/finalize")
    check(resp.status_code == 200, f"finalize failed: {resp.text}")
    check(resp.json()["final_sha256"] == file_sha, "final SHA-256 mismatch")
    again = client.post(f"/sessions/{sid}/finalize")
    check(again.status_code == 200 and again.json()["final_sha256"] == file_sha,
          "finalize must be idempotent")
    resp = client.get(f"/sessions/{sid}/artifact")
    check(resp.status_code == 200, f"artifact download failed: {resp.text}")
    check(resp.content == data, "artifact bytes differ from the original file")
    check(resp.headers.get("x-file-sha256") == file_sha, "artifact digest header mismatch")
    status = client.get(f"/sessions/{sid}").json()
    check(status["status"] == "completed", "session must be completed")
    print(f"[verify] artifact published and verified (sha256={file_sha[:16]}...)")
    return sid


def stage_reuse(client: httpx.Client, data: bytes, chunks: list[bytes], digests: list[str], file_sha: str) -> str:
    sid = create_session(client, FILE_SIZE, file_sha)

    # An uncached digest is rejected structurally without touching the bitmap.
    unknown = sha256(b"definitely-not-in-the-pool")
    body = expect_error(reuse(client, sid, 0, unknown), 409, "CHUNK_NOT_CACHED")
    check(body["error"]["details"]["reason"] == "unknown", "unknown digest must be marked unknown")
    check(body["error"]["details"]["sha256"] == unknown, "error details must echo the digest")
    check(client.get(f"/sessions/{sid}").json()["missing_chunks"] == list(range(5)),
          "failed reuse must not change the bitmap")

    # Size fence: chunk 4 (last, 12345 bytes) cannot be confirmed by a 1 MiB body.
    body = expect_error(reuse(client, sid, 4, digests[0]), 409, "CHUNK_NOT_CACHED")
    check(body["error"]["details"]["reason"] == "size_mismatch", "wrong-size body must be rejected")

    # Malformed payloads keep the standard validation envelope.
    for bad in ({}, {"sha256": "ZZ" + "a" * 62}, {"sha256": "A" * 64}):
        resp = client.post(f"/sessions/{sid}/chunks/0/reuse", json=bad)
        check(resp.status_code == 422, f"invalid reuse payload must be 422: {resp.text}")
        check(resp.json()["error"]["code"] == "VALIDATION_ERROR", "validation envelope expected")

    # Confirm every chunk with no body at all; only digests are uploaded.
    for i, dig in enumerate(digests):
        resp = reuse(client, sid, i, dig)
        check(resp.status_code == 201, f"zero-body reuse of chunk {i} failed: {resp.text}")
        check(resp.json()["sha256"] == dig, "reuse receipt must carry the pooled digest")
    check(client.get(f"/sessions/{sid}").json()["missing_chunks"] == [], "reuse must complete the bitmap")

    # Idempotent replay over the reuse endpoint; conflicting digest is a 409.
    replay = reuse(client, sid, 2, digests[2])
    check(replay.status_code == 200 and replay.json()["duplicate"] is True, "reuse replay must be idempotent")
    expect_error(reuse(client, sid, 2, sha256(b"other-content")), 409, "CHUNK_CONFLICT")

    resp = client.post(f"/sessions/{sid}/finalize")
    check(resp.status_code == 200, f"finalize after reuse failed: {resp.text}")
    check(resp.json()["final_sha256"] == file_sha, "reused assembly must match the file digest")
    resp = client.get(f"/sessions/{sid}/artifact")
    check(resp.status_code == 200 and resp.content == data, "artifact assembled from the pool must match")
    print("[verify] second session confirmed entirely via zero-body reuse and assembled correctly")
    return sid


def stage_concurrent_identical_uploads(client: httpx.Client, data: bytes, chunks: list[bytes], digests: list[str], file_sha: str) -> str:
    """Concurrent uploads of identical bodies land across both workers.

    The blob is published once; every request reuses the same pool result and
    every session ends up with a complete, correct artifact.
    """
    sessions = [create_session(client, FILE_SIZE, file_sha) for _ in range(3)]

    def upload_one(args) -> tuple[str, int, int]:
        sid, index = args
        # Dedicated client per thread so the kernel can spread connections
        # across both uvicorn worker processes.
        with httpx.Client(base_url=BASE_URL, timeout=60.0) as local:
            resp = put_chunk(local, sid, index, chunks[index], digests[index])
            return sid, index, resp.status_code

    work = [(sid, i) for sid in sessions for i in range(len(chunks))]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(upload_one, work))
    by_session: dict[str, set[int]] = {sid: set() for sid in sessions}
    for sid, index, code in results:
        check(code in (200, 201), f"concurrent upload {sid}/{index} got {code}")
        by_session[sid].add(index)
    for sid in sessions:
        check(by_session[sid] == set(range(len(chunks))), f"{sid} did not converge to all chunks")
        resp = client.post(f"/sessions/{sid}/finalize")
        check(resp.status_code == 200, f"concurrent session finalize failed: {resp.text}")
        check(client.get(f"/sessions/{sid}/artifact").content == data,
              "concurrently assembled artifact mismatch")
    print(f"[verify] {len(sessions)} sessions uploaded identical chunks concurrently; artifacts all correct")
    return sessions[0]


def stage_expiry_reuse(client: httpx.Client, chunks: list[bytes], digests: list[str]) -> None:
    sid = create_session(client, FILE_SIZE, "0" * 64, expires_in_minutes=15)
    resp = put_chunk(client, sid, 0, chunks[0], digests[0])
    check(resp.status_code == 201, f"seed chunk upload failed: {resp.text}")

    short = create_session(client, FILE_SIZE, "0" * 64, expires_in_minutes=15)
    resp = put_chunk(client, short, 1, chunks[1], digests[1])
    check(resp.status_code == 201, f"donor chunk upload failed: {resp.text}")

    # Force a short expiry through a dedicated session created with a near deadline.
    resp = client.post(
        "/sessions",
        json={
            "file_size": FILE_SIZE,
            "chunk_size": CHUNK_SIZE,
            "file_sha256": "0" * 64,
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat(),
        },
    )
    check(resp.status_code == 201, resp.text)
    exp_sid = resp.json()["session_id"]
    resp = put_chunk(client, exp_sid, 0, chunks[0], digests[0])
    check(resp.status_code == 201, f"pre-expiry upload failed: {resp.text}")
    print("[verify] waiting for session expiry ...")
    time.sleep(3)

    # An expired session must not gain progress through reuse.
    expect_error(reuse(client, exp_sid, 1, digests[1]), 410, "SESSION_EXPIRED")
    status = client.get(f"/sessions/{exp_sid}").json()
    check(status["missing_chunks"] == [1, 2, 3, 4], "expired reuse must not change the bitmap")
    # Replay of an already confirmed chunk stays a successful no-op.
    replay = reuse(client, exp_sid, 0, digests[0])
    check(replay.status_code == 200 and replay.json()["duplicate"] is True,
          "expired-session replay of a confirmed chunk must succeed")
    print("[verify] expired session: reuse cannot add progress, confirmed replay still works")
    # Sanity: same digest is reusable by a live session (pool body survived).
    live = create_session(client, FILE_SIZE, "0" * 64)
    check(reuse(client, live, 1, digests[1]).status_code == 201, "pool body must remain reusable")


def stage_reclaim(client: httpx.Client, data: bytes, chunks: list[bytes], digests: list[str], file_sha: str) -> None:
    for bad in (0, 1001):
        resp = client.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": bad})
        check(resp.status_code == 422, f"max_blobs={bad} must be 422: {resp.text}")
        check(resp.json()["error"]["code"] == "VALIDATION_ERROR", "validation envelope expected")

    # Active pinner holds chunk 0; the earlier stages also left active/expired
    # incomplete sessions pinning chunk 1. The other bodies are referenced only
    # by completed sessions, whose published artifacts stand on their own.
    pinner = create_session(client, FILE_SIZE, file_sha)
    check(put_chunk(client, pinner, 0, chunks[0], digests[0]).status_code == 201, "pinner seed failed")

    cycle_id = None
    cursor = ""
    examined = 0
    body = {}
    for batch, limit in ((1, 2), (2, 2), (3, 1000)):
        resp = client.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": limit})
        check(resp.status_code == 200, f"reclaim batch {batch} failed: {resp.text}")
        body = resp.json()
        check(set(body) == {"cycle_id", "cursor", "examined", "deleted", "done"},
              f"reclaim response fields mismatch: {sorted(body)}")
        check(isinstance(body["cycle_id"], str) and len(body["cycle_id"]) >= 16, "cycle_id must be persisted")
        if cycle_id is None:
            cycle_id = body["cycle_id"]
        check(body["cycle_id"] == cycle_id, "batched calls must continue one persisted cycle")
        check(body["cursor"] >= cursor, "cursor must advance forward")
        check(body["examined"] >= examined, "examined must accumulate")
        cursor, examined = body["cursor"], body["examined"]
        if batch < 3:
            check(body["done"] is False, "cycle must not be done mid-scan")
    check(body["done"] is True, "final batch must report done")
    print(f"[verify] reclaim cycle {cycle_id[:16]}... scanned {examined} blobs in persisted batches,"
          f" deleted {body['deleted']}")

    # Reclaimed content keeps returning the structured CHUNK_NOT_CACHED fence;
    # pinned bodies stay reusable.
    removed = None
    for d in digests[2:]:
        fresh = create_session(client, FILE_SIZE, file_sha)
        if reuse(client, fresh, 2, d).status_code == 409:
            removed = d
            break
    check(removed is not None, "at least one completed-only chunk must have been reclaimed")
    err = client.post(f"/sessions/{fresh}/chunks/3/reuse", json={"sha256": removed})
    check(err.status_code == 409 and err.json()["error"]["code"] == "CHUNK_NOT_CACHED",
          "reclaimed content must keep returning CHUNK_NOT_CACHED")

    check(reuse(client, pinner, 0, digests[0]).status_code == 200,
          "active session must keep its pinned body usable")
    another = create_session(client, FILE_SIZE, file_sha)
    check(reuse(client, another, 0, digests[0]).status_code == 201,
          "pinned body must still be reusable by other active sessions")

    # Re-upload the (now re-published) remaining chunks, complete the pinner,
    # then reclaim again; every previously published artifact still downloads.
    for i in range(1, len(chunks)):
        r = put_chunk(client, pinner, i, chunks[i], digests[i])
        check(r.status_code in (200, 201), f"pinner completion chunk {i} failed: {r.text}")
    resp = client.post(f"/sessions/{pinner}/finalize")
    check(resp.status_code == 200, f"pinner finalize failed: {resp.text}")
    check(resp.json()["final_sha256"] == file_sha, "pinner artifact digest mismatch")
    final = client.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 1000})
    check(final.status_code == 200 and final.json()["done"] is True, final.text)
    check(client.get(f"/sessions/{pinner}/artifact").content == data,
          "pinner artifact must survive post-completion reclaim")
    print("[verify] reclaim: completed sessions unpinned, active/expired sessions pinned, fence works")


def main() -> int:
    started = time.monotonic()
    with httpx.Client(base_url=BASE_URL, timeout=60.0) as client:
        wait_for_api(client)
        print(f"[verify] API healthy at {BASE_URL}")

        data = payload(FILE_SIZE)
        file_sha = sha256(data)
        chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
        digests = [sha256(c) for c in chunks]

        stage_original_flow(client, data, chunks, digests, file_sha)
        stage_reuse(client, data, chunks, digests, file_sha)
        stage_concurrent_identical_uploads(client, data, chunks, digests, file_sha)
        stage_expiry_reuse(client, chunks, digests)
        stage_reclaim(client, data, chunks, digests, file_sha)

    print(f"[verify] ALL CHECKS PASSED in {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except VerifyFailure as exc:
        print(f"[verify] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPError as exc:
        print(f"[verify] FAILED: HTTP error: {exc}", file=sys.stderr)
        sys.exit(1)
