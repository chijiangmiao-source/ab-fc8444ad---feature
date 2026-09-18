"""One-shot acceptance client: exercises the full resume flow against a live API.

Run with:  API_BASE_URL=http://api:8000 python -m app.verify
Exit code 0 means every check passed.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
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


def reuse_chunk(client: httpx.Client, sid: str, index: int, digest: str) -> httpx.Response:
    return client.post(f"/sessions/{sid}/chunks/{index}/reuse", json={"sha256": digest})


def new_session(client: httpx.Client, file_size: int, file_sha: str, ttl: timedelta,
                chunk_size: int = CHUNK_SIZE) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": file_size,
            "chunk_size": chunk_size,
            "file_sha256": file_sha,
            "expires_at": (datetime.now(timezone.utc) + ttl).isoformat(),
        },
    )
    check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
    return resp.json()


def main() -> int:
    started = time.monotonic()
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
        wait_for_api(client)
        print(f"[verify] API healthy at {BASE_URL}")

        data = payload(FILE_SIZE)
        file_sha = sha256(data)
        chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
        digests = [sha256(c) for c in chunks]

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
        session = resp.json()
        sid = session["session_id"]
        check(session["total_chunks"] == len(chunks), "total_chunks mismatch")
        check(session["missing_chunks"] == list(range(len(chunks))), "fresh session must miss every chunk")
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

        # ===== global chunk pool: body-less reuse across sessions =====
        session_b = new_session(client, FILE_SIZE, file_sha, timedelta(minutes=15))
        sid_b = session_b["session_id"]
        resp = reuse_chunk(client, sid_b, 0, digests[0])
        check(resp.status_code == 201 and resp.json()["duplicate"] is False,
              f"first reuse failed: {resp.status_code} {resp.text}")
        replay = reuse_chunk(client, sid_b, 0, digests[0])
        check(replay.status_code == 200 and replay.json()["duplicate"] is True,
              f"reuse replay must be idempotent: {replay.status_code} {replay.text}")
        expect_error(reuse_chunk(client, sid_b, 0, digests[1]), 409, "CHUNK_CONFLICT")
        expect_error(reuse_chunk(client, sid_b, 0, digests[0].upper()), 422, "VALIDATION_ERROR")
        missing_before = client.get(f"/sessions/{sid_b}").json()["missing_chunks"]
        ghost = sha256(b"content that was never uploaded")
        expect_error(reuse_chunk(client, sid_b, 1, ghost), 404, "CHUNK_NOT_CACHED")
        check(client.get(f"/sessions/{sid_b}").json()["missing_chunks"] == missing_before,
              "CHUNK_NOT_CACHED must not change the bitmap")
        for i in range(1, len(chunks)):
            resp = reuse_chunk(client, sid_b, i, digests[i])
            check(resp.status_code == 201, f"reuse chunk {i} failed: {resp.text}")
        resp = client.post(f"/sessions/{sid_b}/finalize")
        check(resp.status_code == 200, f"finalize of reused session failed: {resp.text}")
        check(client.get(f"/sessions/{sid_b}/artifact").content == data,
              "artifact of a fully reused session must match the original bytes")
        print("[verify] cross-session reuse confirmed chunks without bodies; artifact verified")

        # ===== reclaim: unpinned bodies are collected, pinned ones survive =====
        # a run-unique single-chunk file: after its session completes, nothing
        # anywhere pins its blob, so reclaim must collect exactly that content
        unique = payload(CHUNK_SIZE + 77)
        unique_sha = sha256(unique)
        session_g = new_session(client, len(unique), unique_sha, timedelta(minutes=15),
                                chunk_size=len(unique))
        sid_g = session_g["session_id"]
        resp = put_chunk(client, sid_g, 0, unique, unique_sha)
        check(resp.status_code == 201, f"unique blob upload failed: {resp.text}")
        check(client.post(f"/sessions/{sid_g}/finalize").status_code == 200,
              "unique session must complete so its blob is unpinned")

        resp = client.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 1000})
        check(resp.status_code == 200, f"reclaim failed: {resp.status_code} {resp.text}")
        cycle = resp.json()
        check(set(cycle) == {"cycle_id", "cursor", "examined", "deleted", "done"},
              f"reclaim response shape wrong: {cycle}")
        check(cycle["done"] is True and cycle["deleted"] >= len(chunks),
              f"expected all unpinned blobs reclaimed, got {cycle}")
        # the unique content is gone from the pool now
        session_h = new_session(client, len(unique), unique_sha, timedelta(minutes=15),
                                chunk_size=len(unique))
        expect_error(reuse_chunk(client, session_h["session_id"], 0, unique_sha), 404, "CHUNK_NOT_CACHED")
        print(f"[verify] reclaim cycle {cycle['cycle_id']} collected {cycle['deleted']} unpinned blobs")

        # re-upload one chunk; an active session pins it against reclaim
        session_c = new_session(client, FILE_SIZE, file_sha, timedelta(minutes=15))
        sid_c = session_c["session_id"]
        resp = put_chunk(client, sid_c, 0, chunks[0], digests[0])
        check(resp.status_code == 201, f"re-upload after reclaim failed: {resp.text}")
        session_d = new_session(client, FILE_SIZE, file_sha, timedelta(minutes=15))
        check(reuse_chunk(client, session_d["session_id"], 0, digests[0]).status_code == 201,
              "reuse of re-published content failed")
        again = client.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 1000}).json()
        check(again["cycle_id"] > cycle["cycle_id"], "cycle_id must be persisted and increasing")
        check(again["deleted"] == 0, f"pinned blob must survive reclaim: {again}")
        session_e = new_session(client, FILE_SIZE, file_sha, timedelta(minutes=15))
        check(reuse_chunk(client, session_e["session_id"], 0, digests[0]).status_code == 201,
              "pinned blob must stay reusable")
        print("[verify] pinned bodies survive reclaim; cycle ids persist")

        # ===== expired sessions: replay only, no new progress via reuse =====
        session_f = new_session(client, FILE_SIZE, file_sha, timedelta(seconds=1.5))
        sid_f = session_f["session_id"]
        resp = put_chunk(client, sid_f, 0, chunks[0], digests[0])
        check(resp.status_code == 201, f"chunk upload to short-lived session failed: {resp.text}")
        time.sleep(2.0)
        expect_error(reuse_chunk(client, sid_f, 1, digests[1]), 410, "SESSION_EXPIRED")
        replay = reuse_chunk(client, sid_f, 0, digests[0])
        check(replay.status_code == 200 and replay.json()["duplicate"] is True,
              f"expired session must still allow idempotent replay: {replay.status_code}")
        print("[verify] expired session: reuse blocked for new chunks, replay still idempotent")

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
