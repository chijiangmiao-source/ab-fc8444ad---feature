"""Global content-addressed chunk pool: reuse, dedup, reclaim, upgrade, recovery."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import clock
from app.config import Settings
from app.main import create_app


# ---------- helpers ----------

def make_bytes(size: int, seed: str = "payload") -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def future_expiry(hours: float = 1.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def create_session(client, payload: bytes, chunk_size: int, *, file_sha256=None, expires_at=None) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(payload),
            "chunk_size": chunk_size,
            "file_sha256": file_sha256 or sha256(payload),
            "expires_at": expires_at or future_expiry(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def put_chunk(client, sid, index, body, digest=None):
    digest = digest if digest is not None else sha256(body)
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers={"X-Chunk-SHA256": digest})


def reuse(client, sid, index, digest):
    return client.post(f"/sessions/{sid}/chunks/{index}/reuse", json={"sha256": digest})


def reclaim(client, max_blobs):
    return client.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": max_blobs})


def chunk(payload, chunk_size, i):
    return payload[i * chunk_size : (i + 1) * chunk_size]


def upload_all(client, sid, payload, chunk_size):
    for i in range(-(-len(payload) // chunk_size)):
        resp = put_chunk(client, sid, i, chunk(payload, chunk_size, i))
        assert resp.status_code == 201, resp.text


def pool_files(data_dir) -> list[Path]:
    return sorted((data_dir / "pool" / "blobs").glob("*.blob"))


def db_rows(data_dir, sql, args=()):
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def blob_row(data_dir, digest, size):
    rows = db_rows(data_dir, "SELECT * FROM pool_blobs WHERE sha256=? AND size=?", (digest, size))
    return rows[0] if rows else None


def chunk_row(data_dir, sid, index):
    rows = db_rows(data_dir, "SELECT * FROM chunks WHERE session_id=? AND chunk_index=?", (sid, index))
    return rows[0] if rows else None


def downgrade_chunk(data_dir, sid, index):
    """Convert a pool-backed chunk row into a pre-pool (legacy) record."""
    row = chunk_row(data_dir, sid, index)
    assert row and row["blob_sha256"], "expected a pool-backed row"
    legacy_dir = data_dir / "chunks" / sid
    legacy_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = legacy_dir / f"{index:08d}.chunk"
    legacy_path.write_bytes(Path(row["path"]).read_bytes())
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    try:
        conn.execute(
            "UPDATE chunks SET path=?, blob_sha256=NULL, blob_size=NULL"
            " WHERE session_id=? AND chunk_index=?",
            (str(legacy_path), sid, index),
        )
        conn.execute("DELETE FROM pool_blobs WHERE sha256=? AND size=?", (row["sha256"], row["size"]))
        conn.commit()
    finally:
        conn.close()
    Path(row["path"]).unlink()
    return legacy_path


# ---------- reuse endpoint ----------

def test_reuse_confirms_chunk_from_pool(client):
    payload = make_bytes(10)
    a = create_session(client, payload, 4)["session_id"]
    assert put_chunk(client, a, 0, chunk(payload, 4, 0)).status_code == 201

    b = create_session(client, payload, 4)["session_id"]
    resp = reuse(client, b, 0, sha256(chunk(payload, 4, 0)))
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["duplicate"] is False
    assert body["sha256"] == sha256(chunk(payload, 4, 0))
    assert body["size"] == 4
    assert body["received_count"] == 1
    assert client.get(f"/sessions/{b}").json()["missing_chunks"] == [1, 2]

    # both sessions finish from the same pooled body
    for i in (1, 2):
        assert put_chunk(client, a, i, chunk(payload, 4, i)).status_code == 201
        assert reuse(client, b, i, sha256(chunk(payload, 4, i))).status_code == 201
    for sid in (a, b):
        assert client.post(f"/sessions/{sid}/finalize").status_code == 200
        assert client.get(f"/sessions/{sid}/artifact").content == payload


def test_reuse_idempotent_replay_and_conflict(client):
    payload = make_bytes(10)
    a = create_session(client, payload, 4)["session_id"]
    put_chunk(client, a, 0, chunk(payload, 4, 0))
    b = create_session(client, payload, 4)["session_id"]
    assert reuse(client, b, 0, sha256(chunk(payload, 4, 0))).status_code == 201

    replay = reuse(client, b, 0, sha256(chunk(payload, 4, 0)))
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    assert replay.json()["received_count"] == 1

    other = make_bytes(4, "other")
    conflict = reuse(client, b, 0, sha256(other))
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "CHUNK_CONFLICT"
    assert client.get(f"/sessions/{b}").json()["missing_chunks"] == [1, 2]


def test_reuse_unknown_digest_is_not_cached_and_bitmap_untouched(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, chunk(payload, 4, 0))
    before = client.get(f"/sessions/{sid}").json()

    digest = sha256(b"never uploaded anywhere")
    resp = reuse(client, sid, 1, digest)
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "CHUNK_NOT_CACHED"
    assert body["error"]["details"]["sha256"] == digest
    assert body["error"]["details"]["chunk_index"] == 1
    after = client.get(f"/sessions/{sid}").json()
    assert after["missing_chunks"] == before["missing_chunks"]
    assert after["received_count"] == before["received_count"]


def test_reuse_validation_errors(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    digest = sha256(chunk(payload, 4, 0))

    resp = reuse(client, sid, 0, digest.upper())
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    resp = reuse(client, sid, 0, "zz")
    assert resp.status_code == 422
    resp = client.post(f"/sessions/{sid}/chunks/0/reuse", json={"sha256": 123})
    assert resp.status_code == 422

    for bad_index in ("3", "-1", "abc"):
        resp = reuse(client, sid, bad_index, digest)
        assert resp.status_code == 400, bad_index
        assert resp.json()["error"]["code"] == "CHUNK_INDEX_OUT_OF_RANGE"

    resp = reuse(client, "no-such-session", 0, digest)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "SESSION_NOT_FOUND"


def test_reuse_expired_session(client, monkeypatch):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, chunk(payload, 4, 0))

    later = datetime.now(timezone.utc) + timedelta(hours=2)
    monkeypatch.setattr(clock, "utcnow", lambda: later)
    # idempotent replay of a confirmed chunk is still allowed
    replay = reuse(client, sid, 0, sha256(chunk(payload, 4, 0)))
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    # but reuse must not add progress to an expired session
    resp = reuse(client, sid, 1, sha256(chunk(payload, 4, 1)))
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "SESSION_EXPIRED"
    assert client.get(f"/sessions/{sid}").json()["missing_chunks"] == [1, 2]


def test_reuse_completed_session(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    upload_all(client, sid, payload, 4)
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200

    replay = reuse(client, sid, 0, sha256(chunk(payload, 4, 0)))
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    other = make_bytes(4, "other")
    conflict = reuse(client, sid, 0, sha256(other))
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "CHUNK_CONFLICT"


# ---------- dedup / arbitration ----------

def test_identical_content_stored_once(client, data_dir):
    payload = make_bytes(12)
    sids = [create_session(client, payload, 4)["session_id"] for _ in range(3)]
    for sid in sids:
        assert put_chunk(client, sid, 0, chunk(payload, 4, 0)).status_code == 201
    assert len(pool_files(data_dir)) == 1
    row = blob_row(data_dir, sha256(chunk(payload, 4, 0)), 4)
    assert row["state"] == "available"
    assert row["ref_count"] == 3
    # every session's chunk row points at the same pool file
    for sid in sids:
        rec = chunk_row(data_dir, sid, 0)
        assert rec["blob_sha256"] == sha256(chunk(payload, 4, 0))
        assert rec["path"] == row["path"]


def test_concurrent_uploads_have_single_publisher(data_dir):
    """Two app instances (two SQLite connections, like two workers) uploading
    the same body concurrently must produce exactly one pool file."""
    app1 = create_app(Settings(data_dir=data_dir))
    app2 = create_app(Settings(data_dir=data_dir))
    payload = make_bytes(8)
    body = chunk(payload, 4, 0)
    with TestClient(app1) as c1, TestClient(app2) as c2:
        sids = [create_session(c1, payload, 4)["session_id"] for _ in range(6)]
        results = []

        def upload(sid, n):
            client = c1 if n % 2 == 0 else c2
            results.append(put_chunk(client, sid, 0, body).status_code)

        threads = [threading.Thread(target=upload, args=(sid, n)) for n, sid in enumerate(sids)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert sorted(results) == [201] * 6
    assert len(pool_files(data_dir)) == 1
    assert blob_row(data_dir, sha256(body), 4)["ref_count"] == 6


def test_failed_requests_leave_no_pollution(client, data_dir):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    # digest mismatch and size mismatch never reach the pool
    put_chunk(client, sid, 0, chunk(payload, 4, 0), digest=sha256(b"wrong"))
    put_chunk(client, sid, 0, payload[:3])
    assert pool_files(data_dir) == []
    assert db_rows(data_dir, "SELECT * FROM pool_blobs") == []
    assert db_rows(data_dir, "SELECT * FROM chunks") == []
    status = client.get(f"/sessions/{sid}").json()
    assert status["received_count"] == 0


# ---------- reclaim ----------

def test_reclaim_response_shape_and_cycle_ids(client):
    first = reclaim(client, 5)
    assert first.status_code == 200
    body = first.json()
    assert set(body) == {"cycle_id", "cursor", "examined", "deleted", "done"}
    assert body["examined"] == 0
    assert body["deleted"] == 0
    assert body["done"] is True
    again = reclaim(client, 5).json()
    assert again["cycle_id"] == body["cycle_id"] + 1


def test_reclaim_validation(client):
    for bad in (0, 1001, -3, "many"):
        resp = reclaim(client, bad)
        assert resp.status_code == 422, bad
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    resp = client.post("/maintenance/chunk-pool/reclaim", json={})
    assert resp.status_code == 422


def test_reclaim_deletes_only_unpinned_blobs(client, data_dir):
    payload = make_bytes(8)
    a = create_session(client, payload, 4)["session_id"]
    b = create_session(client, payload, 4)["session_id"]
    put_chunk(client, a, 0, chunk(payload, 4, 0))
    assert reuse(client, b, 0, sha256(chunk(payload, 4, 0))).status_code == 201
    put_chunk(client, b, 1, chunk(payload, 4, 1))
    # completing b releases its pins; a still pins chunk 0
    assert client.post(f"/sessions/{b}/finalize").status_code == 200

    body = reclaim(client, 10).json()
    assert body["deleted"] == 1  # only chunk 1's blob; chunk 0 stays pinned by a
    assert body["done"] is True
    assert blob_row(data_dir, sha256(chunk(payload, 4, 0)), 4)["state"] == "available"
    assert blob_row(data_dir, sha256(chunk(payload, 4, 1)), 4) is None

    # a can still finalize from the pinned blob
    put_chunk(client, a, 1, chunk(payload, 4, 1))
    assert client.post(f"/sessions/{a}/finalize").status_code == 200
    body = reclaim(client, 10).json()
    assert body["deleted"] >= 1
    assert pool_files(data_dir) == []

    # reclaimed content is no longer reusable
    c = create_session(client, payload, 4)["session_id"]
    resp = reuse(client, c, 0, sha256(chunk(payload, 4, 0)))
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHUNK_NOT_CACHED"


def test_reclaim_cursor_paginates_and_survives_restart(client, data_dir):
    payloads = [make_bytes(4, "one"), make_bytes(4, "two")]
    for data in payloads:
        sid = create_session(client, data, 4)["session_id"]
        put_chunk(client, sid, 0, data)
        assert client.post(f"/sessions/{sid}/finalize").status_code == 200
    assert len(pool_files(data_dir)) == 2

    first = reclaim(client, 1).json()
    assert first["examined"] == 1
    assert first["deleted"] == 1
    assert first["done"] is False
    assert first["cursor"] != 0

    # a new process (fresh connection) resumes from the persisted cursor
    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        second = reclaim(c2, 1).json()
        assert second["cycle_id"] == first["cycle_id"] + 1
        assert second["examined"] == 1
        assert second["deleted"] == 1
        assert second["done"] is False  # exactly max_blobs examined: end not proven
        third = reclaim(c2, 1).json()
        assert third["examined"] == 0
        assert third["done"] is True
        assert third["cursor"] == 0
    assert pool_files(data_dir) == []


def test_reclaim_never_deletes_pinned_blob_of_expired_session(client, data_dir, monkeypatch):
    payload = make_bytes(4)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, payload)
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    monkeypatch.setattr(clock, "utcnow", lambda: later)
    assert client.get(f"/sessions/{sid}").json()["status"] == "expired"
    body = reclaim(client, 10).json()
    assert body["deleted"] == 0
    assert blob_row(data_dir, sha256(payload), 4)["state"] == "available"


def test_completed_session_survives_reclaim_of_its_blobs(client, data_dir):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    upload_all(client, sid, payload, 4)
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200
    deleted = reclaim(client, 100).json()["deleted"]
    assert deleted == 3
    # the completed session is carried by its artifact alone
    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "completed"
    assert status["received_count"] == 3
    assert client.get(f"/sessions/{sid}/artifact").content == payload


# ---------- legacy volume upgrade ----------

def test_legacy_chunks_promote_lazily_and_finalize_mixed(client, data_dir):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    upload_all(client, sid, payload, 4)
    legacy_paths = [downgrade_chunk(data_dir, sid, i) for i in range(3)]
    assert pool_files(data_dir) == []

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        assert status["received_count"] == 3
        assert status["missing_chunks"] == []

        # another session uploads the same content first: promotion must adopt
        other = create_session(c2, payload, 4)["session_id"]
        assert put_chunk(c2, other, 0, chunk(payload, 4, 0)).status_code == 201
        assert len(pool_files(data_dir)) == 1

        # replaying chunk 0 promotes the legacy record onto the existing blob
        replay = put_chunk(c2, sid, 0, chunk(payload, 4, 0))
        assert replay.status_code == 200
        assert replay.json()["duplicate"] is True
        assert len(pool_files(data_dir)) == 1
        assert not legacy_paths[0].exists()
        rec = chunk_row(data_dir, sid, 0)
        assert rec["blob_sha256"] == sha256(chunk(payload, 4, 0))

        # replaying chunk 1 publishes a second blob; chunk 2 stays legacy
        assert put_chunk(c2, sid, 1, chunk(payload, 4, 1)).status_code == 200
        assert len(pool_files(data_dir)) == 2
        assert legacy_paths[2].exists()

        # finalize reads pool blobs and the remaining legacy file directly
        resp = c2.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200
        assert resp.json()["final_sha256"] == sha256(payload)
        assert c2.get(f"/sessions/{sid}/artifact").content == payload


def test_legacy_reuse_replay_promotes(client, data_dir):
    payload = make_bytes(8)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, chunk(payload, 4, 0))
    legacy_path = downgrade_chunk(data_dir, sid, 0)

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        replay = reuse(c2, sid, 0, sha256(chunk(payload, 4, 0)))
        assert replay.status_code == 200
        assert replay.json()["duplicate"] is True
        assert not legacy_path.exists()
        rec = chunk_row(data_dir, sid, 0)
        assert rec["blob_sha256"] == sha256(chunk(payload, 4, 0))
        assert blob_row(data_dir, sha256(chunk(payload, 4, 0)), 4)["ref_count"] == 1
        # now another session can reuse the promoted content
        other = create_session(c2, payload, 4)["session_id"]
        assert reuse(c2, other, 0, sha256(chunk(payload, 4, 0))).status_code == 201


def test_corrupt_legacy_chunk_is_unconfirmed_on_access(client, data_dir):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    upload_all(client, sid, payload, 4)
    legacy_path = downgrade_chunk(data_dir, sid, 1)
    # corrupt the legacy body, keeping its size
    legacy_path.write_bytes(b"\xff" * 4)

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        # size still matches at restart, so the row survives reconciliation...
        assert c2.get(f"/sessions/{sid}").json()["received_count"] == 3
        # ...but accessing it re-verifies the digest and un-confirms the chunk
        resp = reuse(c2, sid, 1, sha256(chunk(payload, 4, 1)))
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "CHUNK_NOT_CACHED"
        status = c2.get(f"/sessions/{sid}").json()
        assert status["missing_chunks"] == [1]
        assert chunk_row(data_dir, sid, 1) is None
        # re-uploading heals the session
        assert put_chunk(c2, sid, 1, chunk(payload, 4, 1)).status_code == 201
        assert c2.post(f"/sessions/{sid}/finalize").status_code == 200


def test_missing_legacy_chunk_is_unconfirmed_at_restart(client, data_dir):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    upload_all(client, sid, payload, 4)
    legacy_path = downgrade_chunk(data_dir, sid, 2)
    legacy_path.unlink()

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        assert status["missing_chunks"] == [2]
        assert chunk_row(data_dir, sid, 2) is None
        assert put_chunk(c2, sid, 2, chunk(payload, 4, 2)).status_code == 201
        assert c2.post(f"/sessions/{sid}/finalize").status_code == 200


# ---------- restart convergence of pool intermediate states ----------

def test_genuine_v1_volume_migrates_and_finalizes(data_dir):
    """A pre-pool database (no blob columns, no pool tables) is migrated in
    place; legacy sessions keep working and finalize from legacy files."""
    payload = make_bytes(10)
    sid = "legacy-session"
    chunk_dir = data_dir / "chunks" / sid
    chunk_dir.mkdir(parents=True)
    (data_dir / "artifacts").mkdir()
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.executescript(
        """
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY, file_size INTEGER NOT NULL,
            chunk_size INTEGER NOT NULL, total_chunks INTEGER NOT NULL,
            file_sha256 TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
            bitmap BLOB NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL,
            completed_at TEXT, final_sha256 TEXT, artifact_path TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            size INTEGER NOT NULL, sha256 TEXT NOT NULL, path TEXT NOT NULL,
            received_at TEXT NOT NULL,
            PRIMARY KEY (session_id, chunk_index)
        );
        """
    )
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, len(payload), 4, 3, sha256(payload), "active", bytes([0b111]),
         future_expiry(), datetime.now(timezone.utc).isoformat(), None, None, None),
    )
    for i in range(3):
        body = chunk(payload, 4, i)
        path = chunk_dir / f"{i:08d}.chunk"
        path.write_bytes(body)
        conn.execute(
            "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
            (sid, i, len(body), sha256(body), str(path), datetime.now(timezone.utc).isoformat()),
        )
    conn.commit()
    conn.close()

    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        status = c.get(f"/sessions/{sid}").json()
        assert status["received_count"] == 3
        resp = c.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200
        assert resp.json()["final_sha256"] == sha256(payload)
        assert c.get(f"/sessions/{sid}/artifact").content == payload
    # migration added the pool schema without touching legacy rows
    cols = {r["name"] for r in db_rows(data_dir, "PRAGMA table_info(chunks)")}
    assert {"blob_sha256", "blob_size"} <= cols
    assert db_rows(data_dir, "SELECT * FROM pool_blobs") == []
    rows = db_rows(data_dir, "SELECT blob_sha256 FROM chunks WHERE session_id=?", (sid,))
    assert all(r["blob_sha256"] is None for r in rows)


def _insert_blob_row(data_dir, digest, size, path, state, created_at, sealed_at=None):
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    try:
        conn.execute(
            "INSERT INTO pool_blobs (sha256, size, path, state, ref_count, created_at, sealed_at)"
            " VALUES (?, ?, ?, ?, 0, ?, ?)",
            (digest, size, str(path), state, created_at, sealed_at),
        )
        conn.commit()
    finally:
        conn.close()


def _age(path: Path, seconds: float = 120.0):
    old = time.time() - seconds
    os.utime(path, (old, old))


def test_recovery_converges_intermediate_pool_states(client, data_dir):
    payload = make_bytes(4)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, payload)
    pinned = blob_row(data_dir, sha256(payload), 4)

    blobs = data_dir / "pool" / "blobs"
    now = datetime.now(timezone.utc)
    old_iso = (now - timedelta(seconds=120)).isoformat()

    # stale "writing" reservation from a crashed publisher (file + row)
    stale_writing = blobs / "stale-writing.blob"
    stale_writing.write_bytes(b"partial")
    _insert_blob_row(data_dir, "a" * 64, 6, stale_writing, "writing", old_iso)
    # fresh "writing" reservation: a live publisher may be working on it
    fresh_writing = blobs / "fresh-writing.blob"
    fresh_writing.write_bytes(b"incoming")
    _insert_blob_row(data_dir, "b" * 64, 9, fresh_writing, "writing", now.isoformat())
    # sealed blob whose reclaimer crashed before deleting the file
    sealed = blobs / "sealed.blob"
    sealed.write_bytes(b"doomed")
    _insert_blob_row(data_dir, "c" * 64, 6, sealed, "sealed", old_iso, old_iso)
    # sealed row whose file is already gone (crash between unlink and row delete)
    _insert_blob_row(data_dir, "d" * 64, 6, blobs / "already-gone.blob", "sealed", old_iso, old_iso)
    # orphan blob files without any row
    stale_orphan = blobs / "stale-orphan.blob"
    stale_orphan.write_bytes(b"orphan")
    _age(stale_orphan)
    fresh_orphan = blobs / "fresh-orphan.blob"
    fresh_orphan.write_bytes(b"orphan")
    # stale staging temp
    stale_tmp = data_dir / "pool" / "tmp" / ".crashed.tmp"
    stale_tmp.write_bytes(b"half")
    _age(stale_tmp)

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        # confirmed session is untouched
        assert c2.get(f"/sessions/{sid}").json()["received_count"] == 1
        assert Path(pinned["path"]).exists()
        assert blob_row(data_dir, sha256(payload), 4)["state"] == "available"
        # stale intermediates converged to "no body, no row"
        assert not stale_writing.exists()
        assert blob_row(data_dir, "a" * 64, 6) is None
        assert not sealed.exists()
        assert blob_row(data_dir, "c" * 64, 6) is None
        assert blob_row(data_dir, "d" * 64, 6) is None
        assert not stale_orphan.exists()
        assert not stale_tmp.exists()
        # fresh files belong to live workers: untouched
        assert fresh_writing.exists()
        assert blob_row(data_dir, "b" * 64, 9)["state"] == "writing"
        assert fresh_orphan.exists()


def test_reclaim_and_republish_same_content_converges(client, data_dir):
    payload = make_bytes(4)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, payload)
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200
    assert reclaim(client, 10).json()["deleted"] == 1
    assert pool_files(data_dir) == []

    # re-uploading the same content publishes a fresh blob
    other = create_session(client, payload, 4)["session_id"]
    assert put_chunk(client, other, 0, payload).status_code == 201
    assert len(pool_files(data_dir)) == 1
    assert client.post(f"/sessions/{other}/finalize").status_code == 200
    assert client.get(f"/sessions/{other}/artifact").content == payload


def test_sealed_blob_is_not_reusable(client, data_dir):
    """Fencing: once reclaim seals a candidate, reuse must get CHUNK_NOT_CACHED."""
    payload = make_bytes(4)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, payload)
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200  # releases the pin
    digest = sha256(payload)
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.execute(
        "UPDATE pool_blobs SET state='sealed', sealed_at=? WHERE sha256=?",
        (datetime.now(timezone.utc).isoformat(), digest),
    )
    conn.commit()
    conn.close()

    other = create_session(client, payload, 4)["session_id"]
    resp = reuse(client, other, 0, digest)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "CHUNK_NOT_CACHED"
    assert client.get(f"/sessions/{other}").json()["missing_chunks"] == [0]


def test_legacy_content_is_not_reusable_until_promoted(client, data_dir):
    """The pool is the only reuse source: a legacy (pre-pool) chunk held by
    another session does not satisfy a reuse request."""
    payload = make_bytes(8)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, chunk(payload, 4, 0))
    downgrade_chunk(data_dir, sid, 0)

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        other = create_session(c2, payload, 4)["session_id"]
        resp = reuse(c2, other, 0, sha256(chunk(payload, 4, 0)))
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "CHUNK_NOT_CACHED"
        # once the owning session replays it (promoting the legacy body)...
        assert put_chunk(c2, sid, 0, chunk(payload, 4, 0)).status_code == 200
        # ...the other session can reuse it
        assert reuse(c2, other, 0, sha256(chunk(payload, 4, 0))).status_code == 201
