"""Core upload/resume/finalize logic shared by the HTTP routes.

Chunk bodies live in a global content-addressed pool keyed by (sha256, size);
sessions hold logical references (chunk rows) that pin pool blobs. All
arbitration between concurrent workers happens through SQLite transactions and
compare-and-set updates — never through process-local locks.
"""

from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncIterable

from . import clock
from .bitmap import clear_bit, count_set, missing_indices, new_bitmap, set_bit
from .db import Database
from .errors import ApiError
from .schemas import CreateSessionRequest
from .storage import ChunkStore, fsync_dir, hash_file

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# A "writing" pool row older than this is treated as abandoned by a crashed
# publisher; younger rows may belong to a live publisher in another worker.
_WRITING_STALE_AFTER = timedelta(seconds=60)
# Startup recovery only collects temp/orphan files whose mtime is older than
# this, so it never deletes bodies a live worker is publishing right now.
_RECOVERY_STALE_AFTER = timedelta(seconds=60)
# How long an upload waits for a concurrent publisher of the same content.
_PUBLISH_WAIT_TIMEOUT = 30.0
# How long a reuse request waits for an in-flight publish before deciding the
# content is not cached.
_REUSE_WAIT_TIMEOUT = 2.0
# Bounded retries for the publish -> reference-commit race window.
_COMMIT_RETRY_LIMIT = 5


class _BlobGone(Exception):
    """The pool blob vanished (or is not usable) before the reference committed."""


class UploadService:
    def __init__(self, db: Database, store: ChunkStore):
        self.db = db
        self.store = store

    # ---- sessions ----

    def create_session(self, req: CreateSessionRequest) -> dict:
        now = clock.utcnow()
        if req.expires_at <= now:
            raise ApiError(
                422,
                "SESSION_EXPIRES_IN_PAST",
                "expires_at must be in the future",
                {"expires_at": req.expires_at.isoformat()},
            )
        total = -(-req.file_size // req.chunk_size)  # ceil division
        session_id = uuid.uuid4().hex
        self.db.create_session(
            {
                "session_id": session_id,
                "file_size": req.file_size,
                "chunk_size": req.chunk_size,
                "total_chunks": total,
                "file_sha256": req.file_sha256,
                "status": "active",
                "bitmap": bytes(new_bitmap(total)),
                "expires_at": req.expires_at.isoformat(),
                "created_at": now.isoformat(),
            }
        )
        return self.public_session(self.get_session_or_404(session_id))

    def status(self, session_id: str) -> dict:
        return self.public_session(self.get_session_or_404(session_id))

    # ---- chunks ----

    async def upload_chunk(
        self,
        session_id: str,
        raw_index: str,
        declared_digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)
        index = self._parse_index(session, raw_index)
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_CHUNK_DIGEST",
                "X-Chunk-SHA256 must be 64 hexadecimal characters",
                {"received": declared_digest},
            )

        tmp, size, actual = await self.store.write_tmp(stream)
        try:
            expected = self.expected_chunk_size(session, index)
            if size != expected:
                raise ApiError(
                    400,
                    "CHUNK_SIZE_MISMATCH",
                    f"chunk {index} must be exactly {expected} bytes, got {size}",
                    {"chunk_index": index, "expected_size": expected, "actual_size": size},
                )
            if actual != digest:
                raise ApiError(
                    400,
                    "CHUNK_DIGEST_MISMATCH",
                    "chunk body SHA-256 does not match X-Chunk-SHA256; chunk was discarded",
                    {"chunk_index": index, "declared_sha256": digest, "actual_sha256": actual},
                )
            # keep the legacy per-session directory layout intact
            self.store.chunk_dir(session_id).mkdir(parents=True, exist_ok=True)
            for _ in range(_COMMIT_RETRY_LIMIT):
                self._ensure_blob(digest, size, source=tmp)
                try:
                    return self._commit_upload(session_id, index, digest, size)
                except _BlobGone:
                    continue  # the blob was reclaimed underneath us; republish
            raise ApiError(500, "INTERNAL_ERROR", "chunk reference could not be committed")
        finally:
            self.store.discard(tmp)

    def _commit_upload(self, session_id: str, index: int, digest: str, size: int) -> tuple[dict, int]:
        """Record the chunk reference and flip the bitmap bit in one transaction."""
        legacy_to_delete: str | None = None
        with self.db.write_tx() as conn:
            session = self.db.q_get_session(conn, session_id)
            if session is None:
                raise ApiError(
                    404, "SESSION_NOT_FOUND", f"no such session: {session_id}",
                    {"session_id": session_id},
                )
            fresh = True
            existing = self.db.q_get_chunk(conn, session_id, index)
            if existing is not None:
                if existing["sha256"] != digest:
                    raise ApiError(
                        409,
                        "CHUNK_CONFLICT",
                        "chunk index already holds different content; the stored chunk is unchanged",
                        {
                            "chunk_index": index,
                            "stored_sha256": existing["sha256"],
                            "rejected_sha256": digest,
                        },
                    )
                if session["status"] == "completed":
                    body, status_code, fresh = self._chunk_receipt(session, existing, True), 200, False
                elif existing["blob_sha256"] is None:
                    # legacy record replayed with a verified body: back it with
                    # the pool blob published from that body
                    blob = self._require_available(conn, digest, size)
                    if self.db.q_flip_chunk_to_blob(
                        conn, session_id, index, digest, size, blob["path"]
                    ):
                        self.db.q_incr_refcount(conn, digest, size, 1)
                    legacy_to_delete = existing["path"]
                    body, status_code, fresh = self._chunk_receipt(session, existing, True), 200, False
                elif Path(existing["path"]).exists():
                    body, status_code, fresh = self._chunk_receipt(session, existing, True), 200, False
                else:
                    # the confirmed body vanished: un-confirm and re-ingest
                    self._unconfirm(conn, session, existing)
            if fresh:
                if self.is_expired(session):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; new chunks are rejected",
                        {"expires_at": session["expires_at"]},
                    )
                if session["status"] != "active":
                    raise ApiError(
                        409,
                        "SESSION_ALREADY_COMPLETED",
                        "session is already completed and immutable",
                    )
                blob = self._require_available(conn, digest, size)
                record = {
                    "session_id": session_id,
                    "chunk_index": index,
                    "size": size,
                    "sha256": digest,
                    "path": blob["path"],
                    "received_at": clock.utcnow().isoformat(),
                    "blob_sha256": digest,
                    "blob_size": size,
                }
                bitmap = bytearray(session["bitmap"])
                set_bit(bitmap, index)
                session["bitmap"] = bytes(bitmap)
                self.db.q_insert_chunk(conn, record)
                self.db.q_incr_refcount(conn, digest, size, 1)
                self.db.q_update_bitmap(conn, session_id, session["bitmap"])
                body, status_code = self._chunk_receipt(session, record, False), 201
        if legacy_to_delete is not None:
            # only after the reference commit (and the pool dir fsync done at
            # publish time) may the old per-session file be removed
            self.store.discard(Path(legacy_to_delete))
            fsync_dir(Path(legacy_to_delete).parent)
        return body, status_code

    def reuse_chunk(self, session_id: str, raw_index: str, digest: str) -> tuple[dict, int]:
        """Confirm a chunk without a body, from content already in the pool."""
        session = self.get_session_or_404(session_id)
        index = self._parse_index(session, raw_index)
        expected = self.expected_chunk_size(session, index)

        # A legacy confirmation being replayed is promoted into the pool first
        # (re-verifying size and digest), so identical content converges to a
        # single pool blob.
        row = self.db.get_chunk(session_id, index)
        if (
            row is not None
            and row["sha256"] == digest
            and row["blob_sha256"] is None
            and session["status"] == "active"
        ):
            self._promote_legacy(session, row)

        self._await_blob(digest, expected, _REUSE_WAIT_TIMEOUT)
        try:
            with self.db.write_tx() as conn:
                session = self.db.q_get_session(conn, session_id)
                if session is None:
                    raise ApiError(
                        404, "SESSION_NOT_FOUND", f"no such session: {session_id}",
                        {"session_id": session_id},
                    )
                existing = self.db.q_get_chunk(conn, session_id, index)
                if existing is not None:
                    if existing["sha256"] == digest:
                        return self._chunk_receipt(session, existing, True), 200
                    raise ApiError(
                        409,
                        "CHUNK_CONFLICT",
                        "chunk index already holds different content; the stored chunk is unchanged",
                        {
                            "chunk_index": index,
                            "stored_sha256": existing["sha256"],
                            "rejected_sha256": digest,
                        },
                    )
                if self.is_expired(session):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; new chunks are rejected",
                        {"expires_at": session["expires_at"]},
                    )
                if session["status"] != "active":
                    raise ApiError(
                        409,
                        "SESSION_ALREADY_COMPLETED",
                        "session is already completed and immutable",
                    )
                blob = self._require_available(conn, digest, expected)
                record = {
                    "session_id": session_id,
                    "chunk_index": index,
                    "size": expected,
                    "sha256": digest,
                    "path": blob["path"],
                    "received_at": clock.utcnow().isoformat(),
                    "blob_sha256": digest,
                    "blob_size": expected,
                }
                bitmap = bytearray(session["bitmap"])
                set_bit(bitmap, index)
                session["bitmap"] = bytes(bitmap)
                self.db.q_insert_chunk(conn, record)
                self.db.q_incr_refcount(conn, digest, expected, 1)
                self.db.q_update_bitmap(conn, session_id, session["bitmap"])
                return self._chunk_receipt(session, record, False), 201
        except _BlobGone:
            # unknown, still being written, or fenced for reclaim — and if the
            # blob file was lost underneath an 'available' row, drop that row
            self.db.delete_pool_blob(digest, expected, state="available")
            raise ApiError(
                404,
                "CHUNK_NOT_CACHED",
                "chunk content is not cached in the pool (or is being reclaimed);"
                " upload it with a body first",
                {"chunk_index": index, "sha256": digest},
            )

    # ---- pool publishing ----

    def _ensure_blob(self, sha256: str, size: int, source: Path) -> None:
        """Make sure an available pool blob exists for (sha256, size).

        Publishes `source` (hard link, source kept) unless another request —
        possibly in another worker — already published the same content. The
        database row is the single arbitration point: exactly one publisher
        establishes the pool file, everyone else reuses that result.
        """
        deadline = time.monotonic() + _PUBLISH_WAIT_TIMEOUT
        while True:
            row = self.db.get_pool_blob(sha256, size)
            if row is not None:
                if row["state"] == "available":
                    if Path(row["path"]).exists():
                        return
                    # 'available' row whose file was lost: remove and republish
                    self.db.delete_pool_blob(sha256, size, state="available")
                    continue
                if row["state"] == "sealed":
                    # doomed content: finish the pending deletion (idempotent)
                    self.store.discard(Path(row["path"]))
                    self.db.delete_pool_blob(sha256, size, state="sealed")
                    continue
                # state == "writing": another publisher is in flight (or crashed)
                if _is_stale(row["created_at"], _WRITING_STALE_AFTER) or time.monotonic() > deadline:
                    if self.db.delete_pool_blob(
                        sha256, size, state="writing", created_at=row["created_at"]
                    ):
                        self.store.discard(Path(row["path"]))
                    continue
                time.sleep(0.05)
                continue
            dest = self.store.pool_blob_path(sha256, size)
            if not self.db.insert_pool_blob_writing(
                sha256, size, str(dest), clock.utcnow().isoformat()
            ):
                continue  # a racer reserved the key first; re-read and adopt
            try:
                self.store.link_into_pool(source, dest)
            except FileExistsError:
                self.db.delete_pool_blob(sha256, size, state="writing")
                continue
            if self.db.mark_pool_blob_available(sha256, size, str(dest)):
                return
            self.store.discard(dest)  # lost a takeover race; our file is orphaned

    def _require_available(self, conn, sha256: str, size: int) -> dict:
        """In-transaction check that the blob is still usable; this is what
        fences reuse/reference commits against a concurrent reclaim."""
        blob = self.db.q_get_pool_blob(conn, sha256, size)
        if blob is None or blob["state"] != "available" or not Path(blob["path"]).exists():
            raise _BlobGone
        return blob

    def _await_blob(self, sha256: str, size: int, timeout: float) -> None:
        """Give an in-flight publisher a brief chance to finish."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            row = self.db.get_pool_blob(sha256, size)
            if row is None or row["state"] != "writing":
                return
            time.sleep(0.05)

    # ---- legacy promotion ----

    def _promote_legacy(self, session: dict, row: dict) -> bool:
        """Lazily move a pre-pool chunk file into the global pool.

        The legacy body is re-verified (size and SHA-256) before anything is
        promoted; a missing or corrupt body un-confirms the chunk exactly like
        startup reconciliation would. The old file is removed only after the
        reference commit and the pool directory fsync.
        """
        session_id, index = session["session_id"], row["chunk_index"]
        path = Path(row["path"])
        digest, size = row["sha256"], row["size"]
        if not self._verify_file(path, size, digest):
            with self.db.write_tx() as conn:
                fresh = self.db.q_get_session(conn, session_id)
                current = self.db.q_get_chunk(conn, session_id, index)
                if fresh is not None and current is not None and current["blob_sha256"] is None:
                    self._unconfirm(conn, fresh, current)
            self.store.discard(path)
            return False
        for _ in range(3):
            self._ensure_blob(digest, size, source=path)
            try:
                with self.db.write_tx() as conn:
                    blob = self._require_available(conn, digest, size)
                    if self.db.q_flip_chunk_to_blob(conn, session_id, index, digest, size, blob["path"]):
                        self.db.q_incr_refcount(conn, digest, size, 1)
            except _BlobGone:
                continue  # the blob was reclaimed mid-promotion; republish
            self.store.discard(path)
            fsync_dir(path.parent)
            return True
        return False

    @staticmethod
    def _verify_file(path: Path, size: int, sha256: str) -> bool:
        try:
            if path.stat().st_size != size:
                return False
        except OSError:
            return False
        try:
            return hash_file(path) == sha256
        except OSError:
            return False

    # ---- reclaim ----

    def reclaim_pool(self, max_blobs: int) -> dict:
        """One bounded reclaim cycle over the persisted cursor.

        Candidates are fenced (sealed) inside the same kind of transaction
        that adds references, so the race between reclaim and reuse is
        serializable: whoever commits first wins.
        """
        cycle_id, cursor = self.db.begin_reclaim_cycle()
        candidates = self.db.list_pool_candidates(cursor, max_blobs)
        examined = 0
        deleted = 0
        last_rowid = cursor
        for row in candidates:
            examined += 1
            last_rowid = row["blob_rowid"]
            if row["state"] != "available":
                continue  # intermediate states converge at startup recovery
            if not self.db.seal_pool_blob(row["sha256"], row["size"], clock.utcnow().isoformat()):
                continue  # pinned, or a racing reclaimer got there first
            self.store.discard(Path(row["path"]))
            fsync_dir(self.store.pool_blobs_dir)
            self.db.delete_pool_blob(row["sha256"], row["size"], state="sealed")
            deleted += 1
        done = examined < max_blobs
        new_cursor = 0 if done else last_rowid
        self.db.finish_reclaim_cycle(new_cursor)
        return {
            "cycle_id": cycle_id,
            "cursor": new_cursor,
            "examined": examined,
            "deleted": deleted,
            "done": done,
        }

    # ---- finalize / artifact ----

    def finalize(self, session_id: str) -> dict:
        session = self.get_session_or_404(session_id)
        if session["status"] == "completed" and self.store.artifact_path(session_id).exists():
            return self._finalize_receipt(session)

        total = session["total_chunks"]
        missing = missing_indices(session["bitmap"], total)
        if missing:
            details = {
                "missing_chunks": missing,
                "received_count": total - len(missing),
                "total_chunks": total,
            }
            if self.is_expired(session):
                raise ApiError(
                    410,
                    "SESSION_EXPIRED",
                    "session expired with chunks still missing",
                    {**details, "expires_at": session["expires_at"]},
                )
            raise ApiError(409, "CHUNKS_INCOMPLETE", "cannot finalize; chunks are missing", details)

        rows = self.db.list_chunks(session_id)
        by_index = {row["chunk_index"]: row for row in rows}
        paths, lost = [], []
        for i in range(total):
            row = by_index.get(i)
            if row is None or not Path(row["path"]).exists():
                lost.append(i)
            else:
                # pool blob or not-yet-promoted legacy chunk: read in place,
                # never copied into a per-session layout
                paths.append(Path(row["path"]))
        if lost:
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "chunk files are missing on disk",
                {"missing_chunks": lost, "total_chunks": total},
            )

        tmp, size, digest = self.store.assemble_to_tmp(paths)
        if size != session["file_size"] or digest != session["file_sha256"]:
            self.store.discard(tmp)
            raise ApiError(
                422,
                "INTEGRITY_MISMATCH",
                "assembled file does not match the declared SHA-256; uploaded chunks are kept",
                {
                    "declared_sha256": session["file_sha256"],
                    "assembled_sha256": digest,
                    "declared_size": session["file_size"],
                    "assembled_size": size,
                },
            )
        final = self.store.publish(tmp, session_id)
        completed_at = clock.utcnow().isoformat()
        self.db.mark_completed(session_id, completed_at, digest, str(final))
        return self._finalize_receipt(self.get_session_or_404(session_id))

    def artifact_file(self, session_id: str) -> tuple[Path, str]:
        session = self.get_session_or_404(session_id)
        path = self.store.artifact_path(session_id)
        if session["status"] != "completed" or not path.exists():
            raise ApiError(
                409,
                "ARTIFACT_NOT_READY",
                "no published artifact for this session",
                {"status": self._derived_status(session)},
            )
        return path, session["final_sha256"]

    # ---- helpers ----

    def get_session_or_404(self, session_id: str) -> dict:
        session = self.db.get_session(session_id)
        if session is None:
            raise ApiError(
                404,
                "SESSION_NOT_FOUND",
                f"no such session: {session_id}",
                {"session_id": session_id},
            )
        return session

    def _parse_index(self, session: dict, raw_index: str) -> int:
        total = session["total_chunks"]
        try:
            index = int(raw_index)
        except ValueError:
            index = -1
        if index < 0 or index >= total:
            raise ApiError(
                400,
                "CHUNK_INDEX_OUT_OF_RANGE",
                f"chunk index {raw_index!r} is out of range; valid indices are 0..{total - 1}",
                {"chunk_index": raw_index, "total_chunks": total},
            )
        return index

    def _unconfirm(self, conn, session: dict, row: dict) -> None:
        """Drop a chunk confirmation whose body failed verification."""
        self.db.q_delete_chunk(conn, row["session_id"], row["chunk_index"])
        if row["blob_sha256"] is not None:
            self.db.q_incr_refcount(conn, row["blob_sha256"], row["blob_size"], -1)
        bitmap = bytearray(session["bitmap"])
        clear_bit(bitmap, row["chunk_index"])
        session["bitmap"] = bytes(bitmap)
        self.db.q_update_bitmap(conn, row["session_id"], session["bitmap"])

    @staticmethod
    def expected_chunk_size(session: dict, index: int) -> int:
        if index == session["total_chunks"] - 1:
            return session["file_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
        return session["chunk_size"]

    @staticmethod
    def is_expired(session: dict) -> bool:
        return clock.utcnow() >= datetime.fromisoformat(session["expires_at"])

    def _derived_status(self, session: dict) -> str:
        if session["status"] == "completed":
            return "completed"
        return "expired" if self.is_expired(session) else "active"

    def public_session(self, session: dict) -> dict:
        total = session["total_chunks"]
        missing = missing_indices(session["bitmap"], total)
        status = self._derived_status(session)
        return {
            "session_id": session["session_id"],
            "status": status,
            "file_size": session["file_size"],
            "chunk_size": session["chunk_size"],
            "total_chunks": total,
            "file_sha256": session["file_sha256"],
            "received_count": total - len(missing),
            "missing_chunks": missing,
            "expires_at": session["expires_at"],
            "created_at": session["created_at"],
            "completed_at": session["completed_at"],
            "final_sha256": session["final_sha256"],
            "artifact_url": f"/sessions/{session['session_id']}/artifact" if status == "completed" else None,
        }

    def _chunk_receipt(self, session: dict, record: dict, duplicate: bool) -> dict:
        total = session["total_chunks"]
        return {
            "session_id": session["session_id"],
            "chunk_index": record["chunk_index"],
            "size": record["size"],
            "sha256": record["sha256"],
            "duplicate": duplicate,
            "received_count": count_set(session["bitmap"], total),
            "total_chunks": total,
        }

    @staticmethod
    def _finalize_receipt(session: dict) -> dict:
        session_id = session["session_id"]
        return {
            "session_id": session_id,
            "status": "completed",
            "file_size": session["file_size"],
            "final_sha256": session["final_sha256"],
            "artifact_size": session["file_size"],
            "artifact_url": f"/sessions/{session_id}/artifact",
            "completed_at": session["completed_at"],
        }


def _is_stale(iso_ts: str, after: timedelta) -> bool:
    try:
        return clock.utcnow() - datetime.fromisoformat(iso_ts) > after
    except (TypeError, ValueError):
        return True


def reconcile(db: Database, store: ChunkStore) -> None:
    """Rebuild durable state after a (possibly unclean) restart.

    Sessions: chunk rows whose files vanished or have a wrong size are dropped,
    chunk files without a matching row are removed, the persisted bitmap is
    rebuilt from the surviving rows, and leftover temp files are removed.
    Completed sessions are left untouched: the atomic artifact alone carries
    them, even if their historical pool blobs were reclaimed since.

    Pool: persisted intermediate states are converged — sealed blobs finish
    being deleted, stale "writing" reservations from crashed publishers are
    dropped, and stale temp/orphan files are removed. Nothing is re-hashed and
    available blobs are never scanned; fresh files are left alone so a live
    worker publishing right now is never disturbed.
    """
    _reconcile_sessions(db, store)
    _reconcile_pool(db, store)


def _reconcile_sessions(db: Database, store: ChunkStore) -> None:
    for session in db.list_sessions():
        session_id = session["session_id"]
        chunk_dir = store.chunk_dir(session_id)
        if session["status"] == "completed":
            if chunk_dir.exists():
                for entry in chunk_dir.iterdir():
                    if entry.suffix == ".tmp":
                        entry.unlink()
            continue
        confirmed: set[int] = set()
        dropped: list[dict] = []
        for row in db.list_chunks(session_id):
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed.add(row["chunk_index"])
            else:
                dropped.append(row)
        if chunk_dir.exists():
            for entry in chunk_dir.iterdir():
                if entry.suffix == ".tmp":
                    entry.unlink()
                elif entry.suffix == ".chunk":
                    try:
                        index = int(entry.stem)
                    except ValueError:
                        entry.unlink()
                        continue
                    if index not in confirmed:
                        entry.unlink()
        bitmap = new_bitmap(session["total_chunks"])
        for index in confirmed:
            set_bit(bitmap, index)
        if dropped or bytes(bitmap) != session["bitmap"]:
            db.reconcile_session_chunks(session_id, dropped, bytes(bitmap))


def _reconcile_pool(db: Database, store: ChunkStore) -> None:
    stale_ts = (clock.utcnow() - _RECOVERY_STALE_AFTER).timestamp()
    for row in db.list_pool_intermediate():
        if row["state"] == "sealed":
            # crashed between sealing and deleting: converge to "no body, no row"
            store.discard(Path(row["path"]))
            db.delete_pool_blob(row["sha256"], row["size"], state="sealed")
        elif row["state"] == "writing" and _is_stale(row["created_at"], _RECOVERY_STALE_AFTER):
            # crashed publisher: converge to "no body, no row"
            store.discard(Path(row["path"]))
            db.delete_pool_blob(row["sha256"], row["size"], state="writing")
    # orphan blob files (e.g. a publisher crashed between linking and
    # committing); only stale files, never a body a live worker just linked
    for entry in store.pool_blobs_dir.glob("*.blob"):
        try:
            if entry.stat().st_mtime >= stale_ts:
                continue
        except OSError:
            continue
        if not db.pool_blob_path_exists(str(entry)):
            store.discard(entry)
    # stale staging and assembly temp files
    for directory in (store.pool_tmp_dir, store.artifacts_dir):
        for entry in directory.glob("*.tmp"):
            try:
                if entry.stat().st_mtime < stale_ts:
                    entry.unlink()
            except OSError:
                pass
    db.recompute_refcounts()
