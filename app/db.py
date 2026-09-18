"""SQLite persistence: sessions, per-chunk records, the global chunk pool and
the reclaim cursor.

Correctness never relies on process-local locks: every multi-statement write
runs inside a BEGIN IMMEDIATE transaction and every pool state transition is a
compare-and-set guarded by SQLite itself, so several uvicorn workers sharing
the same database file and data volume stay consistent. The RLock only
serializes this process' own connection object.

Pool blob states:
    writing    publisher reserved the (sha256, size) key, body not visible yet
    available  body verified and fsynced; reusable and reclaimable
    sealed     reclaim fenced the blob for deletion; no new references allowed
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id    TEXT PRIMARY KEY,
    file_size     INTEGER NOT NULL,
    chunk_size    INTEGER NOT NULL,
    total_chunks  INTEGER NOT NULL,
    file_sha256   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    bitmap        BLOB NOT NULL,
    expires_at    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    completed_at  TEXT,
    final_sha256  TEXT,
    artifact_path TEXT
);
CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    path        TEXT NOT NULL,
    received_at TEXT NOT NULL,
    blob_sha256 TEXT,
    blob_size   INTEGER,
    PRIMARY KEY (session_id, chunk_index),
    FOREIGN KEY (session_id) REFERENCES sessions (session_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS pool_blobs (
    sha256     TEXT NOT NULL,
    size       INTEGER NOT NULL,
    path       TEXT NOT NULL,
    state      TEXT NOT NULL DEFAULT 'writing',
    ref_count  INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    sealed_at  TEXT,
    PRIMARY KEY (sha256, size)
);
CREATE TABLE IF NOT EXISTS reclaim_state (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    cursor    INTEGER NOT NULL DEFAULT 0,
    cycle_seq INTEGER NOT NULL DEFAULT 0
);
"""

# chunks columns added after the initial release (pre-pool volumes lack them)
_CHUNK_COLUMNS = (
    ("blob_sha256", "ALTER TABLE chunks ADD COLUMN blob_sha256 TEXT"),
    ("blob_size", "ALTER TABLE chunks ADD COLUMN blob_size INTEGER"),
)


class Database:
    """Single-connection store; every write commits immediately."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=30000")
        self._migrate()

    def _migrate(self) -> None:
        with self.lock:
            self._conn.executescript(SCHEMA)
            for name, ddl in _CHUNK_COLUMNS:
                cols = {row[1] for row in self._conn.execute("PRAGMA table_info(chunks)")}
                if name in cols:
                    continue
                try:
                    self._conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass  # another worker applied the migration first
            cols = {row[1] for row in self._conn.execute("PRAGMA table_info(chunks)")}
            if any(name not in cols for name, _ in _CHUNK_COLUMNS):
                raise RuntimeError("chunks table migration failed")
            self._conn.execute(
                "INSERT OR IGNORE INTO reclaim_state (id, cursor, cycle_seq) VALUES (1, 0, 0)"
            )

    @contextmanager
    def write_tx(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE transaction, serialized against every other writer
        in every worker sharing this database file."""
        with self.lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict | None:
        return dict(row) if row is not None else None

    # ---- sessions ----

    def create_session(self, rec: dict) -> None:
        with self.write_tx() as conn:
            conn.execute(
                "INSERT INTO sessions (session_id, file_size, chunk_size, total_chunks,"
                " file_sha256, status, bitmap, expires_at, created_at)"
                " VALUES (:session_id, :file_size, :chunk_size, :total_chunks,"
                " :file_sha256, :status, :bitmap, :expires_at, :created_at)",
                rec,
            )

    def get_session(self, session_id: str) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        return self._dict(row)

    def list_sessions(self) -> list[dict]:
        with self.lock:
            rows = self._conn.execute("SELECT * FROM sessions ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    # ---- chunks (autocommit reads) ----

    def get_chunk(self, session_id: str, index: int) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
                (session_id, index),
            ).fetchone()
        return self._dict(row)

    def list_chunks(self, session_id: str) -> list[dict]:
        with self.lock:
            rows = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
                (session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- in-transaction helpers (require an open write_tx) ----

    def q_get_session(self, conn: sqlite3.Connection, session_id: str) -> dict | None:
        row = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
        return self._dict(row)

    def q_get_chunk(self, conn: sqlite3.Connection, session_id: str, index: int) -> dict | None:
        row = conn.execute(
            "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
            (session_id, index),
        ).fetchone()
        return self._dict(row)

    def q_insert_chunk(self, conn: sqlite3.Connection, rec: dict) -> None:
        conn.execute(
            "INSERT INTO chunks (session_id, chunk_index, size, sha256, path, received_at,"
            " blob_sha256, blob_size)"
            " VALUES (:session_id, :chunk_index, :size, :sha256, :path, :received_at,"
            " :blob_sha256, :blob_size)",
            rec,
        )

    def q_update_bitmap(self, conn: sqlite3.Connection, session_id: str, bitmap: bytes) -> None:
        conn.execute("UPDATE sessions SET bitmap = ? WHERE session_id = ?", (bitmap, session_id))

    def q_delete_chunk(self, conn: sqlite3.Connection, session_id: str, index: int) -> None:
        conn.execute(
            "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?", (session_id, index)
        )

    def q_flip_chunk_to_blob(
        self, conn: sqlite3.Connection, session_id: str, index: int, sha256: str, size: int, path: str
    ) -> bool:
        """Point a legacy (blob-less) chunk row at a pool blob. The NULL guard
        makes concurrent promotions idempotent so the refcount moves once."""
        cur = conn.execute(
            "UPDATE chunks SET blob_sha256 = ?, blob_size = ?, path = ?"
            " WHERE session_id = ? AND chunk_index = ? AND blob_sha256 IS NULL",
            (sha256, size, path, session_id, index),
        )
        return cur.rowcount == 1

    def q_get_pool_blob(self, conn: sqlite3.Connection, sha256: str, size: int) -> dict | None:
        row = conn.execute(
            "SELECT * FROM pool_blobs WHERE sha256 = ? AND size = ?", (sha256, size)
        ).fetchone()
        return self._dict(row)

    def q_incr_refcount(self, conn: sqlite3.Connection, sha256: str, size: int, delta: int) -> None:
        conn.execute(
            "UPDATE pool_blobs SET ref_count = MAX(0, ref_count + ?) WHERE sha256 = ? AND size = ?",
            (delta, sha256, size),
        )

    # ---- pool blobs (single-statement compare-and-set operations) ----

    def get_pool_blob(self, sha256: str, size: int) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM pool_blobs WHERE sha256 = ? AND size = ?", (sha256, size)
            ).fetchone()
        return self._dict(row)

    def insert_pool_blob_writing(self, sha256: str, size: int, path: str, created_at: str) -> bool:
        """Reserve the content key; False means another publisher won."""
        with self.lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO pool_blobs (sha256, size, path, state, ref_count, created_at)"
                " VALUES (?, ?, ?, 'writing', 0, ?)",
                (sha256, size, path, created_at),
            )
            return cur.rowcount == 1

    def mark_pool_blob_available(self, sha256: str, size: int, path: str) -> bool:
        """Flip our own reserved blob to available; False if it was taken over."""
        with self.lock:
            cur = self._conn.execute(
                "UPDATE pool_blobs SET state = 'available'"
                " WHERE sha256 = ? AND size = ? AND state = 'writing' AND path = ?",
                (sha256, size, path),
            )
            return cur.rowcount == 1

    def delete_pool_blob(
        self, sha256: str, size: int, *, state: str, created_at: str | None = None
    ) -> bool:
        """Compare-and-set delete; only removes the row if it is still in the
        expected state (and, optionally, still the same publication)."""
        sql = "DELETE FROM pool_blobs WHERE sha256 = ? AND size = ? AND state = ?"
        args: list = [sha256, size, state]
        if created_at is not None:
            sql += " AND created_at = ?"
            args.append(created_at)
        with self.lock:
            cur = self._conn.execute(sql, args)
            return cur.rowcount == 1

    def seal_pool_blob(self, sha256: str, size: int, sealed_at: str) -> bool:
        """Fence an unpinned blob for deletion; False if it is pinned or a
        racing reclaimer/reuser got there first."""
        with self.lock:
            cur = self._conn.execute(
                "UPDATE pool_blobs SET state = 'sealed', sealed_at = ?"
                " WHERE sha256 = ? AND size = ? AND state = 'available' AND ref_count = 0",
                (sealed_at, sha256, size),
            )
            return cur.rowcount == 1

    def list_pool_intermediate(self) -> list[dict]:
        """Blobs in a persisted intermediate state (never the whole pool)."""
        with self.lock:
            rows = self._conn.execute(
                "SELECT * FROM pool_blobs WHERE state IN ('writing', 'sealed')"
            ).fetchall()
        return [dict(r) for r in rows]

    def list_pool_candidates(self, after_rowid: int, limit: int) -> list[dict]:
        with self.lock:
            rows = self._conn.execute(
                "SELECT rowid AS blob_rowid, * FROM pool_blobs"
                " WHERE rowid > ? ORDER BY rowid LIMIT ?",
                (after_rowid, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def pool_blob_path_exists(self, path: str) -> bool:
        with self.lock:
            row = self._conn.execute(
                "SELECT 1 FROM pool_blobs WHERE path = ? LIMIT 1", (path,)
            ).fetchone()
        return row is not None

    def recompute_refcounts(self) -> None:
        """Rebuild ref_count from chunk rows of non-completed sessions (pins).

        Database-only maintenance: no file contents are read or re-hashed.
        """
        with self.write_tx() as conn:
            conn.execute(
                "UPDATE pool_blobs SET ref_count = ("
                " SELECT COUNT(*) FROM chunks c JOIN sessions s ON c.session_id = s.session_id"
                " WHERE c.blob_sha256 = pool_blobs.sha256 AND c.blob_size = pool_blobs.size"
                " AND s.status != 'completed')"
            )

    # ---- reclaim cycles ----

    def begin_reclaim_cycle(self) -> tuple[int, int]:
        """Persist a new cycle id and read the durable cursor."""
        with self.write_tx() as conn:
            conn.execute("UPDATE reclaim_state SET cycle_seq = cycle_seq + 1 WHERE id = 1")
            row = conn.execute("SELECT cycle_seq, cursor FROM reclaim_state WHERE id = 1").fetchone()
            return row["cycle_seq"], row["cursor"]

    def finish_reclaim_cycle(self, cursor: int) -> None:
        with self.write_tx() as conn:
            conn.execute("UPDATE reclaim_state SET cursor = ? WHERE id = 1", (cursor,))

    # ---- completion / reconcile ----

    def mark_completed(self, session_id: str, completed_at: str, final_sha256: str, artifact_path: str) -> None:
        """Mark the session completed and release its pool pins in one
        transaction: the artifact alone carries the content from now on."""
        with self.write_tx() as conn:
            rows = conn.execute(
                "SELECT blob_sha256 AS bsha, blob_size AS bsize, COUNT(*) AS n FROM chunks"
                " WHERE session_id = ? AND blob_sha256 IS NOT NULL"
                " GROUP BY blob_sha256, blob_size",
                (session_id,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE pool_blobs SET ref_count = MAX(0, ref_count - ?)"
                    " WHERE sha256 = ? AND size = ?",
                    (row["n"], row["bsha"], row["bsize"]),
                )
            conn.execute(
                "UPDATE sessions SET status = 'completed', completed_at = ?,"
                " final_sha256 = ?, artifact_path = ? WHERE session_id = ?",
                (completed_at, final_sha256, artifact_path, session_id),
            )

    def reconcile_session_chunks(self, session_id: str, dropped: list[dict], bitmap: bytes) -> None:
        """Drop chunk rows whose bodies are gone and persist the rebuilt
        bitmap, releasing any pool pins, all in one transaction."""
        with self.write_tx() as conn:
            for row in dropped:
                conn.execute(
                    "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                    (session_id, row["chunk_index"]),
                )
                if row["blob_sha256"] is not None:
                    conn.execute(
                        "UPDATE pool_blobs SET ref_count = MAX(0, ref_count - 1)"
                        " WHERE sha256 = ? AND size = ?",
                        (row["blob_sha256"], row["blob_size"]),
                    )
            conn.execute("UPDATE sessions SET bitmap = ? WHERE session_id = ?", (bitmap, session_id))

    def close(self) -> None:
        with self.lock:
            self._conn.close()
