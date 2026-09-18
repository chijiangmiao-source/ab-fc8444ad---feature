"""SQLite persistence.

除了原有的 sessions / chunks 表外，新版本增加：

- ``pool_blobs``      —— 全局内容寻址分片池，状态机 publishing/available/sealed
- ``reclaim_cycles``  —— 回收周期（cycle_id 持久化）
- ``reclaim_progress`` —— 每周期的 keyset 游标与累计计数（重启续扫）
- ``reclaim_candidates`` —— 候选/删除的持久中间态（崩溃收敛依据）

所有跨进程并发裁决都依赖 SQLite 的写事务串行化（WAL +
``BEGIN IMMEDIATE`` + ``busy_timeout``），进程内 RLock 只用于保护单连接，
不作为正确性手段。
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from pathlib import Path

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
    PRIMARY KEY (session_id, chunk_index),
    FOREIGN KEY (session_id) REFERENCES sessions (session_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS pool_blobs (
    sha256       TEXT PRIMARY KEY,
    size         INTEGER NOT NULL,
    state        TEXT NOT NULL,
    publisher_id TEXT,
    created_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reclaim_cycles (
    cycle_id    TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    finished_at TEXT,
    owner_id    TEXT,
    lease_until REAL
);
CREATE TABLE IF NOT EXISTS reclaim_progress (
    cycle_id  TEXT PRIMARY KEY,
    cursor_sha TEXT NOT NULL DEFAULT '',
    examined  INTEGER NOT NULL DEFAULT 0,
    deleted   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS reclaim_candidates (
    cycle_id   TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    ordinal    INTEGER NOT NULL,
    outcome    TEXT NOT NULL,
    PRIMARY KEY (cycle_id, sha256)
);
"""


class Database:
    """每进程一个 SQLite 连接；写事务一律 BEGIN IMMEDIATE。"""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(path), check_same_thread=False, isolation_level=None, timeout=30.0
        )
        self._conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chunks_pool ON chunks(pool_sha256)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chunks_sha ON chunks(sha256)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pool_state ON pool_blobs(state)"
            )

    def _migrate(self) -> None:
        """把旧版本卷上的库结构升级到当前版本（仅加列/建表，不搬数据）。"""
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(chunks)")}
        if "pool_sha256" not in cols:
            self._conn.execute("ALTER TABLE chunks ADD COLUMN pool_sha256 TEXT")

    @contextlib.contextmanager
    def tx(self):
        """串行化写事务：BEGIN IMMEDIATE 立刻取保留写锁。

        多个 worker 进程在同一 SQLite 文件上由此获得全序，配合
        busy_timeout 等待而非立刻 BUSY。
        """
        with self.lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ---- sessions ----

    def create_session(self, rec: dict) -> None:
        with self.tx() as conn:
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
        return dict(row) if row else None

    def list_sessions(self) -> list[dict]:
        with self.lock:
            rows = self._conn.execute(
                "SELECT * FROM sessions ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- chunks ----

    def get_chunk(self, conn, session_id: str, index: int) -> dict | None:
        row = conn.execute(
            "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
            (session_id, index),
        ).fetchone()
        return dict(row) if row else None

    def list_chunks(self, session_id: str) -> list[dict]:
        with self.lock:
            rows = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
                (session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_chunk(self, conn, session_id: str, index: int) -> None:
        conn.execute(
            "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
            (session_id, index),
        )

    def update_bitmap(self, conn, session_id: str, bitmap: bytes) -> None:
        conn.execute(
            "UPDATE sessions SET bitmap = ? WHERE session_id = ?", (bitmap, session_id)
        )

    def mark_completed(
        self, session_id: str, completed_at: str, final_sha256: str, artifact_path: str
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE sessions SET status = 'completed', completed_at = ?,"
                " final_sha256 = ?, artifact_path = ? WHERE session_id = ?",
                (completed_at, final_sha256, artifact_path, session_id),
            )

    def close(self) -> None:
        with self.lock:
            self._conn.close()
