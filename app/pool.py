"""全局内容寻址分片池。

磁盘布局（均在 ``DATA_DIR/pool`` 下）::

    ab/<64 位小写 sha256>.blob   正文，按摘要前两位分桶，同一内容全局唯一
    .staging/<uuid>.tmp          上传落盘的临时正文（校验通过后才入池）
    .trash/<sha>.<cycle>.trash   回收封存后的改名目标（带周期 token）

池记录（``pool_blobs``）状态机::

    publishing ——（finalize：正文就位）——> available
    available  ——（回收事务封存）——> sealed ——（删除提交后行消失）
    sealed/publishing ——（携带正文的后来者 reserve 接管）——> publishing ——> available

正确性只依赖 SQLite 写事务（WAL + BEGIN IMMEDIATE）与同卷原子改名，
不依赖任何进程内锁。
"""

from __future__ import annotations

import hashlib
import os
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import AsyncIterable

PUBLISHING = "publishing"
AVAILABLE = "available"
SEALED = "sealed"

# 游离文件（崩溃残留 .tmp）宽限时间，避免误删存活进程事务窗口内刚建立、
# 尚未提交可见的临时正文。
ORPHAN_GRACE_SECONDS = 60.0

_COPY_BUFFER = 1024 * 1024


class ChunkPool:
    def __init__(self, root: Path, worker_id: str | None = None):
        self.root = root
        self.staging_dir = root / ".staging"
        self.trash_dir = root / ".trash"
        self.root.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.trash_dir.mkdir(parents=True, exist_ok=True)
        self.worker_id = worker_id or uuid.uuid4().hex

    # ---- 路径 ----

    def blob_path(self, digest: str) -> Path:
        return self.root / digest[:2] / f"{digest}.blob"

    def trash_path(self, digest: str, token: str) -> Path:
        # token 是回收候选所属周期：即使同一摘要被重新 arm，旧封存副本与
        # 新正文也互不同名，删除旧副本绝不会波及新链接。
        return self.trash_dir / f"{digest}.{token}.trash"

    def staging_path(self) -> Path:
        return self.staging_dir / f"{uuid.uuid4().hex}.tmp"

    # ---- 上传临时正文 ----

    async def write_staging(self, stream: AsyncIterable[bytes]) -> tuple[Path, int, str]:
        tmp = self.staging_path()
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as fh:
                async for part in stream:
                    if not part:
                        continue
                    hasher.update(part)
                    fh.write(part)
                    size += len(part)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            self.discard(tmp)
            raise
        return tmp, size, hasher.hexdigest()

    # ---- 池记录/正文裁决 ----
    #
    # 发布分三个阶段，使任意崩溃点都只留下可凭 pool_blobs 行收敛的持久中间态：
    #   1) reserve：写事务内把行置为 publishing（新行或接管 sealed 行）并提交；
    #   2) materialize：事务外把已 fsync 的正文 hardlink 到内容路径；
    #   3) finalize：写事务内确认正文就位后置 available。
    # 因而行永远先于正文存在，不会出现"游离正文无记录"的泄漏；
    # 复用只能确认 available 行；回收只封存 available 行。

    def get_blob(self, conn, digest: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM pool_blobs WHERE sha256 = ?", (digest,)
        ).fetchone()
        return dict(row) if row else None

    def reserve(self, conn, digest: str, size: int, now: str) -> str:
        """阶段 1：确保该内容有一行 publishing/available（调用方持有写事务）。

        - 无记录：本请求成为发布者（publishing）；
        - available 且尺寸相符：复用；
        - sealed（回收已封存）：接管为 publishing，随后用自带正文重建；
        - publishing（另一发布者在途）：共同发布，随后各自幂等落正文。
        """
        row = self.get_blob(conn, digest)
        if row is None:
            conn.execute(
                "INSERT INTO pool_blobs (sha256, size, state, publisher_id, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (digest, size, PUBLISHING, self.worker_id, now),
            )
            return PUBLISHING
        if row["state"] == AVAILABLE:
            return AVAILABLE
        # sealed（回收已封存）或另一发布者崩溃遗留的 publishing：接管为
        # publishing 并刷新 publisher/created_at，使启动宽限随本发布者重置。
        conn.execute(
            "UPDATE pool_blobs SET state = ?, size = ?, publisher_id = ?, created_at = ?"
            " WHERE sha256 = ?",
            (PUBLISHING, size, self.worker_id, now, digest),
        )
        return PUBLISHING

    def finalize(self, conn, digest: str, size: int) -> str:
        """阶段 3：正文已 materialize，写事务内置 available；返回发布/复用。"""
        final = self.blob_path(digest)
        row = self.get_blob(conn, digest)
        if (
            row is not None
            and row["state"] == AVAILABLE
            and final.exists()
            and final.stat().st_size == size
        ):
            return "reused"
        if not final.exists() or final.stat().st_size != size:
            # 正文未就位（极少见的落盘失败）：保留 publishing 由启动恢复收敛。
            raise FileNotFoundError(f"pool body not materialized: {digest}")
        conn.execute(
            "UPDATE pool_blobs SET state = ?, size = ? WHERE sha256 = ?",
            (AVAILABLE, size, digest),
        )
        return "published"

    def materialize(self, digest: str, source: Path) -> None:
        """阶段 2：把已 fsync 的正文硬链接到内容路径（事务外，幂等）。"""
        self._materialize(self.blob_path(digest), source)

    def recover_intermediate(self, conn, now_ts: float, grace_seconds: float) -> list[str]:
        """收敛崩溃留下的 publishing 行。

        只处理创建时间已超过宽限的行（此时不可能还有存活发布者处在
        reserve→materialize→finalize 窗口，避免与另一 worker 的在途发布/
        紧随其后的回收竞争）：

        - 正文就位（仅核对尺寸，不重新散列）：置 available；
        - 正文缺失：删除行（后来的相同内容请求会重新发布）。

        宽限内的行一律不触碰；注意 finalize 与会话引用在同一事务，崩溃在
        finalize 前不会留下任何引用，所以延迟收敛不会悬空已确认会话。
        """
        made_available: list[str] = []
        rows = conn.execute("SELECT * FROM pool_blobs WHERE state = ?", (PUBLISHING,)).fetchall()
        for row in rows:
            created = _parse_iso_epoch(row["created_at"])
            if now_ts - created <= grace_seconds:
                continue
            final = self.blob_path(row["sha256"])
            if final.exists() and final.stat().st_size == row["size"]:
                conn.execute(
                    "UPDATE pool_blobs SET state = ? WHERE sha256 = ?",
                    (AVAILABLE, row["sha256"]),
                )
                made_available.append(row["sha256"])
            else:
                conn.execute("DELETE FROM pool_blobs WHERE sha256 = ?", (row["sha256"],))
        return made_available

    @staticmethod
    def _materialize(final: Path, source: Path) -> None:
        """把 source（已 fsync 的正文）以硬链接落到内容寻址路径。

        跨设备退化为复制并 fsync。在 reserve 提交之后、finalize 之前执行：
        若此处崩溃，pool_blobs 行停留在 publishing（无任何引用），由启动
        恢复在宽限后凭行收敛（正文就位则置可用，否则删行）。
        """
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            return
        try:
            os.link(source, final)
        except FileExistsError:
            return
        except OSError:
            with open(source, "rb") as src, open(final, "wb") as dst:
                shutil.copyfileobj(src, dst, _COPY_BUFFER)
                dst.flush()
                os.fsync(dst.fileno())
        _fsync_dir(final.parent)

    def add_reference(self, conn, rec: dict) -> bytes:
        """在同一写事务内重读当前位图、置位并插入分片引用。

        必须重读而非沿用请求开始时的位图快照：同一 target 会话的多个并发
        分片请求各自在独立事务提交，否则后提交者会覆盖前者的位（lost
        update）。返回提交后的最新位图。
        """
        from .bitmap import set_bit

        row = conn.execute(
            "SELECT bitmap FROM sessions WHERE session_id = ?",
            (rec["session_id"],),
        ).fetchone()
        bitmap = bytearray(row["bitmap"])
        set_bit(bitmap, rec["chunk_index"])
        conn.execute(
            "INSERT INTO chunks (session_id, chunk_index, size, sha256, path,"
            " received_at, pool_sha256)"
            " VALUES (:session_id, :chunk_index, :size, :sha256, :path,"
            " :received_at, :pool_sha256)",
            rec,
        )
        conn.execute(
            "UPDATE sessions SET bitmap = ? WHERE session_id = ?",
            (bytes(bitmap), rec["session_id"]),
        )
        return bytes(bitmap)

    def attach_chunk_to_pool(self, conn, session_id: str, index: int, digest: str) -> None:
        conn.execute(
            "UPDATE chunks SET pool_sha256 = ?, path = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (digest, str(self.blob_path(digest)), session_id, index),
        )

    def is_pinned(self, conn, digest: str) -> bool:
        """完成会话由成品独立承载，不再钉住池正文；其余会话都钉住。"""
        row = conn.execute(
            "SELECT 1 FROM chunks c JOIN sessions s ON s.session_id = c.session_id"
            " WHERE c.pool_sha256 = ? AND s.status <> 'completed' LIMIT 1",
            (digest,),
        ).fetchone()
        return row is not None

    # ---- 临时文件 / 改名 ----

    def seal_rename(self, digest: str, token: str) -> Path:
        """available -> sealed 提交后调用：blob 原子改名到 .trash。"""
        blob = self.blob_path(digest)
        trash = self.trash_path(digest, token)
        try:
            os.replace(blob, trash)
        except FileNotFoundError:
            pass
        _fsync_dir(blob.parent)
        _fsync_dir(trash.parent)
        return trash

    def restore_rename(self, digest: str, token: str) -> None:
        """回收删除前发现记录被后来者重新 arm 为 available：把正文放回。"""
        blob = self.blob_path(digest)
        trash = self.trash_path(digest, token)
        blob.parent.mkdir(parents=True, exist_ok=True)
        if blob.exists():
            # 后来者已重建正文，两份按内容寻址必然相同，删除封存副本即可。
            self.discard(trash)
            _fsync_dir(blob.parent)
            _fsync_dir(trash.parent)
            return
        try:
            os.replace(trash, blob)
        except FileNotFoundError:
            return
        _fsync_dir(blob.parent)
        _fsync_dir(trash.parent)

    def sweep_stale_staging(self) -> None:
        cutoff = time.time() - ORPHAN_GRACE_SECONDS
        for entry in self.staging_dir.iterdir():
            try:
                if entry.is_file() and entry.stat().st_mtime < cutoff:
                    entry.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def discard(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _parse_iso_epoch(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()
