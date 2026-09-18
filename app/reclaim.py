"""分片池回收：持久游标分批扫描 + 封存/删除两阶段 + 并发栅栏。

每个回收周期（``reclaim_cycles``）持有一把持久租约（``owner_id`` /
``lease_until``），周期内游标与累计计数写入 ``reclaim_progress``，
被封存的候选写入 ``reclaim_candidates``。因此：

- 每次调用最多检查 ``max_blobs`` 个候选，重启后从游标继续，绝不整池扫描；
- 复用与回收的先后由同一个 BEGIN IMMEDIATE 写事务裁决
  （复用先拿引用 -> 候选 pinned 跳过；回收先封存 -> 复用只见 sealed，
  返回 ``CHUNK_NOT_CACHED``）；
- 崩溃在文件删除前后任意时刻发生，启动恢复都凭持久中间态收敛。
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

from . import clock
from .db import Database
from .pool import AVAILABLE, PUBLISHING, SEALED, ChunkPool

LEASE_SECONDS = 10.0
# 启动恢复只处理租约早于该宽限的中间态，绝不触碰存活回收者窗口内的候选。
STALE_LEASE_SECONDS = 10.0


class Reclaimer:
    def __init__(self, db: Database, pool: ChunkPool):
        self.db = db
        self.pool = pool
        self.owner_id = uuid.uuid4().hex

    # ---- 对外入口 ----

    def run(self, max_blobs: int) -> dict:
        with self.db.tx() as conn:
            cycle = conn.execute(
                "SELECT * FROM reclaim_cycles WHERE finished_at IS NULL"
                " ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            now = time.time()
            if cycle is None:
                cycle_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO reclaim_cycles (cycle_id, created_at, owner_id, lease_until)"
                    " VALUES (?, ?, ?, ?)",
                    (cycle_id, clock.utcnow().isoformat(), self.owner_id, now + LEASE_SECONDS),
                )
                conn.execute(
                    "INSERT INTO reclaim_progress (cycle_id, cursor_sha, examined, deleted)"
                    " VALUES (?, '', 0, 0)",
                    (cycle_id,),
                )
            elif (
                cycle["owner_id"] not in (None, self.owner_id)
                and cycle["lease_until"] is not None
                and cycle["lease_until"] > now
            ):
                # 另一个存活 worker 正在该周期内回收：返回其持久进度，不并发扫描。
                prog = conn.execute(
                    "SELECT * FROM reclaim_progress WHERE cycle_id = ?",
                    (cycle["cycle_id"],),
                ).fetchone()
                return {
                    "cycle_id": cycle["cycle_id"],
                    "cursor": prog["cursor_sha"],
                    "examined": prog["examined"],
                    "deleted": prog["deleted"],
                    "done": False,
                }
            else:
                # 租约为空（上一调用优雅释放）或已过期（崩溃）：接管同一周期。
                cycle_id = cycle["cycle_id"]
                conn.execute(
                    "UPDATE reclaim_cycles SET owner_id = ?, lease_until = ? WHERE cycle_id = ?",
                    (self.owner_id, now + LEASE_SECONDS, cycle_id),
                )

        budget = max_blobs
        try:
            while budget > 0:
                sealed_digest: str | None = None
                with self.db.tx() as conn:
                    self._renew(conn, cycle_id)
                    prog = conn.execute(
                        "SELECT * FROM reclaim_progress WHERE cycle_id = ?", (cycle_id,)
                    ).fetchone()
                    row = conn.execute(
                        "SELECT * FROM pool_blobs WHERE sha256 > ? ORDER BY sha256 LIMIT 1",
                        (prog["cursor_sha"],),
                    ).fetchone()
                    if row is None:
                        conn.execute(
                            "UPDATE reclaim_cycles SET finished_at = ? WHERE cycle_id = ?",
                            (clock.utcnow().isoformat(), cycle_id),
                        )
                        return self._progress(cycle_id, prog, done=True)
                    digest = row["sha256"]
                    examined = prog["examined"] + 1
                    if row["state"] == AVAILABLE and not self.pool.is_pinned(conn, digest):
                        conn.execute(
                            "UPDATE pool_blobs SET state = ? WHERE sha256 = ? AND state = ?",
                            (SEALED, digest, AVAILABLE),
                        )
                        conn.execute(
                            "INSERT INTO reclaim_candidates (cycle_id, sha256, ordinal, outcome)"
                            " VALUES (?, ?, ?, 'sealed')",
                            (cycle_id, digest, examined),
                        )
                        sealed_digest = digest
                    conn.execute(
                        "UPDATE reclaim_progress SET cursor_sha = ?, examined = ? WHERE cycle_id = ?",
                        (digest, examined, cycle_id),
                    )

                if sealed_digest is not None:
                    # 提交封存后才动文件：sealed 已对所有 worker 可见，复用自此被栅栏挡住。
                    self.pool.seal_rename(sealed_digest, cycle_id)
                    if self._finish_candidate(cycle_id, sealed_digest):
                        with self.db.tx() as conn:
                            self._renew(conn, cycle_id)
                            conn.execute(
                                "UPDATE reclaim_progress SET deleted = deleted + 1"
                                " WHERE cycle_id = ?",
                                (cycle_id,),
                            )
                budget -= 1

            with self.db.tx() as conn:
                prog = conn.execute(
                    "SELECT * FROM reclaim_progress WHERE cycle_id = ?", (cycle_id,)
                ).fetchone()
            return self._progress(cycle_id, prog, done=False)
        finally:
            # 每次调用结束时其候选都已有最终结论，释放租约；崩溃留下的
            # 租约则等待 STALE_LEASE_SECONDS 后由启动恢复收敛。
            with self.db.tx() as conn:
                conn.execute(
                    "UPDATE reclaim_cycles SET owner_id = NULL, lease_until = NULL"
                    " WHERE cycle_id = ? AND owner_id = ?",
                    (cycle_id, self.owner_id),
                )

    @staticmethod
    def _progress(cycle_id: str, prog, done: bool) -> dict:
        return {
            "cycle_id": cycle_id,
            "cursor": prog["cursor_sha"] if prog is not None else "",
            "examined": prog["examined"] if prog is not None else 0,
            "deleted": prog["deleted"] if prog is not None else 0,
            "done": done,
        }

    def _finish_candidate(self, cycle_id: str, digest: str) -> bool:
        """封存之后的第二阶段裁决与文件收尾，返回该候选正文是否已删除。

        顺序保证复用绝不会看到“available 但正文尚未归位”：
          1) 决策事务：只做删除（带状态守护）或不落任何状态变更；
          2) 文件收尾：删除封存副本，或把封存副本归位（记录仍 sealed，
             复用继续被栅栏挡住）；
          3) 终态事务：归位完成后才把 sealed 守护式地置 available，
             并写候选 outcome。
        任一步崩溃时候选都停留在 'sealed'，启动恢复据当时状态幂等重判。
        """
        with self.db.tx() as conn:
            self._renew(conn, cycle_id)
            row = self.pool.get_blob(conn, digest)
            pinned = row is not None and self.pool.is_pinned(conn, digest)
            if row is not None and (row["state"] == AVAILABLE or pinned):
                action = "restore"
                make_available = True
            elif row is not None and row["state"] == PUBLISHING:
                # 后来者已 reserve 接管：归还正文供其 finalize，状态保持 publishing。
                action = "restore"
                make_available = False
            elif row is None:
                action = "delete"
                make_available = False
            else:  # sealed 且无引用：守护式删除，被并发接管则不删。
                cur = conn.execute(
                    "DELETE FROM pool_blobs WHERE sha256 = ? AND state = 'sealed'",
                    (digest,),
                )
                action = "delete" if cur.rowcount == 1 else "restore"
                make_available = action == "restore"

        # 决策事务提交后做文件收尾；此前崩溃 -> 候选仍 'sealed'，恢复重判。
        if action == "restore":
            self.pool.restore_rename(digest, cycle_id)
        else:
            self.pool.discard(self.pool.trash_path(digest, cycle_id))
            _fsync_dir(self.pool.trash_dir)

        with self.db.tx() as conn:
            self._renew(conn, cycle_id)
            if make_available:
                # 仅当仍为 sealed 才置可用；若已被并发接管为 publishing 则不动。
                conn.execute(
                    "UPDATE pool_blobs SET state = 'available'"
                    " WHERE sha256 = ? AND state = 'sealed'",
                    (digest,),
                )
            conn.execute(
                "UPDATE reclaim_candidates SET outcome = ? WHERE cycle_id = ? AND sha256 = ?",
                ("restored" if action == "restore" else "deleted", cycle_id, digest),
            )
        return action == "delete"

    def _renew(self, conn, cycle_id: str) -> None:
        conn.execute(
            "UPDATE reclaim_cycles SET lease_until = ? WHERE cycle_id = ?",
            (time.time() + LEASE_SECONDS, cycle_id),
        )

    # ---- 启动恢复：只收敛持久中间态，不遍历/散列整个池 ----

    def recover(self) -> None:
        """启动收敛：只处理租约过期周期内仍为 'sealed' 的候选。

        与在线回收同一顺序（决策事务 -> 文件收尾 -> 终态事务），保证任何
        中途崩溃后候选仍是 'sealed'，可反复幂等重判：

        - 记录已删：仅清理可能残留的封存副本；
        - 记录 available / 已被引用：先归位正文，再守护式置 available；
        - 记录 publishing（后来者接管）：正文已就位才归位并置可用，否则跳过，
          绝不替存活接管者删除它即将使用的封存副本；
        - 记录仍 sealed 且无引用：守护式删除记录并清封存副本。
        """
        stale_before = time.time() - STALE_LEASE_SECONDS
        with self.db.tx() as conn:
            stale_cycles = [
                r["cycle_id"]
                for r in conn.execute(
                    "SELECT cycle_id FROM reclaim_cycles"
                    " WHERE lease_until IS NULL OR lease_until < ?",
                    (stale_before,),
                )
            ]
            if not stale_cycles:
                return
            placeholders = ",".join("?" * len(stale_cycles))
            pending = conn.execute(
                f"SELECT * FROM reclaim_candidates WHERE outcome = 'sealed'"
                f" AND cycle_id IN ({placeholders})",
                stale_cycles,
            ).fetchall()
            decisions: list[tuple[str, str, str, bool]] = []
            for cand in pending:
                digest, token = cand["sha256"], cand["cycle_id"]
                row = self.pool.get_blob(conn, digest)
                body_ready = self.pool.blob_path(digest).exists()
                if row is None:
                    decisions.append(("unlink_trash", digest, token, False))
                elif row["state"] == AVAILABLE or self.pool.is_pinned(conn, digest):
                    # 有明确保留理由（后来者已发布可用，或出现了引用）。
                    decisions.append(("restore", digest, token, True))
                elif row["state"] == PUBLISHING:
                    if body_ready:
                        decisions.append(("restore", digest, token, True))
                    # 正文未就位：可能是存活接管者的 reserve->materialize 窗口，本轮不动。
                else:  # sealed 且无引用：守护式删除，被并发接管则不删。
                    cur = conn.execute(
                        "DELETE FROM pool_blobs WHERE sha256 = ? AND state = 'sealed'",
                        (digest,),
                    )
                    if cur.rowcount == 1:
                        decisions.append(("finish_delete", digest, token, False))
                    else:
                        decisions.append(("restore", digest, token, False))

        # 文件收尾先于任何 available 终态，复用不会看到“可用但无正文”。
        # 删除只动本周期拥有的 token-trash；在线封存已把正文从内容路径移走，
        # 因此绝不能 rename/删除内容路径，否则可能偷走后来者新建的同内容正文。
        finalize_available: list[str] = []
        for action, digest, token, make_available in decisions:
            trash = self.pool.trash_path(digest, token)
            content = self.pool.blob_path(digest)
            if action == "restore":
                self.pool.restore_rename(digest, token)
            else:
                if trash.exists():
                    self.pool.discard(trash)
                    _fsync_dir(self.pool.trash_dir)
                elif content.exists():
                    # 崩溃发生在封存提交之后、内容->trash 改名之前。删除前再读：
                    # 若已有后来者重新建立池记录，正文归它所有，不得删除。
                    with self.db.tx() as rconn:
                        reborn = self.pool.get_blob(rconn, digest)
                    if reborn is None:
                        self.pool.discard(content)
                        _fsync_dir(content.parent)
                    # reborn is not None: 保留正文，候选结论仍记 deleted（其封存
                    # 动作已被后来者取代），不影响正确性。
            if make_available:
                finalize_available.append(digest)

        with self.db.tx() as conn:
            for action, digest, token, make_available in decisions:
                if action == "restore" and digest in finalize_available:
                    conn.execute(
                        "UPDATE pool_blobs SET state = 'available'"
                        " WHERE sha256 = ? AND state IN ('sealed', 'publishing')",
                        (digest,),
                    )
                outcome = "restored" if action == "restore" else "deleted"
                conn.execute(
                    "UPDATE reclaim_candidates SET outcome = ? WHERE cycle_id = ? AND sha256 = ?",
                    (outcome, token, digest),
                )


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
