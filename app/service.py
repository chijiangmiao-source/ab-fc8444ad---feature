"""Core upload/resume/finalize logic plus the content-addressed chunk pool."""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import AsyncIterable

from . import clock
from .bitmap import count_set, missing_indices, new_bitmap, set_bit
from .db import Database
from .errors import ApiError
from .pool import AVAILABLE, SEALED, ChunkPool
from .reclaim import Reclaimer
from .schemas import CreateSessionRequest
from .storage import ChunkStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VERIFY_BUFFER = 1024 * 1024


class UploadService:
    def __init__(self, db: Database, store: ChunkStore, pool: ChunkPool):
        self.db = db
        self.store = store
        self.pool = pool
        self.reclaimer = Reclaimer(db, pool)

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
        total = session["total_chunks"]
        index = self._valid_index(raw_index, total)
        digest = self._valid_digest(declared_digest)

        tmp, size, actual = await self.pool.write_staging(stream)
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
            with self.db.tx() as conn:
                existing = self.db.get_chunk(conn, session_id, index)
                if existing is not None:
                    if existing["sha256"] == digest:
                        return self._receipt_from_db(conn, session, existing, True), 200
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
                self._require_writable(session)

            # 三阶段发布：reserve（持久中间态）-> 落正文 -> finalize 与引用同事务。
            # 失败请求在 finalize 之前不会留下位图或引用污染。
            with self.db.tx() as conn:
                self.pool.reserve(conn, digest, size, clock.utcnow().isoformat())
            self.pool.materialize(digest, tmp)
            try:
                with self.db.tx() as conn:
                    # 提交前复查：预检与 reserve 之间会话可能恰好过期/完成。
                    fresh_session = self.db.get_session(session_id)
                    self._require_writable(fresh_session)
                    self.pool.finalize(conn, digest, size)
                    record = self._chunk_record(session_id, index, size, digest)
                    fresh_session["bitmap"] = self.pool.add_reference(conn, record)
                    session = fresh_session
            except sqlite3.IntegrityError:
                # 同序号并发：另一请求刚提交引用。按同内容幂等 / 异内容冲突裁决。
                with self.db.tx() as conn:
                    winner = self.db.get_chunk(conn, session_id, index)
                if winner is not None and winner["sha256"] == digest:
                    session["bitmap"] = self.db.get_session(session_id)["bitmap"]
                    return self._chunk_receipt(session, winner, duplicate=True), 200
                raise ApiError(
                    409,
                    "CHUNK_CONFLICT",
                    "chunk index already holds different content; the stored chunk is unchanged",
                    {
                        "chunk_index": index,
                        "stored_sha256": winner["sha256"] if winner else None,
                        "rejected_sha256": digest,
                    },
                )
            return self._chunk_receipt(session, record, duplicate=False), 201
        finally:
            self.pool.discard(tmp)

    def reuse_chunk(
        self, session_id: str, raw_index: str, declared_digest: str
    ) -> tuple[dict, int]:
        """无正文确认：仅当池中已有该 (sha256, 目标尺寸) 的可用正文。"""
        session = self.get_session_or_404(session_id)
        total = session["total_chunks"]
        index = self._valid_index(raw_index, total)
        digest = self._valid_digest(declared_digest)

        with self.db.tx() as conn:
            existing = self.db.get_chunk(conn, session_id, index)
            if existing is not None:
                # 幂等重放先于一切裁决：活动会话与过期未完成会话都可重放已确认片。
                if existing["sha256"] == digest:
                    return self._receipt_from_db(conn, session, existing, True), 200
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
            # 目标序号尚无确认片：过期/已完成会话不得借复用增加进度，位图不变。
            self._require_writable(session)
            expected = self.expected_chunk_size(session, index)
            blob = self.pool.get_blob(conn, digest)
            if blob is None:
                reason = "unknown"
            elif blob["state"] == SEALED:
                reason = "reclaiming"
            elif blob["state"] != AVAILABLE or blob["size"] != expected:
                reason = "size_mismatch" if blob["size"] != expected else "unavailable"
            else:
                reason = None
            if reason is not None:
                raise ApiError(
                    409,
                    "CHUNK_NOT_CACHED",
                    "no verified, available pool body with this digest can confirm this chunk;"
                    " the bitmap is unchanged",
                    {
                        "chunk_index": index,
                        "sha256": digest,
                        "expected_size": expected,
                        "reason": reason,
                    },
                )
            record = self._chunk_record(session_id, index, expected, digest)
            session["bitmap"] = self.pool.add_reference(conn, record)
            return self._chunk_receipt(session, record, duplicate=False), 201

    def _receipt_from_db(self, conn, session: dict, record: dict, duplicate: bool) -> dict:
        """幂等重放收据：位图取事务内最新值，避免用过时快照算 received_count。"""
        row = conn.execute(
            "SELECT bitmap FROM sessions WHERE session_id = ?", (session["session_id"],)
        ).fetchone()
        session["bitmap"] = row["bitmap"]
        return self._chunk_receipt(session, record, duplicate)

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

        paths, lost, unavailable = self._resolve_chunk_bodies(session)
        if lost:
            self._revoke_missing(session, lost)
        if lost or unavailable:
            details = {
                "missing_chunks": sorted(set(lost) | set(unavailable)),
                "total_chunks": total,
            }
            if self.is_expired(session):
                raise ApiError(
                    410,
                    "SESSION_EXPIRED",
                    "session expired with chunks unreadable",
                    {**details, "expires_at": session["expires_at"]},
                )
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "chunk bodies are missing or temporarily unavailable",
                details,
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

    # ---- maintenance ----

    def reclaim_pool(self, max_blobs: int) -> dict:
        return self.reclaimer.run(max_blobs)

    # ---- internals ----

    def _resolve_chunk_bodies(self, session: dict) -> tuple[list[Path], list[int], list[int]]:
        """解析每一片的正文路径，惰性把旧布局分片提升进全局池。

        返回 (按序正文路径, lost, unavailable)：
        - lost：引用/旧文件本身缺失或旧片损坏，撤销该片确认；
        - unavailable：引用指向的池正文暂不可见但池记录仍在（发布/回收
          恢复在途），保留确认，本次组装失败，重启/稍后收敛后可再组装。
        """
        sid = session["session_id"]
        total = session["total_chunks"]
        resolved: dict[int, Path] = {}
        pending_promote: list[tuple[int, dict, Path]] = []
        lost: list[int] = []
        unavailable: list[int] = []

        rows = {r["chunk_index"]: r for r in self.db.list_chunks(sid)}
        for i in range(total):
            row = rows.get(i)
            if row is None:
                lost.append(i)
                continue
            digest = row["pool_sha256"]
            if digest:
                path = self.pool.blob_path(digest)
                if path.exists() and path.stat().st_size == row["size"]:
                    resolved[i] = path
                else:
                    with self.db.tx() as conn:
                        blob_row = self.pool.get_blob(conn, digest)
                    if blob_row is None:
                        lost.append(i)  # 池记录已不存在：按既有缺文件规则撤销
                    else:
                        unavailable.append(i)
                continue
            legacy = Path(row["path"])
            if legacy.exists() and legacy.stat().st_size == row["size"]:
                pending_promote.append((i, row, legacy))
            else:
                lost.append(i)

        for i, row, legacy in pending_promote:
            ok, digest = _rehash(legacy, row["size"], row["sha256"])
            if not ok:
                lost.append(i)
                self._revoke_lost_row(sid, i)
                continue
            stale_legacy: Path | None = legacy
            promoted_by_us = False
            with self.db.tx() as conn:
                fresh = self.db.get_chunk(conn, sid, i)
                if fresh is not None and fresh["pool_sha256"]:
                    # 另一个 worker 已完成提升：只复用，旧文件归对方/启动收敛清理。
                    digest = fresh["pool_sha256"]
                    stale_legacy = None
                elif fresh is None:
                    lost.append(i)
                    continue
                else:
                    self.pool.reserve(conn, digest, row["size"], clock.utcnow().isoformat())
                    promoted_by_us = True
            if promoted_by_us:
                # 两 worker 同时提升同一内容：reserve 串行化后只有内容路径一份正文。
                self.pool.materialize(digest, stale_legacy)
                with self.db.tx() as conn:
                    self.pool.finalize(conn, digest, row["size"])
                    self.pool.attach_chunk_to_pool(conn, sid, i, digest)
            if stale_legacy is not None:
                # 引用已与位图一起提交、池 shard 目录已随提升 fsync，方可删旧正文。
                self.pool.discard(stale_legacy)
                _fsync_dir(stale_legacy.parent)
            path = self.pool.blob_path(digest)
            if path.exists() and path.stat().st_size == row["size"]:
                resolved[i] = path
            else:
                lost.append(i)

        ordered = [resolved[i] for i in range(total) if i in resolved]
        return ordered, lost, unavailable

    def _revoke_lost_row(self, session_id: str, index: int) -> None:
        """旧分片重验失败：按既有规则撤销该片确认并重建位图。"""
        with self.db.tx() as conn:
            session = self.db.get_session(session_id)
            if session is None:
                return
            self.db.delete_chunk(conn, session_id, index)
            self._rebuild_bitmap(conn, session)

    def _revoke_missing(self, session: dict, lost: list[int]) -> None:
        with self.db.tx() as conn:
            for index in lost:
                self.db.delete_chunk(conn, session["session_id"], index)
            self._rebuild_bitmap(conn, session)

    def _rebuild_bitmap(self, conn, session: dict) -> None:
        total = session["total_chunks"]
        bitmap = new_bitmap(total)
        rows = conn.execute(
            "SELECT chunk_index FROM chunks WHERE session_id = ?",
            (session["session_id"],),
        ).fetchall()
        for r in rows:
            set_bit(bitmap, r["chunk_index"])
        self.db.update_bitmap(conn, session["session_id"], bytes(bitmap))

    def _require_writable(self, session: dict) -> None:
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

    @staticmethod
    def _valid_index(raw_index: str, total: int) -> int:
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

    @staticmethod
    def _valid_digest(declared_digest: str) -> str:
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_CHUNK_DIGEST",
                "digest must be 64 lowercase hexadecimal characters",
                {"received": declared_digest},
            )
        return digest

    def _chunk_record(self, session_id: str, index: int, size: int, digest: str) -> dict:
        return {
            "session_id": session_id,
            "chunk_index": index,
            "size": size,
            "sha256": digest,
            "path": str(self.pool.blob_path(digest)),
            "received_at": clock.utcnow().isoformat(),
            "pool_sha256": digest,
        }

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


def _rehash(path: Path, expected_size: int, expected_digest: str) -> tuple[bool, str]:
    """重新核对旧分片的尺寸与摘要（提升前强制）。"""
    hasher = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            block = fh.read(_VERIFY_BUFFER)
            if not block:
                break
            hasher.update(block)
            size += len(block)
    digest = hasher.hexdigest()
    return size == expected_size and digest == expected_digest, digest


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


PUBLISHING_GRACE_SECONDS = 60.0
TRASH_GRACE_SECONDS = 60.0


def reconcile(db: Database, store: ChunkStore, pool: ChunkPool, reclaimer: Reclaimer) -> None:
    """重启收敛：只处理持久化中间态与本卷既有会话相关的分片，不扫描/重散列整个池。

    顺序：先收敛超过宽限的 publishing 池记录，再收敛回收封存/删除中间态
    （仅过期租约候选，绝不碰活跃回收者），最后逐会话核对 chunks 行。
    先收敛 publishing 可让“封存后接管者崩溃”的场景在一次重启内收敛。

    - 池引用缺正文：池记录仍在（任意状态）说明恢复在途，保留引用；
      池记录不存在才按既有“文件缺失”规则撤销确认；
    - 旧布局分片缺失/尺寸不符 -> 撤销该片确认；
    - 旧孤儿分片（无存活行）删除；已提升行残留的旧文件在池正文完好时删除；
    - 残留组装/上传临时文件与已有最终结论的 .trash 封存副本清理。

    两个 worker 同时启动恢复也不会互相删除正在发布的正文：宽限内的
    publishing 与活跃回收者窗口内的候选都不触碰，删除决策以数据库状态为准。
    """
    with db.tx() as conn:
        pool.recover_intermediate(
            conn, clock.utcnow().timestamp(), PUBLISHING_GRACE_SECONDS
        )
    reclaimer.recover()
    pool.sweep_stale_staging()

    for session in db.list_sessions():
        session_id = session["session_id"]
        # 完成会话由原子成品独立承载：历史分片即便已被回收，也不得撤销其
        # 确认、不得改动位图或退回未完成。只清理该目录下的临时残留。
        if session["status"] == "completed" and store.artifact_path(session_id).exists():
            chunk_dir = store.chunk_dir(session_id)
            if chunk_dir.exists():
                for entry in list(chunk_dir.iterdir()):
                    if entry.suffix == ".tmp":
                        entry.unlink(missing_ok=True)
            continue
        surviving = {r["chunk_index"]: r for r in db.list_chunks(session_id)}
        confirmed: set[int] = set()
        for index, row in list(surviving.items()):
            digest = row["pool_sha256"]
            if digest:
                body = pool.blob_path(digest)
                if body.exists() and body.stat().st_size == row["size"]:
                    confirmed.add(index)
                    continue
                # 正文暂不可见：池记录仍在说明发布/回收恢复在途，绝不撤销确认；
                # 只有池记录也不存在才按既有缺文件规则撤销。
                with db.tx() as conn:
                    blob_row = pool.get_blob(conn, digest)
                if blob_row is not None:
                    confirmed.add(index)
                else:
                    with db.tx() as conn:
                        db.delete_chunk(conn, session_id, index)
                    surviving.pop(index, None)
            else:
                legacy = Path(row["path"])
                if legacy.exists() and legacy.stat().st_size == row["size"]:
                    confirmed.add(index)
                else:
                    with db.tx() as conn:
                        db.delete_chunk(conn, session_id, index)
                    surviving.pop(index, None)

        chunk_dir = store.chunk_dir(session_id)
        if chunk_dir.exists():
            for entry in list(chunk_dir.iterdir()):
                if entry.suffix == ".tmp":
                    entry.unlink(missing_ok=True)
                    continue
                if entry.suffix != ".chunk":
                    continue
                try:
                    index = int(entry.stem)
                except ValueError:
                    entry.unlink(missing_ok=True)
                    continue
                row = surviving.get(index)
                if row is None or index not in confirmed:
                    entry.unlink(missing_ok=True)
                elif row["pool_sha256"]:
                    # 已提升且池正文完好：旧文件只是提升后的残留，安全删除；
                    # 池正文是独立硬链接，删旧文件不影响它。
                    body = pool.blob_path(row["pool_sha256"])
                    if body.exists() and body.stat().st_size == row["size"]:
                        entry.unlink(missing_ok=True)

        bitmap = new_bitmap(session["total_chunks"])
        for index in confirmed:
            set_bit(bitmap, index)
        with db.tx() as conn:
            db.update_bitmap(conn, session_id, bytes(bitmap))

    sweep_trash(db, pool)
    store.purge_tmp()


def sweep_trash(db: Database, pool: ChunkPool) -> None:
    """清理已有最终结论（deleted/restored）的封存副本；活跃候选绝不触碰。"""
    if not pool.trash_dir.exists():
        return
    cutoff = time.time() - TRASH_GRACE_SECONDS
    for entry in list(pool.trash_dir.iterdir()):
        if entry.suffix != ".trash":
            continue
        stem = entry.name[: -len(".trash")]
        parts = stem.rsplit(".", 1)
        if len(parts) != 2:
            if entry.stat().st_mtime < cutoff:
                entry.unlink(missing_ok=True)
            continue
        digest, token = parts
        with db.tx() as conn:
            cand = conn.execute(
                "SELECT rc.outcome AS outcome FROM reclaim_candidates rc"
                " WHERE rc.cycle_id = ? AND rc.sha256 = ?",
                (token, digest),
            ).fetchone()
        if cand is None:
            if entry.stat().st_mtime < cutoff:
                entry.unlink(missing_ok=True)
        elif cand["outcome"] in ("deleted", "restored"):
            entry.unlink(missing_ok=True)
        # outcome == sealed：回收仍在途（租约未过期），等 recover 收敛后再清。
