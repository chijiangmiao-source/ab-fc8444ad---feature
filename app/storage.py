"""磁盘布局：旧版按会话分片路径（惰性提升用）与成品文件。

新上传的正文由 :mod:`app.pool` 内容寻址保存；本模块只保留：

- 兼容旧卷的 ``chunks/<session_id>/<index>.chunk`` 路径解析；
- 成品的顺序组装、fsync 与原子发布。
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import Iterable

_COPY_BUFFER = 1024 * 1024


class ChunkStore:
    def __init__(self, root: Path):
        self.root = root
        self.chunks_root = root / "chunks"
        self.artifacts_dir = root / "artifacts"
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    def assemble_to_tmp(self, paths: Iterable[Path]) -> tuple[Path, int, str]:
        """按序拼接正文；直接顺序读取池正文/旧分片，不复制整套分片。"""
        tmp = self.artifacts_dir / f".{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as out:
                for path in paths:
                    with open(path, "rb") as src:
                        while True:
                            block = src.read(_COPY_BUFFER)
                            if not block:
                                break
                            hasher.update(block)
                            out.write(block)
                            size += len(block)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return tmp, size, hasher.hexdigest()

    def publish(self, tmp: Path, session_id: str) -> Path:
        final = self.artifact_path(session_id)
        os.replace(tmp, final)
        _fsync_dir(final.parent)
        return final

    @staticmethod
    def discard(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    def purge_tmp(self) -> None:
        for entry in self.artifacts_dir.glob("*.tmp"):
            entry.unlink(missing_ok=True)
        # 旧布局 chunks 目录下的残留临时文件。
        for entry in self.chunks_root.glob("*/*.tmp"):
            entry.unlink(missing_ok=True)


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
