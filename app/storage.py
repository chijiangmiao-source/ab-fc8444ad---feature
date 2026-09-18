"""On-disk layout for chunk bodies, the content-addressed pool and artifacts.

Every write goes to a temp file, is fsynced, and is then linked or moved into
place atomically, so a crash never leaves a half-written file at a final path.

Layout under DATA_DIR:

    chunks/<session_id>/<index>.chunk       legacy per-session bodies (pre-pool volumes)
    pool/tmp/.<uuid>.tmp                    staging area for bodies being uploaded
    pool/blobs/<sha>-<size>-<id>.blob       one file per published body, shared pool-wide
    artifacts/<session_id>.bin              published artifacts (atomic rename)

Pool blob file names carry a unique suffix so a re-published body never shares
a path with a previously sealed/deleted incarnation of the same content; that
is what makes reclaim-vs-republish races safe without any file locking.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import AsyncIterable, Iterable

_COPY_BUFFER = 1024 * 1024


class ChunkStore:
    def __init__(self, root: Path):
        self.root = root
        self.chunks_root = root / "chunks"
        self.artifacts_dir = root / "artifacts"
        self.pool_dir = root / "pool"
        self.pool_blobs_dir = self.pool_dir / "blobs"
        self.pool_tmp_dir = self.pool_dir / "tmp"
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.pool_blobs_dir.mkdir(parents=True, exist_ok=True)
        self.pool_tmp_dir.mkdir(parents=True, exist_ok=True)

    # ---- legacy per-session layout (kept for pre-pool volumes) ----

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    # ---- artifacts ----

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    # ---- staging ----

    async def write_tmp(self, stream: AsyncIterable[bytes]) -> tuple[Path, int, str]:
        """Stream a request body to a pool temp file; returns (tmp, size, sha256).

        The caller validates size/digest before publishing the temp file into
        the pool; nothing is visible at a final path until then.
        """
        tmp = self.pool_tmp_dir / f".{uuid.uuid4().hex}.tmp"
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
            tmp.unlink(missing_ok=True)
            raise
        return tmp, size, hasher.hexdigest()

    # ---- pool ----

    def pool_blob_path(self, sha256: str, size: int) -> Path:
        """Fresh, unique final path for a to-be-published body."""
        return self.pool_blobs_dir / f"{sha256}-{size}-{uuid.uuid4().hex[:16]}.blob"

    def link_into_pool(self, source: Path, dest: Path) -> None:
        """Atomically publish `source` at `dest` (hard link + dir fsync).

        The source is kept; the caller unlinks it once the matching database
        reference has committed.
        """
        os.link(source, dest)
        fsync_dir(dest.parent)

    # ---- assembly / publish ----

    def assemble_to_tmp(self, paths: Iterable[Path]) -> tuple[Path, int, str]:
        """Concatenate chunk bodies in order; returns (tmp_path, size, sha256)."""
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
        fsync_dir(final.parent)
        return final

    @staticmethod
    def discard(path: Path) -> None:
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            pass


def hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(_COPY_BUFFER), b""):
            hasher.update(block)
    return hasher.hexdigest()


def fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
