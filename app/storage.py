"""On-disk layout for chunk bodies and published artifacts.

Every write goes to a temp file, is fsynced, and is then moved into place with
os.replace so a crash never leaves a half-written file at a final path.
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
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    def tmp_path(self, session_id: str) -> Path:
        """Reserve a unique (not yet opened) temp path for a streaming body."""
        target_dir = self.chunk_dir(session_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / f".{uuid.uuid4().hex}.tmp"

    async def write_chunk_tmp(self, tmp: Path, stream: AsyncIterable[bytes]) -> tuple[int, str]:
        """Stream a request body into ``tmp``; returns (size, sha256).

        The caller validates size/digest before moving the file into place with
        commit_tmp(); nothing is visible at the final path until then. The temp
        file is removed if streaming fails (e.g. the client disconnects).
        """
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
        return size, hasher.hexdigest()

    def commit_tmp(self, tmp: Path, final: Path) -> None:
        os.replace(tmp, final)
        _fsync_dir(final.parent)

    def assemble_to_tmp(self, paths: Iterable[Path]) -> tuple[Path, int, str]:
        """Concatenate chunk files in order; returns (tmp_path, size, sha256)."""
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


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
