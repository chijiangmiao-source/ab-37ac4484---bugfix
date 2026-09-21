"""SQLite persistence: sessions (with received bitmap) and per-chunk digests."""

from __future__ import annotations

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
"""


class Database:
    """Single-connection store guarded by an RLock; every write commits immediately."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def create_session(self, rec: dict) -> None:
        with self.lock, self._conn:
            self._conn.execute(
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
            rows = self._conn.execute("SELECT * FROM sessions ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def get_chunk(self, session_id: str, index: int) -> dict | None:
        with self.lock:
            row = self._conn.execute(
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

    def reserve_chunk_with_bitmap(self, rec: dict, bitmap: bytes) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT INTO chunks (session_id, chunk_index, size, sha256, path, received_at)"
                " VALUES (:session_id, :chunk_index, :size, :sha256, :path, :received_at)",
                rec,
            )
            self._conn.execute(
                "UPDATE sessions SET bitmap = ? WHERE session_id = ?",
                (bitmap, rec["session_id"]),
            )

    def cancel_chunk_reservation(self, session_id: str, index: int, digest: str) -> None:
        with self.lock, self._conn:
            row = self._conn.execute(
                "SELECT sha256 FROM chunks WHERE session_id = ? AND chunk_index = ?",
                (session_id, index),
            ).fetchone()
            if row is None or row["sha256"] != digest:
                return
            self._conn.execute(
                "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                (session_id, index),
            )
            session = self._conn.execute(
                "SELECT bitmap FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if session is not None:
                bitmap = bytearray(session["bitmap"])
                bitmap[index >> 3] &= ~(1 << (index & 7))
                self._conn.execute(
                    "UPDATE sessions SET bitmap = ? WHERE session_id = ?",
                    (bytes(bitmap), session_id),
                )

    def delete_chunk(self, session_id: str, index: int) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                (session_id, index),
            )

    def update_bitmap(self, session_id: str, bitmap: bytes) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE sessions SET bitmap = ? WHERE session_id = ?", (bitmap, session_id)
            )

    def mark_completed(self, session_id: str, completed_at: str, final_sha256: str, artifact_path: str) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE sessions SET status = 'completed', completed_at = ?,"
                " final_sha256 = ?, artifact_path = ? WHERE session_id = ?",
                (completed_at, final_sha256, artifact_path, session_id),
            )

    def close(self) -> None:
        with self.lock:
            self._conn.close()
