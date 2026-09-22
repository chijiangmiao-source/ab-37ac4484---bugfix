"""SQLite persistence: sessions (with received bitmap) and per-chunk digests.

A chunk row starts life in state 'pending' while its body is still streaming:
it is NOT counted in the session bitmap until state becomes 'confirmed', which
happens in the same transaction as the bitmap update -- and only after the body
has been length/digest checked and moved to its final path with os.replace().
'pending' rows therefore never influence progress and are dropped on restart.
"""

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
    state       TEXT NOT NULL DEFAULT 'pending',
    tmp_path    TEXT,
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
            self._migrate_chunks_table()
            self._conn.commit()

    def _migrate_chunks_table(self) -> None:
        """Add the state/tmp_path columns to databases created by older versions.

        Every row written by an older version that still has its file is a
        confirmed chunk; reconcile() rebuilds the bitmap from those rows.
        """
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(chunks)")
        }
        if "state" not in columns:
            self._conn.execute(
                "ALTER TABLE chunks ADD COLUMN state TEXT NOT NULL DEFAULT 'confirmed'"
            )
        if "tmp_path" not in columns:
            self._conn.execute("ALTER TABLE chunks ADD COLUMN tmp_path TEXT")

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

    def get_confirmed_chunk(self, session_id: str, index: int) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?"
                " AND state = 'confirmed'",
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

    def insert_pending_chunk(self, rec: dict, tmp_path: str) -> None:
        """Record an in-flight upload. The bitmap is deliberately left untouched."""
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT INTO chunks (session_id, chunk_index, size, sha256, path,"
                " received_at, state, tmp_path)"
                " VALUES (:session_id, :chunk_index, :size, :sha256, :path,"
                " :received_at, 'pending', :tmp_path)",
                {**rec, "tmp_path": tmp_path},
            )

    def commit_pending_chunk(
        self, session_id: str, index: int, final_path: str, bitmap: bytes
    ) -> None:
        """Promote a pending row to confirmed and set its bitmap bit atomically."""
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE chunks SET state = 'confirmed', path = ?, tmp_path = NULL"
                " WHERE session_id = ? AND chunk_index = ? AND state = 'pending'",
                (final_path, session_id, index),
            )
            if cur.rowcount == 0:
                raise sqlite3.IntegrityError(
                    f"no pending chunk {session_id}:{index} to commit"
                )
            self._conn.execute(
                "UPDATE sessions SET bitmap = ? WHERE session_id = ?",
                (bitmap, session_id),
            )

    def abort_pending_chunk(self, session_id: str, index: int) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?"
                " AND state = 'pending'",
                (session_id, index),
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
