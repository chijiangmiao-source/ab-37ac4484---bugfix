"""Core upload/resume/finalize logic shared by the HTTP routes.

Concurrency model for PUT chunk/{index}
---------------------------------------

A chunk only becomes visible to GET status / finalize *after* its full body has
been streamed, length- and SHA-256-validated, moved to its final path with
os.replace() *and* recorded as 'confirmed' together with its bitmap bit in one
SQLite transaction. While the body is still streaming, at most a 'pending' row
(which never touches the bitmap) exists.

Concurrent uploads of the same (session, index) coordinate through an in-memory
gate:

* the first request ("leader") streams and commits;
* later requests ("followers") drain their own bodies into private temp files
  (so a paused leader cannot cause TCP back-pressure/deadlock) and wait for the
  leader;
* if the leader commits a chunk whose digest matches the follower's header, the
  follower gets the idempotent ``200 duplicate=true``; a different digest is a
  ``409 CHUNK_CONFLICT``;
* if the leader fails, followers serialise on the gate and the first one with a
  complete, valid body promotes it (``201``), so a legitimate retry after a
  failed attempt still finishes the upload.

No 2xx is ever returned for a chunk that cannot also be observed as confirmed
through the status endpoint, finalize, and a process restart.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import AsyncIterable

from . import clock
from .bitmap import count_set, missing_indices, new_bitmap, set_bit
from .db import Database
from .errors import ApiError
from .schemas import CreateSessionRequest
from .storage import ChunkStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class _Gate:
    """Coordinates concurrent uploads of one (session, chunk_index)."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.done = asyncio.Event()
        self.refs = 0


class UploadService:
    def __init__(self, db: Database, store: ChunkStore):
        self.db = db
        self.store = store
        self._gates: dict[tuple[str, int], _Gate] = {}
        self._registry_lock = asyncio.Lock()

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
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_CHUNK_DIGEST",
                "X-Chunk-SHA256 must be 64 hexadecimal characters",
                {"received": declared_digest},
            )

        key = (session_id, index)

        # Join phase: either answer from a durably confirmed chunk, reject by
        # session policy, or join the in-flight gate for this index.
        async with self._registry_lock:
            confirmed = self.db.get_confirmed_chunk(session_id, index)
            if confirmed is not None:
                session = self.get_session_or_404(session_id)
                if confirmed["sha256"] == digest:
                    return self._chunk_receipt(session, confirmed, duplicate=True), 200
                raise ApiError(
                    409,
                    "CHUNK_CONFLICT",
                    "chunk index already holds different content; the stored chunk is unchanged",
                    {
                        "chunk_index": index,
                        "stored_sha256": confirmed["sha256"],
                        "rejected_sha256": digest,
                    },
                )

            session = self.get_session_or_404(session_id)
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

            gate = self._gates.get(key)
            if gate is None:
                gate = _Gate()
                self._gates[key] = gate
            gate.refs += 1
            is_leader = gate.refs == 1

        try:
            if is_leader:
                return await self._upload_as_leader(gate, session_id, index, digest, stream)
            return await self._upload_as_follower(gate, session_id, index, digest, stream)
        finally:
            async with self._registry_lock:
                gate.refs -= 1
                if gate.refs == 0:
                    self._gates.pop(key, None)

    async def _upload_as_leader(
        self,
        gate: _Gate,
        session_id: str,
        index: int,
        digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)
        expected = self.expected_chunk_size(session, index)
        final_path = self.store.chunk_path(session_id, index)
        tmp = self.store.tmp_path(session_id)
        record = {
            "session_id": session_id,
            "chunk_index": index,
            "size": expected,
            "sha256": digest,
            "path": str(final_path),
            "received_at": clock.utcnow().isoformat(),
        }
        # Pending row only: the bitmap is not updated until validation + commit.
        self.db.insert_pending_chunk(record, str(tmp))

        body_error: ApiError | None = None
        committed_tmp = False
        try:
            size, actual = await self.store.write_chunk_tmp(tmp, stream)
            if size != expected:
                body_error = ApiError(
                    400,
                    "CHUNK_SIZE_MISMATCH",
                    f"chunk {index} must be exactly {expected} bytes, got {size}",
                    {"chunk_index": index, "expected_size": expected, "actual_size": size},
                )
            elif actual != digest:
                body_error = ApiError(
                    400,
                    "CHUNK_DIGEST_MISMATCH",
                    "chunk body SHA-256 does not match X-Chunk-SHA256; chunk was discarded",
                    {"chunk_index": index, "declared_sha256": digest, "actual_sha256": actual},
                )
            if body_error is not None:
                raise body_error

            async with gate.lock:
                # Re-check policy after the body finished streaming: the
                # session may have expired (or been finalized) meanwhile.
                session = self.get_session_or_404(session_id)
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
                self.store.commit_tmp(tmp, final_path)
                self._commit_bitmap(session_id, index, str(final_path))
                committed_tmp = True
                confirmed = self.db.get_confirmed_chunk(session_id, index)
            session = self.get_session_or_404(session_id)
            return self._chunk_receipt(session, confirmed, duplicate=False), 201
        finally:
            # Settle durable state before releasing followers: when they take
            # the gate lock there must no longer be a pending row for the index.
            if not committed_tmp:
                self.db.abort_pending_chunk(session_id, index)
                self.store.discard(tmp)
            gate.done.set()

    async def _upload_as_follower(
        self,
        gate: _Gate,
        session_id: str,
        index: int,
        digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)
        expected = self.expected_chunk_size(session, index)
        final_path = self.store.chunk_path(session_id, index)
        tmp = self.store.tmp_path(session_id)

        # Drain this request's body into a private temp file concurrently with
        # the leader -- never block on the (possibly paused) leader, otherwise
        # TCP flow control could stall both connections. Any temp file left
        # behind (by validation failure or disconnect) is cleaned in finally.
        body_error: ApiError | None = None
        size = actual = None
        size, actual = await self.store.write_chunk_tmp(tmp, stream)
        if size != expected:
            body_error = ApiError(
                400,
                "CHUNK_SIZE_MISMATCH",
                f"chunk {index} must be exactly {expected} bytes, got {size}",
                {"chunk_index": index, "expected_size": expected, "actual_size": size},
            )
        elif actual != digest:
            body_error = ApiError(
                400,
                "CHUNK_DIGEST_MISMATCH",
                "chunk body SHA-256 does not match X-Chunk-SHA256; chunk was discarded",
                {"chunk_index": index, "declared_sha256": digest, "actual_sha256": actual},
            )

        committed_tmp = False
        try:
            await gate.done.wait()
            async with gate.lock:
                confirmed = self.db.get_confirmed_chunk(session_id, index)
                if confirmed is not None:
                    # Leader (or a prior follower) durably settled this index.
                    session = self.get_session_or_404(session_id)
                    if confirmed["sha256"] == digest:
                        return self._chunk_receipt(session, confirmed, duplicate=True), 200
                    raise ApiError(
                        409,
                        "CHUNK_CONFLICT",
                        "chunk index already holds different content; the stored chunk is unchanged",
                        {
                            "chunk_index": index,
                            "stored_sha256": confirmed["sha256"],
                            "rejected_sha256": digest,
                        },
                    )

                # Leader failed: a valid retry body may take over, one winner.
                if body_error is not None:
                    raise body_error
                # Re-check policy after waiting: the session may have expired
                # (or been completed) while this body was streaming.
                session = self.get_session_or_404(session_id)
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
                # Sweep any leftover pending row before claiming the index.
                self.db.abort_pending_chunk(session_id, index)
                record = {
                    "session_id": session_id,
                    "chunk_index": index,
                    "size": expected,
                    "sha256": digest,
                    "path": str(final_path),
                    "received_at": clock.utcnow().isoformat(),
                }
                self.db.insert_pending_chunk(record, str(tmp))
                try:
                    self.store.commit_tmp(tmp, final_path)
                    self._commit_bitmap(session_id, index, str(final_path))
                    committed_tmp = True
                except BaseException:
                    self.db.abort_pending_chunk(session_id, index)
                    raise
                confirmed = self.db.get_confirmed_chunk(session_id, index)
                session = self.get_session_or_404(session_id)
                return self._chunk_receipt(session, confirmed, duplicate=False), 201
        finally:
            if not committed_tmp:
                self.store.discard(tmp)

    def _commit_bitmap(self, session_id: str, index: int, final_path: str) -> None:
        """Promote the pending row and set its bit in a single transaction."""
        session = self.get_session_or_404(session_id)
        bitmap = bytearray(session["bitmap"])
        set_bit(bitmap, index)
        self.db.commit_pending_chunk(session_id, index, final_path, bytes(bitmap))

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

        paths, lost = [], []
        for i in range(total):
            path = self.store.chunk_path(session_id, i)
            if path.exists():
                paths.append(path)
            else:
                lost.append(i)
        if lost:
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "chunk files are missing on disk",
                {"missing_chunks": lost, "total_chunks": total},
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

    # ---- helpers ----

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


def reconcile(db: Database, store: ChunkStore) -> None:
    """Rebuild durable state after a (possibly unclean) restart.

    - 'pending' chunk rows (stream interrupted by a crash) are dropped;
    - confirmed rows whose files vanished or have a wrong size are dropped;
    - chunk files without a matching confirmed row (crash between replace and
      commit) are removed;
    - the persisted bitmap is rebuilt from the surviving confirmed rows;
    - leftover temp files are removed.

    Net effect: confirmed chunks are never reported missing, and unconfirmed
    bytes are never reported as received.
    """
    for session in db.list_sessions():
        session_id = session["session_id"]
        confirmed: set[int] = set()
        for row in db.list_chunks(session_id):
            if row.get("state") != "confirmed":
                db.delete_chunk(session_id, row["chunk_index"])
                continue
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed.add(row["chunk_index"])
            else:
                db.delete_chunk(session_id, row["chunk_index"])
        chunk_dir = store.chunk_dir(session_id)
        if chunk_dir.exists():
            for entry in chunk_dir.iterdir():
                if entry.suffix == ".tmp":
                    entry.unlink()
                elif entry.suffix == ".chunk":
                    try:
                        index = int(entry.stem)
                    except ValueError:
                        entry.unlink()
                        continue
                    if index not in confirmed:
                        entry.unlink()
        bitmap = new_bitmap(session["total_chunks"])
        for index in confirmed:
            set_bit(bitmap, index)
        db.update_bitmap(session_id, bytes(bitmap))
    store.purge_tmp()
