"""Acceptance coverage for concurrent / interrupted chunk uploads.

These tests drive a *real* uvicorn server over TCP so every request is an
independent HTTP connection. Two raw sockets ("A" and "B") act as a
controllable streaming barrier: bytes only leave the wire when the test says
so, which lets us assert progress semantics while an upload is paused
mid-body.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import select
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import uvicorn

from app.config import Settings
from app.main import create_app

CHUNK = 1024 * 1024  # 1 MiB; the scenario uses a single-chunk file


def make_bytes(size: int, seed: str) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- server -----


class LiveServer:
    """Uvicorn on an ephemeral port; stop()/start() models an API restart."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.port: int | None = None
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None
        self._bound: socket.socket | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> "LiveServer":
        # Bind the listening socket ourselves so the ephemeral port is known
        # before the server thread finishes wiring its socket list.
        bound = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bound.bind(("127.0.0.1", 0))
        self._bound = bound
        self.port = bound.getsockname()[1]

        def run() -> None:
            app = create_app(Settings(data_dir=self.data_dir))
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            config = uvicorn.Config(
                app,
                host="127.0.0.1",
                port=0,
                log_level="error",
                timeout_graceful_shutdown=2,
            )
            server = uvicorn.Server(config)
            self._server = server
            loop.run_until_complete(server.serve(sockets=[bound]))
            loop.close()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        if not self._healthcheck():
            raise RuntimeError("test server is not healthy")
        return self

    def stop(self) -> None:
        assert self._server is not None
        self._server.should_exit = True
        assert self._thread is not None
        self._thread.join(timeout=15)
        assert not self._thread.is_alive(), "server thread did not stop"
        if self._bound is not None:
            try:
                self._bound.close()
            except OSError:
                pass
            self._bound = None

    def _healthcheck(self) -> bool:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"{self.url}/healthz", timeout=1).status_code == 200:
                    return True
            except httpx.HTTPError:
                time.sleep(0.05)
        return False


@pytest.fixture()
def live_server(tmp_path):
    server = LiveServer(tmp_path / "data").start()
    yield server
    server.stop()


# ---------------------------------------------------------------- helpers ----


def create_session(client: httpx.Client, payload: bytes, chunk_size: int) -> str:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(payload),
            "chunk_size": chunk_size,
            "file_sha256": sha(payload),
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def raw_connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=15)
    return sock


def send_put_head(sock: socket.socket, sid: str, index: int, digest: str, length: int) -> None:
    sock.sendall(
        (
            f"PUT /sessions/{sid}/chunks/{index} HTTP/1.1\r\n"
            f"Host: testserver\r\n"
            f"Content-Length: {length}\r\n"
            f"X-Chunk-SHA256: {digest}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode()
    )


def response_waiting(sock: socket.socket, timeout: float = 15) -> tuple[int, dict]:
    """Read the full HTTP/1.1 response (server closes the connection)."""
    raw = bytearray()
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        raw += chunk
    assert raw, f"no response from peer (raw={raw!r})"
    head, _, body_bytes = bytes(raw).partition(b"\r\n\r\n")
    status = int(head.split(b"\r\n", 1)[0].split()[1])
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {"raw": body_bytes}
    return status, body


def has_response_waiting(sock: socket.socket, timeout: float = 0.2) -> bool:
    readable, _, _ = select.select([sock], [], [], timeout)
    return bool(readable)


def assert_no_response(sock: socket.socket, label: str) -> None:
    assert not has_response_waiting(sock), f"{label} answered before the chunk was durable"


def get_status(client: httpx.Client, sid: str) -> dict:
    resp = client.get(f"/sessions/{sid}")
    assert resp.status_code == 200, resp.text
    return resp.json()


def chunk_files(data_dir: Path, sid: str) -> tuple[list, list]:
    directory = data_dir / "chunks" / sid
    tmps, chunks = [], []
    if directory.exists():
        for entry in directory.iterdir():
            (tmps if entry.name.endswith(".tmp") else chunks).append(entry.name)
    return sorted(tmps), sorted(chunks)


# ----------------------------------------------------------------- tests -----


def test_paused_leader_blocks_progress_and_concurrent_success(live_server):
    """The reported bug: A lies about the digest and pauses; B retries fully.

    While A is in flight the chunk must remain missing for status AND finalize,
    and B must not receive any success. After A is rejected, B's valid body is
    promoted and the session publishes an artifact byte-identical to P.
    """
    P, Q = make_bytes(CHUNK, "P"), make_bytes(CHUNK, "Q")
    p_digest = sha(P)
    assert P != Q and len(P) == len(Q)

    with httpx.Client(base_url=live_server.url, timeout=15) as client:
        sid = create_session(client, P, CHUNK)

        conn_a = raw_connect(live_server.port)
        try:
            # A: header claims P's digest, body is Q, and it pauses early.
            send_put_head(conn_a, sid, 0, p_digest, len(Q))
            conn_a.sendall(Q[:4096])
            time.sleep(0.4)  # let the server consume the prefix

            status = get_status(client, sid)
            assert status["received_count"] == 0
            assert status["missing_chunks"] == [0]

            fin = client.post(f"/sessions/{sid}/finalize")
            assert fin.status_code == 409
            assert fin.json()["error"]["code"] == "CHUNKS_INCOMPLETE"
            assert fin.json()["error"]["details"]["missing_chunks"] == [0]

            # B: independent connection, complete and correct P, same digest.
            conn_b = raw_connect(live_server.port)
            try:
                send_put_head(conn_b, sid, 0, p_digest, len(P))
                conn_b.sendall(P)

                # While A is paused B must not be told "duplicate success";
                # progress must keep reporting the chunk as missing.
                for _ in range(6):
                    assert_no_response(conn_b, "concurrent request B")
                    status = get_status(client, sid)
                    assert status["received_count"] == 0
                    assert status["missing_chunks"] == [0]

                # Resume A with the remainder of Q -> digest mismatch.
                conn_a.sendall(Q[4096:])
                status_a, body_a = response_waiting(conn_a)
                assert status_a == 400
                assert body_a["error"]["code"] == "CHUNK_DIGEST_MISMATCH"

                # B now promotes its already-validated body (201, not a bogus
                # early 200); its success corresponds to a durable chunk.
                status_b, body_b = response_waiting(conn_b)
                assert status_b == 201, (status_b, body_b)
                assert body_b["duplicate"] is False
                assert body_b["sha256"] == p_digest
                assert body_b["received_count"] == 1
            finally:
                conn_b.close()

            # Both interfaces now agree: the chunk is present and finalizable.
            status = get_status(client, sid)
            assert status["received_count"] == 1
            assert status["missing_chunks"] == []

            # A sequential replay of P is the real idempotent duplicate.
            replay = client.put(
                f"/sessions/{sid}/chunks/0", content=P, headers={"X-Chunk-SHA256": p_digest}
            )
            assert replay.status_code == 200
            assert replay.json()["duplicate"] is True

            fin = client.post(f"/sessions/{sid}/finalize")
            assert fin.status_code == 200, fin.text
            assert fin.json()["final_sha256"] == p_digest

            artifact = client.get(f"/sessions/{sid}/artifact")
            assert artifact.status_code == 200
            assert artifact.content == P
            assert artifact.headers["x-file-sha256"] == p_digest
        finally:
            conn_a.close()

    # Restart proves B's success was durable, not an in-memory reservation.
    live_server.stop()
    live_server.start()
    with httpx.Client(base_url=live_server.url, timeout=15) as client:
        status = get_status(client, sid)
        assert status["status"] == "completed"
        assert client.get(f"/sessions/{sid}/artifact").content == P


def test_concurrent_same_content_waits_and_succeeds_idempotently(live_server):
    """B uploads the same bytes while A is still streaming: it waits, then gets
    a duplicate 200 -- and that success is backed by the durable chunk."""
    P = make_bytes(CHUNK, "same")
    p_digest = sha(P)

    with httpx.Client(base_url=live_server.url, timeout=15) as client:
        sid = create_session(client, P, CHUNK)

        conn_a = raw_connect(live_server.port)
        conn_b = raw_connect(live_server.port)
        try:
            send_put_head(conn_a, sid, 0, p_digest, len(P))
            conn_a.sendall(P[:4096])  # leader pauses
            time.sleep(0.4)

            send_put_head(conn_b, sid, 0, p_digest, len(P))
            conn_b.sendall(P)  # follower fully drained, must wait

            for _ in range(5):
                assert_no_response(conn_b, "follower B")
                assert get_status(client, sid)["missing_chunks"] == [0]

            conn_a.sendall(P[4096:])  # leader completes and commits
            status_a, body_a = response_waiting(conn_a)
            assert status_a == 201 and body_a["duplicate"] is False

            status_b, body_b = response_waiting(conn_b)
            assert status_b == 200 and body_b["duplicate"] is True
            assert body_b["sha256"] == p_digest

            assert get_status(client, sid)["missing_chunks"] == []
            assert client.post(f"/sessions/{sid}/finalize").status_code == 200
            assert client.get(f"/sessions/{sid}/artifact").content == P
        finally:
            conn_a.close()
            conn_b.close()


def test_concurrent_conflicting_content_never_overwrites(live_server):
    """Two different valid bodies race: the leader wins, the follower gets 409
    and the winner's bytes are what finalizes."""
    P, Q = make_bytes(CHUNK, "win"), make_bytes(CHUNK, "lose")

    with httpx.Client(base_url=live_server.url, timeout=15) as client:
        sid = create_session(client, P, CHUNK)  # session manifest declares P

        conn_a = raw_connect(live_server.port)
        conn_b = raw_connect(live_server.port)
        try:
            send_put_head(conn_a, sid, 0, sha(P), len(P))
            conn_a.sendall(P[:4096])  # leader P pauses
            time.sleep(0.4)

            send_put_head(conn_b, sid, 0, sha(Q), len(Q))
            conn_b.sendall(Q)  # follower Q waits
            assert_no_response(conn_b, "follower B")

            conn_a.sendall(P[4096:])
            status_a, body_a = response_waiting(conn_a)
            assert status_a == 201 and body_a["sha256"] == sha(P)

            status_b, body_b = response_waiting(conn_b)
            assert status_b == 409
            assert body_b["error"]["code"] == "CHUNK_CONFLICT"

            status = get_status(client, sid)
            assert status["received_count"] == 1 and status["missing_chunks"] == []

            assert client.post(f"/sessions/{sid}/finalize").status_code == 200
            assert client.get(f"/sessions/{sid}/artifact").content == P
        finally:
            conn_a.close()
            conn_b.close()


def test_aborted_upload_leaves_no_progress_after_restart(live_server, tmp_path):
    """A chunk transfer is cut off mid-body: nothing is counted, no file lands,
    and an API restart keeps the durable progress clean. A legal retry then
    completes the session."""
    data_dir = tmp_path / "data"
    P = make_bytes(CHUNK, "restart")

    with httpx.Client(base_url=live_server.url, timeout=15) as client:
        sid = create_session(client, P, CHUNK)

        conn_a = raw_connect(live_server.port)
        send_put_head(conn_a, sid, 0, sha(P), len(P))
        conn_a.sendall(P[:8192])
        time.sleep(0.4)
        assert get_status(client, sid)["missing_chunks"] == [0]

        conn_a.close()  # hard abort with most of the body unsent
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            tmps, chunks = chunk_files(data_dir, sid)
            if not tmps and not chunks:
                break
            time.sleep(0.05)
        tmps, chunks = chunk_files(data_dir, sid)
        assert tmps == [] and chunks == []
        assert get_status(client, sid)["received_count"] == 0

    # Restart with the abandoned request long gone.
    live_server.stop()
    live_server.start()

    with httpx.Client(base_url=live_server.url, timeout=15) as client:
        status = get_status(client, sid)
        assert status["received_count"] == 0
        assert status["missing_chunks"] == [0]
        tmps, chunks = chunk_files(data_dir, sid)
        assert tmps == [] and chunks == []

        resp = client.put(
            f"/sessions/{sid}/chunks/0", content=P, headers={"X-Chunk-SHA256": sha(P)}
        )
        assert resp.status_code == 201, resp.text
        assert client.post(f"/sessions/{sid}/finalize").status_code == 200
        artifact = client.get(f"/sessions/{sid}/artifact")
        assert artifact.status_code == 200
        assert artifact.content == P
        assert artifact.headers["x-file-sha256"] == sha(P)
