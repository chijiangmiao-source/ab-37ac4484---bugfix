"""Concurrency acceptance tests for the chunk upload protocol.

These tests drive a real uvicorn process over TCP with *two independent HTTP
connections* and request bodies that block mid-stream on controllable barriers,
reproducing the interleaving where:

* connection A streams part of a wrong body (digest header still declares the
  correct one) and pauses;
* connection B concurrently retries with the complete, correct body;
* status / finalize must not count the unconfirmed chunk;
* B must never receive a premature 2xx;
* after A is rejected, B's correct body is what becomes the durable chunk.

They also restart the API after an aborted transfer to prove unfinished
requests never pollute persisted progress.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ONE_MIB = 1024 * 1024
PREFIX = 4096


def make_bytes(size: int, seed: str = "payload") -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def future_expiry(hours: float = 1.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def free_tcp_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ApiServer:
    def __init__(self, data_dir: Path, log_path: Path, inflight_wait_timeout: float | None = None):
        self.data_dir = data_dir
        self.log_path = log_path
        self.inflight_wait_timeout = inflight_wait_timeout
        self.port = free_tcp_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.proc: subprocess.Popen | None = None

    def _spawn(self) -> None:
        env = {
            **os.environ,
            "DATA_DIR": str(self.data_dir),
            "PYTHONUNBUFFERED": "1",
        }
        if self.inflight_wait_timeout is not None:
            env["INFLIGHT_WAIT_TIMEOUT"] = str(self.inflight_wait_timeout)
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.asgi:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=open(self.log_path, "ab"),
            stderr=subprocess.STDOUT,
        )

    def start(self) -> None:
        self._spawn()
        self.wait_healthy()

    def wait_healthy(self, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise AssertionError(
                    f"uvicorn exited early (code {self.proc.returncode}); log:\n{self.log_path.read_text()}"
                )
            try:
                with urllib.request.urlopen(f"{self.base_url}/healthz", timeout=1) as resp:
                    if resp.status == 200:
                        return
            except (urllib.error.URLError, ConnectionError, OSError) as exc:
                last_error = exc
            time.sleep(0.05)
        raise AssertionError(f"API did not become healthy: {last_error}")

    def stop(self) -> None:
        if self.proc is None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc = None

    def restart(self) -> None:
        self.stop()
        self._spawn()
        self.wait_healthy()


@pytest.fixture()
def make_server(tmp_path):
    data_dir = tmp_path / "data"
    log_path = tmp_path / "api.log"
    servers: list[ApiServer] = []

    def _make(inflight_wait_timeout: float | None = None) -> ApiServer:
        server = ApiServer(data_dir, log_path, inflight_wait_timeout)
        server.start()
        servers.append(server)
        return server

    yield _make
    for server in servers:
        server.stop()


def create_session_sync(base_url: str, payload: bytes, chunk_size: int) -> str:
    req = urllib.request.Request(
        f"{base_url}/sessions",
        data=json.dumps(
            {
                "file_size": len(payload),
                "chunk_size": chunk_size,
                "file_sha256": sha256(payload),
                "expires_at": future_expiry(),
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 201
        return json.loads(resp.read())["session_id"]


def session_dir(data_dir: Path, sid: str) -> Path:
    return data_dir / "chunks" / sid


def tmp_files(sid_dir: Path) -> list[Path]:
    if not sid_dir.exists():
        return []
    return [p for p in sid_dir.iterdir() if p.name.endswith(".tmp")]


def chunk_files(sid_dir: Path) -> list[Path]:
    if not sid_dir.exists():
        return []
    return [p for p in sid_dir.iterdir() if p.name.endswith(".chunk")]


async def wait_until(predicate, description: str, timeout: float = 15.0, interval: float = 0.02):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if predicate():
            return
        if loop.time() >= deadline:
            raise AssertionError(f"timed out waiting for: {description}")
        await asyncio.sleep(interval)


def gated_put(client: httpx.AsyncClient, url: str, digest: str, parts: list):
    """PUT whose body sends parts sequentially; (data, gate) pairs pause on gate."""

    async def body():
        for data, gate in parts:
            if gate is not None:
                await gate.wait()
            yield data

    return client.put(url, content=body(), headers={"X-Chunk-SHA256": digest})


def full_put(client: httpx.AsyncClient, url: str, digest: str, data: bytes):
    async def body():
        yield data

    return client.put(url, content=body(), headers={"X-Chunk-SHA256": digest})


async def run_concurrent_interleave(server: ApiServer, *, structured_inflight: bool) -> None:
    """The bug-report interleaving, parameterised over B's wait outcome.

    A declares P's digest but streams same-length wrong content Q, pausing after
    a prefix. B streams the complete correct P on a separate connection.
    """
    p_body = make_bytes(ONE_MIB, "correct-payload")
    q_body = make_bytes(ONE_MIB, "wrong-payload")
    assert p_body != q_body
    p_digest = sha256(p_body)
    sid = await asyncio.to_thread(create_session_sync, server.base_url, p_body, ONE_MIB)
    sid_dir = session_dir(server.data_dir, sid)
    url = f"{server.base_url}/sessions/{sid}/chunks/0"

    gate = asyncio.Event()
    async with (
        httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_a,
        httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_b,
        httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_ctl,
    ):
        # --- A streams a prefix of Q, then stalls with the request open. ---
        task_a = asyncio.create_task(
            gated_put(
                conn_a,
                url,
                p_digest,  # header still claims P
                [(q_body[:PREFIX], None), (q_body[PREFIX:], gate)],
            )
        )
        await wait_until(
            lambda: len(tmp_files(sid_dir)) == 1,
            "A's temp file after the first prefix",
        )

        # While A is mid-flight the chunk must look missing everywhere.
        status = (await conn_ctl.get(f"/sessions/{sid}")).json()
        assert status["received_count"] == 0, status
        assert status["missing_chunks"] == [0], status
        resp = await conn_ctl.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 409, resp.text
        assert resp.json()["error"]["code"] == "CHUNKS_INCOMPLETE"
        assert resp.json()["error"]["details"]["missing_chunks"] == [0]

        # --- B uploads the complete correct P on an independent connection. ---
        task_b = asyncio.create_task(full_put(conn_b, url, p_digest, p_body))
        # B spools its whole body, then must block behind A: never a 2xx early.
        await wait_until(
            lambda: any(p.stat().st_size == ONE_MIB for p in tmp_files(sid_dir)),
            "B's fully spooled temp file",
        )
        # Progress remains unchanged while both requests are unresolved.
        await asyncio.sleep(0.2)
        status = (await conn_ctl.get(f"/sessions/{sid}")).json()
        assert status["received_count"] == 0
        assert status["missing_chunks"] == [0]

        if structured_inflight:
            resp_b = await asyncio.wait_for(task_b, timeout=5)
            assert resp_b.status_code == 409, resp_b.text
            body_b = resp_b.json()
            assert body_b["error"]["code"] == "CHUNK_UPLOAD_IN_PROGRESS", body_b
            assert resp_b.headers.get("retry-after") == "1"

            # B's rejected attempt must leave no body behind; A is still going.
            await wait_until(lambda: len(tmp_files(sid_dir)) == 1, "B spool cleanup after 409")
            assert chunk_files(sid_dir) == []
        else:
            # Default wait budget: B is still parked behind A, no early success.
            await asyncio.sleep(0.3)
            assert not task_b.done(), "concurrent retry must not succeed before A finishes"

            # Let A finish: digest mismatch rejects Q and releases the slot.
            gate.set()
            resp_a = await asyncio.wait_for(task_a, timeout=15)
            assert resp_a.status_code == 400, resp_a.text
            assert resp_a.json()["error"]["code"] == "CHUNK_DIGEST_MISMATCH"

            # B was waiting on A; its already-spooled, correct body is promoted.
            resp_b = await asyncio.wait_for(task_b, timeout=15)
            assert resp_b.status_code == 201, resp_b.text
            receipt = resp_b.json()
            assert receipt["duplicate"] is False
            assert receipt["sha256"] == p_digest
            assert receipt["received_count"] == 1

        if structured_inflight:
            # A still owns the slot; let it finish and be rejected.
            gate.set()
            resp_a = await asyncio.wait_for(task_a, timeout=15)
            assert resp_a.status_code == 400
            assert resp_a.json()["error"]["code"] == "CHUNK_DIGEST_MISMATCH"

            # A failed; a fresh legitimate retry now uploads P and publishes.
            async with httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_retry:
                resp = await full_put(conn_retry, url, p_digest, p_body)
                assert resp.status_code == 201, resp.text
                assert resp.json()["received_count"] == 1

        await _finish_and_verify(conn_ctl, sid, p_body)


async def _finish_and_verify(conn: httpx.AsyncClient, sid: str, p_body: bytes) -> None:
    status = (await conn.get(f"/sessions/{sid}")).json()
    assert status["received_count"] == 1, status
    assert status["missing_chunks"] == [], status

    resp = await conn.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 200, resp.text
    assert resp.json()["final_sha256"] == sha256(p_body)

    resp = await conn.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 200 and resp.json()["final_sha256"] == sha256(p_body)

    download = await conn.get(f"/sessions/{sid}/artifact")
    assert download.status_code == 200
    assert download.content == p_body
    assert download.headers["x-file-sha256"] == sha256(p_body)


def test_concurrent_retry_waits_then_takes_over(make_server):
    server = make_server()
    asyncio.run(run_concurrent_interleave(server, structured_inflight=False))


def test_concurrent_retry_structured_in_progress_then_retry_succeeds(make_server):
    server = make_server(inflight_wait_timeout=0.5)
    asyncio.run(run_concurrent_interleave(server, structured_inflight=True))


async def holder_succeeds_and_waiter_gets_durable_duplicate(server: ApiServer) -> None:
    p_body = make_bytes(ONE_MIB, "both-correct")
    p_digest = sha256(p_body)
    sid = await asyncio.to_thread(create_session_sync, server.base_url, p_body, ONE_MIB)
    sid_dir = session_dir(server.data_dir, sid)
    url = f"{server.base_url}/sessions/{sid}/chunks/0"

    gate = asyncio.Event()
    async with (
        httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_a,
        httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_b,
        httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_ctl,
    ):
        # A is the slow-but-correct holder this time.
        task_a = asyncio.create_task(
            gated_put(
                conn_a,
                url,
                p_digest,
                [(p_body[:PREFIX], None), (p_body[PREFIX:], gate)],
            )
        )
        await wait_until(lambda: len(tmp_files(sid_dir)) == 1, "A's partial temp file")

        task_b = asyncio.create_task(full_put(conn_b, url, p_digest, p_body))
        await wait_until(
            lambda: any(p.stat().st_size == ONE_MIB for p in tmp_files(sid_dir)),
            "B's fully spooled temp file",
        )
        await asyncio.sleep(0.3)
        assert not task_b.done(), "B must not get 2xx before A's body is durable"

        # A finishes with the correct body: it commits; B's parked retry must
        # then observe the durable chunk as a genuine idempotent duplicate.
        gate.set()
        resp_a = await asyncio.wait_for(task_a, timeout=15)
        assert resp_a.status_code == 201, resp_a.text
        assert resp_a.json()["duplicate"] is False

        resp_b = await asyncio.wait_for(task_b, timeout=15)
        assert resp_b.status_code == 200, resp_b.text
        receipt_b = resp_b.json()
        assert receipt_b["duplicate"] is True
        assert receipt_b["sha256"] == p_digest
        assert receipt_b["received_count"] == 1

        status = (await conn_ctl.get(f"/sessions/{sid}")).json()
        assert status["received_count"] == 1
        assert status["missing_chunks"] == []

    # B's 2xx must correspond to a chunk confirmed after restart as well.
    await asyncio.to_thread(server.restart)
    async with httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn:
        status = (await conn.get(f"/sessions/{sid}")).json()
        assert status["received_count"] == 1
        assert status["missing_chunks"] == []

        resp = await conn.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200, resp.text
        assert resp.json()["final_sha256"] == p_digest
        download = await conn.get(f"/sessions/{sid}/artifact")
        assert download.status_code == 200
        assert download.content == p_body
        assert download.headers["x-file-sha256"] == p_digest


def test_waiting_retry_gets_durable_duplicate_after_holder_commits(make_server):
    server = make_server()
    asyncio.run(holder_succeeds_and_waiter_gets_durable_duplicate(server))


async def abort_midstream_and_restart(server: ApiServer) -> None:
    p_body = make_bytes(ONE_MIB, "restart-payload")
    p_digest = sha256(p_body)
    sid = await asyncio.to_thread(create_session_sync, server.base_url, p_body, ONE_MIB)
    sid_dir = session_dir(server.data_dir, sid)
    url = f"{server.base_url}/sessions/{sid}/chunks/0"

    gate = asyncio.Event()
    async with httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_a:
        task_a = asyncio.create_task(
            gated_put(
                conn_a,
                url,
                p_digest,
                [(p_body[:PREFIX], None), (p_body[PREFIX:], gate)],
            )
        )
        await wait_until(lambda: len(tmp_files(sid_dir)) == 1, "partial temp file")

        # Abort the transfer mid-stream (client drops the connection).
        task_a.cancel()
        with pytest.raises((asyncio.CancelledError, httpx.HTTPError)):
            await task_a

    # Server notices the disconnect and removes every trace of the attempt.
    await wait_until(lambda: not tmp_files(sid_dir), "temp file cleanup after abort")
    assert chunk_files(sid_dir) == []

    async with httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn_ctl:
        status = (await conn_ctl.get(f"/sessions/{sid}")).json()
        assert status["received_count"] == 0
        assert status["missing_chunks"] == [0]

    # Restart the API: unfinished requests must not survive reconciliation.
    await asyncio.to_thread(server.restart)
    assert tmp_files(sid_dir) == []
    assert chunk_files(sid_dir) == []

    async with httpx.AsyncClient(base_url=server.base_url, timeout=30) as conn:
        status = (await conn.get(f"/sessions/{sid}")).json()
        assert status["received_count"] == 0
        assert status["missing_chunks"] == [0]

        # The legitimate retry works against the freshly restarted process.
        resp = await full_put(conn, url, p_digest, p_body)
        assert resp.status_code == 201, resp.text
        await _finish_and_verify(conn, sid, p_body)


def test_aborted_transfer_does_not_pollute_restart(make_server):
    server = make_server()
    asyncio.run(abort_midstream_and_restart(server))
