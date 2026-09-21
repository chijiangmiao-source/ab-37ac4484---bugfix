"""Resume-edge coverage: validation, idempotency, expiry, restart, integrity."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app import clock
from app.config import Settings
from app.main import create_app


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


def create_session(client, payload: bytes, chunk_size: int, *, file_sha256=None, expires_at=None) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(payload),
            "chunk_size": chunk_size,
            "file_sha256": file_sha256 or sha256(payload),
            "expires_at": expires_at or future_expiry(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


_AUTO = object()


def put_chunk(client, sid, index, body, digest=_AUTO):
    headers = {}
    if digest is _AUTO:
        digest = sha256(body)
    if digest is not None:
        headers["X-Chunk-SHA256"] = digest
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers=headers)


def chunk(payload, chunk_size, i):
    return payload[i * chunk_size : (i + 1) * chunk_size]


def upload_all(client, sid, payload, chunk_size):
    for i in range(-(-len(payload) // chunk_size)):
        resp = put_chunk(client, sid, i, chunk(payload, chunk_size, i))
        assert resp.status_code == 201, resp.text


# ---------- session creation ----------

def test_create_session_computes_total_chunks(client):
    body = create_session(client, make_bytes(10), 4)
    assert body["total_chunks"] == 3
    assert body["status"] == "active"
    assert body["received_count"] == 0
    assert body["missing_chunks"] == [0, 1, 2]


def test_create_session_validation_errors(client):
    base = {"file_size": 10, "chunk_size": 4, "file_sha256": "a" * 64, "expires_at": future_expiry()}
    for patch in (
        {"file_size": 0},
        {"chunk_size": 0},
        {"file_sha256": "not-a-sha"},
        {"expires_at": "2030-01-01 00:00:00"},  # naive timestamp
    ):
        resp = client.post("/sessions", json={**base, **patch})
        assert resp.status_code == 422, patch
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    resp = client.post("/sessions", json={**base, "expires_at": past})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "SESSION_EXPIRES_IN_PAST"


# ---------- chunk upload ----------

def test_upload_out_of_order_and_status(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    assert put_chunk(client, sid, 2, chunk(payload, 4, 2)).status_code == 201
    assert put_chunk(client, sid, 0, chunk(payload, 4, 0)).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == [1]
    assert status["received_count"] == 2
    assert put_chunk(client, sid, 1, chunk(payload, 4, 1)).status_code == 201
    assert client.get(f"/sessions/{sid}").json()["missing_chunks"] == []


def test_digest_mismatch_is_not_recorded(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    resp = put_chunk(client, sid, 0, chunk(payload, 4, 0), digest=sha256(b"wrong"))
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "CHUNK_DIGEST_MISMATCH"
    assert client.get(f"/sessions/{sid}").json()["missing_chunks"] == [0, 1, 2]


def test_size_mismatch_is_not_recorded(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    resp = put_chunk(client, sid, 0, payload[:3])  # non-last chunk must be 4 bytes
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "CHUNK_SIZE_MISMATCH"
    resp = put_chunk(client, sid, 2, make_bytes(4, "x"))  # last chunk must be 2 bytes
    assert resp.status_code == 400
    assert resp.json()["error"]["details"]["expected_size"] == 2
    assert client.get(f"/sessions/{sid}").json()["missing_chunks"] == [0, 1, 2]


def test_index_out_of_range(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    for bad_index in ("3", "100", "-1", "abc"):
        resp = put_chunk(client, sid, bad_index, make_bytes(4))
        assert resp.status_code == 400, bad_index
        assert resp.json()["error"]["code"] == "CHUNK_INDEX_OUT_OF_RANGE"


def test_missing_or_malformed_digest_header(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    resp = put_chunk(client, sid, 0, chunk(payload, 4, 0), digest=None)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    resp = put_chunk(client, sid, 0, chunk(payload, 4, 0), digest="zz")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_CHUNK_DIGEST"


def test_idempotent_reupload_and_conflict(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    first = put_chunk(client, sid, 0, chunk(payload, 4, 0))
    assert first.status_code == 201
    assert first.json()["duplicate"] is False
    replay = put_chunk(client, sid, 0, chunk(payload, 4, 0))
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    conflict = put_chunk(client, sid, 0, make_bytes(4, "other"))
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "CHUNK_CONFLICT"
    # the conflict must not have polluted the confirmed chunk
    status = client.get(f"/sessions/{sid}").json()
    assert status["received_count"] == 1
    assert status["missing_chunks"] == [1, 2]


def test_single_chunk_file(client):
    payload = make_bytes(3)
    sid = create_session(client, payload, 1024)["session_id"]
    assert put_chunk(client, sid, 0, payload).status_code == 201
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 200
    assert resp.json()["final_sha256"] == sha256(payload)


# ---------- finalize / artifact ----------

def test_finalize_requires_all_chunks(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, chunk(payload, 4, 0))
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"]["code"] == "CHUNKS_INCOMPLETE"
    assert body["error"]["details"]["missing_chunks"] == [1, 2]


def test_finalize_publishes_artifact(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    upload_all(client, sid, payload, 4)
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["final_sha256"] == sha256(payload)
    again = client.post(f"/sessions/{sid}/finalize")
    assert again.status_code == 200
    assert again.json()["final_sha256"] == body["final_sha256"]
    download = client.get(f"/sessions/{sid}/artifact")
    assert download.status_code == 200
    assert download.content == payload
    assert download.headers["x-file-sha256"] == sha256(payload)
    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "completed"
    assert status["artifact_url"] == f"/sessions/{sid}/artifact"


def test_artifact_not_ready_before_finalize(client):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    resp = client.get(f"/sessions/{sid}/artifact")
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "ARTIFACT_NOT_READY"


def test_integrity_mismatch_keeps_state(client):
    payload = make_bytes(10)
    declared = sha256(b"declared-content-differs")
    sid = create_session(client, payload, 4, file_sha256=declared)["session_id"]
    upload_all(client, sid, payload, 4)
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "INTEGRITY_MISMATCH"
    assert body["error"]["details"]["assembled_sha256"] == sha256(payload)
    assert body["error"]["details"]["declared_sha256"] == declared
    # state is preserved: chunks stay confirmed, no artifact is published
    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "active"
    assert status["received_count"] == 3
    assert status["missing_chunks"] == []
    assert client.get(f"/sessions/{sid}/artifact").status_code == 409


# ---------- expiry ----------

def test_expired_session_rejects_new_chunks(client, monkeypatch):
    payload = make_bytes(10)
    sid = create_session(client, payload, 4)["session_id"]
    put_chunk(client, sid, 0, chunk(payload, 4, 0))
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    monkeypatch.setattr(clock, "utcnow", lambda: later)
    resp = put_chunk(client, sid, 1, chunk(payload, 4, 1))
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "SESSION_EXPIRED"
    # idempotent replay of an already confirmed chunk stays a no-op success
    replay = put_chunk(client, sid, 0, chunk(payload, 4, 0))
    assert replay.status_code == 200
    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "expired"
    assert status["missing_chunks"] == [1, 2]
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "SESSION_EXPIRED"


# ---------- restart ----------

def test_restart_resumes_from_confirmed_chunks(data_dir):
    payload = make_bytes(10)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 4)["session_id"]
        assert put_chunk(c1, sid, 0, chunk(payload, 4, 0)).status_code == 201
        assert put_chunk(c1, sid, 2, chunk(payload, 4, 2)).status_code == 201

    # leftovers of an unclean shutdown: a temp file and an orphan chunk file
    chunk_dir = data_dir / "chunks" / sid
    (chunk_dir / ".partial.tmp").write_bytes(b"half-written")
    (chunk_dir / "00000001.chunk").write_bytes(chunk(payload, 4, 1))  # never committed to SQLite

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        assert status["received_count"] == 2
        assert status["missing_chunks"] == [1]  # orphan bytes must not count as confirmed
        assert not (chunk_dir / ".partial.tmp").exists()
        assert not (chunk_dir / "00000001.chunk").exists()
        assert put_chunk(c2, sid, 1, chunk(payload, 4, 1)).status_code == 201
        resp = c2.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200
        assert resp.json()["final_sha256"] == sha256(payload)
        assert c2.get(f"/sessions/{sid}/artifact").content == payload


# ---------- structured errors ----------

def test_unknown_session_and_route(client):
    resp = client.get("/sessions/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "SESSION_NOT_FOUND"
    resp = client.put(
        "/sessions/does-not-exist/chunks/0",
        content=b"xxxx",
        headers={"X-Chunk-SHA256": sha256(b"xxxx")},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "SESSION_NOT_FOUND"
    resp = client.get("/no-such-route")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"
