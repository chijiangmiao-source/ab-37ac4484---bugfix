"""One-shot acceptance client: exercises the full resume flow against a live API.

Run with:  API_BASE_URL=http://api:8000 python -m app.verify
Exit code 0 means every check passed.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
CHUNK_SIZE = 1024 * 1024
FILE_SIZE = CHUNK_SIZE * 4 + 12345  # 5 chunks, last one short


class VerifyFailure(Exception):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise VerifyFailure(message)


def payload(size: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"verify-payload:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def wait_for_api(client: httpx.Client, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise VerifyFailure(f"API at {BASE_URL} did not become healthy within {timeout:.0f}s")


def expect_error(resp: httpx.Response, status: int, code: str) -> dict:
    check(resp.status_code == status, f"expected HTTP {status}, got {resp.status_code}: {resp.text}")
    body = resp.json()
    check(isinstance(body.get("error"), dict), f"error envelope missing: {body}")
    check(
        body["error"].get("code") == code,
        f"expected error code {code}, got {body['error'].get('code')}",
    )
    return body


def put_chunk(client: httpx.Client, sid: str, index: int, body: bytes, digest: str) -> httpx.Response:
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers={"X-Chunk-SHA256": digest})


def main() -> int:
    started = time.monotonic()
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
        wait_for_api(client)
        print(f"[verify] API healthy at {BASE_URL}")

        data = payload(FILE_SIZE)
        file_sha = sha256(data)
        chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
        digests = [sha256(c) for c in chunks]

        resp = client.post(
            "/sessions",
            json={
                "file_size": FILE_SIZE,
                "chunk_size": CHUNK_SIZE,
                "file_sha256": file_sha,
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
            },
        )
        check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
        session = resp.json()
        sid = session["session_id"]
        check(session["total_chunks"] == len(chunks), "total_chunks mismatch")
        check(session["missing_chunks"] == list(range(len(chunks))), "fresh session must miss every chunk")
        print(f"[verify] session {sid} created ({len(chunks)} chunks, {FILE_SIZE} bytes)")

        # Invalid chunks must be rejected and never recorded.
        expect_error(put_chunk(client, sid, 0, chunks[0], digests[1]), 400, "CHUNK_DIGEST_MISMATCH")
        expect_error(put_chunk(client, sid, len(chunks), chunks[0], digests[0]), 400, "CHUNK_INDEX_OUT_OF_RANGE")
        truncated = chunks[0][:-1]
        expect_error(put_chunk(client, sid, 0, truncated, sha256(truncated)), 400, "CHUNK_SIZE_MISMATCH")
        status = client.get(f"/sessions/{sid}").json()
        check(status["received_count"] == 0, "rejected chunks must not be recorded")
        print("[verify] digest/size/index violations rejected and not recorded")

        # Simulate a dropped connection: only the first 3 chunks go out.
        for i in range(3):
            resp = put_chunk(client, sid, i, chunks[i], digests[i])
            check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")
        status = client.get(f"/sessions/{sid}").json()
        check(status["missing_chunks"] == [3, 4], f"expected missing [3, 4], got {status['missing_chunks']}")
        print("[verify] partial upload visible after 'interruption': missing [3, 4]")

        # Idempotent replay of the same bytes; conflicting bytes must get 409.
        resp = put_chunk(client, sid, 1, chunks[1], digests[1])
        check(resp.status_code == 200 and resp.json()["duplicate"] is True,
              f"idempotent replay failed: {resp.status_code} {resp.text}")
        other = bytes(len(chunks[1]))  # same length, different content
        expect_error(put_chunk(client, sid, 1, other, sha256(other)), 409, "CHUNK_CONFLICT")
        print("[verify] idempotent replay accepted, conflicting content rejected with 409")

        # Finalize too early must fail and list the missing chunks.
        body = expect_error(client.post(f"/sessions/{sid}/finalize"), 409, "CHUNKS_INCOMPLETE")
        check(body["error"]["details"]["missing_chunks"] == [3, 4], "finalize error must list missing chunks")

        # Resume: upload the remaining chunks.
        for i in (3, 4):
            resp = put_chunk(client, sid, i, chunks[i], digests[i])
            check(resp.status_code == 201, f"resume chunk {i} failed: {resp.text}")
        status = client.get(f"/sessions/{sid}").json()
        check(status["missing_chunks"] == [], "all chunks should be received now")
        print("[verify] resumed upload completed the bitmap")

        # Finalize, re-finalize (idempotent), download and verify the artifact.
        resp = client.post(f"/sessions/{sid}/finalize")
        check(resp.status_code == 200, f"finalize failed: {resp.text}")
        check(resp.json()["final_sha256"] == file_sha, "final SHA-256 mismatch")
        again = client.post(f"/sessions/{sid}/finalize")
        check(again.status_code == 200 and again.json()["final_sha256"] == file_sha,
              "finalize must be idempotent")
        resp = client.get(f"/sessions/{sid}/artifact")
        check(resp.status_code == 200, f"artifact download failed: {resp.text}")
        check(resp.content == data, "artifact bytes differ from the original file")
        check(resp.headers.get("x-file-sha256") == file_sha, "artifact digest header mismatch")
        status = client.get(f"/sessions/{sid}").json()
        check(status["status"] == "completed", "session must be completed")
        print(f"[verify] artifact published and verified (sha256={file_sha[:16]}...)")

    print(f"[verify] ALL CHECKS PASSED in {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except VerifyFailure as exc:
        print(f"[verify] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPError as exc:
        print(f"[verify] FAILED: HTTP error: {exc}", file=sys.stderr)
        sys.exit(1)
