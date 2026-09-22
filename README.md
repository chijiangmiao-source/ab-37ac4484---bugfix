# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL`）持久化：会话、每片 SHA-256、接收位图（BLOB）
- 分片正文落盘：先写 `.tmp` 并 fsync，`os.replace` 就位后才在同一事务里入账
- 成品发布：按序组装 → fsync → 校验整文件 SHA-256 → `os.replace` 原子改名

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传流程，退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传边界测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
```

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions` 表（含接收位图 BLOB、整文件 SHA-256、过期时间）与 `chunks` 表（每片 SHA-256、大小、落盘路径） |
| `chunks/<session_id>/00000000.chunk` | 分片正文，序号从 0 开始、8 位零填充命名 |
| `artifacts/<session_id>.bin` | 校验通过后原子发布的成品 |

进程启动时执行 `reconcile`：丢弃文件缺失或尺寸不符的 chunks 行、删除未入账的孤儿分片与
残留 `.tmp`、按存活行重建位图 —— **已确认分片绝不误判为缺失，未确认字节绝不误判为已收**。

## 接口约定

- 分片总数 = `ceil(file_size / chunk_size)`，序号从 `0` 开始
- 除最后一片外，每片长度必须等于 `chunk_size`；最后一片为剩余字节（`file_size - chunk_size × (total-1)`）
- 所有错误均为结构化 JSON：`{"error": {"code": "...", "message": "...", "details": {...}}}`

### 1. 建档 `POST /sessions`

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{
        "file_size": 4206649,
        "chunk_size": 1048576,
        "file_sha256": "b5fa3134…(整文件 SHA-256，64 位十六进制)",
        "expires_at": "2026-09-18T12:00:00+00:00"
      }'
```

`201` 响应（`total_chunks = ceil(4206649 / 1048576) = 5`）：

```json
{
  "session_id": "029c773e…",
  "status": "active",
  "file_size": 4206649,
  "chunk_size": 1048576,
  "total_chunks": 5,
  "file_sha256": "b5fa3134…",
  "received_count": 0,
  "missing_chunks": [0, 1, 2, 3, 4],
  "expires_at": "2026-09-18T12:00:00+00:00",
  "created_at": "2026-09-18T03:31:21.684930+00:00",
  "completed_at": null,
  "final_sha256": null,
  "artifact_url": null
}
```

`expires_at` 必须带时区且晚于当前时间，否则 `422 SESSION_EXPIRES_IN_PAST` / `422 VALIDATION_ERROR`。

### 2. 上传分片 `PUT /sessions/{session_id}/chunks/{index}`

请求体为分片原始字节，请求头 `X-Chunk-SHA256` 携带该片摘要：

```bash
CHUNK_SHA=$(sha256sum part0.bin | cut -d' ' -f1)
curl -s -X PUT "http://localhost:8000/sessions/$SID/chunks/0" \
  -H "X-Chunk-SHA256: $CHUNK_SHA" \
  --data-binary @part0.bin
```

- 新片入库：`201`，响应含 `"duplicate": false`
- **同序号同内容重传：幂等成功** `200`，`"duplicate": true`，状态不变
- **同序号不同内容：`409 CHUNK_CONFLICT`**，已确认分片不被覆盖
- 摘要与正文不符 `400 CHUNK_DIGEST_MISMATCH`、长度不符 `400 CHUNK_SIZE_MISMATCH`、
  序号越界 `400 CHUNK_INDEX_OUT_OF_RANGE` —— 均**不记入**任何状态
- 会话过期后新分片一律 `410 SESSION_EXPIRED`（已确认分片的幂等重放仍是 `200`）

#### 并发上传同一分片

只有当分片正文**长度与摘要均校验通过、已原子落盘且完成数据库入账（位图置位）**
后，该分片才计入 `received_count`，也才允许任何请求得到 2xx。分片仍在传输
（正文未完整落盘）期间：

- 状态接口与 finalize 一律把它视为缺片，两个接口结论永远一致；
- 携带完整正确正文的并发重试不会提前成功：服务先把其正文缓冲到独立临时文件，
  再等待在途请求结束（默认最多 30 s，可用 `INFLIGHT_WAIT_TIMEOUT` 调整）：
  - 在途请求成功 → 等待方收到幂等 `200 duplicate=true`（其缓冲正文被丢弃）；
  - 在途请求失败（摘要/长度不符或连接中断）→ 等待方已缓冲的正确正文直接被
    采纳为正式分片并返回 `201`，客户端无需重发；
  - 等待超时 → `409 CHUNK_UPLOAD_IN_PROGRESS`（结构化响应，含
    `Retry-After: 1`），客户端稍后按状态接口补传即可。
- 在途请求失败后，合法重试始终可以继续上传、完成发布。

```json
{
  "session_id": "029c773e…",
  "chunk_index": 0,
  "size": 1048576,
  "sha256": "9f86d081…",
  "duplicate": false,
  "received_count": 1,
  "total_chunks": 5
}
```

### 3. 状态 `GET /sessions/{session_id}`

```bash
curl -s http://localhost:8000/sessions/$SID
```

`missing_chunks` 为**升序**缺片序号，检测设备据此补传；`status` 为
`active` / `expired` / `completed`。

### 4. 发布 `POST /sessions/{session_id}/finalize`

```bash
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

- 缺片：`409 CHUNKS_INCOMPLETE`（`details.missing_chunks` 列出缺片）；若已过期则 `410 SESSION_EXPIRED`
- 到齐后按序组装并校验整文件 SHA-256：
  - 一致 → 原子发布，`200` 返回 `final_sha256` 与 `artifact_url`；重复调用幂等
  - 不一致 → `422 INTEGRITY_MISMATCH`（`details` 含双方摘要与尺寸），**已上传分片全部保留**

```json
{
  "session_id": "029c773e…",
  "status": "completed",
  "file_size": 4206649,
  "final_sha256": "b5fa3134…",
  "artifact_size": 4206649,
  "artifact_url": "/sessions/029c773e…/artifact",
  "completed_at": "2026-09-18T03:31:21.731585+00:00"
}
```

### 5. 下载成品 `GET /sessions/{session_id}/artifact`

```bash
curl -s -OJ http://localhost:8000/sessions/$SID/artifact
sha256sum 029c773e….bin   # 与建档 file_sha256 比对即可复核
```

响应头 `X-File-SHA256` 附带成品摘要；未发布时 `409 ARTIFACT_NOT_READY`。

### 6. 健康检查 `GET /healthz`

返回 `{"status": "ok"}`，供 Compose healthcheck 与验收客户端使用。

## 错误码一览

| HTTP | code | 含义 |
|---|---|---|
| 400 | `CHUNK_INDEX_OUT_OF_RANGE` | 序号非整数或超出 `0..total-1` |
| 400 | `INVALID_CHUNK_DIGEST` | `X-Chunk-SHA256` 不是 64 位十六进制 |
| 400 | `CHUNK_SIZE_MISMATCH` | 分片长度不等于该片应有长度 |
| 400 | `CHUNK_DIGEST_MISMATCH` | 正文 SHA-256 与声明摘要不符 |
| 404 | `SESSION_NOT_FOUND` | 会话不存在 |
| 404 | `NOT_FOUND` | 路由不存在 |
| 409 | `CHUNK_CONFLICT` | 同序号不同内容，已确认分片不变 |
| 409 | `CHUNK_UPLOAD_IN_PROGRESS` | 同序号分片仍有请求在途，等待超时；按 `Retry-After` 重试 |
| 409 | `CHUNKS_INCOMPLETE` | 尚有缺片，不能发布 |
| 409 | `SESSION_ALREADY_COMPLETED` | 会话已完成，不可再写新分片 |
| 409 | `ARTIFACT_NOT_READY` | 成品尚未发布 |
| 410 | `SESSION_EXPIRED` | 会话已过期，拒绝新分片 |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败 |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 测试

`pytest` 覆盖续传边界：建档参数校验、乱序上传、摘要/尺寸/越界拒绝且不入账、
同内容幂等重传、异内容 409 不污染进度、缺片状态升序、发布成功与幂等、
完整性失败保留现场、过期拒绝新分片、重启后从已确认位置续作（含孤儿分片与
残留临时文件清理）、结构化错误形态；`test_concurrent_upload.py` 另起真实
uvicorn、用两条独立 TCP 连接与可控流式屏障验证：暂停期间状态/finalize 一致
报缺片、并发正确重试不会提前成功（等待接管或结构化在途响应）、失败后重试
最终发布出与原文一致的成品，以及传输中止并重启后未完成请求不污染持久化进度。
