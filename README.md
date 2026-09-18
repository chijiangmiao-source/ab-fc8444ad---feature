# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

分片正文保存在**全局内容寻址分片池**中：同一份内容（`(sha256, size)`）在磁盘上只有一份，
不同会话各自持有独立的逻辑分片引用；已入池的内容可以**免正文复用**确认分片；
无引用的池正文由**回收循环**安全清理。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL` + `busy_timeout`）持久化：会话、每片 SHA-256、
  接收位图（BLOB）、池正文表、回收游标
- 多分片正文落盘：先写 `.tmp` 并 fsync，硬链接入池并 fsync 目录后，才在同一事务里入账
- 成品发布：按序组装 → fsync → 校验整文件 SHA-256 → `os.replace` 原子改名
- Compose 中 API 以 **2 个 uvicorn worker** 运行，共享同一 SQLite 与数据卷；
  全部并发裁决由 SQLite 事务与比较-交换（CAS）完成，**不依赖进程内锁**

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000，容器内 2 个 worker
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传 + 分片池流程，退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传边界 + 分片池测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
```

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions`（接收位图 BLOB、整文件 SHA-256、过期时间）、`chunks`（每片 SHA-256、大小、正文路径、池引用）、`pool_blobs`（池正文：路径、状态、引用计数）、`reclaim_state`（回收游标与周期序号） |
| `pool/blobs/<sha256>-<size>-<id>.blob` | 池正文，全局唯一一份；文件名带唯一后缀，同一内容的不同"世代"绝不复用同一路径 |
| `pool/tmp/.<uuid>.tmp` | 上传暂存（校验通过前不可见） |
| `chunks/<session_id>/00000000.chunk` | **旧版**按会话保存的分片正文（升级前的数据卷；新上传一律入池） |
| `artifacts/<session_id>.bin` | 校验通过后原子发布的成品 |

进程启动时执行 `reconcile`：

- **会话**：丢弃文件缺失或尺寸不符的 chunks 行并释放其池引用、删除未入账的孤儿分片与
  残留 `.tmp`、按存活行重建位图 —— **已确认分片绝不误判为缺失，未确认字节绝不误判为已收**。
  已完成会话只认原子成品，其历史分片即使已被回收也**不会退回未完成**。
- **分片池**：只处理持久化中间态，**不重新散列**任何正文——
  `sealed` 行继续完成删除（文件与池记录同归于无）、超时 `writing` 行（发布者崩溃）连同行与文件清除、
  超时孤儿文件与暂存清理、引用计数按 chunks 行重建。所有文件删除都带 mtime 宽限，
  **另一个正在发布的 worker 的正文不会被启动恢复误删**。

## 并发裁决（多 worker 正确性）

所有跨进程不变量都由 SQLite 保证，进程内 `RLock` 只保护本进程连接对象：

- **写事务一律 `BEGIN IMMEDIATE`**：分片引用插入、位图翻转、引用计数增减在同一事务提交；
  两个 worker 的写事务被 SQLite 串行化。
- **发布仲裁**：`(sha256, size)` 主键 + `INSERT OR IGNORE`（`writing` 状态）决定唯一发布者；
  落选者复用已发布的池文件。崩溃的发布者留下 `writing` 行，由超时接管/启动恢复收敛。
- **复用 ↔ 回收栅栏**：复用在写事务内核对 `state='available'` 后才建立引用（引用计数 +1）；
  回收以 `state='available' AND ref_count=0` 的 CAS 封存候选。二者必居其一：
  复用先提交 → 回收跳过；回收先封存 → 复用得到 `CHUNK_NOT_CACHED`。
- **池路径按"世代"唯一**：重新发布同一内容使用新路径，回收删除旧路径与再发布永不互相踩踏。
- 可串行结果：**任何已返回确认的活动或过期未完成会话都不会引用已删除文件**；
  失败请求不留下位图、引用计数或临时文件污染（被拒绝上传可能留下的无引用池正文由回收循环清理）。

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

- 新片入库：`201`，响应含 `"duplicate": false`；正文经长度与 SHA-256 校验后以
  `(sha256, size)` 为键入全局池，同一内容磁盘上只保留一份
- **同序号同内容重传：幂等成功** `200`，`"duplicate": true`，状态不变
- **同序号不同内容：`409 CHUNK_CONFLICT`**，已确认分片不被覆盖
- 摘要与正文不符 `400 CHUNK_DIGEST_MISMATCH`、长度不符 `400 CHUNK_SIZE_MISMATCH`、
  序号越界 `400 CHUNK_INDEX_OUT_OF_RANGE` —— 均**不记入**任何状态
- 会话过期后新分片一律 `410 SESSION_EXPIRED`（已确认分片的幂等重放仍是 `200`）

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
- 到齐后**直接顺序读取池正文或尚未提升的旧分片**组装（不为每个会话复制整套分片），
  校验整文件 SHA-256：
  - 一致 → 原子发布，`200` 返回 `final_sha256` 与 `artifact_url`；重复调用幂等
  - 不一致 → `422 INTEGRITY_MISMATCH`（`details` 含双方摘要与尺寸），**已上传分片全部保留**
- 完成是会话级状态切换：成品独立承载内容，该会话的分片引用在同一事务里解除对池正文的钉住

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

### 6. 复用分片 `POST /sessions/{session_id}/chunks/{index}/reuse`

请求体仅含小写 SHA-256：`{"sha256": "9f86d081…"}`。仅当全局池中存在**已完成校验、
状态可用、尺寸符合该序号**的正文时，无需上传正文即可确认该片：

```bash
curl -s -X POST "http://localhost:8000/sessions/$SID/chunks/0/reuse" \
  -H 'Content-Type: application/json' \
  -d '{"sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}'
```

- 确认成功：`201`（`"duplicate": false`），响应字段与上传一致
- 目标序号已有相同内容：**幂等成功** `200`，`"duplicate": true`（过期会话也允许这种重放）
- 目标序号已有不同内容：`409 CHUNK_CONFLICT`
- 内容未知、仍在写入或正在回收：`404 CHUNK_NOT_CACHED`，**位图不变**
- 会话过期：`410 SESSION_EXPIRED`（不得借复用增加进度）；已完成：`409 SESSION_ALREADY_COMPLETED`
- 请求体不是 64 位小写十六进制：`422 VALIDATION_ERROR`

### 7. 回收 `POST /maintenance/chunk-pool/reclaim`

请求体 `{"max_blobs": N}`（`1 ≤ N ≤ 1000`）。每次调用从**持久化游标**起最多检查
`max_blobs` 个候选（不一次加载或遍历整个池），重启后从游标继续：

```bash
curl -s -X POST http://localhost:8000/maintenance/chunk-pool/reclaim \
  -H 'Content-Type: application/json' \
  -d '{"max_blobs": 100}'
```

```json
{"cycle_id": 7, "cursor": 0, "examined": 100, "deleted": 12, "done": true}
```

- `cycle_id`：持久化的周期序号，单调递增，重启不复位
- `cursor`：本周期结束后的持久游标（扫到末尾归 `0`，下轮从头开始）
- `examined`：本周期检查的候选数（≤ `max_blobs`）
- `deleted`：实际删除的池正文数
- `done`：本周期是否扫到池尾

**钉住规则**：活动会话与过期未完成会话仍钉住其正文；已完成会话由成品独立承载，
其分片不再钉住。回收只删除 `available` 且引用计数为 0 的正文。

**回收状态机与崩溃收敛**：候选先被 CAS 封存为 `sealed`（并发栅栏：此后复用一律
`CHUNK_NOT_CACHED`），再删文件、最后删池记录。文件删除前后任一时刻进程退出，
重启都会收敛到"可用正文且引用完整"或"正文与池记录均不存在"，绝不出现悬空引用。

### 8. 健康检查 `GET /healthz`

返回 `{"status": "ok"}`，供 Compose healthcheck 与验收客户端使用。

## 旧数据卷升级

升级兼容现有数据卷，无需迁移窗口：

- 旧 `chunks/<session_id>/<index>.chunk` 文件与旧 chunks 表记录**按需惰性提升**入全局池
  （会话再次上传该片、或对旧记录做复用重放时触发）；提升前**重新核对尺寸与摘要**，
  两个 worker 同时提升同一内容仍只产生一个池文件；引用提交且目录 fsync 完成后才删除旧文件。
- 旧分片缺失或损坏：按既有规则撤销该片确认（启动 reconcile 核对尺寸；访问时核对摘要），
  会话可重新上传该片自愈。
- 已完成会话只依赖原子成品：不因其历史分片已被回收而退回未完成，位图与成品下载不受影响。
- 启动**不会**重新散列整个池，只处理持久化中间态及被访问的旧分片。
- 数据库结构自动迁移（新增列与表均为幂等 DDL），多 worker 同时启动不会互相干扰。

## 错误码一览

| HTTP | code | 含义 |
|---|---|---|
| 400 | `CHUNK_INDEX_OUT_OF_RANGE` | 序号非整数或超出 `0..total-1` |
| 400 | `INVALID_CHUNK_DIGEST` | `X-Chunk-SHA256` 不是 64 位十六进制 |
| 400 | `CHUNK_SIZE_MISMATCH` | 分片长度不等于该片应有长度 |
| 400 | `CHUNK_DIGEST_MISMATCH` | 正文 SHA-256 与声明摘要不符 |
| 404 | `SESSION_NOT_FOUND` | 会话不存在 |
| 404 | `CHUNK_NOT_CACHED` | 复用的内容不在池中（未知、写入中或正在回收），位图不变 |
| 404 | `NOT_FOUND` | 路由不存在 |
| 409 | `CHUNK_CONFLICT` | 同序号不同内容，已确认分片不变 |
| 409 | `CHUNKS_INCOMPLETE` | 尚有缺片，不能发布 |
| 409 | `SESSION_ALREADY_COMPLETED` | 会话已完成，不可再写新分片 |
| 409 | `ARTIFACT_NOT_READY` | 成品尚未发布 |
| 410 | `SESSION_EXPIRED` | 会话已过期，拒绝新分片 |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败 |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 测试

`pytest` 覆盖续传边界（建档参数校验、乱序上传、摘要/尺寸/越界拒绝且不入账、
同内容幂等重传、异内容 409 不污染进度、缺片状态升序、发布成功与幂等、
完整性失败保留现场、过期拒绝新分片、**重启后从已确认位置续作**、结构化错误形态）
与全局分片池（跨会话复用与幂等/冲突/未缓存、同一内容磁盘唯一、双连接并发发布仲裁、
回收只删无引用正文、游标分页与跨重启续扫、过期会话钉住、旧卷惰性提升与损坏撤销、
启动对池中间态的收敛）——全部使用真实数据断言，而非固定响应。

`verify` 验收客户端在真实服务上跑完整流程：续传、幂等、冲突、发布、跨会话复用、
回收（含钉住保护与 `cycle_id` 持久化）、过期会话复用语义，可重复运行于同一数据卷。
