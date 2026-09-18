# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL`、`busy_timeout`、**写事务一律 `BEGIN IMMEDIATE`**）
  持久化：会话、每片 SHA-256、接收位图（BLOB）、全局池记录、回收周期/游标/候选
- **全局内容寻址分片池**：新分片经长度与 SHA-256 校验后以 `(sha256, size)` 为键，
  正文全局只保留一份（硬链接共享）；不同会话只持有独立的逻辑分片引用
- 分片正文发布分三阶段（`reserve → 落正文 → finalize`），引用与目标会话位图**同一事务**
- 成品发布：直接顺序读池正文/未提升旧片 → fsync → 校验整文件 SHA-256 → `os.replace` 原子改名
- Compose 中 API 以 **2 个独立 worker 进程**共享同一 SQLite 与数据卷；正确性只依赖
  数据库写事务串行化与同卷原子改名，**不依赖任何进程内锁**

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000（API 内为 2 worker）
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传 + 全局池流程，退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传 + 内容池/回收/旧卷升级边界测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
# 本地多 worker 验证（与 Compose 等价）：
DATA_DIR=./data uvicorn app.asgi:app --port 8000 --workers 2
```

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions`（位图 BLOB、整文件 SHA-256、过期时间）、`chunks`（每片 SHA-256、大小、逻辑引用 `pool_sha256`）、`pool_blobs`（池记录：`publishing`/`available`/`sealed`）、`reclaim_cycles` / `reclaim_progress` / `reclaim_candidates`（持久回收状态） |
| `pool/ab/<sha256>.blob` | **全局唯一**的分片正文，按摘要前两位分桶；多会话经硬链接共享同一份 |
| `pool/.staging/<uuid>.tmp` | 上传临时正文，校验通过后才硬链接入池；过期残留由启动收敛清理 |
| `pool/.trash/<sha256>.<cycle>.trash` | 回收封存副本（带周期 token，避免与重新发布的新正文互删） |
| `chunks/<session_id>/00000000.chunk` | **旧版本布局**，仅为兼容旧卷保留；访问时惰性提升进池后删除 |
| `artifacts/<session_id>.bin` | 校验通过后原子发布的成品，完成会话唯一依赖 |

### 并发裁决（上传 / 复用 / 回收 / 组装）

- **同一会话不同序号并发上传**：每个写事务内**重读当前位图**再置位，杜绝 lost update；
  引用插入与位图更新在同一 `BEGIN IMMEDIATE` 事务。
- **跨会话并发上传相同正文**：`pool_blobs` 以 `sha256` 为主键，`reserve` 事务把并发
  请求串行化为“一个发布者 + 其余复用者”，磁盘上只有一个 `<sha256>.blob`。
- **复用 vs 回收**：二者只在写事务内判定。复用先取得引用 → 回收扫描发现被钉住而跳过；
  回收先把 `available` 改 `sealed`（提交后把正文原子改名到 `.trash`）→ 复用只见
  `sealed`，返回结构化 `CHUNK_NOT_CACHED`（`reason="reclaiming"`），位图不变。
- **封存后又出现携带正文的后来者**（上传/旧卷提升）：`sealed → publishing → available`
  重新发布，回收在删除提交前复查状态并把封存副本归位；任一步崩溃都由启动恢复收敛。
- **钉住规则**：完成会话由原子成品独立承载，其分片**不**钉住池文件；活动会话与
  过期未完成会话的引用钉住正文。

### 崩溃收敛（任意时刻进程退出）

启动 `reconcile` 只处理**持久化中间态**，绝不遍历或重新散列整个池：

1. 收敛过期租约的回收候选（活跃回收者窗口内的候选绝不触碰）：
   正文与记录都在 → 归位为 `available`；记录已删 → 清残留 `.trash`；仍 `sealed`
   且无引用 → 续完删除；
2. 收敛残留的 `publishing` 行：正文就位（仅核尺寸）→ 置 `available`；
   超宽限且无正文 → 删行；
3. 逐会话核对 `chunks` 行：旧分片缺失/尺寸不符按既有规则撤销确认并重算位图；
   池引用缺正文但**池记录仍在**（恢复在途）时保留确认，绝不产生悬空引用；
4. 清理孤儿旧分片、残留 `.tmp`/`.staging`、过期 `.trash`。

因此磁盘上只可能收敛到两种终态：**可用正文 + 完整引用**，或**正文与池记录均不存在**。

### 旧卷升级（惰性，不整池重散列）

旧 `chunks/<sid>/<index>.chunk` 与旧 `chunks` 表记录在**被访问时**（如 `finalize`）
才提升：先在事务外重新核对尺寸**与** SHA-256，再在写事务内 `reserve → 硬链接入池 →
finalize` 并把引用改指池正文；**事务提交且目录 fsync 完成后才删除旧文件**。两个 worker
同时提升同一内容时，由同一写事务串行化，仍只产生一个池文件。旧分片缺失/同尺寸损坏
分别在启动核对与访问重验时按既有规则撤销该片确认。已完成会话只依赖原子成品，历史
分片被回收也不会退回未完成。

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

### 6. 无正文复用确认 `POST /sessions/{session_id}/chunks/{index}/reuse`

请求体**仅含小写 SHA-256**，不发送分片字节：当全局池中已存在“完成长度+SHA-256
校验、状态可用、尺寸符合目标序号”的正文时，直接为本会话确认该片。

```bash
curl -s -X POST "http://localhost:8000/sessions/$SID/chunks/0/reuse" \
  -H 'Content-Type: application/json' \
  -d '{"sha256": "9f86d081…(64 位小写十六进制)"}'
```

- 成功新确认：`201`（`"duplicate": false`），响应体与上传收据一致；磁盘不新增正文
- 目标序号已是相同内容：**幂等成功** `200`，`"duplicate": true`
- 目标序号已是不同内容：沿用现有 `409 CHUNK_CONFLICT`
- 池中无该摘要 / 摘要对应正文正被回收（`sealed`）/ 尺寸不符：`409 CHUNK_NOT_CACHED`，
  `details.reason` 分别为 `unknown` / `reclaiming` / `size_mismatch`，**位图与引用不变**
- 会话过期或已完成：不得借复用接口增加进度 —— 已确认片的幂等重放仍 `200`，
  新增片按现有语义 `410 SESSION_EXPIRED` / `409 SESSION_ALREADY_COMPLETED`
- 请求体不是 64 位**小写**十六进制：`422 VALIDATION_ERROR`

```json
{"error": {"code": "CHUNK_NOT_CACHED", "message": "…", "details": {
  "chunk_index": 0, "sha256": "…", "expected_size": 1048576, "reason": "unknown"}}}
```

### 7. 分片池回收 `POST /maintenance/chunk-pool/reclaim`

```bash
curl -s -X POST http://localhost:8000/maintenance/chunk-pool/reclaim \
  -H 'Content-Type: application/json' -d '{"max_blobs": 100}'
```

`max_blobs` 取值 `1..1000`，每次调用**最多**检查这么多个候选，绝不一次性加载/遍历
整个池。返回持久化的回收进度：

```json
{"cycle_id": "e0ff8a74…", "cursor": "db2e7f1b…", "examined": 5, "deleted": 3, "done": false}
```

- `cycle_id` 持久标识一个回收周期；多次调用（含重启后）在该周期内从持久 `cursor`
  继续扫描并累计 `examined`/`deleted`；扫完全部池正文后 `done=true`
- 完成会话不再钉住池文件，活动/过期未完成会话钉住；每次调用结束优雅释放租约，
  崩溃遗留的候选待租约过期后由启动恢复收敛
- 回收进行两阶段：写事务内 `available → sealed` 并记候选（提交后正文原子改名到
  `.trash`），删除提交前再次裁决复用竞争 —— 复用先取引用则跳过并归位，回收先封存
  则复用得到 `CHUNK_NOT_CACHED`；删除提交后才移除封存副本
- 再次回收（`done` 已真的周期之后）开启新周期，此时池中无候选则 `examined=0`、
  `done=true`

### 8. 健康检查 `GET /healthz`

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
| 409 | `CHUNK_NOT_CACHED` | 池中无可用/相符正文（`unknown`/`reclaiming`/`size_mismatch`），位图不变 |
| 409 | `CHUNKS_INCOMPLETE` | 尚有缺片，不能发布 |
| 409 | `SESSION_ALREADY_COMPLETED` | 会话已完成，不可再写新分片 |
| 409 | `ARTIFACT_NOT_READY` | 成品尚未发布 |
| 410 | `SESSION_EXPIRED` | 会话已过期，拒绝新分片/新增复用 |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败（含复用摘要非小写、`max_blobs` 越界） |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 测试

`pytest` 共 38 个用例，除原有续传边界（建档参数校验、乱序上传、摘要/尺寸/越界拒绝
且不入账、同内容幂等重传、异内容 409 不污染进度、缺片升序、发布成功与幂等、完整性
失败保留现场、过期拒绝新分片、重启续作、孤儿分片与临时文件清理、结构化错误形态）外，
新增对真实磁盘/真实 SQLite 的覆盖：

- 跨会话相同正文全局只存一份、多 worker（双 app 共享卷）并发上传同一正文只有一个池文件；
- 同会话同序号并发的幂等/位图不丢失；无正文复用成功、未知/回收中/尺寸不符返回
  `CHUNK_NOT_CACHED` 且位图/引用零污染、幂等重放与 409、过期会话不能借复用增加进度；
- 回收：活动/过期未完成钉住、完成会话解钉、`max_blobs` 分页与持久 `cycle_id`/游标在
  重启后续扫、回收后成品仍可下载；
- 旧卷：按旧布局真实建卷，惰性提升（提升前重验尺寸与摘要）、缺片撤销确认、同尺寸
  损坏在访问时撤销、双 worker 并发提升同一内容只产生一个池文件；
- 崩溃收敛：回收删除前后任意中断 → 重启收敛到“正文与记录均不存在”或“可用正文+完整引用”。

`verify` 验收服务对**真实 4 MiB 分片**跑完整流程，并构造跨两个 worker 的并发相同上传、
零正文复用、过期复用拒绝与分批回收。
