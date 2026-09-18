"""全局内容寻址分片池：去重、复用、回收栅栏、旧卷惰性提升与崩溃收敛。

这些测试使用真实磁盘与真实 SQLite（部分场景用两个共享数据卷的 app
实例模拟多 worker），不使用固定响应。
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app import clock
from app.config import Settings
from app.main import create_app
from tests.test_upload_api import chunk, create_session, make_bytes, put_chunk, sha256


# ---------- 内容去重 ----------

def test_identical_body_stored_once_and_shared(data_dir):
    payload = make_bytes(10)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c:
        s1 = create_session(c, payload, 4)["session_id"]
        s2 = create_session(c, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)
        assert put_chunk(c, s1, 0, part).status_code == 201
        assert put_chunk(c, s2, 0, part).status_code == 201
        dig = sha256(part)
        blobs = list((data_dir / "pool").rglob("*.blob"))
        assert len(blobs) == 1
        assert blobs[0].name == f"{dig}.blob"
        # 两个会话的 chunks 行都逻辑指向同一池正文（硬链接共享 inode）。
        db = sqlite3.connect(data_dir / "db.sqlite3")
        rows = db.execute(
            "SELECT session_id, pool_sha256 FROM chunks WHERE chunk_index = 0"
        ).fetchall()
        db.close()
        assert {r[0] for r in rows} == {s1, s2}
        assert {r[1] for r in rows} == {dig}


def test_concurrent_upload_same_body_single_publisher(data_dir):
    """两个 worker（各自 SQLite 连接）并发上传同一正文到两个会话。"""
    payload = make_bytes(4)
    settings = Settings(data_dir=data_dir)
    app1 = create_app(settings)
    app2 = create_app(settings)
    with TestClient(app1) as c1, TestClient(app2) as c2:
        s1 = create_session(c1, payload, 4)["session_id"]
        s2 = create_session(c2, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)
        barrier = threading.Barrier(2)

        def upload(client, sid):
            barrier.wait()
            r = put_chunk(client, sid, 0, part)
            return r.status_code

        with ThreadPoolExecutor(max_workers=2) as pool:
            codes = list(pool.map(upload, (c1, c2), (s1, s2)))
        assert sorted(codes) == [201, 201], codes
        blobs = list((data_dir / "pool").rglob("*.blob"))
        assert len(blobs) == 1
        for client, sid in ((c1, s1), (c2, s2)):
            assert client.get(f"/sessions/{sid}").json()["missing_chunks"] == []
            assert client.post(f"/sessions/{sid}/finalize").status_code == 200
            assert client.get(f"/sessions/{sid}/artifact").content == payload


def test_concurrent_upload_same_session_same_index_is_idempotent(data_dir):
    payload = make_bytes(4)
    settings = Settings(data_dir=data_dir)
    app1 = create_app(settings)
    app2 = create_app(settings)
    with TestClient(app1) as c1, TestClient(app2) as c2:
        sid = create_session(c1, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)
        barrier = threading.Barrier(2)

        def upload(client):
            barrier.wait()
            return put_chunk(client, sid, 0, part).status_code

        with ThreadPoolExecutor(max_workers=2) as ex:
            codes = list(ex.map(upload, (c1, c2)))
        assert sorted(codes) == [200, 201], codes
        assert len(list((data_dir / "pool").rglob("*.blob"))) == 1


# ---------- reuse ----------

def test_reuse_confirms_without_body(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        s1 = create_session(c, payload, 4)["session_id"]
        s2 = create_session(c, payload, 4)["session_id"]
        part = chunk(payload, 4, 1)
        dig = sha256(part)
        assert put_chunk(c, s1, 1, part).status_code == 201

        resp = c.post(f"/sessions/{s2}/chunks/1/reuse", json={"sha256": dig})
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["duplicate"] is False
        assert body["sha256"] == dig
        assert body["chunk_index"] == 1
        assert body["received_count"] == 1
        status = c.get(f"/sessions/{s2}").json()
        assert status["missing_chunks"] == [0, 2]
        # 磁盘上正文仍只有一份
        assert len(list((data_dir / "pool").rglob("*.blob"))) == 1


def test_reuse_unknown_and_size_mismatch_leave_bitmap(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        s1 = create_session(c, payload, 4)["session_id"]
        s2 = create_session(c, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)  # 4 字节正文
        dig = sha256(part)
        assert put_chunk(c, s1, 0, part).status_code == 201

        # 未知摘要
        unknown = sha256(b"never-uploaded")
        resp = c.post(f"/sessions/{s2}/chunks/0/reuse", json={"sha256": unknown})
        assert resp.status_code == 409
        err = resp.json()["error"]
        assert err["code"] == "CHUNK_NOT_CACHED"
        assert err["details"]["reason"] == "unknown"
        assert err["details"]["chunk_index"] == 0

        # 尺寸不符：index 2 是末片，期望 2 字节，池正文是 4 字节
        resp = c.post(f"/sessions/{s2}/chunks/2/reuse", json={"sha256": dig})
        assert resp.status_code == 409
        assert resp.json()["error"]["details"]["reason"] == "size_mismatch"

        # 位图与引用计数均未被污染
        assert c.get(f"/sessions/{s2}").json()["missing_chunks"] == [0, 1, 2]
        db = sqlite3.connect(data_dir / "db.sqlite3")
        n = db.execute(
            "SELECT COUNT(*) FROM chunks WHERE session_id = ?", (s2,)
        ).fetchone()[0]
        db.close()
        assert n == 0


def test_reuse_idempotent_replay_and_conflict(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        sid = create_session(c, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)
        dig = sha256(part)
        assert put_chunk(c, sid, 0, part).status_code == 201
        # 同内容幂等重放（无正文接口）
        replay = c.post(f"/sessions/{sid}/chunks/0/reuse", json={"sha256": dig})
        assert replay.status_code == 200
        assert replay.json()["duplicate"] is True
        # 同序号不同内容 -> 现有冲突错误
        other = sha256(make_bytes(4, "other"))
        conflict = c.post(f"/sessions/{sid}/chunks/0/reuse", json={"sha256": other})
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "CHUNK_CONFLICT"
        status = c.get(f"/sessions/{sid}").json()
        assert status["received_count"] == 1


def test_reuse_validation_errors(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        sid = create_session(c, payload, 4)["session_id"]
        for bad in ({"sha256": "ZZ" + "a" * 62}, {"sha256": "A" * 64}, {}):
            resp = c.post(f"/sessions/{sid}/chunks/0/reuse", json=bad)
            assert resp.status_code == 422, bad
            assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
        resp = c.post(f"/sessions/{sid}/chunks/99/reuse", json={"sha256": "a" * 64})
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "CHUNK_INDEX_OUT_OF_RANGE"


def test_reuse_expired_session_cannot_add_progress(data_dir, monkeypatch):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        sid = create_session(c, payload, 4)["session_id"]
        donor = create_session(c, payload, 4)["session_id"]
        part0 = chunk(payload, 4, 0)
        part1 = chunk(payload, 4, 1)
        assert put_chunk(c, sid, 0, part0).status_code == 201
        assert put_chunk(c, donor, 1, part1).status_code == 201

        later = datetime.now(timezone.utc) + timedelta(hours=2)
        monkeypatch.setattr(clock, "utcnow", lambda: later)

        # 过期会话不得借复用增加新片
        resp = c.post(f"/sessions/{sid}/chunks/1/reuse", json={"sha256": sha256(part1)})
        assert resp.status_code == 410
        assert resp.json()["error"]["code"] == "SESSION_EXPIRED"
        status = c.get(f"/sessions/{sid}").json()
        assert status["missing_chunks"] == [1, 2]

        # 已确认片的幂等重放仍然成功（活动池有正文）
        replay = c.post(f"/sessions/{sid}/chunks/0/reuse", json={"sha256": sha256(part0)})
        assert replay.status_code == 200
        assert replay.json()["duplicate"] is True


def test_reuse_against_sealed_blob_is_chunk_not_cached(data_dir):
    """回收先封存候选 -> 复用得到结构化 CHUNK_NOT_CACHED，位图不变。"""
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        donor = create_session(c, payload, 4)["session_id"]
        target = create_session(c, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)
        dig = sha256(part)
        assert put_chunk(c, donor, 0, part).status_code == 201
        # 完成 donor：其分片不再钉住池文件
        for i in range(3):
            p = chunk(payload, 4, i)
            assert put_chunk(c, donor, i, p).status_code in (200, 201)
        assert c.post(f"/sessions/{donor}/finalize").status_code == 200

        # 直接构造“回收已封存”窗口：池记录 sealed、正文在 .trash。
        db_path = data_dir / "db.sqlite3"
        with sqlite3.connect(db_path) as dbx:
            dbx.execute("UPDATE pool_blobs SET state = 'sealed' WHERE sha256 = ?", (dig,))
        blob = data_dir / "pool" / dig[:2] / f"{dig}.blob"
        trash = data_dir / "pool" / ".trash" / f"{dig}.cyc1.trash"
        blob.rename(trash)

        resp = c.post(f"/sessions/{target}/chunks/0/reuse", json={"sha256": dig})
        assert resp.status_code == 409
        err = resp.json()["error"]
        assert err["code"] == "CHUNK_NOT_CACHED"
        assert err["details"]["reason"] == "reclaiming"
        assert c.get(f"/sessions/{target}").json()["missing_chunks"] == [0, 1, 2]


# ---------- 回收 ----------

def test_active_session_pins_pool_blobs(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        sid = create_session(c, payload, 4)["session_id"]
        for i in (0, 1):  # 缺第 2 片，会话保持活动
            assert put_chunk(c, sid, i, chunk(payload, 4, i)).status_code == 201
        resp = c.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 100})
        assert resp.status_code == 200
        body = resp.json()
        assert body["examined"] == 2
        assert body["deleted"] == 0
        assert body["done"] is True
        assert len(list((data_dir / "pool").rglob("*.blob"))) == 2
        # 活动会话缺片，发布仍被拒绝
        assert c.post(f"/sessions/{sid}/finalize").status_code == 409
        # 回收未删除正文：补齐后正常发布
        assert put_chunk(c, sid, 2, chunk(payload, 4, 2)).status_code == 201
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200
        assert c.get(f"/sessions/{sid}/artifact").content == payload


def test_reclaim_pagination_cursor_and_resume_across_restart(data_dir):
    payload = make_bytes(40)  # 10 个 4 字节分片，内容两两不同
    settings = Settings(data_dir=data_dir)
    with TestClient(create_app(settings)) as c:
        sid = create_session(c, payload, 4)["session_id"]
        for i in range(10):
            assert put_chunk(c, sid, i, chunk(payload, 4, i)).status_code == 201
        # 完成会话以解除钉住
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200

    cycle_id = None
    examined = 0
    settings = Settings(data_dir=data_dir)
    for call in range(4):
        with TestClient(create_app(settings)) as c:
            resp = c.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 3})
            assert resp.status_code == 200
            body = resp.json()
            if cycle_id is None:
                cycle_id = body["cycle_id"]
            assert body["cycle_id"] == cycle_id  # 同一持久周期，游标续扫
            examined += 3 if call < 3 else 1
            assert body["examined"] == examined
            if call < 3:
                assert body["done"] is False
                assert body["deleted"] == examined
            else:
                assert body["done"] is True
    assert len(list((data_dir / "pool").rglob("*.blob"))) == 0
    # 每次回收调用是新的 HTTP 请求；新周期从头再扫一次应直接 done
    with TestClient(create_app(settings)) as c:
        resp = c.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 1000})
        assert resp.json()["done"] is True
        assert resp.json()["examined"] == 0
        assert resp.json()["cycle_id"] != cycle_id


def test_reclaim_request_validation(data_dir):
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        for bad in (0, -1, 1001):
            resp = c.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": bad})
            assert resp.status_code == 422
            assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


def test_reclaim_completed_session_keeps_artifact(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        sid = create_session(c, payload, 4)["session_id"]
        for i in range(3):
            assert put_chunk(c, sid, i, chunk(payload, 4, i)).status_code == 201
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200
        assert c.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 100}).json()["deleted"] == 3
        # 成品独立承载：历史分片被回收后仍为 completed，成品可下载
        status = c.get(f"/sessions/{sid}").json()
        assert status["status"] == "completed"
        art = c.get(f"/sessions/{sid}/artifact")
        assert art.status_code == 200 and art.content == payload
        # 再次 finalize 幂等成功，不回退未完成
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200


def test_reclaim_mixed_pin_and_unpin(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        done_sid = create_session(c, payload, 4)["session_id"]
        active_sid = create_session(c, payload, 4)["session_id"]
        for i in range(3):
            assert put_chunk(c, done_sid, i, chunk(payload, 4, i)).status_code == 201
        assert c.post(f"/sessions/{done_sid}/finalize").status_code == 200
        # 活动会话引用其中两片相同内容 -> 这两片被钉住
        assert put_chunk(c, active_sid, 0, chunk(payload, 4, 0)).status_code == 201
        assert put_chunk(c, active_sid, 1, chunk(payload, 4, 1)).status_code == 201
        body = c.post("/maintenance/chunk-pool/reclaim", json={"max_blobs": 100}).json()
        assert body["examined"] == 3
        assert body["deleted"] == 1  # 只有第 3 片无活动/过期未完成引用
        assert len(list((data_dir / "pool").rglob("*.blob"))) == 2


# ---------- 旧卷惰性提升 ----------

def _seed_legacy_volume(data_dir: Path, sessions: list[tuple[str, bytes, int, list[int]]]):
    """直接按旧版本布局写库与 chunks/<sid>/<index>.chunk。"""
    db_path = data_dir / "db.sqlite3"
    data_dir.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(
        """
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY, file_size INTEGER, chunk_size INTEGER,
            total_chunks INTEGER, file_sha256 TEXT, status TEXT, bitmap BLOB,
            expires_at TEXT, created_at TEXT, completed_at TEXT, final_sha256 TEXT,
            artifact_path TEXT);
        CREATE TABLE chunks (
            session_id TEXT, chunk_index INTEGER, size INTEGER, sha256 TEXT,
            path TEXT, received_at TEXT, PRIMARY KEY (session_id, chunk_index));
        """
    )
    now = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    for sid, payload, cs, indices in sessions:
        total = -(-len(payload) // cs)
        bitmap = bytearray((total + 7) // 8)
        cdir = data_dir / "chunks" / sid
        cdir.mkdir(parents=True, exist_ok=True)
        for i in indices:
            part = chunk(payload, cs, i)
            path = cdir / f"{i:08d}.chunk"
            path.write_bytes(part)
            con.execute(
                "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                (sid, i, len(part), sha256(part), str(path), now),
            )
            bitmap[i >> 3] |= 1 << (i & 7)
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, len(payload), cs, total, sha256(payload), "active",
             bytes(bitmap), now, now, None, None, None),
        )
    con.commit()
    con.close()


def test_legacy_volume_lazy_promotion_on_finalize(data_dir):
    payload = make_bytes(10)
    sid = "legacy-session-1"
    _seed_legacy_volume(data_dir, [(sid, payload, 4, [0, 1, 2])])

    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        status = c.get(f"/sessions/{sid}").json()
        assert status["missing_chunks"] == []  # 启动不重散列，旧确认全部保留
        # 启动本身不应提前提升
        assert list((data_dir / "pool").rglob("*.blob")) == []

        resp = c.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200, resp.text
        assert c.get(f"/sessions/{sid}/artifact").content == payload
        # 提升后：池内一份正文，旧文件已删除，引用指向池
        blobs = list((data_dir / "pool").rglob("*.blob"))
        assert len(blobs) == 3
        assert not list((data_dir / "chunks" / sid).glob("*.chunk"))
        con = sqlite3.connect(data_dir / "db.sqlite3")
        nulls = con.execute(
            "SELECT COUNT(*) FROM chunks WHERE session_id = ? AND pool_sha256 IS NULL",
            (sid,),
        ).fetchone()[0]
        con.close()
        assert nulls == 0


def test_legacy_missing_chunk_revokes_confirmation(data_dir):
    payload = make_bytes(10)
    sid = "legacy-session-2"
    _seed_legacy_volume(data_dir, [(sid, payload, 4, [0, 1, 2])])
    (data_dir / "chunks" / sid / "00000001.chunk").unlink()

    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        status = c.get(f"/sessions/{sid}").json()
        assert status["missing_chunks"] == [1]  # 既有规则：撤销该片确认
        part = chunk(payload, 4, 1)
        assert put_chunk(c, sid, 1, part).status_code == 201
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200
        assert c.get(f"/sessions/{sid}/artifact").content == payload


def test_legacy_corrupt_same_size_chunk_revoked_on_access(data_dir):
    payload = make_bytes(10)
    sid = "legacy-session-3"
    _seed_legacy_volume(data_dir, [(sid, payload, 4, [0, 1, 2])])
    # 同尺寸异内容：启动只核对尺寸故仍确认，访问（finalize）重验时撤销。
    (data_dir / "chunks" / sid / "00000000.chunk").write_bytes(b"xxxx")

    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        assert c.get(f"/sessions/{sid}").json()["missing_chunks"] == []
        resp = c.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "CHUNKS_INCOMPLETE"
        assert resp.json()["error"]["details"]["missing_chunks"] == [0]
        assert c.get(f"/sessions/{sid}").json()["missing_chunks"] == [0]


def test_concurrent_legacy_promotion_single_blob(data_dir):
    """两个 worker 同时 finalize 引用同一旧内容的两个会话。"""
    payload = make_bytes(4)
    s1, s2 = "legacy-a", "legacy-b"
    _seed_legacy_volume(data_dir, [(s1, payload, 4, [0]), (s2, payload, 4, [0])])
    settings = Settings(data_dir=data_dir)
    app1 = create_app(settings)
    app2 = create_app(settings)
    with TestClient(app1) as c1, TestClient(app2) as c2:
        barrier = threading.Barrier(2)

        def finalize(client, sid):
            barrier.wait()
            return client.post(f"/sessions/{sid}/finalize").status_code

        with ThreadPoolExecutor(max_workers=2) as ex:
            codes = list(ex.map(finalize, (c1, c2), (s1, s2)))
        assert codes == [200, 200], codes
        blobs = list((data_dir / "pool").rglob("*.blob"))
        assert len(blobs) == 1
        assert c1.get(f"/sessions/{s1}/artifact").content == payload
        assert c2.get(f"/sessions/{s2}/artifact").content == payload


# ---------- 崩溃收敛 / 污染 ----------

def test_failed_upload_leaves_no_pool_or_tmp_pollution(data_dir):
    payload = make_bytes(10)
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        sid = create_session(c, payload, 4)["session_id"]
        part = chunk(payload, 4, 0)
        # 摘要不符
        resp = put_chunk(c, sid, 0, part, digest=sha256(b"wrong"))
        assert resp.status_code == 400
        assert list((data_dir / "pool" / ".staging").iterdir()) == []
        con = sqlite3.connect(data_dir / "db.sqlite3")
        assert con.execute("SELECT COUNT(*) FROM pool_blobs").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0
        con.close()


def test_recover_finishes_interrupted_reclaim_delete(data_dir):
    payload = make_bytes(10)
    sid = "crash-1"
    _seed_legacy_volume(data_dir, [(sid, payload, 4, [0, 1, 2])])
    # 第一次启动：finalize 完成 -> 池化 + 成品；随后构造回收中断现场。
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200

    con = sqlite3.connect(data_dir / "db.sqlite3")
    con.row_factory = sqlite3.Row
    digests = [r[0] for r in con.execute("SELECT sha256 FROM pool_blobs")]
    cycle = "stuck-cycle"
    old_lease = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
    con.execute(
        "INSERT INTO reclaim_cycles (cycle_id, created_at, finished_at, owner_id, lease_until)"
        " VALUES (?, ?, NULL, ?, ?)",
        (cycle, "2000-01-01T00:00:00+00:00", "dead", old_lease),
    )
    for n, dig in enumerate(digests):
        con.execute(
            "INSERT INTO reclaim_candidates (cycle_id, sha256, ordinal, outcome)"
            " VALUES (?, ?, ?, 'sealed')",
            (cycle, dig, n + 1),
        )
        con.execute("UPDATE pool_blobs SET state = 'sealed' WHERE sha256 = ?", (dig,))
        blob = data_dir / "pool" / dig[:2] / f"{dig}.blob"
        trash = data_dir / "pool" / ".trash" / f"{dig}.{cycle}.trash"
        blob.rename(trash)
    con.commit()
    con.close()

    # 重启：只处理持久中间态，必须收敛为“池记录与正文均不存在”。
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        status = c.get(f"/sessions/{sid}").json()
        assert status["status"] == "completed"
        assert c.get(f"/sessions/{sid}/artifact").content == payload
    con = sqlite3.connect(data_dir / "db.sqlite3")
    assert con.execute("SELECT COUNT(*) FROM pool_blobs").fetchone()[0] == 0
    pending = con.execute(
        "SELECT COUNT(*) FROM reclaim_candidates WHERE outcome = 'sealed'"
    ).fetchone()[0]
    con.close()
    assert pending == 0
    assert list((data_dir / "pool").rglob("*.blob")) == []
    assert list((data_dir / "pool" / ".trash").iterdir()) == []


def test_recover_restores_sealed_blob_when_reference_appears(data_dir):
    """封存后、删除前出现新的活动引用：重启收敛为“正文可用且引用完整”。"""
    payload = make_bytes(10)
    part0 = chunk(payload, 4, 0)
    donor = "crash-donor"
    active = "crash-active"
    # donor 是 3 片文件（三片齐全以完成发布）；active 是内容等于 donor 第 0 片的单片文件。
    _seed_legacy_volume(
        data_dir, [(donor, payload, 4, [0, 1, 2]), (active, part0, 4, [])]
    )
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        assert c.post(f"/sessions/{donor}/finalize").status_code == 200

    con = sqlite3.connect(data_dir / "db.sqlite3")
    con.row_factory = sqlite3.Row
    dig = sha256(part0)
    assert con.execute(
        "SELECT 1 FROM pool_blobs WHERE sha256 = ?", (dig,)
    ).fetchone() is not None
    size = 4
    cycle = "stuck-cycle-2"
    old_lease = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
    con.execute(
        "INSERT INTO reclaim_cycles VALUES (?, ?, NULL, ?, ?)",
        (cycle, "2000-01-01T00:00:00+00:00", "dead", old_lease),
    )
    con.execute(
        "INSERT INTO reclaim_candidates VALUES (?, ?, 1, 'sealed')", (cycle, dig)
    )
    con.execute("UPDATE pool_blobs SET state = 'sealed' WHERE sha256 = ?", (dig,))
    # 竞争结果：活动会话已取得该正文的引用（复用先于删除提交的等价持久现场）。
    blob_name = f"{dig}.blob"
    con.execute(
        "INSERT INTO chunks (session_id, chunk_index, size, sha256, path, received_at, pool_sha256)"
        " VALUES (?, 0, ?, ?, ?, ?, ?)",
        (active, size, dig, str(data_dir / "pool" / dig[:2] / blob_name),
         "2000-01-01T00:00:00+00:00", dig),
    )
    bitmap = bytearray(1)
    bitmap[0] = 1
    con.execute("UPDATE sessions SET bitmap = ? WHERE session_id = ?", (bytes(bitmap), active))
    con.commit()
    con.close()
    blob = data_dir / "pool" / dig[:2] / f"{dig}.blob"
    trash = data_dir / "pool" / ".trash" / f"{dig}.{cycle}.trash"
    blob.rename(trash)

    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        # 引用不悬空：正文归位，活动会话补齐其余片后可发布
        status = c.get(f"/sessions/{active}").json()
        assert status["missing_chunks"] == []
        part0 = chunk(payload, 4, 0)
        replay = c.post(f"/sessions/{active}/chunks/0/reuse", json={"sha256": sha256(part0)})
        assert replay.status_code == 200
    assert blob.exists()


def test_recover_seal_committed_before_rename(data_dir):
    """封存事务已提交、但内容->trash 改名前崩溃：正文仍在内容路径。

    无引用时重启必须删除记录与正文，且不触碰其他内容；随后相同内容可重新上传。
    """
    payload = make_bytes(10)
    sid = "crash-prenaming"
    _seed_legacy_volume(data_dir, [(sid, payload, 4, [0, 1, 2])])
    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200

    con = sqlite3.connect(data_dir / "db.sqlite3")
    con.row_factory = sqlite3.Row
    dig = sha256(chunk(payload, 4, 1))
    cycle = "prenaming-cycle"
    old_lease = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
    con.execute(
        "INSERT INTO reclaim_cycles VALUES (?, ?, NULL, ?, ?)",
        (cycle, "2000-01-01T00:00:00+00:00", "dead", old_lease),
    )
    con.execute(
        "INSERT INTO reclaim_candidates VALUES (?, ?, 1, 'sealed')", (cycle, dig)
    )
    con.execute("UPDATE pool_blobs SET state = 'sealed' WHERE sha256 = ?", (dig,))
    con.commit()
    con.close()
    # 关键：正文仍在内容路径，.trash 中没有副本（改名前崩溃）。
    blob = data_dir / "pool" / dig[:2] / f"{dig}.blob"
    assert blob.exists()
    assert not (data_dir / "pool" / ".trash").exists() or not list(
        (data_dir / "pool" / ".trash").iterdir()
    )

    with TestClient(create_app(Settings(data_dir=data_dir))) as c:
        assert c.get(f"/sessions/{sid}").json()["status"] == "completed"
        assert c.get(f"/sessions/{sid}/artifact").content == payload
    assert not blob.exists()
    con = sqlite3.connect(data_dir / "db.sqlite3")
    assert con.execute(
        "SELECT COUNT(*) FROM pool_blobs WHERE sha256 = ?", (dig,)
    ).fetchone()[0] == 0
    pending = con.execute(
        "SELECT outcome FROM reclaim_candidates WHERE cycle_id = ? AND sha256 = ?",
        (cycle, dig),
    ).fetchone()[0]
    con.close()
    assert pending == "deleted"
