import importlib
import json
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app.batch import BatchCoordinator, BatchJournal, BatchRequest
from app.checkpoints import CheckpointStore
from app.engine import DatabaseRegistry, status
from app.release import ReleaseStore


def make_db(path, table="t"):
    conn = sqlite3.connect(path)
    conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute(f"INSERT INTO {table} VALUES (1, 'one')")
    conn.commit()
    conn.close()


def plan_payload(sql_a="ALTER TABLE t ADD COLUMN n TEXT;",
                 sql_b="ALTER TABLE t ADD COLUMN n TEXT;"):
    return {
        "databases": [
            {"alias": "a", "expected_version": 0,
             "scripts": [{"version": 1, "description": "m1", "sql": sql_a}]},
            {"alias": "b", "expected_version": 0,
             "scripts": [{"version": 1, "description": "m1", "sql": sql_b}]},
        ]
    }


ALICE = {"x-reviewer-token": "token-alice"}
BOB = {"x-reviewer-token": "token-bob"}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    make_db(tmp_path / "a.db")
    make_db(tmp_path / "b.db")
    cfg = tmp_path / "aliases.json"
    cfg.write_text(json.dumps({
        "aliases": {"a": "a.db", "b": "b.db"},
        "review_mode": True,
        "reviewers": {"token-alice": "alice", "token-bob": "bob"},
    }))
    monkeypatch.setenv("MIGRATION_CONFIG", str(cfg))
    monkeypatch.setenv("MIGRATION_CHECKPOINT_DIR", str(tmp_path / "cps"))
    monkeypatch.setenv("MIGRATION_BATCH_DIR", str(tmp_path / "batches"))
    monkeypatch.setenv("MIGRATION_RELEASE_DIR", str(tmp_path / "releases"))
    import app.config
    import app.main

    importlib.reload(app.config)
    importlib.reload(app.main)
    yield TestClient(app.main.app), tmp_path, app.main


def create_order(c):
    r = c.post("/releases", json=plan_payload(), headers=ALICE)
    assert r.status_code == 201, r.text
    return r.json()


def approve_order(c, order):
    r = c.post(f"/releases/{order['release_id']}/approve",
               json={"digest": order["digest"]}, headers=BOB)
    assert r.status_code == 200, r.text
    return r.json()


def test_create_release_identity_from_credential(client):
    c, tmp_path, main = client
    order = create_order(c)
    assert order["status"] == "pending_approval"
    assert order["author"] == "alice"  # 身份来自凭据，不信任请求署名
    assert len(order["digest"]) == 64
    assert order["plan"]["databases"][0]["scripts"][0]["sql"].startswith("ALTER")
    # 请求体内署名无效
    r = c.post("/releases", json={**plan_payload(), "author": "mallory"}, headers=ALICE)
    assert r.status_code == 201 and r.json()["author"] == "alice"
    # 无凭据 / 假凭据
    assert c.post("/releases", json=plan_payload()).status_code == 401
    assert c.post("/releases", json=plan_payload(),
                  headers={"x-reviewer-token": "forged"}).status_code == 401


def test_self_review_and_digest_mismatch_rejected(client):
    c, tmp_path, main = client
    order = create_order(c)
    rid = order["release_id"]
    # 作者不能审自己的单
    r = c.post(f"/releases/{rid}/approve", json={"digest": order["digest"]}, headers=ALICE)
    assert r.status_code == 403
    r = c.post(f"/releases/{rid}/reject", headers=ALICE)
    assert r.status_code == 403
    # 摘要不符（防偷换 SQL 后蒙混批准）
    r = c.post(f"/releases/{rid}/approve", json={"digest": "0" * 64}, headers=BOB)
    assert r.status_code == 409
    assert c.get(f"/releases/{rid}").json()["status"] == "pending_approval"


def test_approve_then_execute_success_and_idempotent_reexecute(client):
    c, tmp_path, main = client
    order = approve_order(c, create_order(c))
    assert order["status"] == "approved" and order["approver"] == "bob"
    rid = order["release_id"]

    r = c.post(f"/releases/{rid}/execute", headers=ALICE)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "succeeded"
    assert body["release_id"] == rid and body["batch_id"]
    assert status(tmp_path / "a.db")["current_version"] == 1
    assert status(tmp_path / "b.db")["current_version"] == 1

    # 批次关联与结果持久化
    saved = c.get(f"/releases/{rid}").json()
    assert saved["status"] == "succeeded"
    assert saved["batch_id"] == body["batch_id"]
    assert saved["result"]["status"] == "succeeded"
    assert c.get(f"/batches/{body['batch_id']}").json()["status"] == "succeeded"

    # 重复执行：返回已有结果，不产生新批次
    r2 = c.post(f"/releases/{rid}/execute", headers=BOB)
    assert r2.status_code == 200
    assert r2.json()["batch_id"] == body["batch_id"]
    assert r2.json()["idempotent_replay"] is True
    assert len(c.get("/batches").json()["batches"]) == 1


def test_reject_and_cancel_are_terminal(client):
    c, tmp_path, main = client
    # 拒绝后不能执行
    order = create_order(c)
    rid = order["release_id"]
    r = c.post(f"/releases/{rid}/reject", headers=BOB)
    assert r.status_code == 200 and r.json()["status"] == "rejected"
    assert c.post(f"/releases/{rid}/execute", headers=ALICE).status_code == 409
    # 拒绝是终态：再批准失败
    assert c.post(f"/releases/{rid}/approve",
                  json={"digest": order["digest"]}, headers=BOB).status_code == 409

    # 作者可撤销未执行的单（含已批准的），撤销后不能执行
    order2 = approve_order(c, create_order(c))
    rid2 = order2["release_id"]
    # 非作者不能撤销
    assert c.post(f"/releases/{rid2}/cancel", headers=BOB).status_code == 409
    r = c.post(f"/releases/{rid2}/cancel", headers=ALICE)
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert c.post(f"/releases/{rid2}/execute", headers=ALICE).status_code == 409
    assert status(tmp_path / "a.db")["current_version"] == 0


def test_concurrent_decisions_single_terminal_state(client):
    c, tmp_path, main = client
    order = create_order(c)
    rid = order["release_id"]
    results = []

    def act(fn):
        results.append(fn().status_code)

    threads = [
        threading.Thread(target=act, args=(lambda: c.post(
            f"/releases/{rid}/approve", json={"digest": order["digest"]}, headers=BOB),)),
        threading.Thread(target=act, args=(lambda: c.post(
            f"/releases/{rid}/reject", headers=BOB),)),
        threading.Thread(target=act, args=(lambda: c.post(
            f"/releases/{rid}/cancel", headers=ALICE),)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = sorted(results).count(200)
    final = c.get(f"/releases/{rid}").json()["status"]
    # 终态一致：要么只有一个决定生效；要么是 批准->作者撤销 的合法串联。
    if ok == 1:
        assert final in ("approved", "rejected", "cancelled")
    else:
        assert ok == 2 and final == "cancelled"
    if final != "approved":
        assert c.post(f"/releases/{rid}/execute", headers=ALICE).status_code == 409


def test_execute_rejected_when_db_changed_after_approval(client):
    c, tmp_path, main = client
    order = approve_order(c, create_order(c))
    rid = order["release_id"]
    # 审批后库被改动：版本前进，执行必须拒绝且不重跑
    from app.engine import apply_manifest
    from app.manifest import MigrationManifest

    apply_manifest(
        tmp_path / "a.db",
        MigrationManifest.model_validate({
            "expected_version": 0,
            "scripts": [{"version": 1, "description": "other", "sql": "ALTER TABLE t ADD COLUMN x TEXT;"}],
        }),
        main.registry.lock_for("a"),
    )
    r = c.post(f"/releases/{rid}/execute", headers=ALICE)
    assert r.status_code == 409
    assert r.json()["code"] == "version_conflict"
    saved = c.get(f"/releases/{rid}").json()
    assert saved["status"] == "failed"
    assert saved["batch_id"]  # 批次关联保留
    # 失败不自动重跑；人工再执行返回已有失败结果
    r2 = c.post(f"/releases/{rid}/execute", headers=ALICE)
    assert r2.status_code == 422 and r2.json()["idempotent_replay"] is True
    assert len(c.get("/batches").json()["batches"]) == 1


def test_review_mode_closes_direct_entries(client):
    c, tmp_path, main = client
    payload = {"expected_version": 0,
               "scripts": [{"version": 1, "description": "d", "sql": "ALTER TABLE t ADD COLUMN n TEXT;"}]}
    assert c.post("/databases/a/migrate", json=payload).status_code == 403
    assert c.post("/batches", json=plan_payload()).status_code == 403
    r = c.post("/databases/a/restore",
               json={"checkpoint_id": "cp_x", "expected_version": 0})
    assert r.status_code == 403
    # 只读与发布单入口仍可用
    assert c.get("/databases/a/version").status_code == 200
    assert c.get("/releases").status_code == 200


def test_release_persists_across_restart_and_undecided(tmp_path):
    store = ReleaseStore(tmp_path / "rel")
    plan = {"databases": [{"alias": "a", "expected_version": 0, "scripts": [
        {"version": 1, "description": "m", "sql": "SELECT 1;"}]}]}
    order = store.create("alice", plan)
    rid = order["release_id"]
    store.approve(rid, "bob")
    store.begin_execute(rid)
    store.attach_batch(rid, "batch_xyz")

    # 模拟重启：执行中断标为未决，批次关联不丢
    store2 = ReleaseStore(tmp_path / "rel")
    assert store2.mark_unfinished_undecided() == [rid]
    saved = store2.get(rid)
    assert saved["status"] == "undecided"
    assert saved["batch_id"] == "batch_xyz"
    assert saved["author"] == "alice" and saved["approver"] == "bob"
    assert saved["digest"] == order["digest"]
    assert store2.mark_unfinished_undecided() == []


def test_compensation_io_error_does_not_abort_other_restores(tmp_path):
    make_db(tmp_path / "a.db")
    make_db(tmp_path / "b.db")
    cfg = tmp_path / "aliases.json"
    cfg.write_text(json.dumps({"aliases": {"a": "a.db", "b": "b.db"}}))
    import app.config

    settings = app.config.load_settings(cfg)
    registry = DatabaseRegistry()
    checkpoints = CheckpointStore(tmp_path / "cps")
    journal = BatchJournal(tmp_path / "batches")
    coordinator = BatchCoordinator(settings, registry, checkpoints, journal)

    original_restore = checkpoints.restore

    def flaky_restore(alias, *args, **kwargs):
        if alias == "a":
            raise OSError("disk I/O error")  # 非业务异常也不能中断其余库恢复
        return original_restore(alias, *args, **kwargs)

    checkpoints.restore = flaky_restore
    request = BatchRequest.model_validate({
        "databases": [
            {"alias": "a", "expected_version": 0, "scripts": [
                {"version": 1, "description": "m1", "sql": "ALTER TABLE t ADD COLUMN n TEXT;"}]},
            {"alias": "b", "expected_version": 0, "scripts": [
                {"version": 1, "description": "m1", "sql": "INSERT INTO nope VALUES (1);"}]},
        ]
    })
    status_code, payload = coordinator.execute(request)
    assert status_code == 422
    assert payload["status"] == "compensation_incomplete"
    dbs = {d["alias"]: d for d in payload["databases"]}
    assert dbs["a"]["status"] == "restore_failed"
    assert "disk I/O error" in dbs["a"]["error"]  # 保留真实错误
    assert dbs["b"]["status"] == "failed"
