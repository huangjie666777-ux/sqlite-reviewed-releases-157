"""多库关联发布：统一协调、批次日志持久化与失败补偿。

协调器在进程内用一把全局批次锁串行化所有批次，再按别名字典序
一次性持有全部涉及库的锁，与既有单库迁移/检查点/恢复互斥，
重叠批次不会交叉执行，锁顺序一致因此不会死锁。

批次日志落在应用库之外的独立 SQLite 文件（batch_dir/journal.db），
计划、每个准备/迁移/补偿步骤与最终结果逐步持久化；重启后未结束
批次标记为 undecided，不自动重放 SQL。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from .checkpoints import CheckpointStore
from .config import Settings
from .engine import (
    DatabaseBusy,
    DatabaseRegistry,
    MigrationError,
    ScriptFailed,
    VersionConflict,
    apply_manifest,
    preflight,
)
from .manifest import MigrationItem, MigrationManifest


class BatchError(MigrationError):
    """批次业务失败基类。"""


class UnknownBatch(BatchError):
    pass


class BatchDatabaseItem(BaseModel):
    alias: str = Field(min_length=1)
    expected_version: int = Field(ge=0)
    scripts: list[MigrationItem]

    @field_validator("scripts")
    @classmethod
    def _validate_chain(cls, items):
        # 复用单库清单的完整链式校验（非空、从 1 连续、数量上限）。
        MigrationManifest(expected_version=0, scripts=items)
        return items

    def manifest(self) -> MigrationManifest:
        return MigrationManifest(
            expected_version=self.expected_version, scripts=self.scripts
        )


class BatchRequest(BaseModel):
    databases: list[BatchDatabaseItem] = Field(min_length=1)

    @field_validator("databases")
    @classmethod
    def _unique_aliases(cls, items):
        seen: set[str] = set()
        for item in items:
            if item.alias in seen:
                raise ValueError(f"duplicate alias in batch: {item.alias}")
            seen.add(item.alias)
        return items


TERMINAL_STATUSES = frozenset(
    {"succeeded", "prepare_failed", "compensated", "compensation_incomplete"}
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class BatchJournal:
    """批次计划/步骤/结果的持久化日志，存放在应用库之外。"""

    def __init__(self, batch_dir: Path) -> None:
        self._dir = Path(batch_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._db = self._dir / "journal.db"
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    result_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def create_batch(self, plan: dict) -> str:
        batch_id = "batch_%s_%s" % (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            uuid.uuid4().hex[:12],
        )
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO batches (batch_id, status, created_at, updated_at, plan_json) "
                "VALUES (?, 'planned', ?, ?, ?)",
                (batch_id, _now(), _now(), json.dumps(plan, ensure_ascii=False)),
            )
        self.add_event(batch_id, "planned", {"plan": plan})
        return batch_id

    def set_status(self, batch_id: str, status: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                (status, _now(), batch_id),
            )

    def set_result(self, batch_id: str, status: str, result: dict) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE batches SET status = ?, updated_at = ?, result_json = ? "
                "WHERE batch_id = ?",
                (status, _now(), json.dumps(result, ensure_ascii=False), batch_id),
            )

    def add_event(self, batch_id: str, phase: str, payload: dict) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO events (batch_id, ts, phase, payload_json) VALUES (?, ?, ?, ?)",
                (batch_id, _now(), phase, json.dumps(payload, ensure_ascii=False)),
            )

    def get(self, batch_id: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                return None
            events = conn.execute(
                "SELECT ts, phase, payload_json FROM events WHERE batch_id = ? ORDER BY seq",
                (batch_id,),
            ).fetchall()
        return {
            "batch_id": row["batch_id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "plan": json.loads(row["plan_json"]),
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "events": [
                {"ts": e["ts"], "phase": e["phase"], "payload": json.loads(e["payload_json"])}
                for e in events
            ],
        }

    def list(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT batch_id, status, created_at, updated_at FROM batches "
                "ORDER BY created_at, batch_id"
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_unfinished_undecided(self) -> list[str]:
        """重启后调用：未达终态的批次标为 undecided，不重放、不宣称成功。"""
        terminal = tuple(sorted(TERMINAL_STATUSES | {"undecided"}))
        placeholders = ",".join("?" * len(terminal))
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT batch_id, status FROM batches WHERE status NOT IN (%s)" % placeholders,
                terminal,
            ).fetchall()
            conn.execute(
                "UPDATE batches SET status = 'undecided', updated_at = ? "
                "WHERE status NOT IN (%s)" % placeholders,
                (_now(), *terminal),
            )
        for row in rows:
            self.add_event(
                row["batch_id"],
                "undecided",
                {"previous_status": row["status"], "reason": "service restarted mid-batch"},
            )
        return [r["batch_id"] for r in rows]


def _http_status_for(exc: Exception) -> int:
    if isinstance(exc, VersionConflict):
        return 409
    if isinstance(exc, DatabaseBusy):
        return 503
    return 422


def _error_code(exc: Exception) -> str:
    if isinstance(exc, VersionConflict):
        return "version_conflict"
    if isinstance(exc, DatabaseBusy):
        return "database_busy"
    if isinstance(exc, ScriptFailed):
        return "migration_failed"
    return "migration_error"


class BatchCoordinator:
    """统一协调一次多库发布：准备 -> 迁移 -> （失败时）逆序补偿。"""

    def __init__(
        self,
        settings: Settings,
        registry: DatabaseRegistry,
        checkpoints: CheckpointStore,
        journal: BatchJournal,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._checkpoints = checkpoints
        self._journal = journal
        self._batch_lock = threading.Lock()

    def execute(self, request: BatchRequest, on_batch_created=None):
        """执行批次，返回 (HTTP 状态码, 响应体)。业务异常不泄漏为 500。

        on_batch_created 若提供，在批次 ID 落库后立即回调，便于调用方
        （如发布单）在任何异常发生前持久化批次关联。
        """
        unknown = [d.alias for d in request.databases if d.alias not in self._settings.aliases]
        if unknown:
            return 404, {
                "detail": "unknown alias: " + ", ".join(sorted(unknown)),
                "code": "unknown_alias",
            }
        paths = {d.alias: self._settings.aliases[d.alias] for d in request.databases}
        seen_paths: dict[Path, str] = {}
        for alias, path in paths.items():
            if path in seen_paths:
                return 422, {
                    "detail": (
                        "aliases %r and %r resolve to the same database file"
                        % (seen_paths[path], alias)
                    ),
                    "code": "invalid_batch",
                }
            seen_paths[path] = alias

        plan = {
            "databases": [
                {
                    "alias": d.alias,
                    "expected_version": d.expected_version,
                    "script_versions": [s.version for s in d.scripts],
                }
                for d in request.databases
            ]
        }
        batch_id = self._journal.create_batch(plan)
        if on_batch_created is not None:
            on_batch_created(batch_id)

        # 全局批次锁保证批次互斥；别名单调加锁避免与单库操作死锁。
        with self._batch_lock:
            aliases = sorted(paths)
            locks = [self._registry.lock_for(a) for a in aliases]
            for lock in locks:
                lock.acquire()
            try:
                status_code, payload = self._run(batch_id, request.databases, paths)
            finally:
                for lock in reversed(locks):
                    lock.release()
        payload["batch_id"] = batch_id
        return status_code, payload

    def _run(self, batch_id, databases, paths):
        journal = self._journal
        states: dict[str, dict] = {
            d.alias: {
                "alias": d.alias,
                "status": "pending",
                "checkpoint_id": None,
                "before_version": None,
                "after_version": None,
                "error": None,
            }
            for d in databases
        }

        def result(status: str) -> dict:
            return {"status": status, "databases": list(states.values())}

        # 阶段一：准备。任一库版本/历史/脚本校验或检查点失败，整批不升级。
        journal.set_status(batch_id, "preparing")
        for item in databases:
            state = states[item.alias]
            manifest = item.manifest()
            try:
                before = preflight(paths[item.alias], manifest)
                meta = self._checkpoints.create(
                    item.alias, paths[item.alias], self._registry.lock_for(item.alias)
                )
            except MigrationError as exc:
                state["status"] = "prepare_failed"
                state["error"] = str(exc)
                journal.add_event(
                    batch_id, "prepare_failed",
                    {"alias": item.alias, "error": str(exc)},
                )
                payload = result("prepare_failed")
                payload["code"] = _error_code(exc)
                payload["detail"] = str(exc)
                if isinstance(exc, ScriptFailed):
                    payload["failed_version"] = exc.version
                    payload["reason"] = exc.reason
                journal.set_result(batch_id, "prepare_failed", payload)
                return _http_status_for(exc), payload
            state["status"] = "prepared"
            state["before_version"] = before
            state["checkpoint_id"] = meta["id"]
            journal.add_event(
                batch_id, "prepared",
                {"alias": item.alias, "checkpoint_id": meta["id"], "before_version": before},
            )

        # 阶段二：按输入顺序迁移。
        journal.set_status(batch_id, "migrating")
        migrated: list[str] = []
        for item in databases:
            state = states[item.alias]
            try:
                applied = apply_manifest(
                    paths[item.alias], item.manifest(), self._registry.lock_for(item.alias)
                )
            except MigrationError as exc:
                state["status"] = "failed"
                state["error"] = str(exc)
                journal.add_event(
                    batch_id, "migration_failed",
                    {"alias": item.alias, "error": str(exc)},
                )
                return self._compensate(batch_id, states, migrated, exc)
            state["status"] = "migrated"
            state["after_version"] = applied.after_version
            migrated.append(item.alias)
            journal.add_event(
                batch_id, "migrated",
                {
                    "alias": item.alias,
                    "before_version": applied.before_version,
                    "after_version": applied.after_version,
                    "applied_versions": applied.applied,
                },
            )

        payload = result("succeeded")
        journal.set_result(batch_id, "succeeded", payload)
        return 200, payload

    def _compensate(self, batch_id, states, migrated, exc):
        """逆序恢复已升级库；单个补偿失败仍继续恢复其余库。"""
        journal = self._journal
        journal.set_status(batch_id, "compensating")
        for alias in reversed(migrated):
            state = states[alias]
            try:
                restored = self._checkpoints.restore(
                    alias,
                    self._settings.aliases[alias],
                    state["checkpoint_id"],
                    expected_version=state["after_version"],
                    lock=self._registry.lock_for(alias),
                )
            except Exception as restore_exc:
                # 文件读写等任何异常都不能中断其余库的恢复；保留真实错误。
                state["status"] = "restore_failed"
                state["error"] = str(restore_exc)
                journal.add_event(
                    batch_id, "restore_failed",
                    {"alias": alias, "error": str(restore_exc)},
                )
                continue
            state["status"] = "restored"
            state["after_version"] = restored["after_version"]
            journal.add_event(
                batch_id, "restored",
                {"alias": alias, "checkpoint_id": state["checkpoint_id"]},
            )
        for state in states.values():
            if state["status"] in ("pending", "prepared"):
                state["status"] = "not_executed"
        incomplete = any(s["status"] == "restore_failed" for s in states.values())
        final = "compensation_incomplete" if incomplete else "compensated"
        payload = {
            "status": final,
            "code": _error_code(exc),
            "detail": str(exc),
            "databases": list(states.values()),
        }
        if isinstance(exc, ScriptFailed):
            payload["failed_version"] = exc.version
            payload["reason"] = exc.reason
        journal.set_result(batch_id, final, payload)
        return _http_status_for(exc), payload
