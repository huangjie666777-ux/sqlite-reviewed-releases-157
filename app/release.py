"""双人审核发布单：固定方案经另一人批准后才能执行，避免偷换 SQL 上线。

- 身份只来自服务端配置的凭据（aliases.json 的 "reviewers"），
  不信任请求体内的署名。
- 提交发布单时保存有序库别名、预期版本、完整清单和原始 SQL，
  服务生成 ID 与内容 SHA256；提交后不可改，调整须新建。
- 待审单由他人（非作者）批准或拒绝；批准必须携带所查看的内容摘要，
  摘要不符或自我审核一律拒绝。作者可撤销未开始执行的单。
- 执行只提交发布单 ID，按批准方案调用既有多库协调器；版本与历史
  仍在库锁内检查，审批后库已变化会被拒绝。并发/重复执行最多产生
  一个批次，重复执行返回已有结果，失败不自动重跑。
- 发布单、审核身份、决定、批次关联与结果持久化在应用库之外的
  release_dir/releases.db（独立 SQLite，WAL）；重启可查，执行中断
  标为 undecided，不重放 SQL、不虚报成功。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .batch import BatchCoordinator, BatchRequest
from .config import Settings
from .engine import MigrationError


class ReleaseError(MigrationError):
    """发布单业务失败基类。"""


class UnknownRelease(ReleaseError):
    pass


class UnknownCredential(ReleaseError):
    pass


class SelfReviewForbidden(ReleaseError):
    pass


class DigestMismatch(ReleaseError):
    pass


class ReleaseStateError(ReleaseError):
    """当前状态不允许该操作（终态冲突）。"""


# 待审 -> 批准/拒绝/撤销 -> 执行 -> 成功/失败；undecided 为重启后未决标记。
TERMINAL_STATUSES = frozenset({"rejected", "cancelled", "succeeded", "failed", "undecided"})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def plan_digest(plan: dict) -> str:
    """发布单内容（有序库、预期版本、完整清单与原始 SQL）的 SHA256。"""
    canonical = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ReleaseStore:
    """发布单与审核决定的持久化存储，位于应用库之外。"""

    def __init__(self, release_dir: Path) -> None:
        self._dir = Path(release_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._db = self._dir / "releases.db"
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS releases (
                    release_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    author TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    approver TEXT,
                    decided_by TEXT,
                    batch_id TEXT,
                    result_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS release_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    release_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    actor TEXT,
                    payload_json TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    # ---- 写入 ----

    def create(self, author: str, plan: dict) -> dict:
        release_id = "rel_%s_%s" % (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            uuid.uuid4().hex[:12],
        )
        digest = plan_digest(plan)
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO releases (release_id, status, author, digest, plan_json, "
                "created_at, updated_at) VALUES (?, 'pending_approval', ?, ?, ?, ?, ?)",
                (release_id, author, digest, json.dumps(plan, ensure_ascii=False),
                 _now(), _now()),
            )
        self._add_event(release_id, "created", author, {"digest": digest, "plan": plan})
        return self.get(release_id)

    def _transition(self, release_id: str, expect: tuple, **fields) -> bool:
        """条件更新实现原子状态流转；并发下只有一个转移成功。"""
        status = fields.pop("status")
        assignments = ", ".join(["status = ?"] + [k + " = ?" for k in fields] + ["updated_at = ?"])
        params = [status, *fields.values(), _now(), release_id, *expect]
        placeholders = ",".join("?" * len(expect))
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "UPDATE releases SET " + assignments +
                " WHERE release_id = ? AND status IN (" + placeholders + ")",
                params,
            )
            return cur.rowcount == 1

    def approve(self, release_id: str, approver: str) -> bool:
        return self._transition(
            release_id, ("pending_approval",),
            status="approved", approver=approver, decided_by=approver,
        )

    def reject(self, release_id: str, reviewer: str) -> bool:
        return self._transition(
            release_id, ("pending_approval",),
            status="rejected", decided_by=reviewer,
        )

    def cancel(self, release_id: str, author: str) -> bool:
        # 仅未开始执行（待审或已批准）可撤销。
        return self._transition(
            release_id, ("pending_approval", "approved"),
            status="cancelled", decided_by=author,
        )

    def begin_execute(self, release_id: str) -> bool:
        return self._transition(release_id, ("approved",), status="executing")

    def attach_batch(self, release_id: str, batch_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE releases SET batch_id = ?, updated_at = ? WHERE release_id = ?",
                (batch_id, _now(), release_id),
            )

    def finish_execute(self, release_id: str, status: str, result: dict) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE releases SET status = ?, result_json = ?, updated_at = ? "
                "WHERE release_id = ?",
                (status, json.dumps(result, ensure_ascii=False), _now(), release_id),
            )

    def _add_event(self, release_id: str, phase: str, actor, payload: dict) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO release_events (release_id, ts, phase, actor, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (release_id, _now(), phase, actor, json.dumps(payload, ensure_ascii=False)),
            )

    def add_event(self, release_id: str, phase: str, actor, payload: dict) -> None:
        self._add_event(release_id, phase, actor, payload)

    # ---- 查询 ----

    @staticmethod
    def _row_to_dict(row) -> dict:
        return {
            "release_id": row["release_id"],
            "status": row["status"],
            "author": row["author"],
            "digest": row["digest"],
            "plan": json.loads(row["plan_json"]),
            "approver": row["approver"],
            "decided_by": row["decided_by"],
            "batch_id": row["batch_id"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def get(self, release_id: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE release_id = ?", (release_id,)
            ).fetchone()
            if row is None:
                return None
            events = conn.execute(
                "SELECT ts, phase, actor, payload_json FROM release_events "
                "WHERE release_id = ? ORDER BY seq",
                (release_id,),
            ).fetchall()
        data = self._row_to_dict(row)
        data["events"] = [
            {"ts": e["ts"], "phase": e["phase"], "actor": e["actor"],
             "payload": json.loads(e["payload_json"])}
            for e in events
        ]
        return data

    def list(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT release_id, status, author, approver, digest, batch_id, "
                "created_at, updated_at FROM releases ORDER BY created_at, release_id"
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_unfinished_undecided(self) -> list[str]:
        """重启后调用：执行中断的发布单标为 undecided，不重放、不虚报成功。"""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT release_id, status FROM releases WHERE status = 'executing'"
            ).fetchall()
            conn.execute(
                "UPDATE releases SET status = 'undecided', updated_at = ? "
                "WHERE status = 'executing'",
                (_now(),),
            )
        for row in rows:
            self._add_event(
                row["release_id"], "undecided", None,
                {"previous_status": row["status"],
                 "reason": "service restarted mid-execution"},
            )
        return [r["release_id"] for r in rows]


class ReleaseService:
    """发布单状态机：创建 -> 他人批准/拒绝 -> 作者撤销 -> 按批准方案执行。"""

    def __init__(
        self,
        settings: Settings,
        store: ReleaseStore,
        coordinator: BatchCoordinator,
    ) -> None:
        self._settings = settings
        self._store = store
        self._coordinator = coordinator

    def identify(self, credential) -> str:
        """凭据确认身份；未配置或不认识一律拒绝。"""
        person = self._settings.reviewers.get(credential or "")
        if person is None:
            raise UnknownCredential("unknown or missing reviewer credential")
        return person

    def _validate_plan(self, request: BatchRequest) -> None:
        unknown = [d.alias for d in request.databases
                   if d.alias not in self._settings.aliases]
        if unknown:
            raise ReleaseError("unknown alias: " + ", ".join(sorted(unknown)))
        seen = {}
        for item in request.databases:
            path = self._settings.aliases[item.alias]
            if path in seen:
                raise ReleaseError(
                    "aliases %r and %r resolve to the same database file"
                    % (seen[path], item.alias)
                )
            seen[path] = item.alias

    def create(self, author: str, request: BatchRequest) -> dict:
        """保存完整方案与内容摘要；提交后不可改，调整须新建。"""
        self._validate_plan(request)
        plan = {
            "databases": [
                {
                    "alias": d.alias,
                    "expected_version": d.expected_version,
                    "scripts": [
                        {"version": s.version, "description": s.description, "sql": s.sql}
                        for s in d.scripts
                    ],
                }
                for d in request.databases
            ]
        }
        return self._store.create(author, plan)

    def _get_or_raise(self, release_id: str) -> dict:
        order = self._store.get(release_id)
        if order is None:
            raise UnknownRelease(f"unknown release: {release_id}")
        return order

    def approve(self, release_id: str, reviewer: str, digest: str) -> dict:
        order = self._get_or_raise(release_id)
        if order["author"] == reviewer:
            raise SelfReviewForbidden("author cannot review their own release")
        if digest != order["digest"]:
            raise DigestMismatch(
                "digest mismatch: stored=%s submitted=%s" % (order["digest"], digest)
            )
        if not self._store.approve(release_id, reviewer):
            raise ReleaseStateError(
                "release %s is not pending approval (status=%s)"
                % (release_id, self._get_or_raise(release_id)["status"])
            )
        self._store.add_event(release_id, "approved", reviewer, {"digest": digest})
        return self._store.get(release_id)

    def reject(self, release_id: str, reviewer: str) -> dict:
        order = self._get_or_raise(release_id)
        if order["author"] == reviewer:
            raise SelfReviewForbidden("author cannot review their own release")
        if not self._store.reject(release_id, reviewer):
            raise ReleaseStateError(
                "release %s is not pending approval (status=%s)"
                % (release_id, self._get_or_raise(release_id)["status"])
            )
        self._store.add_event(release_id, "rejected", reviewer, {})
        return self._store.get(release_id)

    def cancel(self, release_id: str, author: str) -> dict:
        order = self._get_or_raise(release_id)
        if order["author"] != author:
            raise ReleaseStateError("only the author can cancel a release")
        if not self._store.cancel(release_id, author):
            raise ReleaseStateError(
                "release %s cannot be cancelled (status=%s)"
                % (release_id, self._get_or_raise(release_id)["status"])
            )
        self._store.add_event(release_id, "cancelled", author, {})
        return self._store.get(release_id)

    def execute(self, release_id: str):
        """按批准方案执行；只接受发布单 ID，方案以持久化内容为准。

        并发/重复执行最多产生一个批次：非首个执行者拿到已有结果或
        明确的状态冲突，绝不重复应用。失败不自动重跑。
        """
        order = self._get_or_raise(release_id)
        if not self._store.begin_execute(release_id):
            current = self._get_or_raise(release_id)
            if current["status"] in ("succeeded", "failed") and current["result"]:
                # 幂等：返回已有执行结果，不产生新批次。
                payload = dict(current["result"])
                payload["release_id"] = release_id
                payload["idempotent_replay"] = True
                return (200 if current["status"] == "succeeded" else 422), payload
            raise ReleaseStateError(
                "release %s is not approved for execution (status=%s)"
                % (release_id, current["status"])
            )
        self._store.add_event(release_id, "executing", None, {})

        def on_batch_created(batch_id: str) -> None:
            # 批次关联先落库，之后任何异常都不会丢失关联。
            self._store.attach_batch(release_id, batch_id)
            self._store.add_event(release_id, "batch_created", None, {"batch_id": batch_id})

        request = BatchRequest.model_validate(order["plan"])
        try:
            status_code, payload = self._coordinator.execute(
                request, on_batch_created=on_batch_created
            )
        except Exception as exc:
            # 协调器本身不抛业务异常；兜底保证状态与批次关联不丢。
            self._store.finish_execute(
                release_id, "undecided", {"error": "coordinator crashed: %s" % exc}
            )
            raise
        final = "succeeded" if payload.get("status") == "succeeded" else "failed"
        payload = dict(payload)
        payload["release_id"] = release_id
        self._store.finish_execute(release_id, final, payload)
        self._store.add_event(
            release_id, "finished", None,
            {"status": final, "batch_id": payload.get("batch_id")},
        )
        return status_code, payload

