"""FastAPI 入口：提交迁移清单 / 查询当前版本 / 检查点与整库恢复 / 多库批次发布 / 双人审核发布单。"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from .checkpoints import (
    CheckpointAliasMismatch,
    CheckpointCorrupt,
    CheckpointStore,
    UnknownCheckpoint,
)
from .batch import BatchCoordinator, BatchJournal, BatchRequest, UnknownBatch
from .config import MAX_REQUEST_BYTES, load_settings
from .engine import (
    DatabaseBusy,
    DatabaseRegistry,
    HistoryMismatch,
    MigrationError,
    ScriptFailed,
    VersionConflict,
    apply_manifest,
    status,
)
from .manifest import MigrationManifest
from .release import (
    DigestMismatch,
    ReleaseError,
    ReleaseService,
    ReleaseStateError,
    ReleaseStore,
    SelfReviewForbidden,
    UnknownCredential,
    UnknownRelease,
)

settings = load_settings()
registry = DatabaseRegistry()
checkpoints = CheckpointStore(settings.checkpoint_dir)
batch_journal = BatchJournal(settings.batch_dir)
batches = BatchCoordinator(settings, registry, checkpoints, batch_journal)
# 重启后发现未结束批次：标为未决，不自动重放 SQL、不宣称成功。
batch_journal.mark_unfinished_undecided()
release_store = ReleaseStore(settings.release_dir)
releases = ReleaseService(settings, release_store, batches)
# 重启后发现执行中断的发布单：标为未决，不重放 SQL、不虚报成功。
release_store.mark_unfinished_undecided()

app = FastAPI(title="SQLite Migration Backend", version="1.0.0")


@app.middleware("http")
async def limit_body(request: Request, call_next):
    declared = request.headers.get("content-length")
    if declared and int(declared) > MAX_REQUEST_BYTES:
        return JSONResponse(
            status_code=413,
            content={"detail": f"request body too large (> {MAX_REQUEST_BYTES} bytes)"},
        )
    response = await call_next(request)
    return response


@app.exception_handler(MigrationError)
async def _migration_error_handler(_: Request, exc: MigrationError) -> JSONResponse:
    if isinstance(exc, VersionConflict):
        status_code = 409
        code = "version_conflict"
    elif isinstance(exc, HistoryMismatch):
        status_code = 422
        code = "history_mismatch"
    elif isinstance(exc, DatabaseBusy):
        status_code = 503
        code = "database_busy"
    elif isinstance(exc, ScriptFailed):
        status_code = 422
        code = "migration_failed"
    elif isinstance(exc, UnknownCheckpoint):
        status_code = 404
        code = "unknown_checkpoint"
    elif isinstance(exc, CheckpointAliasMismatch):
        status_code = 409
        code = "checkpoint_alias_mismatch"
    elif isinstance(exc, CheckpointCorrupt):
        status_code = 422
        code = "checkpoint_corrupt"
    elif isinstance(exc, UnknownBatch):
        status_code = 404
        code = "unknown_batch"
    elif isinstance(exc, UnknownRelease):
        status_code = 404
        code = "unknown_release"
    elif isinstance(exc, UnknownCredential):
        status_code = 401
        code = "unknown_credential"
    elif isinstance(exc, SelfReviewForbidden):
        status_code = 403
        code = "self_review_forbidden"
    elif isinstance(exc, DigestMismatch):
        status_code = 409
        code = "digest_mismatch"
    elif isinstance(exc, ReleaseStateError):
        status_code = 409
        code = "release_state_conflict"
    elif isinstance(exc, ReleaseError):
        status_code = 422
        code = "invalid_release"
    else:
        status_code = 400
        code = "migration_error"
    payload = {"detail": str(exc), "code": code}
    if isinstance(exc, ScriptFailed):
        payload["failed_version"] = exc.version
        payload["reason"] = exc.reason
    return JSONResponse(status_code=status_code, content=payload)


def _resolve(alias: str):
    db_path = settings.aliases.get(alias)
    if db_path is None:
        return JSONResponse(
            status_code=404, content={"detail": f"unknown alias: {alias}", "code": "unknown_alias"}
        )
    return db_path


def _review_mode_closed() -> JSONResponse:
    """审核模式启用时，直接迁移/批次/恢复入口关闭，避免绕过审批。"""
    return JSONResponse(
        status_code=403,
        content={
            "detail": "review mode enabled: use /releases workflow instead",
            "code": "review_mode_required",
        },
    )


def _identity(request: Request):
    """凭据确认身份；失败时返回 401 响应。"""
    try:
        return releases.identify(request.headers.get("x-reviewer-token"))
    except UnknownCredential as exc:
        return JSONResponse(
            status_code=401,
            content={"detail": str(exc), "code": "unknown_credential"},
        )


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "aliases": sorted(settings.aliases)}


@app.get("/databases/{alias}/version")
async def get_version(alias: str):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    return status(resolved)


@app.post("/databases/{alias}/migrate")
async def migrate(alias: str, request: Request):
    if settings.review_mode:
        return _review_mode_closed()
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    raw = await request.body()
    if len(raw) > MAX_REQUEST_BYTES:
        return JSONResponse(
            status_code=413,
            content={"detail": f"request body too large (> {MAX_REQUEST_BYTES} bytes)"},
        )
    try:
        manifest = MigrationManifest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_manifest"},
        )
    result = apply_manifest(resolved, manifest, registry.lock_for(alias))
    return {
        "alias": alias,
        "before_version": result.before_version,
        "after_version": result.after_version,
        "applied_versions": result.applied,
        "already_applied": not result.applied,
    }


class RestoreRequest(BaseModel):
    checkpoint_id: str = Field(min_length=1)
    expected_version: int = Field(ge=0)


@app.post("/databases/{alias}/checkpoints", status_code=201)
async def create_checkpoint(alias: str):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    meta = checkpoints.create(alias, resolved, registry.lock_for(alias))
    return meta


@app.get("/databases/{alias}/checkpoints")
async def list_checkpoints(alias: str):
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    return {"alias": alias, "checkpoints": checkpoints.list(alias)}


@app.post("/databases/{alias}/restore")
async def restore_checkpoint(alias: str, request: Request):
    if settings.review_mode:
        return _review_mode_closed()
    resolved = _resolve(alias)
    if isinstance(resolved, JSONResponse):
        return resolved
    raw = await request.body()
    try:
        payload = RestoreRequest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_restore_request"},
        )
    result = checkpoints.restore(
        alias,
        resolved,
        payload.checkpoint_id,
        payload.expected_version,
        registry.lock_for(alias),
    )
    return {
        "alias": alias,
        "checkpoint_id": result["checkpoint"]["id"],
        "before_version": result["before_version"],
        "after_version": result["after_version"],
    }


@app.post("/batches")
async def submit_batch(request: Request):
    """多库关联发布：统一准备、按序迁移、失败逆序补偿。"""
    if settings.review_mode:
        return _review_mode_closed()
    raw = await request.body()
    if len(raw) > MAX_REQUEST_BYTES:
        return JSONResponse(
            status_code=413,
            content={"detail": f"request body too large (> {MAX_REQUEST_BYTES} bytes)"},
        )
    try:
        batch_request = BatchRequest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_batch"},
        )
    status_code, payload = batches.execute(batch_request)
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/batches")
async def list_batches() -> dict:
    return {"batches": batch_journal.list()}


@app.get("/batches/{batch_id}")
async def get_batch(batch_id: str):
    detail = batch_journal.get(batch_id)
    if detail is None:
        return JSONResponse(
            status_code=404,
            content={"detail": f"unknown batch: {batch_id}", "code": "unknown_batch"},
        )
    return detail


class ApproveRequest(BaseModel):
    digest: str = Field(min_length=1)


@app.post("/releases", status_code=201)
async def create_release(request: Request):
    """提交发布单：保存有序库、预期版本、完整清单与原始 SQL，生成 ID 与内容摘要。"""
    person = _identity(request)
    if isinstance(person, JSONResponse):
        return person
    raw = await request.body()
    if len(raw) > MAX_REQUEST_BYTES:
        return JSONResponse(
            status_code=413,
            content={"detail": f"request body too large (> {MAX_REQUEST_BYTES} bytes)"},
        )
    try:
        plan = BatchRequest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_release"},
        )
    try:
        order = releases.create(person, plan)
    except ReleaseError as exc:
        code = "unknown_alias" if str(exc).startswith("unknown alias") else "invalid_release"
        return JSONResponse(
            status_code=404 if code == "unknown_alias" else 422,
            content={"detail": str(exc), "code": code},
        )
    return order


@app.get("/releases")
async def list_releases() -> dict:
    return {"releases": release_store.list()}


@app.get("/releases/{release_id}")
async def get_release(release_id: str):
    order = release_store.get(release_id)
    if order is None:
        return JSONResponse(
            status_code=404,
            content={"detail": f"unknown release: {release_id}", "code": "unknown_release"},
        )
    return order


@app.post("/releases/{release_id}/approve")
async def approve_release(release_id: str, request: Request):
    """他人批准：必须携带所查看的内容摘要，拒绝自我审核与摘要不符。"""
    person = _identity(request)
    if isinstance(person, JSONResponse):
        return person
    raw = await request.body()
    try:
        payload = ApproveRequest.model_validate_json(raw)
    except ValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors()), "code": "invalid_approve_request"},
        )
    return releases.approve(release_id, person, payload.digest)


@app.post("/releases/{release_id}/reject")
async def reject_release(release_id: str, request: Request):
    person = _identity(request)
    if isinstance(person, JSONResponse):
        return person
    return releases.reject(release_id, person)


@app.post("/releases/{release_id}/cancel")
async def cancel_release(release_id: str, request: Request):
    """作者撤销未开始执行的单。"""
    person = _identity(request)
    if isinstance(person, JSONResponse):
        return person
    return releases.cancel(release_id, person)


@app.post("/releases/{release_id}/execute")
async def execute_release(release_id: str, request: Request):
    """只提交发布单 ID，按批准方案执行；重复执行返回已有结果。"""
    person = _identity(request)
    if isinstance(person, JSONResponse):
        return person
    status_code, payload = releases.execute(release_id)
    return JSONResponse(status_code=status_code, content=payload)
