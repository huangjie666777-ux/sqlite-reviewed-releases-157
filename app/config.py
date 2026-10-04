"""Runtime configuration loaded from aliases.json.


HTTP 调用方只提交数据库别名，真实文件路径只在服务端配置，

避免客户端借迁移接口操作任意 SQLite 文件。

"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

MAX_REQUEST_BYTES = int(os.environ.get("MIGRATION_MAX_REQUEST_BYTES", 2 * 1024 * 1024))
MAX_SCRIPTS = int(os.environ.get("MIGRATION_MAX_SCRIPTS", 200))
SQLITE_BUSY_TIMEOUT_SECONDS = float(os.environ.get("MIGRATION_SQLITE_TIMEOUT", 5))

# 服务端内部保存迁移历史的表名。脚本对该表的任何读写都会被拒绝。
MIGRATION_TABLE = "__schema_migration_log__"


@dataclass(frozen=True)
class Settings:
    aliases: dict[str, Path]
    checkpoint_dir: Path
    batch_dir: Path
    release_dir: Path
    review_mode: bool
    reviewers: dict[str, str]


def load_settings(path: str | os.PathLike[str] | None = None) -> Settings:
    cfg_path = Path(path or os.environ.get("MIGRATION_CONFIG", "aliases.json"))
    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    aliases: dict[str, Path] = {}
    for alias, db_path in raw["aliases"].items():
        if not isinstance(alias, str) or not alias.strip():
            raise ValueError(f"invalid alias: {alias!r}")
        p = Path(db_path)
        if not p.is_absolute():
            p = cfg_path.parent / p
        aliases[alias] = p.resolve()
    if not aliases:
        raise ValueError("aliases.json must define at least one alias")
    # 检查点目录必须在应用库之外；默认 <配置目录>/checkpoints，可用
    # aliases.json 的 "checkpoint_dir" 或 MIGRATION_CHECKPOINT_DIR 覆盖。
    raw_dir = os.environ.get("MIGRATION_CHECKPOINT_DIR") or raw.get("checkpoint_dir")
    if raw_dir:
        checkpoint_dir = Path(raw_dir)
        if not checkpoint_dir.is_absolute():
            checkpoint_dir = cfg_path.parent / checkpoint_dir
    else:
        checkpoint_dir = cfg_path.parent / "checkpoints"
    checkpoint_dir = checkpoint_dir.resolve()
    for alias, db_path in aliases.items():
        if db_path == checkpoint_dir or checkpoint_dir in db_path.parents:
            raise ValueError(
                f"checkpoint_dir must not contain database files: {alias}"
            )
    # 批次日志目录同样在应用库之外；默认 <配置目录>/batches，可用
    # aliases.json 的 "batch_dir" 或 MIGRATION_BATCH_DIR 覆盖。
    raw_batch = os.environ.get("MIGRATION_BATCH_DIR") or raw.get("batch_dir")
    if raw_batch:
        batch_dir = Path(raw_batch)
        if not batch_dir.is_absolute():
            batch_dir = cfg_path.parent / batch_dir
    else:
        batch_dir = cfg_path.parent / "batches"
    batch_dir = batch_dir.resolve()
    for alias, db_path in aliases.items():
        if db_path == batch_dir or batch_dir in db_path.parents:
            raise ValueError(f"batch_dir must not contain database files: {alias}")
    # 发布单存储目录同样在应用库之外；默认 <配置目录>/releases，可用
    # aliases.json 的 "release_dir" 或 MIGRATION_RELEASE_DIR 覆盖。
    raw_release = os.environ.get("MIGRATION_RELEASE_DIR") or raw.get("release_dir")
    if raw_release:
        release_dir = Path(raw_release)
        if not release_dir.is_absolute():
            release_dir = cfg_path.parent / release_dir
    else:
        release_dir = cfg_path.parent / "releases"
    release_dir = release_dir.resolve()
    for alias, db_path in aliases.items():
        if db_path == release_dir or release_dir in db_path.parents:
            raise ValueError(f"release_dir must not contain database files: {alias}")
    # 双人审核模式：aliases.json 的 "review_mode" 或 MIGRATION_REVIEW_MODE 启用；
    # "reviewers" 配置 凭据 -> 人员 映射，凭据即身份，不信任请求内署名。
    raw_mode = os.environ.get("MIGRATION_REVIEW_MODE")
    if raw_mode is None:
        review_mode = bool(raw.get("review_mode", False))
    else:
        review_mode = raw_mode.strip().lower() in ("1", "true", "yes", "on")
    reviewers: dict[str, str] = {}
    for token, person in (raw.get("reviewers") or {}).items():
        if not isinstance(token, str) or not token.strip():
            raise ValueError(f"invalid reviewer credential: {token!r}")
        if not isinstance(person, str) or not person.strip():
            raise ValueError(f"invalid reviewer name for credential: {token!r}")
        reviewers[token] = person
    if review_mode and len(reviewers) < 2:
        raise ValueError("review_mode requires at least two reviewers in aliases.json")
    return Settings(
        aliases=aliases,
        checkpoint_dir=checkpoint_dir,
        batch_dir=batch_dir,
        release_dir=release_dir,
        review_mode=review_mode,
        reviewers=reviewers,
    )
