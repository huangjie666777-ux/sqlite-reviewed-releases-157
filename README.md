# SQLite 版本迁移后端

基于 FastAPI + Python 3.10 标准库 `sqlite3` 的结构/数据迁移服务，供部署程序升级**已有的应用 SQLite 库**。HTTP 调用方只提交库别名、完整迁移清单和预期版本；真实文件路径只在服务端 `aliases.json` 中配置。

## 功能范围

- **别名映射**：`aliases.json` 把别名映射到库文件，HTTP 不接受路径。
- **清单校验**：每项含正整数 `version`、非空 `description`、非空 `sql`；版本必须从 1 连续递增，重复版本、空脚本拒绝；脚本数与请求体大小受限。
- **历史指纹**：库内 `__schema_migration_log__` 表保存版本、说明和原始 SQL 的 UTF-8 字节 SHA256。每次提交必须携带**完整已应用历史**且摘要一致，遗漏或改写一律拒绝；只执行未应用后缀，全部已应用时直接成功且不重执行。
- **单事务原子性**：未应用后缀的结构变更、数据变更和迁移记录在同一个 `BEGIN IMMEDIATE` 事务中提交，任何一条脚本失败整批回滚，响应返回 `failed_version` 与原因。
- **SQL 词法切分**（`app/sqlsplit.py`）：理解单/双引号、反引号、`[方括号]`、行/块注释（可嵌套）、括号深度以及 `CREATE TRIGGER ... BEGIN ... END` 体，正确处理字符串/注释中的分号和含多条语句的触发器体。
- **执行保护**（`app/guard.py`）：保护基于 SQLite 授权回调（`set_authorizer`），不是关键词文本搜索。禁止脚本使用事务控制（`BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE/END`）、`ATTACH/DETACH`、`PRAGMA`、`VACUUM`、非白名单函数（含 `load_extension`，扩展加载被阻断），禁止读写迁移记录表（包括在迁移记录表上建触发器等间接途径），限制只能访问主库。
- **外键约束**：连接强制 `PRAGMA foreign_keys = ON`，每条脚本执行后做 `PRAGMA foreign_key_check`，违规则回滚整批。
- **并发保护**：进程内每个别名一把锁串行化“版本检查 + 执行”；数据库层使用 `BEGIN IMMEDIATE` 与 `busy_timeout` 协调跨进程写锁。并发提交不会重复应用或绕过预期版本；忙（`database is locked`）返回 503，预期版本不匹配返回 409。
- **先锁后检**：迁移在 `BEGIN IMMEDIATE` 取得数据库写锁之后才检查预期版本与历史指纹，检查与执行之间不会被其它写入者改写。
- **延迟外键**：外键完整性在整批末尾统一 `PRAGMA foreign_key_check`，允许跨脚本“先破坏后修复”的合法序列；末尾仍违规则整批回滚并返回失败版本。
- **检查点**（`app/checkpoints.py`）：部署前可按别名创建整库一致性快照。快照经 SQLite 在线备份 API 生成，包含应用表、数据、索引、触发器、迁移记录以及已提交的 WAL 数据，不是复制主文件。ID 由服务生成，绑定别名、迁移版本、创建时间和快照 SHA256；目录（`catalog.json`）与快照文件持久保存在应用库之外的 `checkpoint_dir`，重启可查询，历史快照只增不改，临时/不完整快照不出现在列表中。
- **整库恢复**：按 ID 恢复，必须携带预期当前版本且只能恢复同一别名的检查点。恢复前校验快照 SHA256 与 `PRAGMA integrity_check`，在目标旁的临时文件重建并再次校验后原子替换原库；恢复后新增对象与数据消失，版本与历史回到检查点，快照与目录保留，可再次迁移。任何失败（未知 ID、别名不符、版本冲突、快照损坏、库忙）都明确拒绝且原库不变，不留半恢复库。
- **重启可查**：版本状态全部来自库内真实记录，服务重启后直接读取。
- **短连接**：每次请求使用独立连接并在结束后关闭（`contextlib.closing`）。
- **多库关联发布**（`app/batch.py`）：`POST /batches` 提交有序库列表，每项含配置别名、预期版本和完整迁移清单（与单库清单同一套校验）。空列表、重复别名、多个别名映射同一库文件、非法清单一律拒绝；HTTP 不接受宿主路径。协调器先对全部库做版本/历史/脚本检查并逐库创建检查点，**全部准备成功才按输入顺序迁移**；准备失败任何库都不升级。
- **失败补偿**：任一库迁移失败即停止后续库，已升级库按**逆序**恢复到各自检查点（结构、数据、迁移历史一起回退）；失败库保持原状态，未执行库不升级。单个补偿失败仍继续恢复其余库，响应逐库标注 `migrated/restored/restore_failed/failed/not_executed` 并保留错误与检查点 ID，部分补偿记为 `compensation_incomplete`，不会谎报为全部回滚。
- **互斥与单进程**：批次持有一把全局批次锁串行化，再按别名字典序一次性持有全部涉及库的锁，与单库迁移/检查点/恢复互斥；重叠批次不交叉执行，锁顺序一致不会死锁。发布期间应用停写由调用方配合，协调只保证单进程。
- **批次日志**：服务生成批次 ID，计划、每个准备/迁移/补偿步骤与最终结果逐步写入应用库之外的 `batch_dir/journal.db`（独立 SQLite，WAL）。`GET /batches`、`GET /batches/{id}` 可随时查询，重启后历史仍在；重启时发现未结束批次标记为 `undecided`，不自动重放 SQL、不宣称成功。
 - **双人审核发布单**（`app/release.py`）：固定方案须经另一人批准才能执行，避免偷换 SQL 上线。身份只来自服务端 `aliases.json` 的 `reviewers` 凭据映射（请求头 `X-Reviewer-Token`），不信任请求内署名。提交时保存有序库别名、预期版本、完整清单和原始 SQL，服务生成发布单 ID 与内容 SHA256；提交后不可改，调整须新建。待审单由他人（非作者）批准或拒绝，批准必须携带所查看的内容摘要，自我审核与摘要不符一律拒绝；作者可撤销未开始执行的单。并发批准/拒绝/撤销/执行通过条件更新保证一致终态，拒绝或撤销后不能执行。执行只提交发布单 ID，按批准方案调用既有批次协调器，版本与历史仍在库锁内检查，审批后库已变化会被拒绝；并发或重复执行最多产生一个批次，重复执行返回已有结果，失败不自动重跑。发布单、审核身份、决定、批次关联与结果持久化在应用库之外的 `release_dir/releases.db`，重启可查；执行中断标为 `undecided`，不重放 SQL、不虚报成功，批次关联在执行前落库不会丢失。
 - **审核模式开关**：`aliases.json` 的 `review_mode: true`（或 `MIGRATION_REVIEW_MODE`）启用后，`POST /databases/{alias}/migrate`、`POST /databases/{alias}/restore`、`POST /batches` 一律 403（`review_mode_required`），只能走 `/releases` 审批流，避免绕过；未启用时原接口行为不变。

## 目录结构

```
app/
  config.py      别名配置与限制常量
  manifest.py    清单模型与 SHA256
  sqlsplit.py    SQL 词法切分/首关键字
  guard.py       语句层禁止项 + SQLite 授权回调
  engine.py      版本读取、历史核对、事务化应用
  checkpoints.py 检查点快照、目录持久化与原子恢复
  batch.py       多库批次协调、批次日志持久化与失败补偿
  main.py        FastAPI 路由
  release.py     双人审核发布单：状态机、凭据身份、持久化与执行联动
scripts/make_example_db.py  生成示例库
examples/      演示用清单
tests/         pytest 测试（57 项）
aliases.json   别名 -> 库文件映射（路径相对于该文件）
```

## API

- `GET  /health`
- `GET  /databases/{alias}/version` — 当前版本和每条已应用记录（版本/说明/摘要）
- `POST /databases/{alias}/migrate` — 提交完整清单
- `POST /databases/{alias}/checkpoints` — 创建检查点，返回 `id/alias/version/created_at/sha256/size_bytes`
- `GET  /databases/{alias}/checkpoints` — 列出该别名可见检查点
- `POST /databases/{alias}/restore` — 按 ID 整库恢复，请求体 `{"checkpoint_id": "...", "expected_version": N}`
- `POST /batches` — 多库关联发布，请求体 `{"databases": [{"alias", "expected_version", "scripts": [...]}]}`；成功返回每库 `before_version/after_version/checkpoint_id`，失败返回逐库状态与补偿结果
- `GET  /batches` — 列出全部批次（ID、状态、时间）
- `GET  /batches/{batch_id}` — 批次详情：计划、逐步事件、最终结果
 - `POST /releases` — 提交发布单（头 `X-Reviewer-Token` 确认作者身份），请求体同 `/batches`；返回服务生成的 `release_id`、内容 `digest`、完整方案，状态 `pending_approval`
 - `GET  /releases` / `GET /releases/{release_id}` — 列表 / 详情（作者、审批人、摘要、批次关联、结果、事件流）
 - `POST /releases/{release_id}/approve` — 他人批准，请求体 `{"digest": "<所查看的内容摘要>"}`；自我审核 403、摘要不符 409
 - `POST /releases/{release_id}/reject` — 他人拒绝（终态）
 - `POST /releases/{release_id}/cancel` — 作者撤销未开始执行的单（终态）
 - `POST /releases/{release_id}/execute` — 只提交发布单 ID，按批准方案执行；重复执行返回已有结果（`idempotent_replay: true`），不产生新批次

请求体：

```json
{
  "expected_version": 0,
  "scripts": [
    {"version": 1, "description": "...", "sql": "ALTER TABLE ...; UPDATE ...;"}
  ]
}
```

错误码：`invalid_manifest`(422)、`invalid_batch`(422)、`invalid_release`(422)、`history_mismatch`(422)、`migration_failed`(422，含 `failed_version`/`reason`，禁用 SQL——包括含未闭合字符串的脚本——也走此码而非 500)、`version_conflict`(409)、`database_busy`(503)、`unknown_alias`(404)、`unknown_checkpoint`(404)、`unknown_batch`(404)、`unknown_release`(404)、`unknown_credential`(401)、`self_review_forbidden`(403)、`digest_mismatch`(409)、`release_state_conflict`(409)、`review_mode_required`(403)、`checkpoint_alias_mismatch`(409)、`checkpoint_corrupt`(422)、请求体超限 413。

## 运行

```bash
# 1. 生成示例库 data/demo.db 与 data/billing.db
.venv/bin/python scripts/make_example_db.py

# 2. 启动
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8011

# 3. 查询版本
curl -s http://127.0.0.1:8011/databases/demo/version

# 4. 成功升级（v1 加列 + v2 多语句触发器）
curl -s -X POST http://127.0.0.1:8011/databases/demo/migrate \
  -H 'Content-Type: application/json' --data @examples/manifests_demo.json

# 5. 幂等重放（expected_version=2）
curl -s -X POST http://127.0.0.1:8011/databases/demo/migrate \
  -H 'Content-Type: application/json' --data @examples/manifests_demo_resend.json

# 6. 失败回滚演示（v1 成功、v2 外键违反 -> 整批回滚）
curl -s -X POST http://127.0.0.1:8011/databases/billing/migrate \
  -H 'Content-Type: application/json' --data @examples/manifests_billing_fail.json

# 7. 篡改迁移记录表被拒
curl -s -X POST http://127.0.0.1:8011/databases/billing/migrate \
  -H 'Content-Type: application/json' --data @examples/manifests_forbidden.json

# 8. 部署前创建检查点（返回服务生成的 id）
curl -s -X POST http://127.0.0.1:8011/databases/demo/checkpoints

# 9. 列出检查点
curl -s http://127.0.0.1:8011/databases/demo/checkpoints

# 10. 升级后整库恢复到检查点（expected_version 为当前版本）
curl -s -X POST http://127.0.0.1:8011/databases/demo/restore \
  -H 'Content-Type: application/json' \
  -d `'{"checkpoint_id": "cp_...", "expected_version": 2}'`

# 11. 多库关联发布：demo + billing 一起升级（全部准备成功才迁移）
curl -s -X POST http://127.0.0.1:8011/batches \
  -H 'Content-Type: application/json' --data @examples/batch_success.json

# 12. 失败补偿演示：billing 外键违规，demo 已升级但被逆序恢复
curl -s -X POST http://127.0.0.1:8011/batches \
  -H 'Content-Type: application/json' --data @examples/batch_fail.json

# 13. 查询批次（重启后仍可追溯；未结束批次重启后标为 undecided）
curl -s http://127.0.0.1:8011/batches
curl -s http://127.0.0.1:8011/batches/batch_...
```

### 双人审核发布单（review_mode 启用时）

```bash
# 1. alice 凭据提交发布单（请求体同 /batches；身份只认 X-Reviewer-Token）
REL=$(curl -s -X POST http://127.0.0.1:8011/releases \
  -H 'Content-Type: application/json' -H 'X-Reviewer-Token: dev-token-alice' \
  --data @examples/batch_success.json)
RID=$(echo $REL | jq -r .release_id); DIGEST=$(echo $REL | jq -r .digest)

# 2. bob 查看方案后携带内容摘要批准（自我审核 403、摘要不符 409）
curl -s -X POST http://127.0.0.1:8011/releases/$RID/approve \
  -H 'Content-Type: application/json' -H 'X-Reviewer-Token: ops-token-bob' \
  -d "{\"digest\": \"$DIGEST\"}"

# 3. 执行只提交发布单 ID；重复执行返回已有结果，不产生新批次
curl -s -X POST http://127.0.0.1:8011/releases/$RID/execute \
  -H 'X-Reviewer-Token: dev-token-alice'

# 4. 查询（重启后仍可追溯；执行中断的重启后标为 undecided）
curl -s http://127.0.0.1:8011/releases/$RID
```

审核模式启用时，`POST /databases/{alias}/migrate`、`POST /databases/{alias}/restore`、`POST /batches` 返回 403 `review_mode_required`。

## 测试

```bash
.venv/bin/python -m pytest -q
```

覆盖：词法切分（字符串/注释分号、触发器多语句、CASE..END）、历史摘要核对、遗漏/改写拒绝、预期版本冲突、整批回滚、外键违反回滚、禁止语句与禁止函数、未闭合字符串折算为业务错误、迁移记录表写入/建触发器拒绝、请求体限制、重启读取真实记录、多库批次成功/失败补偿/未执行库/准备失败不升级/校验拒绝/补偿不完全不谎报/批次日志重启未决标记、补偿中文件 IO 异常不中断其余库恢复且保留真实错误、发布单创建/凭据身份/自我审核与摘要不符拒绝/批准执行/幂等重执行/拒绝与撤销终态/并发决定唯一终态/审批后库变化拒绝执行/审核模式关闭直接入口/发布单重启持久化与未决标记。

## 可调环境变量

- `MIGRATION_CONFIG`（默认 `aliases.json`）
- `MIGRATION_MAX_REQUEST_BYTES`（默认 2 MiB）
- `MIGRATION_MAX_SCRIPTS`（默认 200）
- `MIGRATION_SQLITE_TIMEOUT`（busy_timeout，默认 5 秒）
- `MIGRATION_CHECKPOINT_DIR`（检查点目录，默认 `<配置目录>/checkpoints`，必须在应用库之外）
- `MIGRATION_BATCH_DIR`（批次日志目录，默认 `<配置目录>/batches`，必须在应用库之外）
 - `MIGRATION_RELEASE_DIR`（发布单目录，默认 `<配置目录>/releases`，必须在应用库之外）
 - `MIGRATION_REVIEW_MODE`（双人审核开关，`1/true/yes/on` 启用；也可用 aliases.json 的 `review_mode`）

`aliases.json` 审核配置示例：

```json
{
  "aliases": {"demo": "data/demo.db", "billing": "data/billing.db"},
  "review_mode": true,
  "reviewers": {"dev-token-alice": "alice", "ops-token-bob": "bob"}
}
```

`reviewers` 是 凭据 -> 人员 的映射，凭据即身份；启用审核模式至少配置两人。

## 说明与边界

- 授权回调在 SQLite 解析期工作，能同时约束普通语句和触发器体引用；内部执行的 `foreign_key_check` 与迁移记录写入通过临时切换全放行回调完成。
- 函数采用白名单以阻断 `load_extension`；若脚本需要其它内建函数，在 `app/guard.py` 的 `ALLOWED_FUNCTIONS` 中登记。
- 迁移脚本面向应用表的 DDL/DML；纯 `SELECT` 无副作用，被拒绝。
- 进程内锁只覆盖单进程；多进程部署时靠 `BEGIN IMMEDIATE` 与 busy_timeout 保证跨进程串行，冲突时调用方应按 409/503 重试。
