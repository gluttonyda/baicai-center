# Baicai Center

Baicai Center 是一个用于分布式 Google Drive 搬运的主控调度系统。

本次版本变化见 [中文更新日志](CHANGELOG.md)、[2026-10-21 依赖拆分与运行时加固说明](docs/CODE_CHANGES_20261021_runtime.md)、[2026-10-21 调度互斥与鉴权加固说明](docs/CODE_CHANGES_20261021.md) 和 [2026-09-14 代码差异说明](docs/CODE_CHANGES_20260914.md)。

它的核心目标是让一个中心节点充当“主控大脑”，统一接收文件清单、管理账号池、调度多个地区的 Worker 节点，并在 Google Drive 上传配额触发后自动切换账号继续搬运。

> 本项目不是 Google 官方项目。它依赖 `rclone` 和你自己的 Google Drive / Shared Drive / Service Account / OAuth 配置。

## 适用场景

- 多地区服务器共同搬运同一批文件
- 多个 Google Drive 账号轮换上传
- 单账号触发 403 upload limit 后自动冷却并切换账号
- 先搬到中转盘，再由专用 cleaner 账号清理到最终大盘
- 大盘已有文件先做历史同步，避免重复搬运

## 架构概览

```text
                    +----------------------+
                    |      Center Node     |
                    |  FastAPI + Scheduler |
                    |        MySQL         |
                    +----------+-----------+
                               |
             dispatch task     | report status / files
                               |
        +----------------------+----------------------+
        |                      |                      |
+-------+--------+     +-------+--------+     +-------+--------+
| Worker Node A  |     | Worker Node B  |     | Worker Node C  |
| region/account |     | region/account |     | region/account |
| scanner+rclone |     | scanner+rclone |     | scanner+rclone |
+-------+--------+     +-------+--------+     +-------+--------+
        |
        | rclone copy / move
        v
 uploader staging drives  ->  cleaner accounts  ->  final target drive
```

## 核心组件

### Center

根目录是 Center 节点代码：

- `main.py`：FastAPI API、任务调度器、账号锁、状态机、cleaner 派发
- `init_sync.py`：把目标盘已有文件注入数据库，建立防重复屏障
- `SCHEMA_MINIMAL.sql`：最小可运行表结构
- `MIGRATION_SPLIT_ACCOUNT_LOCKS.sql`：账号锁字段拆分迁移
- `MIGRATION_DISPATCH_PROTOCOL_V2.sql`：节点签名、可恢复调度租约、来源指纹迁移
- `MIGRATION_FILE_PATH_HASH.sql`：补齐 `file_tasks.file_path_hash` 及完整唯一索引；遇到重复路径、不兼容的已有列或前缀唯一索引时终止且不改数据
- `scripts/`：数据库检查和维护脚本

Center 维护以下状态：

- `file_tasks`：文件任务状态
- `drive_accounts`：上传账号、cleaner 账号、账号锁和配额封禁
- `worker_registry`：Worker 节点注册地址和活跃时间
- `account_upload_events`：按文件和派发批次去重的上传成功事件

### Worker

`baicai-work/` 是 Worker 节点运行包：

- `worker.py`：接收 Center 派发，执行 `rclone copy/move`
- `scanner.py`：扫描本地目录并上报 Center
- `scanner-loop.sh`：容器内定时扫描入口
- `docker-compose.yml`：Worker API + Scanner 部署模板

Worker 默认监听：

```text
/task/upload
```

Center 默认暴露：

```text
GET  /
GET  /health/ready
POST /api/report_files
POST /api/report_status
POST /api/dispatch/validate
GET  /api/upload_usage
GET  /api/upload_usage/daily
```

`/api/upload_usage` 会返回每个 uploader 账号在最近 24 小时内的上传量，并附带当前锁状态与 `unlock_at_*` 解锁时间。

## 搬运流程

1. Scanner 扫描 Worker 本地目录。
2. Scanner 调用 Center 的 `/api/report_files` 上报待搬运文件。
3. Center 将文件写入 `file_tasks`，初始状态为 `pending`。
4. Center 定时选择可用 Worker 和可用 uploader 账号。
5. Worker 收到任务后用 `rclone copy` 或 `rclone move` 执行搬运。
6. 普通 uploader 成功后，任务进入 `staged`。
7. Cleaner 账号把本批 `staged` 文件按清单从中转盘移动到最终目标盘（`rclone move --files-from-raw`），中转目录里不在清单中的文件保持不动。
8. Cleaner 成功后，任务进入 `completed`。

任务状态：

```text
pending -> uploading -> staged -> completed
                    \-> pending / not_found
```

`403_quota`、`credential_invalid` 和 `failed` 是 Worker 回报类型，不是新增任务状态；Center 根据回报更新账号状态，并将尚未完成的任务重新排队。直传账号成功后直接进入 `completed`。

## 账号与锁

账号分为：

- `uploader`：执行上传或移动
- `cleaner`：把中转盘文件清理到目标盘
- `vip`：预留角色

账号锁拆分为：

- `dispatch_locked_until`：短期调度执行锁，防止同一账号被重复派发
- `dispatch_cooldown_until`：派发或清道夫失败后的调度冷却
- `quota_blocked_until`：403 quota 后的封禁锁，默认 24 小时

当 Worker 上报 `403_quota`：

1. Center 释放短期调度锁。
2. Center 写入 `quota_blocked_until`。
3. 当前未完成任务回退到 `pending`。
4. Center 立即触发新一轮 uploader 调度。即时调度与每 60 秒的定时调度共用互斥，不会同时运行；多个 Center 进程之间另有 MySQL `GET_LOCK` 保护（只是抢不到就跳过，没有实现选主）。

Cleaner 策略：

- 同组仍有 `uploading` 时绝不清理
- 同组 uploader 全部 quota 封禁时立即清理
- 未全部封禁时，等待空闲窗口达到 `CLEANER_IDLE_MINUTES` 后清理

## 快速开始

部署前置：Center 需要可访问的 MySQL 8.x；Center 和 Worker 均需要 Docker Engine 与 Compose 插件。请先初始化或迁移数据库再启动 Center，否则 `/health/ready` 会保持失败，容器不会被标记为 healthy。

### 1. 准备 Center 配置

```bash
cp .env.example .env
```

至少填写：

```text
API_TOKEN
WORKER_TOKEN_MAP
DB_HOST
DB_USER
DB_PASSWORD
DB_NAME
WORKER_API_URL_MAP
```

### 2. 初始化数据库

首次部署可参考：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < SCHEMA_MINIMAL.sql
```

如果是旧库升级，先停止 Center 和 Scanner，再执行：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_UPLOAD_USAGE_STATS.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_DISPATCH_PROTOCOL_V2.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_FILE_PATH_HASH.sql
```

以上迁移检查缺失字段和索引后再添加，可以重复执行。执行前仍需备份；MySQL DDL 可能隐式提交，不能依靠单个事务回滚整个升级。迁移不会自动解释或清空旧账号的配额封禁与人工停用状态。如果 `file_tasks` 中存在完全相同的 `file_path` 多行，`MIGRATION_FILE_PATH_HASH.sql` 会以 `MIGRATION_ABORTED_duplicate_file_path_rows_see_docs` 错误终止，不删除任何数据。已有 `file_path_hash` 列不是 `CHAR(64) AS (SHA2(file_path, 256)) STORED`，或该列上存在前缀/表达式唯一索引（例如 `UNIQUE (file_path_hash(1))`，会把完整哈希不同的路径判为重复）时，脚本分别以 `MIGRATION_ABORTED_incompatible_file_path_hash_column_see_docs`、`MIGRATION_ABORTED_prefix_unique_file_path_hash_index_see_docs` 终止，不覆盖列、不删除索引，`/health/ready` 也返回 503。人工处理方法见 [Center 节点说明](docs/center_node.md#file_path_hash-迁移)。

数据库准备完成后启动 Center，并检查数据库就绪状态：

```bash
docker compose up -d --build
curl --fail http://127.0.0.1:8080/health/ready
```

### 3. 建立历史防重复屏障

如果目标盘已经有大量文件，先同步历史文件：

```bash
python init_sync.py
```

执行前确认：

```text
WORKER_NODE_NAME=Init_Scanner
RCLONE_REMOTE_ACCOUNT=your-rclone-remote
TARGET_DIR=your-target-dir
```

不要用真实 Worker 节点名运行 `init_sync.py`，否则可能覆盖真实 Worker 的注册地址。

使用协议 v2 时，还需为 `Init_Scanner` 在 `WORKER_TOKEN_MAP` 中单独配置密钥，并给导入进程设置匹配的 `NODE_TOKEN`（`init_sync.py` 不需要 `API_TOKEN`）。Center 只接受 `HISTORY_SYNC_NODE_NAMES`（默认 `Init_Scanner`）中的签名节点提交历史同步，其他签名节点会收到 403；legacy 管理员 Bearer 仍保持兼容。

### 4. 部署 Worker

进入 Worker 包：

```bash
cd baicai-work
cp .env.example .env
```

至少填写：

```text
CENTER_URL
CENTER_API
WORKER_NODE_NAME
WORKER_API_URL
TARGET_REMOTE_DIR
NODE_TOKEN
```

HMAC-only 部署不需要 `API_TOKEN`。只有开启 `ALLOW_LEGACY_BEARER_AUTH=true` 或 `SCANNER_SEND_BEARER_AUTH=true` 时才必须填写与 Center 一致的非空值，否则启动失败。

Center 使用 `STAGING_REMOTE_DIR`、`LEGACY_DIRECT_REMOTE_DIR` 和
`CLEANER_DEST_REMOTE` 决定每个任务的实际远端。当前路由为：普通中转账号与
`baicai_big2@1-3` 使用 `LongEmby-B`，旧 `baicai_big@1-3` 使用
`LongEmby-A`，清道夫统一落盘到 `baicai-big2`。

启动前确认 `/data/links`、`/cache/links`、`/data/rclone-cache` 和 rclone 配置在宿主机存在。Scanner 会分别上报这三个根目录，Worker 的 `ALLOWED_BASE_DIRS` 必须包含相同目录。

`NODE_TOKEN` 必须与 Center 的 `WORKER_TOKEN_MAP` 中当前 `WORKER_NODE_NAME` 对应值一致，并至少使用 32 个随机字符。节点签名依赖 Unix 时间戳，Center 与所有 Worker 都应启用 NTP；滚动升级期间可暂时保留 `ALLOW_LEGACY_NODE_AUTH=true` 和 `SCANNER_SEND_BEARER_AUTH=true`，确认旧节点排空后再关闭兼容认证。

启动 Worker：

```bash
docker compose up -d --build
```

Worker 会把已接收任务和待补送的最终回执持久化到 `runtime/task_journal.db`。容器或宿主机重启后，Worker 会先调用 Center 校验 dispatch 租约，只恢复仍归属于本节点的任务；过期租约会取消，并终止仍在运行的旧 `rclone`。普通升级或重启不要删除 `runtime/task_journal.db`。

Uploader 的单进程并发由 `RCLONE_TRANSFERS` 和 `RCLONE_CHECKERS` 控制，默认为 `4/8`。在每节点最多两个 uploader 进程时，本地阵列最多承受 8 路文件数据流。`RCLONE_BWLIMIT` 是单进程限速，不是节点总限速。

Worker 暂时看不到文件时会回报 `not_found`。Center 默认等待 `NOT_FOUND_RETRY_MINUTES=60` 后重新排队，避免 Scanner 指纹缓存不变时任务永久滞留。

如果 Center 数据库重置过，清理 Worker 本地扫描缓存：

```bash
rm -f runtime/scanner_cache.db
```

只有 Center 数据库被完整重置时才应删除 Scanner 缓存。`runtime/task_journal.db` 仍应保留，让 Worker 对旧任务完成租约校验和收口。

### 5. 清理遗留 `.clfstmp`

维护脚本默认预览超过 24 小时的候选项。请显式选择要检查的 remote：

```bash
python scripts/cleanup_drive_clfstmp.py --remote your-rclone-remote
```

核对候选项后，显式执行永久删除：

```bash
python scripts/cleanup_drive_clfstmp.py --remote your-rclone-remote --apply --min-age-hours 24
```

可通过 `--config` 指定 rclone 配置文件；重复 `--remote` 只适用于访问同一共享盘的账号。
本地清单与数据库的只读核对示例：

```bash
python scripts/compare_worker_cache_paths.py --worker-node worker-node-1 --local-list tmp/local-paths.txt
```

这些工具的输出可能包含真实文件路径，应留在本地。账号 SQL 生成器只输出项目命名约定下的账号和映射，重复导入保留已有启停与配额状态；现场停用策略需单独维护。

## 测试

依赖分为两份：`requirements.txt` 只包含运行时依赖，Center 镜像只安装这一份；`requirements-dev.txt` 包含 pytest 等开发和测试依赖。开发机同时安装两者，测试需要同时把根目录和 `baicai-work/` 放进 `PYTHONPATH`：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
PYTHONPATH=.:baicai-work pytest -q
```

Center 镜像以非 root 用户 `baicai`（uid/gid 10001）运行；Worker 镜像暂时仍以 root 运行，原因见 [2026-10-21 依赖拆分与运行时加固说明](docs/CODE_CHANGES_20261021_runtime.md#worker-仍以-root-运行)。

`tests/test_mysql_integration.py` 需要真实 MySQL 8。未设置下列环境变量时，这些用例显示为 skipped，不会失败：

```bash
BAICAI_TEST_MYSQL_HOST=127.0.0.1 \
BAICAI_TEST_MYSQL_USER=root \
BAICAI_TEST_MYSQL_PASSWORD='<测试库密码>' \
BAICAI_TEST_MYSQL_DATABASE=baicai_review \
PYTHONPATH=.:baicai-work pytest -q tests/test_mysql_integration.py
```

每个用例都会创建 `review_` 前缀的临时库并在结束后删除，测试账号需要建库和删库权限，不要指向生产实例。`tests/test_worker_integration.py` 的 2 个 rclone 用例会调用 `PATH` 中的真实 `rclone`（建议与生产一致使用 1.65 及以上，只访问本地临时目录）；找不到 `rclone` 时显示为 skipped，`pytest -ra` 会输出安装方式。发布前应在有 rclone 的环境中确认它们是 passed。

`scripts/reset_db.py` 会清空任务数据，必须显式确认：`--dry-run` 只读预览，`--yes` 用于非交互环境；不传 `--yes` 时只有在交互终端中输入数据库名才会执行。用法与兼容影响见 [变更说明](docs/CODE_CHANGES_20261021_runtime.md#scriptsreset_dbpy)。

测试数量会随协议和回归用例调整，以 `pytest` 最终输出为准，不在文档中维护容易失真的固定数字。

## 安全说明

不要提交真实运行配置：

- `.env`
- `baicai-work/.env`
- `rclone.conf`
- 数据库备份
- 运行日志
- 重复文件清单
- 本地扫描缓存
- Worker 任务 journal 及 SQLite 旁路文件
- GitHub 登录凭据和部署专用的一次性 SQL

本仓库已通过 `.gitignore` 忽略这些内容。公开仓库中只应保留 `.env.example`。

`WORKER_API_URL_MAP` 为空时，Center 会信任节点上报的 `worker_api_url` 并向该地址发起请求。为兼容现有部署，空映射仍允许启动；生产环境应配置完整白名单。

## 文档

- [文档索引](docs/README.md)
- [Center 节点说明](docs/center_node.md)
- [Worker 节点说明](docs/worker_node.md)
- [部署分类说明](docs/部署分类说明.md)
- [配置任务清单](docs/配置任务清单.md)

## License

当前仓库尚未声明开源协议。公开前请根据你的使用意图补充 `LICENSE`。
