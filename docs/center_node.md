# Center 节点说明

Center 是 Baicai 的主控大脑，负责 API、数据库状态、账号锁和任务调度。

## Center 负责什么

- 接收 Worker 扫描到的文件清单
- 把文件写入 `file_tasks`
- 维护 Worker 注册信息
- 从账号池里选择可用 uploader
- 向 Worker 派发上传任务
- 接收 Worker 状态回报
- 处理 quota 封禁和凭据失效
- 派发 cleaner，把中转盘文件清理到最终目标盘

## 核心文件

根目录中的 Center 文件：

- `main.py`：Center API 与调度器
- `init_sync.py`：目标盘历史文件同步脚本
- `Dockerfile`：Center 镜像构建
- `docker-compose.yml`：Center 容器启动配置
- `requirements.txt`：Center Python 依赖
- `SCHEMA_MINIMAL.sql`：最小可运行数据库结构
- `MIGRATION_SPLIT_ACCOUNT_LOCKS.sql`：账号锁字段迁移脚本
- `MIGRATION_UPLOAD_USAGE_STATS.sql`：旧库上传统计迁移
- `MIGRATION_DISPATCH_PROTOCOL_V2.sql`：旧库租约、来源指纹与协议版本迁移
- `MIGRATION_FILE_PATH_HASH.sql`：补齐 `file_tasks.file_path_hash` 生成列及唯一索引
- `scripts/check_db.py`：数据库连接检查
- `scripts/query_db.py`：任务状态查询示例
- `scripts/reset_db.py`：数据库重置辅助脚本

## API

### `GET /`

进程存活接口。

### `GET /health/ready`

检查数据库连接、协议 v2 所需字段，以及 `file_tasks.file_path_hash` 的列定义和唯一索引（规则见 [file_path_hash 迁移](#file_path_hash-迁移)）；通过返回 `ready`，失败返回 HTTP 503。部署健康检查应使用此接口。

### `POST /api/report_files`

Worker Scanner 调用，用于上报新文件。

主要字段：

- `worker_node`
- `worker_api_url`
- `files`
- `is_history_sync`
- `upload_method`
- `protocol_version=2`

文件项包含 `path`、`source_base_dir`、`size_bytes`、`mtime`；返回的 `deferred_files` 表示本次尚未接纳的新指纹，Scanner 必须重试。路径保留合法空格和原始 Unicode。

`is_history_sync=true` 会把新路径直接写为 `completed`。HMAC 请求的节点名必须在 `HISTORY_SYNC_NODE_NAMES`（逗号分隔，默认 `Init_Scanner`）中，否则返回 403。启用 `ALLOW_LEGACY_NODE_AUTH` 时，持有共享 `API_TOKEN` 的 Bearer 请求仍可提交，以兼容旧 `init_sync.py`。

### `POST /api/report_status`

Worker 调用，用于回报任务状态。

支持状态：

- `uploading`
- `completed`
- `failed`
- `403_quota`
- `clean_success`
- `not_found`
- `credential_invalid`

回报必须关联 `dispatch_id` 和 `worker_node`。Center 在事务内校验当前账号租约，拒绝已改派批次的回执。

### `POST /api/dispatch/validate`

Worker 重启恢复 journal 任务前，核对该批次仍归属本节点且存在活跃任务。节点接口默认使用 `WORKER_TOKEN_MAP` 中的独立密钥进行 HMAC 验证。

## 调度任务

Center 启动后会运行后台调度器：

- uploader 调度：每 60 秒执行一次
- cleaner 调度：启用 `CLEANER_DISPATCH_ENABLED` 时按 `CLEANER_DISPATCH_INTERVAL_MINUTES` 执行
- cleaner 强制巡检：启用 cleaner 调度时每天 03:00 执行一次

调度时区为：

```text
Asia/Shanghai
```

定时 uploader 调度与 403/凭据失效后的即时调度共用一个进程内非阻塞锁，同一时刻只运行一轮。即时触发遇到调度忙时短暂重试，最长 `DISPATCH_TRIGGER_MAX_WAIT_SECONDS`（默认 120 秒）。每轮调度还会在一个不进入连接池的专用连接上获取 MySQL `GET_LOCK`；多个 Center 进程共用同一库时，未抢到锁的进程跳过本轮。这里没有实现选主，cleaner 调度也不在该互斥范围内。默认镜像只运行一个 uvicorn 进程。

连接池使用 `ping=1`：每次从池中取出连接时都会检测，并替换被服务端超时或 `KILL` 断开的连接。

每节点 uploader 并发由 `MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY`、`MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT` 控制，默认白天 2 路、夜间 3 路，夜间为北京时间 01:00–10:00。Center 与 Worker 需保持一致；旧单值 `MAX_CONCURRENT_UPLOADS_PER_WORKER` 已被替代。

## 数据库表

### `file_tasks`

记录文件任务。

关键字段：

- `file_path`
- `source_base_dir`、`size_bytes`、`source_mtime`
- `status`
- `worker_node`
- `upload_method`
- `account_used`
- `dispatch_id`
- `created_at`
- `updated_at`

### `drive_accounts`

记录 Google Drive 账号池。

关键字段：

- `account_name`
- `role`
- `is_active`
- `cleaner_mapping`
- `dispatch_locked_until`
- `dispatch_cooldown_until`
- `quota_blocked_until`
- `current_dispatch_id`、`current_worker_node`

### `worker_registry`

记录 Worker 节点。

关键字段：

- `node_name`
- `api_url`
- `protocol_version`
- `last_active`

### `account_upload_events`

保存账号已确认上传的文件大小和时间；以 `(file_task_id, dispatch_id)` 去重，支持滚动 24 小时统计。API `/api/upload_usage` 同时返回账号当前锁与可用状态。

## 账号锁

Center 分别记录执行租约、调度冷却和配额封禁：

- `dispatch_locked_until`：短期执行锁
- `dispatch_cooldown_until`：调度失败后的短期冷却
- `quota_blocked_until`：quota 封禁锁

短期锁用于防止并发调度重复派发同一账号。

quota 锁用于防止触发上传限制的账号继续被调度。

Uploader 凭据失效回报 `credential_invalid` 并停用账号；配额/限流错误回报 `403_quota`；其他失败回报 `failed` 并回退未完成任务。所有回执均先保留本批次已确认成功的文件。

## Cleaner 策略

硬条件：

- 同组仍有 `uploading` 任务时，绝不清理
- `force_all=True` 也不能绕过这个条件

软条件：

- 同组 uploader 全部 quota blocked 时立即清理
- 否则必须等待 `CLEANER_IDLE_MINUTES` 空闲窗口

Worker 收到 cleaner 任务后，使用 `rclone move --files-from-raw` 只搬运本次 dispatch 的文件（每批最多 `MAX_CLEANER_FILES_PER_REQUEST` 个）。中转目录里的其他文件不受影响。`--drive-use-trash=false` 只决定源端副本删除时是否进入回收站，不会删除最终目标盘上的文件。

## 部署

复制配置：

```bash
cp .env.example .env
```

至少填写：

```text
API_TOKEN
DB_HOST
DB_USER
DB_PASSWORD
DB_NAME
WORKER_API_URL_MAP
WORKER_TOKEN_MAP
```

为每个 Worker 生成独立随机 `NODE_TOKEN`，在 `WORKER_TOKEN_MAP` 中按同名节点配置。协议 v2 默认关闭 `ALLOW_LEGACY_NODE_AUTH` 和 `ALLOW_LEGACY_UNLEASED_REPORTS`；历史同步节点也需单独配置密钥。

启动：

```bash
docker compose up -d --build
```

健康检查：

```bash
curl --fail http://127.0.0.1:8080/health/ready
```

## 数据库初始化

新库可导入最小结构：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < SCHEMA_MINIMAL.sql
```

旧库升级前备份，并停止 Center 与各节点 Scanner，然后按顺序执行四项迁移：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_UPLOAD_USAGE_STATS.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_DISPATCH_PROTOCOL_V2.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_FILE_PATH_HASH.sql
```

迁移注意：

- 旧 `cooldown_until` 历史上可能混用调度锁和 quota 封禁
- 使用当前版本的四个迁移脚本；它们检查已有结构，补齐字段、表和索引，调整上传事件唯一索引，不自动重写业务封禁状态
- 新逻辑读取 `dispatch_locked_until`、`dispatch_cooldown_until` 和 `quota_blocked_until`
- 如果要保留旧状态，需要人工判断并回填
- 新库导入当前 `SCHEMA_MINIMAL.sql` 已包含这些结构；执行后用 `/health/ready` 验证

### file_path_hash 迁移

`report_files` 依赖 `file_tasks.file_path_hash`（`SHA2(file_path, 256)` 的 STORED 生成列）上的唯一索引，通过 `ON DUPLICATE KEY UPDATE` 去重，并按该列回查任务指纹。`/health/ready` 只在同时满足以下条件时返回 `ready`，否则返回 503：

- 该列为 `CHAR(64)`、STORED 生成列，生成表达式等价于 `SHA2(file_path, 256)`（忽略空白、反引号和大小写）。普通列在上报时为 NULL，唯一索引允许多个 NULL，会静默插入重复任务；其他表达式（例如 `SHA2(LOWER(file_path), 256)`）会合并不同路径或让按哈希回查失效。
- 存在只含完整 `file_path_hash`（`SUB_PART` 为 NULL）的唯一索引。
- 该列上没有前缀或表达式唯一索引，例如 `UNIQUE (file_path_hash(1))`、`UNIQUE ((LEFT(file_path_hash, 1)))`。这类索引会把完整哈希不同的路径（例如 `movie/0.mkv` 与 `movie/4.mkv`，SHA-256 首字符相同）判为重复，让后上报的路径改写另一条任务而不入库；即使同时存在完整唯一索引也一样。

`MIGRATION_FILE_PATH_HASH.sql` 的执行顺序：

1. 已有 `file_path_hash` 列不满足上述列定义时，以 `Table '...MIGRATION_ABORTED_incompatible_file_path_hash_column_see_docs' doesn't exist` 错误终止。
2. 该列上存在前缀或表达式唯一索引时，以 `Table '...MIGRATION_ABORTED_prefix_unique_file_path_hash_index_see_docs' doesn't exist` 错误终止。只补建完整索引无法消除错误冲突，脚本也不会代为删除索引。
3. 已存在只含完整 `file_path_hash` 的唯一索引时，跳过后续检查和变更。
4. 否则检查是否有完全相同（逐字节）的 `file_path` 多行。存在时以 `Table '...MIGRATION_ABORTED_duplicate_file_path_rows_see_docs' doesn't exist` 错误终止。
5. 缺少列时添加生成列。STORED 列会重建表，大表需预留维护窗口。
6. 创建唯一索引 `uk_file_tasks_file_path_hash`。

第 1、2、4 步终止时都还没有执行任何 DDL，不会覆盖列、删除索引或修改数据。

列或索引不兼容时，先只读查看现有定义：

```sql
SELECT COLUMN_TYPE, IS_NULLABLE, EXTRA, GENERATION_EXPRESSION
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='file_path_hash';

SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, EXPRESSION
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND NON_UNIQUE=0
ORDER BY INDEX_NAME, SEQ_IN_INDEX;
```

确认这些列或索引没有其他用途后，备份并人工删除前缀/表达式唯一索引，或删除、改名不兼容的列，再重新执行迁移。前缀唯一索引存在期间，被误判为重复的路径没有入库（Scanner 会收到 `deferred_files` 并继续上报），同前缀的已有任务可能被重新排队或改写了节点和来源指纹，处理后建议核对 `pending` 任务。

迁移失败后，先只读列出重复项：

```sql
SELECT SHA2(file_path, 256) AS path_hash,
       COUNT(*) AS row_count,
       GROUP_CONCAT(id ORDER BY id) AS task_ids,
       GROUP_CONCAT(status ORDER BY id) AS statuses
FROM file_tasks
GROUP BY path_hash
HAVING COUNT(*) > 1;
```

清理须人工决定，脚本不会代为选择保留哪一行。建议：

- 先停止 Center 调度，并确认重复行都不是 `uploading` 或 `staged`；否则等待对应 dispatch 结束。
- 每个路径保留一行，优先保留状态最新、`account_upload_events` 引用的行。该表通过外键引用 `file_tasks.id`，直接删除被引用的行会失败。
- 处理前备份涉及的行，处理后重新执行迁移，并确认 `/health/ready` 成功。

大小写不同的路径（例如 `Movie/A.mkv` 与 `movie/a.mkv`）的哈希不同，不算重复。

## 历史同步

目标盘已有文件需要先建立防重复屏障：

```bash
python init_sync.py
```

执行前确认：

```text
WORKER_NODE_NAME=Init_Scanner
RCLONE_REMOTE_ACCOUNT=your-rclone-remote
TARGET_DIR=your-target-dir
NODE_TOKEN=CHANGE_ME_WITH_INIT_SCANNER_TOKEN
```

为 `Init_Scanner` 配置独立 `WORKER_TOKEN_MAP` 条目；启用 URL 白名单时也加入同名 `WORKER_API_URL_MAP` 条目。脚本要求使用该保留节点名，避免覆盖真实 Worker 的注册地址。`init_sync.py` 只使用 `NODE_TOKEN` 签名，不需要 `API_TOKEN`。`HISTORY_SYNC_NODE_NAMES` 默认值 `Init_Scanner` 已覆盖该脚本；其他节点不要加入该名单。

## 运维注意

- `WORKER_API_URL_MAP` 非空时，Center 只接受白名单中的 Worker
- `WORKER_API_URL_MAP` 为空时，Center 信任节点上报的 `worker_api_url` 并向其发起请求。签名节点（以及启用 legacy 时持有共享令牌的调用方）可以借此让 Center 访问任意 HTTP/HTTPS 地址。为兼容现有部署，空映射仍可启动；生产环境应配置完整白名单
- `worker_api_url` 必须是完整 HTTP/HTTPS URL
- 数据库缺少必填环境变量时，服务会直接启动失败
- Center 不需要部署 `worker.py` 和 `scanner.py`
- 公开仓库不要提交 `.env`、数据库备份和运行日志
