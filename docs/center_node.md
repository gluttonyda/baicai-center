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
- `scripts/check_db.py`：数据库连接检查
- `scripts/query_db.py`：任务状态查询示例
- `scripts/reset_db.py`：数据库重置辅助脚本

## API

### `GET /`

进程存活接口。

### `GET /health/ready`

检查数据库连接及协议 v2 所需字段；通过返回 `ready`，失败返回 HTTP 503。部署健康检查应使用此接口。

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

旧库升级前备份并停止调度，按顺序执行三项迁移：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_UPLOAD_USAGE_STATS.sql
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_DISPATCH_PROTOCOL_V2.sql
```

迁移注意：

- 旧 `cooldown_until` 历史上可能混用调度锁和 quota 封禁
- 使用当前版本的三个迁移脚本；它们检查已有结构，补齐字段和表、调整上传事件唯一索引，不自动重写业务封禁状态
- 新逻辑读取 `dispatch_locked_until`、`dispatch_cooldown_until` 和 `quota_blocked_until`
- 如果要保留旧状态，需要人工判断并回填
- 新库导入当前 `SCHEMA_MINIMAL.sql` 已包含这些结构；执行后用 `/health/ready` 验证

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

为 `Init_Scanner` 配置独立 `WORKER_TOKEN_MAP` 条目；启用 URL 白名单时也加入同名 `WORKER_API_URL_MAP` 条目。脚本要求使用该保留节点名，避免覆盖真实 Worker 的注册地址。

## 运维注意

- `WORKER_API_URL_MAP` 非空时，Center 只接受白名单中的 Worker
- `worker_api_url` 必须是完整 HTTP/HTTPS URL
- 数据库缺少必填环境变量时，服务会直接启动失败
- Center 不需要部署 `worker.py` 和 `scanner.py`
- 公开仓库不要提交 `.env`、数据库备份和运行日志
