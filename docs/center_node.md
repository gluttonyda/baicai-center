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
- `scripts/check_db.py`：数据库连接检查
- `scripts/query_db.py`：任务状态查询示例
- `scripts/reset_db.py`：数据库重置辅助脚本

## API

### `GET /`

健康检查接口。

### `POST /api/report_files`

Worker Scanner 调用，用于上报新文件。

主要字段：

- `worker_node`
- `worker_api_url`
- `files`
- `is_history_sync`
- `upload_method`

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

## 调度任务

Center 启动后会运行后台调度器：

- uploader 调度：每 60 秒执行一次
- cleaner 调度：按 `CLEANER_DISPATCH_INTERVAL_MINUTES` 执行
- cleaner 强制巡检：每天 03:00 执行一次

调度时区为：

```text
Asia/Shanghai
```

## 数据库表

### `file_tasks`

记录文件任务。

关键字段：

- `file_path`
- `status`
- `worker_node`
- `upload_method`
- `account_used`
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
- `quota_blocked_until`

### `worker_registry`

记录 Worker 节点。

关键字段：

- `node_name`
- `api_url`
- `last_active`

## 账号锁

Center 使用两种锁：

- `dispatch_locked_until`：短期执行锁
- `quota_blocked_until`：quota 封禁锁

短期锁用于防止并发调度重复派发同一账号。

quota 锁用于防止触发上传限制的账号继续被调度。

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
```

启动：

```bash
docker compose up -d --build
```

健康检查：

```bash
curl http://127.0.0.1:8080/
```

## 数据库初始化

新库可导入最小结构：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < SCHEMA_MINIMAL.sql
```

旧库升级执行：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
```

迁移注意：

- 旧 `cooldown_until` 历史上可能混用调度锁和 quota 封禁
- 新逻辑只读取 `dispatch_locked_until` 和 `quota_blocked_until`
- 如果要保留旧状态，需要人工判断并回填

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
```

不要使用真实 Worker 节点名运行 `init_sync.py`，否则会覆盖 `worker_registry` 中真实节点的 `api_url`。

## 运维注意

- `WORKER_API_URL_MAP` 非空时，Center 只接受白名单中的 Worker
- `worker_api_url` 必须是完整 HTTP/HTTPS URL
- 数据库缺少必填环境变量时，服务会直接启动失败
- Center 不需要部署 `worker.py` 和 `scanner.py`
- 公开仓库不要提交 `.env`、数据库备份和运行日志
