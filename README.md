# Baicai Center

Baicai Center 是一个用于分布式 Google Drive 搬运的主控调度系统。

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
- `scripts/`：数据库检查和维护脚本

Center 维护三类状态：

- `file_tasks`：文件任务状态
- `drive_accounts`：上传账号、cleaner 账号、账号锁和配额封禁
- `worker_registry`：Worker 节点注册地址和活跃时间

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
POST /api/report_files
POST /api/report_status
```

## 搬运流程

1. Scanner 扫描 Worker 本地目录。
2. Scanner 调用 Center 的 `/api/report_files` 上报待搬运文件。
3. Center 将文件写入 `file_tasks`，初始状态为 `pending`。
4. Center 定时选择可用 Worker 和可用 uploader 账号。
5. Worker 收到任务后用 `rclone copy` 或 `rclone move` 执行搬运。
6. 普通 uploader 成功后，任务进入 `staged`。
7. Cleaner 账号把 `staged` 文件从中转盘移动到最终目标盘。
8. Cleaner 成功后，任务进入 `completed`。

任务状态：

```text
pending -> uploading -> staged -> completed
                    \-> failed / not_found
                    \-> 403_quota
```

## 账号与锁

账号分为：

- `uploader`：执行上传或移动
- `cleaner`：把中转盘文件清理到目标盘
- `vip`：预留角色

账号锁拆分为：

- `dispatch_locked_until`：短期调度执行锁，防止同一账号被重复派发
- `quota_blocked_until`：403 quota 后的封禁锁，默认 24 小时

当 Worker 上报 `403_quota`：

1. Center 释放短期调度锁。
2. Center 写入 `quota_blocked_until`。
3. 当前未完成任务回退到 `pending`。
4. Center 立即触发新一轮 uploader 调度。

Cleaner 策略：

- 同组仍有 `uploading` 时绝不清理
- 同组 uploader 全部 quota 封禁时立即清理
- 未全部封禁时，等待空闲窗口达到 `CLEANER_IDLE_MINUTES` 后清理

## 快速开始

### 1. 准备 Center 配置

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

启动 Center：

```bash
docker compose up -d --build
```

健康检查：

```bash
curl http://127.0.0.1:8080/
```

### 2. 初始化数据库

首次部署可参考：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < SCHEMA_MINIMAL.sql
```

如果是旧库升级，需要执行：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
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

### 4. 部署 Worker

进入 Worker 包：

```bash
cd baicai-work
cp .env.example .env
```

至少填写：

```text
API_TOKEN
CENTER_URL
CENTER_API
WORKER_NODE_NAME
WORKER_API_URL
TARGET_REMOTE_DIR
```

启动 Worker：

```bash
docker compose up -d --build
```

如果 Center 数据库重置过，清理 Worker 本地扫描缓存：

```bash
rm -f runtime/scanner_cache.db
```

## 测试

本仓库的测试需要同时把根目录和 `baicai-work/` 放进 `PYTHONPATH`：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -r baicai-work/requirements.txt
PYTHONPATH=.:baicai-work pytest -q
```

当前基线：

```text
73 passed
```

## 安全说明

不要提交真实运行配置：

- `.env`
- `baicai-work/.env`
- `rclone.conf`
- 数据库备份
- 运行日志
- 重复文件清单
- 本地扫描缓存

本仓库已通过 `.gitignore` 忽略这些内容。公开仓库中只应保留 `.env.example`。

## 文档

- [文档索引](docs/README.md)
- [Center 节点说明](docs/center_node.md)
- [Worker 节点说明](docs/worker_node.md)
- [部署分类说明](docs/部署分类说明.md)
- [配置任务清单](docs/配置任务清单.md)

## License

当前仓库尚未声明开源协议。公开前请根据你的使用意图补充 `LICENSE`。
