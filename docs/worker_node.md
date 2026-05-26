# Worker 节点说明

Worker 是实际搬运节点，可以部署在不同地区、不同机器或不同网络环境中。

一个 Center 可以控制多个 Worker。每个 Worker 负责扫描本地文件、接收 Center 派发、调用 `rclone` 执行搬运，并把结果回报给 Center。

## Worker 负责什么

- 扫描本地目录
- 上报待搬运文件
- 接收 Center 下发任务
- 执行 `rclone copy` 或 `rclone move`
- 校验任务路径是否越界
- 上报 `uploading` 心跳
- 上报成功、失败、quota、缺失文件等状态
- 执行 cleaner 任务

## 核心文件

`baicai-work/` 目录是 Worker 运行包：

- `worker.py`：Worker API 与 rclone 执行逻辑
- `scanner.py`：本地扫描与上报逻辑
- `scanner-loop.sh`：循环扫描入口
- `Dockerfile`：Worker 镜像构建
- `docker-compose.yml`：Worker API 和 Scanner 服务编排
- `.env.example`：Worker 配置模板
- `requirements.txt`：Worker Python 依赖

## Worker API

### `POST /task/upload`

Center 调用这个接口派发任务。

任务类型：

- `task_type=upload`：普通上传/移动任务
- `task_type=clean`：cleaner 清理任务

主要字段：

- `account_name`
- `dest_name`
- `files`
- `upload_method`

## Scanner

Scanner 默认扫描两类目录：

- `/data/links`：按 `copy` 上报
- `/data/rclone-cache`：按 `move` 上报

相关控制参数：

- `LINKS_DELAY_HOURS`
- `RCLONE_CACHE_DELAY_HOURS`
- `RCLONE_CACHE_STABLE_SECONDS`
- `SCANNER_CHUNK_SIZE`
- `SCANNER_LOOP_SECONDS`

Scanner 使用本地 SQLite 缓存避免重复上报：

```text
CACHE_DB_PATH
```

如果 Center 数据库重置过，需要删除 Worker 缓存后重新扫描。

## rclone 行为

Uploader 任务会调用 `rclone copy` 或 `rclone move`。

Cleaner 任务会调用 `rclone move`，把中转盘文件移动到最终目标盘。

上传命令会使用：

```text
--drive-stop-on-upload-limit
--size-only
```

当 `rclone` 以非零退出码结束当前 uploader 任务时，Worker 会向 Center 上报：

```text
403_quota
```

Center 收到后会封禁该账号一段时间，并把未完成任务回退给其他账号。

## 路径安全

Worker 只允许处理 `ALLOWED_BASE_DIRS` 下的文件。

所有文件路径会先经过：

- 去空白
- `normpath`
- 越界检查
- 本地存在性检查

不存在或越界的文件会回报为 `not_found`。

## 部署

进入 Worker 目录：

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
ALLOWED_BASE_DIRS
```

确认宿主机有可用 rclone 配置：

```text
/root/.config/rclone/rclone.conf
```

启动：

```bash
docker compose up -d --build
```

## 默认挂载目录

`baicai-work/docker-compose.yml` 默认挂载：

```text
./links        -> /data/links
./rclone-cache -> /data/rclone-cache
./logs/rclone  -> /data/logs/rclone
./runtime      -> /opt/baicai-worker/runtime
```

## 运行检查

查看容器：

```bash
docker compose ps
```

查看日志：

```bash
docker compose logs -f worker-api
docker compose logs -f scanner
```

清理扫描缓存：

```bash
rm -f runtime/scanner_cache.db
```

## 状态回报

Worker 会回报：

- `uploading`
- `completed`
- `failed`
- `403_quota`
- `clean_success`
- `not_found`
- `credential_invalid`

长任务会周期性上报 `uploading`，用于刷新 Center 里的节点活跃时间和任务更新时间。

## 注意

- Worker 不需要部署 `main.py`
- Worker 不需要直接访问 MySQL
- Worker 和 Center 的 `API_TOKEN` 必须一致
- `WORKER_NODE_NAME` 必须和 Center 的 `WORKER_API_URL_MAP` 匹配
- `WORKER_API_URL` 必须是 Center 能访问到的地址
- 不要把真实 `.env` 或 `rclone.conf` 提交到公开仓库
