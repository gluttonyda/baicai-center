# baicai-work

`baicai-work/` 是 Baicai Worker 节点运行包。

Worker 部署在实际搬运机器上，负责扫描本地文件、接收 Center 调度、调用 `rclone` 上传或清理文件。

## 文件说明

- `worker.py`：Worker FastAPI 服务，接收 Center 派发
- `scanner.py`：扫描本地目录并向 Center 上报文件
- `scanner-loop.sh`：容器内循环执行 scanner
- `Dockerfile`：Worker 镜像构建
- `docker-compose.yml`：Worker API + Scanner 编排
- `.env.example`：Worker 配置模板
- `requirements.txt`：Python 依赖

## 快速启动

复制配置：

```bash
cp .env.example .env
```

至少填写：

```text
API_TOKEN
NODE_TOKEN
CENTER_URL
CENTER_API
WORKER_NODE_NAME
WORKER_API_URL
TARGET_REMOTE_DIR
ALLOWED_BASE_DIRS
```

`NODE_TOKEN` 必须与 Center 的 `WORKER_TOKEN_MAP[WORKER_NODE_NAME]` 一致。协议 v2 默认使用 HMAC，保留 `ALLOW_LEGACY_BEARER_AUTH=false`、`SCANNER_SEND_BEARER_AUTH=false`；`API_TOKEN` 仍是必填兼容配置。

确认宿主机有 rclone 配置：

```text
/root/.config/rclone/rclone.conf
```

启动：

```bash
docker compose up -d --build
```

## 默认挂载

```text
/data/links         -> /data/links
/cache/links        -> /cache/links
/data/rclone-cache  -> /data/rclone-cache
./logs/rclone  -> /data/logs/rclone
./runtime      -> /opt/baicai-worker/runtime
/usr/bin/rclone     -> /usr/bin/rclone（只读）
/root/.config/rclone -> /root/.config/rclone
```

Scanner 会扫描：

- `/data/links`：`copy`
- `/cache/links`：`copy`
- `/data/rclone-cache`：`move`

前三项为宿主机绝对路径。调整时同步 Compose 挂载、`*_SCAN_DIR` 和 `ALLOWED_BASE_DIRS`；Worker 会核对来源根目录和文件指纹，保留路径中合法空格。

Scanner 缓存大小和修改时间，文件变化后重新上报；Center 暂缓接纳的 `deferred_files` 会在后续扫描重试。

上传账号并发改用 `MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY`、`MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT`，默认白天 2 路、北京时间 01:00–10:00 夜间 3 路，与 Center 保持一致。单个 rclone 内并发由 `RCLONE_TRANSFERS`、`RCLONE_CHECKERS` 控制。

Uploader 已移除 `--size-only`。凭据失效回报 `credential_invalid`，配额/限流错误或退出码 8 回报 `403_quota`，其余非零退出码回报 `failed`，并保留已确认成功文件的记录。

## 服务

`docker-compose.yml` 默认包含：

- `worker-api`：监听 `8001`，接收 Center 任务
- `scanner`：按 `SCANNER_LOOP_SECONDS` 循环扫描

查看状态：

```bash
docker compose ps
```

查看日志：

```bash
docker compose logs -f worker-api
docker compose logs -f scanner
```

## 重扫注意

Worker 将 `dispatch_id`、任务内容和最终回执保存在 `TASK_JOURNAL_DB_PATH`（默认 `runtime/task_journal.db`）。重启时先向 Center 验证租约再恢复任务，未送达的最终回执自动补送；普通重启必须保留 `runtime/`。

如果 Center 数据库重置过，先停止 Scanner 并备份本地扫描缓存，再清理缓存以便全量重报：

```bash
rm -f runtime/scanner_cache.db
```

否则 scanner 会认为历史文件已经上报过。不要删除独立的 `task_journal.db`。

升级前先完成 [Center 数据库迁移](../docs/center_node.md#数据库初始化)，确认 `/health/ready` 返回成功后再启动节点。

## 安全注意

不要提交：

- `.env`
- `runtime/`
- `logs/`
- `links/`
- `rclone-cache/`
- `rclone.conf`

这些文件可能包含真实凭据、日志或媒体路径。
