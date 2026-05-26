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
CENTER_URL
CENTER_API
WORKER_NODE_NAME
WORKER_API_URL
TARGET_REMOTE_DIR
```

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
./links        -> /data/links
./rclone-cache -> /data/rclone-cache
./logs/rclone  -> /data/logs/rclone
./runtime      -> /opt/baicai-worker/runtime
```

Scanner 会扫描：

- `/data/links`
- `/data/rclone-cache`

Worker 只会处理 `ALLOWED_BASE_DIRS` 白名单下的路径。

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

如果 Center 数据库重置过，需要删除 Worker 本地扫描缓存：

```bash
rm -f runtime/scanner_cache.db
```

否则 scanner 会认为历史文件已经上报过。

## 安全注意

不要提交：

- `.env`
- `runtime/`
- `logs/`
- `links/`
- `rclone-cache/`
- `rclone.conf`

这些文件可能包含真实凭据、日志或媒体路径。
