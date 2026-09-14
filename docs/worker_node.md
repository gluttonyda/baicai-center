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
- `dispatch_id`、`worker_node`：派发批次与所属节点
- `remote_dir`：本批次使用的远端目录
- `source_base_dirs`、`expected_sizes`、`expected_mtimes`：与文件列表对应的来源根目录和指纹

接口默认验证节点 HMAC；本节点的 `NODE_TOKEN` 必须与 Center 的 `WORKER_TOKEN_MAP[WORKER_NODE_NAME]` 一致。重复 `dispatch_id` 只接纳相同内容，不会重复启动 rclone。

## Scanner

Scanner 默认扫描三个根目录：

- `/data/links`：按 `copy` 上报
- `/cache/links`：按 `copy` 上报
- `/data/rclone-cache`：按 `move` 上报

相关控制参数：

- `LINKS_DELAY_HOURS`
- `CACHE_LINKS_DELAY_HOURS`
- `RCLONE_CACHE_DELAY_HOURS`
- `RCLONE_CACHE_STABLE_SECONDS`
- `SCANNER_CHUNK_SIZE`
- `SCANNER_LOOP_SECONDS`

Scanner 使用本地 SQLite 保存 `(来源根目录, 相对路径)` 的大小和修改时间指纹，文件变化后重新上报；Center 返回的 `deferred_files` 不写入已上报缓存，后续扫描继续重试：

```text
CACHE_DB_PATH
```

如果 Center 数据库重置过，需要删除 Worker 缓存后重新扫描。

## rclone 行为

Uploader 任务会调用 `rclone copy` 或 `rclone move`。

Cleaner 任务会调用 `rclone move`，把中转盘文件移动到最终目标盘。

上传命令保留：

```text
--drive-stop-on-upload-limit
```

当前已移除 `--size-only`，避免仅凭大小跳过同尺寸但内容已变化的文件。文件上传与检查并发分别由 `RCLONE_TRANSFERS`、`RCLONE_CHECKERS` 控制。

Uploader 错误分流：

- 凭据失效：`credential_invalid`，停用账号，等待重新授权后人工启用。
- 配额/限流日志或退出码 8：`403_quota`，设置配额封禁并回退未完成任务。
- 其他非零退出码：`failed`，按普通失败回退任务。

已经确认成功的文件会先回报，失败不会抹掉这部分完成记录。

## 路径安全

Worker 只允许处理 `ALLOWED_BASE_DIRS` 下的文件。

所有文件路径会先经过：

- 保留文件名中的合法首尾空格、大小写和 Unicode 原值，拒绝换行、回车及 NUL
- `normpath`
- 结合来源根目录、`realpath` 和白名单执行越界检查
- 本地存在性检查

解析不到的本地文件会进入缺失检查；`move` 任务可进一步核对远端已存在的文件。确认缺失才回报 `not_found`；请求字段非法则直接拒绝。

## 部署

进入 Worker 目录：

```bash
cd baicai-work
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

`API_TOKEN` 仍为必填兼容配置；协议 v2 节点通信默认使用 HMAC。保持 `ALLOW_LEGACY_BEARER_AUTH=false`、`SCANNER_SEND_BEARER_AUTH=false`，并同步各节点时钟。

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
/data/links         -> /data/links
/cache/links        -> /cache/links
/data/rclone-cache  -> /data/rclone-cache
./logs/rclone  -> /data/logs/rclone
./runtime      -> /opt/baicai-worker/runtime
/usr/bin/rclone     -> /usr/bin/rclone（只读）
/root/.config/rclone -> /root/.config/rclone
```

前三项是宿主机绝对路径；调整时同时对齐 Compose 挂载、Scanner 的 `*_SCAN_DIR` 和 Worker 的 `ALLOWED_BASE_DIRS`。宿主机的 rclone 二进制也需可执行且兼容已有 remote 配置。

`TASK_JOURNAL_DB_PATH` 默认位于持久化的 `runtime/`：任务先落盘再启动，最终回执失败后定时补送；恢复任务前向 Center 校验当前租约，已过期或改派的批次取消。普通重启不要删除该目录。

上传账号并发使用 `MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY` 和 `MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT`，替代旧单值参数；默认北京时间 01:00–10:00 为夜间，分别为白天 2 路、夜间 3 路。Center 和 Worker 应保持相同设置，时间边界可通过 `UPLOAD_CONCURRENCY_NIGHT_START_HOUR`、`UPLOAD_CONCURRENCY_NIGHT_END_HOUR` 配置。

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

仅在数据库重置后需要全量重报时执行，并先停止 Scanner、备份缓存；保留独立的 `task_journal.db`。

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
- Worker 的 `NODE_TOKEN` 必须匹配 Center 对应节点的 `WORKER_TOKEN_MAP`；启用旧版兼容时 `API_TOKEN` 也必须一致
- `WORKER_NODE_NAME` 必须和 Center 的 `WORKER_API_URL_MAP` 匹配
- `WORKER_API_URL` 必须是 Center 能访问到的地址
- 不要把真实 `.env` 或 `rclone.conf` 提交到公开仓库
