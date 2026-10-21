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

`account_name` 和 `dest_name` 会拼成 rclone 参数 `<名称>:<目录>`，因此只接受 `[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}`，例如 `baicai_1@1`、`baicai-1@1-3`、`baicai-big2`。以 `-` 开头（会被当作选项）、包含冒号或逗号（连接串及后端参数）、路径分隔符、空白或控制符的名称会被 422 拒绝，并且不再去除首尾空白。签名、节点名和 Bearer 均按 UTF-8 字节常量时间比较，非 ASCII 输入返回 401，不会导致 500。

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


## 镜像内的 rclone

`baicai-work/Dockerfile` 直接安装官方 rclone 构建（当前固定 `1.65.2`，amd64/arm64/arm 分别校验 SHA256），不再使用发行版仓库里的 1.60.1——那个版本不接受含 `@` 的 remote 名，而现场账号名（`baicai_1@1`）都带 `@`。

- 只有指向本机目录的 rclone 配置需要额外挂载（`/root/.config/rclone`）。
- 宿主机 rclone 挂载（`/usr/bin/rclone`）**已不再必需**：新版节点可以直接删掉这一行；保留它只是为了让想固定宿主机版本的老节点行为不变。
- CI 会在 `worker image (linux/amd64)` 与 `worker image (linux/arm64)` 两个 job 里断言版本 ≥ 1.65，并真的用 `rclone lsf "baicai_1@1:"` 解析一次带 `@` 的 remote，防止回退。

## rclone 行为

Uploader 任务按来源根目录分组，调用 `rclone copy|move <来源根目录> <账号>:<目标目录> --files-from-raw <本批清单>`，只搬运本批文件。清单中是 `resolve_task_path` 规范化后的相对路径（见下文“路径安全”），逐行原样匹配：首尾空白、`#`/`;` 开头都是文件名的一部分，不做 glob。成功记录取自 rclone `INFO` 日志中 `Copied (…)`、`Deleted`、`Skipped` 行的对象路径，按原样回报。`move` 任务的文件在本地缺失时，用同一份 raw 清单对目标目录执行 `lsf`/`lsjson`，复核远端是否已存在。`--files-from-raw` 需要 rclone 1.52 或更高版本。以前使用 `--files-from` 时这类文件会被静默跳过，升级影响见 [uploader 改用 raw 清单说明](CODE_CHANGES_20261021_uploader_raw.md#兼容影响)。

Cleaner 任务会调用 `rclone move --files-from-raw <本批清单>`，只把本次 dispatch 的文件从中转盘移动到最终目标盘。中转目录里不在清单中的文件保持不动。清单路径与上传时的远端相对路径规则一致：去掉前导 `/` 后执行 `normpath`，拒绝 `..` 越界路径。`--files-from-raw` 需要 rclone 1.52 或更高版本。`--drive-use-trash=false` 只影响服务端搬运后源端副本的删除方式（不进回收站），不会删除最终目标盘上的文件。

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
NODE_TOKEN
CENTER_URL
CENTER_API
WORKER_NODE_NAME
WORKER_API_URL
TARGET_REMOTE_DIR
ALLOWED_BASE_DIRS
```

协议 v2 节点通信默认使用 HMAC。保持 `ALLOW_LEGACY_BEARER_AUTH=false`、`SCANNER_SEND_BEARER_AUTH=false`，并同步各节点时钟；此时可以不配置 `API_TOKEN`。只有 Worker 开启 `ALLOW_LEGACY_BEARER_AUTH=true` 或 Scanner 开启 `SCANNER_SEND_BEARER_AUTH=true` 时，才必须配置与 Center 一致的非空 `API_TOKEN`，否则进程启动失败。

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
- Worker 的 `NODE_TOKEN` 必须匹配 Center 对应节点的 `WORKER_TOKEN_MAP`；启用旧版兼容时 `API_TOKEN` 也必须配置且与 Center 一致
- `WORKER_NODE_NAME` 必须和 Center 的 `WORKER_API_URL_MAP` 匹配
- `WORKER_API_URL` 必须是 Center 能访问到的地址
- 不要把真实 `.env` 或 `rclone.conf` 提交到公开仓库
