# Baicai-Center 运行目录说明

本目录是 `baicai-center` 的生产运行目录，当前代码基于已验证的测试副本同步整理而来，用于后续运行、维护和回归。

## 部署前提

- 架构模型：单 `Center` 对接多个 `Worker`
- 当前目录已作为后续运行与维护基线，相关文档和测试资产已按目录整理
- 所有敏感配置通过环境变量或 `.env` 文件注入

## 核心组件

### `main.py`

- 负责任务入库、调度、状态回传和清道夫派发
- 已增加输入校验、Worker 地址白名单、下发失败回滚和 `uploading` 心跳处理
- 已拆分 `dispatch_locked_until` 与 `quota_blocked_until`
- 清道夫策略已升级为“两段式”：
  - 同组仍在上传时绝不清理
  - 全组 uploader 配额耗尽时立即清理
  - 未耗尽时等待空闲窗口后再清理

### `baicai-work/`

- 放置 `Worker` 节点运行包，包括 `worker.py`、`scanner.py`、`docker-compose.yml` 与 `.env` 模板
- `worker.py` 负责执行上传与清道夫任务，已支持长任务心跳、状态回传和路径越界校验
- `scanner.py` 负责扫描 `/data/links` 与 `/data/rclone-cache`，并按延迟窗口向 Center 流式上报

### `init_sync.py`

- 负责将大盘现有文件流式注入 Center，建立防重屏障
- 已改为边读边分块提交，失败时返回非零退出码

## 快速开始

### 1. 准备配置

复制示例配置：

```bash
cp .env.example .env
```

至少检查这些变量：

- `API_TOKEN`
- `DB_HOST`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `WORKER_API_URL_MAP`
- `CENTER_URL`
- `CENTER_API`
- `WORKER_NODE_NAME`：如果运行 `init_sync.py`，这里必须设为 `Init_Scanner`

### 2. 启动 Center

```bash
docker compose up -d --build
```

启动后可访问根接口确认存活：

```bash
curl http://127.0.0.1:8080/
```

### 2.1 执行账号锁字段迁移

如果当前代码包含 `dispatch_locked_until` / `quota_blocked_until` 逻辑，上线前需要先执行：

```bash
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
```

说明：

- 旧 `cooldown_until` 历史上混用了“调度锁”和“403 配额封禁”，不能自动精确拆分
- 新代码只读取 `dispatch_locked_until` / `quota_blocked_until`
- 若需保留历史状态，需在上线前人工判别并写入新字段

### 3. 建立防重护城河

首次部署或重置 `file_tasks` 后，先执行：

```bash
python init_sync.py
```

成功后会输出历史同步摘要。

执行 `init_sync.py` 前请确认：

- `WORKER_NODE_NAME=Init_Scanner`
- 不要使用任何真实 Worker 节点名，否则会覆盖 `worker_registry` 中真实节点的注册地址

### 4. 清理 Worker 扫描缓存

如果你重置过 Center 数据库，必须在每个 Worker 节点执行：

```bash
rm -f /opt/baicai-worker/scanner_cache.db
```

否则 Worker 会误以为历史文件已经上报过。

## 测试说明

- 当前目录已收敛为生产运行目录，不再保留历史测试副本与测试缓存
- 如需做 Center 回归，请在独立测试副本中执行，不要直接在生产目录运行全量测试
- `Worker` 联调与容器启动文件统一位于 `baicai-work/`

## 关键环境变量

### Center

- `API_TOKEN`
- `DB_HOST`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `WORKER_API_URL_MAP`
- `MAX_FILES_PER_REQUEST`
- `MAX_SUCCESSFUL_FILES`
- `MAX_MISSING_FILES`
- `MAX_CLEANER_FILES_PER_REQUEST`
- `CLEANER_IDLE_MINUTES`
- `ACCOUNT_DISPATCH_LEASE_MINUTES`
- `QUOTA_BLOCK_HOURS`

### Worker

- `CENTER_URL`
- `TARGET_REMOTE_DIR`
- `ALLOWED_BASE_DIRS`

### Scanner

- `CENTER_API`
- `WORKER_NODE_NAME`
- `WORKER_API_URL`
- `CACHE_DB_PATH`
- `SCANNER_CHUNK_SIZE`
- `LINKS_DELAY_HOURS`
- `RCLONE_CACHE_DELAY_HOURS`

### Init Sync

- `RCLONE_REMOTE_ACCOUNT`
- `TARGET_DIR`
- `INIT_SYNC_CHUNK_SIZE`

## 已完成的关键整改

- 配置从硬编码迁移到环境变量读取
- Worker 上报和 Center 派发均增加 HTTP 失败显式处理
- `uploading` 心跳可刷新节点活跃时间
- 0 文件命中不再误判为成功，改为 `not_found`
- Worker 路径增加越界防护
- Scanner 增加异常日志和统计摘要
- 历史同步改为流式分块提交
- 清理条件改为“上传活跃时绝不清、全组 quota 耗尽立即清、否则空闲后再清”
- 账号锁已拆分为调度锁和 quota 封禁锁
- `Worker` 上传命令已启用 `rclone --drive-stop-on-upload-limit`
- 一旦 uploader 里的 `rclone` 结束当前任务并以非零退出码返回，`Worker` 会上报 `403_quota`
- Center 收到 `403_quota` 后会将该账号写入 `quota_blocked_until` 封禁 24 小时，并把剩余任务回退给新账号继续
- `403_quota` / `credential_invalid` 回执都会立即触发一次 uploader 重调度，不必等待 60 秒巡检
- Uploader 不再产出 `retry_later`，也不再基于日志里的限流关键词或“无有效速度”单独触发 `403_quota`
- Uploader 与 Cleaner 现在都只保留标准输出日志，不再额外双写本地明细日志
- 现在 uploader 里只要 `rclone` 以非零退出码结束当前任务，就会导致账号切换和 24 小时封禁
- 增加回归测试覆盖 `main.py`、`worker.py`、`scanner.py`、`init_sync.py`
- 增加多 Worker 分发测试和 Worker 真实 `rclone` 往返测试

## 注意事项

- 若后续继续做大改，请先在独立测试副本完成联调，再回灌当前运行目录
- 若 `WORKER_API_URL_MAP` 非空，则 Center 只接受白名单中的 `worker_node`
- `force_all=True` 只能绕过“策略等待”，不能绕过“同组仍在上传”的安全挡板
- 账号组的判定规则依赖命名约定：默认取账号名中 `@` 之前的前缀作为组名，例如 `baicai_1@1 -> baicai_1`
- 生产切换前至少要完成一次真实测试库迁移验证和一次双 Worker 联调
