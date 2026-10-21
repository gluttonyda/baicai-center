# 文档索引

这里整理 Baicai Center 的部署、配置和运行说明。

Baicai Center 的定位是分布式 Google Drive 搬运主控系统：一个 Center 统一管理任务、账号池和调度，多个 Worker 分布在不同地区或不同机器上执行 `rclone` 搬运。

## 推荐阅读顺序

1. [根 README](../README.md)：项目用途、架构、快速开始
2. [部署分类说明](部署分类说明.md)：哪些文件放 Center，哪些文件放 Worker
3. [配置任务清单](配置任务清单.md)：部署前逐项检查配置
4. [Center 节点说明](center_node.md)：Center API、数据库、调度策略
5. [Worker 节点说明](worker_node.md)：Worker API、Scanner、rclone 执行逻辑

## 系统角色

### Center

Center 是主控大脑，负责：

- 接收 Worker 上报的文件清单
- 维护 `file_tasks`
- 维护 `drive_accounts`
- 选择可用 Worker 和可用账号
- 派发 uploader 任务
- 根据 `staged` 状态派发 cleaner 任务
- 处理 `403_quota`、`failed`、`not_found`、`credential_invalid`

### Worker

Worker 是执行节点，负责：

- 扫描本地目录
- 向 Center 上报新文件
- 接收 Center 派发的上传任务
- 调用 `rclone copy` / `rclone move`
- 周期性上报 `uploading` 心跳
- 上报最终状态
- 持久化派发任务和最终回执，重启后校验租约并恢复未完成工作

### Cleaner

Cleaner 是一种专用账号任务，不是单独服务。它由 Worker 执行，但账号角色是 `cleaner`。

它负责把普通 uploader 已经搬到中转盘的文件继续移动到最终目标盘。

## 核心数据流

```text
Scanner
  -> Center /api/report_files
  -> file_tasks.pending
  -> Center scheduler
  -> Worker /task/upload
  -> rclone copy/move
  -> Center /api/report_status
  -> staged / completed / failed / not_found
```

Cleaner 数据流：

```text
file_tasks.staged
  -> Center cleaner scheduler
  -> Worker /task/upload task_type=clean
  -> rclone move
  -> Center /api/report_status clean_success
  -> file_tasks.completed
```

## 关键策略

### 账号锁

- `dispatch_locked_until`：防止同一账号短时间重复派发
- `dispatch_cooldown_until`：调度失败后的短期冷却
- `quota_blocked_until`：账号触发上传配额后的封禁时间

`dispatch_id` 和节点名共同标识账号租约；过期回执不能改变后续批次。节点请求默认使用独立 `NODE_TOKEN` 生成 HMAC，Center 在 `WORKER_TOKEN_MAP` 中配置对应密钥。

### quota 处理

Uploader 按日志和退出码区分错误：凭据失效上报 `credential_invalid`；检测到配额/限流错误或退出码为 8 时上报 `403_quota`；其他非零退出码上报 `failed`。凭据失效会停用账号，普通失败只回退未完成任务。

Center 收到后会：

1. 给账号写入 `quota_blocked_until`
2. 把仍在 `uploading` 的任务回退到 `pending`
3. 立即触发下一轮调度

### cleaner 处理

Cleaner 不会在同组仍上传时抢跑。

允许清理的情况：

- 同组 uploader 全部 quota blocked
- 或同组已达到 `CLEANER_IDLE_MINUTES` 空闲窗口

## 常用命令

### Center 启动

```bash
docker compose up -d --build
```

### Worker 启动

```bash
cd baicai-work
docker compose up -d --build
```

### Center 健康检查

```bash
curl --fail http://127.0.0.1:8080/health/ready
```

`/health/ready` 同时检查数据库、协议 v2 必要字段，以及 `file_path_hash` 是否为 `SHA2(file_path, 256)` 的 `CHAR(64)` STORED 生成列、是否有完整唯一索引且没有前缀/表达式唯一索引；根路径 `/` 仅表示 API 进程可响应。旧库需按 [Center 节点说明](center_node.md#数据库初始化) 依次执行拆分锁、上传统计、派发协议和 `file_path_hash` 迁移。

### 运行测试

```bash
PYTHONPATH=.:baicai-work pytest -q
```

真实 MySQL 用例需要设置 `BAICAI_TEST_MYSQL_HOST`、`BAICAI_TEST_MYSQL_USER`、`BAICAI_TEST_MYSQL_PASSWORD`、`BAICAI_TEST_MYSQL_DATABASE`；未设置时会被跳过。说明见 [2026-10-21 调度互斥与鉴权加固说明](CODE_CHANGES_20261021.md#测试)。

### 清理 Worker 扫描缓存

仅在 Center 数据库重置、确实需要全量重报时操作；先停止 Scanner 并备份缓存。普通重启保留 `runtime/`，尤其不要删除 `task_journal.db`。

```bash
rm -f baicai-work/runtime/scanner_cache.db
```

## 公开仓库注意事项

公开仓库只能提交模板和代码，不应提交：

- `.env`
- `baicai-work/.env`
- `rclone.conf`
- 数据库备份
- 生产日志
- 媒体路径清单
- Worker 本地缓存

这些内容已在 [.gitignore](../.gitignore) 中排除。
