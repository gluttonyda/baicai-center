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
- `quota_blocked_until`：账号触发上传配额后的封禁时间

### quota 处理

当 Worker 发现当前 `rclone` 任务以非零退出码结束时，会上报 `403_quota`。

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
curl http://127.0.0.1:8080/
```

### 运行测试

```bash
PYTHONPATH=.:baicai-work pytest -q
```

### 清理 Worker 扫描缓存

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
