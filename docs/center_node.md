# Center 节点文件说明

此目录只包含部署在 `Center` 节点上的代码和运维脚本。

## 核心文件

- `main.py`：Center API 与调度器
- `init_sync.py`：大盘历史同步脚本
- `Dockerfile`：Center 镜像构建文件
- `docker-compose.yml`：Center 启动配置
- `requirements.txt`：Python 依赖
- `MIGRATION_SPLIT_ACCOUNT_LOCKS.sql`：账号锁字段拆分迁移脚本

## 运维脚本

- `check_db.py`
- `query_db.py`
- `reset_db.py`
- `SQL_QUERY_GUIDE.md`

## 部署说明

1. 复制环境变量示例：
   `cp .env.example .env`
2. 按实际环境填写数据库和 Token。
3. 如果当前代码依赖 `dispatch_locked_until` / `quota_blocked_until`，先执行迁移脚本：
   `mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql`
4. 执行：
   `docker compose up -d --build`
5. 如果要重建防重屏障，执行：
   `python init_sync.py`

## 清理策略

- 硬安全条件：
  - 同组仍有 `uploading` 任务时，绝不允许清理
  - 该条件不会被 `force_all=True` 绕过
- 软策略条件：
  - 同组全部 uploader 账号都处于 `quota_blocked_until` 时，立即清理
  - 未全部耗尽时，只有在空闲窗口达到 `CLEANER_IDLE_MINUTES` 后才允许清理
- 账号锁语义：
  - `dispatch_locked_until` 仅表示调度执行锁
  - `quota_blocked_until` 仅表示 `403_quota` 配额封禁

## init_sync 注意事项

- `init_sync.py` 使用的 `WORKER_NODE_NAME` 必须固定为 `Init_Scanner`
- 不得填写任何真实 Worker 节点名，如 `worker-node-1`
- 原因是 `init_sync.py` 会向 `worker_registry` 注册一个临时节点；若名称与真实 Worker 重名，会覆盖真实节点的 `api_url`，导致后续调度派发到错误地址

## 测试

- 单测目录：`tests/`
- 关键覆盖：
  - API 状态流转
  - quota 封禁与清道夫条件
  - 多 Worker 分发
  - `init_sync` 流式分块
- 在 `_test_mod` 根目录执行全量回归：
  `./.venv/bin/pytest -ra --cov=center_node --cov=worker_node`

## 注意

- `Center` 节点不需要 `worker.py`
- `Center` 节点不需要 `scanner.py`
- 旧 `cooldown_until` 已不作为新逻辑判断依据，迁移前不要只更新代码不更新表结构
