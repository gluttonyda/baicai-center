# Worker 节点文件说明

此目录只包含部署在 `Worker` 节点上的代码。

## 核心文件

- `worker.py`：接收 Center 派发并执行上传/清道夫任务
- `scanner.py`：扫描本地目录并上报 Center
- `requirements.txt`：Python 依赖
- `sanitize.sh`：辅助脚本

## 部署说明

1. 复制环境变量示例：
   `cp .env.example .env`
2. 根据节点实际情况填写：
   - `WORKER_NODE_NAME`
   - `WORKER_API_URL`
   - `CENTER_URL`
   - `CENTER_API`
3. 确保本地目录存在：
   - `/data/links`
   - `/data/rclone-cache`

## 当前行为

- 上传任务：
  - 路径会先做 `normpath` 和越界校验
  - 本地 0 文件命中不再上报 `completed`，改为 `not_found`
  - 长任务每 5 分钟会上报一次 `uploading` 心跳
- 清道夫任务：
  - 会回传 `clean_success` 和已清理文件列表
  - 长任务同样会上报 `uploading` 心跳
- 状态回执：
  - `report_status()` 对非 `2xx` 响应会重试，重试耗尽后抛出异常
  - 上传命令统一带 `rclone --drive-stop-on-upload-limit`
  - 当 uploader 里的 `rclone` 结束当前任务并以非零退出码返回时，`Worker` 上报 `403_quota`
  - Center 收到 `403_quota` 后会封禁该账号 24 小时，并立即触发新账号重调度
  - Uploader 不再产出 `retry_later`，也不再根据日志限流关键词或“无有效速度”单独触发 `403_quota`
  - Uploader 与 Cleaner 现在都只保留标准输出日志，不再额外双写本地明细日志

## 测试

- 单测目录：`tests/`
- 已覆盖：
  - 非法路径与越界路径
  - `not_found` / `403_quota` / `failed` 回执
  - `scanner` 的缓存迁移、扫描摘要与心跳
  - FastAPI 入口 `/task/upload`
  - 本地 `rclone` 上传到 staging 后再清理到目标目录的往返链路

建议在 `_test_mod` 根目录执行：

```bash
./.venv/bin/pytest worker_node/tests -ra
```

## 注意

- `Worker` 节点不需要 `main.py`
- `Worker` 节点不需要 `init_sync.py`
- 调度端默认使用账号名中 `@` 之前的前缀作为账号组，例如 `baicai_1@1 -> baicai_1`
- 如果 Center 重置过数据库，记得删除本地缓存数据库再重新扫描：
  `rm -f /opt/baicai-worker/scanner_cache.db`
- 若 Center 端已切到 `dispatch_locked_until` / `quota_blocked_until` 逻辑，Worker 无需改数据库，但必须保证状态上报字段与 Center 新状态机保持一致
