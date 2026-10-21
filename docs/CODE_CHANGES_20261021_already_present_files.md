# 2026-10-21 已存在的文件在上传派发内结算（Issue #15）

## 问题

rclone 在 `--log-level INFO` 下，对"目标端已存在且一致"的文件**不输出任何逐文件日志行**：既不会匹配 `RCLONE_TRANSFER_LOG_PATTERN`，也不会进入 `successful_files`（`Unchanged skipping` 只在 DEBUG 级别出现）。

PR #10 之后 `completed` 回执只按 `successful_files` 标记成功，因此这类文件会：

1. 一直停留在 `uploading`；
2. 超过 `UPLOADING_STALE_MINUTES`（默认 30 分钟）后被 `release_stale_uploading_tasks` 回退成 `pending`；
3. 下一轮调度重新派发 → Worker 再次判定"已存在"→ 再次无输出 → 无限循环；
4. 期间该账号因为存在 `uploading` 行被整体排除，同账号其他任务一起被拖慢。

`move` 模式原本只在**本地文件已不存在**时才调用 `detect_remote_existing_files` 补偿；`copy` 模式、或本地文件仍在而远端已一致时没有任何补偿路径。

## 改动

`baicai-work/worker.py`：

- 新增 `skip_already_uploaded_files()`：上传前用一次 `rclone lsjson --files-from-raw`（带 `Size`/`ModTime` 指纹比对）复核这批文件在远端是否已存在且一致，命中即 `mark_success()` 并从待传清单剔除；
  - 期望指纹取自本地 `os.stat`，大小或 mtime（容差 1 秒）不一致的**不会**被跳过，仍照常上传；
  - 复核失败（rclone 退出码非 0、超时、响应不是合法 JSON、格式异常）时返回原清单，退回"照常上传"，不会因为没有证据就误记成功。
- `run_uploader()` 在每个 `base_dir` 分组进入 rclone 前调用它；整组都被结算时直接 `continue`，不再启动 rclone。
- `move` 模式下顺带删除已结算文件的本地副本（带 `realpath` 越界保护）：move 的语义是搬完后本地消失，留着会被扫描器当成新文件重新入库，形成慢速重复上传。

## 代价

每次派发、每个 `base_dir` 分组多一次 `lsjson` 远端查询（受既有 `REMOTE_PROBE_TIMEOUT_SECONDS` 约束）。相比"每 30 分钟重派整批并占用账号"的代价，这是可接受的一换。

## 测试

- 真 rclone 集成用例（`tests/test_worker_integration.py`）：
  - `test_worker_rclone_marks_already_present_file_as_success`：远端已有同内容同 mtime 的文件 → 回执 `completed` 且 `successful_files` 含该文件、远端 mtime 不变（未被重传）。**修复前该用例失败**（`successful_files` 为空）。
  - `test_worker_rclone_still_uploads_when_remote_copy_differs`：远端同名但大小不同 → 照常上传并覆盖内容。
  - `test_worker_rclone_move_deletes_local_when_remote_already_present`：move 模式 → 记成功、本地副本被删除、远端文件保留。
- 单元用例：`tests/test_worker.py` 增加 autouse fixture，把 `skip_already_uploaded_files` 置为直通，避免新增的 lsjson 复核打乱这些用例对 rclone 调用序列的断言；复核逻辑本身由上面的真 rclone 用例覆盖。
- 全量：**344 passed, 0 skipped, 4 warnings**（真 MySQL 8.4.8 + 真实 rclone v1.65.0）；非 root 同为 344 passed。

## 残留

- 复核按"大小 + mtime"判定一致。若旧文件 mtime 与本地不同（例如被别的工具重写过），仍会重新上传一次——这是保守方向，不会误跳过内容不同的文件。
- 大小写/尾随空格的比较仍受 `utf8mb4_unicode_ci` 与 Python 严格比较的差异影响（与成功标记路径同一类问题，见 Issue #7 相关文档）。
- 若整批文件都被结算、且 `successful_files` 之外的路径确实不存在，仍走既有的 `not_found`/租约回收路径。
