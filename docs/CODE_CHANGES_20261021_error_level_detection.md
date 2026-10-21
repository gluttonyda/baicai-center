# 2026-10-21 错误判定只作用于错误级别日志（Issue #8）说明

本文记录 `fix/worker-error-line-detection` 分支的代码行为、兼容影响和测试方式。内容针对仓库代码，不代表已部署到任何运行节点。

## 问题

`is_quota_error` 与 `is_credential_invalid_error` 对 rclone 的**整行**文本做子串匹配，而成功日志行包含完整文件路径：

```text
INFO  : token expired.mkv: Copied (new)
INFO  : upload limit 2024.mkv: Copied (new)
```

- 命中 `credential_invalid` → `run_uploader` 立刻 `process.kill()` 中断整批，Center 把账号 `is_active=0`，需要人工 reconnect。
- 命中 `403_quota` → 账号封禁 24 小时并回退任务。

也就是说，**文件名**里出现这些关键字就能误封一个完全正常的账号。

## 变更

| 范围 | 之前 | 现在 |
| --- | --- | --- |
| 错误判定范围 | 每一行都判定 | 只有 `NOTICE`/`ERROR`/`CRITICAL`/`ALERT`/`EMERGENCY` 级别的行才判定；`INFO`/`DEBUG` 一律不判定 |
| 双保险 | 无 | 已由 `parse_rclone_transfer_line` 识别成功的行（`Copied`/`Moved`/`Skipped`/`Deleted`）直接跳过错误判定 |
| 无法识别级别 | — | 保留旧行为（整行匹配），避免 `--log-format` 差异导致漏判 |
| rclone 日志格式 | 依赖节点默认值 | 命令里显式传 `--log-format date,time` 与 `--use-json-log=false`，把「非 JSON、带日期时间前缀」固定下来 |
| markers 列表 | 未变 | 未变（本次只改"在哪些行上判定"） |

新增 `rclone_log_level()` / `is_error_level_line()` 两个辅助函数，日志级别正则兼容可选的日期时间前缀。uploader 的 rclone 命令显式指定 `--log-format date,time` 并关闭 JSON 日志（`--use-json-log=false`）。注意 rclone 的 `--log-format` **只接受** `date`/`time`/`microseconds`/`UTC`/`longfile`/`shortfile`/`pid`/`nolevel`/`json`——级别与消息不可配置、始终输出。最初写成 `date,time,level,msg` 会让新版 rclone 直接以 `invalid choice "level"` 退出（本地 1.65.0-DEV 恰好接受，CI 用 rclone-current 才暴露）。

## 兼容影响

- 真实错误仍然生效：`ERROR : ... googleapi: Error 403: User rate limit exceeded`、`ERROR : ... invalid_grant` 等照旧触发封号/停用。
- 若某版本 rclone 把真实错误打成 `INFO` 级别，将不再自动触发封号；需要依赖日志监控或人工处理（这是收窄判定的代价）。
- 无法识别行格式时回退旧行为，因此不会因为日志格式变化而彻底失去检测能力（但那条路径仍可能误判文件名）。

## 测试

在隔离环境（Python 3.10、MySQL 8.4.8、rclone v1.65.0）执行全量：**332 passed**，4 条 warning 均为既有 `on_event` 弃用提示。

新增用例：

- `INFO  : token expired.mkv: Copied (new)`、`INFO  : upload limit 2024.mkv: Copied (new)`、`INFO  : There was nothing to transfer`、`DEBUG` 行 → 都不参与错误判定。
- `ERROR`/`NOTICE` 行（含 `User rate limit exceeded`、`invalid_grant`）→ 仍参与判定。
- 无级别前缀的裸文本 `token expired` → 保留旧行为（判定为 True）。
- 端到端风格：`run_uploader` 处理 `INFO  : token expired.mkv: Copied (new)` 时状态为 `completed`、该文件被记为成功、且没有出现"凭据失效"日志。
- 正向端到端：`ERROR : movie/a.mkv: couldn't fetch token: invalid_grant` 仍会 kill 进程并回报 `credential_invalid`。
- 回退契约参数化：带 PID 前缀、短横线日期、JSON 日志、只有时间前缀的四种格式都断言 `is_error_level_line(...) is True`。
- 命令断言：uploader 的 rclone 参数里 `--log-format` 必须为 `date,time`，且包含 `--use-json-log=false`。

## 残留风险

1. **级别解析依赖格式。** 只识别 `<日期> <时间> <LEVEL>` 与裸 `LEVEL` 两种形态；自定义 `--log-format` 让级别缺失时，回退整行匹配，仍可能误判含关键字的文件名。
2. **NOTICE 级别的语义。** rclone 的重试提示是 `NOTICE`，若提示文本里带 `quota exceeded`（例如 `pacer: low level retry ... quota exceeded`），仍会触发 `403_quota`——这与改动前一致，属预期行为（确实是配额类错误）。
3. **文件名问题只解决了一半。** 若同一路径在 `ERROR` 行里出现且消息本身无关配额/凭据（例如连接重置），仍可能因路径命中关键字而误判；这次没有剥离对象路径做二次匹配（因路径本身可含 `: `，切分不可靠）。
