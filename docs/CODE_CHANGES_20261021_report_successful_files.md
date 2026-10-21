# 2026-10-21 按文件标记上传成功（Issue #7）说明

本文记录 `fix/report-uses-successful-files` 分支的代码行为、兼容影响和测试方式。内容针对仓库代码，不代表已部署到任何运行节点。

## 问题

`receive_worker_report` 在 `status=="completed"` 时调用 `mark_all_uploading_tasks_success`，不看 `successful_files`，把该账号在该 dispatch 下**所有** `uploading` 行置成功，并按整批写 `account_upload_events`。

rclone 只对实际动作的文件打日志行；漏搬、被跳过、或整批"目标端已存在"（只输出 `There was nothing to transfer`）时，Worker 的成功清单可能少于派发清单，但 Center 仍认为整批成功，文件从未上传却已 `staged`/`completed`。

## 变更

| 范围 | 之前 | 现在 |
| --- | --- | --- |
| `status=="completed"` 的任务标记 | 一律整批置成功 | 协议 v2 节点：只认 `successful_files`（由既有"抢救"步骤精确标记），未列出的任务保持 `uploading`；协议 <2 或未知节点：保留整批语义并打 warning |
| 用量事件 | 按整批插入 | v2 走 `insert_upload_usage_events_for_files`，只记实际成功的文件 |
| 路径匹配 | `file_path IN (...)` 精确字符串比较 | SQL 侧归一化后比较：`TRIM(LEADING '/' FROM REGEXP_REPLACE(file_path, '^([.]/)+', ''))`，即库里存的 `/movie/a.mkv`、`./movie/b.mkv` 与 Worker 回报的 `movie/a.mkv`、`movie/b.mkv` 都能命中 |

## 一条回报只对应一行（跟进修复）

最初的做法是 `UPDATE ... WHERE <归一化表达式> IN (...)`，有两个问题：

1. **一条回报命中多行**：`file_tasks` 的唯一键是原始 `file_path` 的哈希，所以 `a.mkv` 与 `/a.mkv` 可以共存。归一化后两者相同，一次回报会把**从未上传的那行**也置成功并多记一条用量事件——正是本 Issue 要消灭的“假成功”。
2. **`strip()` 破坏首尾空白**：Python 侧归一化原本调用 `strip()`，而 Worker 的 `mark_success` 只把反斜杠换成 `/`、不裁空白（Issue #2 明确要求保留）。结果是首字符为空白或首级目录以空白开头的路径永远匹配不上。

现在的做法：

- 新增 `resolve_successful_task_ids()`：先按归一化值取出候选行，再为**每条回报路径**选唯一的行——精确同名优先，其次才用归一化兜底；同一行不会被两条回报重复认领。
- 成功标记与用量事件都按选出的 `id` 更新（`WHERE id IN (...)`），不再对列套表达式。
- `normalize_relative_path` 去掉 `strip()`，只处理反斜杠、前导 `./` 与前导 `/`。

## 为什么未列出的任务保持 `uploading`

不直接改回 `pending`：move 模式下 rclone 可能已经搬走文件但没打日志行，立刻重派会重复搬运并可能因源文件消失而误判 `not_found`。保持 `uploading` 后由既有机制收敛：

- `release_stale_uploading_tasks`（`UPLOADING_STALE_MINUTES`，默认 30 分钟）把超时的 `uploading` 回退为 `pending`，下一轮调度重新派发；
- Center 会在日志里记一条 warning，含账号、dispatch 和未标记数量。

代价是这类任务的收敛慢一个租约周期（默认 30 分钟），换来的是不会把"不确定"当成"成功"。

## 兼容影响

- 协议 v2 的 Worker（当前 `baicai-work/worker.py`）无需改动：它本来就在回执里发送 `successful_files`。
- 旧 Worker（`protocol_version < 2`，或 `worker_registry` 里查不到记录）：行为不变，仍整批置成功，日志会打 `协议版本 <2` 的 warning，便于识别哪些节点还没升级。
- 若 Worker 某次回报 `completed` 但 `successful_files` 为空（例如全部文件已在远端、rclone 未产生任何逐文件日志），v2 节点下不会有任何任务被标记成功；这些任务会按上文在租约超时后重派，不会丢数据，但会多跑一轮。
- 路径归一化只处理**前导** `/` 与 `./`。库里存有内部 `//`、尾部 `/` 或大小写不同的路径时仍可能匹配不上；这类任务会留在 `uploading` 并在日志中体现，需要人工核对（见残留风险）。

## 测试

在隔离环境（Python 3.10、MySQL 8.4.8、rclone v1.65.0）执行全量：**336 passed**，非 root 同为 336 passed。

新增用例：

- 单元：`_normalized_path_expression` 的 SQL 文本不含反斜杠（CI 无数据库时也能拦住正则退化），以及 `normalize_relative_path` 的前导 `/`、`./` 处理。
- 单元：v2 节点回报 2/3 个文件时 `mark_all_uploading_tasks_success` 不被调用，并打"保持 uploading"警告；v1 节点仍走整批并打兼容警告。
- 真库（一报一行）：`movie/x.mkv` 与 `/movie/x.mkv` 共存时，回报 `movie/x.mkv` 只让同名行成功，另一行保持 `uploading`，用量事件 1 条；回报 `/movie/y.mkv` 同样命中带斜杠那行。
- 真库（空白）：` leading/movie.mkv` 原样回报能匹配成功；另有纯函数断言 `normalize_relative_path` 不裁剪空白。
- 真库：库里存 `/movie/slash.mkv`、`./movie/dot.mkv`、`movie/plain.mkv`、`a/movie.mkv`、`1/episode.mkv`、`x/holiday.mp4`、`holiday.mp4`、`movie/untouched.mkv`，Worker 只回报前七个的归一化路径 → 前七条置 `staged`，`movie/untouched.mkv` 保持 `uploading`，用量事件恰好 7 条。用例特意包含单字符开头的目录（`a/`、`1/`、`x/`），用于捕获 SQL 正则退化。

> 踩坑记录：最初的正则写成转义点号（`^(\./)+`）。MySQL 字符串字面量默认按 C 转义解析，会吃掉这一层反斜杠，模式实际变成「任意字符 + `/`」重复，于是 `a/movie.mkv` 被归一成 `movie.mkv`，既漏匹配又可能误匹配。改用字符组 `[.]` 后不再需要任何反斜杠。

## 残留风险

1. **收敛依赖租约超时。** 未被标记的任务要等 `UPLOADING_STALE_MINUTES`（默认 30 分钟）才回退重派。这段时间该账号因为「存在 uploading 行不得派发」会整体被排除，不只是这几个任务慢一轮。
2. **归一化范围有限。** 只处理前导 `/` 与 `./`。内部 `//`、尾部 `/` 仍匹配不上；大小写与尾随空格的比较受表 `utf8mb4_unicode_ci` 影响（不区分大小写、PAD SPACE 忽略尾随空格），与 Python 侧并不完全等价。
3. **整批用量统计不再适用。** 旧实现按整批记用量，现在按成功文件记；如果某个 Worker 版本回报 `completed` 但成功清单为空，该次上传不会产生用量事件（配额统计偏低、不会是偏高的方向）。
4. **cleaner 未改。** `clean_success` 仍是整批把该 dispatch 剩余 `staged` 置 `completed`，不在本次范围内（见 Issue #2 文档的残留风险 9）。
