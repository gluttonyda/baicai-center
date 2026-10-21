# 2026-10-21 uploader 改用 raw 清单说明

本文记录 `fix/uploader-special-filenames` 分支（Issue #2）的代码行为、兼容影响和测试方式。内容针对仓库代码，不代表已部署到任何运行节点。本轮没有修改数据库结构或数据，也没有改动 cleaner、调度器和 Center API。

它处理的是 [2026-10-21 调度互斥与鉴权加固说明](CODE_CHANGES_20261021.md#局限与后续问题) 中“局限与后续问题”的第 4 条。

## 问题

`rclone --files-from` 读取清单时，会先去掉每行首尾的空白（Go `strings.TrimSpace`，除普通空格外还包括 Tab、全角空格 U+3000、NBSP 等 Unicode 空白），然后跳过空行以及以 `#`、`;` 开头的行。uploader 的清单由 Center 下发的相对路径逐行生成，所以：

- 相对路径以 `#` 或 `;` 开头（第一层目录名或文件名以它们开头）的文件不会被搬运，rclone 照样返回 0。
- 相对路径首尾带空白的文件会按裁剪后的名字匹配。同目录下如果恰好有裁剪后同名的文件，搬走的是那个**未列入任务的文件**，回报时也用它的名字。

Issue #2 用真实 rclone 复现过：清单中写入 `normal.txt`、`#hash.txt`、`;semi.txt`、`" spaced.txt "`，`rclone copy --files-from` 退出码为 0，但目标目录中只有 `normal.txt`。本轮新增的集成用例（rclone v1.65.0-DEV）在修改前的输出如下（省略日期时间前缀），未列入任务的 `spaced.txt` 被复制：

```text
INFO  : spaced.txt: Copied (new)
INFO  : normal.txt: Copied (new)
INFO  : 电影/中文 文件.mkv: Copied (new)
INFO  : movie/name: Copied tricky.txt: Copied (new)
```

## 变更摘要

| 位置 | 之前 | 现在 |
| --- | --- | --- |
| `run_uploader` 的 `rclone copy` / `rclone move` | `--files-from` | `--files-from-raw` |
| `detect_remote_existing_files`（`move` 任务本地缺失时的 `lsf` / `lsjson` 远端复核） | `--files-from` | `--files-from-raw` |
| 成功回报的日志解析 | `INFO\s+:\s+(.+?):\s+(Copied…\|Skipped\|Deleted)`：`\s+` 会吃掉路径的前导空白；懒惰匹配遇到路径中的 `: Copied` 会提前截断 | 新增 `parse_rclone_transfer_line`：`INFO\s*: (.*): (Copied(?: \([^)]*\))?\|Moved(?: \([^)]*\))?\|Skipped\|Deleted)$`，只去掉一个分隔空格，贪婪匹配到行尾的动作；同时补上了 `Moved` |
| `lsjson` 的 `ModTime` 解析 | 直接调用 `datetime.fromisoformat`；在 Python 3.10 上无法解析 rclone 固定输出的 9 位小数秒 | 先把小数秒截断或补齐到 6 位再解析 |
| `detect_cleaner_dest_existing_files` | `--files-from` | 已改为 raw（该函数目前无调用方，改它只是避免以后接线时重演同一问题） |

## 三处 `--files-from` 的用途

| 函数 | 用途 | 处理 |
| --- | --- | --- |
| `run_uploader` | 真正搬运：`rclone copy/move <来源根目录> <账号>:<目标目录>` | 改为 raw |
| `detect_remote_existing_files` | 只读探测，不搬运。`move` 任务本地文件已不存在时，复核远端是否已有同名文件（带指纹时还要求大小和修改时间一致），命中的按成功回报。它的清单与上传清单是同一组相对路径 | 改为 raw。否则 `#`/`;` 开头的文件查不到，带空白的文件还会命中裁剪后同名的另一个远端文件，被当作补偿成功 |
| `detect_cleaner_dest_existing_files` | 只读探测，函数名和日志都表明它属于 cleaner 的目标盘复核。仓库内没有任何调用方（测试中也没有） | 本轮只把它的清单参数改为 raw（一行改动，行为不变，因为无调用方），避免以后接线时重演同一问题 |

这几处都不需要注释语义：清单由 Worker 根据 Center 下发的路径生成，里面不会出现注释行。

## 行为说明

### 清单与路径

- 路径仍由 `resolve_task_path` 生成：拒绝 NUL、CR、LF；去掉前导 `/` 后执行 `normpath`；拒绝 `..` 越界；按 `realpath` 核对 `ALLOWED_BASE_DIRS` 和来源根目录。不做 `strip`，也不改名。
- **含控制字符的路径逐文件回执 `not_found`**（Tab、`\x07`、`\x7f` 等）：rclone 的 local backend 会把控制字符转义成符号（Tab→`␉` U+2409），清单里写真实 Tab 时目标端文件名被改写、甚至完全搬不动，而 rclone 仍返回 0。
  - 处理方式是**跳过单个文件 + 记 warning + 计入 `missing_files`**，而不是拒绝整个 payload。原因：`TaskPayload` 若整批返回 422，Center 会把整批任务打回 `pending` 并释放锁，下一个 60 秒周期又取到同一批、再次 422，该账号组会永久卡住（这是审查阶段发现的可用性回退，已修）。
  - 取舍说明：这类文件用转义后的名字作为清单项其实可以上传成功（但目标端文件名会被改写），本轮选择"拒绝并显式回执"而不是"自动改名上传"，避免 Center 记录的路径与实际文件不一致。
- `--files-from-raw` 不做 glob：`glob*star.txt`、`br[ack]et.txt` 这类名字按字面搬运。旧 `--files-from` 内部同样走精确匹配（不展开 glob），所以这里没有语义损失，集成用例已锁定该行为。
- 清单写入的是相对来源根目录的路径；远端复核时则是相对 `<账号>:<目标目录>` 的路径。两者的相对路径相同。
- `--files-from-raw` 逐行原样匹配，只去掉行尾换行（rclone 按行读取并去掉行尾 `\r`），不做 glob。首尾空白、`#`/`;` 开头都算文件名的一部分。之所以继续拒绝 CR/LF，是因为它们会破坏“一行一个路径”。

### 未记录文件告警

一次 `run_uploader` 结束时，在**确实解析到逐文件日志**（`parsed_transfer_lines > 0`）且最终状态为 `completed` 的前提下，对比「本次清单」与「已成功 + 已缺失」的差集；若仍有文件既无成功也无缺失记录，打一条 warning 并列出前 5 个路径。

- 为什么加这两个前提：目标端已存在同一批文件时，rclone 只输出 `There was nothing to transfer`，没有任何 `<路径>: Copied/Skipped` 行；如果无条件比较差集，会把这批文件全部误报为"可能未被搬运"（重派已搬过的批次时很常见）。
- 失败状态（`403_quota`、`credential_invalid`、`failed`）不打印该告警：状态本身已经表达了结果，再叠加一条"可能未被搬运"只会制造噪音。
- 这是可观测性补丁，不改变回报状态；Center 侧「整批标成功」的问题单独跟踪（Issue #7）。

### 成功回报

rclone 文本日志的格式为 `<日期> <时间> INFO  : <对象路径>: <动作>`。对象路径原样输出，带空格时不加引号也不转义。以下是实测输出（省略日期时间前缀）：

```text
INFO  :  spaced.txt : Copied (new)
INFO  : #hash.txt: Deleted
INFO  : movie/name: Copied tricky.txt: Copied (new)
```

解析规则：跳过 `INFO` 后的冒号和**一个**分隔空格，从这里开始贪婪匹配，直到行尾最后一个 `: <动作>` 之前的内容都算路径。动作只接受 `Copied`、`Copied (<括号说明>)`、`Skipped`、`Deleted`，与之前一致。`move` 模式下同一个文件会依次出现 `Copied (new)` 和 `Deleted` 两行，按路径去重。

我们没有改用 `--use-json-log`，原因有三点：

1. 动作固定出现在行尾，路径只是 `INFO  : ` 与行尾 `: <动作>` 之间的内容。路径里能打断“一行一条”的只有 CR/LF，而它们已经被拒绝。所以文本正则可以唯一确定路径，包括首尾带空白和中间含 `: Copied` 的路径。
2. 配额判断（`is_quota_error`）、凭据失效判断（`is_credential_invalid_error`）和控制台日志都基于原始文本行。改成 JSON 就要同时改这两个判断，运维看到的日志格式也会变，这超出了本 Issue 的范围。
3. rclone 1.70 前后日志实现有调整，JSON 字段同样存在版本漂移的风险，并不比文本格式更稳定。

### 远端复核

`move` 任务的文件在本地已不存在时，Worker 用同一份 raw 清单对目标目录执行 `lsf`（不带指纹）或 `lsjson`（带大小或修改时间指纹）。rclone 只会列出清单中的文件，所以不会出现“命中裁剪后同名文件”的情况。

`lsjson` 输出的 `ModTime` 固定带 9 位小数秒（实测 `2023-11-14T22:13:20.000000000Z`）。Python 3.10 的 `fromisoformat` 只接受 3 位或 6 位小数，以前在 3.10 上解析一律抛 `ValueError`，带指纹的复核**从未命中过**。这个缺陷与文件名无关，是新增的真实 rclone 用例暴露出来的。现在先把小数秒截断或补齐到 6 位再解析。Worker 镜像使用 `python:3.11-slim`；根据 3.11 `fromisoformat` 的实现（超过 6 位的小数会截断），生产环境应该不受影响，但本轮只在 3.10 上做了验证。

## 兼容影响

1. **以前被静默跳过的文件，升级后会真正上传。** 这里指 Center 新派发（包括仍处于 `pending`、或者回退后重新排队）的任务中，相对路径以 `#`/`;` 开头或首尾带空白的文件：
   - `copy` 来源（默认 `/data/links`、`/cache/links`）：上传到中转目录，之后由 cleaner 搬到目标盘。
   - `move` 来源（默认 `/data/rclone-cache`）：**会被搬走，本地源文件随之删除。** 如果节点上确实有这类文件而且不希望它们被搬运，请在升级前处理。
2. **升级前已经“完成”的这类任务不会自动补传。** Worker 回报 `completed` 时，Center 调用 `mark_all_uploading_tasks_success` 把本批所有 `uploading` 任务都标为成功，不看 `successful_files`。所以以前被 rclone 跳过的文件，在 Center 中已经进入 `staged`/`completed`，实际却从未上传，仍留在本地。升级不会重新派发它们，Scanner 也只在文件指纹变化时才会重新上报。需要人工排查，可以先只读列出可能受影响的路径（在 Worker 宿主机上执行，按实际来源目录调整）：

   ```bash
   # 第一层名字以 # 或 ; 开头，或以空白开头（相对路径的行首）
   find /data/links /cache/links /data/rclone-cache -mindepth 1 -maxdepth 1 \( -name '#*' -o -name ';*' -o -name '[[:space:]]*' \)
   # 文件名以空白结尾（相对路径的行尾）
   find /data/links /cache/links /data/rclone-cache -type f -name '*[[:space:]]'
   ```

   `[[:space:]]` 是否匹配全角空格等 Unicode 空白取决于宿主机的 locale，结果只能作为线索。
3. **首尾带空白、且同目录有裁剪后同名文件的情况。** 以前搬走的是那个未列入任务的文件（`move` 模式下本地也会删除），现在只搬清单中的文件。以前被误搬的文件不会恢复。
4. **回报路径。** 以前带前导空白的路径回报时会丢掉前导空白，含 `: Copied`、`: Deleted`、`: Skipped` 的路径会被截断，Center 匹配不到这些路径。在 `403_quota`、`failed` 等需要按 `successful_files` 抢救的场景中，这些文件会被回退重传。现在按原样回报。Center 端只拒绝 NUL/CR/LF，带首尾空白的路径可以正常接收。
5. **rclone 版本。** `--files-from-raw` 需要 rclone 1.52 或更高版本，与 cleaner 的要求相同。Worker compose 注释中的生产版本是 1.65；镜像里通过 apt 安装的 1.60 会被宿主机 `/usr/bin/rclone` 的只读挂载覆盖。低于 1.52 的 rclone 会因为不认识这个参数而以非零码退出，uploader 回报 `failed`，任务回退，不会静默跳过。

## 测试

新增用例：

- `tests/test_worker_integration.py`（真实 rclone，alias remote 指向本地临时目录）
  - `test_worker_rclone_uploader_transfers_special_filenames[copy|move]`：任务包含 `normal.txt`、`#hash.txt`、`;semi.txt`、`" spaced.txt "`、`电影/中文 文件.mkv`、`电影/全角空格结尾.mkv` 加 U+3000、`movie/name: Copied tricky.txt`。断言内容如下：回报为 `completed`；`successful_files` 与任务路径逐字相同且无多余项；目标目录中的内容正确；`copy` 保留源文件，`move` 删除源文件；同目录中未列入任务的 `unlisted.txt`、`spaced.txt`、`#unlisted.txt`、`电影/未列出.mkv`、`电影/全角空格结尾.mkv` 留在源目录，并且没有出现在目标目录。
  - `test_worker_rclone_move_recovers_special_filenames_already_on_remote[lsf|lsjson]`：上述文件只存在于远端，本地已缺失。`move` 任务应通过远端复核全部按成功回报，裁剪后同名的远端文件不能被当作补偿成功。
- `tests/test_worker.py`
  - `test_parse_rclone_transfer_line_keeps_exact_object_path`（10 组）：首尾空白、`#`、`;`、中文、路径含 `: Copied`、`Deleted`、`Copied (replaced existing)`、`Skipped`、无日期前缀的行。统计行、`ERROR` 行、`NOTICE … Skipped copy as --dry-run` 行不匹配。
  - `test_remote_probe_accepts_rclone_nanosecond_modtime`：9 位小数秒以及带时区偏移的 `ModTime` 能按指纹命中，相差 3 秒的不命中；命令使用 `--files-from-raw`。

在 Phoenix 隔离测试环境（`python:3.10` 镜像、真实 MySQL 8.4.8、rclone v1.65.0-DEV）中的结果：

| 阶段 | 范围 | 结果 |
| --- | --- | --- |
| 只写集成用例，未改实现 | `tests/test_worker_integration.py` | 4 failed, 4 passed（新增的 4 个全部失败） |
| 只改为 `--files-from-raw`，未改正则 | 同上 | 3 failed, 5 passed：文件都已搬运，但 copy/move 回报中 `" spaced.txt "` 变成 `"spaced.txt "`、`movie/name: Copied tricky.txt` 变成 `movie/name`；lsjson 复核没有命中 |
| 改正则，加临时诊断用例 | 同上 | copy/move 通过；lsjson 仍失败。诊断确认 `ModTime='2023-11-14T22:13:20.000000000Z'`，在 Python 3.10.20 下 `ValueError`（诊断用例已删除） |
| 补 `ModTime` 单测后的全量 | `tests` | 3 failed, 303 passed（ModTime 单测、lsjson 集成用例、临时诊断用例） |
| 全部完成 | `tests/test_worker_integration.py` | 8 passed |
| 全部完成 | 全量 `tests` | 305 passed, 4 warnings, 0 skipped |
| 全部完成 | 全量 `tests`，非 root | 305 passed, 6 warnings（多出的 2 条是非 root 无法写 `.pytest_cache` 的 `PytestCacheWarning`） |

基线为 290 个，新增 15 个，合计 305 个。4 条 warning 是 Worker 中原有的 FastAPI `on_event` 弃用提示，与基线相同。说明两点：日志解析单测在实现之前已经写好，但没有单独跑过红灯，正则的问题由上表第二阶段的真实 rclone 输出证明；全角空格用例是在全部转绿之后才加入的回归保护，没有观察过红灯，“它在旧实现下会被裁剪”这一判断依据的是 `strings.TrimSpace` 的语义。

本地复现（需要 PATH 中有 rclone 1.65 或更高版本）：

```bash
PYTHONPATH=.:baicai-work pytest -q tests/test_worker_integration.py tests/test_worker.py
```

## 残留风险

1. **日志格式依赖（含冒号后空白）。** 解析依赖 rclone 文本日志中 `INFO  : <路径>: <动作>` 的格式，其中 `INFO` 与冒号之间允许任意多空格、冒号后严格一个空格；本轮只在 v1.65.0-DEV 上实测。如果 rclone 调整 padding 或用 `--log-format` 改变分隔，路径会带入多余空格导致 `mark_success` 失配（此时至少还有上面的 warning 兜底）。CI 安装的是 `rclone-current`，会在新版本上跑同一组集成用例。如果将来格式变化，导致解析不到或解析出的路径与任务对不上，这些文件不会被记为成功：部分失败时，`copy` 任务会回退重传，`move` 任务会靠远端复核补偿。但如果最终状态是 `completed`，Center 会整批标为成功，不看 `successful_files`。
2. **`Copied (…) to: <新名>`。** 目标名与源名不同时（例如 `--name-transform` 或大小写转换），rclone 会在动作后追加 `to: <新名>`，这种行不会被匹配。uploader 目前不会产生这种情况。
3. **文件名触发错误判断（原有问题，本轮未改）。** `is_quota_error` 和 `is_credential_invalid_error` 对每一整行做子串匹配，包括 `Copied` 成功行。文件名中如果含有 `upload limit`、`quota exceeded`、`token expired`、`config reconnect` 等字样，会被误判为 `403_quota` 或 `credential_invalid`（后者会停用账号）。建议后续只对非成功行做这两种判断。
4. **反斜杠（原有问题，本轮未改）。** `mark_success` 会把路径中的 `\` 替换为 `/`。Linux 和 Google Drive 的文件名都可以含 `\`，这类文件回报的路径会与原始路径不一致。
5. **回报的是规范化后的路径（原有行为）。** Center 下发的路径如果带前导 `/`、`./` 或 `//`，回报的是 `normpath` 之后的路径。
6. **整批成功（原有行为）。** 清单中的文件如果在 Worker 检查之后、rclone 执行之前被删除，rclone 对清单中不存在的源文件不报错（这一点依据 rclone 源码，未实测），Worker 回报 `completed`，Center 会把它和其他文件一起标为成功。
7. **未在真实 Google Drive 上验证。** 集成用例使用 alias 指向本地目录。根据 rclone 文档，Drive 后端的默认编码只处理非法 UTF-8，不转义首尾空格和 `#`、`;`，这一点没有实测。
8. `detect_cleaner_dest_existing_files` 已改为 raw，但它仍无任何调用方，属于候选删除项。
