# 2026-10-21 派发地址白名单与 URL 校验说明

本文记录 `fix/dispatch-url-policy` 分支（Issue #1）的代码行为、兼容影响和测试方式。它描述的是仓库代码，不代表已经部署到任何运行节点。本轮没有修改数据库结构，也没有修改任何数据库数据。

它处理的是 [2026-10-21 调度互斥与鉴权加固说明](CODE_CHANGES_20261021.md#局限与后续问题) 中“局限与后续问题”的第 1 条。

## 变更摘要

| 范围 | 之前 | 现在 |
| --- | --- | --- |
| uploader / cleaner 派发目标 | 直接使用 `worker_registry.api_url`。启用或修改 `WORKER_API_URL_MAP` 后，在节点重新调用 `report_files` 之前仍向旧地址发请求 | 每次派发前用 `resolve_worker_api_url(node_name, 数据库地址)` 重新解析：白名单非空时使用映射地址，白名单为空时沿用数据库地址 |
| 名单外节点、地址非法的节点 | 照常派发 | 跳过该节点并记录 warning，本轮其余节点照常派发，整轮调度不抛异常 |
| `validate_api_url` | 只检查 `http://`/`https://` 前缀和长度（≤255） | 另外拒绝空白/控制字符、无法解析的地址、userinfo（authority 中含 `@`）、空主机名、端口不是 1-65535 的纯 ASCII 数字 |
| 空白名单启动 | 静默启动 | 新增 `REQUIRE_WORKER_API_URL_MAP`（默认 `false`）。`false` 时启动日志警告一次；`true` 时空映射拒绝启动 |

## 行为说明

### 派发前重新解析地址

- 新增 `resolve_dispatch_api_url()`，内部调用现有的 `resolve_worker_api_url()`：**白名单命中即直接使用映射地址**，不再先校验可能早已失效的历史地址；只有未命中（白名单为空）时才校验并使用数据库地址。返回 `None` 表示跳过该节点。
- uploader：解析在节点循环的最开始，早于并发数查询、账号选择、`acquire_account_lock` 和任务状态更新。被跳过的节点不会产生账号租约、`dispatch_id`，也不会有任务变为 `uploading`。
- cleaner：解析在查到该节点的 `worker_registry` 记录之后、获取 cleaner 租约之前。被跳过时不会获取 cleaner 锁，也不会给 staged 任务绑定 `dispatch_id`。
- 跳过时的日志（warning 级别，不输出原始地址，因为历史记录中可能带 `user:password@`）：
  - `⏭️ 节点 [<node>] 未在 WORKER_API_URL_MAP 白名单中，跳过本次派发。`
  - `⏭️ 节点 [<node>] 的 api_url 不可用（<校验错误>），跳过本次派发。`
- 派发时不会把映射地址写回 `worker_registry`。数据库中的旧地址保持不变，节点下一次 `report_files` 时才会写入映射地址（这是原有逻辑）。

### URL 校验规则

`validate_api_url` 同时用于 `report_files` 的 `worker_api_url` 字段、`WORKER_API_URL_MAP` 中的地址（使用时才校验），以及派发前对数据库地址的重新校验。按以下顺序检查，命中第一条即拒绝：

| 检查 | 错误信息 | 例子 |
| --- | --- | --- |
| 前缀不是 `http://` / `https://`（未改动） | `worker_api_url 必须以 http:// 或 https:// 开头` | `ftp://worker-node-1/task/upload` |
| 长度超过 255（未改动） | `worker_api_url 过长` | |
| 含空白或不可打印字符 | `worker_api_url 不能包含空白或控制字符` | `http://worker node/...`、含 `\n` 或 `\t` |
| `urlsplit` 无法解析 | `worker_api_url 格式非法` | `http://[fd00::1/task/upload` |
| authority 中含 `@` | `worker_api_url 不允许包含用户信息（user:password@）` | `http://admin:pw@worker-node-1:8001/...`、`http://@worker-node-1/...` |
| 主机名为空 | `worker_api_url 缺少主机名` | `http:///task/upload`、`http://:8001/task/upload` |
| 端口非法 | `worker_api_url 端口非法` | `:notaport`、`:99999`、`:0`、`:+8001` |

说明：

- 只检查 authority 中的 `@`；路径、查询串中的 `@`（例如 `?tag=a@b`）不受影响。
- 必须先拒绝空白和控制字符：`urllib.parse.urlsplit` 会静默删除 `\t`、`\r`、`\n`，否则校验的字符串和实际发出请求的字符串可能不一致。
- 端口要求纯 ASCII 数字：CI 和测试镜像使用 Python 3.10，其 `SplitResult.port` 用 `int()` 解析，会把 `+8001` 当成 8001。
- 仍然允许内网主机名、容器服务名、私网 IP、IPv6 字面量和自定义端口，例如 `http://worker-node-1:8001/task/upload`、`http://10.0.0.5:8001/task/upload`、`http://[fd00::1]:8001/task/upload`。
- `report_files` 上报不合法地址时返回 422，错误信息为上表的中文文案，不写数据库。

### `REQUIRE_WORKER_API_URL_MAP`

在导入 `main` 时检查，取值与其他布尔开关一致（`1`/`true`/`yes` 视为开启，大小写不敏感）：

| `WORKER_API_URL_MAP` | `REQUIRE_WORKER_API_URL_MAP` | 结果 |
| --- | --- | --- |
| 非空 | 任意 | 正常启动，不输出空映射警告 |
| 空（未设置、空白或 `{}`） | `false` 或未设置（默认） | 正常启动，日志输出一次 `⚠️ WORKER_API_URL_MAP 为空：……`，其余行为与之前相同 |
| 空（未设置、空白或 `{}`） | `true` | 抛出 `RuntimeError: REQUIRE_WORKER_API_URL_MAP=true 但 WORKER_API_URL_MAP 为空：必须配置 node_name -> api_url 白名单，或显式设置 REQUIRE_WORKER_API_URL_MAP=false 关闭该开关`，进程启动失败 |

警告和检查都在导入阶段，每个进程执行一次。

## 兼容影响

默认配置（`REQUIRE_WORKER_API_URL_MAP` 未设置）不会阻止任何现有部署启动。合法地址的派发行为没有变化。需要注意以下几点：

1. **白名单为空**：派发仍使用数据库地址。唯一的区别是启动时多一条 warning；如果数据库里存有新规则下不合法的历史地址，该节点会被跳过（见第 3 条）。
2. **白名单非空**：
   - 修改映射后，下一轮派发就会使用新地址，不再等待节点重新上报。
   - `worker_registry` 中名单外的节点不再收到新的 uploader/cleaner 任务。这类节点通常是在启用白名单之前注册的，或者已从映射中移除，但仍通过 `report_status` 心跳保持活跃。它们名下的 `pending`、`staged` 任务会一直滞留，直到把节点加回映射，或人工把任务改派到其他节点。已经发出的 dispatch 不受影响，仍可正常回报、续租和收尾。
3. **新的 URL 校验**：
   - Worker 的 `WORKER_API_URL` 如果带 `user:password@`、非法端口或空白字符，`report_files` 会从 200 变为 422，需要先修正 Worker 配置。
   - 白名单非空时，历史地址是否合法已不影响派发（映射优先）；白名单为空时，不合法的历史地址才会导致该节点被跳过，修正 Worker 配置并重新上报，或人工更新 `worker_registry.api_url` 即可恢复。
   - 映射中自身不合法的地址：**进程启动时直接拒绝**（`RuntimeError`，报错含节点名），不再等到派发或上报时才以 500 暴露。映射的 key 与 value 都会去除首尾空白，空的 `node_name` 同样拒绝启动。
4. **开启 `REQUIRE_WORKER_API_URL_MAP=true`**：确认每个活跃节点（包括运行 `init_sync.py` 时使用的 `Init_Scanner`）都已在映射中，再开启。取值会先 `strip` 再识别；写成 ` true ` 也生效。无法识别的取值（例如 `tru`）不会静默当成 `false`，启动时会打一条“无法识别”的 warning。
5. **配置修改需要重启 Center 才生效**：`WORKER_API_URL_MAP` 在导入时读取一次；重启后下一轮派发即使用新地址，不需要等节点重新上报。

上线前可以只读检查一下历史地址中是否有 userinfo（不输出地址本身）：

```sql
SELECT node_name, last_active,
       SUBSTRING_INDEX(SUBSTRING_INDEX(api_url, '/', 3), '/', -1) LIKE '%@%' AS has_userinfo
FROM worker_registry;
```

端口等其他问题以上线后日志中的“跳过本次派发”warning 为准。

## 测试

新增用例：

- `tests/test_main_api.py`
  - uploader、cleaner 各 4 个：映射非空且数据库为旧地址时，POST 目标是映射地址；名单外节点被跳过，没有 POST，也没有任何携带该节点名的 `UPDATE`（未抢租约、未锁任务），记录了 warning，其余节点照常派发；数据库地址端口非法或带 userinfo 时跳过该节点、继续派发其余节点，日志中不出现密码；映射为空时仍使用数据库地址（回归保护）。断言的是 `httpx.post` 的第一个参数（出站 URL）以及 payload 中的 `worker_node`。
  - 用一个按 SQL 片段应答的游标替身（`RoutedCursor`）代替按调用顺序的 `side_effect` 列表。被跳过的节点少执行查询时，后续查询结果不会错位。
  - `validate_api_url` 拒绝 13 种地址：3 种 userinfo、2 种空主机名、4 种非法端口、`ftp://`、损坏的 IPv6、换行符、空格。另有 6 种内网/容器/IPv6/自定义端口/查询串含 `@` 的地址必须通过。
  - `report_files` 对 userinfo、空主机名、非法端口、`ftp://` 返回 422，错误信息为中文，且不执行任何 SQL。
- `tests/test_startup_auth_config.py`（子进程导入 `main`，与现有启动校验写法一致）
  - `REQUIRE_WORKER_API_URL_MAP=true`，映射为空字符串、空白或 `{}` 时，进程非零退出，stderr 中有 `RuntimeError` 和中文提示。
  - 开关未设置或为 `false`、映射为空时，进程正常退出，stderr 中“WORKER_API_URL_MAP 为空”恰好出现 1 次。
  - 开关为 `true`、映射非空时，正常启动且没有空映射警告。

在 Phoenix 隔离测试环境（python:3.10 镜像，真实 MySQL 8.4.8，真实 rclone）中的结果：

| 阶段 | 命令范围 | 结果 |
| --- | --- | --- |
| 只写测试、未改实现 | `tests/test_main_api.py` | 21 failed, 97 passed |
| 只写测试、未改实现 | 全量 `tests` | 26 failed, 256 passed, 4 warnings |
| 实现之后 | `tests/test_main_api.py` | 118 passed |
| 实现之后 | 全量 `tests` | 282 passed, 4 warnings，0 skipped |
| 实现之后 | 全量 `tests`，非 root 运行 | 282 passed, 6 warnings（多出的 2 条是非 root 用户无法写 `.pytest_cache` 的 `PytestCacheWarning`） |

红灯阶段失败的 26 个用例都是新增用例，原因符合预期：出站 URL 仍是旧地址或包含名单外节点、`DID NOT RAISE`、期望 422 实际 200、子进程未拒绝启动或没有警告。新增用例中有 11 个在红灯阶段就通过：两个空映射回归、`ftp://` 的两个拒绝用例、6 个合法地址，以及“开关为 true 且映射非空”的启动用例。它们是回归保护，不是缺陷证明。基线为 245 个，新增 37 个，合计 282 个。4 条 warning 是 Worker 中既有的 FastAPI `on_event` 弃用提示，与基线相同。

本地复现：

```bash
PYTHONPATH=.:baicai-work pytest -q tests/test_main_api.py tests/test_startup_auth_config.py
```

## 残留风险

1. **DNS 重绑定**：校验只针对字符串，不解析域名，也不检查解析出的 IP。映射固定的是主机名而不是 IP，能控制该主机名 DNS 的人仍可以把派发请求指向别处。本轮不处理。
2. **重定向**：本轮不处理。当前依赖 httpx 0.25.2 的默认行为：顶层 `httpx.post` 默认 `follow_redirects=False`，`raise_for_status()` 对 3xx 也会抛出 `HTTPStatusError`。因此 Worker 返回的 3xx 按派发失败处理（uploader 回滚任务和租约，cleaner 释放锁），不会被跟随。这一结论来自 httpx 源码，没有为它单独编写测试。以后如果改用 `httpx.Client(follow_redirects=True)`，需要重新评估。
3. **私网地址**：仍然允许私网、回环和链路本地地址（包括 `127.0.0.1`、`169.254.169.254` 这类元数据地址），因为生产本来就使用内网/容器地址。白名单为空时，签名节点仍可让 Center 请求这类地址，需要由管理员配置白名单并开启 `REQUIRE_WORKER_API_URL_MAP=true` 来控制。
4. **映射只在使用时校验**：启动时不校验 `WORKER_API_URL_MAP` 中每个地址是否合法。不合法的映射条目会让该节点的 `report_files` 返回 500、派发被跳过，而不是在启动时直接报错。之所以不在启动时校验，是为了不让现有部署因为本次升级启动失败。
5. **开关只检查是否为空**：`REQUIRE_WORKER_API_URL_MAP=true` 只保证映射非空，不保证每个活跃节点都在映射中。缺失的节点会在派发时被跳过，并留下 warning。
6. **名单外节点的任务会滞留**：被跳过节点名下的 `pending`/`staged` 任务不会自动改派。cleaner 每轮会对该节点的每个 staged 分组各记录一次 warning，名单外节点积压较多时日志会偏多。
7. **解析器差异**：校验使用 `urllib.parse`，实际请求由 httpx 解析。已经拒绝了会被 `urlsplit` 静默删除的字符，authority 中的 `@` 也一律拒绝。但其他边角写法（例如主机名中的百分号编码）仍可能通过校验。实际结果取决于 httpx 的解析：可能被 httpx 拒绝，也可能变成连接错误，分别走现有的“派发失败”或“结果不确定”分支。本轮没有逐一验证这些写法。
