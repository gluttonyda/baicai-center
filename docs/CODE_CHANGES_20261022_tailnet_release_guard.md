# 2026-10-22 Tailnet 精确豁免与 uploader 维护开关（Issue #22、#23、#24）

## 问题

1. **#22**：Worker 改走 Tailscale 后，`WORKER_API_URL_MAP` 里写的是 TailIP（如 `100.125.75.111`）。TailIP 落在 CGNAT `100.64.0.0/10`，出站策略为了挡住阿里云元数据 `100.100.100.200` 把整个网段列为始终拒绝，所以即使是管理员写进白名单的 TailIP 也无法派发。整段放开不可接受。
2. **#23**：uploader 调度没有开关。lifespan 每 60 秒无条件运行一轮，403/凭据失效回执还会即时触发；只要 Center 起来、有节点心跳，就会回收超时任务并派发真实任务。维护期需要“装上新版本但不派发”。

## 改动

### `WORKER_API_TAILNET_IPS`（#22）

- 默认空，行为与旧版完全一致，也不给已有 map 增加新约束。
- 非空时是逗号分隔的精确 IPv4：只去掉每项两侧空白，必须是规范写法（`str(IPv4Address(x)) == x`）、落在 CGNAT 段、不在 `100.100.100.0/24`。空项、IPv6、v4-mapped、主机名、CIDR、范围、前导零、十进制整数等一律拒绝启动。
- 与 `WORKER_API_URL_MAP` 双向一致：列出的每个 IP 都必须在 map 某个 URL 里以 IP 字面量作主机名出现；map 里任何 CGNAT 字面量（含 v4-mapped 写法）都必须被列出；启用但 map 为空也拒绝启动。只用 `urlsplit`/`ipaddress`，不做 DNS。
- 报错是 `RuntimeError`，只给“第 N 项”，不回显取值、节点名或 URL（可能误填了凭据；形如普通节点名也证明不了不是）；`ipaddress` 自带原值的异常用 `from None` 断开，不随 traceback 打印。
- 运行时：`verify_dispatch_target(api_url, trusted_source=False, *, worker_node=None)`，`resolve_dispatch_api_url` 把节点名传进去（uploader 与 cleaner 共用这一入口）。只有 `trusted_source` 为真、带节点名、完整 `api_url` 等于 `WORKER_API_URL_MAP[worker_node]`、且主机串本身就在批准集合里时，IP 字面量分支才带上豁免；DNS 分支永远不带。
- `_blocked_address_reason` 在命中 `100.64.0.0/10` 时只对这一条 `continue`，不提前返回：其它禁用网段、回环、链路本地、组播、保留等规则照常检查。原始写法是 v4-mapped 时不豁免；`100.100.100.0/24` 即使绕过 loader 被塞进批准集合也照样拒绝。
- 拿不到豁免的情况：缺节点名、非白名单来源、节点错配、端口/路径/查询串/协议与映射不同、节点自报地址、主机名解析到已批准 IP、`WORKER_API_ALLOWED_HOSTS` 命中、十进制/十六进制/八进制/末尾点/v4-mapped 等变形写法。

### `WORKER_API_URL_MAP` 启动校验不回显（#24）

审计复现：map 里写成 `http://100.125.75.111:SENTINEL_SECRET_PORT/task/upload` 时，`parts.port` 的 `ValueError("Port could not be cast to integer value as '…'")` 经两层 `from exc` 留在异常链里，启动 traceback 原样打印端口串；同一报错还直接回显节点名。这条路径在 TailIP loader 之前执行，不受它的 `from None` 保护。

- `_validate_worker_api_url_map_entries` 报错改为 `WORKER_API_URL_MAP 第 N 项的地址非法: <固定原因>`，`from None`；不再回显节点名与 URL。
- `validate_api_url` 里 `urlsplit`（NFKC 报错带整个 netloc）与 `parts.port`（带端口原串）的原始异常改为 `from None`；报错文本不变，上报接口的 422 不受影响。
- `_load_worker_api_url_map` 的 JSON 解析失败只报行列号，`from None`（`JSONDecodeError.doc` 带整段原始 map）。
- TailIP 一致性报错也只给序号，删除了“形如普通节点名就回显”的 `_map_entry_label`。
- 合法性校验本身没有放宽。

### `UPLOADER_DISPATCH_ENABLED`（#23）

- 未设置或为空为 true；只认 true/false/1/0/yes/no/on/off（去首尾空白、不区分大小写）；其它取值 `RuntimeError` 拒绝启动，报错不含原值。
- `dispatch_uploader_tasks()` 在拿进程锁、MySQL 命名锁、连库、发 SQL/HTTP 之前检查，false 时返回 True：定时任务直接结束，即时触发也不会把它当成“调度忙”反复重试。
- 关闭时启动日志提示一次。
- 语义：只冻结下一轮起的 uploader 调度（包括失联节点与 `uploading` 超时回收）；不取消已在执行的轮次，不冻结 `report_status`/`report_files` 等接口的写库，不阻止 Worker 启动恢复。cleaner 继续用已有的 `CLEANER_DISPATCH_ENABLED=false`，代码未改。没有新增 Worker 执行开关。

### 维护期 hold 材料（`deploy/hold/`）

两份 override 模板与一个只创建不启动的安装助手，用法、边界和回滚见 [deploy/hold/README.md](../deploy/hold/README.md)。安装助手只调用 `compose config --format json`、`image inspect`、`ps -aq`、`inspect`、`logs` 与 `compose up --no-start --no-build --pull never --no-deps <服务>`；没有 start/`up -d`/迁移/rclone 分支。

审计后补的两条预检（都在留证目录创建、日志保存和 `up` 之前，只在内存里比较）：

- **旧目标数量**：每个目标服务必须恰好一个旧容器。没有旧容器（首次创建）或多于一个都拒绝；此前缺失时会跳过挂载/端口比较，多个时只取第一个。
- **接单并发逐服务保留**：worker-api 与 scanner 分别比较旧容器实际 Env、最终 compose 的 `MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY/NIGHT`。旧容器必须有显式非空值（缺键时当时用的是镜像/代码默认值，不猜）；最终值必须逐字相同，缺键或改值都拒绝。创建后再拿新容器 Env 与旧值比一次。报错只给服务名与键名。
- 现场求值证据：模板不管 Scanner 时，`.env` 会把 Gomami-2 的旧 Scanner 从 2/2 抬成 3/3、US 的从 2/2 抬成 4/4（旧 Scanner 没随 `.env` 更新重建，API 重建过）。所以 Worker 模板给 Scanner 加了必填的 `BAICAI_HOLD_SCANNER_UPLOADS_DAY/NIGHT`，取当前 scanner 容器的实际值，与 worker-api 的 `BAICAI_HOLD_UPLOADS_DAY/NIGHT` 互不套用。
- Center 角色不做并发核对（本轮范围只覆盖 Worker 两个服务）。

### 文档更正：rclone 优先级

Worker 镜像把官方 rclone 装在 `/usr/local/bin/rclone`，Worker 按 `PATH` 调用 `rclone`，`/usr/local/bin` 排在 `/usr/bin` 之前。现场挂进来的宿主机 `/usr/bin/rclone` 不会被使用，也不能用来固定宿主机版本。此前 `docs/worker_node.md`、`docs/CODE_CHANGES_20261022_worker_image_rclone.md` 与 `baicai-work/docker-compose.yml` 注释写成了“以宿主机为准”，已更正。

## 测试

新增 139 个用例，按纵切片先在 c1b1b44 的代码上跑红，再实现转绿（隔离测试容器：Python 3.10、独立 MySQL）。

| 切片 | 内容 | 改动前 | 改动后 |
| --- | --- | --- | --- |
| #22 | 三个 TailIP 经 `resolve_dispatch_api_url` 放行且解析器被调用即失败；默认空时仍拒；节点/URL/端口/路径/查询/协议错配、缺节点、仅 trusted、自报、DNS 解析到批准 IP、allowlist、v4-mapped 与数字变形全拒；元数据表与 `100.100.100.0/24` 在强行批准时仍拒；只跳过 CGNAT 一条；uploader/cleaner 都把节点名交给出站校验；loader 的非规范/空项/禁区/不一致/空 map 全部 fail-closed 且不做 DNS、不回显；子进程启动校验 | 68 failed / 16 passed（16 个是改动前也必须成立的回归用例） | 84 passed |
| #23 | 默认/合法/非法取值（子进程导入）；关闭时锁、命名锁、连库、调度主体、HTTP 全是炸弹且返回 True；即时触发线程一次返回 True 并被 join；lifespan 注册的定时任务同样只返回 True | 19 failed | 19 passed |
| hold | 假 docker 注入：只创建不启动、留证先于重建且权限 700/600、配置与环境不输出、预检失败不做任何改动、镜像缺失/ID 不符、旧容器未停、evidence 非空、`up` 失败、验收各项漂移、其它容器被动；模板静态契约 | 5 failed / 31 errors（文件不存在） | 36 passed |
| 审计修复（#24、旧目标数量、并发保留） | 子进程完整 traceback 不含 sentinel 端口/主机/节点名/IP、无异常链；JSON 与端口错误 `__cause__` 为空；`validate_api_url` 不挂原始异常；TailIP 报错不回显形如普通节点名的键；旧容器缺失/重复（Worker、Center）在留证与 `up` 前拒绝；API 3→2、Scanner 2→99/3/4、Scanner 套用 API 值、最终缺键、旧容器缺键/为空、两边都缺在留证与 `up` 前拒绝且不回显取值；创建后新容器并发漂移/缺键验收失败；模板逐服务必填 | 28 failed / 4 passed（4 个是改动前也必须成立的回归用例） | 32 passed |

全量：root 与非 root 各 **569 passed, 0 skipped**，4 条 FastAPI `on_event` 弃用警告与基线一致（基线 406 passed）。

## 残留与限制

- 真实 compose 求值（`build: !reset null`、YAML merge key、`${VAR:?}`、`up --pull never`）和真实 docker 上的创建、验收，由 Hermes 在发布前执行；本仓库测试只用假 docker 证明安装助手的调用契约。
- 回滚到旧镜像时旧版 Center 不认识 `UPLOADER_DISPATCH_ENABLED`，回滚容器一旦启动就会派发；回滚只创建不启动。
- 本次不改公网端口与 SSH；Center 的 Tailscale 密钥 2026-10-25T00:42:31Z 到期，需要用户本人处理。
