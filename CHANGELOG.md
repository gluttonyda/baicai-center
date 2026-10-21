# 更新日志

本文件记录代码行为和升级要求，不代表已对运行节点执行部署。日期使用 UTC；2026-09-14 一节的整理时间对应北京时间 2026-09-15。

## 2026-10-21 uploader 改用 raw 清单（Issue #2）

- uploader 的 `rclone copy`/`rclone move`、`move` 任务本地缺失时的远端复核，以及无调用方的 `detect_cleaner_dest_existing_files`，全部由 `--files-from` 改为 `--files-from-raw`。以 `#`、`;` 开头的相对路径不再被当注释跳过；带首尾空白的路径不再被裁剪，也不会误搬同目录中裁剪后同名的、未列入任务的文件。
- 成功回报改用 `parse_rclone_transfer_line`：取 `INFO  : ` 之后、行尾 `: <动作>` 之前的全部内容，不再丢失前导空白，也不会在路径含 `: Copied` 时被截断；动作集合补上 `Moved`。
- 含控制字符（Tab、`\x07`、`\x7f`）的路径**逐文件**回执 `not_found` 并记 warning：rclone 会把这类字符转义成符号（Tab→`␉`），按原名无法搬运。不整批拒绝，避免 Center 每 60 秒重派同一批而卡死账号组。
- 清理器对含控制字符的路径同样逐文件跳过并记 warning，不再静默回报 `clean_success`。
- `lsjson` 远端指纹复核兼容 rclone 固定 9 位小数秒的 `ModTime`：此前在 Python 3.10 上解析失败，复核一律不命中（Worker 镜像为 3.11）。
- 升级提示：需要 rclone 1.52 或更高版本，与 cleaner 相同。升级前已被整批回报 `completed` 但从未上传的文件不会自动补传，需要人工排查（Center 整批标成功的问题见 Issue #7）。
- 在隔离测试容器（Python 3.10、MySQL 8.4.8、rclone v1.65.0-DEV）中执行全量测试：**311 passed**，非 root 同为 311 passed。细节见 [uploader raw 清单说明](docs/CODE_CHANGES_20261021_uploader_raw.md)。

## 2026-10-21 派发地址白名单（Issue #1）

- 派发前按当前 `WORKER_API_URL_MAP` 重新解析目标地址：非空白名单优先于 `worker_registry` 中的历史地址，白名单外节点跳过并记 warning。
- `validate_api_url` 拒绝 userinfo、空主机名、非法端口、空白/控制字符。
- 新增 `REQUIRE_WORKER_API_URL_MAP`（默认 false）：为 true 且白名单为空时拒绝启动；映射条目在启动时校验。
- 升级提示：Worker 的 `WORKER_API_URL` 若带 `user:password@` 或非法端口，`report_files` 会返回 422；升级前可先查 `worker_registry` 中的历史地址。

## 2026-10-21（依赖拆分与运行时加固）

### 调整

- `scripts/reset_db.py` 删除前必须确认：传 `--yes`，或在交互终端输入数据库名。非交互环境且未传 `--yes` 时拒绝并返回 1；确认在连接数据库之前完成，拒绝时不建立连接。
- `scripts/reset_db.py` 新增 `--dry-run`：只打印将删除的表与行数、将释放执行租约的账号数，不删除、不提交。
- `requirements.txt` 只保留运行时依赖，`pytest` 移到新增的 `requirements-dev.txt`；Center 生产镜像不再包含 pytest。
- `cryptography` 从 41.0.5 升到 50.0.2（PyMySQL 的 `caching_sha2_password` 认证需要它；50.0.2 之前的 wheel 仍带 OpenSSL 等公开公告）。fastapi、pydantic、starlette 未升级，后续单独评估。
- `baicai-work/requirements.txt` 去掉 Worker/Scanner 实际未使用的 pymysql、cryptography、apscheduler、dbutils 与 pytest。
- Center 容器改为以非 root 用户 `baicai`（uid/gid 10001）运行，并设置 `PYTHONDONTWRITEBYTECODE=1`、`PYTHONUNBUFFERED=1`。Worker 镜像仍以 root 运行。
- `PATH` 中没有 `rclone` 时，`tests/test_worker_integration.py` 的 2 个真实 rclone 用例显示为 skipped，不再失败。

### 升级与验证

- 在非交互环境中调用 `scripts/reset_db.py` 的自动化需要加 `--yes`。
- 开发机改用 `pip install -r requirements.txt -r requirements-dev.txt` 安装依赖。
- Center 没有宿主机挂载，切换用户不需要调整宿主机权限；`docker exec` 默认以 `baicai` 身份执行。本轮没有构建生产镜像。
- 在隔离测试容器中（无 MySQL、有 rclone）执行全量测试：**219 passed**，26 skipped，4 条 FastAPI `on_event` 弃用警告。带真实 MySQL 的全量运行和不挂载 rclone 的运行尚未执行。
- 兼容影响、Worker 仍以 root 运行的原因和后续步骤见 [2026-10-21 依赖拆分与运行时加固说明](docs/CODE_CHANGES_20261021_runtime.md)。

## 2026-10-21

### 修复

- 定时与即时 uploader 调度共用非阻塞互斥，不再重叠执行；跨进程使用专用连接上的 MySQL `GET_LOCK`，异常后释放。
- Center 与 Worker 的令牌、签名和节点名比较改为按 UTF-8 字节常量时间比较，非 ASCII 输入返回 401/422，不再导致 500。
- 只有 `HISTORY_SYNC_NODE_NAMES`（默认 `Init_Scanner`）中的签名节点可以提交历史同步；legacy 管理员 Bearer 保持兼容。
- Worker 拒绝危险的 `account_name`/`dest_name`（以 `-` 开头、冒号、逗号、路径、空白或控制符）。
- Worker/Scanner 在 HMAC-only 模式下不再要求 `API_TOKEN`；启用 legacy 时令牌为空会启动失败。
- 连接池改为 `ping=1`，取出连接时替换已断开的连接。
- Cleaner 只搬运本批 dispatch 文件，中转目录中的其他文件保持不动。

### 新增

- `MIGRATION_FILE_PATH_HASH.sql`：补齐 `file_path_hash` 生成列和完整唯一索引；发现重复路径、不兼容的已有列（普通列、其他表达式、VIRTUAL、非 `CHAR(64)`）或前缀/表达式唯一索引时终止且不改数据。`/health/ready` 增加对应检查。
- `tests/test_mysql_integration.py`：真实 MySQL 8 集成测试，未配置 `BAICAI_TEST_MYSQL_*` 时跳过。

### 升级与验证

- 旧库在原有三项迁移之后执行 `MIGRATION_FILE_PATH_HASH.sql`。
- 在隔离测试容器（MySQL 8.4.8、内部网络、仅测试凭据）中执行全量测试：**236 passed**（含 26 个真库用例），另有 4 条 FastAPI `on_event` 弃用警告。没有对生产数据库或运行节点执行变更。
- 兼容影响、局限和后续问题见 [2026-10-21 调度互斥与鉴权加固说明](docs/CODE_CHANGES_20261021.md)。

## 2026-09-14

### 新增

- 调度协议 v2：每批任务携带 `dispatch_id` 和 `worker_node`，Center 与节点之间使用独立 HMAC 密钥签名。
- Worker 使用 SQLite 保存任务及待补送回执，重启后先向 Center 验证租约，再恢复仍有效的任务。
- 上传统计接口提供最近 24 小时、按北京时间日期汇总的上传记录，并显示账号启用状态、锁定原因和解锁时间。
- Scanner 增加缓存链接目录扫描，按来源根目录、相对路径、大小和修改时间保存扫描指纹。
- 新增 `MIGRATION_DISPATCH_PROTOCOL_V2.sql`，补齐租约、来源指纹、协议版本字段及索引。
- 新增临时文件清理预览工具和本地清单与数据库的差异检查工具。

### 调整

- Center 对派发结果不确定的网络错误保留租约，接收回执时验证批次归属；Worker 对重复派发返回幂等结果。
- Scanner 对文件变化重新上报；Center 延后接纳的指纹不写入扫描成功缓存，避免变化被遗漏。
- 上传前核验来源目录、文件大小和修改时间；`move` 任务本地缺失时可复核远端指纹以恢复成功记录。
- 通过配置区分中转目录、旧直传目录和 cleaner 目标 remote；跨盘搬运复用源 cleaner 凭据。
- Center 与 Worker 支持北京时间昼夜并发上限，rclone 的文件传输数和检查数可以单独配置。
- 凭据失效、配额/限流错误和普通失败分别处理；已成功文件先记账，未完成文件按对应策略回退。
- `.clfstmp` 和 `.clfstmp.progress` 在扫描、入库、派发和执行阶段过滤；暂时缺失任务支持延时重新排队。
- Center 容器健康检查改用 `/health/ready`，检查数据库就绪状态。
- 数据库重置脚本按外键依赖顺序在事务中删除任务数据，并清理账号执行租约。
- 账号锁迁移改为仅补缺失字段与索引，重复执行保留已有业务状态；账号导入模板不再重置已有账号启停、配额和运行锁。
- 公开维护工具要求显式选择 remote 或 Worker；忽略凭据、上传日志、SQLite journal 及其旁文件，避免随源码发布。

### 升级与验证

- 已有数据库需按顺序确认账号锁、上传统计、协议 v2 三类迁移；新建数据库使用更新后的 `SCHEMA_MINIMAL.sql`。
- 节点须配置与 Center 匹配的独立密钥并保持时钟同步；旧协议兼容开关仅用于升级过渡，新任务派发要求节点声明协议 v2。
- 普通重启保留 Worker 的任务 journal；生产专用账号路由 SQL 不作为通用迁移发布。
- 本次执行 `PYTHONPATH=.:baicai-work pytest -q`：**139 passed**，另有 **4 条 FastAPI `on_event` 弃用警告**。
- 在独立、无外部网络的 MySQL 8.4.8 临时容器内验证新库及旧库升级：三项通用迁移重复执行通过，已有账号状态、任务与统计记录保留；未对生产数据库执行迁移。
- 行为差异、统计口径和升级限制见 [代码差异说明](docs/CODE_CHANGES_20260914.md)。
