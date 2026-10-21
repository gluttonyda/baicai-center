# 2026-10-21 调度互斥与鉴权加固说明

本文记录 `fix/dispatch-auth-hardening` 分支的代码行为、兼容影响、升级步骤和测试方式。它描述的是仓库代码，不代表已经部署到任何运行节点，也不代表对生产数据库执行过迁移。

## 变更摘要

| 范围 | 之前 | 现在 |
| --- | --- | --- |
| uploader 调度互斥 | 定时任务与 `trigger_dispatch_uploader_tasks_async` 可以同时进入 `dispatch_uploader_tasks` | 两个入口共用进程内非阻塞锁，并在专用 MySQL 连接上持有 `GET_LOCK`；忙时定时任务跳过，即时触发稍后重试 |
| 令牌比较 | Center 管理 Bearer、legacy 节点 Bearer 用 `==`/`!=`；HMAC 签名及 Worker 端比较用 `hmac.compare_digest(str, str)`，非 ASCII 输入抛 `TypeError` 变成 500 | 统一按 UTF-8 字节 `compare_digest`，非 ASCII 输入返回 401/422 |
| 历史同步 | 任意签名节点都能提交 `is_history_sync=true`，把新路径直接写成 `completed` | HMAC 节点必须在 `HISTORY_SYNC_NODE_NAMES`（默认 `Init_Scanner`）中，否则 403；legacy 管理员 Bearer 保持兼容 |
| Worker 账号名 | `account_name`/`dest_name` 只去除首尾空白，可包含 `-` 开头、冒号、逗号、路径和控制符 | 只接受 `[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}`，不再 strip |
| `API_TOKEN` | Worker、Scanner 无论是否启用 legacy 都强制要求 | 仅 `ALLOW_LEGACY_BEARER_AUTH=true`（Worker）或 `SCANNER_SEND_BEARER_AUTH=true`（Scanner）时必填；`init_sync.py` 本来就不需要 |
| 连接池 | `PooledDB(ping=0)` | `ping=1`，每次从池中取出连接时检测并替换断开的连接 |
| `file_path_hash` | 只存在于 `SCHEMA_MINIMAL.sql`；没有迁移，`/health/ready` 不检查 | 新增 `MIGRATION_FILE_PATH_HASH.sql`；readiness 与迁移要求该列为 `CHAR(64) AS (SHA2(file_path, 256)) STORED`、存在只含完整列（`SUB_PART` 为 NULL）的唯一索引，且该列上没有前缀/表达式唯一索引；不满足时 readiness 返回 503，迁移拒绝执行且不覆盖列、不删除索引 |
| cleaner | `rclone move <cleaner>:<中转目录> <目标>` 整目录搬运，Center 下发的文件清单只用于计数 | `--files-from-raw` 只搬运本次 dispatch 的文件 |

## 兼容影响

### 调度互斥

- 进程内锁覆盖定时调度和 403/凭据失效后的即时调度。即时触发遇到调度忙时每 0.2 秒重试，最长等待 `DISPATCH_TRIGGER_MAX_WAIT_SECONDS`（默认 120 秒），超时后交给下一次定时调度。
- 跨进程保护使用 MySQL 命名锁 `baicai-dispatch-uploader:<库名摘要>`。锁持有在每轮调度单独建立的连接上，不经过连接池，因为 `GET_LOCK` 属于会话：池化连接归还后可能被其他借用者重入或在错误连接上释放。即使 `RELEASE_LOCK` 失败，关闭该连接也会让 MySQL 释放锁。
- 每轮 uploader 调度会多建立一个短连接（默认每 60 秒一次）。数据库账号不需要额外权限。
- 只保护 uploader 调度。cleaner 调度（`dispatch_cleaner_tasks`）仍依赖账号租约的条件更新防止重复派发，没有加入同一互斥。
- 默认镜像只运行一个 uvicorn 进程。多副本时可以避免同时执行 uploader 调度，但没有实现选主：每个副本仍注册自己的定时任务，抢不到锁的副本只是跳过。

### 鉴权

- `HISTORY_SYNC_NODE_NAMES` 是逗号分隔的节点名单。使用 HMAC 运行 `init_sync.py` 时节点名必须是 `Init_Scanner`（脚本本身也强制该名称），因此默认值不需要修改。
- `ALLOW_LEGACY_NODE_AUTH=true` 时，持有共享 `API_TOKEN` 的 Bearer 请求仍可提交历史同步。共享令牌本身是管理员凭据，无法区分调用方身份；滚动升级完成后应关闭 legacy。
- Worker 端账号名不再去除首尾空白。Center 下发的名称来自 `drive_accounts`，通常不受影响；若现场账号名包含规则外字符（例如空格、`+`），Worker 会以 422 拒绝派发，需要先在 rclone 配置和数据库中改名。旧 journal 中不符合规则的任务在恢复时会解析失败并记录日志，不会启动。
- HMAC-only 节点可以从 `.env` 删除 `API_TOKEN`。启用 legacy 时 `API_TOKEN` 为空或只有空白会启动失败；运行期间若令牌为空，legacy Bearer 也一律拒绝。

### cleaner

- 中转目录中不属于本批次的文件（其他节点的批次、超过 `MAX_CLEANER_FILES_PER_REQUEST` 的部分、数据库中没有记录的文件）不再被搬到最终盘，而是留在中转目录，等待各自的 dispatch 或人工处理。
- 中转目录积压超过单批上限时，需要多轮 cleaner 才能清空；之前一次整目录 move 可以全部搬走。可按需调整 `MAX_CLEANER_FILES_PER_REQUEST` 和 `CLEANER_DISPATCH_INTERVAL_MINUTES`。
- 清单路径与 uploader 的远端相对路径规则一致：去掉前导 `/` 后执行 `normpath`，`..` 越界路径会被拒绝。清单中已不在中转目录的文件不会导致 rclone 失败，`clean_success` 后整批仍标记为 `completed`，与之前相同。
- `--drive-use-trash=false` 的作用对象是源端：服务端搬运完成后，删除中转目录副本时不进入回收站。它不会删除最终目标盘上的文件。`rclone move` 是搬运，不是对目标的永久删除。
- `--files-from-raw` 需要 rclone 1.52 或更高版本；Worker compose 中注释说明的生产版本为 1.65。

### `WORKER_API_URL_MAP`

本轮没有修改该行为。`WORKER_API_URL_MAP` 为空时，Center 会信任节点上报的 `worker_api_url` 并向它发送 HTTP 请求。签名节点（以及启用 legacy 时持有共享令牌的调用方）可以借此让 Center 访问任意 HTTP/HTTPS 地址。为了不让现有合法部署启动失败，本轮只补充文档，建议生产环境始终配置非空白名单。后续处理见文末“后续问题”。

## 升级步骤

1. 备份数据库，停止 Center 和各节点 Scanner。
2. 依次执行全部迁移。四个脚本都会先检查已有结构，可以重复执行：

   ```bash
   mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_SPLIT_ACCOUNT_LOCKS.sql
   mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_UPLOAD_USAGE_STATS.sql
   mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_DISPATCH_PROTOCOL_V2.sql
   mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < MIGRATION_FILE_PATH_HASH.sql
   ```

3. 如果 `MIGRATION_FILE_PATH_HASH.sql` 报错 `Table '...MIGRATION_ABORTED_<原因>_see_docs' doesn't exist`，脚本在任何 DDL 之前就已终止，没有修改或删除数据、列和索引。按 [Center 节点说明](center_node.md#file_path_hash-迁移) 人工核对处理后重新执行：
   - `duplicate_file_path_rows`：`file_tasks` 中存在完全相同的 `file_path` 多行；
   - `incompatible_file_path_hash_column`：已有 `file_path_hash` 不是 `CHAR(64) AS (SHA2(file_path, 256)) STORED`（例如普通可空列、其他表达式、VIRTUAL 或 VARCHAR）；
   - `prefix_unique_file_path_hash_index`：该列上存在前缀或表达式唯一索引（例如 `UNIQUE (file_path_hash(1))`），无论是否同时存在完整唯一索引。
4. 部署新版 Center，确认 `curl --fail http://127.0.0.1:8080/health/ready` 成功。
5. 部署新版 Worker/Scanner。HMAC-only 节点可以删除 `API_TOKEN`；仍需要 legacy 的节点保留它。

## 测试

开发机：

```bash
PYTHONPATH=.:baicai-work pytest -q
```

未设置 `BAICAI_TEST_MYSQL_HOST`、`BAICAI_TEST_MYSQL_USER`、`BAICAI_TEST_MYSQL_PASSWORD`、`BAICAI_TEST_MYSQL_DATABASE` 时，`tests/test_mysql_integration.py` 会整体显示为 skipped，不会失败。

真实 MySQL 8：

```bash
BAICAI_TEST_MYSQL_HOST=127.0.0.1 \
BAICAI_TEST_MYSQL_USER=root \
BAICAI_TEST_MYSQL_PASSWORD='<测试库密码>' \
BAICAI_TEST_MYSQL_DATABASE=baicai_review \
PYTHONPATH=.:baicai-work pytest -q tests/test_mysql_integration.py
```

每个真库用例都会新建 `review_` 前缀的临时库，结束后删除，因此测试账号需要 `CREATE`/`DROP DATABASE` 权限。不要指向生产实例。

真库用例覆盖：

- 跨会话 `GET_LOCK` 跳过、调度中持锁、异常后释放；
- 被 `KILL` 的池连接在事务路径上自动恢复，并保持 `+00:00` 会话时区；
- 旧结构缺少 `file_path_hash` 或缺少唯一索引时 readiness 失败；
- 用真实 `SHA2` 核实 `movie/0.mkv` 与 `movie/4.mkv` 完整哈希不同、首字符相同：在“只有前缀唯一索引”“完整索引与前缀唯一索引并存”“完整索引与 `LEFT(file_path_hash, 1)` 表达式唯一索引并存”三种结构下，第二个路径经 `report_new_files` 被错误合并；三种结构 readiness 均返回 503，迁移拒绝，行数据和全部索引定义不变；
- `file_path_hash` 为普通可空列、`SHA2(LOWER(file_path), 256)`、VIRTUAL、VARCHAR(64) 时 readiness 返回 503，迁移拒绝，行数据、列定义和索引不变；写法不同的等价合法列（小写类型、反引号、`GENERATED ALWAYS`、`NOT NULL`）通过 readiness 和迁移；
- 迁移升级旧结构、重复执行、遇到重复路径时拒绝且不改数据，结果按索引名、列名和 `SUB_PART` 精确断言完整唯一索引；
- 四个迁移在当前结构上重复执行；
- `report_new_files` 的 upsert 语义：not_found 和指纹变化的 completed 任务重新排队，pending 任务刷新，uploading/staged 保留租约与指纹并返回 `deferred_files`，历史同步，以及同路径去重。

cleaner 的真实 rclone 测试位于 `tests/test_worker_integration.py`，需要 `PATH` 中有 `rclone`。

本轮在隔离测试容器（MySQL 8.4.8，内部网络，仅测试凭据）中执行全量测试：**236 passed**（其中 `tests/test_mysql_integration.py` 26 个），另有 4 条 FastAPI `on_event` 弃用警告，与基线相同。

## 已证伪或修正的判断

- “`report_new_files` 的 `ON DUPLICATE KEY UPDATE` 赋值顺序导致状态或租约错误”：在 MySQL 8.4.8 上不成立。MySQL 按从左到右执行赋值，后面的 CASE 读取更新后的 `status`。真库用例构造了“按旧值求值”和“按新值求值”结果不同的数据，实测得到预期语义。该依赖已在代码注释中说明，`status` 必须保持为第一个赋值。
- “旧库缺少 `file_path_hash`”：仓库中所有公开版本的 `SCHEMA_MINIMAL.sql` 都包含该列和唯一键，无法证明某个现有库缺失。确实存在的问题是缺少迁移和 readiness 检查，本轮已补齐。
- “init_sync 需要去除必需的 `API_TOKEN`”：`init_sync.py` 本来就只使用 `NODE_TOKEN` 签名，不读取 `API_TOKEN`。本轮增加了子进程测试，没有改动它。
- Center 管理 Bearer 与 legacy 节点 Bearer 之前用 `==`/`!=` 比较，非 ASCII 输入不会导致 500，但比较不是常量时间。会抛 500 的是 HMAC 签名比较及 Worker 端的 `compare_digest(str, str)`。

## 局限与后续问题

1. `WORKER_API_URL_MAP` 为空时 Center 信任上报地址（SSRF 风险）。建议后续二选一：启动时在空映射下打印醒目警告并提供 `REQUIRE_WORKER_API_URL_MAP` 开关；或对上报地址做主机白名单或私网段限制。需要先确认现网所有节点都已配置映射。
2. uploader 调度跨进程互斥只是“抢不到就跳过”，没有选主；cleaner 调度没有加入该互斥。
3. 即时触发与上一轮触发线程收尾之间仍有很小的窗口，新的触发可能被合并后丢失，由下一次定时调度兜底（60 秒内）。
4. ~~uploader 仍使用 `--files-from`，rclone 会去除行首尾空白并跳过以 `#`/`;` 开头的行。~~ 已在 Issue #2 修复（uploader 改用 `--files-from-raw`），见 [uploader 改用 raw 清单说明](CODE_CHANGES_20261021_uploader_raw.md)。
5. `report_new_files` 使用 `VALUES()`，它在 MySQL 8.0.20 起已被标记为弃用（8.4.8 仍可用）。未来升级 MySQL 前应改为行别名语法，并保持赋值顺序。
6. cleaner 改为精确搬运后，数据库中没有记录、只留在中转目录的文件需要人工处理。
7. FastAPI `on_event` 弃用警告仍未处理。
8. readiness 与 `MIGRATION_FILE_PATH_HASH.sql` 只检查 `file_path_hash` 上的唯一约束。`file_tasks` 其他列上的唯一约束（例如某个旧库若存在 `file_path` 前缀唯一索引）同样可能让 `ON DUPLICATE KEY UPDATE` 命中另一条任务，本轮未检查。检查依赖 `information_schema.STATISTICS.EXPRESSION` 与 `REGEXP_REPLACE`，需要 MySQL 8.0.13 及以上（测试版本为 8.4.8）。
