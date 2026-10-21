# 2026-10-21 依赖拆分与运行时加固说明

本文记录 `chore/ci-and-runtime-hardening` 分支的代码行为、兼容影响和测试方式。它描述的是仓库代码，不代表已经部署到任何运行节点，也没有对任何数据库执行重置。

## 变更摘要

| 范围 | 之前 | 现在 |
| --- | --- | --- |
| `scripts/reset_db.py` | 运行即删除任务数据，没有任何确认 | 必须传 `--yes`，或在交互终端输入数据库名确认；非交互且未传 `--yes` 时拒绝并返回 1。拒绝发生在连接数据库之前。新增只读的 `--dry-run` |
| rclone 集成测试 | `PATH` 中没有 `rclone` 时，`tests/test_worker_integration.py` 的 2 个用例失败 | 这 2 个用例显示为 skipped，原因中给出安装方式 |
| Python 依赖 | `requirements.txt` 同时包含运行时依赖和 `pytest`，生产镜像带测试框架；`cryptography==41.0.5` | 运行时依赖留在 `requirements.txt`，`pytest` 移到新增的 `requirements-dev.txt`；`cryptography==50.0.2` |
| Center 容器 | 以 root 运行 | 以 `baicai`（uid/gid 10001）运行，代码属主为该用户；增加 `PYTHONDONTWRITEBYTECODE=1`、`PYTHONUNBUFFERED=1` |
| Worker 容器 | 以 root 运行 | 未改动，原因见[Worker 仍以 root 运行](#worker-仍以-root-运行) |

## `scripts/reset_db.py`

```bash
# 只读预览：打印将删除的表和行数、将释放执行租约的账号数；不删除、不提交，不需要确认
python scripts/reset_db.py --dry-run

# 交互终端：提示输入数据库名（即 DB_NAME），一致才连接并删除
python scripts/reset_db.py

# 非交互环境（脚本、CI、cron、管道）：必须显式确认
python scripts/reset_db.py --yes
```

- 执行顺序为：读取 `DB_*` 环境变量 → 确认（`--yes`，或在 TTY 中输入数据库名）→ 连接数据库。缺少环境变量、非交互且未传 `--yes`、输入与 `DB_NAME` 不一致、输入被中断（EOF 或 Ctrl-C）时都返回 1，且不建立数据库连接。
- 确认时比较去除首尾空白后的输入与 `DB_NAME`，区分大小写。
- `--dry-run` 优先于 `--yes`，两者同时传入时仍只预览。预览需要连接数据库，只执行 `SHOW TABLES` 和 `SELECT COUNT(*)`，不执行 `DELETE`/`UPDATE`，也不提交。
- 预览数字是执行那一刻的快照。重置前仍应停止 Center 和各节点 Scanner，否则实际删除的行数可能与预览不同。
- 删除范围和顺序没有变化：`account_upload_events` → `file_tasks` → `worker_registry`，随后清空 `drive_accounts` 的执行租约，账号启停与配额状态保留；全部在一个事务中执行，失败时回滚。

### 兼容影响

- 以前在非交互环境直接执行 `python scripts/reset_db.py` 的自动化，现在会被拒绝并返回 1，需要改为显式传 `--yes`（建议先运行 `--dry-run` 核对）。
- 在代码中调用 `reset_database()` 时，默认参数等同于不带 `--yes`；需要删除时显式调用 `reset_database(assume_yes=True)`。

## rclone 集成测试

`tests/test_worker_integration.py` 中的 `test_worker_rclone_upload_and_clean_round_trip` 和 `test_worker_rclone_clean_moves_only_requested_files` 用 `shutil.which("rclone")` 检查可执行文件，找不到时显示为 skipped，`pytest -ra` 会输出安装说明。两者只通过 alias remote 访问本地临时目录，不访问网盘。建议安装与生产一致的 rclone 1.65 及以上：cleaner 使用的 `--files-from-raw` 需要 1.52 及以上，用例中带 `@` 的 remote 名不被发行版 apt 中的 1.60 支持（见 `baicai-work/docker-compose.yml` 注释）。

跳过意味着 cleaner 精确搬运的真实回归在该环境中没有执行。发布前的验证应在有 rclone 的环境中运行，并确认这两个用例是 passed 而不是 skipped。

## 依赖

开发机安装：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
PYTHONPATH=.:baicai-work pytest -q
```

- `requirements.txt` 只保留运行时依赖：fastapi、uvicorn、pymysql、cryptography、pydantic、httpx、apscheduler、dbutils。`httpx` 既是 Center 的运行时依赖，也是 `fastapi.testclient` 所需，留在运行时文件中。
- `requirements-dev.txt` 目前只有 `pytest==8.3.5`，测试没有其他第三方依赖。
- Center 的 `Dockerfile` 仍只安装 `requirements.txt`，生产镜像不再包含 pytest。
- `cryptography` 从 41.0.5 升到 50.0.2。PyMySQL 在非 TLS 连接上完成 MySQL 8 默认的 `caching_sha2_password`（以及 `sha256_password`）认证时，需要它做 RSA 密码交换，因此不能删除。
- 版本选择依据是 GitHub Advisory Database 的 pip 公告范围：42.0.8 仍命中 `GHSA-537c-gmf6-5ccf`（wheel 内置 OpenSSL，`<48.0.1`）、`GHSA-jwv3-5hgf-82ww`（`>=42.0.0,<49.0.0`）、`GHSA-g6cj-pr64-35w5`（`>=44.0.0,<50.0.0`）等多条 high 级公告，因此没有停在 42.x，而取 50.0.2。该版本的实际认证路径已由真实 MySQL 8.4.8 集成用例覆盖（`caching_sha2_password`），不是仅凭版本号推断。
- 仍应把 `pip-audit` 或镜像扫描接入日常检查：wheel 内置 OpenSSL 意味着后续仍可能出现新公告。

### 未做的框架升级（后续单独评估）

fastapi 0.104.1、pydantic 2.5.2，以及由 fastapi 约束解析出的 starlette 0.27.0，本轮都保持不变。升级它们会牵涉 starlette 的行为变化、`on_event` 弃用（当前测试中的 4 条警告），以及 TestClient 与 httpx 必须配套升级等问题，风险高于单个库的补丁升级，需要单独评估，并同时验证 Center 和 Worker。

## Center 容器改为非 root

- 新建用户组和用户 `baicai`（uid/gid 10001，shell 为 nologin，家目录为 `/app`）。`pip install` 仍以 root 执行，依赖安装在系统 site-packages 中，运行用户只能读取。
- `main.py`、`init_sync.py` 以 `--chown=baicai:baicai` 复制，`/app` 目录属主也是 `baicai`，最后切换为 `USER baicai`。
- `EXPOSE 8080` 和 `CMD` 不变。8080 高于 1024，非 root 用户可以监听。
- `docker-compose.yml` 未修改。Center 服务没有任何宿主机挂载，不需要调整宿主机权限；healthcheck 以容器用户运行 `python -c "import urllib.request; ..."` 访问 `/health/ready`，不依赖 root。

兼容影响：

- `docker exec baicai-api ...` 默认以 `baicai` 身份执行。需要 root 排查时使用 `docker exec -u 0 baicai-api ...`。
- 以后如果给 Center 增加宿主机写挂载，宿主机目录需要对 uid 10001 可写。
- 在容器内运行 `init_sync.py` 时，默认的 `FAILED_CHUNKS_DIR=./failed_init_sync_chunks` 位于 `/app` 下，`baicai` 可写。Center 镜像本身不包含 rclone，这一点与之前相同。
- 本轮在测试机构建了 Center 镜像并确认以 uid 10001 运行、`import main` 成功，但没有在预发/生产环境启动过该镜像。上线前仍需在预发环境执行 `docker compose build`，确认容器变为 healthy，并用 `docker exec baicai-api id` 确认 uid 为 10001。

## Worker 仍以 root 运行

本轮不修改 `baicai-work/Dockerfile` 的运行用户。Worker 和 Scanner 依赖以下宿主机挂载，直接切换用户会在运行时失败：

- `/root/.config/rclone`：rclone 配置挂载在容器内 root 的家目录下，rclone 刷新 OAuth token 时会回写该文件。非 root 用户无法进入 `/root`，也无法回写。
- `./runtime`：Worker 的 `task_journal.db` 和 Scanner 缓存都是 SQLite，需要在目录中创建 journal/WAL 旁路文件；现有文件属主为 root。
- `./logs/rclone`：rclone 日志目录。
- `/data/links`、`/cache/links`、`/data/rclone-cache`：`move` 任务会删除源文件，需要对媒体目录有写权限。

这属于运维变更，需要逐台宿主机执行，并准备回滚方案。后续步骤：

1. 选定 uid/gid（可与 Center 一致，使用 10001），在 Worker 镜像中创建用户并设置 `USER`。
2. 把 rclone 配置挂载到非 root 用户的家目录，或用 `RCLONE_CONFIG` 指向可写路径；宿主机配置文件通过 chown 或 ACL 授权给该 uid。
3. 停止 Worker 和 Scanner 后，修改 `runtime/`（包括 SQLite 的 `-wal`/`-shm` 旁路文件）和 `logs/rclone/` 的属主，确认 `task_journal.db` 完整保留。
4. 确认媒体目录对该 uid 可读写（`move` 需要删除源文件）。
5. 先在一个节点灰度，确认上传、cleaner 和 journal 恢复正常后再推广。

- `baicai-work/requirements.txt` 本轮从 9 项精简为 4 项（`fastapi`、`uvicorn`、`pydantic`、`httpx`）。Worker 与 Scanner 只导入这些第三方包，`pymysql`、`cryptography`、`apscheduler`、`dbutils`、`pytest` 均为历史遗留，已移除；Worker 镜像不再携带 `cryptography` 与测试框架。重建 Worker 镜像后确认 `import worker` 正常，且上述 5 个包在镜像中都不存在。

## 测试

在 Phoenix 隔离测试环境中执行（测试镜像基于 python:3.10-slim，安装 `requirements.txt` 和 `requirements-dev.txt`，实际安装 cryptography 50.0.2、pytest 8.3.5）：

| 场景 | 结果 |
| --- | --- |
| 先写 reset_db 用例、未改实现（无 MySQL，有 rclone） | 10 failed（均为 `tests/test_reset_db.py`），208 passed，26 skipped |
| 全部改动后（真实 MySQL 8.4.8 + rclone） | **245 passed，0 skipped**，4 warnings |
| 全部改动后（真实 MySQL，不挂 rclone） | **243 passed，2 skipped**（两个 rclone 用例按设计跳过），4 warnings |
| 真库用例单独执行 | 26 passed |

`tests/test_reset_db.py` 从 2 个用例增加到 11 个（红灯阶段为 10 个，实现后补充了 stdin 已关闭的用例），覆盖：`--yes` 正常删除且不提示、非 TTY 或 stdin 已关闭且无 `--yes` 时拒绝且不连接、TTY 中输入不一致或被中断时拒绝且不连接、TTY 中输入一致后删除、`--dry-run` 只执行查询且不提交、`--dry-run` 与 `--yes` 同时传入仍不修改，以及通过子进程运行脚本时，非 TTY 的 stdin 会被拒绝。

`0 skipped` 的这一次运行同时验证了 `cryptography==50.0.2` 的 MySQL 8.4.8 `caching_sha2_password` 认证路径：真实 MySQL 用例全部通过，说明升级没有破坏连接。

镜像层面另行验证（在同一台测试机本地构建，未推送到任何仓库）：

| 检查 | 结果 |
| --- | --- |
| Center 镜像 `id` | `uid=10001(baicai) gid=10001(baicai)` |
| Center 镜像内 `/app` 属主 | `main.py`、`init_sync.py` 为 `baicai:baicai` |
| Center 镜像内 `import main` | 通过（仅用占位环境变量，不连库） |
| Center 镜像内是否还有 pytest | 否 |
| Worker 镜像内 `import worker` | 通过 |
| Worker 镜像内 pytest/pymysql/cryptography/apscheduler/dbutils | 全部不存在 |

## 持续集成

新增 `.github/workflows/ci.yml`，在 `push` 到 `main` 和所有 PR 上运行三个 job：

| Job | 内容 |
| --- | --- |
| `pytest (3.10 / 3.11)` | 安装官方 rclone（校验 ≥1.65）与 `requirements.txt` + `requirements-dev.txt`，跑 `pytest tests`；随后单独重跑 rclone 集成用例，出现 `SKIPPED` 即失败，避免悄悄丢掉真实回归 |
| `docker image (non-root)` | 构建 Center 镜像，断言容器 uid 非 0、运行时镜像内没有 pytest、`import main` 成功 |
| `worker image` | 构建 Worker 镜像，断言 `import worker` 成功且镜像内不再有 pytest/pymysql/cryptography/apscheduler/dbutils |

- `tests` job 设置 `TASK_JOURNAL_DB_PATH` 指向 `${{ runner.temp }}`：Worker journal 的默认路径是 `/opt/baicai-worker/runtime/task_journal.db`，只有 root 能创建，而 GitHub 托管 runner 以非 root 运行，不覆盖会 `PermissionError`。
- 该结论来自实测：同一套代码以 `--user 1000:1000` 运行，未设变量时 **7 failed**（`test_worker.py` 5 个 + `test_worker_integration.py` 2 个，报错均为 `/opt/baicai-worker` 无写权限），设 `TASK_JOURNAL_DB_PATH` 后 245 passed。
- `CACHE_DB_PATH` 不设也不影响：`tests/test_scanner.py` 的用例都 patch 了 `scanner.CACHE_DB_PATH`，实测未生成缓存库文件。若以后新增不 patch 该路径的用例，需要先给 `scanner.init_cache_db` 补目录创建，否则会以 “unable to open database file” 失败。
- 因此 job env 只覆盖 journal 路径；这不是“两个变量都必需”。
- rclone 版本门禁按数值比较主/次版本，`1.65` 及以上与 `2.x` 都放行，低于 `1.65` 失败；解析不出主次版本同样失败。
- workflow 里的 `actions/checkout`、`actions/setup-python` 固定到 commit SHA，避免可变的 tag 被替换。
- CI 不做真实 MySQL 测试：`tests/test_mysql_integration.py` 在没有 `BAICAI_TEST_MYSQL_*` 时整体 skip，这部分仍需在装了 MySQL 的环境（或本机的隔离容器）里执行，见上文测试小节。
- 仓库此前没有任何 workflow，也没有 `ruff`/`mypy` 配置，因此 CI 暂不含 lint；如需接入要另行加配置，避免把大量既有告警变成阻断。
- rclone 必须用官方构建而不是 `apt install rclone`：发行版 1.60 不支持用例中带 `@` 的 remote 名。

## Worker 镜像内的 rclone 版本（既有隐患，未在本轮修复）

`baicai-work/Dockerfile` 通过 `apt-get install rclone` 安装，Debian bookworm/trixie 提供的是 1.60.1。实测 1.60.1 对含 `@` 的 remote 名会报 `config name contains invalid characters`，而现场账号名（如 `baicai_1@1`）都带 `@`。当前生产 `docker-compose.yml` 把宿主机的 `/usr/bin/rclone` 挂进容器覆盖了它，所以运行不受影响——但这意味着镜像自带的 rclone 是坏的，一旦有人删掉该挂载或换节点部署就会踩到。

CI 的 `worker image` job 现在只打印镜像内 rclone 版本作为信息；要真正修掉，需要把 Dockerfile 改成安装官方构建并按架构选择（amd64/arm64），这属于镜像供应链变更，单独处理。

## 后续问题

1. fastapi、pydantic、starlette 的框架升级需要单独评估；CI 目前只覆盖测试与镜像构建，不覆盖 lint。
2. Worker 切换为非 root 需要按上文步骤进行运维变更。
3. Worker 的 `requirements.txt` 已精简；发布前仍需确认现场镜像确实按新文件重建。
4. 间接依赖（例如 h11、anyio、certifi）没有锁定版本，每次构建可能解析到不同版本。
5. `cryptography` 50.0.2 之后发布的安全公告仍需定期复查，建议把 `pip-audit` 或镜像扫描接入 CI。
6. Worker 镜像内 rclone 仍是 apt 的 1.60.1（见上一节），需要改为官方构建。
7. 测试仍不完全 hermetic：非 root 开发机需要自行设置 `TASK_JOURNAL_DB_PATH`；可后续在 `tests/conftest.py` 用临时目录兜底，而不是依赖 CI 的环境变量。
