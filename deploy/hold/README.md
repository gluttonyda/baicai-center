# 维护期 hold 发布：只安装，不启动

本目录用于“装上新版本、一个容器都不启动”的维护期发布：

| 文件 | 作用 |
| --- | --- |
| `center.hold.override.yml` | Center override 模板：固定镜像、去掉 build、`restart: "no"`、两个派发开关为 false、TailIP 白名单 |
| `worker.hold.override.yml` | Worker override 模板：固定镜像、去掉 build、`restart: "no"`、三个 tailnet 业务 URL、worker-api 与 scanner 各自当前的接单并发 |
| `hold_install.py` | 安装助手：预检 → 保存旧容器日志 → `up --no-start` → 验收；只做维护升级（每个服务必须已有且只有一个旧容器），从不启动容器 |

`.env`、基础 compose、runtime、媒体目录、端口映射一律不改；override 只覆盖镜像、业务 URL 与 hold 开关。公网端口与 SSH 不在本次范围内。

## 能证明什么，不能证明什么

能证明：镜像 ID 与源码 hash；三台 Worker 镜像一致；镜像内 rclone 的路径与版本；compose 能求值；容器绑定的是预期镜像、挂载和端口没有漂移、Worker 两个服务的接单并发与各自旧容器一致；hold 配置生效（`restart=no`、Center 两个派发开关为 false）；容器停留在 created，维护期不会被自动拉起。可选的 Center 离线导入还能证明新版启动配置校验（TailIP/map/开关）可以通过。

不能证明：新容器的 uvicorn/lifespan 能起来；`/health/ready` 与数据库 schema；新容器之间的 tailnet HTTP 链路；HMAC 端到端；Worker 启动时的 journal 恢复；真实网盘上的 rclone 搬运与大请求 MTU；业务语义变化的实际效果。这些都要等用户另行授权启动时逐项验证。

另外两点：

- **Center 的 Tailscale 密钥 2026-10-25T00:42:31Z 到期**，续期需要用户本人在 Tailscale 后台操作；到期后 tailnet 链路不可用。
- 应用 URL 改走内网，只是 Center 与 Worker 之间的 API 流量走 tailnet；rclone 访问网盘（媒体目的端）仍走各节点的互联网出口。

## 绝对不做

- 不执行 `start`、`up -d`、`restart`、`exec`，不跑业务入口，不做数据库迁移。
- 不跑 HTTP 探针：`/api/dispatch/validate` 会开事务写库，空清单的 scanner 心跳会 upsert `worker_registry`。
- 不运行 rclone 业务命令，不挂生产 rclone 配置跑 `listremotes`。
- 不打印 `docker compose config` 全文或任何环境变量值（全文含 `env_file` 里的密钥）；只用 `config --quiet` / `config --images`。唯一例外是第 3 步用 `grep` 只看两个并发键。
- **维护期不要手工 `docker compose up -d`**：不带 hold override 会用旧配置重建并启动 scanner 与 Center，旧 Center 没有派发开关，业务会直接开跑。安装助手拦不住这一点，只能靠交接说明。

## 0. 前置条件

- `docker compose version` ≥ v2.24（`build: !reset null` 需要），且 `up` 支持 `--pull never`。
- 宿主机有 `python3`（≥ 3.8）用于运行 `hold_install.py`。
- 所有目标容器已经停止（安装助手会再检查一遍，未停止就拒绝）。
- 每个目标服务在现场恰好有一个旧容器。没有旧容器的首次创建、或同一服务有多个旧容器时，安装助手拒绝执行，不属于本流程。

## 1. 固定产物

1. 记录合并后的完整 sha（R），`git archive R | gzip -n` 生成源码包并记录 sha256；服务器上再校验一次。
2. tag 用 `<R12>-<release-id>-<arch>`。构建前先确认该 tag 不存在：`docker image inspect <tag>` 必须失败；已存在就中止，**不覆盖、不重打**。
3. Center 在本机构建；Worker 只在 Gomami-1 构建一次，`docker save` 后算 sha256，再 `docker load` 到另外两台，三台的 `docker image inspect --format '{{.Id}}' <tag>` 必须一致。
4. 回滚准备：记录每个目标旧容器的镜像 ID，给它打一个**新的**唯一回滚 tag（同样先确认 tag 不存在）。安装助手拒绝 `latest` 与无 tag 的镜像。

## 2. 离线静态验证（显式 entrypoint、无网络、只读、无任何挂载）

```bash
# Worker：rclone 路径/版本、源码 hash、语法
docker run --rm --network none --read-only --entrypoint sh "$WORKER_IMG" \
  -c 'command -v rclone; rclone version | head -1; sha256sum /app/worker.py /app/scanner.py /app/scanner-loop.sh'
docker run --rm --network none --read-only --entrypoint python "$WORKER_IMG" -B \
  -c 'import ast; [ast.parse(open(p, encoding="utf-8").read(), p) for p in ("/app/worker.py", "/app/scanner.py")]; print("syntax ok")'

# Center：源码 hash、语法
docker run --rm --network none --read-only --entrypoint sh "$CENTER_IMG" -c 'sha256sum /app/main.py /app/init_sync.py'
docker run --rm --network none --read-only --entrypoint python "$CENTER_IMG" -B \
  -c 'import ast; ast.parse(open("/app/main.py", encoding="utf-8").read(), "main.py"); print("syntax ok")'
```

预期 `command -v rclone` 输出 `/usr/local/bin/rclone`、版本 `rclone v1.65.2`。镜像内的 `/usr/local/bin` 在 `PATH` 上排在 `/usr/bin` 之前，现场挂进来的宿主机 `/usr/bin/rclone` 不会被使用。

## 3. 准备 override 与变量

把模板复制到发布目录（例如 `/opt/baicai-release/<R12>/`），不放进生产目录也可以。在执行安装助手的 shell 里 `export` 模板头部列出的 `BAICAI_HOLD_*` 变量（都不是密钥）。

Worker 的并发值按**服务分别**取当前旧容器的实际值，不取 `.env`，也不互相照抄：

| 变量 | 取值来源 |
| --- | --- |
| `BAICAI_HOLD_UPLOADS_DAY` / `BAICAI_HOLD_UPLOADS_NIGHT` | 当前 worker-api 容器 |
| `BAICAI_HOLD_SCANNER_UPLOADS_DAY` / `BAICAI_HOLD_SCANNER_UPLOADS_NIGHT` | 当前 scanner 容器 |

现场旧 Scanner 没有随 `.env` 的近期修改重建，而 worker-api 重建过，所以同一节点上两者可以不同；如果 Scanner 不固定，`.env` 会把它改掉（实测 Gomami-2 的 Scanner 会从 2/2 变成 3/3，US 的会从 2/2 变成 4/4）。只看这两个键：

```bash
docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' baicai-worker-api | grep '^MAX_CONCURRENT_UPLOADS_PER_WORKER_'
docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' baicai-worker-scanner | grep '^MAX_CONCURRENT_UPLOADS_PER_WORKER_'
```

任一键没有输出，说明旧容器当时用的是镜像/代码默认值。安装助手不猜这个默认值，会拒绝创建；这种情况先停下，确认要保留的值再处理。

compose 的变量插值优先取 shell 环境，其次才是项目目录的 `.env`；不要用 `--env-file`，它会替换掉插值来源。先只看报错，不看全文：

```bash
docker compose --project-directory <dir> -f docker-compose.yml -f <override> config --quiet
docker compose --project-directory <dir> -f docker-compose.yml -f <override> config --images
```

`-f` 必须和现场平时使用的 compose 文件完全一致（顺序也一致），hold override 放在最后。

## 4. 安装（按 Center → Gomami-1 → Gomami-2 → US，逐台、写明服务）

```bash
python3 -B hold_install.py --project-dir /opt/baicai-center \
  -f docker-compose.yml -f /opt/baicai-release/<R12>/center.hold.override.yml \
  --role center --service baicai-api \
  --expect-image-id sha256:<center image id> \
  --evidence-dir /opt/baicai-release/<R12>/evidence-center

python3 -B hold_install.py --project-dir /opt/baicai-worker \
  -f docker-compose.yml -f /opt/baicai-release/<R12>/worker.hold.override.yml \
  --role worker --service worker-api --service scanner \
  --expect-image-id sha256:<worker image id> \
  --evidence-dir /opt/baicai-release/<R12>/evidence-<node>
```

安装助手依次：

1. **预检**（任何改动、任何写盘之前）：`compose config --format json` 只在内存里检查每个目标服务 `restart=no`、没有 `build`、镜像为显式非 latest tag；本地镜像 ID 等于 `--expect-image-id`（防止同名 tag 被悄悄覆盖）；Center 两个派发开关为 `"false"`、map 与 TailIP 非空；Worker 的 `CENTER_API` 等于 `CENTER_URL + /api/report_files`。每个目标服务**恰好一个**旧容器（没有或多于一个都拒绝），且为 exited/created。Worker 的 worker-api 与 scanner 分别核对 `MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY/NIGHT`：旧容器里必须有显式非空值，最终 compose 的值必须逐字相同；旧容器缺键、最终配置缺键或值不同都拒绝。报错只给服务名和键名，不带取值。
2. **留证**：evidence-dir 必须不存在或为空（目录 700、文件 600，只留在服务器，不回传）。旧容器 `docker logs` 写入 `<service>-<id12>.log`（可能含敏感内容）；`before.json` 只含 Id、Name、镜像 ID、状态、StartedAt、RestartPolicy、端口、挂载，不含 Env。
3. **创建**：`docker compose ... up --no-start --no-build --pull never --no-deps <明确的服务>`，输出写入 `compose-up.log`。
4. **验收**：每个目标服务恰好一个容器，状态 created、`StartedAt` 为 `0001-01-01T00:00:00Z`、RestartPolicy 为 `no`、镜像 ID 精确等于预期、挂载与端口和旧容器一致；Center 容器里两个派发开关为 false，Worker 新容器的两个并发值与各自旧容器相同（都只在内存比对）；其它所有容器的状态与 StartedAt 不变。结果写入 `after.json`。

退出码 0 表示全部目标已创建且未启动。预检（第 1 步）或留证（第 2 步）失败时没有改动任何容器。**`up` 失败或验收失败时，部分目标可能已经被替换**（旧容器已删、新容器已创建），需要人工检查现场状态；安装助手不重试、不启动、不自动回滚，也不会声称“全部未做”。

### 可选：Center 离线导入（环境取自最终 compose 求值）

容器创建后，可以用它 inspect 出的环境（也就是 compose 最终求值的结果）在无网络、只读的一次性容器里 `import main`，确认新版启动校验能通过。值通过子进程环境传给 `docker run -e KEY`，不进命令行、不落盘；只打印布尔值和计数：

```bash
python3 -B - baicai-api "$CENTER_IMG" <<'EOF'
import json, os, subprocess, sys
container, image = sys.argv[1], sys.argv[2]
info = json.loads(subprocess.run(["docker", "inspect", container], check=True, capture_output=True, text=True).stdout)[0]
env = dict(item.split("=", 1) for item in info["Config"]["Env"])
# 这几个键 compose 不会设置（取自镜像/运行时），又会改变 docker CLI 自身的行为，交给 docker run 默认值。
env = {k: v for k, v in env.items() if k not in ("PATH", "HOME", "HOSTNAME") and not k.startswith("DOCKER_")}
probe = ("import json, main; print(json.dumps({'uploader': main.UPLOADER_DISPATCH_ENABLED, "
         "'cleaner': main.CLEANER_DISPATCH_ENABLED, 'tailnet_ips': len(main.WORKER_API_TAILNET_IPS), "
         "'resolved': {n: main.resolve_dispatch_api_url(n, '') == u for n, u in main.WORKER_API_URL_MAP.items()}}))")
args = ["docker", "run", "--rm", "--network", "none", "--read-only", "--entrypoint", "python"]
for key in env:
    args += ["-e", key]
result = subprocess.run(args + [image, "-B", "-c", probe], env={**os.environ, **env}, capture_output=True, text=True)
print(result.stdout.strip().splitlines()[-1] if result.returncode == 0 else f"import failed (exit {result.returncode})")
EOF
```

预期 `uploader`、`cleaner` 为 false，`tailnet_ips` 为 3，`resolved` 中每个节点为 true。`import main` 只读取环境、校验配置、创建 FastAPI 与锁对象，不连数据库、不启动调度器；TailIP 字面量不做 DNS。失败时只报退出码，需要原因就在服务器上手工看 stderr（新版的报错不回显配置值）。Worker 不做导入，只做第 2 步的静态检查。

## 5. 回滚（同样只创建）

复制同一份模板作为 rollback override，把 `BAICAI_HOLD_*_IMAGE` 换成第 1 步打的回滚 tag（并发变量保持原值），再用旧镜像 ID 作为 `--expect-image-id` 执行同样的安装命令。目标状态仍是 created、`restart=no`，不 prune、不覆盖 tag，`.env` 从头到尾没有改过。

注意：旧版 Center 不认识 `UPLOADER_DISPATCH_ENABLED`，回滚后的容器一旦启动就会照常派发；是否以及何时启动旧版，需要另行决定，并改用不带 hold override 的原 compose。

## 6. 以后获准启动时（不在本次范围）

启动前在停止状态下复核：Worker journal 中 `accepted/running/pending_report` 计数为 0（非零就暂停该节点，不删 journal）；cleaner 目标 remote 兼容；worker-api 与 scanner 的实际并发值。再把重启策略改回 `unless-stopped` 并按服务启动。恢复派发需要用户另行明确授权。
