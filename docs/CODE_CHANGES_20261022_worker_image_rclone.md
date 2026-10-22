# 2026-10-22 Worker 镜像改用官方 rclone 构建（Issue #5）

## 问题

`baicai-work/Dockerfile` 用 `apt-get install rclone` 装的是 Debian 仓库里的 **1.60.1**，该版本拒绝含 `@` 的 remote 名：

```
Failed to create file system for "baicai_1@1:": config name contains invalid characters
```

而现场账号名（`baicai_1@1`、`baicai-1@1-3`）都带 `@`。现在没出事是因为 `docker-compose.yml` 把宿主机 `/usr/bin/rclone`（1.65）挂进容器覆盖了它——漏了这行挂载、或改用 K8s / 镜像内直接跑 scanner 时就会踩到。

## 改动

### `baicai-work/Dockerfile`

- 移除 `apt-get install rclone`，改为下载官方构建并按 SHA256 校验后安装：
  - 版本固定在 `ARG RCLONE_VERSION=1.65.2`（与生产宿主机 1.65 同线）；
  - `amd64` / `arm64` / `arm` 三个架构各有独立的 `ARG RCLONE_SHA256_*` 校验值；
  - 用 BuildKit 注入的 `TARGETARCH` 选择包，老式 `docker build` 没有该变量时回退 `dpkg --print-architecture`；
  - 未知架构直接构建失败，不静默安装错版本；
  - 构建末尾打印 `rclone version`，日志里能直接看到实际版本。
- 其余包（ca-certificates、tzdata、bash）保留；新增 `curl`、`unzip` 用于下载解压。

### CI（`.github/workflows/ci.yml`）

`worker image` job 改为矩阵，两个架构都构建、都跑断言：

| 检查 | 内容 |
| --- | --- |
| 版本门槛 | `rclone version` 必须匹配 `v1.65`~`v1.99`，等于把 apt 1.60 挡在门外 |
| `@` remote 可用性 | 写一份 `[baicai_1@1]` 的 alias 配置，真的执行 `rclone --config … lsf "baicai_1@1:"` 并断言输出文件名 |
| 既有检查 | `worker` / `scanner` 可导入、镜像内没有 pytest/pymysql/cryptography/apscheduler/dbutils |

`linux/arm64` 通过 QEMU（`tonistiigi/binfmt:qemu-v7.0.0-28`，镜像 tag 固定）交叉构建后在模拟环境下运行同样的断言。

### 文档

- `baicai-work/docker-compose.yml`：注释改为“镜像已内置 1.65.2，宿主机挂载是可选的”。
- `docs/worker_node.md`：新增“镜像内的 rclone”一节，说明固件版本、哪些挂载仍然必需（只有 `/root/.config/rclone`）、以及宿主机挂载可以删掉。

## 验收条件对照

- [x] 镜像内 `rclone version` ≥ 1.65，且 `rclone lsf "<name>@1:"` 能解析含 `@` 的 remote 名 —— 在 Oracle-Phoenix 实机构建验证：`rclone v1.65.2`，`lsf "baicai_1@1:"` 输出 `sample.mkv`；CI 里同样断言。
- [x] amd64 与 arm64 都能构建并运行 —— CI 矩阵两平台都构建并跑 version / `@` remote / import 断言。
- [x] CI 中有断言，防止回退到 apt 的旧版本 —— 版本正则 + `@` remote 实测两道。
- [x] 文档说明是否仍需宿主机 rclone 挂载 —— 不再必需，见 `docs/worker_node.md`。

## 残留

- rclone 版本升级仍需人工改 `ARG`：刻意不做“自动取最新”，避免同一镜像 tag 在不同时间构建出不同二进制。
- 镜像内 rclone 与宿主机 rclone 现在可能版本不同（镜像 1.65.2、宿主机 1.65.0-DEV）：**以镜像为准**。镜像装在 `/usr/local/bin/rclone`，宿主机挂载落在 `/usr/bin/rclone`，Worker 按 `PATH` 调用而 `/usr/local/bin` 在前，所以保留宿主机挂载也不会改用宿主机版本。（2026-10-22 更正：此前这里写的是“以宿主机为准”，与实际相反。）两种版本都支持 `--files-from-raw` 与含 `@` 的 remote。
