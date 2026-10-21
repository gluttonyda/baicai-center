# 2026-10-22 节点派发地址的出站策略加固（Issue #1）

## 问题

`worker_api_url` 有两个来源：`WORKER_API_URL_MAP` 白名单，或节点在 `/api/report_files` 自报（并写入 `worker_registry`）。PR #6 之后派发前会重新套用白名单，但仍有三个缺口：

1. **只校验字面量格式**：`http://127.0.0.1:8001/...`、`http://169.254.169.254/...` 这类地址格式完全合法，会被放行；
2. **不校验解析结果**：域名可以解析到回环/链路本地/内网地址，DNS 记录也能在注册之后被改写；
3. **空映射时的信任是隐式的**：`WORKER_API_URL_MAP` 为空就无条件信任自报地址，没有显式开关，部署方看不到自己在信任什么。

## 改动

### 派发前复核实际地址（`verify_dispatch_target`）

`resolve_dispatch_api_url()` 现在会先按白名单解析出地址，再做一次地址分类复核，**每次派发都重新解析**（降低"注册时是公网、之后 DNS 改成元数据地址"的风险）：

- 字面量 IP 直接分类；
- 域名走 `getaddrinfo`，**任一解析结果被拒就整条拒绝**（防止多记录绕过）；
- 始终拒绝：回环（`127/8`、`::1`）、链路本地（`169.254/16`、`fe80::/10`，含云元数据 `169.254.169.254`）、组播、未指定（`0.0.0.0`/`::`）、保留地址；
- 内网地址（RFC1918、IPv6 ULA）默认允许——容器网络里的 Worker 就是内网地址，简单禁止会直接打断现有部署；
- 主机名解析失败同样拒绝并跳过本次派发（不 fail-open）。

### 三个显式开关

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `TRUST_SELF_REPORTED_WORKER_API_URL` | `true` | 空映射时是否信任节点自报地址。设 `false` 后所有节点必须在 `WORKER_API_URL_MAP` 里，否则拒绝派发（启动时打警告） |
| `ALLOW_PRIVATE_WORKER_URLS` | `true` | 是否派发到 RFC1918/ULA。设 `false` 后只有 `WORKER_API_ALLOWED_HOSTS` 里的主机可以 |
| `WORKER_API_ALLOWED_HOSTS` | 空 | 逗号分隔的主机名白名单，命中后跳过地址分类（用于确实要访问内网/代理的受控场景） |

### 启动一致性检查

`log_worker_api_url_map_gaps()` 在启动时比对 `worker_registry` 与白名单：已注册但不在白名单里的节点会被列出（这些节点本轮起会被跳过派发）。数据库不可用时只告警，不影响启动。

### 重定向

两处派发请求显式设置 `follow_redirects=False`：重定向目标不受白名单与地址分类约束，跟随等于绕过。

## 验收条件对照

- [x] 无凭据、错误签名、跨节点身份无法更新注册地址 —— 新增 `test_report_files_rejects_cross_node_identity_spoofing`：用 A 节点密钥声明自己是 B 节点返回 401，且不触发 `worker_registry` 更新。
- [x] 非空映射覆盖自报 URL，未登记节点被拒绝 —— PR #6 起生效，本 PR 未回退。
- [x] 预置旧注册地址后修改映射，两种调度下一次出站都只能访问新批准地址 —— 新增 `test_dispatch_url_resolution_rejects_registered_metadata_address`：`worker_registry` 里是元数据地址时派发返回 `None`；uploader/cleaner 两条路径都经过 `resolve_dispatch_api_url`。
- [x] 空映射的允许/拒绝行为由显式配置控制，并有升级说明 —— `TRUST_SELF_REPORTED_WORKER_API_URL` + 启动警告。
- [x] 覆盖 userinfo、IPv4/IPv6、异常端口、DNS 变化、重定向和明确批准的容器地址 —— 参数化用例覆盖 `127.0.0.1`、`[::1]`、`169.254.169.254`、`0.0.0.0`、`224.0.0.1`、DNS 解析到元数据、内网开关开/关、`WORKER_API_ALLOWED_HOSTS` 命中、解析失败；userinfo/端口由既有 `validate_api_url` 用例覆盖。
- [x] HMAC 请求不携带共享 Bearer —— 既有 `test_encode_worker_request_uses_node_hmac_without_bearer`。

## 测试

- 全量：**373 passed, 0 skipped, 4 warnings**（真 MySQL 8.4.8 + 真实 rclone v1.65.0）。
- `tests/conftest.py` 新增 autouse fixture 为派发主机名提供稳定的测试解析结果（专测地址分类的用例自行覆盖），并对测试用 MySQL 容器名透传真实解析。

## 残留

- **TOCTOU**：解析与真正建连之间仍存在窗口（DNS 可以在两次之间改答案）。彻底缓解要靠出口网络策略（只允许访问 Worker 端口），本次只做到"派发前复核 + 拒绝已知危险地址"。
- 不校验 TLS 证书链是否可信（`https` 仍走系统默认校验，但 Worker 普遍是明文 http）。
- 内网地址默认放行是刻意取舍：容器网络部署必须如此；只要不改默认值，就能通过元数据地址（链路本地）这一关。
- 兼容认证不等于兼容完整 v1 调度，旧 Worker 的行为不变。
