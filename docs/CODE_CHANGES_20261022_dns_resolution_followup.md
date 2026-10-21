# 2026-10-22 派发地址 DNS 解析的后续修复（Issue #1 跟进）

## 问题

1. **普通 `OSError` 会中断整轮调度。** `_getaddrinfo_with_timeout` 把解析线程里的异常原样交回调用方，`_verify_resolved_addresses` 只把 `socket.gaierror` 转成"不可派发"。解析器抛出非 `gaierror` 的 `OSError`（如 CPython 对 `EAI_SYSTEM` 的映射）时，异常会穿过 `resolve_dispatch_api_url`，uploader/cleaner 本轮排在该节点之后的节点全部得不到派发。解析发生在获取账号租约之前，不会遗留租约，也不会 fail-open。
2. **测试默认解析桩与文档不符。** `tests/conftest.py` 的 `stub_dispatch_dns` 文档写的是公网地址 `93.184.216.34`，实际返回 `203.0.113.10`（TEST-NET-3，Python 的 `ipaddress` 判为 `is_private`）。主机名类用例因此隐式走"内网放行"分支；"严格模式 + 自报主机名 + 解析到公网 → 放行"没有任何用例覆盖。[出站策略说明](CODE_CHANGES_20261022_worker_url_egress.md) 中"用真正的公网地址 `93.184.216.34`"的描述与当时代码不符，本次已让代码与描述一致。

## 改动

### `main.py`

`_verify_resolved_addresses` 的解析调用在 `gaierror` 分支之后新增 `except OSError`：转成 `ValueError`（节点本轮跳过、记 warning），保留 `raise … from exc`。

- 只包住解析调用；`TypeError` 等编程错误、线程启动失败的 `RuntimeError` 照常抛出。
- 告警只含固定说明、异常类型和整数 errno（`type(exc.errno) is int`，否则写"未知"）；不拼入 `strerror`、`filename`、`args` 等异常携带的数据。

### `tests/conftest.py`

默认解析桩改为 `93.184.216.34`，并注明不能用 TEST-NET 文档段的原因。MySQL 透传集合与逻辑不变。

### `tests/test_main_api.py`

| 用例 | 覆盖 |
| --- | --- |
| `test_dispatch_{uploader,cleaner}_tasks_skips_node_whose_resolver_raises_oserror_and_continues` | 故障节点排第一：只向后面的健康节点 POST；账号租约获取序列恰为健康节点 |
| `test_verify_dispatch_target_rejects_host_when_resolver_raises_plain_oserror` | 转成 `ValueError`，`__cause__` 是原异常对象 |
| `test_resolve_dispatch_api_url_propagates_programming_errors_from_resolver` | `TypeError` 原对象传播，不被吞成跳过 |
| `test_dispatch_url_resolver_oserror_warning_omits_exception_text[numeric/missing/non-numeric-errno]` | 告警含节点、类型、errno 或占位；注入的哨兵文本不进日志 |
| `test_default_dns_stub_resolves_to_public_address_accepted_in_strict_mode` | 默认桩结果 `is_global`，严格模式下放行 |
| `test_strict_mode_dispatch_url_follows_resolved_address_class` | 严格模式：公网放行，普通私网与元数据拒绝 |

`test_report_status_403_quota`、`test_report_status_credential_invalid_disables_account` 改为局部 mock `trigger_dispatch_uploader_tasks_async` 并断言触发原因：原先它们会启动不被等待的后台调度线程，线程可能在替身撤销后真实连接 `DB_HOST`，而改成公网桩会让有外网的环境里这条路径指向可路由地址。

## 测试

在 Oracle-Phoenix 隔离测试容器（internal 网络，Python 3.10.20、MySQL 8.4.8、rclone v1.65.0-DEV）中执行：

- RED：未改 `main.py` 时新增的 6 个 OSError 用例失败（`OSError` 穿透），编程错误用例通过；未改桩时默认桩契约用例失败（`IPv4Address('203.0.113.10').is_global` 为 False）。
- 变异：捕获扩大为 `Exception`、告警拼入异常文本、忽略私网开关、严格模式拒绝所有不受信任目标，分别被对应新增用例检出；严格模式变异下 `test_main_api.py` 的原有用例全部通过，只有本次新增的两条检出。
- 全量：**406 passed, 0 skipped**（root 与非 root 相同），收集 ID 与基线 397 条相比只新增上表 9 条，无删除。
- Python 3.11 待 CI。

## 残留

- 注入的 `OSError` 只验证 Python 线程包装与异常转换；CPython/glibc 原生 `EAI_SYSTEM` 的实际触发与文本内容未独立验证。
- 系统性解析故障下每个节点都会被拒绝（fail-closed），本轮可能零派发；告警从原来的 APScheduler ERROR 级 traceback 变为逐节点 WARNING，只监听 ERROR 的告警可能捕获不到。
- 线程启动失败、超时解析线程累积、DNS 缓存容量、测试出口护栏、DNS 改写的 TOCTOU 仍为后续项。
