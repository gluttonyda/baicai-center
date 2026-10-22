"""在独立子进程中导入各入口模块，验证 HMAC-only 与 legacy 令牌的启动约束。"""
from pathlib import Path
import os
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKER_DIR = ROOT / "baicai-work"
NODE_TOKEN = "test-node-token-at-least-32-characters"


def run_import(module: str, cwd: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"API_TOKEN", "ALLOW_LEGACY_BEARER_AUTH", "SCANNER_SEND_BEARER_AUTH"}
    }
    env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


WORKER_ENV = {
    "NODE_TOKEN": NODE_TOKEN,
    "CENTER_URL": "http://127.0.0.1:8080",
    "TARGET_REMOTE_DIR": "your-target-dir",
    "WORKER_NODE_NAME": "worker-node-1",
}
SCANNER_ENV = {
    "CENTER_API": "http://127.0.0.1:8080/api/report_files",
    "NODE_TOKEN": NODE_TOKEN,
    "WORKER_NODE_NAME": "worker-node-1",
    "WORKER_API_URL": "http://127.0.0.1:8001/task/upload",
}
INIT_SYNC_ENV = {
    "CENTER_API": "http://127.0.0.1:8080/api/report_files",
    "NODE_TOKEN": NODE_TOKEN,
    "WORKER_NODE_NAME": "Init_Scanner",
    "WORKER_API_URL": "http://127.0.0.1:8001/task/upload",
    "RCLONE_REMOTE_ACCOUNT": "baicai-big",
    "TARGET_DIR": "your-target-dir",
}


def test_worker_starts_hmac_only_without_api_token():
    result = run_import("worker", WORKER_DIR, {**WORKER_ENV, "ALLOW_LEGACY_BEARER_AUTH": "false"})

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("api_token", [None, "", "   "])
def test_worker_refuses_legacy_bearer_without_api_token(api_token):
    extra_env = {**WORKER_ENV, "ALLOW_LEGACY_BEARER_AUTH": "true"}
    if api_token is not None:
        extra_env["API_TOKEN"] = api_token

    result = run_import("worker", WORKER_DIR, extra_env)

    assert result.returncode != 0
    assert "API_TOKEN" in result.stderr


def test_scanner_starts_hmac_only_without_api_token():
    result = run_import("scanner", WORKER_DIR, {**SCANNER_ENV, "SCANNER_SEND_BEARER_AUTH": "false"})

    assert result.returncode == 0, result.stderr


def test_scanner_refuses_legacy_bearer_header_without_api_token():
    result = run_import("scanner", WORKER_DIR, {**SCANNER_ENV, "SCANNER_SEND_BEARER_AUTH": "true"})

    assert result.returncode != 0
    assert "API_TOKEN" in result.stderr


def test_init_sync_starts_without_api_token():
    result = run_import("init_sync", ROOT, INIT_SYNC_ENV)

    assert result.returncode == 0, result.stderr


CENTER_ENV = {
    "API_TOKEN": "test-center-api-token",
    "DB_HOST": "127.0.0.1",
    "DB_USER": "test-db-user",
    "DB_PASSWORD": "test-db-password",
    "DB_NAME": "test-db-name",
}
EMPTY_URL_MAP_WARNING = "WORKER_API_URL_MAP 为空"


def run_center_import(extra_env: dict[str, str], code: str = "import main") -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {
            "WORKER_API_URL_MAP",
            "REQUIRE_WORKER_API_URL_MAP",
            "WORKER_API_TAILNET_IPS",
            "UPLOADER_DISPATCH_ENABLED",
        }
    }
    env.update({**CENTER_ENV, "PYTHONIOENCODING": "utf-8", **extra_env})
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env=env,
        capture_output=True,
        encoding="utf-8",
        timeout=60,
    )


@pytest.mark.parametrize("url_map", ["", "   ", "{}"])
def test_center_refuses_empty_worker_api_url_map_when_required(url_map):
    result = run_center_import({"REQUIRE_WORKER_API_URL_MAP": "true", "WORKER_API_URL_MAP": url_map})

    assert result.returncode != 0
    assert "RuntimeError" in result.stderr
    assert "必须配置" in result.stderr
    assert "REQUIRE_WORKER_API_URL_MAP=false" in result.stderr


@pytest.mark.parametrize("require_flag", [None, "false"])
def test_center_starts_with_empty_worker_api_url_map_and_warns_once(require_flag):
    extra_env = {"WORKER_API_URL_MAP": ""}
    if require_flag is not None:
        extra_env["REQUIRE_WORKER_API_URL_MAP"] = require_flag

    result = run_center_import(extra_env)

    assert result.returncode == 0, result.stderr
    assert result.stderr.count(EMPTY_URL_MAP_WARNING) == 1


def test_center_starts_when_worker_api_url_map_required_and_configured():
    result = run_center_import({
        "REQUIRE_WORKER_API_URL_MAP": "true",
        "WORKER_API_URL_MAP": '{"worker-node-1":"http://worker-node-1:8001/task/upload"}',
    })

    assert result.returncode == 0, result.stderr
    assert EMPTY_URL_MAP_WARNING not in result.stderr


def test_center_refuses_empty_map_when_require_flag_has_padding():
    """防御开关不能因为取值带空格就 fail-open。"""
    result = run_center_import({"REQUIRE_WORKER_API_URL_MAP": " true ", "WORKER_API_URL_MAP": ""})

    assert result.returncode != 0
    assert "必须配置" in result.stderr


def test_center_warns_on_unrecognized_require_flag_but_starts():
    result = run_center_import({"REQUIRE_WORKER_API_URL_MAP": "tru", "WORKER_API_URL_MAP": ""})

    assert result.returncode == 0, result.stderr
    assert "无法识别" in result.stderr


def test_center_refuses_invalid_map_entry_at_startup():
    result = run_center_import({
        "WORKER_API_URL_MAP": '{"worker-node-1":"ftp://worker-node-1/task/upload"}',
    })

    assert result.returncode != 0
    assert "WORKER_API_URL_MAP 第 1 项的地址非法" in result.stderr
    # map 的键和 URL 都可能误填了凭据：报错只给序号。
    assert "worker-node-1" not in result.stderr


# ===== WORKER_API_URL_MAP 启动校验不回显键、URL 与底层异常（Issue #24） =====

SENTINEL_BAD_PORT_MAP = (
    '{"Gomami-1":"http://100.125.75.111:8001/task/upload",'
    '"SENTINEL_SECRET_NODE":"http://100.125.75.111:SENTINEL_SECRET_PORT/task/upload"}'
)
# U+2100 经 NFKC 变成 "a/c"：urlsplit 的 ValueError 会带上整个 netloc。
SENTINEL_NFKC_MAP = '{"SENTINEL_SECRET_NODE":"http://SENTINEL_SECRET_HOST\\u2100x:8001/task/upload"}'


@pytest.mark.parametrize(
    ("url_map", "message"),
    [
        (SENTINEL_BAD_PORT_MAP, "WORKER_API_URL_MAP 第 2 项的地址非法: worker_api_url 端口非法"),
        (SENTINEL_NFKC_MAP, "WORKER_API_URL_MAP 第 1 项的地址非法: worker_api_url 格式非法"),
    ],
    ids=["bad-port", "nfkc-netloc"],
)
def test_center_map_entry_startup_traceback_never_echoes_key_or_url(url_map, message):
    result = run_center_import({"WORKER_API_URL_MAP": url_map})

    assert result.returncode != 0
    # 断言对象是子进程打印的完整 traceback（含异常链），不只是最后一行。
    assert "Traceback (most recent call last)" in result.stderr
    assert f"RuntimeError: {message}" in result.stderr
    assert "SENTINEL" not in result.stderr
    assert "100.125.75.111" not in result.stderr
    assert "Gomami-1" not in result.stderr
    assert "The above exception was the direct cause" not in result.stderr
    assert "During handling of the above exception" not in result.stderr


PRINT_MAP_ERROR_CHAIN = (
    "try:\n"
    "    import main\n"
    "except RuntimeError as exc:\n"
    "    print('CHAIN cause=%s suppressed=%s' % (exc.__cause__ is None, exc.__suppress_context__))\n"
)


@pytest.mark.parametrize(
    "url_map",
    ['{"SENTINEL_SECRET_NODE": SENTINEL_SECRET_VALUE}', SENTINEL_BAD_PORT_MAP],
    ids=["invalid-json", "bad-port"],
)
def test_center_map_startup_errors_cut_exception_chain(url_map):
    # JSONDecodeError.doc 带着整段原始 map；挂在 __cause__ 上就可能被日志/错误收集器带出去。
    result = run_center_import({"WORKER_API_URL_MAP": url_map}, PRINT_MAP_ERROR_CHAIN)

    assert "CHAIN cause=True suppressed=True" in result.stdout, result.stdout + result.stderr
    assert "SENTINEL" not in result.stdout + result.stderr


# ===== WORKER_API_TAILNET_IPS 启动校验（Issue #22） =====

TAILNET_URL_MAP = (
    '{"Gomami-1":"http://100.125.75.111:8001/task/upload",'
    '"Gomami-2":"http://100.100.185.27:8001/task/upload",'
    '"1GServer-US":"http://100.91.91.88:8001/task/upload"}'
)
TAILNET_IPS = "100.125.75.111,100.100.185.27,100.91.91.88"
# 导入前把解析器换成炸弹：启动校验只能用 urlsplit/ipaddress，不能碰 DNS。
NO_DNS_IMPORT = (
    "import socket\n"
    "def _boom(*args, **kwargs):\n"
    "    raise AssertionError('UNEXPECTED-DNS-LOOKUP')\n"
    "socket.getaddrinfo = _boom\n"
    "socket.gethostbyname = _boom\n"
    "import main\n"
    "print('TAILNET_IPS=' + ','.join(sorted(main.WORKER_API_TAILNET_IPS)))\n"
)


def test_center_starts_with_exact_tailnet_ips_and_map_without_dns():
    result = run_center_import(
        {"WORKER_API_URL_MAP": TAILNET_URL_MAP, "WORKER_API_TAILNET_IPS": TAILNET_IPS}, NO_DNS_IMPORT
    )

    assert result.returncode == 0, result.stderr
    assert "TAILNET_IPS=100.100.185.27,100.125.75.111,100.91.91.88" in result.stdout
    assert "UNEXPECTED-DNS-LOOKUP" not in result.stderr


def test_center_default_tailnet_ips_unset_keeps_legacy_startup():
    result = run_center_import(
        {"WORKER_API_URL_MAP": '{"legacy":"http://100.64.0.1:8001/task/upload"}'}, NO_DNS_IMPORT
    )

    assert result.returncode == 0, result.stderr
    assert "TAILNET_IPS=\n" in result.stdout


@pytest.mark.parametrize(
    ("tailnet_ips", "url_map"),
    [
        ("SENTINEL-not-an-ip," + TAILNET_IPS, TAILNET_URL_MAP),
        ("100.100.100.100," + TAILNET_IPS, TAILNET_URL_MAP),
        (TAILNET_IPS, TAILNET_URL_MAP[:-1] + ',"SENTINEL-node":"http://100.88.1.2:8001/SENTINEL-path"}'),
        (TAILNET_IPS, ""),
    ],
    ids=["bad-token", "forbidden-range", "map-unapproved-cgnat", "empty-map"],
)
def test_center_refuses_bad_tailnet_config_without_echoing_values(tailnet_ips, url_map):
    result = run_center_import({"WORKER_API_URL_MAP": url_map, "WORKER_API_TAILNET_IPS": tailnet_ips}, NO_DNS_IMPORT)

    assert result.returncode != 0
    assert "RuntimeError" in result.stderr
    assert "WORKER_API_TAILNET_IPS" in result.stderr
    assert "UNEXPECTED-DNS-LOOKUP" not in result.stderr
    assert "SENTINEL" not in result.stderr
    assert "100.100.100.100" not in result.stderr
    assert "100.88.1.2" not in result.stderr


# ===== UPLOADER_DISPATCH_ENABLED 维护开关（Issue #23） =====

UPLOADER_DISABLED_NOTICE = "UPLOADER_DISPATCH_ENABLED=false"
PRINT_UPLOADER_FLAG = "import main; print('UPLOADER_FLAG=%s' % main.UPLOADER_DISPATCH_ENABLED)"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, True), ("", True), ("   ", True),
        ("true", True), (" TRUE ", True), ("1", True), ("yes", True), ("on", True),
        ("false", False), ("0", False), ("no", False), (" Off ", False),
    ],
)
def test_center_uploader_dispatch_flag_is_strict_and_defaults_to_true(raw, expected):
    extra_env = {"WORKER_API_URL_MAP": ""}
    if raw is not None:
        extra_env["UPLOADER_DISPATCH_ENABLED"] = raw

    result = run_center_import(extra_env, PRINT_UPLOADER_FLAG)

    assert result.returncode == 0, result.stderr
    assert f"UPLOADER_FLAG={expected}" in result.stdout
    # 关闭时启动只提示一次；开启时不提示。
    assert result.stderr.count(UPLOADER_DISABLED_NOTICE) == (0 if expected else 1)


@pytest.mark.parametrize("raw", ["flase", "truee", "SENTINEL-maybe-a-pasted-token"])
def test_center_refuses_unrecognized_uploader_dispatch_flag_without_echoing_value(raw):
    result = run_center_import({"WORKER_API_URL_MAP": "", "UPLOADER_DISPATCH_ENABLED": raw}, PRINT_UPLOADER_FLAG)

    assert result.returncode != 0
    assert "RuntimeError" in result.stderr
    assert "UPLOADER_DISPATCH_ENABLED" in result.stderr
    assert raw not in result.stderr
    assert "UPLOADER_FLAG=" not in result.stdout
