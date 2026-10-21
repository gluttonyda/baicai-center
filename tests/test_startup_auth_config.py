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


def run_center_import(extra_env: dict[str, str]) -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"WORKER_API_URL_MAP", "REQUIRE_WORKER_API_URL_MAP"}
    }
    env.update({**CENTER_ENV, "PYTHONIOENCODING": "utf-8", **extra_env})
    return subprocess.run(
        [sys.executable, "-c", "import main"],
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
    assert "地址非法" in result.stderr
    assert "worker-node-1" in result.stderr
