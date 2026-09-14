"""验证公开维护工具的目标选择和账号状态保留规则，不连接实际数据库或网盘。"""

from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest

from scripts import cleanup_drive_clfstmp as cleanup
from scripts.generate_drive_accounts_sql import render_sql


def test_generated_account_import_preserves_existing_locks_and_activation():
    sql = render_sql(9, 3, 3, "example-final-remote")
    update_clause = sql.split("ON DUPLICATE KEY UPDATE", 1)[1]
    assert "role = VALUES(role)" in update_clause
    assert "cleaner_mapping = VALUES(cleaner_mapping)" in update_clause
    # 不包含任何重置账号启停、调度租约或配额的 SQL 赋值。
    for assignment in ("is_active=", "is_active =", "quota_blocked_until =", "dispatch_locked_until ="):
        assert assignment not in update_clause
    assert "UPDATE drive_accounts" not in sql


def test_cleanup_requires_remote_before_any_network_call():
    with patch.object(cleanup, "REMOTES", []), patch.object(cleanup, "refresh_and_load_clients") as refresh:
        with pytest.raises(ValueError, match="--remote"):
            cleanup.main()
        refresh.assert_not_called()


def test_cleanup_refresh_uses_selected_config():
    with patch.object(cleanup, "CONFIG_PATH", "/tmp/example-rclone.conf"), patch.object(cleanup.subprocess, "run") as run:
        cleanup.refresh_remote("example-remote")
    assert run.call_args.args[0][:5] == [
        "rclone", "--config", "/tmp/example-rclone.conf", "about", "example-remote:"
    ]


def test_cleanup_preview_does_not_delete_candidates():
    clients = [{"remote": "example-remote", "drive_id": "example-drive", "token": "test-token"}]
    candidates = [{"id": "example-file", "name": "sample.clfstmp", "modifiedTime": "2000-01-01T00:00:00Z"}]
    with patch.object(cleanup, "REMOTES", ["example-remote"]), \
         patch.object(cleanup, "refresh_and_load_clients", return_value=clients), \
         patch.object(cleanup, "list_batch", return_value=(candidates, 0)), \
         patch.object(cleanup, "delete_file") as delete_one, \
         patch.object(cleanup, "delete_files_batch") as delete_batch:
        assert cleanup.main() == 0
        delete_one.assert_not_called()
        delete_batch.assert_not_called()


def test_comparison_cli_requires_explicit_worker_before_reading_config():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(root / "scripts/compare_worker_cache_paths.py"), "--local-list", "unused.txt"],
        capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert "--worker-node" in result.stderr
