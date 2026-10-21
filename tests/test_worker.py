from pathlib import Path
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import json
import os
import sys
import tempfile
import time

import pytest

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("NODE_TOKEN", "test-node-token-at-least-32-characters")
os.environ.setdefault("ALLOW_LEGACY_BEARER_AUTH", "true")
os.environ.setdefault("CENTER_URL", "http://127.0.0.1:8080")
os.environ.setdefault("TARGET_REMOTE_DIR", "your-target-dir")
os.environ.setdefault("WORKER_NODE_NAME", "worker-node-1")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "baicai-work"))

import worker
from fastapi.testclient import TestClient


client = TestClient(worker.app)


def test_resolve_task_path_rejects_path_traversal():
    assert worker.resolve_task_path("../etc/passwd") is None


def test_is_credential_invalid_error_matches_rclone_token_failures():
    assert worker.is_credential_invalid_error("couldn't fetch token: invalid_grant: maybe token expired?")
    assert worker.is_credential_invalid_error('try refreshing with "rclone config reconnect baicai_big@1:"')
    assert worker.is_credential_invalid_error("ERROR : token expired while listing directory")
    assert not worker.is_credential_invalid_error("Error 403: Quota exceeded")


def test_current_upload_concurrency_limit_switches_by_beijing_hour():
    night = datetime(2026, 6, 21, 1, 0, 0, tzinfo=timezone(timedelta(hours=8)))
    day = datetime(2026, 6, 21, 10, 0, 0, tzinfo=timezone(timedelta(hours=8)))

    assert worker.current_upload_concurrency_limit(night) == 3
    assert worker.current_upload_concurrency_limit(day) == 2

def test_run_uploader_reports_not_found_when_no_local_files():
    with patch("worker.detect_remote_existing_files", return_value=set()), patch(
        "worker.report_status"
    ) as mock_report_status:
        worker.run_uploader("baicai_1@1", ["missing/file.mkv"], "move")

    mock_report_status.assert_called_once_with(
        "baicai_1@1", "not_found", [], ["missing/file.mkv"]
    )


def test_run_uploader_ignores_clfstmp_files():
    with patch("worker.resolve_task_path") as mock_resolve_task_path, patch("worker.report_status") as mock_report_status:
        worker.run_uploader("baicai_1@1", ["movie/downloading.CLFSTMP"], "move")

    mock_resolve_task_path.assert_not_called()
    mock_report_status.assert_called_once_with(
        "baicai_1@1", "not_found", [], ["movie/downloading.CLFSTMP"]
    )


def test_run_uploader_ignores_clfstmp_progress_files():
    with patch("worker.resolve_task_path") as mock_resolve_task_path, patch("worker.report_status") as mock_report_status:
        worker.run_uploader("baicai_1@1", ["movie/downloading.mkv.clfstmp.progress"], "move")

    mock_resolve_task_path.assert_not_called()
    mock_report_status.assert_called_once_with(
        "baicai_1@1",
        "not_found",
        [],
        ["movie/downloading.mkv.clfstmp.progress"],
    )


def test_receive_task_rejects_duplicate_upload_account():
    payload = {"account_name": "baicai_1@1", "files": ["movie/a.mkv"], "upload_method": "copy"}

    worker.active_upload_accounts.clear()
    worker.active_upload_accounts.add("baicai_1@1")
    try:
        response = client.post(
            "/task/upload",
            json=payload,
            headers={"Authorization": "Bearer test-token"},
        )
    finally:
        worker.active_upload_accounts.clear()

    assert response.status_code == 409
    assert response.json()["detail"] == "account already uploading on this worker"


def test_receive_task_rejects_when_upload_concurrency_limit_reached():
    payload = {"account_name": "baicai_3@1", "files": ["movie/a.mkv"], "upload_method": "copy"}

    worker.active_upload_accounts.clear()
    worker.active_upload_accounts.update({"baicai_1@1", "baicai_2@1"})
    try:
        with patch("worker.current_upload_concurrency_limit", return_value=2):
            response = client.post(
                "/task/upload",
                json=payload,
                headers={"Authorization": "Bearer test-token"},
            )
    finally:
        worker.active_upload_accounts.clear()

    assert response.status_code == 429
    assert response.json()["detail"] == "worker upload concurrency limit reached"


def test_receive_task_allows_cleaner_when_upload_account_active():
    payload = {"task_type": "clean", "account_name": "baicai_1@1", "files": []}

    worker.active_upload_accounts.clear()
    worker.active_cleaner_accounts.clear()
    worker.active_upload_accounts.add("baicai_1@1")
    try:
        with patch("worker.start_detached_task") as mock_start_detached_task:
            response = client.post(
                "/task/upload",
                json=payload,
                headers={"Authorization": "Bearer test-token"},
            )
    finally:
        worker.active_upload_accounts.clear()

    assert response.status_code == 200
    mock_start_detached_task.assert_called_once()


def test_receive_task_rejects_duplicate_cleaner_account():
    payload = {"task_type": "clean", "account_name": "baicai-1@1-3", "files": []}

    worker.active_cleaner_accounts.clear()
    worker.active_cleaner_accounts.add("baicai-1@1-3")
    try:
        response = client.post(
            "/task/upload",
            json=payload,
            headers={"Authorization": "Bearer test-token"},
        )
    finally:
        worker.active_cleaner_accounts.clear()

    assert response.status_code == 409
    assert response.json()["detail"] == "cleaner already running on this worker"


def test_receive_task_rejects_when_cleaner_concurrency_limit_reached():
    payload = {"task_type": "clean", "account_name": "baicai-2@1-3", "files": []}

    worker.active_cleaner_accounts.clear()
    worker.active_cleaner_accounts.add("baicai-1@1-3")
    try:
        with patch("worker.MAX_CONCURRENT_CLEANERS_PER_WORKER", 1):
            response = client.post(
                "/task/upload",
                json=payload,
                headers={"Authorization": "Bearer test-token"},
            )
    finally:
        worker.active_cleaner_accounts.clear()

    assert response.status_code == 429
    assert response.json()["detail"] == "worker cleaner concurrency limit reached"


def test_task_router_reports_failed_on_unhandled_exception():
    payload = worker.TaskPayload(account_name="baicai_1@1", files=["movie/test.mkv"], upload_method="move")

    with patch("worker.run_uploader", side_effect=RuntimeError("boom")), patch("worker.report_status") as mock_report_status:
        worker.task_router(payload)

    mock_report_status.assert_called_once_with("baicai_1@1", "failed")


def test_task_router_clears_cleaner_account_after_finish():
    payload = worker.TaskPayload(task_type="clean", account_name="baicai-1@1-3", files=[])

    worker.active_cleaner_accounts.clear()
    worker.active_cleaner_accounts.add("baicai-1@1-3")
    with patch("worker.run_cleaner"):
        worker.task_router(payload)

    assert "baicai-1@1-3" not in worker.active_cleaner_accounts


def make_cleaner_process(lines=None, returncode=0):
    process = MagicMock()
    process.stdout = list(lines or [])
    process.returncode = returncode
    process.wait.return_value = returncode
    process.__enter__.return_value = process
    process.__exit__.return_value = False
    return process


def test_run_cleaner_moves_only_dispatch_files_and_reports_success():
    process = make_cleaner_process()
    files_from_contents = []

    def capture_popen(cmd, **_kwargs):
        files_from_path = cmd[cmd.index("--files-from-raw") + 1]
        files_from_contents.append(Path(files_from_path).read_text(encoding="utf-8"))
        return process

    with patch(
        "worker.resolve_cleaner_destination_remote",
        return_value="baicai-1@1-3,team_drive=target-team:LongEmby-B",
    ) as mock_resolve_destination, patch("worker.subprocess.Popen", side_effect=capture_popen) as mock_popen, patch(
        "worker.report_status"
    ) as mock_report_status, patch("worker.time.time", return_value=0):
        worker.run_cleaner(
            "baicai-1@1-3",
            "baicai-big2",
            ["movie/a.mkv", "/movie/a.mkv", "movie/./b.mkv", " movie/c.mkv ", "../escape.mkv"],
            "LongEmby-B",
        )

    cmd = mock_popen.call_args_list[0].args[0]
    assert cmd[:4] == [
        "rclone",
        "move",
        "baicai-1@1-3:LongEmby-B",
        "baicai-1@1-3,team_drive=target-team:LongEmby-B",
    ]
    assert files_from_contents == ["movie/a.mkv\nmovie/b.mkv\n movie/c.mkv \n"]
    assert not Path(cmd[cmd.index("--files-from-raw") + 1]).exists()
    assert "--ignore-existing" not in cmd
    assert "--drive-use-trash=false" in cmd
    assert "--files-from" not in cmd
    assert "--stats" not in cmd
    assert cmd[cmd.index("--log-level") + 1] == "ERROR"
    mock_resolve_destination.assert_called_once_with(
        "baicai-1@1-3", "baicai-big2", "LongEmby-B"
    )
    mock_report_status.assert_called_once_with("baicai-1@1-3", "clean_success")


def test_run_cleaner_reports_heartbeat_without_printing_rclone_lines():
    process = make_cleaner_process(
        ["ERROR : movie/private-name.mkv: transient failure\n", "ERROR : retrying\n"]
    )

    with patch(
        "worker.resolve_cleaner_destination_remote",
        return_value="baicai-1@1-3,team_drive=target-team:your-target-dir",
    ), patch("worker.subprocess.Popen", return_value=process), patch(
        "worker.report_heartbeat"
    ) as mock_heartbeat, patch("worker.report_status") as mock_report_status, patch(
        "worker.time.time", side_effect=[0, 301, 302]
    ), patch("builtins.print") as mock_print:
        worker.run_cleaner("baicai-1@1-3", "baicai-big", ["movie/test.mkv"])

    mock_heartbeat.assert_called_once_with("baicai-1@1-3")
    mock_print.assert_not_called()
    mock_report_status.assert_called_once_with("baicai-1@1-3", "clean_success")


def test_run_cleaner_skips_rclone_when_requested_file_list_is_empty():
    with patch("worker.subprocess.Popen") as mock_popen, patch("worker.report_status") as mock_report_status:
        worker.run_cleaner("baicai-1@1-3", "baicai-big", [])

    mock_popen.assert_not_called()
    mock_report_status.assert_called_once_with("baicai-1@1-3", "clean_success")


def test_run_cleaner_nonzero_exit_does_not_complete_dispatch_batch():
    process = make_cleaner_process(
        [f"ERROR : movie/{index}.mkv: failed\n" for index in range(5)],
        returncode=3,
    )

    with patch(
        "worker.resolve_cleaner_destination_remote",
        return_value="baicai-1@1-3,team_drive=target-team:your-target-dir",
    ), patch("worker.subprocess.Popen", return_value=process), patch(
        "worker.report_status"
    ) as mock_report_status, patch("worker.logger.error") as mock_error, patch(
        "worker.time.time", return_value=0
    ), patch("builtins.print") as mock_print:
        worker.run_cleaner("baicai-1@1-3", "baicai-big", ["movie/a.mkv"])

    mock_report_status.assert_called_once_with("baicai-1@1-3", "failed")
    mock_print.assert_not_called()
    assert "movie/0.mkv" not in mock_error.call_args.args[-1]
    assert "movie/4.mkv" in mock_error.call_args.args[-1]


def test_resolve_cleaner_destination_remote_reuses_source_credentials():
    config_result = MagicMock(
        returncode=0,
        stdout=json.dumps(
            {
                "baicai-big": {
                    "type": "drive",
                    "team_drive": "target-team-drive-id",
                    "token": "must-not-be-used-in-command",
                }
            }
        ),
    )

    with patch("worker.subprocess.run", return_value=config_result) as mock_run:
        destination = worker.resolve_cleaner_destination_remote(
            "baicai-1@1-3", "baicai-big", "LongEmby-B"
        )

    assert destination == (
        "baicai-1@1-3,team_drive=target-team-drive-id:LongEmby-B"
    )
    assert "must-not-be-used-in-command" not in destination
    assert mock_run.call_args.args[0] == ["rclone", "config", "dump"]


def test_run_uploader_stops_next_directory_after_stop_on_upload_limit_exit():
    class FakeStdout:
        def __init__(self, lines):
            self.lines = list(lines)
            self.index = 0

        def readline(self):
            if self.index >= len(self.lines):
                return ""
            line = self.lines[self.index]
            self.index += 1
            return line

    class FakeProcess:
        def __init__(self, lines, returncode=7):
            self.stdout = FakeStdout(lines)
            self.returncode = returncode
            self.killed = False

        def poll(self):
            if self.killed:
                return self.returncode
            if self.stdout.index >= len(self.stdout.lines):
                return self.returncode
            return None

        def wait(self):
            return self.returncode

        def kill(self):
            self.killed = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if self.process.stdout.index < len(self.process.stdout.lines):
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess(["NOTICE : rclone exited after stop-on-upload-limit\n"], returncode=8)

    resolved_paths = [("/dir1", "a.mkv"), ("/dir2", "b.mkv")]

    with patch("worker.ALLOWED_BASE_DIRS", ["/dir1", "/dir2"]), patch("worker.resolve_task_path", side_effect=resolved_paths), patch("worker.subprocess.Popen", return_value=process) as mock_popen, patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", side_effect=[(1, "/tmp/a.txt"), (2, "/tmp/b.txt")]), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["a.mkv", "b.mkv"], "move")

    assert mock_popen.call_count == 1
    assert mock_report_status.call_args.args[:2] == ("baicai_1@1", "403_quota")


def test_run_uploader_allows_changed_existing_files_to_be_replaced():
    class FakeStdout:
        def __init__(self):
            self._consumed = False

        def readline(self):
            if self._consumed:
                return ""
            self._consumed = True
            return ""

    class FakeProcess:
        def __init__(self):
            self.stdout = FakeStdout()
            self.returncode = 0

        def poll(self):
            return self.returncode if self.stdout._consumed else None

        def wait(self):
            return self.returncode

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if not self.process.stdout._consumed:
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess()

    with patch("worker.resolve_task_path", return_value=("/dir1", "movie/a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process) as mock_popen, patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader(
            "baicai_big2@1", ["movie/a.mkv"], "copy", remote_dir="LongEmby-B"
        )

    cmd = mock_popen.call_args.args[0]
    assert cmd[3] == "baicai_big2@1:LongEmby-B"
    assert "--ignore-existing" not in cmd
    assert "--size-only" not in cmd
    assert "--drive-stop-on-upload-limit" in cmd
    assert mock_report_status.call_args.args == ("baicai_big2@1", "completed", [], [])


def test_run_uploader_uses_configured_bwlimit():
    class FakeStdout:
        def __init__(self):
            self._consumed = False

        def readline(self):
            if self._consumed:
                return ""
            self._consumed = True
            return ""

    class FakeProcess:
        def __init__(self):
            self.stdout = FakeStdout()
            self.returncode = 0

        def poll(self):
            return self.returncode if self.stdout._consumed else None

        def wait(self):
            return self.returncode

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if not self.process.stdout._consumed:
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess()

    with patch("worker.RCLONE_BWLIMIT", "375M"), patch("worker.RCLONE_TRANSFERS", 4), patch("worker.RCLONE_CHECKERS", 8), patch("worker.resolve_task_path", return_value=("/dir1", "movie/a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process) as mock_popen, patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status"):
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["movie/a.mkv"], "copy")

    cmd = mock_popen.call_args.args[0]
    assert "--bwlimit" in cmd
    assert cmd[cmd.index("--bwlimit") + 1] == "375M"
    assert cmd[cmd.index("--transfers") + 1] == "4"
    assert cmd[cmd.index("--checkers") + 1] == "8"


def test_run_uploader_logs_skipped_files_separately():
    class FakeStdout:
        def __init__(self, lines):
            self.lines = list(lines)
            self.index = 0

        def readline(self):
            if self.index >= len(self.lines):
                return ""
            line = self.lines[self.index]
            self.index += 1
            return line

    class FakeProcess:
        def __init__(self, lines):
            self.stdout = FakeStdout(lines)
            self.returncode = 0

        def poll(self):
            return self.returncode if self.stdout.index >= len(self.stdout.lines) else None

        def wait(self):
            return self.returncode

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if self.process.stdout.index < len(self.process.stdout.lines):
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess(["INFO  : movie/a.mkv: Skipped\n"])

    with patch("worker.resolve_task_path", return_value=("/dir1", "movie/a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process), patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status, patch("worker.logger.warning") as mock_warning:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_big@1", ["movie/a.mkv"], "copy")

    assert any("远端已存在或被 rclone 跳过" in str(call.args[0]) for call in mock_warning.call_args_list)
    assert mock_report_status.call_args.args == ("baicai_big@1", "completed", ["movie/a.mkv"], [])


def test_run_uploader_switches_account_when_rclone_exits_non_zero_after_rate_limit():
    class FakeStdout:
        def __init__(self, lines):
            self.lines = list(lines)
            self.index = 0

        def readline(self):
            if self.index >= len(self.lines):
                return ""
            line = self.lines[self.index]
            self.index += 1
            return line

    class FakeProcess:
        def __init__(self, lines):
            self.stdout = FakeStdout(lines)
            self.returncode = 0
            self.killed = False

        def poll(self):
            if self.killed:
                return self.returncode
            if self.stdout.index >= len(self.stdout.lines):
                return self.returncode
            return None

        def wait(self):
            return self.returncode

        def kill(self):
            self.killed = True
            self.returncode = 9

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if self.process.stdout.index < len(self.process.stdout.lines):
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess([
        "ERROR : movie/a.mkv: Failed to copy: googleapi: Error 403: User rate limit exceeded, userRateLimitExceeded\n",
    ])
    process.returncode = 7

    with patch("worker.resolve_task_path", return_value=("/dir1", "a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process), patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["a.mkv"], "move")

    assert process.killed is False
    assert mock_report_status.call_args.args[:2] == ("baicai_1@1", "403_quota")


def test_run_uploader_uses_stop_on_upload_limit_exit_code_to_switch_account():
    class FakeStdout:
        def __init__(self, lines):
            self.lines = list(lines)
            self.index = 0

        def readline(self):
            if self.index >= len(self.lines):
                return ""
            line = self.lines[self.index]
            self.index += 1
            return line

    class FakeProcess:
        def __init__(self, lines):
            self.stdout = FakeStdout(lines)
            self.returncode = 0
            self.killed = False

        def poll(self):
            if self.killed:
                return self.returncode
            if self.stdout.index >= len(self.stdout.lines):
                return self.returncode
            return None

        def wait(self):
            return self.returncode

        def kill(self):
            self.killed = True
            self.returncode = 9

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if self.process.stdout.index < len(self.process.stdout.lines):
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess([
        "NOTICE : movie/a.mkv: some informational line before rclone exits\n",
    ])
    process.returncode = 8

    with patch("worker.resolve_task_path", return_value=("/dir1", "a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process), patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["a.mkv"], "move")

    assert process.killed is False
    assert mock_report_status.call_args.args[:2] == ("baicai_1@1", "403_quota")


def test_run_uploader_reports_non_quota_non_zero_exit_as_failed():
    class FakeStdout:
        def __init__(self, lines):
            self.lines = list(lines)
            self.index = 0

        def readline(self):
            if self.index >= len(self.lines):
                return ""
            line = self.lines[self.index]
            self.index += 1
            return line

    class FakeProcess:
        def __init__(self, lines):
            self.stdout = FakeStdout(lines)
            self.returncode = 0
            self.killed = False

        def poll(self):
            if self.killed:
                return self.returncode
            if self.stdout.index >= len(self.stdout.lines):
                return self.returncode
            return None

        def wait(self):
            return self.returncode

        def kill(self):
            self.killed = True
            self.returncode = 9

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if self.process.stdout.index < len(self.process.stdout.lines):
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess([
        "NOTICE : transient line before fatal rclone exit\n",
    ])
    process.returncode = 7

    with patch("worker.resolve_task_path", return_value=("/dir1", "a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process), patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["a.mkv"], "move")

    assert process.killed is False
    assert mock_report_status.call_args.args[:2] == ("baicai_1@1", "failed")

def test_run_uploader_stop_on_upload_limit_exit_code_salvages_successful_files():
    class FakeStdout:
        def __init__(self, lines):
            self.lines = list(lines)
            self.index = 0

        def readline(self):
            if self.index >= len(self.lines):
                return ""
            line = self.lines[self.index]
            self.index += 1
            return line

    class FakeProcess:
        def __init__(self, lines):
            self.stdout = FakeStdout(lines)
            self.returncode = 0
            self.killed = False

        def poll(self):
            if self.killed:
                return self.returncode
            if self.stdout.index >= len(self.stdout.lines):
                return self.returncode
            return None

        def wait(self):
            return self.returncode

        def kill(self):
            self.killed = True
            self.returncode = 9

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if self.process.stdout.index < len(self.process.stdout.lines):
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess([
        "INFO  : movie/a.mkv: Copied (replaced existing)\n",
        "NOTICE : rclone exited after hitting upload limit\n",
    ])
    process.returncode = 8

    with patch("worker.resolve_task_path", return_value=("/dir1", "a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process), patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["a.mkv"], "move")

    assert process.killed is False
    assert mock_report_status.call_args.args[:2] == ("baicai_1@1", "403_quota")
    assert mock_report_status.call_args.args[2] == ["movie/a.mkv"]


def test_resolve_task_path_rejects_symlink_escape():
    with tempfile.TemporaryDirectory() as tmpdir:
        allowed_dir = Path(tmpdir) / "allowed"
        outside_dir = Path(tmpdir) / "outside"
        allowed_dir.mkdir()
        outside_dir.mkdir()
        outside_file = outside_dir / "secret.txt"
        outside_file.write_text("secret", encoding="utf-8")
        (allowed_dir / "link.txt").symlink_to(outside_file)

        with patch("worker.ALLOWED_BASE_DIRS", [str(allowed_dir)]):
            assert worker.resolve_task_path("link.txt") is None


def test_report_status_raises_after_retries_exhausted():
    with patch("worker.httpx.post", side_effect=RuntimeError("offline")), patch("worker.time.sleep"):
        try:
            worker.report_status("baicai_1@1", "failed")
        except worker.ReportStatusError as exc:
            assert "战况汇报失败" in str(exc)
        else:
            raise AssertionError("expected ReportStatusError")


def test_run_uploader_reports_partial_missing_files():
    class FakeStdout:
        def __init__(self):
            self._consumed = False

        def readline(self):
            if self._consumed:
                return ""
            self._consumed = True
            return ""

    class FakeProcess:
        def __init__(self):
            self.stdout = FakeStdout()
            self.returncode = 0

        def poll(self):
            return self.returncode if self.stdout._consumed else None

        def wait(self):
            return self.returncode

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSelector:
        def __init__(self, process):
            self.process = process

        def register(self, fileobj, event):
            _ = (fileobj, event)
            return None

        def unregister(self, fileobj):
            _ = fileobj
            return None

        def select(self, timeout=1.0):
            _ = timeout
            if not self.process.stdout._consumed:
                return [(SimpleNamespace(fileobj=self.process.stdout), None)]
            return []

        def close(self):
            return None

    process = FakeProcess()

    with patch("worker.resolve_task_path", side_effect=[("/dir1", "found.mkv"), None]), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process), patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status") as mock_report_status:
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["found.mkv", "missing.mkv"], "move")

    assert mock_report_status.call_args.args[:3] == ("baicai_1@1", "completed", [])
    assert mock_report_status.call_args.args[3] == ["missing.mkv"]


def test_run_uploader_move_recovers_remote_existing_missing_files():
    with patch("worker.detect_remote_existing_files", return_value={"missing/file.mkv"}), patch("worker.report_status") as mock_report_status:
        worker.run_uploader("baicai_1@1", ["missing/file.mkv"], "move")

    mock_report_status.assert_called_once_with("baicai_1@1", "completed", ["missing/file.mkv"], [])


def test_run_uploader_reports_remote_partial_success_with_remaining_missing_files():
    files = ["movie/already-moved.mkv", "movie/still-missing.mkv"]
    with patch(
        "worker.detect_remote_existing_files",
        return_value={"movie/already-moved.mkv"},
    ), patch("worker.report_status") as mock_report_status:
        worker.run_uploader("baicai_1@1", files, "move")

    mock_report_status.assert_called_once_with(
        "baicai_1@1",
        "not_found",
        ["movie/already-moved.mkv"],
        ["movie/still-missing.mkv"],
    )


def test_run_uploader_rejects_local_file_changed_after_dispatch():
    stat_result = SimpleNamespace(st_size=201, st_mtime=2000.0)
    with patch("worker.resolve_task_path", return_value=("/data/links", "movie/a.mkv")), patch(
        "worker.os.stat", return_value=stat_result
    ), patch("worker.report_status") as mock_report_status, patch(
        "worker.subprocess.Popen"
    ) as mock_popen:
        worker.run_uploader(
            "baicai_1@1",
            ["movie/a.mkv"],
            "copy",
            ["/data/links"],
            [200],
            [2000.0],
        )

    mock_popen.assert_not_called()
    mock_report_status.assert_called_once_with(
        "baicai_1@1", "not_found", [], ["movie/a.mkv"]
    )


def test_task_payload_rejects_crlf_paths():
    with pytest.raises(ValueError):
        worker.TaskPayload(account_name="baicai_1@1", files=["movie/a.mkv\n--config=/tmp/evil"])


def test_task_payload_requires_source_dirs_to_align_with_files():
    with patch("worker.ALLOWED_BASE_DIRS", ["/data/links"]):
        with pytest.raises(ValueError):
            worker.TaskPayload(
                account_name="baicai_1@1",
                files=["a.mkv", "b.mkv"],
                source_base_dirs=["/data/links"],
            )


def test_task_payload_requires_expected_fingerprints_to_align_with_files():
    with pytest.raises(ValueError):
        worker.TaskPayload(
            account_name="baicai_1@1",
            files=["a.mkv", "b.mkv"],
            expected_sizes=[1],
        )
    with pytest.raises(ValueError):
        worker.TaskPayload(
            account_name="baicai_1@1",
            files=["a.mkv"],
            expected_mtimes=[-1.0],
        )


def test_resolve_task_path_uses_exact_source_base_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        first_root = Path(tmpdir) / "first"
        second_root = Path(tmpdir) / "second"
        first_root.mkdir()
        second_root.mkdir()
        (first_root / "same.mkv").write_text("first", encoding="utf-8")
        (second_root / "same.mkv").write_text("second", encoding="utf-8")

        with patch("worker.ALLOWED_BASE_DIRS", [str(first_root), str(second_root)]):
            assert worker.resolve_task_path("same.mkv", str(second_root)) == (
                str(second_root),
                "same.mkv",
            )


def test_report_status_includes_dispatch_worker_and_valid_hmac():
    payload = worker.TaskPayload(
        dispatch_id="dispatch-report-1",
        account_name="baicai_1@1",
        files=["a.mkv"],
    )
    response = MagicMock()
    response.raise_for_status.return_value = None
    context_token = worker.current_task_payload.set(payload)
    try:
        with patch("worker.httpx.post", return_value=response) as mock_post:
            worker.report_status("baicai_1@1", "uploading")
    finally:
        worker.current_task_payload.reset(context_token)

    raw_body = mock_post.call_args.kwargs["content"]
    headers = mock_post.call_args.kwargs["headers"]
    report = json.loads(raw_body)
    assert report["dispatch_id"] == "dispatch-report-1"
    assert report["worker_node"] == worker.WORKER_NODE_NAME
    assert "Authorization" not in headers
    assert headers["X-Baicai-Signature"] == worker.make_node_signature(
        headers["X-Baicai-Timestamp"], raw_body
    )


def test_stale_dispatch_report_cancels_local_task_and_journal():
    payload = worker.TaskPayload(
        dispatch_id="dispatch-stale-report-1",
        worker_node=worker.WORKER_NODE_NAME,
        account_name="baicai_1@1",
        files=["movie/a.mkv"],
    )
    response = MagicMock()
    response.json.return_value = {
        "status": "ignored",
        "reason": "stale_dispatch",
    }

    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ), patch("worker._post_signed_payload", return_value=response):
        worker.journal_accept_task(payload)
        context_token = worker.current_task_payload.set(payload)
        try:
            with pytest.raises(worker.LeaseLostError):
                worker.report_status("baicai_1@1", "uploading")
            assert worker.is_dispatch_cancelled(payload.dispatch_id)
            assert worker.journal_get_task(payload.dispatch_id)["state"] == "cancelled"
        finally:
            worker.clear_dispatch_cancellation(payload.dispatch_id)
            worker.current_task_payload.reset(context_token)


def test_receive_task_accepts_valid_node_hmac():
    payload = {
        "dispatch_id": "dispatch-signed-1",
        "account_name": "baicai_1@1",
        "files": ["movie/a.mkv"],
    }
    raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Baicai-Node": worker.WORKER_NODE_NAME,
        "X-Baicai-Timestamp": timestamp,
        "X-Baicai-Signature": worker.make_node_signature(timestamp, raw_body),
    }

    worker.active_upload_accounts.clear()
    try:
        with patch("worker.ALLOW_LEGACY_BEARER_AUTH", False), patch(
            "worker.journal_get_task", return_value=None
        ), patch("worker.journal_accept_task"), patch("worker.start_detached_task"):
            response = client.post("/task/upload", content=raw_body, headers=headers)
    finally:
        worker.active_upload_accounts.clear()

    assert response.status_code == 200
    assert response.json()["dispatch_id"] == "dispatch-signed-1"


def test_receive_task_rejects_expired_node_hmac():
    raw_body = b'{}'
    timestamp = str(int(time.time()) - worker.NODE_SIGNATURE_MAX_AGE_SECONDS - 1)
    headers = {
        "Content-Type": "application/json",
        "X-Baicai-Node": worker.WORKER_NODE_NAME,
        "X-Baicai-Timestamp": timestamp,
        "X-Baicai-Signature": worker.make_node_signature(timestamp, raw_body),
    }

    with patch("worker.ALLOW_LEGACY_BEARER_AUTH", False):
        response = client.post("/task/upload", content=raw_body, headers=headers)

    assert response.status_code == 401


@pytest.mark.parametrize(
    "header_name, header_value",
    [
        ("X-Baicai-Node", "wörker-node-1"),
        ("X-Baicai-Signature", "é" * 64),
    ],
)
def test_receive_task_rejects_non_ascii_hmac_headers_with_401(header_name, header_value):
    raw_body = b'{"account_name":"baicai_1@1","files":[]}'
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Baicai-Node": worker.WORKER_NODE_NAME,
        "X-Baicai-Timestamp": timestamp,
        "X-Baicai-Signature": worker.make_node_signature(timestamp, raw_body),
    }
    headers[header_name] = header_value.encode("utf-8")

    with patch("worker.ALLOW_LEGACY_BEARER_AUTH", False):
        response = client.post("/task/upload", content=raw_body, headers=headers)

    assert response.status_code == 401


def test_receive_task_rejects_non_ascii_legacy_bearer_with_401():
    with patch("worker.ALLOW_LEGACY_BEARER_AUTH", True):
        response = client.post(
            "/task/upload",
            json={"account_name": "baicai_1@1", "files": []},
            headers={"Authorization": "Bearer tést-token".encode("utf-8")},
        )

    assert response.status_code == 401


DANGEROUS_REMOTE_NAMES = [
    "--config=/tmp/attacker.conf",
    "-vv",
    "baicai_1@1:other-dir",
    ":local:",
    "baicai_1@1/../etc",
    "baicai_1@1,team_drive=attacker",
    "baicai_1@1\nother",
    "baicai_1@1\x00",
    "baicai_1@1\t",
    "baicai 1@1",
    "..",
    "baicai\\1@1",
]


@pytest.mark.parametrize("account_name", ["baicai_1@1", "baicai-1@1-3", "baicai_big@2", "baicai_big2@10"])
def test_task_payload_accepts_existing_account_names(account_name):
    payload = worker.TaskPayload(account_name=account_name, files=[])

    assert payload.account_name == account_name


@pytest.mark.parametrize("account_name", DANGEROUS_REMOTE_NAMES)
def test_task_payload_rejects_dangerous_account_name(account_name):
    with pytest.raises(ValueError):
        worker.TaskPayload(account_name=account_name, files=[])


@pytest.mark.parametrize("dest_name", ["baicai-big2", "baicai-big", "baicai_big2@1"])
def test_task_payload_accepts_existing_dest_names(dest_name):
    payload = worker.TaskPayload(task_type="clean", account_name="baicai-1@1-3", dest_name=dest_name, files=[])

    assert payload.dest_name == dest_name


@pytest.mark.parametrize("dest_name", DANGEROUS_REMOTE_NAMES)
def test_task_payload_rejects_dangerous_dest_name(dest_name):
    with pytest.raises(ValueError):
        worker.TaskPayload(task_type="clean", account_name="baicai-1@1-3", dest_name=dest_name, files=[])


def test_receive_task_rejects_option_like_account_name_before_start():
    with patch("worker.start_detached_task") as mock_start:
        response = client.post(
            "/task/upload",
            json={"account_name": "--config=/tmp/attacker.conf", "files": ["movie/a.mkv"]},
            headers={"Authorization": f"Bearer {worker.API_TOKEN}"},
        )

    assert response.status_code == 422
    mock_start.assert_not_called()


@pytest.mark.parametrize("authorization", ["Bearer ", "Bearer x"])
def test_legacy_bearer_cannot_authenticate_with_empty_api_token(authorization):
    with patch("worker.ALLOW_LEGACY_BEARER_AUTH", True), patch("worker.API_TOKEN", ""), patch(
        "worker.start_detached_task"
    ) as mock_start:
        response = client.post(
            "/task/upload",
            json={"account_name": "baicai_1@1", "files": []},
            headers={"Authorization": authorization},
        )

    assert response.status_code == 401
    mock_start.assert_not_called()


def test_task_payload_rejects_non_ascii_worker_node_as_validation_error():
    with pytest.raises(ValueError):
        worker.TaskPayload(account_name="baicai_1@1", worker_node="wörker-node-1", files=[])


def test_receive_task_is_idempotent_by_dispatch_id():
    payload = {
        "dispatch_id": "dispatch-idempotent-1",
        "account_name": "baicai_1@1",
        "files": ["movie/a.mkv"],
    }
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ), patch("worker.start_detached_task") as mock_start:
        worker.active_upload_accounts.clear()
        try:
            first = client.post(
                "/task/upload",
                json=payload,
                headers={"Authorization": "Bearer test-token"},
            )
            second = client.post(
                "/task/upload",
                json=payload,
                headers={"Authorization": "Bearer test-token"},
            )
        finally:
            worker.active_upload_accounts.clear()

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["idempotent"] is True
    assert mock_start.call_count == 1


def test_final_report_is_retained_and_retried_from_journal():
    payload = worker.TaskPayload(
        dispatch_id="dispatch-retry-1",
        account_name="baicai_1@1",
        files=["a.mkv"],
    )
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ):
        worker.journal_accept_task(payload)
        context_token = worker.current_task_payload.set(payload)
        try:
            with patch("worker._post_report_payload", side_effect=RuntimeError("offline")), patch(
                "worker.time.sleep"
            ):
                with pytest.raises(worker.ReportStatusError):
                    worker.report_status("baicai_1@1", "completed", ["a.mkv"])
        finally:
            worker.current_task_payload.reset(context_token)

        assert worker.journal_get_task("dispatch-retry-1")["state"] == "pending_report"
        with patch("worker._post_report_payload") as mock_post:
            worker.retry_pending_final_reports()
        assert mock_post.call_count == 1
        assert worker.journal_get_task("dispatch-retry-1")["state"] == "completed"


def test_recover_unfinished_tasks_restarts_journal_task():
    payload = worker.TaskPayload(
        dispatch_id="dispatch-recover-1",
        account_name="baicai_1@1",
        files=["a.mkv"],
    )
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ):
        worker.journal_accept_task(payload)
        worker.journal_set_state(payload.dispatch_id, "running")
        worker.active_upload_accounts.clear()
        try:
            with patch("worker.validate_recovery_lease", return_value=True), patch(
                "worker.start_detached_task"
            ) as mock_start:
                worker.recover_unfinished_tasks()
        finally:
            worker.active_upload_accounts.clear()

    mock_start.assert_called_once()
    assert mock_start.call_args.args[0].dispatch_id == "dispatch-recover-1"


def test_recover_unfinished_tasks_cancels_stale_dispatch():
    payload = worker.TaskPayload(
        dispatch_id="dispatch-stale-1",
        worker_node=worker.WORKER_NODE_NAME,
        account_name="baicai_1@1",
        files=["a.mkv"],
    )
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ), patch("worker.validate_recovery_lease", return_value=False), patch(
        "worker.start_detached_task"
    ) as mock_start:
        worker.journal_accept_task(payload)
        worker.journal_set_state(payload.dispatch_id, "running")
        worker.recover_unfinished_tasks()

        assert worker.journal_get_task(payload.dispatch_id)["state"] == "cancelled"
        mock_start.assert_not_called()


def test_recovery_resumes_all_leased_tasks_despite_current_admission_limit():
    payloads = [
        worker.TaskPayload(
            dispatch_id=f"dispatch-recover-limit-{index}",
            worker_node=worker.WORKER_NODE_NAME,
            account_name=f"baicai_{index}@1",
        )
        for index in range(1, 4)
    ]
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ), patch("worker.validate_recovery_lease", return_value=True), patch(
        "worker.current_upload_concurrency_limit", return_value=1
    ), patch("worker.start_detached_task") as mock_start:
        worker.active_upload_accounts.clear()
        try:
            for payload in payloads:
                worker.journal_accept_task(payload)
                worker.journal_set_state(payload.dispatch_id, "running")
            worker.recover_unfinished_tasks()
        finally:
            worker.active_upload_accounts.clear()

    assert mock_start.call_count == 3


def test_journal_prunes_only_old_terminal_tasks():
    old_terminal = worker.TaskPayload(
        dispatch_id="dispatch-old-terminal-1",
        worker_node=worker.WORKER_NODE_NAME,
        account_name="baicai_1@1",
    )
    old_running = worker.TaskPayload(
        dispatch_id="dispatch-old-running-1",
        worker_node=worker.WORKER_NODE_NAME,
        account_name="baicai_2@1",
    )
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "worker.TASK_JOURNAL_DB_PATH", str(Path(tmpdir) / "tasks.db")
    ), patch("worker.TASK_JOURNAL_RETENTION_DAYS", 30):
        worker.journal_accept_task(old_terminal)
        worker.journal_mark_completed(old_terminal.dispatch_id)
        worker.journal_accept_task(old_running)
        worker.journal_set_state(old_running.dispatch_id, "running")
        with worker.journal_lock, worker._journal_connection() as connection:
            connection.execute(
                "UPDATE task_journal SET updated_at=?",
                (time.time() - 31 * 86400,),
            )

        assert worker.journal_prune_terminal_tasks() == 1
        assert worker.journal_get_task(old_terminal.dispatch_id) is None
        assert worker.journal_get_task(old_running.dispatch_id)["state"] == "running"


def test_remote_probe_timeout_is_not_treated_as_existing():
    with patch(
        "worker.subprocess.run",
        side_effect=worker.subprocess.TimeoutExpired(cmd="rclone", timeout=1),
    ):
        assert worker.detect_remote_existing_files("baicai_1@1", ["a.mkv"]) == set()


def test_remote_probe_preserves_path_whitespace():
    result = MagicMock(returncode=0, stdout=" movie.mkv \n", stderr="")
    with patch("worker.subprocess.run", return_value=result):
        existing = worker.detect_remote_existing_files("baicai_1@1", [" movie.mkv "])

    assert existing == {" movie.mkv "}


def test_remote_probe_requires_matching_expected_fingerprint():
    remote_items = [
        {
            "Path": "movie/matching.mkv",
            "Size": 200,
            "ModTime": "1970-01-01T00:16:40Z",
        },
        {
            "Path": "movie/old-version.mkv",
            "Size": 199,
            "ModTime": "1970-01-01T00:16:40Z",
        },
    ]
    result = MagicMock(returncode=0, stdout=json.dumps(remote_items), stderr="")
    expected_sizes = {
        "movie/matching.mkv": 200,
        "movie/old-version.mkv": 200,
    }
    expected_mtimes = {
        "movie/matching.mkv": 1000.0,
        "movie/old-version.mkv": 1000.0,
    }

    with patch("worker.subprocess.run", return_value=result) as mock_run:
        existing = worker.detect_remote_existing_files(
            "baicai_1@1",
            list(expected_sizes),
            expected_sizes,
            expected_mtimes,
        )

    assert existing == {"movie/matching.mkv"}
    assert mock_run.call_args.args[0][1] == "lsjson"
