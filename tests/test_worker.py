from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import os
import sys
import tempfile
import time

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("CENTER_URL", "http://127.0.0.1:8080")
os.environ.setdefault("TARGET_REMOTE_DIR", "your-target-dir")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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

def test_run_uploader_reports_not_found_when_no_local_files():
    with patch("worker.report_status") as mock_report_status:
        worker.run_uploader("baicai_1@1", ["missing/file.mkv"], "move")

    mock_report_status.assert_called_once_with("baicai_1@1", "not_found", missing_files=["missing/file.mkv"])


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
        with patch("worker.MAX_CONCURRENT_UPLOADS_PER_WORKER", 2):
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


def test_run_cleaner_reports_uploading_heartbeat_for_long_task():
    process = MagicMock()
    process.stdout = ["INFO  : movie/test.mkv: Moved (server-side copy)\n", "Transferred: 1 / 1, 100%\n"]
    process.returncode = 0
    process.wait.return_value = 0
    process.__enter__.return_value = process
    process.__exit__.return_value = False

    with patch("worker.subprocess.Popen", return_value=process), patch("worker.report_status") as mock_report_status, patch("worker.time.time", side_effect=[0, 301, 302]):
        worker.run_cleaner("baicai-1@1-3", "baicai-big", ["movie/test.mkv"])

    assert any(call.args[:2] == ("baicai-1@1-3", "uploading") for call in mock_report_status.call_args_list)
    assert mock_report_status.call_args_list[-1].args[:2] == ("baicai-1@1-3", "clean_success")
    assert mock_report_status.call_args_list[-1].args[2] == ["movie/test.mkv"]


def test_run_cleaner_batches_actual_cleaned_files_and_ignores_skipped():
    process = MagicMock()
    process.stdout = [
        "INFO  : movie/a.mkv: Moved (server-side copy)\n",
        "INFO  : movie/b.mkv: Copied (server-side copy)\n",
        "INFO  : movie/c.mkv: Skipped\n",
        "INFO  : movie/a.mkv: Deleted\n",
        "INFO  : movie/d.mkv: Deleted\n",
    ]
    process.returncode = 0
    process.wait.return_value = 0
    process.__enter__.return_value = process
    process.__exit__.return_value = False

    with patch("worker.CLEANER_REPORT_BATCH_SIZE", 2), patch("worker.subprocess.Popen", return_value=process), patch("worker.report_status") as mock_report_status, patch("worker.time.time", return_value=0):
        worker.run_cleaner("baicai-1@1-3", "baicai-big", ["movie/requested-only.mkv"])

    assert mock_report_status.call_args_list[0].args == ("baicai-1@1-3", "uploading", ["movie/a.mkv", "movie/b.mkv"])
    assert mock_report_status.call_args_list[-1].args == ("baicai-1@1-3", "clean_success", ["movie/d.mkv"])


def test_run_cleaner_does_not_ignore_existing_destination_files():
    process = MagicMock()
    process.stdout = []
    process.returncode = 0
    process.wait.return_value = 0
    process.__enter__.return_value = process
    process.__exit__.return_value = False

    with patch("worker.subprocess.Popen", return_value=process) as mock_popen, patch("worker.report_status"), patch("worker.time.time", return_value=0):
        worker.run_cleaner("baicai-1@1-3", "baicai-big", ["movie/a.mkv"])

    cmd = mock_popen.call_args.args[0]
    assert "--ignore-existing" not in cmd
    assert "--drive-use-trash=false" in cmd


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


def test_run_uploader_direct_account_uses_ignore_existing():
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
        worker.run_uploader("baicai_big@1", ["movie/a.mkv"], "copy")

    cmd = mock_popen.call_args.args[0]
    assert "--ignore-existing" in cmd
    assert "--drive-stop-on-upload-limit" in cmd
    assert mock_report_status.call_args.args == ("baicai_big@1", "completed", [], [])


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

    with patch("worker.RCLONE_BWLIMIT", "375M"), patch("worker.resolve_task_path", return_value=("/dir1", "movie/a.mkv")), patch("worker.ALLOWED_BASE_DIRS", ["/dir1"]), patch("worker.subprocess.Popen", return_value=process) as mock_popen, patch("worker.selectors.DefaultSelector", return_value=FakeSelector(process)), patch("worker.tempfile.mkstemp", return_value=(1, "/tmp/a.txt")), patch("worker.os.fdopen") as mock_fdopen, patch("worker.os.path.exists", return_value=True), patch("worker.os.remove"), patch("worker.report_status"):
        mock_fdopen.return_value.__enter__.return_value = MagicMock()
        worker.run_uploader("baicai_1@1", ["movie/a.mkv"], "copy")

    cmd = mock_popen.call_args.args[0]
    assert "--bwlimit" in cmd
    assert cmd[cmd.index("--bwlimit") + 1] == "375M"


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


def test_run_uploader_switches_account_for_any_non_zero_exit_code():
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
    assert mock_report_status.call_args.args[:2] == ("baicai_1@1", "403_quota")

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
        "INFO  : movie/a.mkv: Copied (new)\n",
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
