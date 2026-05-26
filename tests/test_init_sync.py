from pathlib import Path
from unittest.mock import MagicMock, patch
import io
import os
import sys
import tempfile

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("CENTER_API", "http://127.0.0.1:8080/api/report_files")
os.environ.setdefault("WORKER_NODE_NAME", "Init_Scanner")
os.environ.setdefault("WORKER_API_URL", "http://127.0.0.1:8001/task/upload")
os.environ.setdefault("RCLONE_REMOTE_ACCOUNT", "baicai-big")
os.environ.setdefault("TARGET_DIR", "your-target-dir")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import init_sync


class DummyProcess:
    def __init__(self, lines, returncode=0, stderr_text=""):
        self.stdout = io.StringIO(lines)
        self.stderr = io.StringIO(stderr_text)
        self.returncode = returncode

    def wait(self):
        return self.returncode


def test_submit_history_chunk_success():
    client = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None
    client.post.return_value = response

    ok = init_sync.submit_history_chunk(client, ["movie/a.mkv"], 1)

    assert ok is True


def test_submit_history_chunk_persists_failed_batch():
    client = MagicMock()
    response = MagicMock()
    response.raise_for_status.side_effect = Exception("boom")
    client.post.return_value = response

    with tempfile.TemporaryDirectory() as tmpdir, patch("init_sync.FAILED_CHUNKS_DIR", tmpdir):
        ok = init_sync.submit_history_chunk(client, ["movie/a.mkv"], 7)
        failed_file = Path(tmpdir) / "batch_00007.txt"

        assert ok is False
        assert failed_file.exists()
        assert failed_file.read_text(encoding="utf-8").strip() == "movie/a.mkv"


def test_sync_remote_snapshot_rejects_non_init_scanner_name():
    with patch("init_sync.WORKER_NODE_NAME", "worker-node-1"):
        try:
            init_sync.sync_remote_snapshot()
        except RuntimeError as exc:
            assert "Init_Scanner" in str(exc)
        else:
            raise AssertionError("预期应拒绝真实 Worker 节点名")


def test_sync_remote_snapshot_streams_in_chunks():
    dummy_process = DummyProcess("movie/a.mkv\nmovie/b.mkv\nmovie/c.mkv\n", returncode=0)

    with patch("init_sync.WORKER_NODE_NAME", "Init_Scanner"), patch("init_sync.CHUNK_SIZE", 2), patch("init_sync.subprocess.Popen", return_value=dummy_process), patch("init_sync.submit_history_chunk", side_effect=[True, True]) as mock_submit:
        success, total_files, success_batches, failed_batches = init_sync.sync_remote_snapshot()

    assert success is True
    assert total_files == 3
    assert success_batches == 2
    assert failed_batches == 0
    assert mock_submit.call_count == 2


def test_sync_remote_snapshot_returns_false_on_rclone_failure():
    dummy_process = DummyProcess("", returncode=1, stderr_text="boom")

    with patch("init_sync.WORKER_NODE_NAME", "Init_Scanner"), patch("init_sync.subprocess.Popen", return_value=dummy_process):
        success, total_files, success_batches, failed_batches = init_sync.sync_remote_snapshot()

    assert success is False
    assert total_files == 0
    assert success_batches == 0
    assert failed_batches == 0
