from pathlib import Path
from unittest.mock import patch
import os
import shutil
import sys
import tempfile

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("NODE_TOKEN", "test-node-token-at-least-32-characters")
os.environ.setdefault("ALLOW_LEGACY_BEARER_AUTH", "true")
os.environ.setdefault("CENTER_URL", "http://127.0.0.1:8080")
os.environ.setdefault("TARGET_REMOTE_DIR", "your-target-dir")
os.environ.setdefault("WORKER_NODE_NAME", "worker-node-1")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import worker

client = TestClient(worker.app)

# Worker 按 PATH 调用 rclone；这些用例只访问本地临时目录，但需要真实可执行文件。
requires_rclone = pytest.mark.skipif(
    shutil.which("rclone") is None,
    reason="PATH 中未找到 rclone：按 https://rclone.org/install/ 安装与生产一致的 1.65 及以上版本"
    "（发行版 apt 中的 1.60 不支持用例里带 @ 的 remote 名）并加入 PATH 后重新运行",
)


def auth_headers():
    return {"Authorization": f"Bearer {worker.API_TOKEN}"}


def write_rclone_config(config_path: Path, uploader_root: Path, dest_root: Path) -> None:
    config_path.write_text(
        "\n".join(
            [
                "[baicai_1@1]",
                "type = alias",
                f"remote = :local:{uploader_root}",
                "",
                "[baicai-1@1-3]",
                "type = alias",
                f"remote = :local:{uploader_root}",
                "",
                "[baicai-big]",
                "type = alias",
                f"remote = :local:{dest_root}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_receive_task_upload_routes_to_background_worker():
    payload = {
        "task_type": "upload",
        "upload_method": "move",
        "account_name": "baicai_1@1",
        "remote_dir": "LongEmby-B",
        "files": ["movie/test.mkv"],
    }

    with patch("worker.run_uploader") as mock_run_uploader:
        response = client.post("/task/upload", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json()["status"] == "Accepted"
    assert response.json()["mode"] == "move"
    assert response.json()["dispatch_id"].startswith("legacy-")
    mock_run_uploader.assert_called_once_with(
        "baicai_1@1", ["movie/test.mkv"], "move", None, None, None, "LongEmby-B"
    )


def test_receive_task_clean_routes_to_background_worker():
    payload = {
        "task_type": "clean",
        "upload_method": "move",
        "account_name": "baicai-1@1-3",
        "dest_name": "baicai-big2",
        "remote_dir": "LongEmby-B",
        "files": ["movie/test.mkv"],
    }

    with patch("worker.run_cleaner") as mock_run_cleaner:
        response = client.post("/task/upload", json=payload, headers=auth_headers())

    assert response.status_code == 200
    mock_run_cleaner.assert_called_once_with(
        "baicai-1@1-3", "baicai-big2", ["movie/test.mkv"], "LongEmby-B"
    )


@requires_rclone
def test_worker_rclone_upload_and_clean_round_trip():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        source_dir = tmp_path / "source"
        uploader_root = tmp_path / "uploader"
        dest_root = tmp_path / "dest"
        config_path = tmp_path / "rclone.conf"
        source_file = source_dir / "movie" / "sample.txt"
        source_file.parent.mkdir(parents=True, exist_ok=True)
        uploader_root.mkdir()
        dest_root.mkdir()
        source_file.write_text("hello worker", encoding="utf-8")
        write_rclone_config(config_path, uploader_root, dest_root)

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch.object(worker, "ALLOWED_BASE_DIRS", [str(source_dir)]):
            upload_reports = []

            def capture_upload(account, status, successful_files=None, missing_files=None):
                upload_reports.append((account, status, successful_files or [], missing_files or []))

            with patch("worker.report_status", side_effect=capture_upload):
                worker.run_uploader("baicai_1@1", ["movie/sample.txt"], "copy")

            staged_file = uploader_root / worker.TARGET_REMOTE_DIR / "movie" / "sample.txt"
            assert staged_file.exists()
            assert upload_reports[-1] == ("baicai_1@1", "completed", ["movie/sample.txt"], [])

            clean_reports = []

            def capture_clean(account, status, successful_files=None, missing_files=None):
                clean_reports.append((account, status, successful_files or [], missing_files or []))

            with patch(
                "worker.resolve_cleaner_destination_remote",
                return_value=f"baicai-big:{worker.TARGET_REMOTE_DIR}",
            ), patch("worker.report_status", side_effect=capture_clean):
                worker.run_cleaner("baicai-1@1-3", "baicai-big", ["movie/sample.txt"])

            dest_file = dest_root / worker.TARGET_REMOTE_DIR / "movie" / "sample.txt"
            assert dest_file.exists()
            assert not staged_file.exists()
            assert clean_reports[-1][:2] == ("baicai-1@1-3", "clean_success")


@requires_rclone
def test_worker_rclone_clean_moves_only_requested_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        uploader_root = tmp_path / "uploader"
        dest_root = tmp_path / "dest"
        config_path = tmp_path / "rclone.conf"
        staging_dir = uploader_root / worker.TARGET_REMOTE_DIR
        dest_dir = dest_root / worker.TARGET_REMOTE_DIR
        requested = ["movie/requested.mkv", "/电影/带 空格.mkv"]
        unlisted = ["movie/unlisted-same-dir.mkv", "other/unlisted.mkv", "movie/#comment-like.mkv"]
        for relative_path in ["movie/requested.mkv", "电影/带 空格.mkv", *unlisted]:
            staged = staging_dir / relative_path
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_text(relative_path, encoding="utf-8")
        dest_root.mkdir()
        write_rclone_config(config_path, uploader_root, dest_root)

        clean_reports = []

        def capture_clean(account, status, successful_files=None, missing_files=None):
            clean_reports.append((account, status))

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch(
            "worker.resolve_cleaner_destination_remote",
            return_value=f"baicai-big:{worker.TARGET_REMOTE_DIR}",
        ), patch("worker.report_status", side_effect=capture_clean):
            worker.run_cleaner(
                "baicai-1@1-3",
                "baicai-big",
                [*requested, "movie/already-moved.mkv", "../escape.mkv"],
            )

        assert clean_reports == [("baicai-1@1-3", "clean_success")]
        for relative_path in ["movie/requested.mkv", "电影/带 空格.mkv"]:
            assert (dest_dir / relative_path).exists()
            assert not (staging_dir / relative_path).exists()
        for relative_path in unlisted:
            assert (staging_dir / relative_path).exists()
            assert not (dest_dir / relative_path).exists()
