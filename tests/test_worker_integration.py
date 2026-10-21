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

            def capture_upload(account, status, successful_files=None, missing_files=None, already_present_files=None):
                upload_reports.append((account, status, successful_files or [], missing_files or []))

            with patch("worker.report_status", side_effect=capture_upload):
                worker.run_uploader("baicai_1@1", ["movie/sample.txt"], "copy")

            staged_file = uploader_root / worker.TARGET_REMOTE_DIR / "movie" / "sample.txt"
            assert staged_file.exists()
            assert upload_reports[-1] == ("baicai_1@1", "completed", ["movie/sample.txt"], [])

            clean_reports = []

            def capture_clean(account, status, successful_files=None, missing_files=None, already_present_files=None):
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

        def capture_clean(account, status, successful_files=None, missing_files=None, already_present_files=None):
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


# `--files-from` 会把 #/; 开头的行当注释、裁剪行首尾空白（含全角空格）；带 ": Copied" 的名字用于检查日志解析不会截断路径。
SPECIAL_UPLOAD_FILES = [
    "normal.txt",
    "#hash.txt",
    ";semi.txt",
    " spaced.txt ",
    "电影/中文 文件.mkv",
    "电影/全角空格结尾.mkv　",
    "movie/name: Copied tricky.txt",
    # --files-from-raw 不做 glob，这些按字面搬运（与旧 --files-from 的 AddFile 行为一致）。
    "glob*star.txt",
    "br[ack]et.txt",
]
# 同目录下未列入任务的文件；其中两个恰好是带空白的任务名被裁剪后的名字。
UNLISTED_UPLOAD_FILES = [
    "unlisted.txt",
    "spaced.txt",
    "#unlisted.txt",
    "电影/未列出.mkv",
    "电影/全角空格结尾.mkv",
]


@requires_rclone
@pytest.mark.parametrize("method", ["copy", "move"])
def test_worker_rclone_uploader_transfers_special_filenames(method):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        source_dir = tmp_path / "source"
        uploader_root = tmp_path / "uploader"
        dest_root = tmp_path / "dest"
        config_path = tmp_path / "rclone.conf"
        staging_dir = uploader_root / worker.TARGET_REMOTE_DIR
        for relative_path in [*SPECIAL_UPLOAD_FILES, *UNLISTED_UPLOAD_FILES]:
            source_file = source_dir / relative_path
            source_file.parent.mkdir(parents=True, exist_ok=True)
            source_file.write_text(f"content of {relative_path!r}", encoding="utf-8")
        uploader_root.mkdir()
        dest_root.mkdir()
        write_rclone_config(config_path, uploader_root, dest_root)

        upload_reports = []

        def capture_upload(account, status, successful_files=None, missing_files=None, already_present_files=None):
            upload_reports.append((account, status, list(successful_files or []), list(missing_files or [])))

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch.object(
            worker, "ALLOWED_BASE_DIRS", [str(source_dir)]
        ), patch("worker.report_status", side_effect=capture_upload):
            worker.run_uploader("baicai_1@1", list(SPECIAL_UPLOAD_FILES), method)

        account, status, successful_files, missing_files = upload_reports[-1]
        assert (account, status, missing_files) == ("baicai_1@1", "completed", [])
        assert sorted(successful_files) == sorted(SPECIAL_UPLOAD_FILES)
        for relative_path in SPECIAL_UPLOAD_FILES:
            staged = staging_dir / relative_path
            assert staged.exists(), relative_path
            assert staged.read_text(encoding="utf-8") == f"content of {relative_path!r}"
            assert (source_dir / relative_path).exists() is (method == "copy"), relative_path
        for relative_path in UNLISTED_UPLOAD_FILES:
            assert (source_dir / relative_path).exists(), relative_path
            assert not (staging_dir / relative_path).exists(), relative_path


@requires_rclone
@pytest.mark.parametrize("with_fingerprint", [False, True], ids=["lsf", "lsjson"])
def test_worker_rclone_move_recovers_special_filenames_already_on_remote(with_fingerprint):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        source_dir = tmp_path / "source"
        uploader_root = tmp_path / "uploader"
        dest_root = tmp_path / "dest"
        config_path = tmp_path / "rclone.conf"
        staging_dir = uploader_root / worker.TARGET_REMOTE_DIR
        remote_mtime = 1_700_000_000
        # 本地已被上一轮 move 搬走，只在远端存在；未列入任务的远端文件不能被当作补偿成功。
        for relative_path in [*SPECIAL_UPLOAD_FILES, *UNLISTED_UPLOAD_FILES]:
            staged = staging_dir / relative_path
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_text(f"content of {relative_path!r}", encoding="utf-8")
            os.utime(staged, (remote_mtime, remote_mtime))
        source_dir.mkdir()
        dest_root.mkdir()
        write_rclone_config(config_path, uploader_root, dest_root)
        expected_sizes = [(staging_dir / path).stat().st_size for path in SPECIAL_UPLOAD_FILES]
        expected_mtimes = [float(remote_mtime)] * len(SPECIAL_UPLOAD_FILES)

        upload_reports = []

        def capture_upload(account, status, successful_files=None, missing_files=None, already_present_files=None):
            upload_reports.append((account, status, list(successful_files or []), list(missing_files or [])))

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch.object(
            worker, "ALLOWED_BASE_DIRS", [str(source_dir)]
        ), patch("worker.report_status", side_effect=capture_upload):
            worker.run_uploader(
                "baicai_1@1",
                list(SPECIAL_UPLOAD_FILES),
                "move",
                None,
                expected_sizes if with_fingerprint else None,
                expected_mtimes if with_fingerprint else None,
            )

        account, status, successful_files, missing_files = upload_reports[-1]
        assert (account, status, missing_files) == ("baicai_1@1", "completed", [])
        assert sorted(successful_files) == sorted(SPECIAL_UPLOAD_FILES)
        for relative_path in [*SPECIAL_UPLOAD_FILES, *UNLISTED_UPLOAD_FILES]:
            assert (staging_dir / relative_path).exists(), relative_path


@requires_rclone
def test_worker_rclone_marks_already_present_file_as_success():
    """Issue #15：远端已存在且指纹一致的文件必须在本次回执里结算成功。

    rclone 在 INFO 级别对这类文件不输出任何逐文件行，旧实现下它不会进入 successful_files，
    Center 会每 UPLOADING_STALE_MINUTES 重派一次。
    """
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

        staged_file = uploader_root / worker.TARGET_REMOTE_DIR / "movie" / "sample.txt"
        staged_file.parent.mkdir(parents=True)
        shutil.copy2(source_file, staged_file)  # 同内容、同 mtime
        staged_mtime = staged_file.stat().st_mtime_ns

        write_rclone_config(config_path, uploader_root, dest_root)

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch.object(worker, "ALLOWED_BASE_DIRS", [str(source_dir)]):
            reports = []

            def capture(account, status, successful_files=None, missing_files=None, already_present_files=None):
                reports.append(
                    (account, status, successful_files or [], missing_files or [], already_present_files or [])
                )

            with patch("worker.report_status", side_effect=capture):
                worker.run_uploader("baicai_1@1", ["movie/sample.txt"], "copy")

            assert reports[-1] == (
                "baicai_1@1",
                "completed",
                ["movie/sample.txt"],
                [],
                ["movie/sample.txt"],
            )
            # 未被重新上传：mtime 保持不变（重传会写入新的 mtime）。
            assert staged_file.stat().st_mtime_ns == staged_mtime


@requires_rclone
def test_worker_rclone_still_uploads_when_remote_copy_differs():
    """指纹不一致（大小不同）时必须照常上传，不能因为"远端有同名文件"就跳过。"""
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

        staged_file = uploader_root / worker.TARGET_REMOTE_DIR / "movie" / "sample.txt"
        staged_file.parent.mkdir(parents=True)
        staged_file.write_text("stale", encoding="utf-8")

        write_rclone_config(config_path, uploader_root, dest_root)

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch.object(worker, "ALLOWED_BASE_DIRS", [str(source_dir)]):
            reports = []

            def capture(account, status, successful_files=None, missing_files=None, already_present_files=None):
                reports.append(
                    (account, status, successful_files or [], missing_files or [], already_present_files or [])
                )

            with patch("worker.report_status", side_effect=capture):
                worker.run_uploader("baicai_1@1", ["movie/sample.txt"], "copy")

            assert reports[-1] == ("baicai_1@1", "completed", ["movie/sample.txt"], [], [])
            assert staged_file.read_text(encoding="utf-8") == "hello worker"


@requires_rclone
def test_worker_rclone_move_deletes_local_when_remote_already_present():
    """""move""语义下，远端已存在且指纹一致的文件要记成功并清掉本地副本，否则会被扫描器重新入库。"""
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

        staged_file = uploader_root / worker.TARGET_REMOTE_DIR / "movie" / "sample.txt"
        staged_file.parent.mkdir(parents=True)
        shutil.copy2(source_file, staged_file)

        write_rclone_config(config_path, uploader_root, dest_root)

        with patch.dict(os.environ, {"RCLONE_CONFIG": str(config_path)}, clear=False), patch.object(worker, "ALLOWED_BASE_DIRS", [str(source_dir)]):
            reports = []

            def capture(account, status, successful_files=None, missing_files=None, already_present_files=None):
                reports.append(
                    (account, status, successful_files or [], missing_files or [], already_present_files or [])
                )

            with patch("worker.report_status", side_effect=capture):
                worker.run_uploader("baicai_1@1", ["movie/sample.txt"], "move")

            assert reports[-1] == (
                "baicai_1@1",
                "completed",
                ["movie/sample.txt"],
                [],
                ["movie/sample.txt"],
            )
            assert not source_file.exists()
            assert staged_file.exists()
