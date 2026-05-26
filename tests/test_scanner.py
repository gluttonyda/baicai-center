from pathlib import Path
from unittest.mock import MagicMock, patch
import os
import sqlite3
import sys
import tempfile

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("CENTER_API", "http://127.0.0.1:8080/api/report_files")
os.environ.setdefault("WORKER_NODE_NAME", "test-worker")
os.environ.setdefault("WORKER_API_URL", "http://127.0.0.1:8001/task/upload")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scanner


def test_flush_tasks_reports_and_persists_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = sqlite3.connect(Path(tmpdir) / "scanner_cache.db")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE reported_files (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL, PRIMARY KEY (base_dir, file_path))")
        cursor.execute(
            """CREATE TABLE file_observations
               (base_dir TEXT NOT NULL,
                file_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                mtime REAL NOT NULL,
                stable_since REAL NOT NULL,
                last_seen REAL NOT NULL,
                PRIMARY KEY (base_dir, file_path))"""
        )
        conn.commit()

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("scanner.httpx.post", return_value=mock_response):
            reported = scanner.flush_tasks(
                [
                    {"base_dir": "/data/links", "file_path": "movie/a.mkv"},
                    {"base_dir": "/data/links", "file_path": "movie/b.mkv"},
                ],
                "move",
                123.0,
                conn,
                cursor,
            )

        assert reported == 2
        rows = cursor.execute("SELECT base_dir, file_path FROM reported_files ORDER BY file_path").fetchall()
        assert rows == [("/data/links", "movie/a.mkv"), ("/data/links", "movie/b.mkv")]


def test_scan_and_report_sends_heartbeat_when_no_files():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE reported_files (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL, PRIMARY KEY (base_dir, file_path))")
    cursor.execute("CREATE TABLE scan_stats (key TEXT PRIMARY KEY, value INTEGER)")
    cursor.execute("INSERT INTO scan_stats (key, value) VALUES ('run_count', 0)")
    conn.commit()

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("scanner.init_cache_db", return_value=conn), patch("scanner.os.path.exists", return_value=False), patch("scanner.httpx.post", return_value=mock_response):
        scanner.scan_and_report()

    assert mock_response.raise_for_status.called


def test_init_cache_db_migrates_legacy_primary_key():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "scanner_cache.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE reported_files (file_path TEXT PRIMARY KEY, reported_at REAL)")
        cursor.execute("INSERT INTO reported_files (file_path, reported_at) VALUES (?, ?)", ("movie/a.mkv", 1.0))
        cursor.execute("CREATE TABLE scan_stats (key TEXT PRIMARY KEY, value INTEGER)")
        cursor.execute("INSERT INTO scan_stats (key, value) VALUES ('run_count', 0)")
        conn.commit()
        conn.close()

        with patch("scanner.CACHE_DB_PATH", str(db_path)):
            migrated_conn = scanner.init_cache_db()
            migrated_cursor = migrated_conn.cursor()
            columns = migrated_cursor.execute("PRAGMA table_info(reported_files)").fetchall()
            rows = migrated_cursor.execute("SELECT base_dir, file_path FROM reported_files").fetchall()
            migrated_conn.close()

        assert [column[1] for column in columns] == ["base_dir", "file_path", "reported_at"]
        assert rows == [("", "movie/a.mkv")]


def test_build_scan_configs_reads_custom_directories():
    with patch.dict(
        os.environ,
        {
            "LINKS_SCAN_DIR": "/custom/links",
            "RCLONE_CACHE_SCAN_DIR": "/custom/cache",
        },
        clear=False,
    ):
        configs = scanner.build_scan_configs()

    assert configs[0]["dir"] == "/custom/links"
    assert configs[0]["upload_method"] == "copy"
    assert configs[1]["dir"] == "/custom/cache"
    assert configs[1]["upload_method"] == "move"


def test_scan_and_report_uses_configured_scan_directories():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE reported_files (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL, PRIMARY KEY (base_dir, file_path))")
    cursor.execute("CREATE TABLE scan_stats (key TEXT PRIMARY KEY, value INTEGER)")
    cursor.execute("INSERT INTO scan_stats (key, value) VALUES ('run_count', 0)")
    conn.commit()

    with tempfile.TemporaryDirectory() as tmpdir:
        links_dir = Path(tmpdir) / "links"
        cache_dir = Path(tmpdir) / "cache"
        links_dir.mkdir()
        cache_dir.mkdir()
        test_file = links_dir / "movie.txt"
        test_file.write_text("hello", encoding="utf-8")

        reported_payloads = []

        def fake_post(url, json, headers, timeout):
            reported_payloads.append(json)
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            return mock_response

        with patch("scanner.init_cache_db", return_value=conn), patch("scanner.httpx.post", side_effect=fake_post), patch.object(
            scanner,
            "SCAN_CONFIGS",
            [
                {"dir": str(links_dir), "delay_seconds": 0, "upload_method": "copy"},
                {"dir": str(cache_dir), "delay_seconds": 0, "upload_method": "move"},
            ],
        ):
            scanner.scan_and_report()

    assert any(payload["files"] == ["movie.txt"] for payload in reported_payloads)
    assert any(payload["upload_method"] == "copy" for payload in reported_payloads)
