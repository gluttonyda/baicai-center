from pathlib import Path
from unittest.mock import MagicMock, patch
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import tempfile

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("NODE_TOKEN", "test-node-token-at-least-32-characters")
os.environ.setdefault("SCANNER_SEND_BEARER_AUTH", "true")
os.environ.setdefault("CENTER_API", "http://127.0.0.1:8080/api/report_files")
os.environ.setdefault("WORKER_NODE_NAME", "test-worker")
os.environ.setdefault("WORKER_API_URL", "http://127.0.0.1:8001/task/upload")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "baicai-work"))

import scanner


def test_flush_tasks_reports_and_persists_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = sqlite3.connect(Path(tmpdir) / "scanner_cache.db")
        cursor = conn.cursor()
        cursor.execute(
            """CREATE TABLE reported_files
               (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL,
                size_bytes INTEGER, mtime REAL,
                PRIMARY KEY (base_dir, file_path))"""
        )
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
        mock_response.json.return_value = {"status": "success", "deferred_files": []}

        with patch("scanner.httpx.post", return_value=mock_response) as mock_post:
            reported = scanner.flush_tasks(
                [
                    {"base_dir": "/data/links", "file_path": "movie/a.mkv", "size_bytes": 10, "mtime": 100.0},
                    {"base_dir": "/data/links", "file_path": "movie/b.mkv", "size_bytes": 20, "mtime": 101.0},
                ],
                "move",
                123.0,
                conn,
                cursor,
            )

        assert reported == 2
        rows = cursor.execute(
            "SELECT base_dir, file_path, size_bytes, mtime FROM reported_files ORDER BY file_path"
        ).fetchall()
        assert rows == [
            ("/data/links", "movie/a.mkv", 10, 100.0),
            ("/data/links", "movie/b.mkv", 20, 101.0),
        ]
        raw_body = mock_post.call_args.kwargs["content"]
        payload = json.loads(raw_body)
        assert payload["protocol_version"] == 2
        assert payload["files"] == [
            {"path": "movie/a.mkv", "size_bytes": 10, "mtime": 100.0, "source_base_dir": "/data/links"},
            {"path": "movie/b.mkv", "size_bytes": 20, "mtime": 101.0, "source_base_dir": "/data/links"},
        ]
        headers = mock_post.call_args.kwargs["headers"]
        expected_signature = hmac.new(
            scanner.NODE_TOKEN.encode("utf-8"),
            headers["X-Baicai-Timestamp"].encode("ascii") + b"\n" + raw_body,
            hashlib.sha256,
        ).hexdigest()
        assert headers["X-Baicai-Node"] == scanner.WORKER_NODE_NAME
        assert headers["X-Baicai-Signature"] == expected_signature
        assert headers["Authorization"] == f"Bearer {scanner.API_TOKEN}"


def test_flush_tasks_does_not_cache_deferred_fingerprint():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE reported_files
           (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL,
            size_bytes INTEGER, mtime REAL,
            PRIMARY KEY (base_dir, file_path))"""
    )
    cursor.execute(
        """CREATE TABLE file_observations
           (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, size_bytes INTEGER NOT NULL,
            mtime REAL NOT NULL, stable_since REAL NOT NULL, last_seen REAL NOT NULL,
            PRIMARY KEY (base_dir, file_path))"""
    )
    cursor.execute(
        "INSERT INTO file_observations VALUES (?, ?, ?, ?, ?, ?)",
        ("/data/links", "movie/active.mkv", 20, 101.0, 1.0, 2.0),
    )
    response = MagicMock()
    response.json.return_value = {
        "status": "success",
        "deferred_files": ["movie/active.mkv"],
    }

    with patch("scanner.post_report", return_value=response):
        reported = scanner.flush_tasks(
            [{
                "base_dir": "/data/links",
                "file_path": "movie/active.mkv",
                "size_bytes": 20,
                "mtime": 101.0,
            }],
            "copy",
            123.0,
            conn,
            cursor,
        )

    assert reported == 0
    assert cursor.execute("SELECT COUNT(*) FROM reported_files").fetchone()[0] == 0
    assert cursor.execute("SELECT COUNT(*) FROM file_observations").fetchone()[0] == 1
    conn.close()


def test_scan_and_report_sends_heartbeat_when_no_files():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE reported_files (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL, PRIMARY KEY (base_dir, file_path))")
    cursor.execute("CREATE TABLE scan_stats (key TEXT PRIMARY KEY, value INTEGER)")
    cursor.execute("INSERT INTO scan_stats (key, value) VALUES ('run_count', 0)")
    conn.commit()

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("scanner.init_cache_db", return_value=conn), patch("scanner.os.path.exists", return_value=False), patch(
        "scanner.httpx.post", return_value=mock_response
    ) as mock_post:
        scanner.scan_and_report()

    assert mock_response.raise_for_status.called
    assert json.loads(mock_post.call_args.kwargs["content"])["protocol_version"] == 2


def test_post_report_can_disable_legacy_bearer_header():
    response = MagicMock()
    with patch.object(scanner, "SCANNER_SEND_BEARER_AUTH", False), patch(
        "scanner.httpx.post", return_value=response
    ) as mock_post, patch("scanner.time.time", return_value=123.9):
        assert scanner.post_report({"files": [], "worker_node": "test-worker"}, timeout=10.0) is response

    kwargs = mock_post.call_args.kwargs
    assert kwargs["headers"]["X-Baicai-Timestamp"] == "123"
    assert "Authorization" not in kwargs["headers"]
    assert kwargs["content"] == b'{"files":[],"worker_node":"test-worker"}'


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

        assert [column[1] for column in columns] == ["base_dir", "file_path", "reported_at", "size_bytes", "mtime"]
        assert rows == [("", "movie/a.mkv")]


def test_init_cache_db_adds_fingerprint_to_existing_root_aware_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "scanner_cache.db"
        conn = sqlite3.connect(db_path)
        conn.execute(
            """CREATE TABLE reported_files
               (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL,
                PRIMARY KEY (base_dir, file_path))"""
        )
        conn.execute(
            "INSERT INTO reported_files (base_dir, file_path, reported_at) VALUES (?, ?, ?)",
            ("/data/links", "movie/a.mkv", 1.0),
        )
        conn.commit()
        conn.close()

        with patch("scanner.CACHE_DB_PATH", str(db_path)):
            migrated_conn = scanner.init_cache_db()
            columns = migrated_conn.execute("PRAGMA table_info(reported_files)").fetchall()
            row = migrated_conn.execute(
                "SELECT base_dir, file_path, size_bytes, mtime FROM reported_files"
            ).fetchone()
            migrated_conn.close()

        assert [column[1] for column in columns] == ["base_dir", "file_path", "reported_at", "size_bytes", "mtime"]
        assert row == ("/data/links", "movie/a.mkv", None, None)


def test_build_scan_configs_reads_custom_directories():
    with patch.dict(
        os.environ,
        {
            "LINKS_SCAN_DIR": "/custom/links",
            "CACHE_LINKS_SCAN_DIR": "/custom/cache-links",
            "RCLONE_CACHE_SCAN_DIR": "/custom/cache",
        },
        clear=False,
    ):
        configs = scanner.build_scan_configs()

    assert configs[0]["dir"] == "/custom/links"
    assert configs[0]["upload_method"] == "copy"
    assert configs[1]["dir"] == "/custom/cache-links"
    assert configs[1]["upload_method"] == "copy"
    assert configs[1]["delay_seconds"] == 3600
    assert configs[2]["dir"] == "/custom/cache"
    assert configs[2]["upload_method"] == "move"


def test_should_ignore_clfstmp_files_case_insensitive():
    assert scanner.should_ignore_file("movie/downloading.clfstmp")
    assert scanner.should_ignore_file("movie/downloading.CLFSTMP")
    assert scanner.should_ignore_file("movie/downloading.mkv.clfstmp.progress")
    assert not scanner.should_ignore_file("movie/complete.mkv")


def test_path_validation_preserves_spaces_and_rejects_line_controls():
    assert scanner.is_reportable_path(" folder/movie .mkv ")
    assert not scanner.is_reportable_path("folder/bad\rname.mkv")
    assert not scanner.is_reportable_path("folder/bad\nname.mkv")
    assert not scanner.is_reportable_path("folder/bad\x00name.mkv")

    with patch.dict(os.environ, {"LINKS_SCAN_DIR": "/data/links "}, clear=False):
        assert scanner.build_scan_configs()[0]["dir"] == "/data/links "

    with patch.dict(os.environ, {"LINKS_SCAN_DIR": "/data/links\nother"}, clear=False):
        try:
            scanner.build_scan_configs()
        except RuntimeError as exc:
            assert "控制字符" in str(exc)
        else:
            raise AssertionError("newline-containing scan root must be rejected")


def test_scan_and_report_uses_configured_scan_directories():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE reported_files
           (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL,
            size_bytes INTEGER, mtime REAL,
            PRIMARY KEY (base_dir, file_path))"""
    )
    cursor.execute("CREATE TABLE scan_stats (key TEXT PRIMARY KEY, value INTEGER)")
    cursor.execute("INSERT INTO scan_stats (key, value) VALUES ('run_count', 0)")
    conn.commit()

    with tempfile.TemporaryDirectory() as tmpdir:
        links_dir = Path(tmpdir) / "links"
        cache_links_dir = Path(tmpdir) / "cache-links"
        cache_dir = Path(tmpdir) / "cache"
        links_dir.mkdir()
        cache_links_dir.mkdir()
        cache_dir.mkdir()
        test_file = links_dir / "movie.txt"
        test_file.write_text("hello", encoding="utf-8")
        test_file_mtime = test_file.stat().st_mtime
        cache_links_file = cache_links_dir / "cache-movie.txt"
        cache_links_file.write_text("cache", encoding="utf-8")
        cache_links_file_mtime = cache_links_file.stat().st_mtime

        reported_payloads = []

        def fake_post(url, content, headers, timeout):
            reported_payloads.append(json.loads(content))
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            return mock_response

        with patch("scanner.init_cache_db", return_value=conn), patch("scanner.httpx.post", side_effect=fake_post), patch.object(
            scanner,
                "SCAN_CONFIGS",
                [
                    {"dir": str(links_dir), "delay_seconds": 0, "upload_method": "copy"},
                    {"dir": str(cache_links_dir), "delay_seconds": 0, "upload_method": "copy"},
                    {"dir": str(cache_dir), "delay_seconds": 0, "upload_method": "move"},
                ],
        ):
            scanner.scan_and_report()

    assert any(
        payload["files"]
        == [
            {
                "path": "movie.txt",
                "size_bytes": 5,
                "mtime": test_file_mtime,
                "source_base_dir": str(links_dir),
            }
        ]
        for payload in reported_payloads
    )
    assert any(
        payload["files"]
        == [
            {
                "path": "cache-movie.txt",
                "size_bytes": 5,
                "mtime": cache_links_file_mtime,
                "source_base_dir": str(cache_links_dir),
            }
        ]
        for payload in reported_payloads
    )
    assert any(payload["upload_method"] == "copy" for payload in reported_payloads)


def test_changed_fingerprint_for_same_path_is_reported_again():
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir) / "scan"
        root.mkdir()
        file_path = root / " movie.txt "
        file_path.write_text("first", encoding="utf-8")
        os.utime(file_path, (100.0, 100.0))
        db_path = Path(tmpdir) / "scanner_cache.db"
        payloads = []

        def fake_post(url, content, headers, timeout):
            payloads.append(json.loads(content))
            response = MagicMock()
            response.raise_for_status.return_value = None
            return response

        with patch("scanner.CACHE_DB_PATH", str(db_path)), patch.object(
            scanner,
            "SCAN_CONFIGS",
            [{"dir": str(root), "delay_seconds": 0, "upload_method": "move", "stable_seconds": 0}],
        ), patch("scanner.httpx.post", side_effect=fake_post):
            scanner.scan_and_report()
            scanner.scan_and_report()
            file_path.write_text("second", encoding="utf-8")
            os.utime(file_path, (200.0, 200.0))
            scanner.scan_and_report()

        file_payloads = [payload for payload in payloads if payload["files"]]
        assert len(file_payloads) == 2
        assert file_payloads[0]["files"][0]["path"] == " movie.txt "
        assert file_payloads[1]["files"][0]["path"] == " movie.txt "
