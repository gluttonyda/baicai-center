from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone
import httpx
import json
import logging
import os
import sys
import threading
import time

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ALLOW_LEGACY_NODE_AUTH", "true")
os.environ.setdefault("ALLOW_LEGACY_UNLEASED_REPORTS", "true")
os.environ.setdefault("DB_HOST", "test-db-host")
os.environ.setdefault("DB_USER", "test-db-user")
os.environ.setdefault("DB_PASSWORD", "test-db-password")
os.environ.setdefault("DB_NAME", "test-db-name")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main
from main import API_TOKEN, app

client = TestClient(app)


@pytest.fixture
def mock_db():
    with patch("main.get_db_connection") as mock_conn:
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = {
            "role": "uploader",
            "current_dispatch_id": None,
            "current_worker_node": None,
        }
        mock_conn.return_value = mock_connection
        yield mock_cursor


def auth_headers():
    return {"Authorization": f"Bearer {API_TOKEN}"}


@pytest.fixture(autouse=True)
def fixed_upload_limit_for_dispatch_tests(request):
    if "dispatch_uploader_tasks" not in request.node.name:
        yield
        return

    with patch.object(main, "current_upload_concurrency_limit", return_value=2):
        yield


def test_current_upload_concurrency_limit_switches_by_beijing_hour():
    night = datetime(2026, 6, 21, 1, 0, 0, tzinfo=timezone(timedelta(hours=8)))
    day = datetime(2026, 6, 21, 10, 0, 0, tzinfo=timezone(timedelta(hours=8)))

    assert main.current_upload_concurrency_limit(night) == 3
    assert main.current_upload_concurrency_limit(day) == 2


def test_upload_remote_dir_routes_legacy_and_new_final_drives():
    assert main.upload_remote_dir_for_account("baicai_big@1") == "LongEmby-A"
    assert main.upload_remote_dir_for_account("baicai_big2@1") == "LongEmby-B"
    assert main.upload_remote_dir_for_account("baicai_2@1") == "LongEmby-B"
    assert main.is_direct_upload_account("baicai_big@1") is True
    assert main.is_direct_upload_account("baicai_big2@1") is True
    assert main.is_direct_upload_account("baicai_2@1") is False


def test_unauthorized_access():
    response = client.post("/api/report_status", json={"account_name": "test", "status": "completed"})
    assert response.status_code == 401


def test_report_status_preserves_leading_and_trailing_spaces():
    request = main.ReportStatusRequest(
        account_name="baicai_1@1",
        status="completed",
        successful_files=[" movie.mkv "],
        missing_files=[" missing.mkv "],
    )

    assert request.successful_files == [" movie.mkv "]
    assert request.missing_files == [" missing.mkv "]


VALID_FILE_PATH_HASH_COLUMN = {
    "data_type": "char",
    "max_length": 64,
    "extra": "STORED GENERATED",
    "generation_expression": "sha2(`file_path`,256)",
}
FULL_HASH_INDEX_ONLY = {"full_hash_indexes": 1, "prefix_hash_indexes": 0}


def health_ready_with_schema(column, indexes):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [column, indexes]

    with patch("main.get_db_connection", return_value=connection):
        response = client.get("/health/ready")

    connection.close.assert_called_once_with()
    return response, cursor


def test_health_ready_queries_database_and_closes_connection():
    response, cursor = health_ready_with_schema(VALID_FILE_PATH_HASH_COLUMN, FULL_HASH_INDEX_ONLY)

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert cursor.execute.call_count == 6
    assert "source_mtime" in cursor.execute.call_args_list[0].args[0]
    assert "protocol_version" in cursor.execute.call_args_list[1].args[0]
    assert "current_dispatch_id" in cursor.execute.call_args_list[2].args[0]
    assert "dispatch_id" in cursor.execute.call_args_list[3].args[0]
    assert "GENERATION_EXPRESSION" in cursor.execute.call_args_list[4].args[0]
    assert "NON_UNIQUE=0" in cursor.execute.call_args_list[5].args[0]
    assert "SUB_PART" in cursor.execute.call_args_list[5].args[0]


@pytest.mark.parametrize(
    "expression",
    ["sha2(`file_path`,256)", "SHA2(file_path, 256)", "  Sha2( `file_path` ,\n256 ) "],
)
def test_health_ready_accepts_equivalent_generation_expression_spellings(expression):
    column = {**VALID_FILE_PATH_HASH_COLUMN, "generation_expression": expression}

    response, _ = health_ready_with_schema(column, FULL_HASH_INDEX_ONLY)

    assert response.status_code == 200


@pytest.mark.parametrize(
    "column",
    [
        None,
        {**VALID_FILE_PATH_HASH_COLUMN, "extra": "", "generation_expression": ""},
        {**VALID_FILE_PATH_HASH_COLUMN, "extra": "VIRTUAL GENERATED"},
        {**VALID_FILE_PATH_HASH_COLUMN, "generation_expression": "sha2(lower(`file_path`),256)"},
        {**VALID_FILE_PATH_HASH_COLUMN, "generation_expression": "sha2(`file_path`,224)"},
        {**VALID_FILE_PATH_HASH_COLUMN, "data_type": "varchar"},
        {**VALID_FILE_PATH_HASH_COLUMN, "max_length": 32},
    ],
    ids=["missing", "plain", "virtual", "lower_path", "sha224", "varchar", "char32"],
)
def test_health_ready_returns_503_for_incompatible_file_path_hash_column(column):
    response, _ = health_ready_with_schema(column, FULL_HASH_INDEX_ONLY)

    assert response.status_code == 503


@pytest.mark.parametrize(
    "indexes",
    [
        {"full_hash_indexes": 0, "prefix_hash_indexes": 0},
        {"full_hash_indexes": 0, "prefix_hash_indexes": 1},
        {"full_hash_indexes": 1, "prefix_hash_indexes": 1},
    ],
    ids=["missing", "prefix_only", "full_and_prefix"],
)
def test_health_ready_returns_503_for_missing_or_prefix_hash_unique_index(indexes):
    response, _ = health_ready_with_schema(VALID_FILE_PATH_HASH_COLUMN, indexes)

    assert response.status_code == 503


def test_db_pool_pings_connections_when_checked_out():
    original_pool = main._db_pool
    main._db_pool = None
    try:
        with patch("main.PooledDB") as mock_pooled_db:
            main.get_db_pool()
    finally:
        main._db_pool = original_pool

    assert mock_pooled_db.call_args.kwargs["ping"] == 1


def test_health_ready_returns_503_when_database_is_unavailable():
    with patch("main.get_db_connection", side_effect=RuntimeError("database unavailable")):
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}


def test_report_status_completed_updates_tasks(mock_db):
    payload = {
        "account_name": "baicai_1@1",
        "status": "completed",
        "successful_files": ["movie/test1.mp4"],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("INSERT IGNORE INTO account_upload_events" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks SET status=%s" in sql for sql in sql_calls)


def test_report_status_uploading_refreshes_worker_last_active(mock_db):
    payload = {
        "account_name": "baicai_1@1",
        "status": "uploading",
        "successful_files": [],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("SET status='uploading', updated_at=UTC_TIMESTAMP()" in sql for sql in sql_calls)
    assert any("SET dispatch_locked_until = DATE_ADD" in sql for sql in sql_calls)
    assert any("UPDATE worker_registry wr" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks" in sql and "updated_at = UTC_TIMESTAMP()" in sql for sql in sql_calls)


def test_report_status_403_quota(mock_db):
    payload = {
        "account_name": "baicai_2@1",
        "status": "403_quota",
        "successful_files": [],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("dispatch_locked_until = NULL" in sql and "quota_blocked_until = DATE_ADD" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks SET status='pending'" in sql for sql in sql_calls)


def test_report_status_credential_invalid_disables_account(mock_db):
    payload = {
        "account_name": "baicai_big@1",
        "status": "credential_invalid",
        "successful_files": [],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("SET is_active = 0" in sql and "dispatch_locked_until = NULL" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks SET status='pending'" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks SET account_used=NULL, updated_at=UTC_TIMESTAMP() WHERE status='pending' AND account_used=%s" in sql for sql in sql_calls)


def test_report_files_uses_configured_worker_api_url(mock_db):
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://malicious.example/task/upload",
        "files": ["movie/test1.mkv"],
        "upload_method": "move",
    }

    with patch.object(main, "WORKER_API_URL_MAP", {"worker-node-1": "http://worker-node-1.example.com:8001/task/upload"}):
        response = client.post("/api/report_files", json=payload, headers=auth_headers())

    assert response.status_code == 200
    insert_args = mock_db.execute.call_args_list[0][0][1]
    assert insert_args == ("worker-node-1", "http://worker-node-1.example.com:8001/task/upload", 1)
    insert_sql = mock_db.execute.call_args_list[0][0][0]
    assert "UTC_TIMESTAMP()" in insert_sql
    assert "NOW()" not in insert_sql


def test_report_files_persists_file_sizes(mock_db):
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1.example.com:8001/task/upload",
        "files": [
            {"path": "movie/sized.mkv", "size_bytes": 123, "mtime": 456.5},
            "movie/legacy.mkv",
        ],
        "upload_method": "move",
    }

    response = client.post("/api/report_files", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql = mock_db.executemany.call_args[0][0]
    values = mock_db.executemany.call_args[0][1]
    assert "(file_path, source_base_dir, size_bytes, source_mtime, status, worker_node, upload_method)" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "COALESCE(file_tasks.size_bytes, VALUES(size_bytes))" in sql
    assert "source_base_dir = CASE" in sql
    assert "worker_node = CASE" in sql
    assert "upload_method = CASE" in sql
    assert "file_tasks.status = 'not_found'" in sql
    assert values == [
        ("movie/sized.mkv", None, 123, 456.5, "pending", "worker-node-1", "move"),
        ("movie/legacy.mkv", None, None, None, "pending", "worker-node-1", "move"),
    ]


def test_report_files_defers_fingerprint_change_retained_by_active_dispatch(mock_db):
    mock_db.fetchall.return_value = [
        {
            "file_path": "movie/active.mkv",
            "source_base_dir": "/cache/downloads",
            "size_bytes": 100,
            "source_mtime": 1000.0,
        }
    ]
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1.example.com:8001/task/upload",
        "files": [
            {
                "path": "movie/active.mkv",
                "source_base_dir": "/cache/links",
                "size_bytes": 200,
                "mtime": 2000.0,
            }
        ],
        "upload_method": "move",
    }

    response = client.post("/api/report_files", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "deferred_files": ["movie/active.mkv"],
    }


def test_report_files_completed_fingerprint_change_can_requeue_and_root_can_refresh(mock_db):
    mock_db.fetchall.return_value = [
        {
            "file_path": "movie/replaced.mkv",
            "source_base_dir": "/cache/links",
            "size_bytes": 200,
            "source_mtime": 2000.0,
        }
    ]
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1.example.com:8001/task/upload",
        "files": [
            {
                "path": "movie/replaced.mkv",
                "source_base_dir": "/cache/links",
                "size_bytes": 200,
                "mtime": 2000.0,
            }
        ],
        "upload_method": "move",
    }

    response = client.post("/api/report_files", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"status": "success", "deferred_files": []}
    upsert_sql = mock_db.executemany.call_args.args[0]
    assert "file_tasks.status = 'completed'" in upsert_sql
    assert "file_tasks.status IN ('pending', 'completed')" in upsert_sql
    assert "WHEN file_tasks.status IN ('uploading', 'staged')" in upsert_sql
    assert "file_tasks.source_mtime <> VALUES(source_mtime)" in upsert_sql
    assert "file_tasks.size_bytes <> VALUES(size_bytes)" in upsert_sql


def test_validate_dispatch_renews_only_matching_live_lease(mock_db):
    mock_db.rowcount = 1
    mock_db.fetchone.side_effect = [
        {"id": 1},
        {"active": 1},
    ]
    payload = {
        "dispatch_id": "dispatch-lease-valid-1",
        "account_name": "baicai_1@1",
        "worker_node": "worker-node-1",
    }

    response = client.post("/api/dispatch/validate", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"active": True}
    sql_calls = [call.args for call in mock_db.execute.call_args_list]
    task_lock_sql, task_lock_params = next(
        (sql, params) for sql, params in sql_calls if "FROM file_tasks" in sql and "FOR UPDATE" in sql
    )
    account_lock_sql, account_lock_params = next(
        (sql, params) for sql, params in sql_calls if "FROM drive_accounts" in sql and "FOR UPDATE" in sql
    )
    renewal_sql, renewal_params = next(
        (sql, params) for sql, params in sql_calls if "UPDATE drive_accounts" in sql and "DATE_ADD" in sql
    )
    assert "status IN ('uploading', 'staged')" in task_lock_sql
    assert task_lock_params == ("dispatch-lease-valid-1",)
    assert "current_dispatch_id=%s" in account_lock_sql
    assert "current_worker_node=%s" in account_lock_sql
    assert "dispatch_locked_until > UTC_TIMESTAMP()" in account_lock_sql
    assert account_lock_params == (
        "baicai_1@1",
        "dispatch-lease-valid-1",
        "worker-node-1",
    )
    assert "current_dispatch_id=%s" in renewal_sql
    assert renewal_params[1:] == account_lock_params


def test_validate_dispatch_rejects_lease_without_active_tasks(mock_db):
    mock_db.fetchone.return_value = None
    payload = {
        "dispatch_id": "dispatch-without-tasks-1",
        "account_name": "baicai_1@1",
        "worker_node": "worker-node-1",
    }

    response = client.post("/api/dispatch/validate", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"active": False}
    sql_calls = [call.args[0] for call in mock_db.execute.call_args_list]
    assert any("current_dispatch_id=NULL" in sql for sql in sql_calls)
    assert not any("UPDATE worker_registry" in sql for sql in sql_calls)


def test_report_files_ignores_clfstmp_files(mock_db):
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1.example.com:8001/task/upload",
        "files": [
            {"path": "movie/complete.mkv", "size_bytes": 123},
            {"path": "movie/downloading.CLFSTMP", "size_bytes": 456},
            {"path": "movie/downloading.mkv.clfstmp.progress", "size_bytes": 789},
        ],
        "upload_method": "copy",
    }

    response = client.post("/api/report_files", json=payload, headers=auth_headers())

    assert response.status_code == 200
    values = mock_db.executemany.call_args[0][1]
    assert values == [
        ("movie/complete.mkv", None, 123, None, "pending", "worker-node-1", "copy")
    ]


def test_report_status_rejects_unknown_status():
    payload = {
        "account_name": "baicai_1@1",
        "status": "mystery",
        "successful_files": [],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 422


def test_dispatch_uploader_tasks_rolls_back_when_worker_post_fails():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [{"id": 1, "file_path": "movie/test1.mkv", "upload_method": "move"}],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("boom")

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("UPDATE file_tasks SET status='pending', dispatch_id=NULL" in sql and "WHERE dispatch_id=%s" in sql for sql in sql_calls)


def test_dispatch_uploader_tasks_sets_uploading_updated_at_to_utc():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [{"id": 1, "file_path": "movie/test1.mkv", "upload_method": "move"}],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any(
        "UPDATE file_tasks SET status='uploading', account_used=%s, dispatch_id=%s, updated_at=UTC_TIMESTAMP()" in sql
        for sql in sql_calls
    )


def test_dispatch_uploader_tasks_breaks_when_tasks_are_preempted():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 0

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [{"id": 1, "file_path": "movie/test1.mkv", "upload_method": "move"}],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post") as mock_post:
        main.dispatch_uploader_tasks()

    mock_post.assert_not_called()


def test_dispatch_uploader_tasks_uses_utc_for_worker_liveness_queries():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [],
    ]

    with patch("main.get_db_connection", return_value=mock_connection):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list[:2]]
    assert all("UTC_TIMESTAMP()" in sql for sql in sql_calls)
    assert all("NOW()" not in sql for sql in sql_calls)


def test_acquire_account_lock_cleaner_checks_dispatch_cooldown():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1

    assert main.acquire_account_lock(mock_cursor, "baicai-1@1-3", "cleaner") is True

    sql = mock_cursor.execute.call_args[0][0]
    assert "dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP()" in sql
    assert "quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP()" in sql
    assert "dispatch_cooldown_until IS NULL OR dispatch_cooldown_until < UTC_TIMESTAMP()" in sql


def test_acquire_account_lock_uploader_still_checks_dispatch_cooldown():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1

    assert main.acquire_account_lock(mock_cursor, "baicai_1@1", "uploader") is True

    sql = mock_cursor.execute.call_args[0][0]
    assert "dispatch_cooldown_until IS NULL OR dispatch_cooldown_until < UTC_TIMESTAMP()" in sql


def test_dispatch_uploader_tasks_clears_pending_preferences_for_unavailable_accounts():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [],
    ]

    with patch("main.get_db_connection", return_value=mock_connection):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    cleanup_sql = next(sql for sql in sql_calls if "UPDATE file_tasks ft" in sql and "JOIN drive_accounts da" in sql)
    assert "ft.status = 'pending'" in cleanup_sql
    assert "da.is_active = 0" in cleanup_sql
    assert "da.quota_blocked_until" in cleanup_sql


def test_dispatch_uploader_tasks_releases_stale_uploading_tasks():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 253
    mock_cursor.fetchall.side_effect = [
        [],
        [{"account_used": "baicai_2@1", "worker_node": "worker-node-3", "dispatch_id": "d" * 32, "task_count": 253}],
        [],
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch.object(main, "release_account_lock") as mock_release_lock:
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks" in sql and "SET status='pending'" in sql for sql in sql_calls)
    mock_release_lock.assert_called_once_with(mock_cursor, "baicai_2@1", "d" * 32, "worker-node-3")


def test_requeue_stale_not_found_tasks_uses_bounded_retry_delay():
    cursor = MagicMock()
    cursor.rowcount = 3

    assert main.requeue_stale_not_found_tasks(cursor) == 3

    sql, params = cursor.execute.call_args.args
    assert "WHERE status='not_found'" in sql
    assert "DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)" in sql
    assert "account_used=NULL" in sql
    assert params == (main.NOT_FOUND_RETRY_MINUTES,)


def test_dispatch_uploader_tasks_releases_lock_when_no_pending_tasks():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    with patch("main.get_db_connection", return_value=mock_connection):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("SET dispatch_locked_until=NULL, current_dispatch_id=NULL" in sql for sql in sql_calls)


def test_dispatch_uploader_tasks_filters_dispatch_and_quota_locks():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    with patch("main.get_db_connection", return_value=mock_connection):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    available_account_sql = next(sql for sql in sql_calls if "FROM drive_accounts da" in sql and "has_preferred_pending" in sql)
    assert "dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP()" in available_account_sql
    assert "quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP()" in available_account_sql
    assert "cleaner.account_name = da.cleaner_mapping" in available_account_sql
    assert "cleaner.role='cleaner'" in available_account_sql


def test_dispatch_uploader_tasks_rechecks_cleaner_lock_when_marking_uploading():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [{"id": 1, "file_path": "movie/test1.mkv", "upload_method": "move"}],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    mark_uploading_sql = next(sql for sql in sql_calls if "UPDATE file_tasks SET status='uploading'" in sql)
    assert "cleaner.account_name = da.cleaner_mapping" in mark_uploading_sql
    assert "cleaner.dispatch_locked_until > UTC_TIMESTAMP()" in mark_uploading_sql


def test_dispatch_uploader_tasks_prefers_original_account_pending_tasks():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1", "has_preferred_pending": 1}],
        [{"id": 1, "file_path": "movie/test1.mkv", "upload_method": "move"}],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    available_account_sql = next(sql for sql in sql_calls if "has_preferred_pending" in sql)
    task_pick_sql = next(sql for sql in sql_calls if "CASE WHEN account_used=%s THEN 0 ELSE 1 END" in sql)
    assert "ORDER BY has_preferred_pending DESC" in available_account_sql
    assert "AND (account_used=%s OR account_used IS NULL)" in task_pick_sql


def test_dispatch_cleaner_tasks_force_all_still_skips_when_group_uploading():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchall.side_effect = [
        [],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
    ]
    mock_cursor.fetchone.side_effect = [
        {"has_uploading": 1},
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post") as mock_post:
        main.dispatch_cleaner_tasks(force_all=True)

    mock_post.assert_not_called()
    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("status='uploading'" in sql and "account_used LIKE" in sql for sql in sql_calls)


def test_dispatch_cleaner_tasks_skips_when_group_recently_active():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchall.side_effect = [
        [],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 1},
        {"has_recent_activity": 1},
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post") as mock_post:
        main.dispatch_cleaner_tasks()

    mock_post.assert_not_called()
    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("updated_at >= DATE_SUB(UTC_TIMESTAMP()" in sql for sql in sql_calls)


def test_dispatch_cleaner_tasks_cleans_immediately_when_group_all_quota_blocked():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
        [{"id": 1, "file_path": "movie/test1.mkv", "source_base_dir": "/cache/downloads"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 2},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
        None,
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response) as mock_post:
        main.dispatch_cleaner_tasks()

    mock_post.assert_called_once()
    dispatched_payload = json.loads(mock_post.call_args.kwargs["content"])
    assert dispatched_payload["dest_name"] == "baicai-big2"
    assert dispatched_payload["remote_dir"] == "LongEmby-B"
    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    task_pick_call = next(
        call
        for call in mock_cursor.execute.call_args_list
        if "JOIN drive_accounts da ON ft.account_used = da.account_name" in call.args[0]
    )
    task_bind_call = next(
        call
        for call in mock_cursor.execute.call_args_list
        if "SET ft.dispatch_id=%s" in call.args[0]
    )
    assert "AND ft.worker_node=%s" in task_pick_call.args[0]
    assert task_pick_call.args[1][1] == "worker-node-1"
    assert "AND ft.worker_node=%s" in task_bind_call.args[0]
    assert task_bind_call.args[1][2] == "worker-node-1"
    assert any("quota_blocked_until IS NOT NULL AND quota_blocked_until > UTC_TIMESTAMP()" in sql for sql in sql_calls)
    assert not any("updated_at >= DATE_SUB(UTC_TIMESTAMP()" in sql for sql in sql_calls)


def test_dispatch_cleaner_tasks_force_all_bypasses_idle_window_but_not_safety():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
        [{"id": 1, "file_path": "movie/test1.mkv", "source_base_dir": "/cache/downloads"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 1},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
        None,
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response) as mock_post:
        main.dispatch_cleaner_tasks(force_all=True)

    mock_post.assert_called_once()
    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert not any("updated_at >= DATE_SUB(UTC_TIMESTAMP()" in sql for sql in sql_calls)


def test_dispatch_cleaner_tasks_skips_stale_nodes_and_releases_cleaner_lock():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [{"node_name": "worker-node-1"}],
        [{"account_name": "baicai-1@1-3", "current_dispatch_id": "d" * 32}],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post") as mock_post, patch("main.release_account_lock") as mock_release_lock:
        main.dispatch_cleaner_tasks()

    mock_post.assert_not_called()
    mock_release_lock.assert_not_called()
    stale_release_sql = next(
        call.args[0]
        for call in mock_cursor.execute.call_args_list
        if "UPDATE drive_accounts" in call.args[0] and "NOT EXISTS" in call.args[0]
    )
    assert "dispatch_id=%s" in stale_release_sql
    assert "status='staged'" in stale_release_sql
    assert "updated_at > DATE_SUB(UTC_TIMESTAMP()" in stale_release_sql


def test_dispatch_cleaner_tasks_stops_retrying_same_node_after_worker_busy():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [
            {"account_used": "baicai_1@1", "worker_node": "worker-node-3"},
            {"account_used": "baicai_2@1", "worker_node": "worker-node-3"},
        ],
        [{"id": 1, "file_path": "movie/test1.mkv", "source_base_dir": "/cache/downloads"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 2},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
        None,
    ]

    request = httpx.Request("POST", "http://worker:8001/task/upload")
    response = httpx.Response(429, request=request)
    busy_error = httpx.HTTPStatusError("worker busy", request=request, response=response)
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = busy_error

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response) as mock_post:
        main.dispatch_cleaner_tasks()

    mock_post.assert_called_once()


def test_load_worker_api_url_map_rejects_invalid_json():
    with patch.dict(os.environ, {"WORKER_API_URL_MAP": "{invalid-json"}, clear=False):
        with pytest.raises(RuntimeError):
            main._load_worker_api_url_map()


def test_report_status_marks_missing_files_not_found(mock_db):
    payload = {
        "account_name": "baicai_1@1",
        "status": "completed",
        "successful_files": [],
        "missing_files": ["movie/missing.mkv"],
    }
    mock_db.fetchone.side_effect = [
        {"role": "uploader"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("status=%s, account_used=NULL" in sql for sql in sql_calls)


def test_report_status_not_found_clears_account_used_when_requeueing_remaining_tasks(mock_db):
    payload = {
        "account_name": "baicai_1@1",
        "status": "not_found",
        "successful_files": [],
        "missing_files": ["movie/missing.mkv"],
    }
    mock_db.fetchone.side_effect = [
        {"role": "uploader"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("UPDATE file_tasks SET status='pending', account_used=NULL, dispatch_id=NULL" in sql for sql in sql_calls)


def test_report_status_rejects_retry_later_status():
    payload = {
        "account_name": "baicai_1@1",
        "status": "retry_later",
        "successful_files": [],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 422


def test_report_status_failed_keeps_original_account_preference(mock_db):
    payload = {
        "account_name": "baicai_1@1",
        "status": "failed",
        "successful_files": [],
    }
    mock_db.fetchone.side_effect = [
        {"role": "uploader"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("UPDATE file_tasks SET status='pending', dispatch_id=NULL" in sql and "account_used=%s" in sql for sql in sql_calls)
    assert not any("UPDATE file_tasks SET status='pending', account_used=NULL WHERE account_used=%s AND status='uploading'" in sql for sql in sql_calls)


def test_report_status_cleaner_failed_sets_dispatch_cooldown(mock_db):
    payload = {
        "account_name": "baicai-1@1-3",
        "status": "failed",
        "successful_files": [],
    }
    mock_db.fetchone.side_effect = [
        {"role": "cleaner"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any(
        "dispatch_locked_until = NULL" in sql and "dispatch_cooldown_until = DATE_ADD" in sql
        for sql in sql_calls
    )
    assert not any("UPDATE file_tasks SET status='pending'" in sql for sql in sql_calls)


def test_trigger_dispatch_uploader_tasks_async_reruns_when_triggered_during_dispatch():
    done = threading.Event()
    call_count = 0

    def fake_dispatch():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            main.trigger_dispatch_uploader_tasks_async("second")
        else:
            done.set()

    main._dispatch_trigger_pending.clear()
    with patch("main.dispatch_uploader_tasks", side_effect=fake_dispatch):
        main.trigger_dispatch_uploader_tasks_async("first")
        assert done.wait(1.0)

    assert call_count == 2
    assert main._dispatch_trigger_pending.is_set() is False


def _make_idle_dispatch_connection(on_close=None):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []
    cursor.rowcount = 0
    if on_close is not None:
        connection.close.side_effect = on_close
    return connection


def test_scheduled_and_triggered_uploader_dispatch_never_overlap():
    state_lock = threading.Lock()
    first_entered = threading.Event()
    release_first = threading.Event()
    second_finished = threading.Event()
    active = 0
    max_active = 0
    calls = 0

    def on_close():
        nonlocal active
        with state_lock:
            active -= 1
            finished_calls = calls
        if finished_calls >= 2:
            second_finished.set()

    def fake_get_db_connection():
        nonlocal active, max_active, calls
        with state_lock:
            active += 1
            calls += 1
            current_call = calls
            max_active = max(max_active, active)
        if current_call == 1:
            first_entered.set()
            assert release_first.wait(5.0)
        return _make_idle_dispatch_connection(on_close)

    main._dispatch_trigger_pending.clear()
    with patch("main.get_db_connection", side_effect=fake_get_db_connection):
        scheduled = threading.Thread(target=main.dispatch_uploader_tasks, name="scheduled-dispatch")
        scheduled.start()
        assert first_entered.wait(2.0)
        main.trigger_dispatch_uploader_tasks_async("403_quota:baicai_1@1")
        time.sleep(0.5)
        with state_lock:
            overlap_seen = max_active
        release_first.set()
        scheduled.join(5.0)
        assert second_finished.wait(5.0)

    assert overlap_seen == 1
    assert max_active == 1
    assert calls == 2


def test_uploader_dispatch_mutex_recovers_after_exception():
    calls = 0

    def flaky_get_db_connection():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("database temporarily unavailable")
        return _make_idle_dispatch_connection()

    with patch("main.get_db_connection", side_effect=flaky_get_db_connection):
        with pytest.raises(RuntimeError):
            main.dispatch_uploader_tasks()
        assert main._dispatch_run_lock.locked() is False
        assert main.dispatch_uploader_tasks() is True

    assert calls == 2


def test_uploader_dispatch_skips_when_db_lock_held_by_other_process(fake_dispatch_db_lock):
    from conftest import make_fake_dispatch_lock_connection

    lock_connection = make_fake_dispatch_lock_connection(acquired=0)
    fake_dispatch_db_lock.side_effect = None
    fake_dispatch_db_lock.return_value = lock_connection

    with patch("main.get_db_connection") as mock_get_db_connection:
        assert main.dispatch_uploader_tasks() is False

    mock_get_db_connection.assert_not_called()
    lock_connection.close.assert_called_once_with()
    assert main._dispatch_run_lock.locked() is False


def test_uploader_dispatch_releases_db_lock_on_same_connection_after_exception(fake_dispatch_db_lock):
    from conftest import make_fake_dispatch_lock_connection

    lock_connection = make_fake_dispatch_lock_connection(acquired=1)
    fake_dispatch_db_lock.side_effect = None
    fake_dispatch_db_lock.return_value = lock_connection
    lock_cursor = lock_connection.cursor.return_value.__enter__.return_value

    with patch("main.get_db_connection", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            main.dispatch_uploader_tasks()

    lock_sql = [call.args[0] for call in lock_cursor.execute.call_args_list]
    assert any("GET_LOCK" in sql for sql in lock_sql)
    assert any("RELEASE_LOCK" in sql for sql in lock_sql)
    lock_connection.close.assert_called_once_with()
    assert main._dispatch_run_lock.locked() is False


def test_report_status_ignores_stale_dispatch_without_mutating_tasks(mock_db):
    mock_db.fetchone.return_value = {
        "role": "uploader",
        "current_dispatch_id": "a" * 32,
        "current_worker_node": "worker-node-1",
    }
    payload = {
        "account_name": "baicai_1@1",
        "status": "completed",
        "dispatch_id": "b" * 32,
        "worker_node": "worker-node-1",
        "successful_files": ["movie/stale.mkv"],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "stale_dispatch"}
    assert len(mock_db.execute.call_args_list) == 2
    assert "FROM file_tasks" in mock_db.execute.call_args_list[0].args[0]
    assert "FOR UPDATE" in mock_db.execute.call_args_list[0].args[0]
    assert "account_used=%s" not in mock_db.execute.call_args_list[0].args[0]
    assert "FROM drive_accounts" in mock_db.execute.call_args_list[1].args[0]
    assert "FOR UPDATE" in mock_db.execute.call_args_list[1].args[0]


def test_report_status_uploading_without_active_dispatch_task_cancels_worker(mock_db):
    dispatch_id = "f" * 32
    mock_db.fetchone.side_effect = [
        None,
        {
            "role": "uploader",
            "current_dispatch_id": dispatch_id,
            "current_worker_node": "worker-node-1",
        },
    ]
    payload = {
        "account_name": "baicai_1@1",
        "status": "uploading",
        "dispatch_id": dispatch_id,
        "worker_node": "worker-node-1",
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "stale_dispatch"}
    assert not any(
        "SET updated_at = UTC_TIMESTAMP()" in call.args[0]
        for call in mock_db.execute.call_args_list
    )
    release_sql = next(
        call.args[0]
        for call in mock_db.execute.call_args_list
        if "SET dispatch_locked_until=NULL" in call.args[0]
    )
    assert "current_dispatch_id=%s AND current_worker_node=%s" in release_sql


def test_report_status_uploading_accepts_heartbeat_with_last_successful_file(mock_db):
    dispatch_id = "9" * 32
    mock_db.fetchone.return_value = {
        "role": "uploader",
        "current_dispatch_id": dispatch_id,
        "current_worker_node": "worker-node-1",
    }
    payload = {
        "account_name": "baicai_1@1",
        "status": "uploading",
        "dispatch_id": dispatch_id,
        "worker_node": "worker-node-1",
        "successful_files": ["movie/last.mkv"],
    }

    with patch(
        "main.refresh_uploader_heartbeat", return_value=(0, 0, 1)
    ):
        response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"status": "success"}


def test_report_status_matching_dispatch_scopes_every_task_update(mock_db):
    dispatch_id = "c" * 32
    mock_db.fetchone.return_value = {
        "role": "uploader",
        "current_dispatch_id": dispatch_id,
        "current_worker_node": "worker-node-1",
    }
    payload = {
        "account_name": "baicai_1@1",
        "status": "completed",
        "dispatch_id": dispatch_id,
        "worker_node": "worker-node-1",
        "successful_files": ["movie/current.mkv"],
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    task_updates = [
        call for call in mock_db.execute.call_args_list
        if "UPDATE file_tasks" in call.args[0]
    ]
    assert task_updates
    assert all("dispatch_id=%s" in call.args[0] for call in task_updates)
    assert any(dispatch_id in call.args[1] for call in task_updates)
    release_sql = next(call.args[0] for call in mock_db.execute.call_args_list if "current_dispatch_id=NULL" in call.args[0])
    assert "current_dispatch_id=%s AND current_worker_node=%s" in release_sql


def test_report_status_cleaner_heartbeat_renews_lease_and_worker(mock_db):
    dispatch_id = "d" * 32
    mock_db.fetchone.return_value = {
        "role": "cleaner",
        "current_dispatch_id": dispatch_id,
        "current_worker_node": "worker-node-2",
    }
    payload = {
        "account_name": "baicai-1@1-3",
        "status": "uploading",
        "dispatch_id": dispatch_id,
        "worker_node": "worker-node-2",
    }

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call.args[0] for call in mock_db.execute.call_args_list]
    assert any("SET dispatch_locked_until = DATE_ADD" in sql and "role=%s" in sql for sql in sql_calls)
    assert any("UPDATE worker_registry SET last_active=UTC_TIMESTAMP() WHERE node_name=%s" in sql for sql in sql_calls)
    task_lock = next(
        call for call in mock_db.execute.call_args_list
        if "FROM file_tasks" in call.args[0] and "FOR UPDATE" in call.args[0]
    )
    assert "account_used" not in task_lock.args[0]
    assert task_lock.args[1] == (dispatch_id,)


def test_dispatch_uploader_payload_has_dispatch_and_source_root():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [{
            "id": 1,
            "file_path": "movie/test.mkv",
            "source_base_dir": "/cache/links",
            "size_bytes": 123,
            "source_mtime": 456.5,
            "upload_method": "move",
        }],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]
    response = MagicMock()
    response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), \
         patch("main.httpx.post", return_value=response) as mock_post, \
         patch("main._new_dispatch_id", return_value="e" * 32):
        main.dispatch_uploader_tasks()

    payload = json.loads(mock_post.call_args.kwargs["content"])
    assert payload["dispatch_id"] == "e" * 32
    assert payload["worker_node"] == "worker-node-1"
    assert payload["source_base_dirs"] == ["/cache/links"]
    assert payload["expected_sizes"] == [123]
    assert payload["expected_mtimes"] == [456.5]
    assert payload["remote_dir"] == "LongEmby-B"
    node_query = next(call.args[0] for call in mock_cursor.execute.call_args_list if "FROM worker_registry" in call.args[0] and "node_name, api_url" in call.args[0])
    assert "protocol_version >= 2" in node_query


def test_dispatch_uploader_timeout_preserves_uncertain_dispatch():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [{"node_name": "worker-node-1", "api_url": "http://worker:8001/task/upload"}],
        [{"account_name": "baicai_1@1"}],
        [{"id": 1, "file_path": "movie/test.mkv", "source_base_dir": "/cache/downloads", "upload_method": "move"}],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    with patch("main.get_db_connection", return_value=mock_connection), \
         patch("main.httpx.post", side_effect=httpx.ReadTimeout("uncertain")), \
         patch("main._new_dispatch_id", return_value="f" * 32):
        main.dispatch_uploader_tasks()

    sql_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert any("SET status='uploading'" in sql and "dispatch_id=%s" in sql for sql in sql_calls)
    assert not any("SET status='pending', dispatch_id=NULL" in sql and "WHERE dispatch_id=%s" in sql for sql in sql_calls)
    assert not any("current_dispatch_id=NULL" in sql for sql in sql_calls)


def test_dispatch_cleaner_rechecks_uploads_after_acquiring_lock():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.side_effect = [
        [],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
        [{"id": 1, "file_path": "movie/test.mkv", "source_base_dir": "/cache/downloads"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 2},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
        {"has_uploading": 1},
    ]

    with patch("main.get_db_connection", return_value=mock_connection), \
         patch("main.httpx.post") as mock_post, \
         patch("main._new_dispatch_id", return_value="1" * 32):
        main.dispatch_cleaner_tasks()

    mock_post.assert_not_called()
    sql_calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert not any("SET ft.dispatch_id=%s" in sql for sql in sql_calls)
    assert any("current_dispatch_id=NULL" in sql and "current_dispatch_id=%s" in sql for sql in sql_calls)


def test_report_files_accepts_valid_node_hmac(mock_db):
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1:8001/task/upload",
        "protocol_version": 2,
        "files": [{"path": " movie.mkv ", "source_base_dir": "/cache/downloads"}],
    }
    raw_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    timestamp = "1700000000"
    token = "node-secret-token-123"
    headers = {
        "Content-Type": "application/json",
        "X-Baicai-Node": "worker-node-1",
        "X-Baicai-Timestamp": timestamp,
        "X-Baicai-Signature": main._node_signature(token, timestamp, raw_body),
    }

    with patch.object(main, "WORKER_TOKEN_MAP", {"worker-node-1": token}), patch("main.time.time", return_value=1700000000):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 200
    assert mock_db.executemany.call_args.args[1][0][0] == " movie.mkv "
    assert mock_db.executemany.call_args.args[1][0][1] == "/cache/downloads"


def _signed_report_files_request(node_name: str, token: str, payload: dict) -> tuple[bytes, dict]:
    raw_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    timestamp = "1700000000"
    return raw_body, {
        "Content-Type": "application/json",
        "X-Baicai-Node": node_name,
        "X-Baicai-Timestamp": timestamp,
        "X-Baicai-Signature": main._node_signature(token, timestamp, raw_body),
    }


def _history_sync_payload(node_name: str) -> dict:
    return {
        "worker_node": node_name,
        "worker_api_url": "http://127.0.0.1:8001/task/upload",
        "protocol_version": 2,
        "files": ["movie/already-on-target.mkv"],
        "is_history_sync": True,
        "upload_method": "move",
    }


def test_report_files_rejects_history_sync_from_ordinary_signed_node(mock_db):
    token = "node-secret-token-123"
    raw_body, headers = _signed_report_files_request(
        "worker-node-1", token, _history_sync_payload("worker-node-1")
    )

    with patch.object(main, "WORKER_TOKEN_MAP", {"worker-node-1": token}), patch(
        "main.time.time", return_value=1700000000
    ):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 403
    mock_db.execute.assert_not_called()
    mock_db.executemany.assert_not_called()


def test_report_files_accepts_history_sync_from_init_scanner(mock_db):
    token = "init-scanner-token-123"
    raw_body, headers = _signed_report_files_request(
        "Init_Scanner", token, _history_sync_payload("Init_Scanner")
    )

    with patch.object(main, "WORKER_TOKEN_MAP", {"Init_Scanner": token}), patch(
        "main.time.time", return_value=1700000000
    ):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 200
    assert mock_db.executemany.call_args.args[1][0][4] == "completed"


def test_report_files_history_sync_node_allowlist_is_configurable(mock_db):
    token = "history-node-token-123"
    raw_body, headers = _signed_report_files_request(
        "history-node-2", token, _history_sync_payload("history-node-2")
    )

    with patch.object(main, "WORKER_TOKEN_MAP", {"history-node-2": token}), patch.object(
        main, "HISTORY_SYNC_NODE_NAMES", frozenset({"Init_Scanner", "history-node-2"}), create=True
    ), patch("main.time.time", return_value=1700000000):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 200


def test_report_files_keeps_legacy_admin_bearer_history_sync_compatibility(mock_db):
    response = client.post(
        "/api/report_files",
        json=_history_sync_payload("legacy-init-sync"),
        headers=auth_headers(),
    )

    assert response.status_code == 200
    assert mock_db.executemany.call_args.args[1][0][4] == "completed"


def test_admin_bearer_with_non_ascii_token_returns_401(mock_db):
    response = client.get(
        "/api/upload_usage",
        headers={"Authorization": "Bearer tést-token".encode("utf-8")},
    )

    assert response.status_code == 401
    mock_db.execute.assert_not_called()


def test_legacy_node_bearer_with_non_ascii_token_returns_401(mock_db):
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1:8001/task/upload",
        "files": [],
    }

    response = client.post(
        "/api/report_files",
        json=payload,
        headers={"Authorization": "Bearer tést-token".encode("utf-8")},
    )

    assert response.status_code == 401
    mock_db.execute.assert_not_called()


def test_node_hmac_with_non_ascii_signature_returns_401(mock_db):
    raw_body = json.dumps(
        {
            "worker_node": "worker-node-1",
            "worker_api_url": "http://worker-node-1:8001/task/upload",
            "files": [],
        },
        separators=(",", ":"),
    ).encode()
    headers = {
        "Content-Type": "application/json",
        "X-Baicai-Node": "worker-node-1",
        "X-Baicai-Timestamp": "1700000000",
        "X-Baicai-Signature": ("é" * 64).encode("utf-8"),
    }

    with patch.object(main, "WORKER_TOKEN_MAP", {"worker-node-1": "node-secret-token-123"}), patch(
        "main.time.time", return_value=1700000000
    ):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 401
    mock_db.execute.assert_not_called()


def test_report_files_rejects_expired_node_hmac(mock_db):
    payload = {
        "worker_node": "worker-node-1",
        "worker_api_url": "http://worker-node-1:8001/task/upload",
        "protocol_version": 2,
        "files": [],
    }
    raw_body = json.dumps(payload, separators=(",", ":")).encode()
    token = "node-secret-token-123"
    headers = {
        "Content-Type": "application/json",
        "X-Baicai-Node": "worker-node-1",
        "X-Baicai-Timestamp": "1699999000",
        "X-Baicai-Signature": main._node_signature(token, "1699999000", raw_body),
    }

    with patch.object(main, "WORKER_TOKEN_MAP", {"worker-node-1": token}), patch(
        "main.time.time", return_value=1700000000
    ):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 401
    mock_db.execute.assert_not_called()


def test_encode_worker_request_uses_node_hmac_without_bearer():
    token = "node-secret-token-123"
    with patch.object(main, "WORKER_TOKEN_MAP", {"worker-node-1": token}), patch("main.time.time", return_value=1700000000):
        raw_body, headers = main.encode_worker_request("worker-node-1", {"task_type": "upload"})

    assert "Authorization" not in headers
    assert headers["X-Baicai-Node"] == "worker-node-1"
    assert headers["X-Baicai-Signature"] == main._node_signature(token, "1700000000", raw_body)

def test_clean_success_completes_dispatch_batch_without_file_list(mock_db):
    dispatch_id = "clean-dispatch-1"
    payload = {
        "account_name": "baicai-1@1-3",
        "status": "clean_success",
        "successful_files": [],
        "dispatch_id": dispatch_id,
        "worker_node": "worker-node-1",
    }
    mock_db.fetchone.side_effect = [
        {"id": 1},
        {
            "role": "cleaner",
            "current_dispatch_id": dispatch_id,
            "current_worker_node": "worker-node-1",
        },
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    batch_call = next(
        call
        for call in mock_db.execute.call_args_list
        if "JOIN drive_accounts da" in call.args[0]
        and "SET ft.status='completed'" in call.args[0]
    )
    assert "ft.dispatch_id=%s" in batch_call.args[0]
    assert batch_call.args[1] == ["baicai-1@1-3", dispatch_id]


def test_clean_success_accepts_legacy_file_details_then_completes_remaining_batch(mock_db):
    payload = {
        "account_name": "baicai-1@1-3",
        "status": "clean_success",
        "successful_files": ["movie/a.mkv"],
    }
    mock_db.fetchone.side_effect = [
        {"role": "cleaner"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("JOIN drive_accounts da" in sql and "ft.file_path IN" in sql for sql in sql_calls)
    assert any("JOIN drive_accounts da" in sql and "SET ft.status='completed'" in sql for sql in sql_calls)


def test_cleaner_uploading_with_successful_files_updates_reported_batch(mock_db):
    payload = {
        "account_name": "baicai-1@1-3",
        "status": "uploading",
        "successful_files": ["movie/a.mkv", "movie/b.mkv"],
    }
    mock_db.fetchone.side_effect = [
        {"role": "cleaner"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("JOIN drive_accounts da" in sql and "ft.file_path IN" in sql for sql in sql_calls)
    assert any("UPDATE worker_registry wr" in sql for sql in sql_calls)


def test_update_cleaner_tasks_by_files_batches_large_updates():
    cursor = MagicMock()

    with patch.object(main, "CLEANER_UPDATE_SQL_CHUNK_SIZE", 2):
        main.update_cleaner_tasks_by_files(cursor, "baicai-1@1-3", ["movie/a.mkv", "movie/b.mkv", "movie/c.mkv"], "completed")

    assert cursor.execute.call_count == 2
    first_params = cursor.execute.call_args_list[0][0][1]
    second_params = cursor.execute.call_args_list[1][0][1]
    assert first_params == ["completed", "baicai-1@1-3", "movie/a.mkv", "movie/b.mkv"]
    assert second_params == ["completed", "baicai-1@1-3", "movie/c.mkv"]


def test_get_upload_usage_returns_beijing_time_fields(mock_db):
    mock_db.fetchall.return_value = [
        {
            "account_name": "baicai_1@1",
            "role": "uploader",
            "is_active": 1,
            "uploaded_files": 2,
            "uploaded_bytes": 2 * 1024 ** 3,
            "last_uploaded_at_utc": "2026-06-10 01:00:00",
            "last_uploaded_at_bj": "2026-06-10 09:00:00",
            "dispatch_locked_until": "2099-01-01 02:00:00",
            "dispatch_cooldown_until": None,
            "quota_blocked_until": "2099-01-01 03:00:00",
        }
    ]

    response = client.get("/api/upload_usage?hours=24", headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {
        "window_hours": 24,
        "timezone": "Asia/Shanghai",
        "total_files": 2,
        "total_bytes": 2 * 1024 ** 3,
        "total_gib": 2.0,
        "accounts": [
            {
                "account_name": "baicai_1@1",
                "role": "uploader",
                "is_active": 1,
                "uploaded_files": 2,
                "uploaded_bytes": 2 * 1024 ** 3,
                "uploaded_gib": 2.0,
                "last_uploaded_at_utc": "2026-06-10 01:00:00",
                "last_uploaded_at_bj": "2026-06-10 09:00:00",
                "dispatch_locked_until_utc": "2099-01-01 02:00:00",
                "dispatch_locked_until_bj": "2099-01-01 10:00:00",
                "dispatch_cooldown_until_utc": None,
                "dispatch_cooldown_until_bj": None,
                "quota_blocked_until_utc": "2099-01-01 03:00:00",
                "quota_blocked_until_bj": "2099-01-01 11:00:00",
                "unlock_at_utc": "2099-01-01 03:00:00",
                "unlock_at_bj": "2099-01-01 11:00:00",
                "active_locks": ["dispatch_locked_until", "quota_blocked_until"],
                "is_available_now": False,
            }
        ],
    }
    sql = mock_db.execute.call_args[0][0]
    assert "FROM drive_accounts da" in sql
    assert "LEFT JOIN (" in sql
    assert "dispatch_locked_until" in sql


def test_get_upload_usage_includes_zero_usage_accounts_and_unlock_time(mock_db):
    mock_db.fetchall.return_value = [
        {
            "account_name": "baicai_2@1",
            "role": "uploader",
            "is_active": 1,
            "uploaded_files": 0,
            "uploaded_bytes": 0,
            "last_uploaded_at_utc": None,
            "last_uploaded_at_bj": None,
            "dispatch_locked_until": None,
            "dispatch_cooldown_until": None,
            "quota_blocked_until": "2099-01-01 00:00:00",
        }
    ]

    response = client.get("/api/upload_usage?hours=24", headers=auth_headers())

    assert response.status_code == 200
    account = response.json()["accounts"][0]
    assert account["uploaded_files"] == 0
    assert account["uploaded_bytes"] == 0
    assert account["unlock_at_utc"] == "2099-01-01 00:00:00"
    assert account["unlock_at_bj"] == "2099-01-01 08:00:00"
    assert account["active_locks"] == ["quota_blocked_until"]
    assert account["is_available_now"] is False


def test_get_daily_upload_usage_groups_by_beijing_date(mock_db):
    mock_db.fetchall.return_value = [
        {
            "upload_date_bj": "2026-06-10",
            "account_name": "baicai_2@1",
            "uploaded_files": 1,
            "uploaded_bytes": 1024 ** 3,
            "first_uploaded_at_utc": "2026-06-09 18:00:00",
            "last_uploaded_at_utc": "2026-06-09 18:00:00",
            "first_uploaded_at_bj": "2026-06-10 02:00:00",
            "last_uploaded_at_bj": "2026-06-10 02:00:00",
        }
    ]

    response = client.get("/api/upload_usage/daily?days=3", headers=auth_headers())

    assert response.status_code == 200
    assert response.json()["days"] == 3
    assert response.json()["timezone"] == "Asia/Shanghai"
    assert response.json()["total_gib"] == 1.0
    assert response.json()["rows"][0]["upload_date_bj"] == "2026-06-10"
    sql = mock_db.execute.call_args[0][0]
    assert "GROUP BY upload_date_bj, account_name" in sql


class RoutedCursor:
    """按 SQL 片段应答的游标替身：被跳过的节点不再查询时，fetch 顺序不会错位。"""

    def __init__(self, routes):
        self.routes = routes
        self.rowcount = 1
        self.executed = []
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._rows = next((handler(params) for fragment, handler in self.routes if fragment in sql), [])

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


def _uploader_dispatch_cursor(registry_rows):
    """每个节点分到独立账号组和一个待办文件；registry_rows 模拟 worker_registry 中的历史地址。"""
    accounts = {row["node_name"]: f"baicai_{index}@1" for index, row in enumerate(registry_rows, start=1)}
    return RoutedCursor([
        ("SELECT node_name, api_url FROM worker_registry", lambda params: registry_rows),
        ("COUNT(DISTINCT account_used) as active_count", lambda params: [{"active_count": 0}]),
        ("AS has_preferred_pending", lambda params: [{"account_name": accounts[params[0]]}]),
        ("GET_LOCK", lambda params: [{"acquired": 1}]),
        ("SELECT id, file_path, source_base_dir, size_bytes", lambda params: [{
            "id": list(accounts).index(params[0]) + 1,
            "file_path": f"movie/{params[0]}.mkv",
            "source_base_dir": "/cache/downloads",
            "size_bytes": 1,
            "source_mtime": 1.0,
            "upload_method": "move",
        }]),
    ])


def _cleaner_dispatch_cursor(registry_urls):
    """每个节点各有一组配额耗尽的 staged 文件；registry_urls 模拟 worker_registry 中的历史地址。"""
    staged_rows = [
        {"account_used": f"baicai_{index}@1", "worker_node": node_name}
        for index, node_name in enumerate(registry_urls, start=1)
    ]
    return RoutedCursor([
        ("AS staged_count", lambda params: staged_rows),
        ("AS blocked_uploaders", lambda params: [{"total_uploaders": 1, "blocked_uploaders": 1}]),
        ("SELECT cleaner_mapping FROM drive_accounts", lambda params: [
            {"cleaner_mapping": f"{params[0].split('@')[0]}-cleaner@1"}
        ]),
        ("SELECT ft.id, ft.file_path, ft.source_base_dir", lambda params: [
            {"id": 1, "file_path": f"movie/{params[1]}.mkv", "source_base_dir": "/cache/downloads"}
        ]),
        ("SELECT api_url FROM worker_registry", lambda params: [{"api_url": registry_urls[params[0]]}]),
    ])


def _run_dispatch_and_collect_targets(dispatch, cursor, url_map):
    """返回每次出站 POST 的 (目标 URL, payload.worker_node)。"""
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    response = MagicMock()
    response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=connection), \
         patch.object(main, "WORKER_API_URL_MAP", url_map), \
         patch.object(main, "CLEANER_DISPATCH_ENABLED", True), \
         patch("main.httpx.post", return_value=response) as mock_post:
        dispatch()

    return [(call.args[0], json.loads(call.kwargs["content"])["worker_node"]) for call in mock_post.call_args_list]


def _updates_touching_node(cursor, node_name):
    return [sql for sql, params in cursor.executed if "UPDATE" in sql and params and node_name in params]


STALE_REGISTRY_URL = "http://10.0.0.9:8001/task/upload"
MAPPED_URL = "http://worker-node-1:8001/task/upload"


def test_dispatch_uploader_tasks_posts_to_mapped_url_instead_of_stale_registry_url():
    cursor = _uploader_dispatch_cursor([{"node_name": "worker-node-1", "api_url": STALE_REGISTRY_URL}])

    targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, {"worker-node-1": MAPPED_URL})

    assert targets == [(MAPPED_URL, "worker-node-1")]


def test_dispatch_uploader_tasks_skips_node_missing_from_url_map(caplog):
    cursor = _uploader_dispatch_cursor([
        {"node_name": "legacy-node", "api_url": "http://legacy-node:8001/task/upload"},
        {"node_name": "worker-node-1", "api_url": STALE_REGISTRY_URL},
    ])

    with caplog.at_level(logging.WARNING):
        targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, {"worker-node-1": MAPPED_URL})

    assert targets == [(MAPPED_URL, "worker-node-1")]
    # 跳过发生在抢占账号租约与锁定任务之前，名单外节点不会留下需要回滚的状态。
    assert _updates_touching_node(cursor, "legacy-node") == []
    assert any(r.levelno == logging.WARNING and "legacy-node" in r.getMessage() for r in caplog.records)


def test_dispatch_uploader_tasks_skips_invalid_registry_url_and_continues(caplog):
    cursor = _uploader_dispatch_cursor([
        {"node_name": "bad-port-node", "api_url": "http://bad-port-node:notaport/task/upload"},
        {"node_name": "userinfo-node", "api_url": "http://admin:s3cret@userinfo-node:8001/task/upload"},
        {"node_name": "worker-node-1", "api_url": STALE_REGISTRY_URL},
    ])

    with caplog.at_level(logging.WARNING):
        targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, {})

    assert targets == [(STALE_REGISTRY_URL, "worker-node-1")]
    assert _updates_touching_node(cursor, "bad-port-node") == []
    assert _updates_touching_node(cursor, "userinfo-node") == []
    assert any(r.levelno == logging.WARNING and "bad-port-node" in r.getMessage() for r in caplog.records)
    assert "s3cret" not in caplog.text


def test_dispatch_uploader_tasks_uses_registry_url_when_url_map_empty():
    cursor = _uploader_dispatch_cursor([{"node_name": "worker-node-1", "api_url": STALE_REGISTRY_URL}])

    targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, {})

    assert targets == [(STALE_REGISTRY_URL, "worker-node-1")]


def test_dispatch_cleaner_tasks_posts_to_mapped_url_instead_of_stale_registry_url():
    cursor = _cleaner_dispatch_cursor({"worker-node-1": STALE_REGISTRY_URL})

    targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, {"worker-node-1": MAPPED_URL})

    assert targets == [(MAPPED_URL, "worker-node-1")]


def test_dispatch_cleaner_tasks_skips_node_missing_from_url_map(caplog):
    cursor = _cleaner_dispatch_cursor({
        "legacy-node": "http://legacy-node:8001/task/upload",
        "worker-node-1": STALE_REGISTRY_URL,
    })

    with caplog.at_level(logging.WARNING):
        targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, {"worker-node-1": MAPPED_URL})

    assert targets == [(MAPPED_URL, "worker-node-1")]
    # 名单外节点不获取 cleaner 租约、不绑定 staged 批次。
    assert _updates_touching_node(cursor, "legacy-node") == []
    assert any(r.levelno == logging.WARNING and "legacy-node" in r.getMessage() for r in caplog.records)


def test_dispatch_cleaner_tasks_skips_invalid_registry_url_and_continues(caplog):
    cursor = _cleaner_dispatch_cursor({
        "bad-port-node": "http://bad-port-node:notaport/task/upload",
        "userinfo-node": "http://admin:s3cret@userinfo-node:8001/task/upload",
        "worker-node-1": STALE_REGISTRY_URL,
    })

    with caplog.at_level(logging.WARNING):
        targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, {})

    assert targets == [(STALE_REGISTRY_URL, "worker-node-1")]
    assert _updates_touching_node(cursor, "bad-port-node") == []
    assert _updates_touching_node(cursor, "userinfo-node") == []
    assert any(r.levelno == logging.WARNING and "bad-port-node" in r.getMessage() for r in caplog.records)
    assert "s3cret" not in caplog.text


def test_dispatch_cleaner_tasks_uses_registry_url_when_url_map_empty():
    cursor = _cleaner_dispatch_cursor({"worker-node-1": STALE_REGISTRY_URL})

    targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, {})

    assert targets == [(STALE_REGISTRY_URL, "worker-node-1")]


UNSAFE_API_URLS = [
    ("http://admin:s3cret@worker-node-1:8001/task/upload", "用户信息"),
    ("http://admin@worker-node-1:8001/task/upload", "用户信息"),
    ("http://@worker-node-1:8001/task/upload", "用户信息"),
    ("http:///task/upload", "主机名"),
    ("http://:8001/task/upload", "主机名"),
    ("http://worker-node-1:notaport/task/upload", "端口"),
    ("http://worker-node-1:99999/task/upload", "端口"),
    ("http://worker-node-1:0/task/upload", "端口"),
    ("http://worker-node-1:+8001/task/upload", "端口"),
    ("ftp://worker-node-1/task/upload", "http:// 或 https://"),
    ("http://[fd00::1/task/upload", "格式非法"),
    # urlsplit 会静默删除 \t\n，不先拒绝会让校验对象与实际出站地址不一致。
    ("http://worker-node-1\n:8001/task/upload", "空白或控制字符"),
    ("http://worker node/task/upload", "空白或控制字符"),
]


@pytest.mark.parametrize(("api_url", "message"), UNSAFE_API_URLS)
def test_validate_api_url_rejects_unsafe_urls(api_url, message):
    with pytest.raises(ValueError, match=message):
        main.validate_api_url(api_url)


@pytest.mark.parametrize(
    "api_url",
    [
        "http://worker-node-1:8001/task/upload",
        "https://worker-node-1.example.com/task/upload",
        "http://10.0.0.5:8001/task/upload",
        "http://172.18.0.3:65535/task/upload",
        "http://[fd00::1]:8001/task/upload",
        "http://worker-node-1:8001/task/upload?tag=a@b",
    ],
)
def test_validate_api_url_accepts_internal_hosts_and_custom_ports(api_url):
    assert main.validate_api_url(api_url) == api_url


@pytest.mark.parametrize(
    ("api_url", "message"),
    [
        ("http://admin:s3cret@worker-node-1:8001/task/upload", "用户信息"),
        ("http:///task/upload", "主机名"),
        ("http://worker-node-1:notaport/task/upload", "端口"),
        ("ftp://worker-node-1/task/upload", "http:// 或 https://"),
    ],
)
def test_report_files_rejects_unsafe_worker_api_url(mock_db, api_url, message):
    payload = {"worker_node": "worker-node-1", "worker_api_url": api_url, "files": ["movie/test1.mkv"]}

    response = client.post("/api/report_files", json=payload, headers=auth_headers())

    assert response.status_code == 422
    assert any(message in error["msg"] for error in response.json()["detail"])
    mock_db.execute.assert_not_called()


INVALID_STALE_USERINFO_URL = "http://admin:s3cret@worker-node-1:8001/task/upload"


def test_dispatch_uploader_tasks_prefers_url_map_over_invalid_stale_url():
    """白名单是权威来源：历史地址即使不合法，也不应阻止发往映射地址。"""
    cursor = _uploader_dispatch_cursor([{"node_name": "worker-node-1", "api_url": INVALID_STALE_USERINFO_URL}])

    targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, {"worker-node-1": MAPPED_URL})

    assert targets == [(MAPPED_URL, "worker-node-1")]


def test_dispatch_cleaner_tasks_prefers_url_map_over_invalid_stale_url():
    cursor = _cleaner_dispatch_cursor({"worker-node-1": INVALID_STALE_USERINFO_URL})

    targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, {"worker-node-1": MAPPED_URL})

    assert targets == [(MAPPED_URL, "worker-node-1")]


def test_load_worker_api_url_map_strips_keys_and_values(monkeypatch):
    monkeypatch.setenv(
        "WORKER_API_URL_MAP",
        '{" worker-node-1 ": " http://worker-node-1:8001/task/upload "}',
    )

    parsed = main._load_worker_api_url_map()

    assert parsed == {"worker-node-1": "http://worker-node-1:8001/task/upload"}


def test_load_worker_api_url_map_rejects_blank_node_name(monkeypatch):
    monkeypatch.setenv("WORKER_API_URL_MAP", '{"  ": "http://worker-node-1:8001/task/upload"}')

    with pytest.raises(RuntimeError, match="node_name"):
        main._load_worker_api_url_map()


def test_validate_worker_api_url_map_entries_rejects_invalid_entry(monkeypatch):
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {"worker-node-1": "ftp://worker-node-1/task/upload"})

    with pytest.raises(RuntimeError, match="worker-node-1"):
        main._validate_worker_api_url_map_entries()


def test_report_status_completed_for_v2_worker_does_not_mark_whole_batch(mock_db, caplog):
    """协议 v2 节点逐文件回报时，未列出的任务必须保持 uploading，不能被整批标成功。"""
    import logging

    payload = {
        "account_name": "baicai_1@1",
        "status": "completed",
        "successful_files": ["movie/test1.mp4"],
    }

    with patch.object(main, "get_worker_protocol_version", return_value=2), \
         patch.object(main, "count_uploading_tasks", return_value=1), \
         patch.object(main, "mark_all_uploading_tasks_success") as mock_mark_all:
        with caplog.at_level(logging.WARNING):
            response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    mock_mark_all.assert_not_called()
    assert any("保持 uploading" in record.getMessage() for record in caplog.records)


def test_report_status_completed_for_v1_worker_keeps_batch_semantics(mock_db, caplog):
    """旧协议节点不发送逐文件明细，保留整批语义并在日志里说明这是兼容路径。"""
    import logging

    payload = {
        "account_name": "baicai_1@1",
        "status": "completed",
        "successful_files": [],
    }

    with patch.object(main, "get_worker_protocol_version", return_value=1), \
         patch.object(main, "mark_all_uploading_tasks_success") as mock_mark_all:
        with caplog.at_level(logging.WARNING):
            response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    mock_mark_all.assert_called_once()
    assert any("协议版本 <2" in record.getMessage() for record in caplog.records)


def test_normalized_path_expression_has_no_backslash_escapes():
    """SQL 侧正则不能依赖反斜杠：MySQL 字面量会吃掉单层转义，模式会退化。

    CI 不跑真库用例，这条纯字符串断言用来在无数据库环境下兜住同类回归。
    """
    expression = main._normalized_path_expression("file_path")

    assert "\\" not in expression
    assert expression == "TRIM(LEADING '/' FROM REGEXP_REPLACE(file_path, '^([.]/)+', ''))"


def test_normalize_relative_path_strips_leading_dot_slash_and_slash():
    assert main.normalize_relative_path("./movie/dot.mkv") == "movie/dot.mkv"
    assert main.normalize_relative_path("/movie/slash.mkv") == "movie/slash.mkv"
    assert main.normalize_relative_path("movie/plain.mkv") == "movie/plain.mkv"


def test_mark_successful_uploads_keeps_concurrency_guards():
    """按 id 更新的同时必须保留状态/账号/dispatch 兜底，避免 check-then-act 覆盖并发变更。"""
    cursor = MagicMock()
    cursor.fetchall.return_value = [{"id": 7, "file_path": "movie/a.mkv"}]

    main.mark_successful_uploads_by_files(
        cursor, "baicai_1@1", "staged", ["movie/a.mkv"], "d" * 32
    )

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    update_sql = next(sql for sql in executed if "UPDATE file_tasks" in sql)
    assert "id IN (" in update_sql
    assert "status='uploading'" in update_sql
    assert "account_used=%s" in update_sql
    assert "dispatch_id=%s" in update_sql

    event_sql = next(sql for sql in executed if "INSERT IGNORE INTO account_upload_events" in sql)
    assert "status='uploading'" in event_sql
    assert "account_used=%s" in event_sql
