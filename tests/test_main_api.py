from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone
import errno
import httpx
import ipaddress
import json
import logging
import os
import sys
import threading
import time
import traceback
from urllib.parse import urlsplit

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

    # 不让真实后台调度线程越过 mock_db 的生命周期，只断言触发契约。
    with patch("main.trigger_dispatch_uploader_tasks_async") as trigger:
        response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    trigger.assert_called_once_with("403_quota:baicai_2@1")
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert any("dispatch_locked_until = NULL" in sql and "quota_blocked_until = DATE_ADD" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks SET status='pending'" in sql for sql in sql_calls)


def test_report_status_credential_invalid_disables_account(mock_db):
    payload = {
        "account_name": "baicai_big@1",
        "status": "credential_invalid",
        "successful_files": [],
    }

    with patch("main.trigger_dispatch_uploader_tasks_async") as trigger:
        response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    trigger.assert_called_once_with("credential_invalid:baicai_big@1")
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


PUBLIC_TEST_ADDRESS = "93.184.216.34"
FLAKY_NODE_URL = "http://flaky-node:8001/task/upload"
HEALTHY_NODE_URL = "http://worker-node-1:8001/task/upload"


def _resolver_failing_for(failing_host, error):
    """failing_host 的解析抛 error，其余主机解析到公网地址（不依赖 conftest 默认桩）。"""
    def _resolve(host, port, *args, **kwargs):
        if host == failing_host:
            raise error
        return [(2, 1, 6, "", (PUBLIC_TEST_ADDRESS, port))]

    return _resolve


def _account_lease_attempts(cursor):
    """acquire_account_lock 发出的 (账号, 节点)；派发前被跳过的节点不应出现在这里。"""
    return [
        (params[3], params[2])
        for sql, params in cursor.executed
        if "SET dispatch_locked_until = DATE_ADD" in sql
    ]


def _isolate_resolver_failure(monkeypatch):
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        _resolver_failing_for("flaky-node", OSError(errno.EMFILE, "Too many open files")),
    )
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    main._DNS_CACHE.clear()


def test_dispatch_uploader_tasks_skips_node_whose_resolver_raises_oserror_and_continues(monkeypatch, caplog):
    """解析线程抛出非 gaierror 的 OSError：只拒绝该节点，继续尝试排在后面的节点。"""
    _isolate_resolver_failure(monkeypatch)
    cursor = _uploader_dispatch_cursor([
        {"node_name": "flaky-node", "api_url": FLAKY_NODE_URL},
        {"node_name": "worker-node-1", "api_url": HEALTHY_NODE_URL},
    ])

    with caplog.at_level(logging.WARNING):
        targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, {})

    assert targets == [(HEALTHY_NODE_URL, "worker-node-1")]
    # 故障节点在获取账号租约之前就被跳过；本节点的任务锁定/回滚都在租约获取之后。
    assert _account_lease_attempts(cursor) == [("baicai_2@1", "worker-node-1")]
    assert any(r.levelno == logging.WARNING and "flaky-node" in r.getMessage() for r in caplog.records)


def test_dispatch_cleaner_tasks_skips_node_whose_resolver_raises_oserror_and_continues(monkeypatch, caplog):
    _isolate_resolver_failure(monkeypatch)
    cursor = _cleaner_dispatch_cursor({"flaky-node": FLAKY_NODE_URL, "worker-node-1": HEALTHY_NODE_URL})

    with caplog.at_level(logging.WARNING):
        targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, {})

    assert targets == [(HEALTHY_NODE_URL, "worker-node-1")]
    assert _account_lease_attempts(cursor) == [("baicai_2-cleaner@1", "worker-node-1")]
    assert any(r.levelno == logging.WARNING and "flaky-node" in r.getMessage() for r in caplog.records)


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

    with pytest.raises(RuntimeError, match="第 1 项的地址非法") as excinfo:
        main._validate_worker_api_url_map_entries()

    assert "worker-node-1" not in _formatted(excinfo.value)


def test_validate_worker_api_url_map_entries_traceback_has_no_raw_port(monkeypatch):
    monkeypatch.setattr(
        main,
        "WORKER_API_URL_MAP",
        {"SENTINEL_SECRET_NODE": "http://100.125.75.111:SENTINEL_SECRET_PORT/task/upload"},
    )

    with pytest.raises(RuntimeError, match="第 1 项的地址非法: worker_api_url 端口非法") as excinfo:
        main._validate_worker_api_url_map_entries()

    text = _formatted(excinfo.value)
    assert "SENTINEL" not in text
    assert "100.125.75.111" not in text


@pytest.mark.parametrize(
    "api_url",
    ["http://worker-node-1:SENTINEL_SECRET_PORT/task/upload", "http://SENTINEL_SECRET_HOST℀x:8001/task/upload"],
    ids=["bad-port", "nfkc-netloc"],
)
def test_validate_api_url_does_not_chain_raw_parser_errors(api_url):
    # urlsplit/parts.port 的原始 ValueError 带端口串或整个 netloc，不能挂在异常链上。
    with pytest.raises(ValueError) as excinfo:
        main.validate_api_url(api_url)

    assert excinfo.value.__cause__ is None and excinfo.value.__suppress_context__
    assert "SENTINEL" not in _formatted(excinfo.value)


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

    # 更新后再按「目标状态 + dispatch_id IS NULL」确认真正生效的 id，用量事件只按这些 id 记。
    verify_sql = next(sql for sql in executed if "SELECT id FROM file_tasks" in sql)
    assert "account_used=%s" in verify_sql
    assert "status=%s" in verify_sql
    assert "dispatch_id IS NULL" in verify_sql

    event_sql = next(sql for sql in executed if "INSERT IGNORE INTO account_upload_events" in sql)
    assert "account_used=%s" in event_sql


def test_normalize_relative_path_keeps_whitespace():
    """首尾空白属于文件名（Issue #2），归一化不能 strip，否则 CI 无库时也能拦住回归。"""
    assert main.normalize_relative_path(" leading/a.mkv ") == " leading/a.mkv "


def test_success_path_sql_contract_placeholder_counts():
    """SQL 契约：每条语句的 %s 数量必须与实参数一致，避免改参数顺序时静默错位。"""
    cursor = MagicMock()
    cursor.fetchall.return_value = [{"id": 7, "file_path": "movie/a.mkv"}]

    main.mark_successful_uploads_by_files(cursor, "baicai_1@1", "staged", ["movie/a.mkv"], "d" * 32)

    for call in cursor.execute.call_args_list:
        sql, params = call.args[0], call.args[1]
        assert sql.count("%s") == len(params), sql[:120]


def test_report_status_request_validates_already_present_files():
    from fastapi import HTTPException  # noqa: F401  （仅用于说明返回码约定）

    accepted = main.ReportStatusRequest(
        account_name="baicai_1@1",
        status="completed",
        successful_files=["movie/a.mkv"],
        already_present_files=["movie/a.mkv"],
    )
    assert accepted.already_present_files == ["movie/a.mkv"]

    with pytest.raises(ValueError):
        main.ReportStatusRequest(
            account_name="baicai_1@1",
            status="completed",
            already_present_files=["bad\npath.mkv"],
        )


def test_report_status_heartbeat_forwards_already_present_files(mock_db):
    """心跳（status=uploading）也会结算 successful_files，必须同样透传已存在清单，否则会被计费。"""
    dispatch_id = "7" * 32
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
        "successful_files": ["movie/a.mkv", "movie/b.mkv"],
        "already_present_files": ["movie/a.mkv"],
    }

    with patch("main.refresh_uploader_heartbeat", return_value=(0, 0, 1)), patch(
        "main.mark_successful_uploads_by_files"
    ) as mark_successful:
        response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    mark_successful.assert_called_once()
    assert mark_successful.call_args.kwargs["already_present_files"] == ["movie/a.mkv"]


def test_report_status_request_rejects_already_present_outside_successful_files():
    """Worker 是信任边界：已存在清单必须是成功清单的子集，否则等于凭空少计用量。"""
    with pytest.raises(ValueError):
        main.ReportStatusRequest(
            account_name="baicai_1@1",
            status="completed",
            successful_files=["movie/a.mkv"],
            already_present_files=["movie/b.mkv"],
        )


# ===== 出站地址策略（Issue #1） =====


@pytest.mark.parametrize(
    "api_url",
    [
        "http://127.0.0.1:8001/task/upload",
        "http://[::1]:8001/task/upload",
        "http://169.254.169.254/latest/meta-data/",
        "http://0.0.0.0:8001/task/upload",
        "http://224.0.0.1:8001/task/upload",
    ],
)
def test_verify_dispatch_target_rejects_loopback_and_metadata(api_url):
    with pytest.raises(ValueError):
        main.verify_dispatch_target(api_url)


def test_verify_dispatch_target_allows_public_address():
    assert main.verify_dispatch_target("http://8.8.8.8:8001/task/upload")


def test_verify_dispatch_target_rejects_dns_resolving_to_metadata(monkeypatch):
    """DNS 记录被指到元数据地址时必须拒绝（防 DNS 重绑定）。"""
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("169.254.169.254", 8001))],
    )
    with pytest.raises(ValueError):
        main.verify_dispatch_target("http://worker-node-1:8001/task/upload")


def test_verify_dispatch_target_allows_private_when_enabled(monkeypatch):
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", True)
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("172.18.0.5", 8001))],
    )
    assert main.verify_dispatch_target("http://worker-node-1:8001/task/upload")


def test_verify_dispatch_target_rejects_private_when_disabled(monkeypatch):
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", False)
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("172.18.0.5", 8001))],
    )
    with pytest.raises(ValueError):
        main.verify_dispatch_target("http://worker-node-1:8001/task/upload")


def test_verify_dispatch_target_honours_allowlisted_host(monkeypatch):
    """allowlist 必须真的起作用：关掉内网放行后，只有 allowlist 里的主机能过。"""
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", False)
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("172.18.0.5", 8001))],
    )
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())

    with pytest.raises(ValueError):
        main.verify_dispatch_target("http://metadata-proxy.local:8001/task/upload")

    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset({"metadata-proxy.local"}))
    assert main.verify_dispatch_target("http://metadata-proxy.local:8001/task/upload")


def test_verify_dispatch_target_rejects_unresolvable_host(monkeypatch):
    def _boom(*args, **kwargs):
        raise main.socket.gaierror("no such host")

    monkeypatch.setattr(main.socket, "getaddrinfo", _boom)
    with pytest.raises(ValueError):
        main.verify_dispatch_target("http://does-not-exist:8001/task/upload")


def test_verify_dispatch_target_rejects_host_when_resolver_raises_plain_oserror(monkeypatch):
    """解析线程里的非 gaierror OSError 同样 fail-closed 成 ValueError，原异常作为 cause 保留。"""
    error = OSError(errno.EMFILE, "Too many open files")
    assert not isinstance(error, main.socket.gaierror)

    def _fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(main.socket, "getaddrinfo", _fail)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    main._DNS_CACHE.clear()

    with pytest.raises(ValueError) as excinfo:
        main.verify_dispatch_target("http://worker-node-1:8001/task/upload")

    assert excinfo.value.__cause__ is error


def test_resolve_dispatch_api_url_propagates_programming_errors_from_resolver(monkeypatch):
    """只有 OSError 按"不可派发"处理；编程错误必须原样抛出，不能被吞成一次跳过。"""
    bug = TypeError("resolver called with wrong arguments")
    calls = []

    def _buggy(*args, **kwargs):
        calls.append(args)
        raise bug

    monkeypatch.setattr(main.socket, "getaddrinfo", _buggy)
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {})
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    main._DNS_CACHE.clear()

    with pytest.raises(TypeError) as excinfo:
        main.resolve_dispatch_api_url("worker-node-1", "http://worker-node-1:8001/task/upload")

    assert excinfo.value is bug
    assert calls


@pytest.mark.parametrize(
    ("error", "errno_text"),
    [
        (OSError(errno.EIO, "SENTINEL-STRERROR", "SENTINEL-FILENAME"), f"errno={errno.EIO}"),
        (OSError("SENTINEL-ONLY-ARG"), "errno=未知"),
        (OSError("SENTINEL-ERRNO", "SENTINEL-STRERROR"), "errno=未知"),
    ],
    ids=["numeric-errno", "missing-errno", "non-numeric-errno"],
)
def test_dispatch_url_resolver_oserror_warning_omits_exception_text(monkeypatch, caplog, error, errno_text):
    """strerror/filename/args 是异常携带的数据，不进告警：只留节点、异常类型和整数 errno。"""
    def _fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(main.socket, "getaddrinfo", _fail)
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {})
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    main._DNS_CACHE.clear()

    with caplog.at_level(logging.WARNING):
        assert main.resolve_dispatch_api_url("worker-node-1", "http://worker-node-1:8001/task/upload") is None

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("worker-node-1" in message and "OSError" in message and errno_text in message for message in warnings)
    assert "SENTINEL" not in caplog.text


def test_default_dns_stub_resolves_to_public_address_accepted_in_strict_mode(monkeypatch):
    """conftest 的默认解析桩必须给真正的公网地址：TEST-NET 段在 Python 里算 is_private，
    会让主机名用例隐式走"内网放行"分支，严格模式下反而被拒。"""
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", False)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    main._DNS_CACHE.clear()

    infos = main.socket.getaddrinfo("worker-node-1", 8001, type=main.socket.SOCK_STREAM)
    addresses = {info[4][0] for info in infos}
    assert addresses
    for address in addresses:
        ip = ipaddress.ip_address(address)
        assert ip.is_global and not ip.is_private, address

    assert main.verify_dispatch_target("http://worker-node-1:8001/task/upload")


def test_strict_mode_dispatch_url_follows_resolved_address_class(monkeypatch):
    """严格模式下的自报主机名：解析到公网放行，解析到普通私网或元数据一律拒绝。"""
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", False)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {})
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    answers = {
        "public-worker": PUBLIC_TEST_ADDRESS,
        "private-worker": "172.18.0.5",
        "metadata-worker": "169.254.169.254",
    }
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda host, port, *args, **kwargs: [(2, 1, 6, "", (answers[host], port))],
    )
    main._DNS_CACHE.clear()

    public_url = "http://public-worker:8001/task/upload"
    assert main.resolve_dispatch_api_url("public-worker", public_url) == public_url
    assert main.resolve_dispatch_api_url("private-worker", "http://private-worker:8001/task/upload") is None
    assert main.resolve_dispatch_api_url("metadata-worker", "http://metadata-worker:8001/task/upload") is None


def test_dispatch_url_resolution_rejects_registered_metadata_address(monkeypatch):
    """预置在 worker_registry 里的历史地址也要在派发前复核。"""
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {})
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    assert main.resolve_dispatch_api_url("worker-node-1", "http://169.254.169.254:80/task/upload") is None


def test_self_reported_url_rejected_when_trust_disabled(monkeypatch):
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {})
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", False)
    with pytest.raises(main.HTTPException):
        main.resolve_worker_api_url("worker-node-1", "http://worker-node-1:8001/task/upload")


def test_report_files_rejects_cross_node_identity_spoofing(mock_db):
    """用 A 节点的密钥声明自己是 B 节点：签名校验必须按声明节点取密钥，跨节点身份不能通过。"""
    token_a = "node-secret-token-aaa"
    token_b = "node-secret-token-bbb"
    payload = {
        "worker_node": "worker-node-2",
        "worker_api_url": "http://169.254.169.254/task/upload",
        "protocol_version": 2,
        "files": [],
    }
    raw_body, headers = _signed_report_files_request("worker-node-2", token_a, payload)

    with patch.object(
        main, "WORKER_TOKEN_MAP", {"worker-node-1": token_a, "worker-node-2": token_b}
    ), patch("main.time.time", return_value=1700000000):
        response = client.post("/api/report_files", content=raw_body, headers=headers)

    assert response.status_code == 401
    assert not any(
        "worker_registry" in str(call.args[0]) and "UPDATE" in str(call.args[0]).upper()
        for call in mock_db.execute.call_args_list
    )


def test_private_url_rejected_when_self_reported_but_allowed_from_map(monkeypatch):
    """严格模式：自报的内网地址被拒绝；白名单里显式批准的同一地址可以派发。"""
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", False)

    monkeypatch.setattr(main, "WORKER_API_URL_MAP", {})
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    assert main.resolve_dispatch_api_url("worker-node-1", "http://172.18.0.5:8001/task/upload") is None

    monkeypatch.setattr(
        main, "WORKER_API_URL_MAP", {"worker-node-1": "http://172.18.0.5:8001/task/upload"}
    )
    assert (
        main.resolve_dispatch_api_url("worker-node-1", "")
        == "http://172.18.0.5:8001/task/upload"
    )


@pytest.mark.parametrize(
    "api_url",
    [
        # 阿里云元数据在 CGNAT 段里，Python 的 is_private 不认它。
        "http://100.100.100.200/latest/meta-data/",
        "http://100.64.0.1:8001/task/upload",
        "http://100.127.255.254:8001/task/upload",
        # IPv4 映射到 IPv6 的写法。
        "http://[::ffff:100.100.100.200]/latest/meta-data/",
        "http://192.0.0.192/latest/meta-data/",
        "http://198.18.0.1:8001/task/upload",
    ],
)
def test_verify_dispatch_target_blocks_metadata_and_special_ranges(api_url):
    with pytest.raises(ValueError):
        main.verify_dispatch_target(api_url)


def test_verify_dispatch_target_blocks_dns_resolving_to_cgnat_metadata(monkeypatch):
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("100.100.100.200", 80))],
    )
    with pytest.raises(ValueError):
        main.verify_dispatch_target("http://metadata.internal:80/latest/meta-data/")


@pytest.mark.parametrize(
    "api_url",
    [
        "http://0.1.2.3:8001/task/upload",      # Linux 会把 0/8 路由到本机
        "http://0.0.0.1:8001/task/upload",
        "http://[fd00:ec2::254]/latest/meta-data/",  # AWS IMDS（IPv6）
        "http://168.63.129.16/machine?id=1",    # Azure WireServer
    ],
)
def test_verify_dispatch_target_blocks_more_cloud_metadata_endpoints(api_url):
    with pytest.raises(ValueError):
        main.verify_dispatch_target(api_url)


# 主流云厂商的元数据/平台端点：新增一家时只往这张表里加一行。
CLOUD_METADATA_ENDPOINTS = [
    ("http://169.254.169.254/latest/meta-data/", "AWS/GCP/Azure/Oracle IMDS (IPv4)"),
    ("http://[fd00:ec2::254]/latest/meta-data/", "AWS IMDS (IPv6)"),
    ("http://[fd20:ce::254]/computeMetadata/v1/", "GCP metadata (IPv6)"),
    ("http://100.100.100.200/latest/meta-data/", "阿里云 metadata"),
    ("http://192.0.0.192/latest/meta-data/", "Oracle Cloud metadata"),
    ("http://169.254.170.2/v2/credentials/", "ECS 任务元数据"),
    ("http://168.63.129.16/machine?id=1", "Azure WireServer"),
]


@pytest.mark.parametrize("api_url,label", CLOUD_METADATA_ENDPOINTS, ids=[item[1] for item in CLOUD_METADATA_ENDPOINTS])
def test_verify_dispatch_target_blocks_every_known_metadata_endpoint(api_url, label):
    for allow_private in (True, False):
        with patch.object(main, "ALLOW_PRIVATE_WORKER_URLS", allow_private):
            with pytest.raises(ValueError):
                main.verify_dispatch_target(api_url)
            # 白名单批准也不能把元数据地址放出来。
            with pytest.raises(ValueError):
                main.verify_dispatch_target(api_url, trusted_source=True)


@pytest.mark.parametrize(
    "api_url",
    [
        "http://2852039166/latest/meta-data/",          # 十进制整数形式的 169.254.169.254
        "http://0xa9fea9fe/latest/meta-data/",          # 十六进制
        "http://0251.0376.0251.0376/latest/meta-data/",  # 八进制
        "http://169.254.169.254./latest/meta-data/",    # 末尾点
    ],
)
def test_verify_dispatch_target_blocks_alternate_ip_literal_forms(api_url, monkeypatch):
    """数值写法最终都会被解析成同一个地址，分类必须作用在解析结果上而不是字面量上。"""
    from conftest import REAL_GETADDRINFO

    monkeypatch.setattr(main.socket, "getaddrinfo", REAL_GETADDRINFO)
    main._DNS_CACHE.clear()
    with pytest.raises(ValueError):
        main.verify_dispatch_target(api_url)


def test_dns_resolution_timeout_is_fail_closed(monkeypatch):
    """自报主机名指向黑洞 DNS 时不能再无限阻塞调度线程。"""
    import threading as _threading

    release = _threading.Event()

    def _hang(*args, **kwargs):
        release.wait(10)
        return []

    monkeypatch.setattr(main.socket, "getaddrinfo", _hang)
    monkeypatch.setattr(main, "_DNS_RESOLVE_TIMEOUT_SECONDS", 0.2)
    main._DNS_CACHE.clear()
    started = time.monotonic()
    try:
        with pytest.raises(ValueError):
            main.verify_dispatch_target("http://blackhole.invalid:8001/task/upload")
        elapsed = time.monotonic() - started
        # 断言耗时而不只是"抛了异常"：桩会挂 10 秒，没有超时保护时照样会抛 ValueError。
        assert elapsed < 3.0, f"解析没有被超时打断（耗时 {elapsed:.1f}s）"
    finally:
        release.set()
        main._DNS_CACHE.clear()


def test_allowlist_does_not_bypass_metadata_addresses(monkeypatch):
    """allowlist 只放开内网判定，元数据/回环仍然拒绝。"""
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset({"proxy.local"}))
    monkeypatch.setattr(
        main.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("169.254.169.254", 80))],
    )
    main._DNS_CACHE.clear()
    with pytest.raises(ValueError):
        main.verify_dispatch_target("http://proxy.local/latest/meta-data/")


# ===== Tailnet 精确豁免（Issue #22） =====

TAILNET_MAP = {
    "Gomami-1": "http://100.125.75.111:8001/task/upload",
    "Gomami-2": "http://100.100.185.27:8001/task/upload",
    "1GServer-US": "http://100.91.91.88:8001/task/upload",
}
TAILNET_IPS = frozenset({"100.125.75.111", "100.100.185.27", "100.91.91.88"})


def _no_dns(*args, **kwargs):
    raise AssertionError("精确 TailIP 字面量与启动校验都不应触发 DNS 解析")


@pytest.fixture
def tailnet_policy(monkeypatch):
    """模拟 loader 校验通过后的运行时状态；raising=False 让基线上的用例以断言失败呈现。"""
    monkeypatch.setattr(main, "WORKER_API_URL_MAP", dict(TAILNET_MAP))
    monkeypatch.setattr(main, "WORKER_API_TAILNET_IPS", TAILNET_IPS, raising=False)
    monkeypatch.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", False)
    monkeypatch.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset())
    monkeypatch.setattr(main.socket, "getaddrinfo", _no_dns)
    main._DNS_CACHE.clear()
    return monkeypatch


def test_worker_api_tailnet_ips_defaults_to_empty():
    assert main.WORKER_API_TAILNET_IPS == frozenset()


@pytest.mark.parametrize("allow_private", [True, False])
@pytest.mark.parametrize("node", sorted(TAILNET_MAP))
def test_approved_tailnet_ip_from_map_is_dispatchable_without_dns(tailnet_policy, node, allow_private):
    tailnet_policy.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", allow_private)

    assert main.resolve_dispatch_api_url(node, "") == TAILNET_MAP[node]
    # worker_registry 里的历史地址不影响结果：白名单命中即用映射地址。
    assert main.resolve_dispatch_api_url(node, STALE_REGISTRY_URL) == TAILNET_MAP[node]


@pytest.mark.parametrize("node", sorted(TAILNET_MAP))
def test_tailnet_ip_stays_blocked_when_approved_list_is_empty(tailnet_policy, node):
    tailnet_policy.setattr(main, "WORKER_API_TAILNET_IPS", frozenset(), raising=False)

    assert main.resolve_dispatch_api_url(node, "") is None
    with pytest.raises(ValueError):
        main.verify_dispatch_target(TAILNET_MAP[node], trusted_source=True)


@pytest.mark.parametrize(
    ("api_url", "trusted_source", "worker_node"),
    [
        (TAILNET_MAP["Gomami-1"], True, None),
        (TAILNET_MAP["Gomami-1"], False, "Gomami-1"),
        (TAILNET_MAP["Gomami-1"], True, "Gomami-2"),
        (TAILNET_MAP["Gomami-1"], True, "unknown-node"),
        ("http://100.125.75.111:8002/task/upload", True, "Gomami-1"),
        ("http://100.125.75.111/task/upload", True, "Gomami-1"),
        ("http://100.125.75.111:8001/other", True, "Gomami-1"),
        ("http://100.125.75.111:8001/task/upload?x=1", True, "Gomami-1"),
        ("https://100.125.75.111:8001/task/upload", True, "Gomami-1"),
        ("http://[::ffff:100.125.75.111]:8001/task/upload", True, "Gomami-1"),
    ],
    ids=[
        "missing-node", "untrusted", "node-mismatch", "unknown-node", "port-mismatch",
        "default-port", "path-mismatch", "query-added", "scheme-mismatch", "v4-mapped",
    ],
)
def test_tailnet_exemption_requires_trusted_node_and_exact_map_url(
    tailnet_policy, api_url, trusted_source, worker_node
):
    with pytest.raises(ValueError):
        main.verify_dispatch_target(api_url, trusted_source, worker_node=worker_node)


@pytest.mark.parametrize("map_empty", [True, False])
def test_self_reported_tailnet_url_never_borrows_approval(tailnet_policy, map_empty):
    tailnet_policy.setattr(main, "TRUST_SELF_REPORTED_WORKER_API_URL", True)
    if map_empty:
        tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {})
        for node, url in TAILNET_MAP.items():
            assert main.resolve_dispatch_api_url(node, url) is None
    else:
        assert main.resolve_dispatch_api_url("self-reporting-node", TAILNET_MAP["Gomami-1"]) is None


@pytest.mark.parametrize("address", ["100.64.0.1", "100.125.75.112", "100.127.255.254"])
def test_unapproved_cgnat_in_map_stays_blocked_when_feature_enabled(tailnet_policy, address):
    url = f"http://{address}:8001/task/upload"
    tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {**TAILNET_MAP, "extra-node": url})

    assert main.resolve_dispatch_api_url("extra-node", "") is None


def test_hostname_resolving_to_approved_tailnet_ip_is_still_blocked(tailnet_policy):
    """DNS 分支永远不带豁免：主机名解析到已批准 IP、即使主机名在 allowlist 里，也照样拒绝。"""
    url = "http://gomami-1.tailnet.example:8001/task/upload"
    tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {"Gomami-1": url})
    tailnet_policy.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset({"gomami-1.tailnet.example"}))
    tailnet_policy.setattr(
        main.socket,
        "getaddrinfo",
        lambda host, port, *args, **kwargs: [(2, 1, 6, "", ("100.125.75.111", port))],
    )
    main._DNS_CACHE.clear()

    assert main.resolve_dispatch_api_url("Gomami-1", "") is None
    with pytest.raises(ValueError):
        main.verify_dispatch_target(url, True, worker_node="Gomami-1")


def test_host_allowlist_alone_cannot_open_tailnet_ip(tailnet_policy):
    tailnet_policy.setattr(main, "WORKER_API_TAILNET_IPS", frozenset(), raising=False)
    tailnet_policy.setattr(main, "WORKER_API_ALLOWED_HOSTS", frozenset({"100.125.75.111"}))

    assert main.resolve_dispatch_api_url("Gomami-1", "") is None


@pytest.mark.parametrize(
    "variant_host",
    [
        "1685932911",            # 十进制整数形式的 100.125.75.111
        "0x647d4b6f",            # 十六进制
        "0144.0175.0113.0157",   # 八进制
        "100.125.75.111.",       # 末尾点
    ],
)
def test_alternate_forms_of_approved_ip_never_get_exemption(tailnet_policy, variant_host):
    """变形写法即使原样写进 map、规范 IP 也在批准列表里，主机串不等于规范文本，就走 DNS 分支且不带豁免。"""
    from conftest import REAL_GETADDRINFO

    url = f"http://{variant_host}:8001/task/upload"
    tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {"Gomami-1": url})
    tailnet_policy.setattr(main.socket, "getaddrinfo", REAL_GETADDRINFO)
    main._DNS_CACHE.clear()

    assert main.resolve_dispatch_api_url("Gomami-1", "") is None


def test_v4_mapped_form_never_exempt_even_if_forced_into_approved_list(tailnet_policy):
    url = "http://[::ffff:100.125.75.111]:8001/task/upload"
    tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {"Gomami-1": url})
    tailnet_policy.setattr(
        main, "WORKER_API_TAILNET_IPS", frozenset({"::ffff:100.125.75.111", "100.125.75.111"}), raising=False
    )

    assert main.resolve_dispatch_api_url("Gomami-1", "") is None


@pytest.mark.parametrize("api_url,label", CLOUD_METADATA_ENDPOINTS, ids=[item[1] for item in CLOUD_METADATA_ENDPOINTS])
def test_metadata_endpoints_blocked_even_when_forced_into_tailnet_approval(tailnet_policy, api_url, label):
    host = urlsplit(api_url).hostname
    tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {"forced-node": api_url})
    tailnet_policy.setattr(main, "WORKER_API_TAILNET_IPS", frozenset({host}), raising=False)

    for allow_private in (True, False):
        tailnet_policy.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", allow_private)
        with pytest.raises(ValueError):
            main.verify_dispatch_target(api_url, True, worker_node="forced-node")
        assert main.resolve_dispatch_api_url("forced-node", "") is None


@pytest.mark.parametrize("address", ["100.100.100.0", "100.100.100.100", "100.100.100.200", "100.100.100.255"])
def test_tailscale_service_range_never_exempt_at_runtime(tailnet_policy, address):
    """100.100.100.0/24（Tailscale 服务地址、阿里云元数据）绕过 loader 硬塞进批准列表也照样拒绝。"""
    url = f"http://{address}:8001/task/upload"
    tailnet_policy.setattr(main, "WORKER_API_URL_MAP", {"forced-node": url})
    tailnet_policy.setattr(main, "WORKER_API_TAILNET_IPS", frozenset({address}), raising=False)

    with pytest.raises(ValueError):
        main.verify_dispatch_target(url, True, worker_node="forced-node")
    assert main.resolve_dispatch_api_url("forced-node", "") is None


def test_tailnet_exemption_skips_only_cgnat_rule(monkeypatch):
    """豁免只跳过 CGNAT 这一条：其它硬拒规则仍然生效，不提前 return。"""
    monkeypatch.setattr(main, "ALLOW_PRIVATE_WORKER_URLS", False)

    assert main._blocked_address_reason(ipaddress.ip_address("100.125.75.111"), tailnet_approved=True) is None
    for address in ("169.254.169.254", "127.0.0.1", "192.0.0.192", "0.1.2.3", "100.100.100.200"):
        assert main._blocked_address_reason(ipaddress.ip_address(address), tailnet_approved=True), address
    assert main._blocked_address_reason(ipaddress.ip_address("::ffff:100.125.75.111"), tailnet_approved=True)
    assert main._blocked_address_reason(ipaddress.ip_address("100.125.75.111"))


@pytest.mark.parametrize("approved", [True, False])
def test_dispatch_uploader_tasks_tailnet_target_follows_approved_list(monkeypatch, approved):
    monkeypatch.setattr(main, "WORKER_API_TAILNET_IPS", TAILNET_IPS if approved else frozenset(), raising=False)
    monkeypatch.setattr(main.socket, "getaddrinfo", _no_dns)
    cursor = _uploader_dispatch_cursor([{"node_name": "Gomami-1", "api_url": STALE_REGISTRY_URL}])

    targets = _run_dispatch_and_collect_targets(main.dispatch_uploader_tasks, cursor, dict(TAILNET_MAP))

    if approved:
        assert targets == [(TAILNET_MAP["Gomami-1"], "Gomami-1")]
    else:
        assert targets == []
        assert _updates_touching_node(cursor, "Gomami-1") == []


@pytest.mark.parametrize("approved", [True, False])
def test_dispatch_cleaner_tasks_tailnet_target_follows_approved_list(monkeypatch, approved):
    monkeypatch.setattr(main, "WORKER_API_TAILNET_IPS", TAILNET_IPS if approved else frozenset(), raising=False)
    monkeypatch.setattr(main.socket, "getaddrinfo", _no_dns)
    cursor = _cleaner_dispatch_cursor({"Gomami-1": STALE_REGISTRY_URL})

    targets = _run_dispatch_and_collect_targets(main.dispatch_cleaner_tasks, cursor, dict(TAILNET_MAP))

    if approved:
        assert targets == [(TAILNET_MAP["Gomami-1"], "Gomami-1")]
    else:
        assert targets == []
        assert _updates_touching_node(cursor, "Gomami-1") == []


def test_dispatch_uploader_tasks_and_cleaner_pass_worker_node_to_target_check(monkeypatch):
    """uploader 与 cleaner 共用同一出站校验，并且都把节点名交给它（豁免必须绑定节点）。"""
    calls = []
    real_verify = main.verify_dispatch_target

    def spy(api_url, trusted_source=False, **kwargs):
        calls.append((api_url, trusted_source, kwargs))
        return real_verify(api_url, trusted_source, **kwargs)

    monkeypatch.setattr(main, "verify_dispatch_target", spy)
    url_map = {"worker-node-1": MAPPED_URL}
    _run_dispatch_and_collect_targets(
        main.dispatch_uploader_tasks,
        _uploader_dispatch_cursor([{"node_name": "worker-node-1", "api_url": STALE_REGISTRY_URL}]),
        url_map,
    )
    _run_dispatch_and_collect_targets(
        main.dispatch_cleaner_tasks, _cleaner_dispatch_cursor({"worker-node-1": STALE_REGISTRY_URL}), url_map
    )

    assert calls == [(MAPPED_URL, True, {"worker_node": "worker-node-1"})] * 2


# ----- 启动配置 WORKER_API_TAILNET_IPS（loader） -----


@pytest.fixture
def loader_no_dns(monkeypatch):
    monkeypatch.setattr(main.socket, "getaddrinfo", _no_dns)
    monkeypatch.setattr(main.socket, "gethostbyname", _no_dns)


def _formatted(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def test_tailnet_loader_accepts_exact_list_without_dns(loader_no_dns):
    raw = " 100.125.75.111 , 100.100.185.27,100.91.91.88 "

    assert main._load_worker_api_tailnet_ips(raw, dict(TAILNET_MAP)) == TAILNET_IPS


@pytest.mark.parametrize("raw", ["", "   "])
def test_tailnet_loader_empty_keeps_legacy_behaviour(loader_no_dns, raw):
    """默认空：不给旧 map 增加新约束（map 里的 CGNAT 字面量照旧由运行时拒绝）。"""
    assert main._load_worker_api_tailnet_ips(raw, {"legacy": "http://100.64.0.1:8001/task/upload"}) == frozenset()
    assert main._load_worker_api_tailnet_ips(raw, {}) == frozenset()


@pytest.mark.parametrize(
    "raw",
    [
        "100.125.75.111,,100.100.185.27,100.91.91.88",
        "100.125.75.111,100.100.185.27,100.91.91.88,",
        ",100.125.75.111,100.100.185.27,100.91.91.88",
        "100.125.75.0111,100.100.185.27,100.91.91.88",
        "100.125.075.111,100.100.185.27,100.91.91.88",
        "100.125.75,100.100.185.27,100.91.91.88",
        "100.125.75.111/32,100.100.185.27,100.91.91.88",
        "100.64.0.0/10",
        "100.125.75.110-100.125.75.111,100.100.185.27,100.91.91.88",
        "100.125.75.111 100.100.185.27,100.91.91.88",
        "100.125.75.111;100.100.185.27;100.91.91.88",
        "fd7a:115c:a1e0::1",
        "::ffff:100.125.75.111,100.100.185.27,100.91.91.88",
        "SENTINEL-gomami-1.tailnet.example,100.100.185.27,100.91.91.88",
        "1685932911,100.100.185.27,100.91.91.88",
        "10.0.0.5",
        "169.254.169.254",
        "100.100.100.100",
        "100.100.100.200",
    ],
)
def test_tailnet_loader_rejects_non_canonical_or_forbidden_tokens(loader_no_dns, raw):
    url_map = {**TAILNET_MAP, "n10": "http://10.0.0.5:8001/task/upload"}

    with pytest.raises(RuntimeError) as excinfo:
        main._load_worker_api_tailnet_ips(raw, url_map)

    text = _formatted(excinfo.value)
    assert "WORKER_API_TAILNET_IPS" in str(excinfo.value)
    for token in (part.strip() for part in raw.split(",")):
        if token:
            assert token not in text


@pytest.mark.parametrize(
    ("raw", "url_map"),
    [
        # 列出的 IP 没有对应的 map 字面量。
        ("100.125.75.111,100.100.185.27,100.91.91.88,100.88.1.2", dict(TAILNET_MAP)),
        # map 里有未批准的 CGNAT 字面量。
        ("100.125.75.111,100.100.185.27,100.91.91.88", {**TAILNET_MAP, "extra-node": "http://100.88.1.2:8001/SENTINEL-path"}),
        # map 里 v4-mapped 写法的 CGNAT 同样不可能被批准。
        ("100.125.75.111,100.100.185.27,100.91.91.88", {**TAILNET_MAP, "extra-node": "http://[::ffff:100.88.1.2]:8001/x"}),
        # 启用了列表但 map 为空。
        ("100.125.75.111", {}),
        # map 只用主机名或变形写法时，规范 IP 没有字面量批准。
        ("100.125.75.111", {"Gomami-1": "http://100.125.75.111.:8001/task/upload"}),
        ("100.125.75.111", {"Gomami-1": "http://gomami-1.tailnet.example:8001/task/upload"}),
    ],
    ids=["ip-missing-from-map", "map-cgnat-unapproved", "map-v4-mapped-cgnat", "empty-map", "map-variant", "map-hostname"],
)
def test_tailnet_loader_requires_bidirectional_consistency_with_map(loader_no_dns, raw, url_map):
    with pytest.raises(RuntimeError) as excinfo:
        main._load_worker_api_tailnet_ips(raw, url_map)

    text = _formatted(excinfo.value)
    for url in url_map.values():
        assert url not in text
    assert "SENTINEL" not in text
    assert "100.88.1.2" not in text


@pytest.mark.parametrize("node_name", ["token=SENTINEL secret!", "SENTINEL_SECRET_NODE"])
def test_tailnet_loader_never_echoes_map_keys(loader_no_dns, node_name):
    # 形如普通节点名也不回显：正则证明不了它不是误填的凭据。
    with pytest.raises(RuntimeError, match="WORKER_API_URL_MAP 第 2 项") as excinfo:
        main._load_worker_api_tailnet_ips(
            "100.125.75.111", {"Gomami-1": TAILNET_MAP["Gomami-1"], node_name: "http://100.88.1.2:8001/x"}
        )

    text = _formatted(excinfo.value)
    assert "SENTINEL" not in text
    assert "Gomami-1" not in text


# ===== UPLOADER_DISPATCH_ENABLED 维护开关（Issue #23） =====


def test_uploader_dispatch_enabled_defaults_to_true():
    assert main.UPLOADER_DISPATCH_ENABLED is True


@pytest.fixture
def uploader_dispatch_disabled(monkeypatch):
    """关闭开关，并把锁/DB/SQL/HTTP 全换成会记账的炸弹；返回被意外触碰的入口列表。"""
    touched = []

    def bomb(label):
        def _explode(*args, **kwargs):
            touched.append(label)
            raise AssertionError(f"{label} must not run while UPLOADER_DISPATCH_ENABLED=false")
        return _explode

    class ExplodingLock:
        acquire = staticmethod(bomb("_dispatch_run_lock.acquire"))
        release = staticmethod(bomb("_dispatch_run_lock.release"))

    monkeypatch.setattr(main, "UPLOADER_DISPATCH_ENABLED", False, raising=False)
    monkeypatch.setattr(main, "_dispatch_run_lock", ExplodingLock())
    monkeypatch.setattr(main, "_acquire_dispatch_db_lock", bomb("_acquire_dispatch_db_lock"))
    monkeypatch.setattr(main, "_open_dispatch_lock_connection", bomb("_open_dispatch_lock_connection"))
    monkeypatch.setattr(main, "get_db_connection", bomb("get_db_connection"))
    monkeypatch.setattr(main, "_dispatch_uploader_tasks_locked", bomb("_dispatch_uploader_tasks_locked"))
    monkeypatch.setattr(main.httpx, "post", bomb("httpx.post"))
    return touched


def test_dispatch_uploader_tasks_disabled_returns_true_without_lock_db_sql_or_http(uploader_dispatch_disabled):
    assert main.dispatch_uploader_tasks() is True
    assert uploader_dispatch_disabled == []


def test_trigger_dispatch_uploader_tasks_async_is_noop_when_disabled(uploader_dispatch_disabled, monkeypatch):
    """即时触发（配额/凭据失效回执）在关闭时一次返回 True，不进入"调度忙"重试循环，也不留线程。"""
    results = []
    real_dispatch = main.dispatch_uploader_tasks

    def recording_dispatch():
        result = real_dispatch()
        results.append(result)
        return result

    monkeypatch.setattr(main, "dispatch_uploader_tasks", recording_dispatch)
    before = set(threading.enumerate())

    main.trigger_dispatch_uploader_tasks_async("test: quota exhausted")

    for thread in [t for t in threading.enumerate() if t not in before]:
        thread.join(5)
        assert not thread.is_alive()
    assert results == [True]
    assert uploader_dispatch_disabled == []


def test_scheduled_uploader_job_is_noop_when_disabled(uploader_dispatch_disabled, monkeypatch):
    """定时入口：lifespan 注册的 uploader 任务在关闭时同样只返回 True。调度器替身不启动任何线程。"""
    jobs = []

    class FakeScheduler:
        def __init__(self, *args, **kwargs):
            pass

        def add_job(self, func, *args, **kwargs):
            jobs.append(func)

        def start(self):
            pass

        def shutdown(self, *args, **kwargs):
            pass

    monkeypatch.setattr(main, "BackgroundScheduler", FakeScheduler)
    monkeypatch.setattr(main, "log_worker_api_url_map_gaps", lambda: None)

    with TestClient(main.app):
        pass

    uploader_jobs = [job for job in jobs if job is main.dispatch_uploader_tasks]
    assert uploader_jobs
    assert [job() for job in uploader_jobs] == [True] * len(uploader_jobs)
    assert uploader_dispatch_disabled == []
