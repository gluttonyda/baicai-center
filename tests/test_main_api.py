from pathlib import Path
from unittest.mock import MagicMock, patch
import httpx
import os
import sys
import threading

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("API_TOKEN", "test-token")
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
        mock_conn.return_value = mock_connection
        yield mock_cursor


def auth_headers():
    return {"Authorization": f"Bearer {API_TOKEN}"}


def test_unauthorized_access():
    response = client.post("/api/report_status", json={"account_name": "test", "status": "completed"})
    assert response.status_code == 403


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
    assert any("UPDATE file_tasks SET account_used=NULL WHERE status='pending' AND account_used=%s" in sql for sql in sql_calls)


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
    assert insert_args == ("worker-node-1", "http://worker-node-1.example.com:8001/task/upload")
    insert_sql = mock_db.execute.call_args_list[0][0][0]
    assert "UTC_TIMESTAMP()" in insert_sql
    assert "NOW()" not in insert_sql


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
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("boom")

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("UPDATE file_tasks SET status='pending', updated_at=UTC_TIMESTAMP() WHERE id IN" in sql for sql in sql_calls)


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
        {"active_count": 2},
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any(
        "UPDATE file_tasks SET status='uploading', account_used=%s, updated_at=UTC_TIMESTAMP()" in sql
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


def test_acquire_account_lock_cleaner_ignores_dispatch_cooldown():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1

    assert main.acquire_account_lock(mock_cursor, "baicai-1@1-3", "cleaner") is True

    sql = mock_cursor.execute.call_args[0][0]
    assert "dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP()" in sql
    assert "quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP()" in sql
    assert "dispatch_cooldown_until" not in sql


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
        [{"account_used": "baicai_2@1", "worker_node": "worker-node-3", "task_count": 253}],
        [],
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch.object(main, "release_account_lock") as mock_release_lock:
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)" in sql for sql in sql_calls)
    assert any("UPDATE file_tasks" in sql and "SET status='pending'" in sql for sql in sql_calls)
    mock_release_lock.assert_called_once_with(mock_cursor, "baicai_2@1")


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
    ]

    with patch("main.get_db_connection", return_value=mock_connection):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("UPDATE drive_accounts SET dispatch_locked_until = NULL" in sql for sql in sql_calls)


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
    ]

    with patch("main.get_db_connection", return_value=mock_connection):
        main.dispatch_uploader_tasks()

    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    available_account_sql = next(sql for sql in sql_calls if "FROM drive_accounts da" in sql and "has_preferred_pending" in sql)
    assert "dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP()" in available_account_sql
    assert "quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP()" in available_account_sql


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
        [{"file_path": "movie/test1.mkv"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 2},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=mock_response) as mock_post:
        main.dispatch_cleaner_tasks()

    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs["json"]["dest_name"] == "baicai-big"
    sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
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
        [{"file_path": "movie/test1.mkv"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 1},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
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
        [{"cleaner_account": "baicai-1@1-3"}],
        [{"account_used": "baicai_1@1", "worker_node": "worker-node-1"}],
    ]

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post") as mock_post, patch("main.release_account_lock") as mock_release_lock:
        main.dispatch_cleaner_tasks()

    mock_post.assert_not_called()
    mock_release_lock.assert_called_once_with(mock_cursor, "baicai-1@1-3")


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
        [{"file_path": "movie/test1.mkv"}],
    ]
    mock_cursor.fetchone.side_effect = [
        None,
        {"total_uploaders": 2, "blocked_uploaders": 2},
        {"cleaner_mapping": "baicai-1@1-3"},
        {"api_url": "http://worker:8001/task/upload"},
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
    assert any("UPDATE file_tasks SET status='pending', account_used=NULL WHERE account_used=%s AND status='uploading'" in sql for sql in sql_calls)


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
    assert any("UPDATE file_tasks SET status='pending' WHERE account_used=%s AND status='uploading'" in sql for sql in sql_calls)
    assert not any("UPDATE file_tasks SET status='pending', account_used=NULL WHERE account_used=%s AND status='uploading'" in sql for sql in sql_calls)


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


def test_clean_success_requires_file_list(mock_db):
    payload = {
        "account_name": "baicai-1@1-3",
        "status": "clean_success",
        "successful_files": [],
    }
    mock_db.fetchone.side_effect = [
        {"role": "cleaner"},
    ]

    response = client.post("/api/report_status", json=payload, headers=auth_headers())

    assert response.status_code == 200
    sql_calls = [call[0][0] for call in mock_db.execute.call_args_list]
    assert not any("JOIN drive_accounts da" in sql and "SET ft.status = 'completed'" in sql for sql in sql_calls)


def test_clean_success_updates_only_reported_files(mock_db):
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
