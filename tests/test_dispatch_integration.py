from pathlib import Path
from unittest.mock import MagicMock, patch
import json
import os
import sys

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ALLOW_LEGACY_NODE_AUTH", "true")
os.environ.setdefault("ALLOW_LEGACY_UNLEASED_REPORTS", "true")
os.environ.setdefault("DB_HOST", "test-db-host")
os.environ.setdefault("DB_USER", "test-db-user")
os.environ.setdefault("DB_PASSWORD", "test-db-password")
os.environ.setdefault("DB_NAME", "test-db-name")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main


def test_dispatch_uploader_tasks_distributes_across_two_workers():
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    mock_cursor.fetchall.side_effect = [
        [],
        [],
        [
            {"node_name": "worker-1", "api_url": "http://worker-1:8001/task/upload"},
            {"node_name": "worker-2", "api_url": "http://worker-2:8001/task/upload"},
        ],
        [
            {"account_name": "baicai_1@1"},
            {"account_name": "baicai_2@1"},
        ],
        [{"id": 101, "file_path": "movie/w1.mkv", "upload_method": "move"}],
        [],
        [
            {"account_name": "baicai_2@1"},
        ],
        [{"id": 202, "file_path": "movie/w2.mkv", "upload_method": "move"}],
        [],
    ]
    mock_cursor.fetchone.side_effect = [
        {"active_count": 0},
        {"acquired": 1},
        None,
        {"active_count": 0},
        {"acquired": 1},
        None,
    ]

    post_response = MagicMock()
    post_response.raise_for_status.return_value = None

    with patch("main.get_db_connection", return_value=mock_connection), patch("main.httpx.post", return_value=post_response) as mock_post, patch("main.current_upload_concurrency_limit", return_value=2):
        main.dispatch_uploader_tasks()

    assert mock_post.call_count == 2
    first_call = mock_post.call_args_list[0]
    second_call = mock_post.call_args_list[1]
    assert first_call.args[0] == "http://worker-1:8001/task/upload"
    assert second_call.args[0] == "http://worker-2:8001/task/upload"
    first_payload = json.loads(first_call.kwargs["content"])
    second_payload = json.loads(second_call.kwargs["content"])
    assert first_payload["account_name"] == "baicai_1@1"
    assert second_payload["account_name"] == "baicai_2@1"
    assert first_payload["files"] == ["movie/w1.mkv"]
    assert second_payload["files"] == ["movie/w2.mkv"]
