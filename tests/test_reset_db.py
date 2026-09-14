from unittest.mock import MagicMock, patch

from scripts import reset_db


def _configured_env():
    return {
        "DB_HOST": "db",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
        "DB_NAME": "baicai",
    }


def test_reset_database_deletes_foreign_key_child_before_parent():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        ("file_tasks",),
        ("worker_registry",),
        ("account_upload_events",),
        ("drive_accounts",),
    ]

    with patch.dict("os.environ", _configured_env(), clear=False), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ):
        result = reset_db.reset_database()

    assert result == 0
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements == [
        "SHOW TABLES;",
        "DELETE FROM `account_upload_events`;",
        "DELETE FROM `file_tasks`;",
        "DELETE FROM `worker_registry`;",
        """
                    UPDATE drive_accounts
                    SET dispatch_locked_until=NULL,
                        current_dispatch_id=NULL,
                        current_worker_node=NULL
                    """,
    ]
    conn.commit.assert_called_once_with()
    conn.close.assert_called_once_with()


def test_reset_database_rolls_back_and_returns_failure():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = RuntimeError("database unavailable")

    with patch.dict("os.environ", _configured_env(), clear=False), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ):
        result = reset_db.reset_database()

    assert result == 1
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()
