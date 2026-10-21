from pathlib import Path
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

from scripts import reset_db


def _configured_env():
    return {
        "DB_HOST": "db",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
        "DB_NAME": "baicai",
    }


def _fake_stdin(is_tty: bool) -> MagicMock:
    stdin = MagicMock()
    stdin.isatty.return_value = is_tty
    return stdin


def _fake_connection(tables):
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(table,) for table in tables]
    return conn, cursor


def test_reset_database_deletes_foreign_key_child_before_parent():
    conn, cursor = _fake_connection(
        ["file_tasks", "worker_registry", "account_upload_events", "drive_accounts"]
    )

    with patch.dict("os.environ", _configured_env(), clear=False), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ):
        result = reset_db.reset_database(assume_yes=True)

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
        result = reset_db.reset_database(assume_yes=True)

    assert result == 1
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()


def test_cli_with_yes_resets_without_prompt():
    conn, cursor = _fake_connection(["file_tasks"])

    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", _fake_stdin(is_tty=False)
    ), patch("builtins.input", side_effect=AssertionError("--yes 不应再提示确认")), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ) as connect:
        result = reset_db.main(["--yes"])

    assert result == 0
    connect.assert_called_once()
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "DELETE FROM `file_tasks`;" in statements
    conn.commit.assert_called_once_with()


def test_cli_without_yes_refuses_non_tty_before_connecting(capsys):
    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", _fake_stdin(is_tty=False)
    ), patch("builtins.input", side_effect=AssertionError("非 TTY 不应提示确认")), patch(
        "scripts.reset_db.pymysql.connect"
    ) as connect:
        result = reset_db.main([])

    assert result != 0
    connect.assert_not_called()
    assert "--yes" in capsys.readouterr().err


def test_cli_without_yes_refuses_closed_stdin_before_connecting():
    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", None
    ), patch("scripts.reset_db.pymysql.connect") as connect:
        result = reset_db.main([])

    assert result != 0
    connect.assert_not_called()


def test_cli_tty_confirmation_mismatch_refuses_before_connecting(capsys):
    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", _fake_stdin(is_tty=True)
    ), patch("builtins.input", return_value="baicai_prod") as prompt, patch(
        "scripts.reset_db.pymysql.connect"
    ) as connect:
        result = reset_db.main([])

    assert result != 0
    prompt.assert_called_once()
    assert "baicai" in prompt.call_args.args[0]
    connect.assert_not_called()
    assert "不一致" in capsys.readouterr().err


def test_cli_tty_confirmation_interrupted_refuses_before_connecting():
    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", _fake_stdin(is_tty=True)
    ), patch("builtins.input", side_effect=EOFError), patch(
        "scripts.reset_db.pymysql.connect"
    ) as connect:
        result = reset_db.main([])

    assert result != 0
    connect.assert_not_called()


def test_cli_tty_confirmation_match_resets():
    conn, cursor = _fake_connection(["file_tasks"])

    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", _fake_stdin(is_tty=True)
    ), patch("builtins.input", return_value="baicai"), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ) as connect:
        result = reset_db.main([])

    assert result == 0
    connect.assert_called_once()
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "DELETE FROM `file_tasks`;" in statements
    conn.commit.assert_called_once_with()


def test_cli_dry_run_reports_counts_without_modifying(capsys):
    conn, cursor = _fake_connection(
        ["file_tasks", "account_upload_events", "drive_accounts"]
    )
    cursor.fetchone.side_effect = [(7,), (42,), (3,)]

    with patch.dict("os.environ", _configured_env(), clear=False), patch.object(
        reset_db.sys, "stdin", _fake_stdin(is_tty=False)
    ), patch("builtins.input", side_effect=AssertionError("dry-run 不应提示确认")), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ):
        result = reset_db.main(["--dry-run"])

    assert result == 0
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0] == "SHOW TABLES;"
    for statement in statements[1:]:
        assert statement.lstrip().upper().startswith("SELECT COUNT(*)")
    assert not any("DELETE" in s.upper() or "UPDATE" in s.upper() for s in statements)
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()

    lines = capsys.readouterr().out.splitlines()

    def line_for(table):
        matched = [line for line in lines if f"{table}:" in line]
        assert len(matched) == 1, lines
        return matched[0]

    assert "7 行" in line_for("account_upload_events")
    assert "42 行" in line_for("file_tasks")
    assert "不存在" in line_for("worker_registry")
    assert "3 " in line_for("drive_accounts")
    assert any("未修改" in line for line in lines)


def test_cli_dry_run_with_yes_still_does_not_modify():
    conn, cursor = _fake_connection(["file_tasks"])
    cursor.fetchone.return_value = (1,)

    with patch.dict("os.environ", _configured_env(), clear=False), patch(
        "scripts.reset_db.pymysql.connect", return_value=conn
    ):
        result = reset_db.main(["--dry-run", "--yes"])

    assert result == 0
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("DELETE" in s.upper() or "UPDATE" in s.upper() for s in statements)
    conn.commit.assert_not_called()


def test_script_entrypoint_refuses_without_yes_when_stdin_is_not_tty():
    root = Path(__file__).resolve().parents[1]
    # 保护失效时也只会解析一个不存在的主机，不会触达测试库。
    env = {**os.environ, **_configured_env(), "DB_HOST": "reset-db-guard.invalid"}
    result = subprocess.run(
        [sys.executable, str(root / "scripts/reset_db.py")],
        stdin=subprocess.DEVNULL, capture_output=True, text=True, env=env, timeout=30,
    )
    assert result.returncode != 0
    assert "--yes" in result.stderr
    assert "DB Error" not in result.stderr
