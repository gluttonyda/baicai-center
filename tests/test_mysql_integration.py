"""真实 MySQL 8 集成测试。

只在提供 BAICAI_TEST_MYSQL_HOST/USER/PASSWORD/DATABASE 时运行；每个测试创建独立的
review_ 前缀临时库并在结束后删除，不读写其他库。
"""
from pathlib import Path
from typing import Optional
from unittest.mock import patch
import hashlib
import os
import sys
import uuid

import pytest

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ALLOW_LEGACY_NODE_AUTH", "true")
os.environ.setdefault("ALLOW_LEGACY_UNLEASED_REPORTS", "true")
os.environ.setdefault("DB_HOST", "test-db-host")
os.environ.setdefault("DB_USER", "test-db-user")
os.environ.setdefault("DB_PASSWORD", "test-db-password")
os.environ.setdefault("DB_NAME", "test-db-name")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pymysql

import main

MYSQL_ENV_NAMES = (
    "BAICAI_TEST_MYSQL_HOST",
    "BAICAI_TEST_MYSQL_USER",
    "BAICAI_TEST_MYSQL_PASSWORD",
    "BAICAI_TEST_MYSQL_DATABASE",
)

pytestmark = pytest.mark.skipif(
    any(not os.getenv(name) for name in MYSQL_ENV_NAMES),
    reason="未配置 BAICAI_TEST_MYSQL_* 环境变量，跳过真实 MySQL 测试",
)


def mysql_settings() -> dict:
    return {
        "host": os.environ["BAICAI_TEST_MYSQL_HOST"],
        "user": os.environ["BAICAI_TEST_MYSQL_USER"],
        "password": os.environ["BAICAI_TEST_MYSQL_PASSWORD"],
    }


def connect(database=None, **kwargs):
    return pymysql.connect(
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        init_command="SET time_zone = '+00:00'",
        **mysql_settings(),
        **kwargs,
    )


def split_sql_script(script: str) -> list[str]:
    """按分号切分迁移脚本；忽略整行 -- 注释，并跳过单引号字符串内的分号。"""
    lines = [line for line in script.splitlines() if not line.strip().startswith("--")]
    statements: list[str] = []
    current: list[str] = []
    in_quote = False
    for char in "\n".join(lines):
        if char == "'":
            in_quote = not in_quote
        if char == ";" and not in_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def run_sql_file(connection, filename: str) -> None:
    with connection.cursor() as cursor:
        for statement in split_sql_script((ROOT / filename).read_text(encoding="utf-8")):
            cursor.execute(statement)
            while cursor.nextset():
                pass


@pytest.fixture
def review_db():
    database = f"review_{uuid.uuid4().hex[:16]}"
    assert database.startswith("review_")
    admin = connect()
    try:
        with admin.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        # 只删除本次成功创建的库；DROP 失败时外层 finally 仍会关闭管理连接。
        try:
            yield database
        finally:
            with admin.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS `{database}`")
    finally:
        admin.close()


@pytest.fixture
def db_conn(review_db):
    connection = connect(review_db)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def main_on_review_db(review_db):
    """让 main 的真实连接池指向临时库，测试结束后关闭池并恢复原配置。"""
    settings = mysql_settings()
    original_pool = main._db_pool
    main._db_pool = None
    try:
        with patch.multiple(
            main,
            DB_HOST=settings["host"],
            DB_USER=settings["user"],
            DB_PASSWORD=settings["password"],
            DB_NAME=review_db,
        ):
            yield review_db
            if main._db_pool is not None:
                main._db_pool.close()
    finally:
        main._db_pool = original_pool


@pytest.fixture
def schema_db(main_on_review_db, db_conn):
    run_sql_file(db_conn, "SCHEMA_MINIMAL.sql")
    return db_conn


def scalar(connection, sql: str, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
    return None if row is None else next(iter(row.values()))


HASH_COLUMN_LINE = "    file_path_hash CHAR(64) AS (SHA2(file_path, 256)) STORED,\n"
HASH_UNIQUE_KEY_LINE = "    UNIQUE KEY uk_file_tasks_file_path_hash (file_path_hash),\n"
HASH_PREFIX_UNIQUE_KEY_LINE = "    UNIQUE KEY prefix_hash (file_path_hash(1)),\n"
HASH_EXPRESSION_UNIQUE_KEY_LINE = "    UNIQUE KEY prefix_hash_expr ((LEFT(file_path_hash, 1))),\n"
FULL_HASH_UNIQUE_INDEX_PART = {
    "index_name": "uk_file_tasks_file_path_hash",
    "seq_in_index": 1,
    "column_name": "file_path_hash",
    "sub_part": None,
}
MIGRATION_FILES = (
    "MIGRATION_SPLIT_ACCOUNT_LOCKS.sql",
    "MIGRATION_UPLOAD_USAGE_STATS.sql",
    "MIGRATION_DISPATCH_PROTOCOL_V2.sql",
    "MIGRATION_FILE_PATH_HASH.sql",
)


def create_schema(connection, *, hash_column: bool = True, hash_unique: bool = True,
                  hash_column_line: str = HASH_COLUMN_LINE, hash_key_lines: Optional[str] = None) -> None:
    """hash_column_line/hash_key_lines 替换 file_path_hash 列与唯一索引行，用于构造异常旧结构。"""
    script = (ROOT / "SCHEMA_MINIMAL.sql").read_text(encoding="utf-8")
    assert HASH_COLUMN_LINE in script and HASH_UNIQUE_KEY_LINE in script
    if hash_key_lines is not None:
        script = script.replace(HASH_UNIQUE_KEY_LINE, hash_key_lines)
    elif not hash_unique or not hash_column:
        replacement = "    KEY idx_file_tasks_file_path_hash (file_path_hash),\n" if hash_column else ""
        script = script.replace(HASH_UNIQUE_KEY_LINE, replacement)
    if not hash_column:
        script = script.replace(HASH_COLUMN_LINE, "")
        assert "file_path_hash" not in script
    else:
        script = script.replace(HASH_COLUMN_LINE, hash_column_line)
    with connection.cursor() as cursor:
        for statement in split_sql_script(script):
            cursor.execute(statement)


def seed_old_tasks(connection, paths: list[str]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO worker_registry (node_name, api_url, protocol_version, last_active) "
            "VALUES ('worker-node-1', 'http://worker-node-1:8001/task/upload', 2, UTC_TIMESTAMP())"
        )
        cursor.execute(
            "INSERT INTO drive_accounts (account_name, role, is_active, quota_blocked_until) "
            "VALUES ('baicai_1@1', 'uploader', 0, '2030-01-01 00:00:00')"
        )
        for index, path in enumerate(paths):
            cursor.execute(
                "INSERT INTO file_tasks (file_path, status, worker_node, account_used, size_bytes) "
                "VALUES (%s, %s, 'worker-node-1', 'baicai_1@1', %s)",
                (path, "staged" if index % 2 else "pending", 100 + index),
            )


def table_snapshot(connection) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id, file_path, status, worker_node, account_used, size_bytes, created_at "
            "FROM file_tasks ORDER BY id"
        )
        return cursor.fetchall()


def file_tasks_index_parts(connection) -> list[dict]:
    """file_tasks 全部索引的逐列定义（含前缀长度与表达式），用于确认迁移没有增删或改写索引。"""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
            "COLUMN_NAME AS column_name, SUB_PART AS sub_part, EXPRESSION AS expression "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' "
            "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
        )
        return cursor.fetchall()


def unique_hash_index_parts(connection) -> list[dict]:
    return [
        {key: part[key] for key in FULL_HASH_UNIQUE_INDEX_PART}
        for part in file_tasks_index_parts(connection)
        if part["non_unique"] == 0 and part["column_name"] == "file_path_hash"
    ]


def hash_column_definition(connection) -> dict:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable, EXTRA AS extra, "
            "GENERATION_EXPRESSION AS generation_expression "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='file_path_hash'"
        )
        return cursor.fetchone()


@pytest.fixture
def api_client():
    from fastapi.testclient import TestClient

    return TestClient(main.app)


def test_health_ready_fails_when_file_path_hash_column_missing(main_on_review_db, db_conn, api_client):
    create_schema(db_conn, hash_column=False)

    assert api_client.get("/health/ready").status_code == 503


def test_health_ready_fails_when_file_path_hash_is_not_unique(main_on_review_db, db_conn, api_client):
    create_schema(db_conn, hash_unique=False)

    assert api_client.get("/health/ready").status_code == 503


def test_health_ready_passes_on_current_schema(schema_db, api_client):
    assert api_client.get("/health/ready").status_code == 200


def test_file_path_hash_migration_upgrades_old_schema_and_is_idempotent(
    main_on_review_db, db_conn, api_client
):
    create_schema(db_conn, hash_column=False)
    paths = ["movie/a.mkv", "Movie/A.mkv", "电影/中文 名称.mkv", " movie/space.mkv "]
    seed_old_tasks(db_conn, paths)
    before = table_snapshot(db_conn)

    run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")
    run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")

    assert table_snapshot(db_conn) == before
    assert unique_hash_index_parts(db_conn) == [FULL_HASH_UNIQUE_INDEX_PART]
    assert scalar(
        db_conn, "SELECT COUNT(*) FROM file_tasks WHERE file_path_hash <> SHA2(file_path, 256)"
    ) == 0
    assert scalar(db_conn, "SELECT quota_blocked_until FROM drive_accounts").year == 2030
    assert api_client.get("/health/ready").status_code == 200


def test_file_path_hash_migration_adds_missing_unique_index(main_on_review_db, db_conn, api_client):
    create_schema(db_conn, hash_unique=False)
    seed_old_tasks(db_conn, ["movie/a.mkv", "movie/b.mkv"])

    run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")
    run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")

    assert unique_hash_index_parts(db_conn) == [FULL_HASH_UNIQUE_INDEX_PART]
    assert api_client.get("/health/ready").status_code == 200


def test_file_path_hash_migration_refuses_duplicate_paths_without_deleting(
    main_on_review_db, db_conn, api_client
):
    create_schema(db_conn, hash_column=False)
    seed_old_tasks(db_conn, ["movie/dup.mkv", "movie/dup.mkv", "movie/unique.mkv"])
    before = table_snapshot(db_conn)

    with pytest.raises(pymysql.err.MySQLError) as error:
        run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")

    assert "MIGRATION_ABORTED_duplicate_file_path_rows" in str(error.value)
    assert table_snapshot(db_conn) == before
    assert api_client.get("/health/ready").status_code == 503


# 两个路径的完整 SHA-256 不同、首个十六进制字符相同；测试内用真实 MySQL SHA2 核实。
PREFIX_COLLISION_PATHS = ("movie/0.mkv", "movie/4.mkv")
PREFIX_UNIQUE_KEY_LAYOUTS = {
    "prefix_only": HASH_PREFIX_UNIQUE_KEY_LINE,
    "full_and_prefix": HASH_UNIQUE_KEY_LINE + HASH_PREFIX_UNIQUE_KEY_LINE,
    "full_and_expression_prefix": HASH_UNIQUE_KEY_LINE + HASH_EXPRESSION_UNIQUE_KEY_LINE,
}


@pytest.mark.parametrize(
    "hash_key_lines", PREFIX_UNIQUE_KEY_LAYOUTS.values(), ids=PREFIX_UNIQUE_KEY_LAYOUTS.keys()
)
def test_prefix_unique_hash_index_blocks_readiness_and_migration_without_changes(
    main_on_review_db, db_conn, api_client, hash_key_lines
):
    create_schema(db_conn, hash_key_lines=hash_key_lines)
    first, second = PREFIX_COLLISION_PATHS
    first_hash, second_hash = (scalar(db_conn, "SELECT SHA2(%s, 256)", (path,)) for path in PREFIX_COLLISION_PATHS)
    assert (first_hash, second_hash) == tuple(
        hashlib.sha256(path.encode("utf-8")).hexdigest() for path in PREFIX_COLLISION_PATHS
    )
    assert first_hash != second_hash and first_hash[0] == second_hash[0]

    with patch.object(main, "ALLOW_LEGACY_NODE_AUTH", True):
        report_files(api_client, [changed_file(first)])
        result = report_files(api_client, [changed_file(second)])

    # 前缀唯一索引把完整哈希不同的第二个路径判为重复，错误走 ON DUPLICATE KEY UPDATE 而没有入库。
    assert scalar(db_conn, "SELECT GROUP_CONCAT(file_path) FROM file_tasks") == first
    assert result["deferred_files"] == [second]

    rows_before = table_snapshot(db_conn)
    indexes_before = file_tasks_index_parts(db_conn)
    assert api_client.get("/health/ready").status_code == 503

    with pytest.raises(pymysql.err.MySQLError) as error:
        run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")

    assert "MIGRATION_ABORTED_prefix_unique_file_path_hash_index" in str(error.value)
    assert table_snapshot(db_conn) == rows_before
    assert file_tasks_index_parts(db_conn) == indexes_before
    assert api_client.get("/health/ready").status_code == 503


INCOMPATIBLE_HASH_COLUMN_LINES = {
    "plain_nullable": "    file_path_hash CHAR(64) NULL,\n",
    "wrong_expression": "    file_path_hash CHAR(64) AS (SHA2(LOWER(file_path), 256)) STORED,\n",
    "virtual": "    file_path_hash CHAR(64) AS (SHA2(file_path, 256)) VIRTUAL,\n",
    "varchar": "    file_path_hash VARCHAR(64) AS (SHA2(file_path, 256)) STORED,\n",
}


@pytest.mark.parametrize(
    "hash_column_line", INCOMPATIBLE_HASH_COLUMN_LINES.values(), ids=INCOMPATIBLE_HASH_COLUMN_LINES.keys()
)
def test_incompatible_hash_column_blocks_readiness_and_migration_without_changes(
    main_on_review_db, db_conn, api_client, hash_column_line
):
    create_schema(db_conn, hash_column_line=hash_column_line)
    seed_old_tasks(db_conn, ["movie/a.mkv", "movie/b.mkv"])
    rows_before = table_snapshot(db_conn)
    column_before = hash_column_definition(db_conn)
    indexes_before = file_tasks_index_parts(db_conn)

    assert api_client.get("/health/ready").status_code == 503

    with pytest.raises(pymysql.err.MySQLError) as error:
        run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")

    assert "MIGRATION_ABORTED_incompatible_file_path_hash_column" in str(error.value)
    assert table_snapshot(db_conn) == rows_before
    assert hash_column_definition(db_conn) == column_before
    assert file_tasks_index_parts(db_conn) == indexes_before
    assert api_client.get("/health/ready").status_code == 503


def test_equivalent_hash_column_definition_passes_readiness_and_migration(main_on_review_db, db_conn, api_client):
    create_schema(
        db_conn,
        hash_column_line="    file_path_hash char(64) GENERATED ALWAYS AS (sha2(`file_path`,256)) STORED NOT NULL,\n",
    )
    seed_old_tasks(db_conn, ["movie/a.mkv", "Movie/A.mkv"])
    rows_before = table_snapshot(db_conn)
    column_before = hash_column_definition(db_conn)

    assert api_client.get("/health/ready").status_code == 200
    run_sql_file(db_conn, "MIGRATION_FILE_PATH_HASH.sql")

    assert table_snapshot(db_conn) == rows_before
    assert hash_column_definition(db_conn) == column_before
    assert unique_hash_index_parts(db_conn) == [FULL_HASH_UNIQUE_INDEX_PART]
    assert api_client.get("/health/ready").status_code == 200


def test_all_migrations_are_repeatable_on_current_schema(schema_db, api_client):
    seed_old_tasks(schema_db, ["movie/a.mkv", "movie/b.mkv"])
    before = table_snapshot(schema_db)

    for _ in range(2):
        for filename in MIGRATION_FILES:
            run_sql_file(schema_db, filename)

    assert table_snapshot(schema_db) == before
    assert unique_hash_index_parts(schema_db) == [FULL_HASH_UNIQUE_INDEX_PART]
    assert api_client.get("/health/ready").status_code == 200


OLD_DISPATCH_ID = "a" * 32


def seed_upsert_fixture(connection) -> None:
    with connection.cursor() as cursor:
        for node_name in ("worker-node-1", "worker-node-2"):
            cursor.execute(
                "INSERT INTO worker_registry (node_name, api_url, protocol_version, last_active) "
                "VALUES (%s, %s, 2, UTC_TIMESTAMP())",
                (node_name, f"http://{node_name}:8001/task/upload"),
            )
        cursor.execute(
            "INSERT INTO drive_accounts (account_name, role, is_active, dispatch_locked_until, "
            "current_dispatch_id, current_worker_node) "
            "VALUES ('baicai_1@1', 'uploader', 1, DATE_ADD(UTC_TIMESTAMP(), INTERVAL 30 MINUTE), %s, 'worker-node-1')",
            (OLD_DISPATCH_ID,),
        )


def insert_task(connection, path: str, status: str, *, account_used=None, dispatch_id=None,
                size_bytes=100, source_mtime=1000.0, source_base_dir="/data/links",
                worker_node="worker-node-1", upload_method="copy") -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO file_tasks (file_path, source_base_dir, size_bytes, source_mtime, status, "
            "worker_node, upload_method, account_used, dispatch_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (path, source_base_dir, size_bytes, source_mtime, status, worker_node, upload_method,
             account_used, dispatch_id),
        )


def task_row(connection, path: str) -> dict:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT status, account_used, dispatch_id, size_bytes, source_mtime, source_base_dir, "
            "worker_node, upload_method FROM file_tasks WHERE file_path_hash=SHA2(%s, 256)",
            (path,),
        )
        return cursor.fetchone()


def report_files(api_client, files: list[dict], *, worker_node="worker-node-2", upload_method="move",
                 is_history_sync=False) -> dict:
    response = api_client.post(
        "/api/report_files",
        json={
            "worker_node": worker_node,
            "worker_api_url": f"http://{worker_node}:8001/task/upload",
            "protocol_version": 2,
            "files": files,
            "is_history_sync": is_history_sync,
            "upload_method": upload_method,
        },
        headers={"Authorization": f"Bearer {main.API_TOKEN}"},
    )
    assert response.status_code == 200, response.text
    return response.json()


def changed_file(path: str) -> dict:
    return {"path": path, "size_bytes": 200, "mtime": 2000.0, "source_base_dir": "/data/rclone-cache"}


@pytest.fixture
def upsert_db(schema_db):
    seed_upsert_fixture(schema_db)
    with patch.object(main, "ALLOW_LEGACY_NODE_AUTH", True):
        yield schema_db


def test_report_files_requeues_not_found_with_new_fingerprint_and_clears_old_binding(upsert_db, api_client):
    insert_task(upsert_db, "movie/not-found.mkv", "not_found", account_used="baicai_1@1")

    result = report_files(api_client, [changed_file("movie/not-found.mkv")])

    # 若后续 CASE 读取的是更新前的 status，会保留旧账号和旧指纹；真实 MySQL 按左到右读取新 status。
    assert task_row(upsert_db, "movie/not-found.mkv") == {
        "status": "pending",
        "account_used": None,
        "dispatch_id": None,
        "size_bytes": 200,
        "source_mtime": 2000.0,
        "source_base_dir": "/data/rclone-cache",
        "worker_node": "worker-node-2",
        "upload_method": "move",
    }
    assert result["deferred_files"] == []


def test_report_files_requeues_completed_file_when_fingerprint_changes(upsert_db, api_client):
    insert_task(upsert_db, "movie/replaced.mkv", "completed", account_used="baicai_1@1")

    result = report_files(api_client, [changed_file("movie/replaced.mkv")])

    row = task_row(upsert_db, "movie/replaced.mkv")
    assert row["status"] == "pending"
    assert (row["account_used"], row["dispatch_id"]) == (None, None)
    assert (row["size_bytes"], row["source_mtime"], row["worker_node"]) == (200, 2000.0, "worker-node-2")
    assert result["deferred_files"] == []


def test_report_files_keeps_completed_file_with_same_fingerprint(upsert_db, api_client):
    insert_task(upsert_db, "movie/done.mkv", "completed", account_used="baicai_1@1")

    result = report_files(
        api_client,
        [{"path": "movie/done.mkv", "size_bytes": 100, "mtime": 1000.0, "source_base_dir": "/data/links"}],
    )

    row = task_row(upsert_db, "movie/done.mkv")
    assert (row["status"], row["account_used"], row["worker_node"]) == ("completed", "baicai_1@1", "worker-node-1")
    assert result["deferred_files"] == []


def test_report_files_refreshes_pending_task_and_clears_preference(upsert_db, api_client):
    insert_task(upsert_db, "movie/pending.mkv", "pending", account_used="baicai_1@1")

    result = report_files(api_client, [changed_file("movie/pending.mkv")])

    row = task_row(upsert_db, "movie/pending.mkv")
    assert row["status"] == "pending"
    assert row["account_used"] is None
    assert (row["size_bytes"], row["source_mtime"], row["source_base_dir"]) == (200, 2000.0, "/data/rclone-cache")
    assert (row["worker_node"], row["upload_method"]) == ("worker-node-2", "move")
    assert result["deferred_files"] == []


@pytest.mark.parametrize("active_status", ["uploading", "staged"])
def test_report_files_preserves_active_dispatch_and_defers_changed_fingerprint(
    upsert_db, api_client, active_status
):
    insert_task(
        upsert_db, "movie/active.mkv", active_status, account_used="baicai_1@1", dispatch_id=OLD_DISPATCH_ID
    )
    before = task_row(upsert_db, "movie/active.mkv")

    result = report_files(api_client, [changed_file("movie/active.mkv")])

    assert task_row(upsert_db, "movie/active.mkv") == before
    assert result["deferred_files"] == ["movie/active.mkv"]
    lease = scalar(upsert_db, "SELECT current_dispatch_id FROM drive_accounts WHERE account_name='baicai_1@1'")
    assert lease == OLD_DISPATCH_ID


def test_report_files_history_sync_marks_new_paths_completed_without_touching_pending(upsert_db, api_client):
    insert_task(upsert_db, "movie/pending.mkv", "pending", account_used="baicai_1@1")

    report_files(
        api_client,
        ["movie/pending.mkv", "movie/history-only.mkv"],
        worker_node="Init_Scanner",
        is_history_sync=True,
    )

    pending = task_row(upsert_db, "movie/pending.mkv")
    assert (pending["status"], pending["account_used"], pending["size_bytes"]) == ("pending", "baicai_1@1", 100)
    assert pending["worker_node"] == "worker-node-1"
    assert task_row(upsert_db, "movie/history-only.mkv")["status"] == "completed"


def test_report_files_deduplicates_same_path_across_reports(upsert_db, api_client):
    report_files(api_client, [changed_file("movie/new.mkv")])
    report_files(api_client, [changed_file("movie/new.mkv"), changed_file("movie/new.mkv")])

    assert scalar(upsert_db, "SELECT COUNT(*) FROM file_tasks WHERE file_path='movie/new.mkv'") == 1
    assert task_row(upsert_db, "movie/new.mkv")["status"] == "pending"


def test_pooled_connection_recovers_after_server_side_disconnect(schema_db):
    first = main.get_db_connection()
    with first.cursor() as cursor:
        cursor.execute("SELECT CONNECTION_ID() AS connection_id")
        killed_id = cursor.fetchone()["connection_id"]
    first.close()

    with schema_db.cursor() as cursor:
        cursor.execute("KILL %s", (killed_id,))

    # 事务路径先调用 begin()，DBUtils 在此处不会自动重试，只能依赖取出连接时的 ping。
    second = main.get_db_connection()
    try:
        second.begin()
        with second.cursor() as cursor:
            cursor.execute("SELECT CONNECTION_ID() AS connection_id, @@session.time_zone AS tz")
            row = cursor.fetchone()
        second.commit()
    finally:
        second.close()

    assert row["connection_id"] != killed_id
    assert row["tz"] == "+00:00"


@pytest.mark.real_dispatch_db_lock
def test_dispatch_db_lock_skips_while_other_session_holds_lock(schema_db):
    lock_name = main._dispatch_db_lock_name()
    other_process = connect(main.DB_NAME)
    try:
        assert scalar(other_process, "SELECT GET_LOCK(%s, 0)", (lock_name,)) == 1
        with patch.object(main, "_dispatch_uploader_tasks_locked") as dispatch_body:
            assert main.dispatch_uploader_tasks() is False
        dispatch_body.assert_not_called()
        assert scalar(other_process, "SELECT RELEASE_LOCK(%s)", (lock_name,)) == 1
    finally:
        other_process.close()

    assert scalar(schema_db, "SELECT IS_FREE_LOCK(%s)", (lock_name,)) == 1


@pytest.mark.real_dispatch_db_lock
def test_dispatch_db_lock_is_held_during_run_and_released_after_exception(schema_db):
    lock_name = main._dispatch_db_lock_name()
    observed_free_lock: list[int] = []

    def failing_dispatch_body():
        observed_free_lock.append(scalar(schema_db, "SELECT IS_FREE_LOCK(%s)", (lock_name,)))
        raise RuntimeError("dispatch body failed")

    with patch.object(main, "_dispatch_uploader_tasks_locked", side_effect=failing_dispatch_body):
        with pytest.raises(RuntimeError):
            main.dispatch_uploader_tasks()

    assert observed_free_lock == [0]
    assert scalar(schema_db, "SELECT IS_FREE_LOCK(%s)", (lock_name,)) == 1
    assert main._dispatch_run_lock.locked() is False

    # 异常后仍可执行完整的真实调度 SQL（空库无节点时只做巡检）。
    assert main.dispatch_uploader_tasks() is True
    assert scalar(schema_db, "SELECT IS_FREE_LOCK(%s)", (lock_name,)) == 1


def test_mark_successful_uploads_matches_normalized_paths(schema_db):
    """库里存的是原始路径（可能带前导 / 或 ./），Worker 回报的是归一化路径，仍必须精确命中。"""
    with schema_db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO worker_registry (node_name, api_url, protocol_version, last_active) "
            "VALUES ('worker-node-1', 'http://worker-node-1:8001/task/upload', 2, UTC_TIMESTAMP())"
        )
        cursor.execute(
            "INSERT INTO drive_accounts (account_name, role, is_active) VALUES ('baicai_1@1', 'uploader', 1)"
        )
        for path in ("/movie/slash.mkv", "./movie/dot.mkv", "movie/plain.mkv", "movie/untouched.mkv"):
            cursor.execute(
                "INSERT INTO file_tasks (file_path, status, worker_node, account_used, size_bytes, dispatch_id) "
                "VALUES (%s, 'uploading', 'worker-node-1', 'baicai_1@1', 10, NULL)",
                (path,),
            )

    with schema_db.cursor() as cursor:
        main.mark_successful_uploads_by_files(
            cursor,
            "baicai_1@1",
            "staged",
            ["movie/slash.mkv", "movie/dot.mkv", "movie/plain.mkv"],
            None,
        )

    with schema_db.cursor() as cursor:
        cursor.execute("SELECT file_path, status FROM file_tasks ORDER BY file_path")
        statuses = {row["file_path"]: row["status"] for row in cursor.fetchall()}
        cursor.execute("SELECT COUNT(*) AS events FROM account_upload_events")
        events = cursor.fetchone()["events"]

    assert statuses["/movie/slash.mkv"] == "staged"
    assert statuses["./movie/dot.mkv"] == "staged"
    assert statuses["movie/plain.mkv"] == "staged"
    assert statuses["movie/untouched.mkv"] == "uploading"
    assert events == 3
