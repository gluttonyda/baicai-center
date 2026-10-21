import argparse
import os
import sys
from typing import Optional, Sequence

import pymysql

# 先删引用任务的用量事件，再删任务，顺序即外键从子表到父表。
TASK_TABLES = ("account_upload_events", "file_tasks", "worker_registry")


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


def load_db_config() -> dict:
    return {
        "host": require_env("DB_HOST"),
        "user": require_env("DB_USER"),
        "password": require_env("DB_PASSWORD"),
        "database": require_env("DB_NAME"),
    }


def confirm_reset(db_name: str, assume_yes: bool) -> bool:
    """连接数据库之前确认删除；返回 False 时调用方不得建立连接。"""
    if assume_yes:
        return True
    if sys.stdin is None or not sys.stdin.isatty():
        print(
            "拒绝执行：非交互环境必须显式传入 --yes；可先用 --dry-run 预览将清理的数据。",
            file=sys.stderr,
        )
        return False
    try:
        answer = input(f"将清空数据库 {db_name} 的任务数据，请输入数据库名确认: ")
    except (EOFError, KeyboardInterrupt):
        print("\n未确认，已取消，未连接数据库。", file=sys.stderr)
        return False
    if answer.strip() != db_name:
        print("输入与数据库名不一致，已取消，未连接数据库。", file=sys.stderr)
        return False
    return True


def preview_reset(cur, tables: set) -> None:
    """只统计将被清理的行数，不执行任何写操作。"""
    for table in TASK_TABLES:
        if table not in tables:
            print(f"[dry-run] {table}: 表不存在，跳过")
            continue
        cur.execute(f"SELECT COUNT(*) FROM `{table}`;")
        print(f"[dry-run] {table}: 将删除 {cur.fetchone()[0]} 行")
    if "drive_accounts" in tables:
        cur.execute(
            """
            SELECT COUNT(*) FROM drive_accounts
            WHERE dispatch_locked_until IS NOT NULL
               OR current_dispatch_id IS NOT NULL
               OR current_worker_node IS NOT NULL
            """
        )
        print(f"[dry-run] drive_accounts: 当前有执行租约的账号 {cur.fetchone()[0]} 个（全部账号的租约字段都会被清空）")
    print("[dry-run] 未修改任何数据。")


def reset_database(assume_yes: bool = False, dry_run: bool = False) -> int:
    try:
        config = load_db_config()
    except RuntimeError as exc:
        print("DB Error:", exc, file=sys.stderr)
        return 1
    # dry-run 只读，不需要确认；真正删除前必须在连接之前通过确认。
    if not dry_run and not confirm_reset(config["database"], assume_yes):
        return 1

    conn = None
    try:
        conn = pymysql.connect(**config)
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES;")
            tables = {row[0] for row in cur.fetchall()}
            print("Existing tables:", sorted(tables))
            if dry_run:
                preview_reset(cur, tables)
                return 0

            # 使用 DELETE 使失败可整体回滚，同时避免 MySQL 的 TRUNCATE 隐式提交和外键限制。
            for table in TASK_TABLES:
                if table in tables:
                    cur.execute(f"DELETE FROM `{table}`;")
                    print(f"Cleared {table}.")
            if "drive_accounts" in tables:
                cur.execute(
                    """
                    UPDATE drive_accounts
                    SET dispatch_locked_until=NULL,
                        current_dispatch_id=NULL,
                        current_worker_node=NULL
                    """
                )
                print("Cleared drive account execution leases.")
        conn.commit()
        return 0
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        print("DB Error:", exc, file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="清空 Center 的任务、上传统计与节点注册数据，并释放账号执行租约。"
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="跳过交互确认直接删除；非交互环境（CI、管道、cron）必须显式传入",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将清理的表与行数，不删除、不提交",
    )
    args = parser.parse_args(argv)
    return reset_database(assume_yes=args.yes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
