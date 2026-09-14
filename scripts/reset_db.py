import os
import sys

import pymysql


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


def reset_database() -> int:
    conn = None
    try:
        conn = pymysql.connect(
            host=require_env("DB_HOST"),
            user=require_env("DB_USER"),
            password=require_env("DB_PASSWORD"),
            database=require_env("DB_NAME"),
        )
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES;")
            tables = {row[0] for row in cur.fetchall()}
            print("Existing tables:", sorted(tables))

            # 先删引用任务的用量事件，再删任务；使用 DELETE 使失败可整体回滚，
            # 同时避免 MySQL 的 TRUNCATE 隐式提交和外键限制。
            for table in ("account_upload_events", "file_tasks", "worker_registry"):
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


if __name__ == "__main__":
    raise SystemExit(reset_database())
