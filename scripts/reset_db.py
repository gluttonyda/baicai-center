import os

import pymysql


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


try:
    conn = pymysql.connect(
        host=require_env("DB_HOST"),
        user=require_env("DB_USER"),
        password=require_env("DB_PASSWORD"),
        database=require_env("DB_NAME"),
    )
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES;")
        tables = [row[0] for row in cur.fetchall()]
        print("Existing tables:", tables)
        if "file_tasks" in tables:
            cur.execute("TRUNCATE TABLE file_tasks;")
            print("Truncated file_tasks.")
        if "worker_registry" in tables:
            cur.execute("TRUNCATE TABLE worker_registry;")
            print("Truncated worker_registry.")
        conn.commit()
except Exception as e:
    print("DB Error:", e)
