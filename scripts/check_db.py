import os

import pymysql


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


conn = pymysql.connect(
    host=require_env("DB_HOST"),
    user=require_env("DB_USER"),
    password=require_env("DB_PASSWORD"),
    database=require_env("DB_NAME"),
    cursorclass=pymysql.cursors.DictCursor,
)
with conn.cursor() as cur:
    cur.execute("SHOW CREATE TABLE file_tasks;")
    print(cur.fetchone()['Create Table'])
