import os

import pymysql


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


db = pymysql.connect(
    host=require_env("DB_HOST"),
    user=require_env("DB_USER"),
    password=require_env("DB_PASSWORD"),
    database=require_env("DB_NAME"),
    cursorclass=pymysql.cursors.DictCursor,
)
with db.cursor() as cursor:
    cursor.execute("SELECT status, COUNT(*) as count FROM file_tasks GROUP BY status")
    status_counts = cursor.fetchall()
    print("=== 全局状态统计 ===")
    for row in status_counts:
        print(f"{row['status']}: {row['count']}")
        
    cursor.execute("SELECT worker_node, COUNT(*) as count FROM file_tasks WHERE status='pending' GROUP BY worker_node")
    pending_nodes = cursor.fetchall()
    print("\n=== 各节点待搬运任务 (pending) ===")
    for row in pending_nodes:
        print(f"{row['worker_node']}: {row['count']}")
