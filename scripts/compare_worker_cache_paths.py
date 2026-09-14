#!/usr/bin/env python3
import argparse
import os
import unicodedata
from collections import Counter
from pathlib import Path

import pymysql


def load_env(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def read_paths(path: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            clean = unicodedata.normalize("NFC", raw.rstrip("\n").replace("\\", "/"))
            clean = clean.lstrip("./")
            if not clean or clean in seen:
                continue
            seen.add(clean)
            result.append(clean)
    return result


def chunks(items: list[str], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def fetch_task_rows(cursor, paths: list[str]) -> dict[str, dict]:
    rows_by_path: dict[str, dict] = {}
    for batch in chunks(paths, 1000):
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(
            f"""
            SELECT file_path, status, worker_node, upload_method, account_used, created_at, updated_at
            FROM file_tasks
            WHERE file_path IN ({placeholders})
            """,
            batch,
        )
        for row in cursor.fetchall():
            rows_by_path[row["file_path"]] = row
    return rows_by_path


def print_samples(title: str, paths: list[str], rows_by_path: dict[str, dict], limit: int) -> None:
    print(f"\n=== {title} ({len(paths)}) ===")
    if not paths:
        print("(none)")
        return
    for path in paths[:limit]:
        row = rows_by_path.get(path)
        if row:
            print(
                f"{path}\tstatus={row['status']}\tworker={row['worker_node']}"
                f"\tmethod={row['upload_method']}\taccount={row['account_used'] or '-'}"
                f"\tcreated={row['created_at']}\tupdated={row['updated_at']}"
            )
        else:
            print(path)


def main() -> int:
    # 节点名由调用者明确指定，公共工具不内置生产节点身份。
    parser = argparse.ArgumentParser(
        description="Compare a worker's local rclone-cache path list with Center file_tasks."
    )
    parser.add_argument("--local-list", required=True, help="Newline-delimited relative paths from /data/rclone-cache.")
    parser.add_argument("--reported-list", help="Optional newline-delimited paths from scanner reported_files for /data/rclone-cache.")
    parser.add_argument("--worker-node", required=True, help="Center worker_node to compare, for example worker-node-1.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=3306)
    parser.add_argument("--sample-limit", type=int, default=50)
    args = parser.parse_args()

    env = {**load_env(args.env_file), **os.environ}
    local_paths = read_paths(args.local_list)
    reported_paths = read_paths(args.reported_list) if args.reported_list else []

    conn = pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        database=env["DB_NAME"],
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT UTC_TIMESTAMP() AS utc_now, NOW() AS local_now")
            print("=== TIME ===")
            print(cursor.fetchone())

            rows_by_path = fetch_task_rows(cursor, local_paths)
            reported_rows_by_path = fetch_task_rows(cursor, reported_paths) if reported_paths else {}
    finally:
        conn.close()

    in_center = [path for path in local_paths if path in rows_by_path]
    missing_center = [path for path in local_paths if path not in rows_by_path]
    history_barrier = [
        path
        for path in in_center
        if rows_by_path[path]["worker_node"] == "Init_Scanner"
        and rows_by_path[path]["status"] == "completed"
        and rows_by_path[path]["account_used"] is None
    ]
    worker_move = [
        path
        for path in in_center
        if rows_by_path[path]["worker_node"] == args.worker_node
        and rows_by_path[path]["upload_method"] == "move"
    ]
    history_set = set(history_barrier)
    worker_move_set = set(worker_move)
    other_center = [
        path
        for path in in_center
        if path not in history_set and path not in worker_move_set
    ]

    print("\n=== SUMMARY ===")
    print(f"local_unique_paths={len(local_paths)}")
    print(f"local_paths_in_center={len(in_center)}")
    print(f"local_paths_missing_center={len(missing_center)}")
    print(f"local_paths_blocked_by_init_history={len(history_barrier)}")
    print(f"local_paths_as_worker_move_tasks={len(worker_move)}")
    print(f"local_paths_in_center_other_bucket={len(other_center)}")

    if reported_paths:
        local_set = set(local_paths)
        reported_set = set(reported_paths)
        reported_missing_center = [path for path in reported_paths if path not in reported_rows_by_path]
        print("\n=== SCANNER CACHE SUMMARY ===")
        print(f"reported_unique_paths={len(reported_paths)}")
        print(f"reported_paths_still_local={len(reported_set & local_set)}")
        print(f"reported_paths_not_currently_local={len(reported_set - local_set)}")
        print(f"reported_paths_missing_center={len(reported_missing_center)}")

    status_counter = Counter()
    worker_counter = Counter()
    method_counter = Counter()
    for path in in_center:
        row = rows_by_path[path]
        status_counter[(row["status"], row["upload_method"])] += 1
        worker_counter[row["worker_node"]] += 1
        method_counter[row["upload_method"]] += 1

    print("\n=== CENTER STATUS/METHOD FOR LOCAL PATHS ===")
    for key, count in status_counter.most_common():
        print(f"{key[0]}\t{key[1]}\t{count}")

    print("\n=== CENTER WORKER FOR LOCAL PATHS ===")
    for worker, count in worker_counter.most_common():
        print(f"{worker}\t{count}")

    print("\n=== CENTER METHOD FOR LOCAL PATHS ===")
    for method, count in method_counter.most_common():
        print(f"{method}\t{count}")

    print_samples("LOCAL PATHS MISSING FROM CENTER", missing_center, rows_by_path, args.sample_limit)
    print_samples("LOCAL PATHS BLOCKED BY INIT HISTORY", history_barrier, rows_by_path, args.sample_limit)
    print_samples("LOCAL PATHS ALREADY AS WORKER MOVE TASKS", worker_move, rows_by_path, args.sample_limit)
    print_samples("LOCAL PATHS IN CENTER OTHER BUCKET", other_center, rows_by_path, args.sample_limit)

    if reported_paths:
        reported_missing_center = [path for path in reported_paths if path not in reported_rows_by_path]
        print_samples(
            "SCANNER REPORTED PATHS MISSING FROM CENTER",
            reported_missing_center,
            reported_rows_by_path,
            args.sample_limit,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
