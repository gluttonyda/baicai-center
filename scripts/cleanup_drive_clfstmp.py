import argparse
import configparser
from email import policy
from email.parser import BytesParser
import itertools
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone


# 公开脚本只提供配置路径默认值，真实 remote 必须在命令行显式指定。
CONFIG_PATH = os.path.expanduser(os.getenv("RCLONE_CONFIG", "~/.config/rclone/rclone.conf"))
REMOTES: list[str] = []
BASE_QUERY = "name contains '.clfstmp' and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
TARGET_SUFFIX = ".clfstmp"
PAGE_SIZE = 1000
MAX_WORKERS = max(1, int(os.getenv("CLFSTMP_DELETE_WORKERS", "1")))
DELETE_SLEEP_SECONDS = max(0.0, float(os.getenv("CLFSTMP_DELETE_SLEEP_SECONDS", "0.75")))
LOOP_SLEEP_SECONDS = max(1, int(os.getenv("CLFSTMP_DELETE_LOOP_SLEEP_SECONDS", "30")))
FAILED_BATCH_SLEEP_SECONDS = max(1, int(os.getenv("CLFSTMP_DELETE_FAILED_BATCH_SLEEP_SECONDS", "180")))
GET_MAX_ATTEMPTS = max(1, int(os.getenv("CLFSTMP_GET_MAX_ATTEMPTS", "10")))
GET_MAX_SLEEP_SECONDS = max(1.0, float(os.getenv("CLFSTMP_GET_MAX_SLEEP_SECONDS", "120")))
DELETE_MAX_ATTEMPTS = max(1, int(os.getenv("CLFSTMP_DELETE_MAX_ATTEMPTS", "4")))
DELETE_MAX_SLEEP_SECONDS = max(1.0, float(os.getenv("CLFSTMP_DELETE_MAX_SLEEP_SECONDS", "15")))
USE_BATCH_DELETE = os.getenv("CLFSTMP_USE_BATCH_DELETE", "1") != "0"
BATCH_DELETE_SIZE = max(1, min(100, int(os.getenv("CLFSTMP_BATCH_DELETE_SIZE", "50"))))


def log(message: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {message}", flush=True)


def refresh_remote(remote: str) -> None:
    subprocess.run(
        ["rclone", "--config", CONFIG_PATH, "about", f"{remote}:", "--drive-pacer-min-sleep", "1000ms", "--tpslimit", "1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def load_clients() -> list[dict[str, str]]:
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    clients: list[dict[str, str]] = []
    for remote in REMOTES:
        section = config[remote]
        clients.append(
            {
                "remote": remote,
                "token": json.loads(section["token"])["access_token"],
                "drive_id": section["team_drive"],
            }
        )
    return clients


def refresh_and_load_clients() -> list[dict[str, str]]:
    for remote in REMOTES:
        refresh_remote(remote)
    return load_clients()


def refresh_client(client: dict[str, str]) -> None:
    refresh_remote(client["remote"])
    refreshed = load_clients()
    replacement = next(item for item in refreshed if item["remote"] == client["remote"])
    client.update(replacement)


def request_json(
    client: dict[str, str],
    url: str,
    method: str = "GET",
    max_attempts: int = GET_MAX_ATTEMPTS,
    max_sleep_seconds: float = GET_MAX_SLEEP_SECONDS,
):
    for attempt in range(max_attempts):
        headers = {"Authorization": f"Bearer {client['token']}"}
        request = urllib.request.Request(url, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                if method == "DELETE":
                    return None
                return json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            if exc.code == 401 and attempt < max_attempts - 1:
                refresh_client(client)
                continue
            if exc.code == 404 and method == "DELETE":
                return None

            retryable = exc.code in (403, 429, 500, 502, 503, 504) and (
                "rateLimitExceeded" in body
                or "userRateLimitExceeded" in body
                or exc.code in (429, 500, 502, 503, 504)
            )
            if retryable and attempt < max_attempts - 1:
                sleep_for = min(max_sleep_seconds, (2**attempt) + random.uniform(0, 1.5))
                log(
                    f"RETRY remote={client['remote']} method={method} "
                    f"code={exc.code} sleep={sleep_for:.1f}s"
                )
                time.sleep(sleep_for)
                continue
            raise RuntimeError(f"{client['remote']} HTTP {exc.code}: {body[:1000]}") from exc


def is_target_file(item: dict[str, str]) -> bool:
    return item.get("name", "").casefold().endswith(TARGET_SUFFIX)


def list_batch(client: dict[str, str], modified_before: str) -> tuple[list[dict[str, str]], int]:
    files: list[dict[str, str]] = []
    skipped_non_suffix = 0
    page_token = None
    while len(files) < PAGE_SIZE:
        params = {
            "corpora": "drive",
            "driveId": client["drive_id"],
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
            "q": f"{BASE_QUERY} and modifiedTime < '{modified_before}'",
            "fields": "nextPageToken,files(id,name,size,modifiedTime)",
            "pageSize": str(PAGE_SIZE),
        }
        if page_token:
            params["pageToken"] = page_token
        url = "https://www.googleapis.com/drive/v3/files?" + urllib.parse.urlencode(params)
        data = request_json(client, url)
        for item in data.get("files", []):
            if is_target_file(item):
                files.append(item)
            else:
                skipped_non_suffix += 1
            if len(files) >= PAGE_SIZE:
                break
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return files, skipped_non_suffix


def delete_file(client: dict[str, str], file_id: str) -> None:
    url = f"https://www.googleapis.com/drive/v3/files/{urllib.parse.quote(file_id)}?supportsAllDrives=true"
    request_json(
        client,
        url,
        method="DELETE",
        max_attempts=DELETE_MAX_ATTEMPTS,
        max_sleep_seconds=DELETE_MAX_SLEEP_SECONDS,
    )


def build_batch_delete_body(items: list[dict[str, str]], boundary: str) -> bytes:
    parts: list[str] = []
    for index, item in enumerate(items):
        file_id = urllib.parse.quote(item["id"], safe="")
        parts.extend(
            [
                f"--{boundary}",
                "Content-Type: application/http",
                "Content-Transfer-Encoding: binary",
                f"Content-ID: {index}",
                "",
                f"DELETE /drive/v3/files/{file_id}?supportsAllDrives=true HTTP/1.1",
                "",
            ]
        )
    parts.extend([f"--{boundary}--", ""])
    return "\r\n".join(parts).encode("utf-8")


def request_batch_delete(client: dict[str, str], items: list[dict[str, str]]) -> tuple[str, bytes]:
    boundary = f"batch_{uuid.uuid4().hex}"
    body = build_batch_delete_body(items, boundary)
    url = "https://www.googleapis.com/batch/drive/v3"
    for attempt in range(DELETE_MAX_ATTEMPTS):
        headers = {
            "Authorization": f"Bearer {client['token']}",
            "Content-Type": f"multipart/mixed; boundary={boundary}",
        }
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.headers.get("Content-Type", ""), response.read()
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", "replace")
            if exc.code == 401 and attempt < DELETE_MAX_ATTEMPTS - 1:
                refresh_client(client)
                continue

            retryable = exc.code in (403, 429, 500, 502, 503, 504) and (
                "rateLimitExceeded" in response_body
                or "userRateLimitExceeded" in response_body
                or exc.code in (429, 500, 502, 503, 504)
            )
            if retryable and attempt < DELETE_MAX_ATTEMPTS - 1:
                sleep_for = min(DELETE_MAX_SLEEP_SECONDS, (2**attempt) + random.uniform(0, 1.5))
                log(
                    f"RETRY remote={client['remote']} method=BATCH_DELETE "
                    f"code={exc.code} sleep={sleep_for:.1f}s"
                )
                time.sleep(sleep_for)
                continue
            raise RuntimeError(f"{client['remote']} HTTP {exc.code}: {response_body[:1000]}") from exc


def parse_batch_delete_response(
    content_type: str,
    response_body: bytes,
    items: list[dict[str, str]],
) -> tuple[int, list[dict[str, str]], list[str]]:
    message_bytes = f"Content-Type: {content_type}\r\n\r\n".encode("utf-8") + response_body
    message = BytesParser(policy=policy.default).parsebytes(message_bytes)
    if not message.is_multipart():
        return 0, items, [response_body.decode("utf-8", "replace")[:1000]]

    deleted = 0
    failed_items: list[dict[str, str]] = []
    errors: list[str] = []
    parts = list(message.iter_parts())
    for index, part in enumerate(parts):
        payload = part.get_payload(decode=True) or b""
        status_line = payload.splitlines()[0].decode("utf-8", "replace") if payload else ""
        fields = status_line.split()
        status = int(fields[1]) if len(fields) >= 2 and fields[1].isdigit() else 0

        if 200 <= status < 300 or status == 404:
            deleted += 1
            continue

        if index < len(items):
            failed_items.append(items[index])
        errors.append(f"batch status={status}: {payload.decode('utf-8', 'replace')[:500]}")

    if len(parts) < len(items):
        failed_items.extend(items[len(parts):])
        errors.append(f"batch response missing {len(items) - len(parts)} parts")

    return deleted, failed_items, errors


def delete_files_batch(client: dict[str, str], items: list[dict[str, str]]) -> tuple[int, list[dict[str, str]], list[str]]:
    content_type, response_body = request_batch_delete(client, items)
    return parse_batch_delete_response(content_type, response_body, items)


def is_rate_limit_error(message: str) -> bool:
    normalized = message.casefold()
    return "ratelimitexceeded" in normalized or "quota exceeded" in normalized


def is_auth_error(message: str) -> bool:
    normalized = message.casefold()
    return "401" in normalized or "invalid_token" in normalized or "invalid authentication credentials" in normalized


def main(apply: bool = False, min_age_hours: float = 24.0) -> int:
    # 在调用任何远端 API 前确认目标，避免沿用某个部署环境的账号清单。
    if not REMOTES:
        raise ValueError("至少需要通过 --remote 指定一个 rclone remote")
    if min_age_hours <= 0:
        raise ValueError("min_age_hours must be greater than zero")
    modified_before = (
        datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    clients = refresh_and_load_clients()
    delete_cycle = itertools.cycle(clients)
    delete_cycle_lock = threading.Lock()
    total_deleted = 0
    loops = 0

    while True:
        loops += 1
        if loops % 20 == 0:
            clients = refresh_and_load_clients()
            delete_cycle = itertools.cycle(clients)
        try:
            batch, skipped_non_suffix = list_batch(clients[0], modified_before)
        except Exception as exc:
            error = str(exc)
            if is_auth_error(error):
                clients = refresh_and_load_clients()
                delete_cycle = itertools.cycle(clients)
                log(f"LIST_AUTH_REFRESH loop={loops} sleep={FAILED_BATCH_SLEEP_SECONDS}s")
                time.sleep(FAILED_BATCH_SLEEP_SECONDS)
                continue
            if is_rate_limit_error(error):
                log(f"LIST_RATE_LIMIT loop={loops} sleep={FAILED_BATCH_SLEEP_SECONDS}s error={error[:500]}")
                time.sleep(FAILED_BATCH_SLEEP_SECONDS)
                continue
            raise
        if not batch:
            log(f"DONE total_deleted={total_deleted} skipped_non_suffix={skipped_non_suffix}")
            return 0

        if not apply:
            log(
                f"DRY_RUN candidates={len(batch)} modified_before={modified_before} "
                "no files were deleted; rerun with --apply to delete"
            )
            for item in batch[:20]:
                log(
                    f"DRY_RUN_CANDIDATE id={item.get('id')} name={item.get('name')!r} "
                    f"modifiedTime={item.get('modifiedTime')}"
                )
            return 0

        log(
            f"BATCH loop={loops} found={len(batch)} "
            f"skipped_non_suffix={skipped_non_suffix} total_deleted={total_deleted}"
        )
        deleted = 0
        failed: list[str] = []

        def delete_one(item: dict[str, str]) -> None:
            errors: list[str] = []
            for _ in range(len(clients)):
                with delete_cycle_lock:
                    client = next(delete_cycle)
                try:
                    delete_file(client, item["id"])
                except Exception as exc:
                    error = str(exc)
                    errors.append(error)
                    if is_auth_error(error):
                        refresh_client(client)
                        log(f"DELETE_CLIENT_AUTH_REFRESH remote={client['remote']} trying_next=true")
                        continue
                    if not is_rate_limit_error(error):
                        raise
                    log(f"DELETE_CLIENT_RATE_LIMIT remote={client['remote']} trying_next=true")
                    continue

                time.sleep(DELETE_SLEEP_SECONDS)
                return

            raise RuntimeError("all delete clients rate-limited: " + " | ".join(errors[:3]))

        def delete_chunk(items: list[dict[str, str]]) -> tuple[int, list[str]]:
            errors: list[str] = []
            remaining = items
            deleted_in_chunk = 0
            for _ in range(len(clients)):
                with delete_cycle_lock:
                    client = next(delete_cycle)
                try:
                    chunk_deleted, remaining_after_delete, batch_errors = delete_files_batch(client, remaining)
                except Exception as exc:
                    error = str(exc)
                    errors.append(error)
                    if is_auth_error(error):
                        refresh_client(client)
                        log(f"BATCH_DELETE_CLIENT_AUTH_REFRESH remote={client['remote']} trying_next=true")
                        continue
                    if not is_rate_limit_error(error):
                        return deleted_in_chunk, errors
                    log(f"BATCH_DELETE_CLIENT_RATE_LIMIT remote={client['remote']} trying_next=true")
                    continue

                deleted_in_chunk += chunk_deleted
                if not remaining_after_delete:
                    time.sleep(DELETE_SLEEP_SECONDS)
                    return deleted_in_chunk, []

                errors.extend(batch_errors)
                retryable_batch_errors = all(
                    is_rate_limit_error(error) or is_auth_error(error) for error in batch_errors
                )
                if not retryable_batch_errors:
                    return deleted_in_chunk, errors
                if any(is_auth_error(error) for error in batch_errors):
                    refresh_client(client)
                    log(
                        f"BATCH_DELETE_CLIENT_AUTH_REFRESH remote={client['remote']} "
                        f"remaining={len(remaining_after_delete)} trying_next=true"
                    )
                remaining = remaining_after_delete
                log(
                    f"BATCH_DELETE_CLIENT_RATE_LIMIT remote={client['remote']} "
                    f"remaining={len(remaining)} trying_next=true"
                )

            return deleted_in_chunk, ["all batch delete clients unavailable: " + " | ".join(errors[:3])]

        next_progress = 100
        if USE_BATCH_DELETE:
            for start in range(0, len(batch), BATCH_DELETE_SIZE):
                chunk = batch[start:start + BATCH_DELETE_SIZE]
                chunk_deleted, chunk_errors = delete_chunk(chunk)
                deleted += chunk_deleted
                while deleted >= next_progress:
                    log(f"BATCH_PROGRESS loop={loops} deleted={next_progress}/{len(batch)}")
                    next_progress += 100
                if chunk_errors:
                    failed.extend(chunk_errors)
                    break
        else:
            for item in batch:
                try:
                    delete_one(item)
                except Exception as exc:
                    failed.append(str(exc))
                    break

                deleted += 1
                while deleted >= next_progress:
                    log(f"BATCH_PROGRESS loop={loops} deleted={next_progress}/{len(batch)}")
                    next_progress += 100

        total_deleted += deleted
        log(f"BATCH_DONE loop={loops} deleted={deleted} failed={len(failed)} total_deleted={total_deleted}")
        for error in failed[:5]:
            log(f"ERROR_SAMPLE {error}")
        if failed:
            if any(is_auth_error(error) for error in failed):
                clients = refresh_and_load_clients()
                delete_cycle = itertools.cycle(clients)
            if not all(is_rate_limit_error(error) or is_auth_error(error) for error in failed):
                return 1
            log(f"BATCH_RETRY_LATER loop={loops} sleep={FAILED_BATCH_SLEEP_SECONDS}s")
            time.sleep(FAILED_BATCH_SLEEP_SECONDS)
            continue
        time.sleep(LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Safely find or delete stale Google Drive .clfstmp objects."
    )
    parser.add_argument("--config", default=CONFIG_PATH, help="rclone configuration file path")
    parser.add_argument(
        "--remote", action="append", required=True,
        help="rclone remote name; repeat only for accounts accessing the same shared drive",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="permanently delete candidates; without this flag the command is read-only",
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=24.0,
        help="only include objects older than this many hours (default: 24)",
    )
    args = parser.parse_args()
    CONFIG_PATH = os.path.expanduser(args.config)
    REMOTES = args.remote
    sys.exit(main(apply=args.apply, min_age_hours=args.min_age_hours))
