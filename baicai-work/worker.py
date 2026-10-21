import os
import hashlib
import hmac
import json
import logging
import math
import re
import selectors
import sqlite3
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Literal, Optional

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator, model_validator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 核心配置区 (根据新节点修改) =================
def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


ALLOW_LEGACY_BEARER_AUTH = os.getenv("ALLOW_LEGACY_BEARER_AUTH", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
# 共享 API_TOKEN 只用于 legacy Bearer 兼容；HMAC-only 部署可以不配置。
API_TOKEN = require_env("API_TOKEN") if ALLOW_LEGACY_BEARER_AUTH else os.getenv("API_TOKEN", "").strip()
NODE_TOKEN = require_env("NODE_TOKEN")
CENTER_URL = require_env("CENTER_URL").rstrip("/")
TARGET_REMOTE_DIR = require_env("TARGET_REMOTE_DIR")
WORKER_NODE_NAME = require_env("WORKER_NODE_NAME")
ALLOWED_BASE_DIRS = [path.strip() for path in os.getenv("ALLOWED_BASE_DIRS", "/data/links,/cache/links,/data/rclone-cache").split(",") if path.strip()]
CLEANER_REPORT_BATCH_SIZE = max(1, int(os.getenv("CLEANER_REPORT_BATCH_SIZE", "1000")))
TASK_JOURNAL_DB_PATH = os.getenv(
    "TASK_JOURNAL_DB_PATH", "/opt/baicai-worker/runtime/task_journal.db"
).strip()
TASK_JOURNAL_RETENTION_DAYS = max(1, int(os.getenv("TASK_JOURNAL_RETENTION_DAYS", "30")))
REPORT_RETRY_INTERVAL_SECONDS = max(1, int(os.getenv("REPORT_RETRY_INTERVAL_SECONDS", "30")))
REMOTE_PROBE_TIMEOUT_SECONDS = max(1, int(os.getenv("REMOTE_PROBE_TIMEOUT_SECONDS", "120")))
NODE_SIGNATURE_MAX_AGE_SECONDS = max(30, int(os.getenv("NODE_SIGNATURE_MAX_AGE_SECONDS", "300")))
RCLONE_BWLIMIT = os.getenv("RCLONE_BWLIMIT", "").strip()
RCLONE_TRANSFERS = max(1, int(os.getenv("RCLONE_TRANSFERS", "4")))
RCLONE_CHECKERS = max(1, int(os.getenv("RCLONE_CHECKERS", "8")))
MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY = max(1, int(os.getenv("MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY", "2")))
MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT = max(
    MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY,
    int(os.getenv("MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT", "3")),
)
UPLOAD_CONCURRENCY_NIGHT_START_HOUR = int(os.getenv("UPLOAD_CONCURRENCY_NIGHT_START_HOUR", "1"))
UPLOAD_CONCURRENCY_NIGHT_END_HOUR = int(os.getenv("UPLOAD_CONCURRENCY_NIGHT_END_HOUR", "10"))
MAX_CONCURRENT_CLEANERS_PER_WORKER = max(1, int(os.getenv("MAX_CONCURRENT_CLEANERS_PER_WORKER", "1")))
BEIJING_TZ = timezone(timedelta(hours=8))
IGNORED_UPLOAD_EXTENSIONS = (".clfstmp", ".clfstmp.progress")

app = FastAPI(title="Baicai Worker Node")
security = HTTPBearer(auto_error=False)
active_upload_accounts: set[str] = set()
active_upload_accounts_lock = threading.Lock()
active_cleaner_accounts: set[str] = set()
active_cleaner_accounts_lock = threading.Lock()
journal_lock = threading.Lock()
report_retry_stop = threading.Event()
cancelled_dispatches: set[str] = set()
cancelled_dispatches_lock = threading.Lock()
current_task_payload: ContextVar[Optional["TaskPayload"]] = ContextVar(
    "current_task_payload", default=None
)


def secure_equals(left: str, right: str) -> bool:
    # compare_digest 对含非 ASCII 的 str 会抛 TypeError，统一按 UTF-8 字节做常量时间比较。
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


# 账号名会拼成 rclone 参数 "<name>:<dir>"：禁止以 - 开头（选项注入）、冒号/逗号（连接串与后端参数）、
# 路径分隔符、空白和控制符；兼容现有 baicai_1@1、baicai-1@1-3、baicai-big2 等命名。
SAFE_REMOTE_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}")


def is_safe_remote_name(value: str) -> bool:
    return SAFE_REMOTE_NAME_PATTERN.fullmatch(value) is not None

class TaskPayload(BaseModel):
    dispatch_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    worker_node: Optional[str] = Field(default=None, min_length=1, max_length=128)
    task_type: Literal["upload", "clean"] = "upload"
    upload_method: Literal["copy", "move"] = "copy"
    account_name: str = Field(min_length=1, max_length=128)
    dest_name: Optional[str] = Field(default="baicai-big2", max_length=128)
    remote_dir: str = Field(default=TARGET_REMOTE_DIR, min_length=1, max_length=1024)
    files: List[str] = Field(default_factory=list)
    source_base_dirs: List[Optional[str]] = Field(default_factory=list)
    expected_sizes: List[Optional[int]] = Field(default_factory=list)
    expected_mtimes: List[Optional[float]] = Field(default_factory=list)

    @field_validator("dispatch_id")
    @classmethod
    def validate_dispatch_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if not value or "\r" in value or "\n" in value:
            raise ValueError("dispatch_id 非法")
        return value

    @field_validator("worker_node")
    @classmethod
    def validate_worker_node(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if any(char in value for char in ("\x00", "\r", "\n")):
            raise ValueError("worker_node 非法")
        if not secure_equals(value, WORKER_NODE_NAME):
            raise ValueError("任务 worker_node 与本节点身份不匹配")
        return value

    @field_validator("account_name")
    @classmethod
    def validate_account_name(cls, value: str) -> str:
        if not is_safe_remote_name(value):
            raise ValueError("account_name 必须是安全的 rclone remote 名称")
        return value

    @field_validator("dest_name")
    @classmethod
    def validate_dest_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        if not is_safe_remote_name(value):
            raise ValueError("dest_name 必须是安全的 rclone remote 名称")
        return value

    @field_validator("remote_dir")
    @classmethod
    def validate_remote_dir(cls, value: str) -> str:
        value = value.strip().strip("/")
        if not value or any(char in value for char in ("\x00", "\r", "\n", ":")):
            raise ValueError("remote_dir 非法")
        if any(part in ("", ".", "..") for part in value.split("/")):
            raise ValueError("remote_dir 包含非法路径段")
        return value

    @field_validator("files")
    @classmethod
    def validate_files(cls, value: List[str]) -> List[str]:
        normalized_files = []
        for file_path in value:
            if file_path == "":
                raise ValueError("files 中不能存在空路径")
            if any(char in file_path for char in ("\x00", "\r", "\n")) or len(file_path) > 1024:
                raise ValueError("files 中存在非法路径")
            normalized_files.append(file_path)
        return normalized_files

    @field_validator("source_base_dirs")
    @classmethod
    def validate_source_base_dirs(cls, value: List[Optional[str]]) -> List[Optional[str]]:
        allowed_by_realpath = {
            os.path.realpath(base_dir): base_dir for base_dir in ALLOWED_BASE_DIRS
        }
        normalized_dirs: list[Optional[str]] = []
        for base_dir in value:
            if base_dir is None:
                normalized_dirs.append(None)
                continue
            if base_dir == "" or any(char in base_dir for char in ("\x00", "\r", "\n")):
                raise ValueError("source_base_dirs 中存在非法目录")
            configured_dir = allowed_by_realpath.get(os.path.realpath(base_dir))
            if configured_dir is None:
                raise ValueError(f"source_base_dirs 目录不在白名单中: {base_dir}")
            normalized_dirs.append(configured_dir)
        return normalized_dirs

    @field_validator("expected_sizes")
    @classmethod
    def validate_expected_sizes(cls, value: List[Optional[int]]) -> List[Optional[int]]:
        if any(size is not None and size < 0 for size in value):
            raise ValueError("expected_sizes 不能包含负数")
        return value

    @field_validator("expected_mtimes")
    @classmethod
    def validate_expected_mtimes(cls, value: List[Optional[float]]) -> List[Optional[float]]:
        if any(mtime is not None and (mtime < 0 or not math.isfinite(mtime)) for mtime in value):
            raise ValueError("expected_mtimes 包含非法时间")
        return value

    @model_validator(mode="after")
    def validate_parallel_file_metadata(self):
        if self.source_base_dirs and len(self.source_base_dirs) != len(self.files):
            raise ValueError("source_base_dirs 必须为空或与 files 等长")
        if self.expected_sizes and len(self.expected_sizes) != len(self.files):
            raise ValueError("expected_sizes 必须为空或与 files 等长")
        if self.expected_mtimes and len(self.expected_mtimes) != len(self.files):
            raise ValueError("expected_mtimes 必须为空或与 files 等长")
        return self


class ReportStatusError(RuntimeError):
    pass


class LeaseLostError(ReportStatusError):
    pass


def cancel_dispatch(dispatch_id: Optional[str]) -> None:
    if dispatch_id:
        with cancelled_dispatches_lock:
            cancelled_dispatches.add(dispatch_id)


def clear_dispatch_cancellation(dispatch_id: Optional[str]) -> None:
    if dispatch_id:
        with cancelled_dispatches_lock:
            cancelled_dispatches.discard(dispatch_id)


def is_dispatch_cancelled(dispatch_id: Optional[str]) -> bool:
    if not dispatch_id:
        return False
    with cancelled_dispatches_lock:
        return dispatch_id in cancelled_dispatches


FINAL_STATUSES = {
    "completed",
    "failed",
    "403_quota",
    "clean_success",
    "not_found",
    "credential_invalid",
}


@contextmanager
def _journal_connection() -> Iterator[sqlite3.Connection]:
    journal_dir = os.path.dirname(TASK_JOURNAL_DB_PATH)
    if journal_dir:
        os.makedirs(journal_dir, exist_ok=True)
    connection = sqlite3.connect(TASK_JOURNAL_DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_task_journal() -> None:
    with journal_lock, _journal_connection() as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS task_journal (
                dispatch_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                state TEXT NOT NULL,
                final_report_json TEXT,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_task_journal_state_updated "
            "ON task_journal(state, updated_at)"
        )


def journal_prune_terminal_tasks() -> int:
    initialize_task_journal()
    cutoff = time.time() - TASK_JOURNAL_RETENTION_DAYS * 86400
    with journal_lock, _journal_connection() as connection:
        cursor = connection.execute(
            "DELETE FROM task_journal WHERE state IN ('completed', 'cancelled') AND updated_at < ?",
            (cutoff,),
        )
        return cursor.rowcount


def serialize_task_payload(payload: TaskPayload) -> str:
    return json.dumps(payload.model_dump(mode="json"), ensure_ascii=True, sort_keys=True)


def journal_get_task(dispatch_id: str) -> Optional[dict]:
    initialize_task_journal()
    with journal_lock, _journal_connection() as connection:
        row = connection.execute(
            "SELECT * FROM task_journal WHERE dispatch_id=?", (dispatch_id,)
        ).fetchone()
    return dict(row) if row is not None else None


def journal_accept_task(payload: TaskPayload) -> tuple[bool, dict]:
    """以 dispatch_id 幂等落盘；同一批次内容变化时拒绝接纳，防止重复执行或任务串用。"""
    if not payload.dispatch_id:
        raise ValueError("持久化任务必须包含 dispatch_id")
    initialize_task_journal()
    payload_json = serialize_task_payload(payload)
    now = time.time()
    with journal_lock, _journal_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO task_journal
                (dispatch_id, payload_json, state, created_at, updated_at)
            VALUES (?, ?, 'accepted', ?, ?)
            """,
            (payload.dispatch_id, payload_json, now, now),
        )
        row = connection.execute(
            "SELECT * FROM task_journal WHERE dispatch_id=?", (payload.dispatch_id,)
        ).fetchone()
    assert row is not None
    existing = dict(row)
    if existing["payload_json"] != payload_json:
        raise ValueError("同一 dispatch_id 对应了不同任务内容")
    return cursor.rowcount == 1, existing


def journal_set_state(dispatch_id: Optional[str], state: str) -> None:
    if not dispatch_id:
        return
    initialize_task_journal()
    with journal_lock, _journal_connection() as connection:
        connection.execute(
            "UPDATE task_journal SET state=?, updated_at=? WHERE dispatch_id=?",
            (state, time.time(), dispatch_id),
        )


def journal_store_final_report(dispatch_id: Optional[str], report_payload: dict) -> None:
    if not dispatch_id:
        return
    initialize_task_journal()
    report_json = json.dumps(report_payload, ensure_ascii=True, sort_keys=True)
    with journal_lock, _journal_connection() as connection:
        connection.execute(
            """
            UPDATE task_journal
            SET state='pending_report', final_report_json=?, last_error=NULL, updated_at=?
            WHERE dispatch_id=? AND state != 'completed'
            """,
            (report_json, time.time(), dispatch_id),
        )


def journal_mark_report_error(dispatch_id: Optional[str], error: Exception) -> None:
    if not dispatch_id:
        return
    initialize_task_journal()
    with journal_lock, _journal_connection() as connection:
        connection.execute(
            "UPDATE task_journal SET last_error=?, updated_at=? WHERE dispatch_id=?",
            (str(error)[:2000], time.time(), dispatch_id),
        )


def journal_mark_completed(dispatch_id: Optional[str]) -> None:
    if not dispatch_id:
        return
    initialize_task_journal()
    with journal_lock, _journal_connection() as connection:
        connection.execute(
            """
            UPDATE task_journal
            SET state='completed', last_error=NULL, updated_at=?
            WHERE dispatch_id=?
            """,
            (time.time(), dispatch_id),
        )


def journal_list_tasks(*states: str) -> list[dict]:
    if not states:
        return []
    initialize_task_journal()
    placeholders = ",".join("?" for _ in states)
    with journal_lock, _journal_connection() as connection:
        rows = connection.execute(
            f"SELECT * FROM task_journal WHERE state IN ({placeholders}) ORDER BY created_at",
            states,
        ).fetchall()
    return [dict(row) for row in rows]


def is_credential_invalid_error(line: str) -> bool:
    normalized = line.lower()
    markers = (
        "invalid_grant",
        "couldn't fetch token",
        "token expired",
        "maybe token expired",
        "config reconnect",
    )
    return any(marker in normalized for marker in markers)


def is_quota_error(line: str) -> bool:
    normalized = line.casefold()
    markers = (
        "user rate limit exceeded",
        "userratelimitexceeded",
        "rate limit exceeded",
        "ratelimitexceeded",
        "quota exceeded",
        "storagequotaexceeded",
        "upload limit",
        "stop-on-upload-limit",
    )
    return any(marker in normalized for marker in markers)


# rclone 文本日志为 "<日期> INFO  : <对象路径>: <动作>"，对象路径原样输出，可能带首尾空格或 ": "。
# 只去掉 "INFO  : " 后的一个分隔空格，再贪婪匹配到行尾的动作，路径不会被截断或错配。
RCLONE_TRANSFER_LOG_PATTERN = re.compile(
    r"INFO\s*: (.*): (Copied(?: \([^)]*\))?|Moved(?: \([^)]*\))?|Skipped|Deleted)$"
)


def has_unsendable_characters(file_path: str) -> bool:
    """rclone 的 local backend 会把 C0/DEL 转义成可见符号（Tab→U+2409），原样清单匹配不到。"""
    return any(ord(char) < 32 or ord(char) == 127 for char in file_path)


def parse_rclone_transfer_line(line: str) -> Optional[tuple[str, str]]:
    match = RCLONE_TRANSFER_LOG_PATTERN.search(line.rstrip("\r\n"))
    if match is None:
        return None
    return match.group(1), match.group(2)


def current_beijing_time() -> datetime:
    return datetime.now(BEIJING_TZ)


def current_upload_concurrency_limit(now_bj: Optional[datetime] = None) -> int:
    now_bj = now_bj or current_beijing_time()
    if UPLOAD_CONCURRENCY_NIGHT_START_HOUR <= now_bj.hour < UPLOAD_CONCURRENCY_NIGHT_END_HOUR:
        return MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT
    return MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY


def should_ignore_upload_file(file_path: str) -> bool:
    return file_path.casefold().endswith(IGNORED_UPLOAD_EXTENSIONS)


def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Security(security)):
    if credentials is None or not API_TOKEN or not secure_equals(credentials.credentials, API_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.credentials


def make_node_signature(timestamp: str, raw_body: bytes) -> str:
    message = timestamp.encode("ascii") + b"\n" + raw_body
    return hmac.new(NODE_TOKEN.encode("utf-8"), message, hashlib.sha256).hexdigest()


async def verify_task_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> str:
    node_name = request.headers.get("X-Baicai-Node", "")
    timestamp = request.headers.get("X-Baicai-Timestamp", "")
    provided_signature = request.headers.get("X-Baicai-Signature", "")
    if node_name and timestamp and provided_signature:
        if not secure_equals(node_name, WORKER_NODE_NAME):
            raise HTTPException(status_code=401, detail="node identity mismatch")
        try:
            signed_at = int(timestamp)
        except ValueError as error:
            raise HTTPException(status_code=401, detail="invalid signature timestamp") from error
        if abs(int(time.time()) - signed_at) > NODE_SIGNATURE_MAX_AGE_SECONDS:
            raise HTTPException(status_code=401, detail="expired signature timestamp")
        expected_signature = make_node_signature(timestamp, await request.body())
        if not secure_equals(provided_signature, expected_signature):
            raise HTTPException(status_code=401, detail="invalid node signature")
        return node_name

    if (
        ALLOW_LEGACY_BEARER_AUTH
        and API_TOKEN
        and credentials is not None
        and secure_equals(credentials.credentials, API_TOKEN)
    ):
        return "legacy"
    raise HTTPException(status_code=401, detail="node signature required")


def _post_signed_payload(path: str, payload: dict) -> httpx.Response:
    raw_body = json.dumps(
        payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    timestamp = str(int(time.time()))
    response = httpx.post(
        f"{CENTER_URL}{path}",
        content=raw_body,
        headers={
            "Content-Type": "application/json",
            "X-Baicai-Node": WORKER_NODE_NAME,
            "X-Baicai-Timestamp": timestamp,
            "X-Baicai-Signature": make_node_signature(timestamp, raw_body),
        },
        timeout=10.0,
    )
    response.raise_for_status()
    return response


def _post_report_payload(report_payload: dict) -> None:
    response = _post_signed_payload("/api/report_status", report_payload)
    result = response.json()
    if (
        isinstance(result, dict)
        and result.get("status") == "ignored"
        and result.get("reason") == "stale_dispatch"
    ):
        dispatch_id = report_payload.get("dispatch_id")
        cancel_dispatch(dispatch_id)
        raise LeaseLostError(f"dispatch lease lost: {dispatch_id}")


def validate_recovery_lease(payload: TaskPayload) -> Optional[bool]:
    """恢复前向中心核对租约；None 表示暂时无法确认，应保留 journal 等待重试。"""
    if not payload.dispatch_id or not payload.worker_node:
        logger.error("拒绝恢复缺少 dispatch_id/worker_node 的旧任务: %s", payload.account_name)
        return False
    try:
        response = _post_signed_payload(
            "/api/dispatch/validate",
            {
                "dispatch_id": payload.dispatch_id,
                "account_name": payload.account_name,
                "worker_node": payload.worker_node,
            },
        )
        result = response.json()
        if not isinstance(result, dict) or not isinstance(result.get("active"), bool):
            raise ValueError("中心返回了非法的 lease 校验结果")
        return result["active"]
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as error:
        logger.warning("暂时无法校验恢复任务的 lease [%s]: %s", payload.dispatch_id, error)
        return None


def report_status(
    account: str,
    status: str,
    successful_files: Optional[List[str]] = None,
    missing_files: Optional[List[str]] = None,
):
    task_payload = current_task_payload.get()
    dispatch_id = task_payload.dispatch_id if task_payload is not None else None
    report_payload = {
        "dispatch_id": dispatch_id,
        "worker_node": WORKER_NODE_NAME,
        "account_name": account,
        "status": status,
        "successful_files": successful_files or [],
        "missing_files": missing_files or [],
    }
    # 先持久化最终回执再发送；中心不可达或进程重启后仍可补送上传结果。
    if status in FINAL_STATUSES:
        journal_store_final_report(dispatch_id, report_payload)

    max_retries = 3
    last_error: Optional[Exception] = None
    for i in range(max_retries):
        try:
            _post_report_payload(report_payload)
            if status in FINAL_STATUSES:
                journal_mark_completed(dispatch_id)
            logger.info(f"📡 战况汇报完毕: {account} -> {status}")
            return
        except LeaseLostError:
            journal_set_state(dispatch_id, "cancelled")
            raise
        except Exception as e:
            last_error = e
            logger.error(f"📡 无法连接中心大脑进行战况汇报 (重试 {i+1}/{max_retries}): {e}")
            if i < max_retries - 1:
                time.sleep(3)
    assert last_error is not None
    if status in FINAL_STATUSES:
        journal_mark_report_error(dispatch_id, last_error)
    raise ReportStatusError(f"战况汇报失败: {account} -> {status}") from last_error


def report_heartbeat(account_name: str, successful_files: Optional[List[str]] = None) -> bool:
    try:
        report_status(account_name, "uploading", successful_files)
        return True
    except ReportStatusError as error:
        logger.warning("💓 [%s] 心跳暂时无法送达，rclone 继续运行: %s", account_name, error)
        return False


def flush_cleaner_success_batch(account_name: str, status: str, batch: list[str]) -> bool:
    try:
        if batch:
            report_status(account_name, status, list(batch))
            batch.clear()
        else:
            report_status(account_name, status)
        return True
    except ReportStatusError:
        if status == "uploading":
            logger.warning("🧹 [%s] 分批进度回执失败，保留批次并继续搬运。", account_name)
            return False
        raise


def detect_remote_existing_files(
    account_name: str,
    file_paths: List[str],
    expected_sizes: Optional[dict[str, Optional[int]]] = None,
    expected_mtimes: Optional[dict[str, Optional[float]]] = None,
    remote_dir: Optional[str] = None,
) -> set[str]:
    if not file_paths:
        return set()

    fd, temp_path = tempfile.mkstemp(prefix="bc_remote_probe_", suffix=".txt")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for file_path in file_paths:
                handle.write(f"{file_path}\n")

        verify_fingerprint = expected_sizes is not None or expected_mtimes is not None
        # 与上传清单一致使用 raw：不裁剪首尾空白、不把 #/; 开头视为注释。
        command = [
            "rclone",
            "lsjson" if verify_fingerprint else "lsf",
            f"{account_name}:{remote_dir or TARGET_REMOTE_DIR}",
            "--files-from-raw",
            temp_path,
            "--files-only",
            "--recursive",
        ]
        if not verify_fingerprint:
            command.extend(["--format", "p"])
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=REMOTE_PROBE_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            logger.warning(
                "⚠️ [%s] 远端存在性补偿检查失败（退出码=%s），将按本地缺失处理。\n%s",
                account_name,
                result.returncode,
                (result.stderr or result.stdout).strip(),
            )
            return set()

        if not verify_fingerprint:
            return {
                line.replace("\\", "/")
                for line in result.stdout.splitlines()
                if line
            }

        try:
            remote_items = json.loads(result.stdout)
        except (TypeError, json.JSONDecodeError):
            logger.warning("⚠️ [%s] 远端指纹响应不是合法 JSON，拒绝补偿成功。", account_name)
            return set()
        if not isinstance(remote_items, list):
            return set()

        matched_files: set[str] = set()
        for item in remote_items:
            if not isinstance(item, dict) or not isinstance(item.get("Path"), str):
                continue
            file_path = item["Path"].replace("\\", "/")
            expected_size = (expected_sizes or {}).get(file_path)
            if expected_size is not None and item.get("Size") != expected_size:
                continue
            expected_mtime = (expected_mtimes or {}).get(file_path)
            if expected_mtime is not None:
                remote_mtime = item.get("ModTime")
                if not isinstance(remote_mtime, str):
                    continue
                # lsjson 固定输出 9 位小数秒，Python 3.10 的 fromisoformat 只接受 3 或 6 位。
                remote_mtime = re.sub(
                    r"\.(\d+)",
                    lambda fraction: "." + fraction.group(1)[:6].ljust(6, "0"),
                    remote_mtime.replace("Z", "+00:00"),
                    count=1,
                )
                try:
                    remote_timestamp = datetime.fromisoformat(remote_mtime).timestamp()
                except ValueError:
                    continue
                if abs(remote_timestamp - expected_mtime) > 1.0:
                    continue
            matched_files.add(file_path)
        return matched_files
    except subprocess.TimeoutExpired:
        logger.warning("⚠️ [%s] 远端存在性补偿检查超时。", account_name)
        return set()
    except FileNotFoundError:
        logger.error("❌ 未找到 rclone 命令，无法执行远端存在性补偿检查。")
        return set()
    except Exception as e:
        logger.warning("⚠️ [%s] 远端存在性补偿检查异常，将按本地缺失处理: %s", account_name, e)
        return set()
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def detect_cleaner_dest_existing_files(
    dest_name: str, files_from_path: str, remote_dir: Optional[str] = None
) -> set[str]:
    target_remote_dir = remote_dir or TARGET_REMOTE_DIR
    try:
        result = subprocess.run(
            [
                "rclone",
                "lsf",
                f"{dest_name}:{target_remote_dir}",
                "--files-from-raw",
                files_from_path,
                "--files-only",
                "--recursive",
                "--format",
                "p",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=REMOTE_PROBE_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            logger.warning(
                "🧹 目标盘存在性复核失败: %s -> %s（退出码=%s）\n%s",
                dest_name,
                target_remote_dir,
                result.returncode,
                (result.stderr or result.stdout).strip(),
            )
            return set()

        return {
            line.replace("\\", "/")
            for line in result.stdout.splitlines()
            if line
        }
    except subprocess.TimeoutExpired:
        logger.warning("🧹 [%s] cleaner 目标盘复核超时。", dest_name)
        return set()
    except FileNotFoundError:
        logger.error("❌ 未找到 rclone 命令，无法执行 cleaner 目标盘复核。")
        return set()
    except Exception as e:
        logger.warning("🧹 cleaner 目标盘复核异常，将按原始 rclone 结果处理: %s", e)
        return set()


def resolve_cleaner_destination_remote(
    account_name: str, dest_name: str, remote_dir: Optional[str] = None
) -> str:
    result = subprocess.run(
        ["rclone", "config", "dump"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError("无法读取 rclone 目标盘配置")

    try:
        config = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("rclone 目标盘配置格式无效") from error

    dest_config = config.get(dest_name)
    if not isinstance(dest_config, dict) or dest_config.get("type") != "drive":
        raise RuntimeError(f"清道夫目标 remote 不是 Google Drive: {dest_name}")
    team_drive = str(dest_config.get("team_drive", "")).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", team_drive):
        raise RuntimeError(f"清道夫目标 remote 缺少合法 team_drive: {dest_name}")

    # 跨盘复制必须继续使用源清道夫凭据，否则目标 remote 的凭据看不到源文件 ID。
    return f"{account_name},team_drive={team_drive}:{remote_dir or TARGET_REMOTE_DIR}"


def resolve_task_path(
    file_path: str, source_base_dir: Optional[str] = None
) -> Optional[tuple[str, str]]:
    if not file_path or any(char in file_path for char in ("\x00", "\r", "\n")):
        logger.warning("⚠️ 拒绝非法任务路径: %r", file_path)
        return None
    normalized_path = os.path.normpath(file_path.lstrip("/"))
    if normalized_path in ("", ".") or normalized_path.startswith("../") or normalized_path == "..":
        logger.warning("⚠️ 拒绝非法任务路径: %s", file_path)
        return None

    candidate_base_dirs = ALLOWED_BASE_DIRS
    if source_base_dir is not None:
        allowed_by_realpath = {
            os.path.realpath(base_dir): base_dir for base_dir in ALLOWED_BASE_DIRS
        }
        configured_base_dir = allowed_by_realpath.get(os.path.realpath(source_base_dir))
        if configured_base_dir is None:
            logger.warning("⚠️ 拒绝白名单外来源根目录: %s", source_base_dir)
            return None
        candidate_base_dirs = [configured_base_dir]

    for base_dir in candidate_base_dirs:
        base_abs = os.path.realpath(base_dir)
        candidate_abs = os.path.realpath(os.path.join(base_abs, normalized_path))
        try:
            if os.path.commonpath([base_abs, candidate_abs]) != base_abs:
                continue
        except ValueError:
            continue

        if os.path.isfile(candidate_abs):
            return base_dir, normalized_path

    return None

# ================= 实时搬运逻辑 =================
def run_uploader(
    account_name: str,
    files: List[str],
    method: str,
    source_base_dirs: Optional[List[Optional[str]]] = None,
    expected_sizes: Optional[List[Optional[int]]] = None,
    expected_mtimes: Optional[List[Optional[float]]] = None,
    remote_dir: Optional[str] = None,
):
    task_remote_dir = remote_dir or TARGET_REMOTE_DIR
    logger.info(
        "🚀 [实时 %s 模式] 账号 %s 启动，目标目录 %s。收到 %s 个文件任务，详细日志已重定向。",
        method.upper(),
        account_name,
        task_remote_dir,
        len(files),
    )
    
    tasks_by_dir = {base_dir: [] for base_dir in ALLOWED_BASE_DIRS}
    missing_files = []
    fingerprint_mismatch_files = []
    ignored_files = []
    task_payload = current_task_payload.get()
    dispatch_id = task_payload.dispatch_id if task_payload is not None else None
    for file_index, file_path in enumerate(files):
        if should_ignore_upload_file(file_path):
            ignored_files.append(file_path)
            continue
        source_base_dir = (
            source_base_dirs[file_index]
            if source_base_dirs and file_index < len(source_base_dirs)
            else None
        )
        if source_base_dir is None:
            resolved = resolve_task_path(file_path)
        else:
            resolved = resolve_task_path(file_path, source_base_dir)
        if resolved is None:
            missing_files.append(file_path)
            continue
        if has_unsendable_characters(file_path):
            # 不整批拒绝：否则 Center 每 60 秒重派同一批并再次 422，账号组会永远卡住。
            logger.warning(
                "⚠️ 账号 [%s] 跳过含控制字符的路径（rclone 会转义字符，无法按原名搬运）：%r",
                account_name,
                file_path,
            )
            missing_files.append(file_path)
            continue
        base_dir, clean_path = resolved
        expected_size = (
            expected_sizes[file_index]
            if expected_sizes and file_index < len(expected_sizes)
            else None
        )
        expected_mtime = (
            expected_mtimes[file_index]
            if expected_mtimes and file_index < len(expected_mtimes)
            else None
        )
        if expected_size is not None or expected_mtime is not None:
            try:
                stat_result = os.stat(os.path.join(base_dir, clean_path))
            except OSError:
                missing_files.append(file_path)
                continue
            if (
                (expected_size is not None and stat_result.st_size != expected_size)
                or (expected_mtime is not None and stat_result.st_mtime != expected_mtime)
            ):
                missing_files.append(file_path)
                fingerprint_mismatch_files.append(file_path)
                continue
        tasks_by_dir[base_dir].append(clean_path)

    # ===== 新增关键调试日志 =====
    total_found = sum(len(flist) for flist in tasks_by_dir.values())
    logger.info(f"🔍 检查本地文件：总共 {len(files)} 个任务中，在本地容器内成功找到 {total_found} 个文件。")
    if missing_files:
        logger.warning("⚠️ 账号 [%s] 有 %s 个任务文件不存在或越界，稍后会单独回执 not_found。", account_name, len(missing_files))
    if fingerprint_mismatch_files:
        logger.warning(
            "⚠️ 账号 [%s] 有 %s 个文件已在派发后发生变化，本批不会上传旧任务版本。",
            account_name,
            len(fingerprint_mismatch_files),
        )
    if ignored_files:
        logger.warning("🧹 账号 [%s] 跳过 %s 个临时后缀任务文件（.clfstmp），不会上传。", account_name, len(ignored_files))
    # ==========================

    final_status = "completed"
    successful_files = []
    successful_files_seen: set[str] = set()
    def mark_success(file_path: str) -> None:
        normalized = file_path.replace("\\", "/")
        if normalized not in successful_files_seen:
            successful_files_seen.add(normalized)
            successful_files.append(normalized)

    if method == "move" and missing_files:
        expected_size_by_path = (
            {
                file_path: expected_sizes[index]
                for index, file_path in enumerate(files)
                if index < len(expected_sizes)
            }
            if expected_sizes
            else None
        )
        expected_mtime_by_path = (
            {
                file_path: expected_mtimes[index]
                for index, file_path in enumerate(files)
                if index < len(expected_mtimes)
            }
            if expected_mtimes
            else None
        )
        recovered_remote_files = detect_remote_existing_files(
            account_name,
            missing_files,
            expected_size_by_path,
            expected_mtime_by_path,
            task_remote_dir,
        )
        if recovered_remote_files:
            for file_path in recovered_remote_files:
                mark_success(file_path)
            missing_files = [fp for fp in missing_files if fp not in recovered_remote_files]
            logger.warning(
                "🩹 账号 [%s] 有 %s 个本地缺失文件已在远端存在，按已成功搬运恢复记录。",
                account_name,
                len(recovered_remote_files),
            )

    missing_files.extend(ignored_files)

    if total_found == 0:
        if successful_files and not missing_files:
            logger.warning("🩹 账号 [%s] 本地文件已不存在，但远端已命中全部文件，按完成处理。", account_name)
            report_status(account_name, "completed", successful_files, [])
        else:
            logger.warning("⚠️ 警告：Center 发来的所有文件在本地均不存在！任务将回退等待人工排查。")
            report_status(
                account_name,
                "not_found",
                successful_files,
                missing_files or files,
            )
        return

    for base_dir, dir_files in tasks_by_dir.items():
        if not dir_files:
            continue
        
        # raw 清单逐行原样匹配：路径已拒绝 NUL/CR/LF，首尾空白和 #/; 开头都是文件名的一部分。
        fd, temp_path = tempfile.mkstemp(prefix="bc_up_", suffix=".txt")
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            for fp in dir_files: f.write(f"{fp}\n")

        cmd = [
            "rclone", method, base_dir, f"{account_name}:{task_remote_dir}",
            "--files-from-raw", temp_path,
            "--transfers", str(RCLONE_TRANSFERS),
            "--checkers", str(RCLONE_CHECKERS),
            "--stats", "15s",               # 15秒刷一次简报
            "--stats-one-line",             # 强制单行，保持观感清爽
            "--log-level", "INFO",
            "--drive-chunk-size", "128M",
            "--buffer-size", "128M",
            "--drive-stop-on-upload-limit"
        ]
        if RCLONE_BWLIMIT:
            cmd.extend(["--bwlimit", RCLONE_BWLIMIT])
        
        try:
            # 取消双写日志，直接将进程输出重定向到控制台并打印全部信息。
            # 这里用 selector 轮询 stdout，避免 rclone 长时间不再输出时 watchdog 失效。
            last_report_time = time.time()
            quota_detected = False
            
            with subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
            ) as process:
                assert process.stdout is not None
                selector = selectors.DefaultSelector()
                selector.register(process.stdout, selectors.EVENT_READ)
                try:
                    while True:
                        current_time = time.time()

                        if is_dispatch_cancelled(dispatch_id):
                            logger.warning("🛑 [%s] dispatch [%s] 已失去租约，终止旧 rclone。", account_name, dispatch_id)
                            process.kill()
                            final_status = "lease_lost"
                            break

                        if current_time - last_report_time > 300:
                            logger.info(f"💓 [{account_name}] 任务执行中... 汇报活跃状态防失联。")
                            report_heartbeat(account_name, successful_files)
                            last_report_time = current_time

                        events = selector.select(timeout=1.0)
                        if not events:
                            if process.poll() is not None:
                                break
                            continue

                        for key, _ in events:
                            line = key.fileobj.readline()
                            if line == "":
                                selector.unregister(key.fileobj)
                                continue

                            print(f"[{account_name}] {line.strip()}", flush=True)

                            transfer = parse_rclone_transfer_line(line)
                            if transfer is not None:
                                file_path, action = transfer
                                if action == "Skipped":
                                    logger.warning("⏭️ [%s] 远端已存在或被 rclone 跳过: %s", account_name, file_path)
                                mark_success(file_path)

                            if is_credential_invalid_error(line):
                                final_status = "credential_invalid"
                                logger.warning(
                                    "⛔ [%s] 命中凭据失效错误（invalid_grant/token expired），立即停用账号，等待人工 reconnect。",
                                    account_name,
                                )
                                process.kill()
                                break
                            if is_quota_error(line):
                                quota_detected = True
                        if final_status in ("403_quota", "credential_invalid"):
                            break
                finally:
                    selector.close()

                process.wait()
                if final_status not in ("403_quota", "credential_invalid", "lease_lost"):
                    if process.returncode == 8 or quota_detected:
                        logger.warning("⛔ [%s] rclone 因 --drive-stop-on-upload-limit 自动停止，立即切号并封号 24 小时。", account_name)
                        final_status = "403_quota"
                    elif process.returncode != 0:
                        logger.warning(
                            "⛔ [%s] rclone 以非零退出码 %s 结束当前上传任务，按普通失败回报。",
                            account_name,
                            process.returncode,
                        )
                        final_status = "failed"
                if final_status in ("403_quota", "credential_invalid", "failed", "lease_lost"):
                    break
        except FileNotFoundError:
            logger.error(f"❌ 未找到 rclone 命令，请确保已正确挂载 /usr/bin/rclone")
            final_status = "failed"
            break
        except Exception as e:
            logger.error(f"❌ 上传执行异常: {e}")
            final_status = "failed"
            break
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)
            
    # 如果遇到 403 但抢救出了部分成功的文件，我们在日志里说明一下
    if final_status == "403_quota" and successful_files:
        logger.warning(f"⚠️ [{account_name}] 触发 403 熔断，但已成功抢救 {len(successful_files)} 个文件的记录！")

    if final_status == "lease_lost":
        return

    report_status(account_name, final_status, successful_files, missing_files)

def run_cleaner(
    account_name: str,
    dest_name: str,
    requested_files: List[str],
    remote_dir: Optional[str] = None,
):
    task_remote_dir = remote_dir or TARGET_REMOTE_DIR
    requested_report_files: list[str] = []
    requested_files_seen: set[str] = set()
    task_payload = current_task_payload.get()
    dispatch_id = task_payload.dispatch_id if task_payload is not None else None
    for file_path in requested_files:
        if not file_path or any(char in file_path for char in ("\x00", "\r", "\n")):
            logger.warning("🧹 [CLEAN-%s] 拒绝非法清理路径: %r", account_name, file_path)
            continue
        # 与上传时 resolve_task_path 的远端相对路径规则一致，保证清单指向中转盘上的同一文件。
        normalized_path = os.path.normpath(file_path.lstrip("/"))
        if normalized_path in ("", ".") or normalized_path == ".." or normalized_path.startswith("../"):
            logger.warning("🧹 [CLEAN-%s] 拒绝越界清理路径: %r", account_name, file_path)
            continue
        if has_unsendable_characters(file_path):
            # 与 uploader 一致：rclone 会转义控制字符，清单写了也搬不动，显式暴露而不是静默成功。
            logger.warning("🧹 [CLEAN-%s] 跳过含控制字符的路径：%r", account_name, file_path)
            continue
        if normalized_path not in requested_files_seen:
            requested_files_seen.add(normalized_path)
            requested_report_files.append(normalized_path)

    if not requested_report_files:
        logger.warning("🧹 [CLEAN-%s] 未收到待清理文件清单，跳过 rclone move。", account_name)
        report_status(account_name, "clean_success")
        return

    logger.info(
        "🧹 清道夫启动: %s -> %s，按本批清单移动 %s 个文件。",
        account_name,
        dest_name,
        len(requested_report_files),
    )

    try:
        cleaner_destination = resolve_cleaner_destination_remote(
            account_name, dest_name, task_remote_dir
        )
    except Exception as error:
        logger.error("❌ 清道夫无法解析目标盘配置: %s", error)
        report_status(account_name, "failed")
        return
    # 只搬运本次 dispatch 的文件；raw 清单不裁剪首尾空白、不把 #/; 开头视为注释。
    fd, files_from_path = tempfile.mkstemp(prefix="bc_clean_", suffix=".txt")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        for file_path in requested_report_files:
            handle.write(f"{file_path}\n")
    cmd = [
        "rclone",
        "move",
        f"{account_name}:{task_remote_dir}",
        cleaner_destination,
        "--files-from-raw",
        files_from_path,
        "--drive-server-side-across-configs",
        "--drive-use-trash=false",
        "--delete-empty-src-dirs",
        "--transfers",
        "32",
        "--tpslimit",
        "3",
        "--log-level",
        "ERROR",
    ]
    last_report_time = time.time()
    error_tail: list[str] = []
    lease_lost = False

    def capture_error_line(line: str) -> None:
        stripped = line.strip()
        if not stripped:
            return
        error_tail.append(stripped)
        if len(error_tail) > 3:
            del error_tail[0]

    try:
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
        ) as p:
            assert p.stdout is not None
            selector = selectors.DefaultSelector()
            selector_active = False
            try:
                try:
                    selector.register(p.stdout, selectors.EVENT_READ)
                    selector_active = True
                except (TypeError, ValueError, OSError):
                    # Test doubles and unusual streams may not expose a file descriptor.
                    for line in p.stdout:
                        capture_error_line(line)
                        if is_dispatch_cancelled(dispatch_id):
                            p.kill()
                            lease_lost = True
                            break
                        current_time = time.time()
                        if current_time - last_report_time > 300:
                            report_heartbeat(account_name)
                            last_report_time = current_time
                else:
                    while True:
                        current_time = time.time()
                        if is_dispatch_cancelled(dispatch_id):
                            logger.warning(
                                "🛑 [CLEAN-%s] dispatch [%s] 已失去租约，终止旧 rclone。",
                                account_name,
                                dispatch_id,
                            )
                            p.kill()
                            lease_lost = True
                            break
                        if current_time - last_report_time > 300:
                            logger.info(f"💓 [CLEAN-{account_name}] 清理执行中... 汇报活跃状态防失联。")
                            report_heartbeat(account_name)
                            last_report_time = current_time

                        events = selector.select(timeout=1.0)
                        if not events:
                            if p.poll() is not None:
                                break
                            continue
                        for key, _ in events:
                            line = key.fileobj.readline()
                            if line == "":
                                try:
                                    selector.unregister(key.fileobj)
                                except KeyError:
                                    pass
                                selector_active = False
                                continue
                            capture_error_line(line)
                        if not selector_active and p.poll() is not None:
                            break
            finally:
                selector.close()

            p.wait()
            if lease_lost:
                return
            if p.returncode == 0:
                logger.info(
                    "🧹 [CLEAN-%s] 本批清单移动完成，通知大脑完成 dispatch 批次（%s 个文件）。",
                    account_name,
                    len(requested_report_files),
                )
                report_status(account_name, "clean_success")
            else:
                error_summary = " | ".join(error_tail)[:1500] or "无错误输出"
                logger.error(
                    "🧹 [CLEAN-%s] 本批清单移动失败（退出码=%s），仅保留末尾摘要: %s",
                    account_name,
                    p.returncode,
                    error_summary,
                )
                report_status(account_name, "failed")
    except ReportStatusError:
        raise
    except FileNotFoundError:
        logger.error("❌ 未找到 rclone 命令，清道夫任务失败。")
        report_status(account_name, "failed")
    except Exception as e:
        logger.error(f"❌ 清道夫崩溃: {e}")
        report_status(account_name, "failed")
    finally:
        if os.path.exists(files_from_path):
            os.remove(files_from_path)

def task_router(payload: TaskPayload):
    context_token = current_task_payload.set(payload)
    try:
        if payload.task_type == "clean":
            run_cleaner(
                payload.account_name,
                payload.dest_name,
                list(payload.files),
                payload.remote_dir,
            )
        else:
            run_uploader(
                payload.account_name,
                payload.files,
                payload.upload_method,
                list(payload.source_base_dirs) if payload.source_base_dirs else None,
                list(payload.expected_sizes) if payload.expected_sizes else None,
                list(payload.expected_mtimes) if payload.expected_mtimes else None,
                payload.remote_dir,
            )
    except ReportStatusError as e:
        logger.error(f"❌ 状态回执失败: {e}")
    except Exception as e:
        logger.error(f"❌ 任务路由未捕获异常: {e}")
        try:
            report_status(payload.account_name, "failed")
        except ReportStatusError as report_error:
            logger.error(f"❌ 补偿失败回执也未送达: {report_error}")
    finally:
        clear_dispatch_cancellation(payload.dispatch_id)
        current_task_payload.reset(context_token)
        if payload.task_type == "clean":
            with active_cleaner_accounts_lock:
                active_cleaner_accounts.discard(payload.account_name)
        else:
            with active_upload_accounts_lock:
                active_upload_accounts.discard(payload.account_name)


def start_detached_task(payload: TaskPayload) -> None:
    journal_set_state(payload.dispatch_id, "running")
    thread = threading.Thread(
        target=task_router,
        args=(payload,),
        daemon=True,
        name=f"baicai-{payload.task_type}-{payload.account_name}",
    )
    thread.start()


def reserve_task_slot(payload: TaskPayload, enforce_limit: bool = True) -> bool:
    if payload.task_type == "clean":
        with active_cleaner_accounts_lock:
            if payload.account_name in active_cleaner_accounts:
                return False
            if enforce_limit and len(active_cleaner_accounts) >= MAX_CONCURRENT_CLEANERS_PER_WORKER:
                return False
            active_cleaner_accounts.add(payload.account_name)
            return True

    with active_upload_accounts_lock:
        if payload.account_name in active_upload_accounts:
            return False
        if enforce_limit and len(active_upload_accounts) >= current_upload_concurrency_limit():
            return False
        active_upload_accounts.add(payload.account_name)
        return True


def release_task_slot(payload: TaskPayload) -> None:
    if payload.task_type == "clean":
        with active_cleaner_accounts_lock:
            active_cleaner_accounts.discard(payload.account_name)
    else:
        with active_upload_accounts_lock:
            active_upload_accounts.discard(payload.account_name)


def recover_unfinished_tasks() -> None:
    for row in journal_list_tasks("accepted", "running"):
        try:
            payload = TaskPayload.model_validate_json(row["payload_json"])
        except Exception as error:
            logger.error("❌ 无法恢复 journal 任务 %s: %s", row["dispatch_id"], error)
            continue
        lease_active = validate_recovery_lease(payload)
        if lease_active is None:
            continue
        if not lease_active:
            cancel_dispatch(payload.dispatch_id)
            journal_set_state(payload.dispatch_id, "cancelled")
            logger.warning(
                "⏭️ 取消已过期或已改派的 journal 任务: %s (%s/%s)",
                payload.dispatch_id,
                payload.task_type,
                payload.account_name,
            )
            continue
        # 这些任务重启前已占用中心租约，恢复时不受昼夜新任务准入上限限制；
        # reserve_task_slot 仍会检查同账号是否正在运行，防止重复启动。
        if not reserve_task_slot(payload, enforce_limit=False):
            continue
        try:
            logger.warning(
                "♻️ 恢复被 Worker/系统重启中断的任务: %s (%s/%s)",
                payload.dispatch_id,
                payload.task_type,
                payload.account_name,
            )
            start_detached_task(payload)
        except Exception:
            release_task_slot(payload)
            logger.exception("❌ journal 任务 %s 恢复启动失败", payload.dispatch_id)


def retry_pending_final_reports() -> None:
    for row in journal_list_tasks("pending_report"):
        if not row.get("final_report_json"):
            continue
        try:
            report_payload = json.loads(row["final_report_json"])
            _post_report_payload(report_payload)
            journal_mark_completed(row["dispatch_id"])
            logger.info("📡 journal 最终回执补送成功: %s", row["dispatch_id"])
        except LeaseLostError:
            journal_set_state(row["dispatch_id"], "cancelled")
            logger.warning("⏭️ journal 最终回执已过期，停止补送: %s", row["dispatch_id"])
        except Exception as error:
            journal_mark_report_error(row["dispatch_id"], error)
            logger.warning("📡 journal 最终回执仍未送达 [%s]: %s", row["dispatch_id"], error)


def journal_recovery_loop() -> None:
    while not report_retry_stop.wait(REPORT_RETRY_INTERVAL_SECONDS):
        retry_pending_final_reports()
        recover_unfinished_tasks()


@app.on_event("startup")
def startup_worker_recovery() -> None:
    initialize_task_journal()
    pruned_count = journal_prune_terminal_tasks()
    if pruned_count:
        logger.info("🧹 已清理 %s 条过期 journal 终态记录。", pruned_count)
    retry_pending_final_reports()
    recover_unfinished_tasks()
    report_retry_stop.clear()
    if not any(thread.name == "baicai-journal-recovery" for thread in threading.enumerate()):
        threading.Thread(
            target=journal_recovery_loop,
            daemon=True,
            name="baicai-journal-recovery",
        ).start()


@app.on_event("shutdown")
def stop_worker_recovery() -> None:
    report_retry_stop.set()


@app.post("/task/upload", dependencies=[Depends(verify_task_auth)])
async def receive_task(payload: TaskPayload):
    if payload.dispatch_id is None:
        payload = payload.model_copy(update={"dispatch_id": f"legacy-{uuid.uuid4().hex}"})

    existing = journal_get_task(payload.dispatch_id)
    if existing is not None:
        if existing["payload_json"] != serialize_task_payload(payload):
            raise HTTPException(status_code=409, detail="dispatch_id payload mismatch")
        logger.info("⏭️ 幂等接受重复任务 %s，当前状态=%s", payload.dispatch_id, existing["state"])
        return {
            "status": "Accepted",
            "mode": payload.upload_method,
            "dispatch_id": payload.dispatch_id,
            "idempotent": True,
            "journal_state": existing["state"],
        }

    if payload.task_type == "clean":
        with active_cleaner_accounts_lock:
            if payload.account_name in active_cleaner_accounts:
                logger.warning(
                    "⏸️ 清道夫账号 [%s] 已在本节点运行，拒绝重复启动新的 rclone move。",
                    payload.account_name,
                )
                raise HTTPException(status_code=409, detail="cleaner already running on this worker")
            if len(active_cleaner_accounts) >= MAX_CONCURRENT_CLEANERS_PER_WORKER:
                logger.warning(
                    "⏸️ 本节点已有 %s 个清道夫 rclone 在运行，达到本地并发上限 %s，拒绝新清理任务。",
                    len(active_cleaner_accounts),
                    MAX_CONCURRENT_CLEANERS_PER_WORKER,
                )
                raise HTTPException(status_code=429, detail="worker cleaner concurrency limit reached")
            active_cleaner_accounts.add(payload.account_name)
    else:
        upload_limit = current_upload_concurrency_limit()
        with active_upload_accounts_lock:
            if payload.account_name in active_upload_accounts:
                logger.warning(
                    "⏸️ 账号 [%s] 已有上传任务在本节点运行，拒绝重复启动新的 rclone。",
                    payload.account_name,
                )
                raise HTTPException(status_code=409, detail="account already uploading on this worker")
            if len(active_upload_accounts) >= upload_limit:
                logger.warning(
                    "⏸️ 本节点已有 %s 个上传 rclone 在运行，达到当前并发上限 %s，拒绝新上传任务。",
                    len(active_upload_accounts),
                    upload_limit,
                )
                raise HTTPException(status_code=429, detail="worker upload concurrency limit reached")
            active_upload_accounts.add(payload.account_name)
    try:
        journal_accept_task(payload)
        start_detached_task(payload)
    except Exception:
        release_task_slot(payload)
        raise
    return {
        "status": "Accepted",
        "mode": payload.upload_method,
        "dispatch_id": payload.dispatch_id,
        "idempotent": False,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
