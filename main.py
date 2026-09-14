import json
import hashlib
import hmac
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, List, Literal, Optional
from contextlib import asynccontextmanager

import httpx
import pymysql
from apscheduler.schedulers.background import BackgroundScheduler
from dbutils.pooled_db import PooledDB
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator, model_validator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- 配置区 ---
def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


API_TOKEN = require_env("API_TOKEN")
DB_HOST = require_env("DB_HOST")
DB_USER = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")
DB_NAME = require_env("DB_NAME")
DB_MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "50"))
DB_MIN_CACHED = int(os.getenv("DB_MIN_CACHED", "1"))
DB_MAX_CACHED = int(os.getenv("DB_MAX_CACHED", "20"))
MAX_FILES_PER_REQUEST = int(os.getenv("MAX_FILES_PER_REQUEST", "10000"))
MAX_SUCCESSFUL_FILES = int(os.getenv("MAX_SUCCESSFUL_FILES", "5000"))
MAX_MISSING_FILES = int(os.getenv("MAX_MISSING_FILES", "5000"))
MAX_CLEANER_FILES_PER_REQUEST = int(os.getenv("MAX_CLEANER_FILES_PER_REQUEST", "5000"))
CLEANER_IDLE_MINUTES = int(os.getenv("CLEANER_IDLE_MINUTES", "20"))
CLEANER_DISPATCH_ENABLED = os.getenv("CLEANER_DISPATCH_ENABLED", "true").lower() in ("1", "true", "yes")
CLEANER_DISPATCH_INTERVAL_MINUTES = max(1, int(os.getenv("CLEANER_DISPATCH_INTERVAL_MINUTES", "15")))
CLEANER_FAILURE_COOLDOWN_MINUTES = max(0, int(os.getenv("CLEANER_FAILURE_COOLDOWN_MINUTES", "120")))
WORKER_STALE_MINUTES = int(os.getenv("WORKER_STALE_MINUTES", "10"))
ACCOUNT_DISPATCH_LEASE_MINUTES = int(os.getenv("ACCOUNT_DISPATCH_LEASE_MINUTES", "60"))
UPLOADING_STALE_MINUTES = int(os.getenv("UPLOADING_STALE_MINUTES", "30"))
NOT_FOUND_RETRY_MINUTES = max(1, int(os.getenv("NOT_FOUND_RETRY_MINUTES", "60")))
QUOTA_BLOCK_HOURS = int(os.getenv("QUOTA_BLOCK_HOURS", "24"))
CLEANER_UPDATE_SQL_CHUNK_SIZE = max(1, min(MAX_SUCCESSFUL_FILES, int(os.getenv("CLEANER_UPDATE_SQL_CHUNK_SIZE", "1000"))))
SCHEDULER_TIMEZONE = "Asia/Shanghai"
MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY = max(1, int(os.getenv("MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY", "2")))
MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT = max(
    MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY,
    int(os.getenv("MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT", "3")),
)
UPLOAD_CONCURRENCY_NIGHT_START_HOUR = int(os.getenv("UPLOAD_CONCURRENCY_NIGHT_START_HOUR", "1"))
UPLOAD_CONCURRENCY_NIGHT_END_HOUR = int(os.getenv("UPLOAD_CONCURRENCY_NIGHT_END_HOUR", "10"))
BEIJING_TZ = timezone(timedelta(hours=8))
IGNORED_UPLOAD_EXTENSIONS = (".clfstmp", ".clfstmp.progress")
ALLOW_LEGACY_UNLEASED_REPORTS = os.getenv("ALLOW_LEGACY_UNLEASED_REPORTS", "false").lower() in ("1", "true", "yes")
STAGING_REMOTE_DIR = os.getenv("STAGING_REMOTE_DIR", "LongEmby-B").strip()
LEGACY_DIRECT_REMOTE_DIR = os.getenv("LEGACY_DIRECT_REMOTE_DIR", "LongEmby-A").strip()
CLEANER_DEST_REMOTE = os.getenv("CLEANER_DEST_REMOTE", "baicai-big2").strip()

for config_name, config_value in (
    ("STAGING_REMOTE_DIR", STAGING_REMOTE_DIR),
    ("LEGACY_DIRECT_REMOTE_DIR", LEGACY_DIRECT_REMOTE_DIR),
    ("CLEANER_DEST_REMOTE", CLEANER_DEST_REMOTE),
):
    if not config_value or any(char in config_value for char in ("\x00", "\r", "\n", ":")):
        raise RuntimeError(f"{config_name} 配置非法")


def upload_remote_dir_for_account(account_name: str) -> str:
    if account_name.startswith("baicai_big@"):
        return LEGACY_DIRECT_REMOTE_DIR
    return STAGING_REMOTE_DIR


def is_direct_upload_account(account_name: str) -> bool:
    return account_name.startswith(("baicai_big@", "baicai_big2@"))

security = HTTPBearer()
optional_security = HTTPBearer(auto_error=False)
_db_pool = None
_db_pool_lock = threading.Lock()
_dispatch_trigger_lock = threading.Lock()
_dispatch_trigger_pending = threading.Event()


def _load_worker_api_url_map() -> dict[str, str]:
    raw_value = os.getenv("WORKER_API_URL_MAP", "").strip()
    if not raw_value:
        return {}

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"WORKER_API_URL_MAP 不是合法 JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("WORKER_API_URL_MAP 必须是 node_name -> api_url 的 JSON 对象")

    normalized: dict[str, str] = {}
    for node_name, api_url in parsed.items():
        if not isinstance(node_name, str) or not isinstance(api_url, str):
            raise RuntimeError("WORKER_API_URL_MAP 中的 node_name 和 api_url 必须为字符串")
        normalized[node_name] = api_url
    return normalized


WORKER_API_URL_MAP = _load_worker_api_url_map()


def _load_worker_token_map() -> dict[str, str]:
    raw_value = os.getenv("WORKER_TOKEN_MAP", "").strip()
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"WORKER_TOKEN_MAP 不是合法 JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("WORKER_TOKEN_MAP 必须是 node_name -> token 的 JSON 对象")

    normalized: dict[str, str] = {}
    for node_name, token in parsed.items():
        if not isinstance(node_name, str) or not node_name.strip():
            raise RuntimeError("WORKER_TOKEN_MAP 包含非法 node_name")
        if not isinstance(token, str) or len(token) < 16:
            raise RuntimeError(f"WORKER_TOKEN_MAP 中节点 [{node_name}] 的 token 过短")
        normalized[node_name.strip()] = token
    return normalized


WORKER_TOKEN_MAP = _load_worker_token_map()
ALLOW_LEGACY_NODE_AUTH = os.getenv("ALLOW_LEGACY_NODE_AUTH", "false").lower() in ("1", "true", "yes")
NODE_AUTH_MAX_CLOCK_SKEW_SECONDS = max(30, int(os.getenv("NODE_AUTH_MAX_CLOCK_SKEW_SECONDS", "300")))


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.credentials


def _node_signature(token: str, timestamp: str, raw_body: bytes) -> str:
    signed_data = timestamp.encode("ascii") + b"\n" + raw_body
    return hmac.new(token.encode("utf-8"), signed_data, hashlib.sha256).hexdigest()


async def verify_node_request(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(optional_security),
) -> dict[str, Optional[str]]:
    node_name = request.headers.get("X-Baicai-Node")
    timestamp = request.headers.get("X-Baicai-Timestamp")
    supplied_signature = request.headers.get("X-Baicai-Signature")

    if node_name or timestamp or supplied_signature:
        if not node_name or not timestamp or not supplied_signature:
            raise HTTPException(status_code=401, detail="节点签名头不完整")
        token = WORKER_TOKEN_MAP.get(node_name)
        if token is None:
            raise HTTPException(status_code=401, detail=f"节点 [{node_name}] 没有已配置的签名密钥")
        try:
            timestamp_value = int(timestamp)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail="节点签名时间戳非法") from exc
        if abs(int(time.time()) - timestamp_value) > NODE_AUTH_MAX_CLOCK_SKEW_SECONDS:
            raise HTTPException(status_code=401, detail="节点签名已过期")

        # 对原始请求体验签，避免 JSON 再序列化改变字节；时间窗口限制旧请求的重放范围。
        expected_signature = _node_signature(token, timestamp, await request.body())
        if not hmac.compare_digest(expected_signature, supplied_signature.lower()):
            raise HTTPException(status_code=401, detail="节点签名无效")
        return {"method": "hmac", "node_name": node_name}

    if ALLOW_LEGACY_NODE_AUTH and credentials is not None and credentials.credentials == API_TOKEN:
        return {"method": "legacy_bearer", "node_name": None}
    raise HTTPException(status_code=401, detail="Node authentication required")


def encode_worker_request(node_name: str, payload: dict[str, Any]) -> tuple[bytes, dict[str, str]]:
    raw_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    token = WORKER_TOKEN_MAP.get(node_name)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        timestamp = str(int(time.time()))
        headers.update(
            {
                "X-Baicai-Node": node_name,
                "X-Baicai-Timestamp": timestamp,
                "X-Baicai-Signature": _node_signature(token, timestamp, raw_body),
            }
        )
    elif ALLOW_LEGACY_NODE_AUTH:
        headers["Authorization"] = f"Bearer {API_TOKEN}"
    else:
        raise RuntimeError(f"节点 [{node_name}] 未配置 WORKER_TOKEN_MAP，拒绝明文共享 Bearer 派发")
    return raw_body, headers


def get_db_pool():
    global _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            _db_pool = PooledDB(
                creator=pymysql,
                maxconnections=DB_MAX_CONNECTIONS,
                mincached=DB_MIN_CACHED,
                maxcached=DB_MAX_CACHED,
                blocking=True,
                ping=0,
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                init_command="SET time_zone = '+00:00'",
            )
    return _db_pool


def get_db_connection():
    return get_db_pool().connection()


def current_beijing_time() -> datetime:
    return datetime.now(BEIJING_TZ)


def current_upload_concurrency_limit(now_bj: Optional[datetime] = None) -> int:
    now_bj = now_bj or current_beijing_time()
    if UPLOAD_CONCURRENCY_NIGHT_START_HOUR <= now_bj.hour < UPLOAD_CONCURRENCY_NIGHT_END_HOUR:
        return MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT
    return MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY


def should_ignore_upload_path(path: str) -> bool:
    return path.casefold().endswith(IGNORED_UPLOAD_EXTENSIONS)


def _parse_db_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None, microsecond=0)
    if isinstance(value, str):
        raw_value = value.strip()
        if not raw_value:
            return None
        for candidate in (raw_value, raw_value.replace(" ", "T")):
            try:
                return datetime.fromisoformat(candidate).replace(tzinfo=None, microsecond=0)
            except ValueError:
                continue
    return None


def _format_db_datetime(value: Any) -> Optional[str]:
    parsed_value = _parse_db_datetime(value)
    if parsed_value is None:
        return None
    return parsed_value.strftime("%Y-%m-%d %H:%M:%S")


def _format_db_datetime_as_bj(value: Any) -> Optional[str]:
    parsed_value = _parse_db_datetime(value)
    if parsed_value is None:
        return None
    return (parsed_value + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


def _build_account_unlock_info(row: dict[str, Any]) -> tuple[Optional[str], Optional[str], list[str], bool]:
    is_active = bool(row.get("is_active", 1))
    if not is_active:
        return None, None, ["inactive"], False

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    active_locks: list[str] = []
    future_lock_times: list[datetime] = []

    for lock_field in ("dispatch_locked_until", "dispatch_cooldown_until", "quota_blocked_until"):
        lock_until = _parse_db_datetime(row.get(lock_field))
        if lock_until is not None and lock_until > now_utc:
            active_locks.append(lock_field)
            future_lock_times.append(lock_until)

    unlock_at_utc = max(future_lock_times) if future_lock_times else None
    return (
        _format_db_datetime(unlock_at_utc),
        _format_db_datetime_as_bj(unlock_at_utc),
        active_locks,
        unlock_at_utc is None,
    )


def validate_api_url(api_url: str) -> str:
    if not api_url.startswith(("http://", "https://")):
        raise ValueError("worker_api_url 必须以 http:// 或 https:// 开头")
    if len(api_url) > 255:
        raise ValueError("worker_api_url 过长")
    return api_url


def resolve_worker_api_url(worker_node: str, reported_api_url: str) -> str:
    validate_api_url(reported_api_url)

    configured_url = WORKER_API_URL_MAP.get(worker_node)
    if configured_url:
        return validate_api_url(configured_url)

    if WORKER_API_URL_MAP:
        raise HTTPException(status_code=400, detail=f"worker_node [{worker_node}] 未在白名单中注册")

    return reported_api_url


class ReportFileItem(BaseModel):
    path: str = Field(min_length=1, max_length=1024)
    size_bytes: Optional[int] = Field(default=None, ge=0)
    mtime: Optional[float] = Field(default=None, ge=0, allow_inf_nan=False)
    source_base_dir: Optional[str] = Field(default=None, min_length=1, max_length=1024)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if value == "":
            raise ValueError("files 中不能出现空路径")
        if any(char in value for char in ("\x00", "\r", "\n")) or len(value) > 1024:
            raise ValueError("files 中存在非法路径")
        return value

    @field_validator("source_base_dir")
    @classmethod
    def validate_source_base_dir(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if value == "" or any(char in value for char in ("\x00", "\r", "\n")):
            raise ValueError("source_base_dir 非法")
        return value


class ReportFilesRequest(BaseModel):
    worker_node: str = Field(min_length=1, max_length=128)
    worker_api_url: str = Field(min_length=8, max_length=255)
    protocol_version: int = Field(default=1, ge=1, le=2)
    files: List[ReportFileItem] = Field(default_factory=list)
    is_history_sync: bool = False
    upload_method: Literal["copy", "move"] = "move"

    @field_validator("worker_node")
    @classmethod
    def validate_worker_node(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("worker_node 不能为空")
        return value

    @field_validator("worker_api_url")
    @classmethod
    def validate_reported_api_url(cls, value: str) -> str:
        return validate_api_url(value.strip())

    @field_validator("files", mode="before")
    @classmethod
    def normalize_files(cls, value: Any) -> List[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("files 必须是数组")
        if len(value) > MAX_FILES_PER_REQUEST:
            raise ValueError(f"单次最多允许上报 {MAX_FILES_PER_REQUEST} 个文件")

        normalized_files = []
        for item in value:
            if isinstance(item, str):
                normalized_files.append(
                    {"path": item, "size_bytes": None, "mtime": None, "source_base_dir": None}
                )
            elif isinstance(item, dict):
                path = item.get("path", item.get("file_path"))
                normalized_files.append(
                    {
                        "path": path,
                        "size_bytes": item.get("size_bytes"),
                        "mtime": item.get("mtime"),
                        "source_base_dir": item.get("source_base_dir"),
                    }
                )
            else:
                raise ValueError("files 必须全部为字符串或文件对象")
        return normalized_files


class ReportStatusRequest(BaseModel):
    account_name: str = Field(min_length=1, max_length=128)
    status: Literal["uploading", "completed", "failed", "403_quota", "clean_success", "not_found", "credential_invalid"]
    dispatch_id: Optional[str] = Field(default=None, min_length=16, max_length=64)
    worker_node: Optional[str] = Field(default=None, min_length=1, max_length=128)
    successful_files: List[str] = Field(default_factory=list)
    missing_files: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_dispatch_identity(self):
        if (self.dispatch_id is None) != (self.worker_node is None):
            raise ValueError("dispatch_id 和 worker_node 必须同时提供")
        if self.dispatch_id is not None:
            self.dispatch_id = self.dispatch_id.strip()
            self.worker_node = self.worker_node.strip()
        return self


    @field_validator("account_name")
    @classmethod
    def validate_account_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("account_name 不能为空")
        return value

    @field_validator("successful_files")
    @classmethod
    def validate_successful_files(cls, value: List[str]) -> List[str]:
        if len(value) > MAX_SUCCESSFUL_FILES:
            raise ValueError(f"successful_files 最多允许 {MAX_SUCCESSFUL_FILES} 个")

        normalized_files = []
        for file_path in value:
            if not isinstance(file_path, str):
                raise ValueError("successful_files 必须全部为字符串")
            if not file_path:
                raise ValueError("successful_files 中不能出现空路径")
            if any(char in file_path for char in ("\x00", "\r", "\n")) or len(file_path) > 1024:
                raise ValueError("successful_files 中存在非法路径")
            normalized_files.append(file_path)
        return normalized_files

    @field_validator("missing_files")
    @classmethod
    def validate_missing_files(cls, value: List[str]) -> List[str]:
        if len(value) > MAX_MISSING_FILES:
            raise ValueError(f"missing_files 最多允许 {MAX_MISSING_FILES} 个")

        normalized_files = []
        for file_path in value:
            if not isinstance(file_path, str):
                raise ValueError("missing_files 必须全部为字符串")
            if not file_path:
                raise ValueError("missing_files 中不能出现空路径")
            if any(char in file_path for char in ("\x00", "\r", "\n")) or len(file_path) > 1024:
                raise ValueError("missing_files 中存在非法路径")
            normalized_files.append(file_path)
        return normalized_files


class DispatchLeaseRequest(BaseModel):
    dispatch_id: str = Field(min_length=16, max_length=64)
    account_name: str = Field(min_length=1, max_length=128)
    worker_node: str = Field(min_length=1, max_length=128)


def _new_dispatch_id() -> str:
    return uuid.uuid4().hex


def _account_lease_condition(dispatch_id: Optional[str], worker_node: Optional[str]) -> tuple[str, list[str]]:
    """同时匹配派发批次和节点，防止旧任务释放或续期新任务的账号租约。"""
    if dispatch_id is None:
        return "current_dispatch_id IS NULL AND current_worker_node IS NULL", []
    return "current_dispatch_id=%s AND current_worker_node=%s", [dispatch_id, worker_node]


def _task_dispatch_condition(dispatch_id: Optional[str], column: str = "dispatch_id") -> tuple[str, list[str]]:
    if dispatch_id is None:
        return f"{column} IS NULL", []
    return f"{column}=%s", [dispatch_id]


def acquire_account_lock(
    cursor,
    account_name: str,
    role: str,
    dispatch_id: Optional[str] = None,
    worker_node: Optional[str] = None,
) -> bool:
    cursor.execute(
        """
        UPDATE drive_accounts
        SET dispatch_locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE),
            current_dispatch_id = %s,
            current_worker_node = %s
        WHERE account_name=%s
          AND role=%s
          AND is_active=1
          AND (dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP())
          AND (dispatch_cooldown_until IS NULL OR dispatch_cooldown_until < UTC_TIMESTAMP())
          AND (quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP())
        """,
        (ACCOUNT_DISPATCH_LEASE_MINUTES, dispatch_id, worker_node, account_name, role),
    )
    return cursor.rowcount == 1


def release_account_lock(
    cursor,
    account_name: str,
    dispatch_id: Optional[str] = None,
    worker_node: Optional[str] = None,
):
    lease_condition, lease_params = _account_lease_condition(dispatch_id, worker_node)
    cursor.execute(
        f"UPDATE drive_accounts "
        f"SET dispatch_locked_until=NULL, current_dispatch_id=NULL, current_worker_node=NULL "
        f"WHERE account_name=%s AND {lease_condition}",
        [account_name] + lease_params,
    )


def set_account_dispatch_cooldown(cursor, account_name: str, minutes: int):
    cursor.execute(
        """
        UPDATE drive_accounts
        SET dispatch_cooldown_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        WHERE account_name=%s
        """,
        (minutes, account_name),
    )


def refresh_account_lease(
    cursor,
    account_name: str,
    role: str,
    dispatch_id: Optional[str],
    worker_node: Optional[str],
) -> int:
    lease_condition, lease_params = _account_lease_condition(dispatch_id, worker_node)
    cursor.execute(
        f"""
        UPDATE drive_accounts
        SET dispatch_locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        WHERE account_name=%s
          AND role=%s
          AND is_active=1
          AND {lease_condition}
        """,
        [ACCOUNT_DISPATCH_LEASE_MINUTES, account_name, role] + lease_params,
    )
    return cursor.rowcount


def refresh_uploader_heartbeat(
    cursor,
    account_name: str,
    dispatch_id: Optional[str] = None,
    worker_node: Optional[str] = None,
) -> tuple[int, int, int]:
    task_condition, task_params = _task_dispatch_condition(dispatch_id)
    cursor.execute(
        f"""
        UPDATE file_tasks
        SET status='uploading', updated_at=UTC_TIMESTAMP()
        WHERE account_used=%s AND status='pending' AND {task_condition}
        """,
        [account_name] + task_params,
    )
    recovered_pending = cursor.rowcount if isinstance(cursor.rowcount, int) else 0

    cursor.execute(
        f"""
        UPDATE file_tasks
        SET updated_at = UTC_TIMESTAMP()
        WHERE account_used=%s AND status='uploading' AND {task_condition}
        """,
        [account_name] + task_params,
    )
    refreshed_tasks = cursor.rowcount if isinstance(cursor.rowcount, int) else 0

    refreshed_lease = refresh_account_lease(
        cursor, account_name, "uploader", dispatch_id, worker_node
    )
    return recovered_pending, refreshed_tasks, refreshed_lease


def acquire_uploader_group_mutex(cursor, group_prefix: str) -> Optional[str]:
    digest = hashlib.sha256(group_prefix.encode("utf-8")).hexdigest()[:32]
    lock_name = f"baicai-uploader-group:{digest}"
    cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
    row = cursor.fetchone() or {}
    return lock_name if row.get("acquired") == 1 else None


def release_uploader_group_mutex(cursor, lock_name: str) -> None:
    cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))


def uploader_group_has_active_lease(cursor, group_prefix: str) -> bool:
    cursor.execute(
        """
        SELECT 1 AS has_active_lease
        FROM drive_accounts
        WHERE role='uploader'
          AND SUBSTRING_INDEX(account_name, '@', 1)=%s
          AND current_dispatch_id IS NOT NULL
          AND dispatch_locked_until IS NOT NULL
          AND dispatch_locked_until > UTC_TIMESTAMP()
        LIMIT 1
        """,
        (group_prefix,),
    )
    return cursor.fetchone() is not None


def trigger_dispatch_uploader_tasks_async(reason: str):
    _dispatch_trigger_pending.set()

    def _runner():
        if not _dispatch_trigger_lock.acquire(blocking=False):
            logger.info("⏭️ 合并重复的即时调度触发：%s", reason)
            return
        try:
            while _dispatch_trigger_pending.is_set():
                _dispatch_trigger_pending.clear()
                logger.info("🚀 触发即时 uploader 重调度：%s", reason)
                dispatch_uploader_tasks()
        except Exception:
            logger.exception("❌ 即时 uploader 重调度失败：%s", reason)
        finally:
            _dispatch_trigger_lock.release()

    threading.Thread(target=_runner, daemon=True, name="dispatch-uploader-trigger").start()


def update_tasks_by_account_and_files(
    cursor,
    account_name: str,
    current_status: str,
    target_status: str,
    file_paths: List[str],
    dispatch_id: Optional[str] = None,
):
    if not file_paths:
        return
    format_strings = ",".join(["%s"] * len(file_paths))
    task_condition, task_params = _task_dispatch_condition(dispatch_id)
    cursor.execute(
        f"UPDATE file_tasks SET status=%s, account_used=NULL, dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
        f"WHERE account_used=%s AND status=%s AND {task_condition} AND file_path IN ({format_strings})",
        [target_status, account_name, current_status] + task_params + file_paths,
    )


def update_cleaner_tasks_by_files(
    cursor,
    cleaner_account: str,
    file_paths: List[str],
    target_status: str,
    dispatch_id: Optional[str] = None,
):
    if not file_paths:
        return
    for start in range(0, len(file_paths), CLEANER_UPDATE_SQL_CHUNK_SIZE):
        chunk = file_paths[start:start + CLEANER_UPDATE_SQL_CHUNK_SIZE]
        format_strings = ",".join(["%s"] * len(chunk))
        task_condition, task_params = _task_dispatch_condition(dispatch_id, "ft.dispatch_id")
        cursor.execute(
            f"""
            UPDATE file_tasks ft
            JOIN drive_accounts da ON ft.account_used = da.account_name
            SET ft.status = %s,
                ft.dispatch_id = NULL,
                ft.updated_at = UTC_TIMESTAMP()
            WHERE ft.status = 'staged'
              AND da.cleaner_mapping = %s
              AND {task_condition}
              AND ft.file_path IN ({format_strings})
            """,
            [target_status, cleaner_account] + task_params + chunk,
        )


def insert_upload_usage_events_for_files(
    cursor,
    account_name: str,
    file_paths: List[str],
    dispatch_id: Optional[str] = None,
):
    if not file_paths:
        return
    for start in range(0, len(file_paths), MAX_SUCCESSFUL_FILES):
        chunk = file_paths[start:start + MAX_SUCCESSFUL_FILES]
        format_strings = ",".join(["%s"] * len(chunk))
        task_condition, task_params = _task_dispatch_condition(dispatch_id)
        cursor.execute(
            f"""
            INSERT IGNORE INTO account_upload_events
                (account_name, file_task_id, dispatch_id, size_bytes, uploaded_at)
            SELECT %s, id, %s, COALESCE(size_bytes, 0), UTC_TIMESTAMP()
            FROM file_tasks
            WHERE account_used=%s
              AND status='uploading'
              AND {task_condition}
              AND file_path IN ({format_strings})
            """,
            [account_name, dispatch_id or "legacy", account_name] + task_params + chunk,
        )


def insert_upload_usage_events_for_account(cursor, account_name: str, dispatch_id: Optional[str] = None):
    task_condition, task_params = _task_dispatch_condition(dispatch_id)
    cursor.execute(
        f"""
        INSERT IGNORE INTO account_upload_events
            (account_name, file_task_id, dispatch_id, size_bytes, uploaded_at)
        SELECT %s, id, %s, COALESCE(size_bytes, 0), UTC_TIMESTAMP()
        FROM file_tasks
        WHERE account_used=%s
          AND status='uploading'
          AND {task_condition}
        """,
        [account_name, dispatch_id or "legacy", account_name] + task_params,
    )


def mark_successful_uploads_by_files(
    cursor,
    account_name: str,
    target_status: str,
    file_paths: List[str],
    dispatch_id: Optional[str] = None,
):
    if not file_paths:
        return
    insert_upload_usage_events_for_files(cursor, account_name, file_paths, dispatch_id)
    for start in range(0, len(file_paths), MAX_SUCCESSFUL_FILES):
        chunk = file_paths[start:start + MAX_SUCCESSFUL_FILES]
        format_strings = ",".join(["%s"] * len(chunk))
        task_condition, task_params = _task_dispatch_condition(dispatch_id)
        cursor.execute(
            f"""
            UPDATE file_tasks
            SET status=%s,
                dispatch_id=NULL,
                updated_at=UTC_TIMESTAMP()
            WHERE account_used=%s
              AND status='uploading'
              AND {task_condition}
              AND file_path IN ({format_strings})
            """,
            [target_status, account_name] + task_params + chunk,
        )


def mark_all_uploading_tasks_success(
    cursor,
    account_name: str,
    target_status: str,
    dispatch_id: Optional[str] = None,
):
    insert_upload_usage_events_for_account(cursor, account_name, dispatch_id)
    task_condition, task_params = _task_dispatch_condition(dispatch_id)
    cursor.execute(
        f"UPDATE file_tasks SET status=%s, dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
        f"WHERE account_used=%s AND status='uploading' AND {task_condition}",
        [target_status, account_name] + task_params,
    )


def group_has_active_uploads(cursor, group_prefix: str) -> bool:
    cursor.execute(
        """
        SELECT 1 AS has_uploading
        FROM file_tasks
        WHERE status='uploading' AND account_used LIKE %s
        UNION
        SELECT 1 AS has_uploading
        FROM drive_accounts
        WHERE role='uploader'
          AND is_active=1
          AND account_name LIKE %s
          AND dispatch_locked_until IS NOT NULL
          AND dispatch_locked_until > UTC_TIMESTAMP()
        LIMIT 1
        """,
        (f"{group_prefix}@%", f"{group_prefix}@%"),
    )
    return cursor.fetchone() is not None


def group_has_recent_activity(cursor, group_prefix: str) -> bool:
    cursor.execute(
        """
        SELECT 1 AS has_recent_activity
        FROM file_tasks
        WHERE account_used LIKE %s
          AND status IN ('uploading', 'staged')
          AND updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        LIMIT 1
        """,
        (f"{group_prefix}@%", CLEANER_IDLE_MINUTES),
    )
    return cursor.fetchone() is not None


def get_group_quota_exhaustion(cursor, group_prefix: str) -> tuple[bool, int, int]:
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_uploaders,
            COALESCE(SUM(
                CASE
                    WHEN quota_blocked_until IS NOT NULL AND quota_blocked_until > UTC_TIMESTAMP() THEN 1
                    ELSE 0
                END
            ), 0) AS blocked_uploaders
        FROM drive_accounts
        WHERE role='uploader' AND is_active=1 AND account_name LIKE %s
        """,
        (f"{group_prefix}@%",),
    )
    result = cursor.fetchone() or {}
    total_uploaders = result.get("total_uploaders", 0) or 0
    blocked_uploaders = result.get("blocked_uploaders", 0) or 0
    return total_uploaders > 0 and total_uploaders == blocked_uploaders, total_uploaders, blocked_uploaders


def revert_uploading_tasks(
    cursor,
    account_name: str,
    clear_account_used: bool,
    dispatch_id: Optional[str] = None,
) -> int:
    task_condition, task_params = _task_dispatch_condition(dispatch_id)
    if clear_account_used:
        cursor.execute(
            f"UPDATE file_tasks SET status='pending', account_used=NULL, dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
            f"WHERE account_used=%s AND status='uploading' AND {task_condition}",
            [account_name] + task_params,
        )
    else:
        cursor.execute(
            f"UPDATE file_tasks SET status='pending', dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
            f"WHERE account_used=%s AND status='uploading' AND {task_condition}",
            [account_name] + task_params,
        )
    return cursor.rowcount


def clear_cleaner_dispatch_tasks(cursor, cleaner_account: str, dispatch_id: Optional[str]):
    task_condition, task_params = _task_dispatch_condition(dispatch_id, "ft.dispatch_id")
    cursor.execute(
        f"""
        UPDATE file_tasks ft
        JOIN drive_accounts da ON ft.account_used = da.account_name
        SET ft.dispatch_id=NULL, ft.updated_at=UTC_TIMESTAMP()
        WHERE ft.status='staged'
          AND da.cleaner_mapping=%s
          AND {task_condition}
        """,
        [cleaner_account] + task_params,
    )


def complete_cleaner_dispatch_tasks(
    cursor, cleaner_account: str, dispatch_id: Optional[str]
) -> int:
    task_condition, task_params = _task_dispatch_condition(dispatch_id, "ft.dispatch_id")
    cursor.execute(
        f"""
        UPDATE file_tasks ft
        JOIN drive_accounts da ON ft.account_used = da.account_name
        SET ft.status='completed', ft.dispatch_id=NULL, ft.updated_at=UTC_TIMESTAMP()
        WHERE ft.status='staged'
          AND da.cleaner_mapping=%s
          AND {task_condition}
        """,
        [cleaner_account] + task_params,
    )
    return cursor.rowcount


def clear_pending_account_preference(cursor, account_name: str) -> int:
    cursor.execute(
        "UPDATE file_tasks SET account_used=NULL, updated_at=UTC_TIMESTAMP() WHERE status='pending' AND account_used=%s",
        (account_name,),
    )
    return cursor.rowcount


def clear_unavailable_pending_account_preferences(cursor) -> int:
    cursor.execute(
        """
        UPDATE file_tasks ft
        JOIN drive_accounts da ON da.account_name = ft.account_used
        SET ft.account_used = NULL,
            ft.updated_at = UTC_TIMESTAMP()
        WHERE ft.status = 'pending'
          AND ft.account_used IS NOT NULL
          AND (
              da.is_active = 0
              OR (da.dispatch_cooldown_until IS NOT NULL AND da.dispatch_cooldown_until > UTC_TIMESTAMP())
              OR (da.quota_blocked_until IS NOT NULL AND da.quota_blocked_until > UTC_TIMESTAMP())
          )
        """
    )
    return cursor.rowcount

    
def get_stale_worker_nodes(cursor) -> list[str]:
    cursor.execute(
        "SELECT node_name FROM worker_registry "
        "WHERE last_active <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)",
        (WORKER_STALE_MINUTES,),
    )
    return [row["node_name"] for row in cursor.fetchall()]


def release_cleaner_locks_for_stale_nodes(cursor, stale_nodes: list[str]) -> int:
    released_count = 0
    for node_name in stale_nodes:
        cursor.execute(
            """
            SELECT account_name, current_dispatch_id
            FROM drive_accounts
            WHERE role='cleaner' AND current_worker_node=%s
            """,
            (node_name,),
        )
        for row in cursor.fetchall():
            dispatch_id = row["current_dispatch_id"]
            cursor.execute(
                """
                UPDATE drive_accounts
                SET dispatch_locked_until=NULL,
                    current_dispatch_id=NULL,
                    current_worker_node=NULL
                WHERE account_name=%s
                  AND current_dispatch_id=%s
                  AND current_worker_node=%s
                  AND NOT EXISTS (
                      SELECT 1 FROM file_tasks
                      WHERE dispatch_id=%s
                        AND status='staged'
                        AND updated_at > DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
                  )
                """,
                (
                    row["account_name"],
                    dispatch_id,
                    node_name,
                    dispatch_id,
                    WORKER_STALE_MINUTES,
                ),
            )
            released_count += cursor.rowcount
    return released_count


def release_stale_uploading_tasks(cursor) -> int:
    cursor.execute(
        """
        SELECT account_used, worker_node, dispatch_id, COUNT(*) AS task_count
        FROM file_tasks
        WHERE status='uploading'
          AND account_used IS NOT NULL
          AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        GROUP BY account_used, worker_node, dispatch_id
        """,
        (UPLOADING_STALE_MINUTES,),
    )
    stale_groups = cursor.fetchall()

    released_count = 0
    for group in stale_groups:
        account_name = group["account_used"]
        worker_node = group["worker_node"]
        dispatch_id = group["dispatch_id"]
        task_condition, task_params = _task_dispatch_condition(dispatch_id)
        cursor.execute(
            f"""
            UPDATE file_tasks
            SET status='pending',
                dispatch_id=NULL,
                updated_at=UTC_TIMESTAMP()
            WHERE status='uploading'
              AND account_used=%s
              AND worker_node=%s
              AND {task_condition}
              AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
            """,
            [account_name, worker_node] + task_params + [UPLOADING_STALE_MINUTES],
        )
        reverted_count = cursor.rowcount
        if reverted_count > 0:
            release_account_lock(cursor, account_name, dispatch_id, worker_node)
            released_count += reverted_count
            logger.warning(
                "🧟 账号 [%s] 在节点 [%s] 的 uploading 心跳超过 %s 分钟未刷新，已释放 %s 个占用任务。",
                account_name,
                worker_node,
                UPLOADING_STALE_MINUTES,
                reverted_count,
            )
    return released_count


def requeue_stale_not_found_tasks(cursor) -> int:
    cursor.execute(
        """
        UPDATE file_tasks
        SET status='pending',
            account_used=NULL,
            dispatch_id=NULL,
            updated_at=UTC_TIMESTAMP()
        WHERE status='not_found'
          AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        """,
        (NOT_FOUND_RETRY_MINUTES,),
    )
    return cursor.rowcount if isinstance(cursor.rowcount, int) else 0



# ================= 核心调度：大批次 + 节点限流 =================
def dispatch_uploader_tasks():
    current_limit = current_upload_concurrency_limit()
    logger.info("🧠 调度巡检：检查各节点负载并分发任务... 当前上传并发上限=%s", current_limit)
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            cleared_pending_preferences = clear_unavailable_pending_account_preferences(cursor)
            if not isinstance(cleared_pending_preferences, int):
                cleared_pending_preferences = 0
            if cleared_pending_preferences > 0:
                logger.warning(
                    "🧹 已清理 %s 个绑定到不可用 uploader 账号的 pending 偏好任务。",
                    cleared_pending_preferences,
                )

            retried_not_found = requeue_stale_not_found_tasks(cursor)
            if retried_not_found > 0:
                logger.warning(
                    "♻️ 已将 %s 个超过 %s 分钟的 not_found 任务重新排队。",
                    retried_not_found,
                    NOT_FOUND_RETRY_MINUTES,
                )

            # 0. 自动释放失联节点的任务（如果节点超过 15 分钟没有活跃，将其 uploading 任务打回 pending）
            dead_nodes = get_stale_worker_nodes(cursor)
            for dead_name in dead_nodes:
                cursor.execute(
                    "SELECT DISTINCT account_used, dispatch_id FROM file_tasks "
                    "WHERE status='uploading' AND worker_node=%s AND account_used IS NOT NULL "
                    "AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)",
                    (dead_name, WORKER_STALE_MINUTES),
                )
                locked_dispatches = [(row["account_used"], row["dispatch_id"]) for row in cursor.fetchall()]
                reverted_count = 0
                for account_name, dispatch_id in locked_dispatches:
                    task_condition, task_params = _task_dispatch_condition(dispatch_id)
                    cursor.execute(
                        f"UPDATE file_tasks SET status='pending', dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
                        f"WHERE status='uploading' AND worker_node=%s AND account_used=%s "
                        f"AND {task_condition} "
                        f"AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)",
                        [dead_name, account_name] + task_params + [WORKER_STALE_MINUTES],
                    )
                    reverted_for_dispatch = cursor.rowcount
                    if reverted_for_dispatch:
                        release_account_lock(cursor, account_name, dispatch_id, dead_name)
                        reverted_count += reverted_for_dispatch
                if reverted_count > 0:
                    logger.warning(f"🧟 节点 [{dead_name}] 失联超过 {WORKER_STALE_MINUTES} 分钟，已释放 {reverted_count} 个占用的任务")

            release_stale_uploading_tasks(cursor)

            # 1. 查找阈值内活跃的节点（排除临时的 Init_Scanner 节点）
            cursor.execute(
                "SELECT node_name, api_url FROM worker_registry "
                "WHERE last_active > DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE) "
                "AND node_name != 'Init_Scanner' AND protocol_version >= 2",
                (WORKER_STALE_MINUTES,),
            )
            nodes = cursor.fetchall()
            
            # 记录本次巡检中已经被派发过的账号前缀（即中转盘组名），防止多个并发打向同一个中转盘
            dispatched_groups_this_cycle = set()
            
            for node in nodes:
                node_name = node['node_name']
                target_api_url = node['api_url']
                
                # 2. 检查该节点的并发数：正在 uploading 的不同账号数
                cursor.execute("""
                    SELECT COUNT(DISTINCT account_used) as active_count 
                    FROM file_tasks 
                    WHERE worker_node = %s AND status = 'uploading'
                """, (node_name,))
                active_count = cursor.fetchone()['active_count']
                
                # 如果节点还有并发余量，就在本次巡检中尽可能地给它塞满任务
                while active_count < current_limit:
                    # 3. 寻找一个尚未被【任何节点】占用的可用账号
                    cursor.execute("""
                        SELECT
                            da.account_name,
                            EXISTS(
                                SELECT 1
                                FROM file_tasks ft
                                WHERE ft.status='pending'
                                  AND ft.worker_node=%s
                                  AND ft.account_used = da.account_name
                                LIMIT 1
                            ) AS has_preferred_pending
                        FROM drive_accounts da
                        WHERE role='uploader' AND is_active=1 
                        AND (dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP())
                        AND (dispatch_cooldown_until IS NULL OR dispatch_cooldown_until < UTC_TIMESTAMP())
                        AND (quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP())
                        AND NOT EXISTS (
                            SELECT 1
                            FROM drive_accounts cleaner
                            WHERE cleaner.account_name = da.cleaner_mapping
                              AND cleaner.role='cleaner'
                              AND cleaner.dispatch_locked_until IS NOT NULL
                              AND cleaner.dispatch_locked_until > UTC_TIMESTAMP()
                        )
                        AND account_name NOT IN (
                            SELECT DISTINCT account_used FROM file_tasks 
                            WHERE status='uploading' AND account_used IS NOT NULL
                        )
                        ORDER BY has_preferred_pending DESC, da.account_name ASC
                    """, (node_name,))
                    available_accounts = cursor.fetchall()
                    
                    selected_account = None
                    for acc_row in available_accounts:
                        acc_name = acc_row['account_name']
                        group_prefix = acc_name.split('@')[0]
                        if group_prefix in dispatched_groups_this_cycle:
                            continue
                        group_lock_name = acquire_uploader_group_mutex(cursor, group_prefix)
                        if group_lock_name is None:
                            continue
                        try:
                            if uploader_group_has_active_lease(cursor, group_prefix):
                                continue
                            dispatch_id = _new_dispatch_id()
                            if not acquire_account_lock(cursor, acc_name, "uploader", dispatch_id, node_name):
                                continue
                            selected_account = (acc_name, group_prefix, dispatch_id)
                            break
                        finally:
                            release_uploader_group_mutex(cursor, group_lock_name)
                    
                    if not selected_account:
                        logger.info(f"⏳ 节点 [{node_name}]：暂时没有多余的可用账号或账号组均已被分配...")
                        break 

                    account_name, group_prefix, dispatch_id = selected_account

                    # 4. 捞取该节点的待办任务：一次派发 500 个
                    cursor.execute("""
                        SELECT id, file_path, source_base_dir, size_bytes, source_mtime, upload_method FROM file_tasks
                        WHERE status='pending'
                          AND worker_node=%s
                          AND (account_used=%s OR account_used IS NULL)
                          AND LOWER(file_path) NOT LIKE '%%.clfstmp'
                          AND LOWER(file_path) NOT LIKE '%%.clfstmp.progress'
                        ORDER BY
                          CASE WHEN account_used=%s THEN 0 ELSE 1 END,
                          created_at ASC
                        LIMIT 500
                    """, (node_name, account_name, account_name))
                    tasks = cursor.fetchall()
                    if not tasks: 
                        release_account_lock(cursor, account_name, dispatch_id, node_name)
                        break

                    # 以第一条任务的策略为准，保证批次内动作一致
                    first_method = tasks[0]['upload_method']
                    batch_tasks = [t for t in tasks if t['upload_method'] == first_method]
                    task_ids = [t['id'] for t in batch_tasks]
                    
                    # 5. 更新数据库状态：锁定任务
                    format_strings = ','.join(['%s'] * len(task_ids))
                    cursor.execute(
                        f"UPDATE file_tasks SET status='uploading', account_used=%s, dispatch_id=%s, updated_at=UTC_TIMESTAMP() "
                        f"WHERE status='pending' AND id IN ({format_strings}) "
                        f"AND NOT EXISTS ("
                        f"    SELECT 1 "
                        f"    FROM drive_accounts da "
                        f"    JOIN drive_accounts cleaner ON cleaner.account_name = da.cleaner_mapping "
                        f"    WHERE da.account_name=%s "
                        f"      AND cleaner.role='cleaner' "
                        f"      AND cleaner.dispatch_locked_until IS NOT NULL "
                        f"      AND cleaner.dispatch_locked_until > UTC_TIMESTAMP()"
                        f")",
                        [account_name, dispatch_id] + task_ids + [account_name],
                    )
                    if cursor.rowcount != len(task_ids):
                        logger.warning("⚠️ 节点 [%s] 的候选任务在锁定前已被其他流程抢占，本轮跳过该批次。", node_name)
                        cursor.execute(
                            "UPDATE file_tasks SET status='pending', dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
                            "WHERE dispatch_id=%s AND account_used=%s AND status='uploading'",
                            (dispatch_id, account_name),
                        )
                        release_account_lock(cursor, account_name, dispatch_id, node_name)
                        break
                    
                    # 将该组前缀加入已派发集合，防止后续并发再选中该组
                    dispatched_groups_this_cycle.add(group_prefix)
                    
                    # 6. 推送指令给 Worker
                    payload = {
                        "task_type": "upload",
                        "upload_method": first_method,
                        "account_name": account_name,
                        "remote_dir": upload_remote_dir_for_account(account_name),
                        "dispatch_id": dispatch_id,
                        "worker_node": node_name,
                        "files": [t['file_path'] for t in batch_tasks],
                        "source_base_dirs": [t.get('source_base_dir') for t in batch_tasks],
                        "expected_sizes": [t.get('size_bytes') for t in batch_tasks],
                        "expected_mtimes": [t.get('source_mtime') for t in batch_tasks],
                    }
                    try:
                        raw_body, request_headers = encode_worker_request(node_name, payload)
                        response = httpx.post(
                            target_api_url,
                            content=raw_body,
                            headers=request_headers,
                            timeout=10.0,
                        )
                        response.raise_for_status()
                        logger.info(f"🚀 节点 [{node_name}]：分配账号 {account_name} 搬运 {len(batch_tasks)} 个文件 (模式: {first_method})")
                        # 成功派发一次，增加当前循环的并发计数，继续下一轮 while
                        active_count += 1
                    except httpx.RequestError as e:
                        logger.warning(
                            "⚠️ 呼叫节点 [%s] 出现传输错误，派发结果不确定；保留 dispatch [%s] 的任务与租约等待心跳。错误: %s",
                            node_name,
                            dispatch_id,
                            e,
                        )
                        break
                    except Exception as e:
                        logger.error(f"❌ 呼叫节点 [{node_name}] 失败: {e}")
                        cursor.execute(
                            f"UPDATE file_tasks SET status='pending', dispatch_id=NULL, updated_at=UTC_TIMESTAMP() "
                            f"WHERE dispatch_id=%s AND id IN ({format_strings})",
                            [dispatch_id] + task_ids,
                        )
                        release_account_lock(cursor, account_name, dispatch_id, node_name)
                        break # 如果节点失联报错，就停止给它派发更多任务
    finally:
        db.close()

# (清道夫逻辑保持不变)
def dispatch_cleaner_tasks(force_all=False):
    if not CLEANER_DISPATCH_ENABLED:
        return

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            stale_nodes = set(get_stale_worker_nodes(cursor))
            released_cleaner_locks = release_cleaner_locks_for_stale_nodes(cursor, list(stale_nodes))
            if released_cleaner_locks > 0:
                logger.warning(
                    "🧹 已为失联节点释放 %s 个 cleaner 调度锁，等待下轮重新派发。",
                    released_cleaner_locks,
                )

            cursor.execute(
                """
                SELECT
                    account_used,
                    worker_node,
                    COUNT(*) AS staged_count,
                    MIN(updated_at) AS oldest_staged
                FROM file_tasks
                WHERE status='staged'
                GROUP BY account_used, worker_node
                ORDER BY staged_count DESC, oldest_staged ASC
                """
            )
            staged_rows = cursor.fetchall()
            
            # 用于记录本次调度中已经派发过的清道夫账号，防止向多个节点重复派发同一个清道夫账号
            dispatched_cleaners = set()
            # 当前 worker 只允许有限个 cleaner；同一轮不要反复打同一个节点造成 409/429 风暴。
            cleaner_nodes_touched = set()
            
            for row in staged_rows:
                acc_name = row['account_used']
                node_name = row['worker_node']

                if node_name in stale_nodes:
                    logger.warning("🧟 清道夫跳过节点 [%s]：节点已失联，等待恢复后重派。", node_name)
                    continue

                if node_name in cleaner_nodes_touched:
                    logger.info("⏭️ 清道夫跳过节点 [%s]：本轮已经尝试过该节点。", node_name)
                    continue
                
                # 提取账号组前缀 (比如 baicai_1@1 -> baicai_1)
                group_prefix = acc_name.split('@')[0]
                
                # 硬安全挡板：force_all 也不能绕过同组上传中的保护。
                if group_has_active_uploads(cursor, group_prefix):
                    logger.info(f"⏳ 清道夫跳过 [{acc_name}]：同组账号 [{group_prefix}] 仍在上传，当前绝不允许清理。")
                    continue

                all_quota_exhausted, total_uploaders, blocked_uploaders = get_group_quota_exhaustion(cursor, group_prefix)
                clean_reason = "all_quota_exhausted" if all_quota_exhausted else None

                if not all_quota_exhausted:
                    # 软策略：未整组耗尽时，优先等待空闲窗口；force_all 只能绕过这层等待。
                    if not force_all and group_has_recent_activity(cursor, group_prefix):
                        logger.info(
                            "⏳ 清道夫跳过 [%s]：同组账号 [%s] 在最近 %s 分钟内仍有活动，继续等待空闲窗口。",
                            acc_name,
                            group_prefix,
                            CLEANER_IDLE_MINUTES,
                        )
                        continue
                    clean_reason = "force_all" if force_all else "idle_window"

                logger.info(
                    "🧾 清道夫准入 [%s]：group=%s, reason=%s, quota_blocked=%s/%s",
                    acc_name,
                    group_prefix,
                    clean_reason,
                    blocked_uploaders,
                    total_uploaders,
                )
                
                # 从数据库直接获取预先映射好的专职清道夫账号
                cursor.execute("SELECT cleaner_mapping FROM drive_accounts WHERE account_name=%s", (acc_name,))
                mapping_row = cursor.fetchone()
                cleaner_account = mapping_row['cleaner_mapping'] if mapping_row else None
                
                if not cleaner_account:
                    continue

                # 如果这个清道夫账号在本次调度中已经被派发过了，就直接跳过，避免重复消耗 API 和服务器资源
                if cleaner_account in dispatched_cleaners:
                    continue

                cursor.execute(
                    """
                    SELECT ft.id, ft.file_path, ft.source_base_dir
                    FROM file_tasks ft
                    JOIN drive_accounts da ON ft.account_used = da.account_name
                    WHERE ft.status='staged'
                      AND da.cleaner_mapping=%s
                      AND ft.worker_node=%s
                    ORDER BY ft.created_at ASC
                    LIMIT %s
                    """,
                    (cleaner_account, node_name, MAX_CLEANER_FILES_PER_REQUEST),
                )
                staged_tasks = cursor.fetchall()
                if not staged_tasks:
                    continue
                staged_files = [task["file_path"] for task in staged_tasks]
                staged_task_ids = [task["id"] for task in staged_tasks]

                cursor.execute(
                    "SELECT api_url FROM worker_registry "
                    "WHERE node_name=%s AND protocol_version >= 2 "
                    "AND last_active > DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)",
                    (node_name, WORKER_STALE_MINUTES),
                )
                reg = cursor.fetchone()
                if reg:
                    dispatch_id = _new_dispatch_id()
                    try:
                        # 清道夫锁必须通过条件更新获取，避免并发调度器重复派发同一账号。
                        if not acquire_account_lock(cursor, cleaner_account, "cleaner", dispatch_id, node_name):
                            continue

                        # 获取 cleaner 锁后再次检查 uploader，关闭“检查后、加锁前”启动上传的竞态窗口。
                        if group_has_active_uploads(cursor, group_prefix):
                            logger.warning(
                                "⏭️ 清道夫 [%s] 获取锁后发现组 [%s] 已开始上传，撤销本次派发。",
                                cleaner_account,
                                group_prefix,
                            )
                            release_account_lock(cursor, cleaner_account, dispatch_id, node_name)
                            continue

                        task_id_placeholders = ",".join(["%s"] * len(staged_task_ids))
                        cursor.execute(
                            f"UPDATE file_tasks ft "
                            f"JOIN drive_accounts da ON ft.account_used=da.account_name "
                            f"SET ft.dispatch_id=%s, ft.updated_at=UTC_TIMESTAMP() "
                            f"WHERE ft.status='staged' AND da.cleaner_mapping=%s "
                            f"AND ft.worker_node=%s "
                            f"AND ft.id IN ({task_id_placeholders})",
                            [dispatch_id, cleaner_account, node_name] + staged_task_ids,
                        )
                        if cursor.rowcount != len(staged_task_ids):
                            clear_cleaner_dispatch_tasks(cursor, cleaner_account, dispatch_id)
                            release_account_lock(cursor, cleaner_account, dispatch_id, node_name)
                            logger.warning("⏭️ 清道夫 [%s] 的 staged 批次在锁定时发生变化，本轮取消。", cleaner_account)
                            continue

                        # 注意这里下发的 account_name 是专属清道夫账号 cleaner_account，而不是上传时的账号
                        raw_body, request_headers = encode_worker_request(node_name, {
                            "task_type": "clean",
                            "account_name": cleaner_account,
                            "dispatch_id": dispatch_id,
                            "worker_node": node_name,
                            "dest_name": CLEANER_DEST_REMOTE,
                            "remote_dir": STAGING_REMOTE_DIR,
                            "files": staged_files,
                            "source_base_dirs": [task.get("source_base_dir") for task in staged_tasks],
                        })
                        response = httpx.post(
                            reg['api_url'],
                            content=raw_body,
                            headers=request_headers,
                            timeout=10.0,
                        )
                        response.raise_for_status()
                        logger.info(
                            "🧹 成功向节点 [%s] 派发清道夫任务: %s (清理组 %s, 文件数 %s)",
                            node_name,
                            cleaner_account,
                            group_prefix,
                            len(staged_files),
                        )
                        # 记录该清道夫已被派发
                        dispatched_cleaners.add(cleaner_account)
                        cleaner_nodes_touched.add(node_name)
                    except httpx.RequestError as e:
                        logger.warning(
                            "⚠️ 呼叫节点 [%s] 清道夫出现传输错误，派发结果不确定；保留 dispatch [%s] 与租约。错误: %s",
                            node_name,
                            dispatch_id,
                            e,
                        )
                        dispatched_cleaners.add(cleaner_account)
                        cleaner_nodes_touched.add(node_name)
                    except httpx.HTTPStatusError as e:
                        logger.error(f"❌ 呼叫节点 [{node_name}] 清道夫失败: {e}")
                        # worker 已明确表示清道夫忙或重复运行，本轮不要再继续向同节点派发。
                        if e.response.status_code in (409, 429):
                            cleaner_nodes_touched.add(node_name)
                        # 呼叫失败，立即释放锁
                        clear_cleaner_dispatch_tasks(cursor, cleaner_account, dispatch_id)
                        release_account_lock(cursor, cleaner_account, dispatch_id, node_name)
                    except Exception as e:
                        logger.error(f"❌ 呼叫节点 [{node_name}] 清道夫失败: {e}")
                        # 呼叫失败，立即释放锁
                        clear_cleaner_dispatch_tasks(cursor, cleaner_account, dispatch_id)
                        release_account_lock(cursor, cleaner_account, dispatch_id, node_name)
    finally:
        db.close()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    scheduler = BackgroundScheduler(timezone=SCHEDULER_TIMEZONE)
    scheduler.add_job(dispatch_uploader_tasks, 'interval', seconds=60)
    if CLEANER_DISPATCH_ENABLED:
        scheduler.add_job(dispatch_cleaner_tasks, 'interval', minutes=CLEANER_DISPATCH_INTERVAL_MINUTES)
        scheduler.add_job(dispatch_cleaner_tasks, 'cron', hour=3, args=[True], timezone=SCHEDULER_TIMEZONE)
    else:
        logger.warning("🧹 Cleaner dispatch is disabled by CLEANER_DISPATCH_ENABLED=false.")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def home(): return {"status": "Command Center Active"}


@app.get("/health/ready")
def health_ready():
    db = None
    try:
        db = get_db_connection()
        with db.cursor() as cursor:
            cursor.execute(
                "SELECT source_base_dir, source_mtime, dispatch_id FROM file_tasks LIMIT 0"
            )
            cursor.execute(
                "SELECT protocol_version FROM worker_registry LIMIT 0"
            )
            cursor.execute(
                "SELECT dispatch_locked_until, current_dispatch_id, current_worker_node "
                "FROM drive_accounts LIMIT 0"
            )
            cursor.execute(
                "SELECT dispatch_id FROM account_upload_events LIMIT 0"
            )
        return {"status": "ready"}
    except Exception as exc:
        logger.error("数据库 readiness 检查失败: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    finally:
        if db is not None:
            db.close()


@app.post("/api/report_files")
def report_new_files(req: ReportFilesRequest, node_auth: dict[str, Optional[str]] = Depends(verify_node_request)):
    if node_auth["method"] == "hmac" and node_auth["node_name"] != req.worker_node:
        raise HTTPException(status_code=400, detail="签名节点与 worker_node 不一致")
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            resolved_api_url = resolve_worker_api_url(req.worker_node, req.worker_api_url)
            cursor.execute(
                "INSERT INTO worker_registry (node_name, api_url, protocol_version, last_active) "
                "VALUES (%s, %s, %s, UTC_TIMESTAMP()) "
                "ON DUPLICATE KEY UPDATE api_url=VALUES(api_url), "
                "protocol_version=VALUES(protocol_version), last_active=UTC_TIMESTAMP()",
                (req.worker_node, resolved_api_url, req.protocol_version),
            )

            accepted_files = [f for f in req.files if not should_ignore_upload_path(f.path)]
            ignored_count = len(req.files) - len(accepted_files)
            if ignored_count:
                logger.warning(
                    "🧹 节点 [%s] 上报中忽略 %s 个临时文件（后缀: %s）。",
                    req.worker_node,
                    ignored_count,
                    ", ".join(IGNORED_UPLOAD_EXTENSIONS),
                )

            if accepted_files:
                initial_status = "completed" if req.is_history_sync else "pending"
                sql = """
                    INSERT INTO file_tasks
                        (file_path, source_base_dir, size_bytes, source_mtime, status, worker_node, upload_method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        status = CASE
                            WHEN VALUES(status) = 'pending' AND (
                                file_tasks.status = 'not_found'
                                OR (
                                    (
                                        file_tasks.status = 'completed'
                                    )
                                    AND (
                                        (file_tasks.size_bytes IS NOT NULL AND VALUES(size_bytes) IS NOT NULL
                                         AND file_tasks.size_bytes <> VALUES(size_bytes))
                                        OR (file_tasks.source_mtime IS NOT NULL AND VALUES(source_mtime) IS NOT NULL
                                            AND file_tasks.source_mtime <> VALUES(source_mtime))
                                    )
                                )
                            )
                            THEN 'pending'
                            ELSE file_tasks.status
                        END,
                        account_used = CASE
                            WHEN file_tasks.status = 'pending' AND VALUES(status) = 'pending'
                            THEN NULL
                            ELSE file_tasks.account_used
                        END,
                        dispatch_id = CASE
                            WHEN file_tasks.status = 'pending' AND VALUES(status) = 'pending'
                            THEN NULL
                            ELSE file_tasks.dispatch_id
                        END,
                        size_bytes = CASE
                            WHEN VALUES(status) = 'pending' AND file_tasks.status = 'pending'
                            THEN COALESCE(VALUES(size_bytes), file_tasks.size_bytes)
                            WHEN file_tasks.status IN ('uploading', 'staged')
                            THEN file_tasks.size_bytes
                            ELSE COALESCE(file_tasks.size_bytes, VALUES(size_bytes))
                        END,
                        source_mtime = CASE
                            WHEN VALUES(status) = 'pending' AND file_tasks.status = 'pending'
                            THEN COALESCE(VALUES(source_mtime), file_tasks.source_mtime)
                            WHEN file_tasks.status IN ('uploading', 'staged')
                            THEN file_tasks.source_mtime
                            ELSE COALESCE(file_tasks.source_mtime, VALUES(source_mtime))
                        END,
                        source_base_dir = CASE
                            WHEN VALUES(status) = 'pending' AND file_tasks.status IN ('pending', 'completed')
                            THEN COALESCE(VALUES(source_base_dir), file_tasks.source_base_dir)
                            WHEN file_tasks.status IN ('uploading', 'staged')
                            THEN file_tasks.source_base_dir
                            ELSE COALESCE(file_tasks.source_base_dir, VALUES(source_base_dir))
                        END,
                        worker_node = CASE
                            WHEN VALUES(status) = 'pending' AND file_tasks.status = 'pending'
                            THEN VALUES(worker_node)
                            ELSE file_tasks.worker_node
                        END,
                        upload_method = CASE
                            WHEN VALUES(status) = 'pending' AND file_tasks.status = 'pending'
                            THEN VALUES(upload_method)
                            ELSE file_tasks.upload_method
                        END
                """
                
                values = [
                    (
                        f.path,
                        f.source_base_dir,
                        f.size_bytes,
                        f.mtime,
                        initial_status,
                        req.worker_node,
                        req.upload_method,
                    )
                    for f in accepted_files
                ]
                chunk_size = 5000
                for i in range(0, len(values), chunk_size):
                    chunk = values[i:i+chunk_size]
                    cursor.executemany(sql, chunk)

                # 活跃任务保留派发时的文件指纹；新指纹尚未入库时返回 deferred_files，
                # 让 Scanner 继续上报，直到旧任务结束后可以更新记录或重新排队。
                deferred_files: list[str] = []
                incoming_by_path = {item.path: item for item in accepted_files}
                accepted_paths = list(incoming_by_path)
                for i in range(0, len(accepted_paths), chunk_size):
                    path_chunk = accepted_paths[i:i + chunk_size]
                    path_hashes = [
                        hashlib.sha256(path.encode("utf-8")).hexdigest()
                        for path in path_chunk
                    ]
                    placeholders = ",".join(["%s"] * len(path_chunk))
                    cursor.execute(
                        f"""
                        SELECT file_path, source_base_dir, size_bytes, source_mtime
                        FROM file_tasks
                        WHERE file_path_hash IN ({placeholders})
                        """,
                        path_hashes,
                    )
                    matched_paths: set[str] = set()
                    for row in cursor.fetchall():
                        if row["file_path"] not in incoming_by_path:
                            logger.error(
                                "检测到 file_path_hash 理论碰撞，拒绝缓存路径: %r",
                                row["file_path"],
                            )
                            continue
                        matched_paths.add(row["file_path"])
                        incoming = incoming_by_path[row["file_path"]]
                        fingerprint_matches = (
                            (incoming.size_bytes is None or row["size_bytes"] == incoming.size_bytes)
                            and (incoming.mtime is None or row["source_mtime"] == incoming.mtime)
                            and (
                                incoming.source_base_dir is None
                                or row["source_base_dir"] == incoming.source_base_dir
                            )
                        )
                        if not fingerprint_matches:
                            deferred_files.append(incoming.path)
                    deferred_files.extend(
                        path for path in path_chunk if path not in matched_paths
                    )
                    
                if req.is_history_sync:
                    logger.info(f"📚 历史同步：成功将 {len(accepted_files)} 个已存在的大盘文件注入数据库防重复屏障。")
                else:
                    logger.info(f"📥 接收新情报：节点 [{req.worker_node}] 汇报 {len(accepted_files)} 个新文件入库待搬运。")

        return {
            "status": "success",
            "deferred_files": deferred_files if accepted_files else [],
        }
    finally:
        db.close()


@app.post("/api/dispatch/validate")
def validate_dispatch_lease(
    req: DispatchLeaseRequest,
    node_auth: dict[str, Optional[str]] = Depends(verify_node_request),
):
    if node_auth["method"] == "hmac" and node_auth["node_name"] != req.worker_node:
        raise HTTPException(status_code=400, detail="签名节点与 worker_node 不一致")

    db = get_db_connection()
    try:
        db.begin()
        with db.cursor() as cursor:
            cursor.execute(
                """
                SELECT id
                FROM file_tasks
                WHERE dispatch_id=%s AND status IN ('uploading', 'staged')
                LIMIT 1
                FOR UPDATE
                """,
                (req.dispatch_id,),
            )
            has_active_task = cursor.fetchone() is not None
            if not has_active_task:
                cursor.execute(
                    """
                    UPDATE drive_accounts
                    SET dispatch_locked_until=NULL,
                        current_dispatch_id=NULL,
                        current_worker_node=NULL
                    WHERE account_name=%s
                      AND current_dispatch_id=%s
                      AND current_worker_node=%s
                    """,
                    (req.account_name, req.dispatch_id, req.worker_node),
                )
                active = False
            else:
                cursor.execute(
                """
                SELECT 1 AS active
                FROM drive_accounts
                WHERE account_name=%s
                  AND is_active=1
                  AND current_dispatch_id=%s
                  AND current_worker_node=%s
                  AND dispatch_locked_until IS NOT NULL
                  AND dispatch_locked_until > UTC_TIMESTAMP()
                FOR UPDATE
                """,
                (
                    req.account_name,
                    req.dispatch_id,
                    req.worker_node,
                ),
                )
                active = cursor.fetchone() is not None
            if active:
                cursor.execute(
                    """
                    UPDATE drive_accounts
                    SET dispatch_locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
                    WHERE account_name=%s
                      AND current_dispatch_id=%s
                      AND current_worker_node=%s
                    """,
                    (
                        ACCOUNT_DISPATCH_LEASE_MINUTES,
                        req.account_name,
                        req.dispatch_id,
                        req.worker_node,
                    ),
                )
                cursor.execute(
                    """
                    UPDATE file_tasks
                    SET updated_at=UTC_TIMESTAMP()
                    WHERE dispatch_id=%s AND status IN ('uploading', 'staged')
                    """,
                    (req.dispatch_id,),
                )
                cursor.execute(
                    "UPDATE worker_registry SET last_active=UTC_TIMESTAMP() WHERE node_name=%s",
                    (req.worker_node,),
                )
        db.commit()
        return {"active": active}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@app.get("/api/upload_usage", dependencies=[Depends(verify_token)])
def get_upload_usage(hours: int = Query(default=24, ge=1, le=24 * 14)):
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    da.account_name,
                    da.role,
                    da.is_active,
                    da.dispatch_locked_until,
                    da.dispatch_cooldown_until,
                    da.quota_blocked_until,
                    COALESCE(usage_stats.uploaded_files, 0) AS uploaded_files,
                    COALESCE(usage_stats.uploaded_bytes, 0) AS uploaded_bytes,
                    usage_stats.last_uploaded_at_utc,
                    usage_stats.last_uploaded_at_bj
                FROM drive_accounts da
                LEFT JOIN (
                    SELECT
                        account_name,
                        COUNT(*) AS uploaded_files,
                        COALESCE(SUM(size_bytes), 0) AS uploaded_bytes,
                        MAX(uploaded_at) AS last_uploaded_at_utc,
                        MAX(DATE_ADD(uploaded_at, INTERVAL 8 HOUR)) AS last_uploaded_at_bj
                    FROM account_upload_events
                    WHERE uploaded_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s HOUR)
                    GROUP BY account_name
                ) usage_stats ON usage_stats.account_name = da.account_name
                WHERE da.role = 'uploader'
                ORDER BY uploaded_bytes DESC, uploaded_files DESC, da.account_name ASC
                """,
                (hours,),
            )
            accounts = []
            total_bytes = 0
            total_files = 0
            for row in cursor.fetchall():
                uploaded_bytes = int(row["uploaded_bytes"] or 0)
                uploaded_files = int(row["uploaded_files"] or 0)
                unlock_at_utc, unlock_at_bj, active_locks, is_available_now = _build_account_unlock_info(row)
                total_bytes += uploaded_bytes
                total_files += uploaded_files
                accounts.append(
                    {
                        "account_name": row["account_name"],
                        "role": row["role"],
                        "is_active": row["is_active"],
                        "uploaded_files": uploaded_files,
                        "uploaded_bytes": uploaded_bytes,
                        "uploaded_gib": round(uploaded_bytes / (1024 ** 3), 3),
                        "last_uploaded_at_utc": _format_db_datetime(row["last_uploaded_at_utc"]),
                        "last_uploaded_at_bj": _format_db_datetime(row["last_uploaded_at_bj"]),
                        "dispatch_locked_until_utc": _format_db_datetime(row["dispatch_locked_until"]),
                        "dispatch_locked_until_bj": _format_db_datetime_as_bj(row["dispatch_locked_until"]),
                        "dispatch_cooldown_until_utc": _format_db_datetime(row["dispatch_cooldown_until"]),
                        "dispatch_cooldown_until_bj": _format_db_datetime_as_bj(row["dispatch_cooldown_until"]),
                        "quota_blocked_until_utc": _format_db_datetime(row["quota_blocked_until"]),
                        "quota_blocked_until_bj": _format_db_datetime_as_bj(row["quota_blocked_until"]),
                        "unlock_at_utc": unlock_at_utc,
                        "unlock_at_bj": unlock_at_bj,
                        "active_locks": active_locks,
                        "is_available_now": is_available_now,
                    }
                )

        return {
            "window_hours": hours,
            "timezone": "Asia/Shanghai",
            "total_files": total_files,
            "total_bytes": total_bytes,
            "total_gib": round(total_bytes / (1024 ** 3), 3),
            "accounts": accounts,
        }
    finally:
        db.close()


@app.get("/api/upload_usage/daily", dependencies=[Depends(verify_token)])
def get_daily_upload_usage(days: int = Query(default=7, ge=1, le=90)):
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    DATE(DATE_ADD(uploaded_at, INTERVAL 8 HOUR)) AS upload_date_bj,
                    account_name,
                    COUNT(*) AS uploaded_files,
                    COALESCE(SUM(size_bytes), 0) AS uploaded_bytes,
                    MIN(uploaded_at) AS first_uploaded_at_utc,
                    MAX(uploaded_at) AS last_uploaded_at_utc,
                    MIN(DATE_ADD(uploaded_at, INTERVAL 8 HOUR)) AS first_uploaded_at_bj,
                    MAX(DATE_ADD(uploaded_at, INTERVAL 8 HOUR)) AS last_uploaded_at_bj
                FROM account_upload_events
                WHERE uploaded_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                GROUP BY upload_date_bj, account_name
                ORDER BY upload_date_bj DESC, uploaded_bytes DESC, uploaded_files DESC, account_name ASC
                """,
                (days,),
            )
            rows = []
            total_bytes = 0
            total_files = 0
            for row in cursor.fetchall():
                uploaded_bytes = int(row["uploaded_bytes"] or 0)
                uploaded_files = int(row["uploaded_files"] or 0)
                total_bytes += uploaded_bytes
                total_files += uploaded_files
                rows.append(
                    {
                        "upload_date_bj": row["upload_date_bj"],
                        "account_name": row["account_name"],
                        "uploaded_files": uploaded_files,
                        "uploaded_bytes": uploaded_bytes,
                        "uploaded_gib": round(uploaded_bytes / (1024 ** 3), 3),
                        "first_uploaded_at_utc": row["first_uploaded_at_utc"],
                        "last_uploaded_at_utc": row["last_uploaded_at_utc"],
                        "first_uploaded_at_bj": row["first_uploaded_at_bj"],
                        "last_uploaded_at_bj": row["last_uploaded_at_bj"],
                    }
                )

        return {
            "days": days,
            "timezone": "Asia/Shanghai",
            "total_files": total_files,
            "total_bytes": total_bytes,
            "total_gib": round(total_bytes / (1024 ** 3), 3),
            "rows": rows,
        }
    finally:
        db.close()


@app.post("/api/report_status")
def receive_worker_report(req: ReportStatusRequest, node_auth: dict[str, Optional[str]] = Depends(verify_node_request)):
    if node_auth["method"] == "hmac" and node_auth["node_name"] != req.worker_node:
        raise HTTPException(status_code=400, detail="签名节点与 worker_node 不一致")
    db = get_db_connection()
    trigger_dispatch_now = False
    try:
        db.begin()
        with db.cursor() as cursor:
            # baicai_big 和 baicai_big2 都是最终盘账号，成功后不进入 staged。
            is_direct_upload = is_direct_upload_account(req.account_name)
            # 决定成功的任务是进入中转候车区(staged)还是直接完成(completed)
            success_target_status = "completed" if is_direct_upload else "staged"
            has_active_task = True
            if req.dispatch_id is not None:
                cursor.execute(
                    """
                    SELECT id
                    FROM file_tasks
                    WHERE dispatch_id=%s
                      AND status IN ('uploading', 'staged')
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (req.dispatch_id,),
                )
                has_active_task = cursor.fetchone() is not None

            cursor.execute(
                "SELECT role, current_dispatch_id, current_worker_node "
                "FROM drive_accounts WHERE account_name=%s FOR UPDATE",
                (req.account_name,),
            )
            role_row = cursor.fetchone()
            if role_row is None:
                raise HTTPException(status_code=404, detail=f"账号 [{req.account_name}] 不存在")

            account_role = role_row.get("role", "uploader")
            current_dispatch_id = role_row.get("current_dispatch_id")
            current_worker_node = role_row.get("current_worker_node")

            if req.dispatch_id is None:
                lease_matches = (
                    ALLOW_LEGACY_UNLEASED_REPORTS
                    and current_dispatch_id is None
                    and current_worker_node is None
                )
            else:
                lease_matches = (
                    req.dispatch_id == current_dispatch_id
                    and req.worker_node == current_worker_node
                )

            # 先校验租约，再处理成功文件、配额和账号状态，避免延迟回执污染后续批次。
            if not lease_matches:
                logger.warning(
                    "⏭️ 忽略账号 [%s] 的过期回执: report_dispatch=%s, report_node=%s, current_dispatch=%s, current_node=%s",
                    req.account_name,
                    req.dispatch_id,
                    req.worker_node,
                    current_dispatch_id,
                    current_worker_node,
                )
                db.commit()
                return {"status": "ignored", "reason": "stale_dispatch"}

            if req.status == "uploading" and not has_active_task:
                logger.warning(
                    "⏭️ 账号 [%s] 的 dispatch [%s] 已无活跃任务，通知 Worker 终止旧进程。",
                    req.account_name,
                    req.dispatch_id,
                )
                release_account_lock(
                    cursor,
                    req.account_name,
                    req.dispatch_id,
                    req.worker_node,
                )
                db.commit()
                return {"status": "ignored", "reason": "stale_dispatch"}

            # 1. 如果 Worker 汇报了部分成功的文件（即使最终状态是 403 或 failed），先抢救这些记录
            if req.successful_files:
                logger.info(f"🛡️ 抢救记录：从账号 {req.account_name} 抢救出 {len(req.successful_files)} 个已完成文件")
                if account_role == "cleaner":
                    update_cleaner_tasks_by_files(
                        cursor,
                        req.account_name,
                        req.successful_files,
                        "completed",
                        req.dispatch_id,
                    )
                else:
                    mark_successful_uploads_by_files(
                        cursor,
                        req.account_name,
                        success_target_status,
                        req.successful_files,
                        req.dispatch_id,
                    )

            if req.missing_files:
                missing_target_status = "not_found"
                logger.warning(
                    "⚠️ 账号 [%s] 上报 %s 个缺失文件，已转为 %s。",
                    req.account_name,
                    len(req.missing_files),
                    missing_target_status,
                )
                update_tasks_by_account_and_files(
                    cursor,
                    req.account_name,
                    "uploading",
                    missing_target_status,
                    req.missing_files,
                    req.dispatch_id,
                )

            if req.status == "uploading":
                if account_role != "cleaner":
                    recovered_pending, _refreshed_tasks, refreshed_lease = refresh_uploader_heartbeat(
                        cursor,
                        req.account_name,
                        req.dispatch_id,
                        req.worker_node,
                    )
                    if recovered_pending > 0:
                        logger.warning(
                            "🩹 账号 [%s] 心跳恢复 %s 个已误回退的 pending 任务为 uploading。",
                            req.account_name,
                            recovered_pending,
                        )
                    if req.dispatch_id is not None and refreshed_lease == 0:
                        db.commit()
                        return {"status": "ignored", "reason": "stale_dispatch"}
                else:
                    refreshed_lease = refresh_account_lease(
                        cursor,
                        req.account_name,
                        "cleaner",
                        req.dispatch_id,
                        req.worker_node,
                    )
                    if refreshed_lease == 0:
                        db.commit()
                        return {"status": "ignored", "reason": "stale_dispatch"}

                if req.worker_node is not None:
                    cursor.execute(
                        "UPDATE worker_registry SET last_active=UTC_TIMESTAMP() WHERE node_name=%s",
                        (req.worker_node,),
                    )
                else:
                    task_condition, task_params = _task_dispatch_condition(req.dispatch_id, "ft.dispatch_id")
                    cursor.execute(
                        f"""
                        UPDATE worker_registry wr
                        JOIN file_tasks ft ON wr.node_name=ft.worker_node
                        SET wr.last_active=UTC_TIMESTAMP()
                        WHERE ft.account_used=%s AND {task_condition}
                        """,
                        [req.account_name] + task_params,
                    )
                db.commit()
                return {"status": "success"}

            # 2. 处理最终的状态和账号冷却逻辑
            lease_condition, lease_params = _account_lease_condition(req.dispatch_id, req.worker_node)
            if req.status == "403_quota":
                # 先回退任务，再释放账号租约。任何一步中断后，重复回执都能继续收敛。
                revert_uploading_tasks(cursor, req.account_name, clear_account_used=True, dispatch_id=req.dispatch_id)
                # 403 配额耗尽：释放调度执行锁，并打上独立的配额封禁时间。
                cursor.execute(
                    f"""
                    UPDATE drive_accounts
                    SET dispatch_locked_until = NULL,
                        quota_blocked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s HOUR),
                        current_dispatch_id=NULL,
                        current_worker_node=NULL
                    WHERE account_name=%s AND {lease_condition}
                    """,
                    [QUOTA_BLOCK_HOURS, req.account_name] + lease_params,
                )
                trigger_dispatch_now = True
            elif req.status == "credential_invalid":
                if account_role != "cleaner":
                    revert_uploading_tasks(cursor, req.account_name, clear_account_used=True, dispatch_id=req.dispatch_id)
                    clear_pending_account_preference(cursor, req.account_name)
                else:
                    clear_cleaner_dispatch_tasks(cursor, req.account_name, req.dispatch_id)
                cursor.execute(
                    f"""
                    UPDATE drive_accounts
                    SET is_active = 0,
                        dispatch_locked_until = NULL,
                        dispatch_cooldown_until = NULL,
                        cooldown_until = NULL,
                        current_dispatch_id=NULL,
                        current_worker_node=NULL
                    WHERE account_name=%s AND {lease_condition}
                    """,
                    [req.account_name] + lease_params,
                )
                logger.warning(
                    "⛔ 账号 [%s] 命中凭据失效错误，已永久停用，需人工 reconnect 后再手动恢复。",
                    req.account_name,
                )
                trigger_dispatch_now = True
            elif req.status == "completed":
                mark_all_uploading_tasks_success(cursor, req.account_name, success_target_status, req.dispatch_id)
                release_account_lock(cursor, req.account_name, req.dispatch_id, req.worker_node)
            elif req.status == "clean_success":
                if account_role != "cleaner":
                    logger.warning("⚠️ 非清道夫账号 [%s] 上报 clean_success，已忽略批量状态变更。", req.account_name)
                    clear_cleaner_dispatch_tasks(cursor, req.account_name, req.dispatch_id)
                else:
                    completed_count = complete_cleaner_dispatch_tasks(
                        cursor, req.account_name, req.dispatch_id
                    )
                    logger.info(
                        "🧹 清道夫账号 [%s] 整盘移动成功，已将 dispatch [%s] 剩余 %s 条 staged 记录标记为 completed。",
                        req.account_name,
                        req.dispatch_id,
                        completed_count,
                    )
                release_account_lock(cursor, req.account_name, req.dispatch_id, req.worker_node)
            elif req.status == "failed":
                if account_role == "cleaner":
                    clear_cleaner_dispatch_tasks(cursor, req.account_name, req.dispatch_id)
                    cursor.execute(
                        f"""
                        UPDATE drive_accounts
                        SET dispatch_locked_until = NULL,
                            dispatch_cooldown_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE),
                            current_dispatch_id=NULL,
                            current_worker_node=NULL
                        WHERE account_name=%s AND {lease_condition}
                        """,
                        [CLEANER_FAILURE_COOLDOWN_MINUTES, req.account_name] + lease_params,
                    )
                    logger.warning(
                        "🧹 清道夫账号 [%s] 执行失败，进入 %s 分钟调度冷却。",
                        req.account_name,
                        CLEANER_FAILURE_COOLDOWN_MINUTES,
                    )
                else:
                    # 上传失败时，仅将仍处于 uploading 的任务打回 pending
                    revert_uploading_tasks(cursor, req.account_name, clear_account_used=False, dispatch_id=req.dispatch_id)
                    release_account_lock(cursor, req.account_name, req.dispatch_id, req.worker_node)
            elif req.status == "not_found":
                logger.warning("⚠️ 账号 [%s] 上报 not_found，明确缺失任务保持 not_found 等待 Scanner 重新发现。", req.account_name)
                revert_uploading_tasks(cursor, req.account_name, clear_account_used=True, dispatch_id=req.dispatch_id)
                release_account_lock(cursor, req.account_name, req.dispatch_id, req.worker_node)
        db.commit()
        if trigger_dispatch_now:
            trigger_dispatch_uploader_tasks_async(f"{req.status}:{req.account_name}")
        return {"status": "success"}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
