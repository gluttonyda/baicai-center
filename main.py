import json
import logging
import os
import threading
from typing import Any, List, Literal, Optional
from contextlib import asynccontextmanager

import httpx
import pymysql
from apscheduler.schedulers.background import BackgroundScheduler
from dbutils.pooled_db import PooledDB
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator

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
CLEANER_DISPATCH_INTERVAL_MINUTES = max(1, int(os.getenv("CLEANER_DISPATCH_INTERVAL_MINUTES", "15")))
WORKER_STALE_MINUTES = int(os.getenv("WORKER_STALE_MINUTES", "10"))
ACCOUNT_DISPATCH_LEASE_MINUTES = int(os.getenv("ACCOUNT_DISPATCH_LEASE_MINUTES", "60"))
UPLOADING_STALE_MINUTES = int(os.getenv("UPLOADING_STALE_MINUTES", "30"))
QUOTA_BLOCK_HOURS = int(os.getenv("QUOTA_BLOCK_HOURS", "24"))
CLEANER_UPDATE_SQL_CHUNK_SIZE = max(1, min(MAX_SUCCESSFUL_FILES, int(os.getenv("CLEANER_UPDATE_SQL_CHUNK_SIZE", "1000"))))
SCHEDULER_TIMEZONE = "Asia/Shanghai"

security = HTTPBearer()
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


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.credentials


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

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        clean_path = value.strip()
        if not clean_path:
            raise ValueError("files 中不能出现空路径")
        if "\x00" in clean_path or len(clean_path) > 1024:
            raise ValueError("files 中存在非法路径")
        return clean_path


class ReportFilesRequest(BaseModel):
    worker_node: str = Field(min_length=1, max_length=128)
    worker_api_url: str = Field(min_length=8, max_length=255)
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
                normalized_files.append({"path": item, "size_bytes": None})
            elif isinstance(item, dict):
                path = item.get("path", item.get("file_path"))
                normalized_files.append({"path": path, "size_bytes": item.get("size_bytes")})
            else:
                raise ValueError("files 必须全部为字符串或文件对象")
        return normalized_files


class ReportStatusRequest(BaseModel):
    account_name: str = Field(min_length=1, max_length=128)
    status: Literal["uploading", "completed", "failed", "403_quota", "clean_success", "not_found", "credential_invalid"]
    successful_files: List[str] = Field(default_factory=list)
    missing_files: List[str] = Field(default_factory=list)

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
            clean_path = file_path.strip()
            if not clean_path:
                raise ValueError("successful_files 中不能出现空路径")
            if "\x00" in clean_path or len(clean_path) > 1024:
                raise ValueError("successful_files 中存在非法路径")
            normalized_files.append(clean_path)
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
            clean_path = file_path.strip()
            if not clean_path:
                raise ValueError("missing_files 中不能出现空路径")
            if "\x00" in clean_path or len(clean_path) > 1024:
                raise ValueError("missing_files 中存在非法路径")
            normalized_files.append(clean_path)
        return normalized_files


def acquire_account_lock(cursor, account_name: str, role: str) -> bool:
    cooldown_clause = ""
    if role == "uploader":
        cooldown_clause = "AND (dispatch_cooldown_until IS NULL OR dispatch_cooldown_until < UTC_TIMESTAMP())"
    cursor.execute(
        f"""
        UPDATE drive_accounts
        SET dispatch_locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        WHERE account_name=%s
          AND role=%s
          AND is_active=1
          AND (dispatch_locked_until IS NULL OR dispatch_locked_until < UTC_TIMESTAMP())
          {cooldown_clause}
          AND (quota_blocked_until IS NULL OR quota_blocked_until < UTC_TIMESTAMP())
        """,
        (ACCOUNT_DISPATCH_LEASE_MINUTES, account_name, role),
    )
    return cursor.rowcount == 1


def release_account_lock(cursor, account_name: str):
    cursor.execute("UPDATE drive_accounts SET dispatch_locked_until = NULL WHERE account_name=%s", (account_name,))


def set_account_dispatch_cooldown(cursor, account_name: str, minutes: int):
    cursor.execute(
        """
        UPDATE drive_accounts
        SET dispatch_cooldown_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        WHERE account_name=%s
        """,
        (minutes, account_name),
    )


def refresh_uploader_heartbeat(cursor, account_name: str) -> int:
    cursor.execute(
        """
        UPDATE file_tasks
        SET status='uploading', updated_at=UTC_TIMESTAMP()
        WHERE account_used=%s AND status='pending'
        """,
        (account_name,),
    )
    recovered_pending = cursor.rowcount if isinstance(cursor.rowcount, int) else 0

    cursor.execute(
        """
        UPDATE file_tasks
        SET updated_at = UTC_TIMESTAMP()
        WHERE account_used=%s AND status='uploading'
        """,
        (account_name,),
    )

    cursor.execute(
        """
        UPDATE drive_accounts
        SET dispatch_locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        WHERE account_name=%s
          AND role='uploader'
          AND is_active=1
        """,
        (ACCOUNT_DISPATCH_LEASE_MINUTES, account_name),
    )
    return recovered_pending


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


def update_tasks_by_account_and_files(cursor, account_name: str, current_status: str, target_status: str, file_paths: List[str]):
    if not file_paths:
        return
    format_strings = ",".join(["%s"] * len(file_paths))
    cursor.execute(
        f"UPDATE file_tasks SET status=%s, account_used=NULL, updated_at=UTC_TIMESTAMP() "
        f"WHERE account_used=%s AND status=%s AND file_path IN ({format_strings})",
        [target_status, account_name, current_status] + file_paths,
    )


def update_cleaner_tasks_by_files(cursor, cleaner_account: str, file_paths: List[str], target_status: str):
    if not file_paths:
        return
    for start in range(0, len(file_paths), CLEANER_UPDATE_SQL_CHUNK_SIZE):
        chunk = file_paths[start:start + CLEANER_UPDATE_SQL_CHUNK_SIZE]
        format_strings = ",".join(["%s"] * len(chunk))
        cursor.execute(
            f"""
            UPDATE file_tasks ft
            JOIN drive_accounts da ON ft.account_used = da.account_name
            SET ft.status = %s,
                ft.updated_at = UTC_TIMESTAMP()
            WHERE ft.status = 'staged'
              AND da.cleaner_mapping = %s
              AND ft.file_path IN ({format_strings})
            """,
            [target_status, cleaner_account] + chunk,
        )


def insert_upload_usage_events_for_files(cursor, account_name: str, file_paths: List[str]):
    if not file_paths:
        return
    for start in range(0, len(file_paths), MAX_SUCCESSFUL_FILES):
        chunk = file_paths[start:start + MAX_SUCCESSFUL_FILES]
        format_strings = ",".join(["%s"] * len(chunk))
        cursor.execute(
            f"""
            INSERT IGNORE INTO account_upload_events
                (account_name, file_task_id, size_bytes, uploaded_at)
            SELECT %s, id, COALESCE(size_bytes, 0), UTC_TIMESTAMP()
            FROM file_tasks
            WHERE account_used=%s
              AND status='uploading'
              AND file_path IN ({format_strings})
            """,
            [account_name, account_name] + chunk,
        )


def insert_upload_usage_events_for_account(cursor, account_name: str):
    cursor.execute(
        """
        INSERT IGNORE INTO account_upload_events
            (account_name, file_task_id, size_bytes, uploaded_at)
        SELECT %s, id, COALESCE(size_bytes, 0), UTC_TIMESTAMP()
        FROM file_tasks
        WHERE account_used=%s
          AND status='uploading'
        """,
        (account_name, account_name),
    )


def mark_successful_uploads_by_files(cursor, account_name: str, target_status: str, file_paths: List[str]):
    if not file_paths:
        return
    insert_upload_usage_events_for_files(cursor, account_name, file_paths)
    for start in range(0, len(file_paths), MAX_SUCCESSFUL_FILES):
        chunk = file_paths[start:start + MAX_SUCCESSFUL_FILES]
        format_strings = ",".join(["%s"] * len(chunk))
        cursor.execute(
            f"""
            UPDATE file_tasks
            SET status=%s,
                updated_at=UTC_TIMESTAMP()
            WHERE account_used=%s
              AND status='uploading'
              AND file_path IN ({format_strings})
            """,
            [target_status, account_name] + chunk,
        )


def mark_all_uploading_tasks_success(cursor, account_name: str, target_status: str):
    insert_upload_usage_events_for_account(cursor, account_name)
    cursor.execute(
        "UPDATE file_tasks SET status=%s, updated_at=UTC_TIMESTAMP() WHERE account_used=%s AND status='uploading'",
        (target_status, account_name),
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


def revert_uploading_tasks(cursor, account_name: str, clear_account_used: bool) -> int:
    if clear_account_used:
        cursor.execute(
            "UPDATE file_tasks SET status='pending', account_used=NULL, updated_at=UTC_TIMESTAMP() WHERE account_used=%s AND status='uploading'",
            (account_name,),
        )
    else:
        cursor.execute(
            "UPDATE file_tasks SET status='pending', updated_at=UTC_TIMESTAMP() WHERE account_used=%s AND status='uploading'",
            (account_name,),
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
            SELECT DISTINCT da.cleaner_mapping AS cleaner_account
            FROM file_tasks ft
            JOIN drive_accounts da ON ft.account_used = da.account_name
            WHERE ft.status='staged'
              AND ft.worker_node=%s
              AND da.cleaner_mapping IS NOT NULL
            """,
            (node_name,),
        )
        cleaner_accounts = [row["cleaner_account"] for row in cursor.fetchall()]
        for cleaner_account in cleaner_accounts:
            release_account_lock(cursor, cleaner_account)
            released_count += cursor.rowcount
    return released_count


def release_stale_uploading_tasks(cursor) -> int:
    cursor.execute(
        """
        SELECT account_used, worker_node, COUNT(*) AS task_count
        FROM file_tasks
        WHERE status='uploading'
          AND account_used IS NOT NULL
          AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
        GROUP BY account_used, worker_node
        """,
        (UPLOADING_STALE_MINUTES,),
    )
    stale_groups = cursor.fetchall()

    released_count = 0
    for group in stale_groups:
        account_name = group["account_used"]
        worker_node = group["worker_node"]
        cursor.execute(
            """
            UPDATE file_tasks
            SET status='pending',
                updated_at=UTC_TIMESTAMP()
            WHERE status='uploading'
              AND account_used=%s
              AND worker_node=%s
              AND updated_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)
            """,
            (account_name, worker_node, UPLOADING_STALE_MINUTES),
        )
        reverted_count = cursor.rowcount
        if reverted_count > 0:
            release_account_lock(cursor, account_name)
            released_count += reverted_count
            logger.warning(
                "🧟 账号 [%s] 在节点 [%s] 的 uploading 心跳超过 %s 分钟未刷新，已释放 %s 个占用任务。",
                account_name,
                worker_node,
                UPLOADING_STALE_MINUTES,
                reverted_count,
            )
    return released_count



# ================= 核心调度：大批次 + 节点限流 (每个节点并发 <= 2) =================
def dispatch_uploader_tasks():
    logger.info("🧠 调度巡检：检查各节点负载并分发任务...")
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

            # 0. 自动释放失联节点的任务（如果节点超过 15 分钟没有活跃，将其 uploading 任务打回 pending）
            dead_nodes = get_stale_worker_nodes(cursor)
            for dead_name in dead_nodes:
                cursor.execute(
                    "SELECT DISTINCT account_used FROM file_tasks "
                    "WHERE status='uploading' AND worker_node=%s AND account_used IS NOT NULL",
                    (dead_name,),
                )
                locked_accounts = [row["account_used"] for row in cursor.fetchall()]
                cursor.execute(
                    "UPDATE file_tasks SET status='pending', updated_at=UTC_TIMESTAMP() WHERE status='uploading' AND worker_node=%s",
                    (dead_name,),
                )
                reverted_count = cursor.rowcount
                for account_name in locked_accounts:
                    release_account_lock(cursor, account_name)
                if reverted_count > 0:
                    logger.warning(f"🧟 节点 [{dead_name}] 失联超过 {WORKER_STALE_MINUTES} 分钟，已释放 {reverted_count} 个占用的任务")

            release_stale_uploading_tasks(cursor)

            # 1. 查找阈值内活跃的节点（排除临时的 Init_Scanner 节点）
            cursor.execute(
                "SELECT node_name, api_url FROM worker_registry "
                "WHERE last_active > DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE) "
                "AND node_name != 'Init_Scanner'",
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
                while active_count < 2:
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
                        if group_prefix not in dispatched_groups_this_cycle:
                            if not acquire_account_lock(cursor, acc_name, "uploader"):
                                continue
                            selected_account = (acc_name, group_prefix)
                            break
                    
                    if not selected_account:
                        logger.info(f"⏳ 节点 [{node_name}]：暂时没有多余的可用账号或账号组均已被分配...")
                        break 

                    account_name, group_prefix = selected_account

                    # 4. 捞取该节点的待办任务：一次派发 500 个
                    cursor.execute("""
                        SELECT id, file_path, upload_method FROM file_tasks 
                        WHERE status='pending'
                          AND worker_node=%s
                          AND (account_used=%s OR account_used IS NULL)
                        ORDER BY
                          CASE WHEN account_used=%s THEN 0 ELSE 1 END,
                          created_at ASC
                        LIMIT 500
                    """, (node_name, account_name, account_name))
                    tasks = cursor.fetchall()
                    if not tasks: 
                        release_account_lock(cursor, account_name)
                        break

                    # 以第一条任务的策略为准，保证批次内动作一致
                    first_method = tasks[0]['upload_method']
                    batch_tasks = [t for t in tasks if t['upload_method'] == first_method]
                    task_ids = [t['id'] for t in batch_tasks]
                    
                    # 5. 更新数据库状态：锁定任务
                    format_strings = ','.join(['%s'] * len(task_ids))
                    cursor.execute(
                        f"UPDATE file_tasks SET status='uploading', account_used=%s, updated_at=UTC_TIMESTAMP() "
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
                        [account_name] + task_ids + [account_name],
                    )
                    if cursor.rowcount != len(task_ids):
                        logger.warning("⚠️ 节点 [%s] 的候选任务在锁定前已被其他流程抢占，本轮跳过该批次。", node_name)
                        release_account_lock(cursor, account_name)
                        break
                    
                    # 将该组前缀加入已派发集合，防止后续并发再选中该组
                    dispatched_groups_this_cycle.add(group_prefix)
                    
                    # 6. 推送指令给 Worker
                    payload = {
                        "task_type": "upload",
                        "upload_method": first_method,
                        "account_name": account_name,
                        "files": [t['file_path'] for t in batch_tasks]
                    }
                    try:
                        response = httpx.post(target_api_url, json=payload, headers={"Authorization": f"Bearer {API_TOKEN}"}, timeout=10.0)
                        response.raise_for_status()
                        logger.info(f"🚀 节点 [{node_name}]：分配账号 {account_name} 搬运 {len(batch_tasks)} 个文件 (模式: {first_method})")
                        # 成功派发一次，增加当前循环的并发计数，继续下一轮 while
                        active_count += 1
                    except Exception as e:
                        logger.error(f"❌ 呼叫节点 [{node_name}] 失败: {e}")
                        cursor.execute(f"UPDATE file_tasks SET status='pending', updated_at=UTC_TIMESTAMP() WHERE id IN ({format_strings})", task_ids)
                        release_account_lock(cursor, account_name)
                        break # 如果节点失联报错，就停止给它派发更多任务
    finally:
        db.close()

# (清道夫逻辑保持不变)
def dispatch_cleaner_tasks(force_all=False):
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
                    SELECT ft.file_path
                    FROM file_tasks ft
                    JOIN drive_accounts da ON ft.account_used = da.account_name
                    WHERE ft.status='staged'
                      AND da.cleaner_mapping=%s
                    ORDER BY ft.created_at ASC
                    LIMIT %s
                    """,
                    (cleaner_account, MAX_CLEANER_FILES_PER_REQUEST),
                )
                staged_files = [row["file_path"] for row in cursor.fetchall()]
                if not staged_files:
                    continue

                cursor.execute(
                    "SELECT api_url FROM worker_registry "
                    "WHERE node_name=%s AND last_active > DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)",
                    (node_name, WORKER_STALE_MINUTES),
                )
                reg = cursor.fetchone()
                if reg:
                    try:
                        # 清道夫锁必须通过条件更新获取，避免并发调度器重复派发同一账号。
                        if not acquire_account_lock(cursor, cleaner_account, "cleaner"):
                            continue

                        # 注意这里下发的 account_name 是专属清道夫账号 cleaner_account，而不是上传时的账号
                        response = httpx.post(
                            reg['api_url'],
                            json={
                                "task_type": "clean",
                                "account_name": cleaner_account,
                                "dest_name": "baicai-big",
                                "files": staged_files,
                            },
                            headers={"Authorization": f"Bearer {API_TOKEN}"},
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
                    except httpx.HTTPStatusError as e:
                        logger.error(f"❌ 呼叫节点 [{node_name}] 清道夫失败: {e}")
                        # worker 已明确表示清道夫忙或重复运行，本轮不要再继续向同节点派发。
                        if e.response.status_code in (409, 429):
                            cleaner_nodes_touched.add(node_name)
                        # 呼叫失败，立即释放锁
                        release_account_lock(cursor, cleaner_account)
                    except Exception as e:
                        logger.error(f"❌ 呼叫节点 [{node_name}] 清道夫失败: {e}")
                        # 呼叫失败，立即释放锁
                        release_account_lock(cursor, cleaner_account)
    finally:
        db.close()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    scheduler = BackgroundScheduler(timezone=SCHEDULER_TIMEZONE)
    scheduler.add_job(dispatch_uploader_tasks, 'interval', seconds=60)
    scheduler.add_job(dispatch_cleaner_tasks, 'interval', minutes=CLEANER_DISPATCH_INTERVAL_MINUTES)
    scheduler.add_job(dispatch_cleaner_tasks, 'cron', hour=3, args=[True], timezone=SCHEDULER_TIMEZONE)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def home(): return {"status": "Command Center Active"}

@app.post("/api/report_files", dependencies=[Depends(verify_token)])
def report_new_files(req: ReportFilesRequest):
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            resolved_api_url = resolve_worker_api_url(req.worker_node, req.worker_api_url)
            cursor.execute(
                "INSERT INTO worker_registry (node_name, api_url, last_active) VALUES (%s, %s, UTC_TIMESTAMP()) "
                "ON DUPLICATE KEY UPDATE api_url=VALUES(api_url), last_active=UTC_TIMESTAMP()",
                (req.worker_node, resolved_api_url),
            )
            
            if req.files:
                initial_status = "completed" if req.is_history_sync else "pending"
                sql = """
                    INSERT INTO file_tasks
                        (file_path, size_bytes, status, worker_node, upload_method)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        size_bytes = COALESCE(file_tasks.size_bytes, VALUES(size_bytes))
                """
                
                values = [(f.path, f.size_bytes, initial_status, req.worker_node, req.upload_method) for f in req.files]
                chunk_size = 5000
                for i in range(0, len(values), chunk_size):
                    chunk = values[i:i+chunk_size]
                    cursor.executemany(sql, chunk)
                    
                if req.is_history_sync:
                    logger.info(f"📚 历史同步：成功将 {len(req.files)} 个已存在的大盘文件注入数据库防重复屏障。")
                else:
                    logger.info(f"📥 接收新情报：节点 [{req.worker_node}] 汇报 {len(req.files)} 个新文件入库待搬运。")

        return {"status": "success"}
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
                    account_name,
                    COUNT(*) AS uploaded_files,
                    COALESCE(SUM(size_bytes), 0) AS uploaded_bytes,
                    MAX(uploaded_at) AS last_uploaded_at_utc,
                    MAX(DATE_ADD(uploaded_at, INTERVAL 8 HOUR)) AS last_uploaded_at_bj
                FROM account_upload_events
                WHERE uploaded_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s HOUR)
                GROUP BY account_name
                ORDER BY uploaded_bytes DESC, uploaded_files DESC, account_name ASC
                """,
                (hours,),
            )
            accounts = []
            total_bytes = 0
            total_files = 0
            for row in cursor.fetchall():
                uploaded_bytes = int(row["uploaded_bytes"] or 0)
                uploaded_files = int(row["uploaded_files"] or 0)
                total_bytes += uploaded_bytes
                total_files += uploaded_files
                accounts.append(
                    {
                        "account_name": row["account_name"],
                        "uploaded_files": uploaded_files,
                        "uploaded_bytes": uploaded_bytes,
                        "uploaded_gib": round(uploaded_bytes / (1024 ** 3), 3),
                        "last_uploaded_at_utc": row["last_uploaded_at_utc"],
                        "last_uploaded_at_bj": row["last_uploaded_at_bj"],
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


@app.post("/api/report_status", dependencies=[Depends(verify_token)])
def receive_worker_report(req: ReportStatusRequest):
    db = get_db_connection()
    trigger_dispatch_now = False
    try:
        with db.cursor() as cursor:
            # 判断当前账号是否是直传大盘账号（baicai_big）
            is_direct_upload = req.account_name.startswith("baicai_big")
            # 决定成功的任务是进入中转候车区(staged)还是直接完成(completed)
            success_target_status = "completed" if is_direct_upload else "staged"
            cursor.execute("SELECT role FROM drive_accounts WHERE account_name=%s", (req.account_name,))
            role_row = cursor.fetchone()
            account_role = role_row["role"] if role_row else "uploader"

            # 1. 如果 Worker 汇报了部分成功的文件（即使最终状态是 403 或 failed），先抢救这些记录
            if req.successful_files:
                logger.info(f"🛡️ 抢救记录：从账号 {req.account_name} 抢救出 {len(req.successful_files)} 个已完成文件")
                if account_role == "cleaner":
                    update_cleaner_tasks_by_files(cursor, req.account_name, req.successful_files, "completed")
                else:
                    mark_successful_uploads_by_files(cursor, req.account_name, success_target_status, req.successful_files)

            if req.missing_files:
                logger.warning("⚠️ 账号 [%s] 上报 %s 个缺失文件，已标记为 not_found。", req.account_name, len(req.missing_files))
                update_tasks_by_account_and_files(cursor, req.account_name, "uploading", "not_found", req.missing_files)

            if req.status == "uploading":
                if account_role != "cleaner":
                    recovered_pending = refresh_uploader_heartbeat(cursor, req.account_name)
                    if recovered_pending > 0:
                        logger.warning(
                            "🩹 账号 [%s] 心跳恢复 %s 个已误回退的 pending 任务为 uploading。",
                            req.account_name,
                            recovered_pending,
                        )
                cursor.execute(
                    """
                    UPDATE worker_registry wr
                    JOIN (
                        SELECT DISTINCT worker_node
                        FROM file_tasks
                        WHERE account_used=%s AND status='uploading'
                    ) active_nodes ON wr.node_name = active_nodes.worker_node
                    SET wr.last_active = UTC_TIMESTAMP()
                    """,
                    (req.account_name,),
                )
                cursor.execute(
                    """
                    UPDATE file_tasks
                    SET updated_at = UTC_TIMESTAMP()
                    WHERE account_used=%s AND status='uploading'
                    """,
                    (req.account_name,),
                )
                return {"status": "success"}

            # 2. 处理最终的状态和账号冷却逻辑
            if req.status == "403_quota":
                # 403 配额耗尽：释放调度执行锁，并打上独立的配额封禁时间。
                cursor.execute(
                    """
                    UPDATE drive_accounts
                    SET dispatch_locked_until = NULL,
                        quota_blocked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s HOUR)
                    WHERE account_name=%s
                    """,
                    (QUOTA_BLOCK_HOURS, req.account_name),
                )
                # 将剩下的未传完的任务打回 pending 供其他账号接手
                revert_uploading_tasks(cursor, req.account_name, clear_account_used=True)
                trigger_dispatch_now = True
            elif req.status == "credential_invalid":
                cursor.execute(
                    """
                    UPDATE drive_accounts
                    SET is_active = 0,
                        dispatch_locked_until = NULL,
                        dispatch_cooldown_until = NULL,
                        cooldown_until = NULL
                    WHERE account_name=%s
                    """,
                    (req.account_name,),
                )
                if account_role != "cleaner":
                    revert_uploading_tasks(cursor, req.account_name, clear_account_used=True)
                    clear_pending_account_preference(cursor, req.account_name)
                logger.warning(
                    "⛔ 账号 [%s] 命中凭据失效错误，已永久停用，需人工 reconnect 后再手动恢复。",
                    req.account_name,
                )
                trigger_dispatch_now = True
            elif req.status == "completed":
                mark_all_uploading_tasks_success(cursor, req.account_name, success_target_status)
                release_account_lock(cursor, req.account_name)
            elif req.status == "clean_success":
                if account_role != "cleaner":
                    logger.warning("⚠️ 非清道夫账号 [%s] 上报 clean_success，已忽略批量状态变更。", req.account_name)
                elif not req.successful_files:
                    logger.info("ℹ️ 清道夫账号 [%s] 上报 clean_success 收尾包，未携带新的 successful_files。", req.account_name)
                release_account_lock(cursor, req.account_name)
            elif req.status == "failed":
                if account_role == "cleaner":
                    release_account_lock(cursor, req.account_name)
                else:
                    # 上传失败时，仅将仍处于 uploading 的任务打回 pending
                    revert_uploading_tasks(cursor, req.account_name, clear_account_used=False)
                    release_account_lock(cursor, req.account_name)
            elif req.status == "not_found":
                logger.warning("⚠️ 账号 [%s] 上报 not_found，相关任务将退回 pending 等待人工排查。", req.account_name)
                if req.missing_files:
                    cursor.execute(
                        "UPDATE file_tasks SET status='pending', account_used=NULL, updated_at=UTC_TIMESTAMP() "
                        "WHERE account_used=%s AND status='uploading'",
                        (req.account_name,),
                    )
                else:
                    cursor.execute(
                        "UPDATE file_tasks SET status='not_found', account_used=NULL, updated_at=UTC_TIMESTAMP() WHERE account_used=%s AND status='uploading'",
                        (req.account_name,),
                    )
                release_account_lock(cursor, req.account_name)
        if trigger_dispatch_now:
            trigger_dispatch_uploader_tasks_async(f"{req.status}:{req.account_name}")
        return {"status": "success"}
    finally:
        db.close()
