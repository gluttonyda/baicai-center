import os
import logging
import sqlite3
import time
import unicodedata

import httpx

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 核心控制区 =================
def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


def _get_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        logger.warning("⚠️ 环境变量 [%s] 不是合法整数，已回退为默认值 %s", name, default)
        return default


def _get_path_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value or default


CENTER_API = require_env("CENTER_API")
API_TOKEN = require_env("API_TOKEN")
WORKER_NODE_NAME = require_env("WORKER_NODE_NAME")
WORKER_API_URL = require_env("WORKER_API_URL")
CACHE_DB_PATH = os.getenv("CACHE_DB_PATH", "/opt/baicai-worker/scanner_cache.db")
SCANNER_CHUNK_SIZE = _get_int_env("SCANNER_CHUNK_SIZE", 1000)
LINKS_DELAY_HOURS = _get_int_env("LINKS_DELAY_HOURS", 48)
RCLONE_CACHE_DELAY_HOURS = _get_int_env("RCLONE_CACHE_DELAY_HOURS", 1)
RCLONE_CACHE_STABLE_SECONDS = _get_int_env("RCLONE_CACHE_STABLE_SECONDS", 900)


def build_scan_configs():
    return [
        {
            "dir": _get_path_env("LINKS_SCAN_DIR", "/data/links"),
            "delay_seconds": LINKS_DELAY_HOURS * 3600,
            "upload_method": "copy",
            "stable_seconds": 0,
        },
        {
            "dir": _get_path_env("RCLONE_CACHE_SCAN_DIR", "/data/rclone-cache"),
            "delay_seconds": RCLONE_CACHE_DELAY_HOURS * 3600,
            "upload_method": "move",
            "stable_seconds": RCLONE_CACHE_STABLE_SECONDS,
        },
    ]


SCAN_CONFIGS = build_scan_configs()

IGNORE_EXTENSIONS = ('.part', '.!qB', '.aria2', '.tmp', '.crdownload')

# ================= 数据库逻辑 =================
def init_cache_db():
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        conn.execute("PRAGMA busy_timeout = 5000")
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS reported_files 
                          (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL,
                           PRIMARY KEY (base_dir, file_path))''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS scan_stats 
                          (key TEXT PRIMARY KEY, value INTEGER)''')
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS file_observations
               (base_dir TEXT NOT NULL,
                file_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                mtime REAL NOT NULL,
                stable_since REAL NOT NULL,
                last_seen REAL NOT NULL,
                PRIMARY KEY (base_dir, file_path))'''
        )
        cursor.execute("INSERT OR IGNORE INTO scan_stats (key, value) VALUES ('run_count', 0)")
        cursor.execute("PRAGMA table_info(reported_files)")
        columns = [row[1] for row in cursor.fetchall()]
        if columns == ["file_path", "reported_at"]:
            cursor.execute("ALTER TABLE reported_files RENAME TO reported_files_legacy")
            cursor.execute(
                '''CREATE TABLE reported_files
                   (base_dir TEXT NOT NULL, file_path TEXT NOT NULL, reported_at REAL,
                    PRIMARY KEY (base_dir, file_path))'''
            )
            cursor.execute(
                """
                INSERT OR IGNORE INTO reported_files (base_dir, file_path, reported_at)
                SELECT '', file_path, reported_at FROM reported_files_legacy
                """
            )
            cursor.execute("DROP TABLE reported_files_legacy")
        conn.commit()
        return conn
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败，请检查目录权限: {e}")
        exit(1)


def ensure_file_is_stable(cursor, base_dir: str, rel_path: str, size_bytes: int, mtime: float, stable_seconds: int, current_time: float) -> bool:
    if stable_seconds <= 0:
        return True

    cursor.execute(
        """
        SELECT size_bytes, mtime, stable_since
        FROM file_observations
        WHERE base_dir = ? AND file_path = ?
        """,
        (base_dir, rel_path),
    )
    row = cursor.fetchone()

    if row is None:
        cursor.execute(
            """
            INSERT INTO file_observations (base_dir, file_path, size_bytes, mtime, stable_since, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (base_dir, rel_path, size_bytes, mtime, current_time, current_time),
        )
        return False

    if row[0] != size_bytes or row[1] != mtime:
        cursor.execute(
            """
            UPDATE file_observations
            SET size_bytes = ?, mtime = ?, stable_since = ?, last_seen = ?
            WHERE base_dir = ? AND file_path = ?
            """,
            (size_bytes, mtime, current_time, current_time, base_dir, rel_path),
        )
        return False

    cursor.execute(
        """
        UPDATE file_observations
        SET last_seen = ?
        WHERE base_dir = ? AND file_path = ?
        """,
        (current_time, base_dir, rel_path),
    )
    return (current_time - row[2]) >= stable_seconds

def flush_tasks(file_list, method, current_time, conn, cursor):
    if not file_list: return 0
    
    logger.info(f"📦 发现 {len(file_list)} 个需 [{method}] 的新文件，准备流式汇报...")
    
    reported_count = 0
    for i in range(0, len(file_list), SCANNER_CHUNK_SIZE):
        chunk = file_list[i:i + SCANNER_CHUNK_SIZE]
        payload = {
            "worker_node": WORKER_NODE_NAME, "worker_api_url": WORKER_API_URL,
            "upload_method": method, "files": [item["file_path"] for item in chunk]
        }
        try:
            res = httpx.post(CENTER_API, json=payload, headers={"Authorization": f"Bearer {API_TOKEN}"}, timeout=60.0)
            res.raise_for_status()
            
            cursor.executemany(
                "INSERT OR IGNORE INTO reported_files (base_dir, file_path, reported_at) VALUES (?, ?, ?)",
                [(item["base_dir"], item["file_path"], current_time) for item in chunk],
            )
            cursor.executemany(
                "DELETE FROM file_observations WHERE base_dir = ? AND file_path = ?",
                [(item["base_dir"], item["file_path"]) for item in chunk],
            )
            conn.commit() 
            reported_count += len(chunk)
            logger.info(f"✅ 已流式同步 {len(chunk)} 个文件...")
        except Exception as e:
            logger.error(f"❌ 批次流式上报失败 (跳过本地记录): {e}")
    
    # 清空列表释放内存
    file_list.clear()
    return reported_count

def scan_and_report():
    current_time = time.time()
    pending_tasks = {"copy": [], "move": []}
    
    conn = init_cache_db()
    try:
        cursor = conn.cursor()
        
        # --- 1. 强制更新运行计数并立即 Commit ---
        cursor.execute("UPDATE scan_stats SET value = value + 1 WHERE key = 'run_count'")
        conn.commit() 
        
        cursor.execute("SELECT value FROM scan_stats WHERE key = 'run_count'")
        run_count = cursor.fetchone()[0]
        logger.info(f"🤖 侦察兵任务开始 (第 {run_count} 次扫描)...")

        # --- 2. 流式扫描文件并即时上报 ---
        reported_count = 0
        for config in SCAN_CONFIGS:
            base_dir = config["dir"]
            method = config["upload_method"]
            threshold_time = current_time - config["delay_seconds"]
            stable_seconds = config.get("stable_seconds", 0)
            scan_metrics = {
                "scanned": 0,
                "ignored_extensions": 0,
                "too_new": 0,
                "waiting_stable": 0,
                "already_reported": 0,
                "errors": 0,
                "reported": 0,
            }
            
            if not os.path.exists(base_dir):
                logger.warning(f"⚠️ 扫描目录不存在，已跳过: {base_dir}")
                continue

            for root, _, files in os.walk(base_dir):
                for file in files:
                    scan_metrics["scanned"] += 1
                    if file.endswith(IGNORE_EXTENSIONS):
                        scan_metrics["ignored_extensions"] += 1
                        continue
                    full_path = os.path.join(root, file)
                    try:
                        stat = os.stat(full_path)
                        if stat.st_mtime >= threshold_time:
                            scan_metrics["too_new"] += 1
                            continue

                        rel_path = os.path.relpath(full_path, base_dir)
                        rel_path = rel_path.replace('\\', '/')
                        rel_path = unicodedata.normalize('NFC', rel_path)

                        cursor.execute(
                            "SELECT 1 FROM reported_files WHERE base_dir = ? AND file_path = ?",
                            (base_dir, rel_path),
                        )
                        if cursor.fetchone() is not None:
                            scan_metrics["already_reported"] += 1
                            continue

                        if not ensure_file_is_stable(
                            cursor,
                            base_dir,
                            rel_path,
                            stat.st_size,
                            stat.st_mtime,
                            stable_seconds,
                            current_time,
                        ):
                            scan_metrics["waiting_stable"] += 1
                            continue

                        pending_tasks[method].append({"base_dir": base_dir, "file_path": rel_path})

                        if len(pending_tasks[method]) >= 1000:
                            flushed = flush_tasks(pending_tasks[method], method, current_time, conn, cursor)
                            reported_count += flushed
                            scan_metrics["reported"] += flushed
                    except FileNotFoundError:
                        scan_metrics["errors"] += 1
                        logger.warning(f"⚠️ 扫描时文件消失，已跳过: {full_path}")
                    except PermissionError as e:
                        scan_metrics["errors"] += 1
                        logger.warning(f"⚠️ 无权限读取文件 [{full_path}]: {e}")
                    except OSError as e:
                        scan_metrics["errors"] += 1
                        logger.warning(f"⚠️ 读取文件元数据失败 [{full_path}]: {e}")
            
            flushed = flush_tasks(pending_tasks[method], method, current_time, conn, cursor)
            reported_count += flushed
            scan_metrics["reported"] += flushed
            # 即使本轮没有成功上报，也要持久化 file_observations，
            # 否则等待稳定的文件会在下次扫描时被当成首次看到。
            conn.commit()
            logger.info(
                "📊 扫描摘要 [%s]：总文件=%s, 忽略后缀=%s, 未到延迟=%s, 等待稳定=%s, 已缓存=%s, 实际上报=%s, 异常=%s",
                base_dir,
                scan_metrics["scanned"],
                scan_metrics["ignored_extensions"],
                scan_metrics["too_new"],
                scan_metrics["waiting_stable"],
                scan_metrics["already_reported"],
                scan_metrics["reported"],
                scan_metrics["errors"],
            )
        
        if reported_count == 0:
            logger.info("✨ 战区平稳，本地与大脑已同步。发送心跳...")
            payload = {
                "worker_node": WORKER_NODE_NAME, "worker_api_url": WORKER_API_URL,
                "upload_method": "copy", "files": []
            }
            try:
                res = httpx.post(CENTER_API, json=payload, headers={"Authorization": f"Bearer {API_TOKEN}"}, timeout=10.0)
                res.raise_for_status()
                logger.info("💓 已向中心大脑发送心跳，保持节点活跃状态。")
            except Exception as e:
                logger.error(f"❌ 心跳发送失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    scan_and_report()
