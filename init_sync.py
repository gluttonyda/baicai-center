import logging
import os
import subprocess
import sys
import threading
import unicodedata

import httpx

# ================= 配置区 =================
def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


CENTER_API = require_env("CENTER_API")
API_TOKEN = require_env("API_TOKEN")
WORKER_NODE_NAME = require_env("WORKER_NODE_NAME")
WORKER_API_URL = require_env("WORKER_API_URL")
RESERVED_INIT_SYNC_NODE_NAME = "Init_Scanner"

# 这里必须使用一个能访问到大盘（your-target-dir）的可用 rclone 账号配置名
RCLONE_REMOTE_ACCOUNT = require_env("RCLONE_REMOTE_ACCOUNT")
TARGET_DIR = require_env("TARGET_DIR")
CHUNK_SIZE = int(os.getenv("INIT_SYNC_CHUNK_SIZE", "10000"))
# =========================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
FAILED_CHUNKS_DIR = os.getenv("FAILED_CHUNKS_DIR", "./failed_init_sync_chunks")


def validate_init_sync_worker_node_name() -> None:
    if WORKER_NODE_NAME != RESERVED_INIT_SYNC_NODE_NAME:
        raise RuntimeError(
            f"init_sync.py 的 WORKER_NODE_NAME 必须为 {RESERVED_INIT_SYNC_NODE_NAME}，"
            "不得使用任何真实 Worker 节点名，否则会覆盖 worker_registry 中的真实节点地址。"
        )


def persist_failed_chunk(chunk: list[str], batch_index: int) -> None:
    os.makedirs(FAILED_CHUNKS_DIR, exist_ok=True)
    failed_file = os.path.join(FAILED_CHUNKS_DIR, f"batch_{batch_index:05d}.txt")
    with open(failed_file, "w", encoding="utf-8") as handle:
        for item in chunk:
            handle.write(f"{item}\n")
    logger.error("❌ 已将失败批次 %s 写入本地文件: %s", batch_index, failed_file)

def submit_history_chunk(client: httpx.Client, chunk: list[str], batch_index: int) -> bool:
    payload = {
        "worker_node": WORKER_NODE_NAME,
        "worker_api_url": WORKER_API_URL,
        "files": chunk,
        "is_history_sync": True,
        "upload_method": "move",
    }

    try:
        response = client.post(
            CENTER_API,
            json=payload,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
            timeout=60.0,
        )
        response.raise_for_status()
        logger.info(f"✅ 成功提交批次 {batch_index} ({len(chunk)} 个文件)")
        return True
    except Exception as e:
        logger.error(f"❌ 提交批次 {batch_index} 失败: {e}")
        persist_failed_chunk(chunk, batch_index)
        return False


def consume_stderr(stderr_stream, chunks: list[str]) -> None:
    try:
        for line in stderr_stream:
            chunks.append(line)
    except Exception as exc:
        chunks.append(f"[stderr-consumer-error] {exc}")


def sync_remote_snapshot() -> tuple[bool, int, int, int]:
    """
    以真正流式的方式读取大盘文件列表，并分块同步到 Center。
    """
    validate_init_sync_worker_node_name()
    logger.info(f"🔄 正在从 Google Drive [{RCLONE_REMOTE_ACCOUNT}:{TARGET_DIR}] 全量拉取大盘快照，这可能需要几分钟...")

    cmd = [
        "rclone", "lsf", f"{RCLONE_REMOTE_ACCOUNT}:{TARGET_DIR}",
        "--files-only",
        "--recursive",
        "--tpslimit", "3",
        "--tpslimit-burst", "3",
        "--fast-list",
    ]

    total_files = 0
    success_batches = 0
    failed_batches = 0
    chunk: list[str] = []
    batch_index = 0
    stderr_chunks: list[str] = []

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding="utf-8",
        )

        if process.stdout is None or process.stderr is None:
            logger.error("❌ 无法读取 rclone 的标准输出或错误输出。")
            return False, total_files, success_batches, failed_batches

        stderr_thread = threading.Thread(
            target=consume_stderr,
            args=(process.stderr, stderr_chunks),
            daemon=True,
            name="init-sync-stderr-consumer",
        )
        stderr_thread.start()

        with httpx.Client() as client:
            for line in process.stdout:
                clean_path = line.rstrip("\r\n")
                if not clean_path:
                    continue

                clean_path = clean_path.replace("\\", "/")
                clean_path = unicodedata.normalize("NFC", clean_path)
                chunk.append(clean_path)
                total_files += 1

                if len(chunk) >= CHUNK_SIZE:
                    batch_index += 1
                    if submit_history_chunk(client, chunk, batch_index):
                        success_batches += 1
                    else:
                        failed_batches += 1
                    chunk = []

            process.wait()
            stderr_thread.join(timeout=1.0)
            if process.returncode != 0:
                error_output = "".join(stderr_chunks).strip()
                if not error_output:
                    error_output = f"exit code {process.returncode}"
                logger.error(f"❌ rclone 拉取失败: {error_output}")
                return False, total_files, success_batches, failed_batches

            if chunk:
                batch_index += 1
                if submit_history_chunk(client, chunk, batch_index):
                    success_batches += 1
                else:
                    failed_batches += 1

        return failed_batches == 0, total_files, success_batches, failed_batches
    except Exception as e:
        logger.error(f"❌ 拉取或同步过程发生异常: {e}")
        return False, total_files, success_batches, failed_batches

if __name__ == "__main__":
    success, total_files, success_batches, failed_batches = sync_remote_snapshot()
    logger.info(
        "📊 历史同步摘要：总文件=%s, 成功批次=%s, 失败批次=%s",
        total_files,
        success_batches,
        failed_batches,
    )
    if success:
        logger.info("🎉 大盘历史数据重建完毕！现在大脑数据库里已经有了防重复的完美屏障。")
        if total_files == 0:
            logger.warning("⚠️ 本次未发现远端文件，请确认目标目录是否为空。")
        sys.exit(0)

    logger.error("❌ 大盘历史数据重建未完成，请根据日志排查失败批次后重试。")
    sys.exit(1)
