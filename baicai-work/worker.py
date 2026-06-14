import os
import logging
import re
import selectors
import subprocess
import tempfile
import threading
import time
from typing import List, Literal, Optional

import httpx
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 核心配置区 (根据新节点修改) =================
def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少必需环境变量: {name}")
    return value


API_TOKEN = require_env("API_TOKEN")
CENTER_URL = require_env("CENTER_URL").rstrip("/")
TARGET_REMOTE_DIR = require_env("TARGET_REMOTE_DIR")
ALLOWED_BASE_DIRS = [path.strip() for path in os.getenv("ALLOWED_BASE_DIRS", "/data/links,/data/rclone-cache").split(",") if path.strip()]
CLEANER_REPORT_BATCH_SIZE = max(1, int(os.getenv("CLEANER_REPORT_BATCH_SIZE", "1000")))
RCLONE_BWLIMIT = os.getenv("RCLONE_BWLIMIT", "").strip()
MAX_CONCURRENT_UPLOADS_PER_WORKER = max(1, int(os.getenv("MAX_CONCURRENT_UPLOADS_PER_WORKER", "2")))
MAX_CONCURRENT_CLEANERS_PER_WORKER = max(1, int(os.getenv("MAX_CONCURRENT_CLEANERS_PER_WORKER", "1")))

app = FastAPI(title="Baicai Worker Node")
security = HTTPBearer()
active_upload_accounts: set[str] = set()
active_upload_accounts_lock = threading.Lock()
active_cleaner_accounts: set[str] = set()
active_cleaner_accounts_lock = threading.Lock()

class TaskPayload(BaseModel):
    task_type: Literal["upload", "clean"] = "upload"
    upload_method: Literal["copy", "move"] = "copy"
    account_name: str = Field(min_length=1, max_length=128)
    dest_name: Optional[str] = Field(default="baicai-big", max_length=128)
    files: List[str] = Field(default_factory=list)

    @field_validator("account_name")
    @classmethod
    def validate_account_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("account_name 不能为空")
        return value

    @field_validator("dest_name")
    @classmethod
    def validate_dest_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("dest_name 不能为空字符串")
        return value

    @field_validator("files")
    @classmethod
    def validate_files(cls, value: List[str]) -> List[str]:
        normalized_files = []
        for file_path in value:
            clean_path = file_path.strip()
            if not clean_path:
                raise ValueError("files 中不能存在空路径")
            if "\x00" in clean_path or len(clean_path) > 1024:
                raise ValueError("files 中存在非法路径")
            normalized_files.append(clean_path)
        return normalized_files


class ReportStatusError(RuntimeError):
    pass


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
def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.credentials

def report_status(
    account: str,
    status: str,
    successful_files: Optional[List[str]] = None,
    missing_files: Optional[List[str]] = None,
):
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    max_retries = 3
    last_error: Optional[Exception] = None
    for i in range(max_retries):
        try:
            response = httpx.post(
                f"{CENTER_URL}/api/report_status",
                json={
                    "account_name": account,
                    "status": status,
                    "successful_files": successful_files or [],
                    "missing_files": missing_files or [],
                },
                headers=headers,
                timeout=10.0,
            )
            response.raise_for_status()
            logger.info(f"📡 战况汇报完毕: {account} -> {status}")
            return
        except Exception as e:
            last_error = e
            logger.error(f"📡 无法连接中心大脑进行战况汇报 (重试 {i+1}/{max_retries}): {e}")
            if i < max_retries - 1:
                time.sleep(3)
    raise ReportStatusError(f"战况汇报失败: {account} -> {status}") from last_error


def flush_cleaner_success_batch(account_name: str, status: str, batch: list[str]) -> None:
    if batch:
        report_status(account_name, status, list(batch))
        batch.clear()
    else:
        report_status(account_name, status)


def is_direct_upload_account(account_name: str) -> bool:
    return account_name.startswith("baicai_big")


def detect_remote_existing_files(account_name: str, file_paths: List[str]) -> set[str]:
    if not file_paths:
        return set()

    fd, temp_path = tempfile.mkstemp(prefix="bc_remote_probe_", suffix=".txt")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for file_path in file_paths:
                handle.write(f"{file_path}\n")

        result = subprocess.run(
            [
                "rclone",
                "lsf",
                f"{account_name}:{TARGET_REMOTE_DIR}",
                "--files-from",
                temp_path,
                "--files-only",
                "--recursive",
                "--format",
                "p",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            logger.warning(
                "⚠️ [%s] 远端存在性补偿检查失败（退出码=%s），将按本地缺失处理。\n%s",
                account_name,
                result.returncode,
                (result.stderr or result.stdout).strip(),
            )
            return set()

        return {
            line.strip().replace("\\", "/")
            for line in result.stdout.splitlines()
            if line.strip()
        }
    except FileNotFoundError:
        logger.error("❌ 未找到 rclone 命令，无法执行远端存在性补偿检查。")
        return set()
    except Exception as e:
        logger.warning("⚠️ [%s] 远端存在性补偿检查异常，将按本地缺失处理: %s", account_name, e)
        return set()
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def resolve_task_path(file_path: str) -> Optional[tuple[str, str]]:
    normalized_path = os.path.normpath(file_path.lstrip("/"))
    if normalized_path in ("", ".") or normalized_path.startswith("../") or normalized_path == "..":
        logger.warning("⚠️ 拒绝非法任务路径: %s", file_path)
        return None

    for base_dir in ALLOWED_BASE_DIRS:
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
def run_uploader(account_name: str, files: List[str], method: str):
    logger.info(f"🚀 [实时 {method.upper()} 模式] 账号 {account_name} 启动。收到 {len(files)} 个文件任务，详细日志已重定向。")
    
    tasks_by_dir = {base_dir: [] for base_dir in ALLOWED_BASE_DIRS}
    missing_files = []
    is_direct_upload = is_direct_upload_account(account_name)
    for file_path in files:
        resolved = resolve_task_path(file_path)
        if resolved is None:
            missing_files.append(file_path)
            continue
        base_dir, clean_path = resolved
        tasks_by_dir[base_dir].append(clean_path)

    # ===== 新增关键调试日志 =====
    total_found = sum(len(flist) for flist in tasks_by_dir.values())
    logger.info(f"🔍 检查本地文件：总共 {len(files)} 个任务中，在本地容器内成功找到 {total_found} 个文件。")
    if missing_files:
        logger.warning("⚠️ 账号 [%s] 有 %s 个任务文件不存在或越界，稍后会单独回执 not_found。", account_name, len(missing_files))
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
        recovered_remote_files = detect_remote_existing_files(account_name, missing_files)
        if recovered_remote_files:
            for file_path in recovered_remote_files:
                mark_success(file_path)
            missing_files = [fp for fp in missing_files if fp not in recovered_remote_files]
            logger.warning(
                "🩹 账号 [%s] 有 %s 个本地缺失文件已在远端存在，按已成功搬运恢复记录。",
                account_name,
                len(recovered_remote_files),
            )

    if total_found == 0:
        if successful_files and not missing_files:
            logger.warning("🩹 账号 [%s] 本地文件已不存在，但远端已命中全部文件，按完成处理。", account_name)
            report_status(account_name, "completed", successful_files, [])
        else:
            logger.warning("⚠️ 警告：Center 发来的所有文件在本地均不存在！任务将回退等待人工排查。")
            report_status(account_name, "not_found", missing_files=missing_files or files)
        return

    for base_dir, dir_files in tasks_by_dir.items():
        if not dir_files:
            continue
        
        fd, temp_path = tempfile.mkstemp(prefix="bc_up_", suffix=".txt")
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            for fp in dir_files: f.write(f"{fp}\n")
        
        cmd = [
            "rclone", method, base_dir, f"{account_name}:{TARGET_REMOTE_DIR}",
            "--files-from", temp_path, 
            "--transfers", "10",            # 🔥 满足 10 并发
            "--checkers", "16",
            "--stats", "15s",               # 15秒刷一次简报
            "--stats-one-line",             # 强制单行，保持观感清爽
            "--log-level", "INFO",
            "--drive-chunk-size", "128M",
            "--buffer-size", "128M",
            "--drive-stop-on-upload-limit",
            "--size-only"
        ]
        if RCLONE_BWLIMIT:
            cmd.extend(["--bwlimit", RCLONE_BWLIMIT])
        if is_direct_upload:
            cmd.append("--ignore-existing")
        
        try:
            # 取消双写日志，直接将进程输出重定向到控制台并打印全部信息。
            # 这里用 selector 轮询 stdout，避免 rclone 长时间不再输出时 watchdog 失效。
            last_report_time = time.time()
            
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

                        if current_time - last_report_time > 300:
                            logger.info(f"💓 [{account_name}] 任务执行中... 汇报活跃状态防失联。")
                            report_status(account_name, "uploading", successful_files)
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

                            if "Copied (new)" in line or "Skipped" in line or "Deleted" in line:
                                match = re.search(r'INFO\s+:\s+(.+?):\s+(Copied \(new\)|Skipped|Deleted)', line)
                                if match:
                                    file_path = match.group(1)
                                    action = match.group(2)
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
                        if final_status in ("403_quota", "credential_invalid"):
                            break
                finally:
                    selector.close()

                process.wait()
                if final_status not in ("403_quota", "credential_invalid"):
                    if process.returncode == 8:
                        logger.warning("⛔ [%s] rclone 因 --drive-stop-on-upload-limit 自动停止，立即切号并封号 24 小时。", account_name)
                        final_status = "403_quota"
                    elif process.returncode != 0:
                        logger.warning(
                            "⛔ [%s] rclone 以非零退出码 %s 结束当前上传任务，立即切号并封号 24 小时。",
                            account_name,
                            process.returncode,
                        )
                        final_status = "403_quota"
                if final_status in ("403_quota", "credential_invalid", "failed"):
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

    report_status(account_name, final_status, successful_files, missing_files)

def run_cleaner(account_name: str, dest_name: str, requested_files: List[str]):
    logger.info(
        f"🧹 清道夫启动: {account_name} -> {dest_name}，直接搬运整个中转盘；"
        f"大脑当前下发待确认文件数 {len(requested_files)}"
    )
    cleaned_files_seen: set[str] = set()
    pending_report_files: list[str] = []
    requested_report_files = []
    requested_files_seen: set[str] = set()
    source_missing = False
    for file_path in requested_files:
        normalized_path = file_path.strip().replace("\\", "/")
        if normalized_path and normalized_path not in requested_files_seen:
            requested_files_seen.add(normalized_path)
            requested_report_files.append(normalized_path)
    
    cmd = [
        "rclone", "move", f"{account_name}:{TARGET_REMOTE_DIR}", 
        f"{dest_name}:{TARGET_REMOTE_DIR}",
        "--drive-server-side-across-configs",
        "--drive-use-trash=false",
        "--delete-empty-src-dirs",
        "--size-only",
        "--transfers", "32", 
        "--tpslimit", "3",
        "--stats", "15s",
        "--log-level", "INFO"
    ]
    try:
        last_report_time = time.time()
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True) as p:
            for line in p.stdout:
                print(f"[CLEAN-{account_name}] {line.strip()}", flush=True)
                normalized_line = line.lower()
                if "directory not found" in normalized_line:
                    source_missing = True
                current_time = time.time()
                if current_time - last_report_time > 300:
                    logger.info(f"💓 [CLEAN-{account_name}] 清理执行中... 汇报活跃状态防失联。")
                    report_status(account_name, "uploading")
                    last_report_time = current_time
                if "Skipped" not in line and ("Copied" in line or "Moved" in line or "Deleted" in line):
                    match = re.search(r'INFO\s+:\s+(.+?):\s+((?:Copied|Moved)(?: \([^)]*\))?|Deleted)', line)
                    if match:
                        cleaned_path = match.group(1)
                        if cleaned_path not in cleaned_files_seen:
                            cleaned_files_seen.add(cleaned_path)
                            pending_report_files.append(cleaned_path)
                            if len(pending_report_files) >= CLEANER_REPORT_BATCH_SIZE:
                                flush_cleaner_success_batch(account_name, "uploading", pending_report_files)
            p.wait()
            if p.returncode == 0:
                if pending_report_files:
                    flush_cleaner_success_batch(account_name, "clean_success", pending_report_files)
                elif requested_report_files:
                    logger.warning(
                        "🧹 [CLEAN-%s] rclone 成功但没有搬运明细，按大脑下发的 %s 个 staged 文件完成清理记录。",
                        account_name,
                        len(requested_report_files),
                    )
                    report_status(account_name, "clean_success", requested_report_files)
                else:
                    report_status(account_name, "clean_success")
            elif source_missing and requested_report_files:
                logger.warning(
                    "🧹 [CLEAN-%s] 源目录不存在，按大脑下发的 %s 个 staged 文件清理旧记录。",
                    account_name,
                    len(requested_report_files),
                )
                report_status(account_name, "clean_success", requested_report_files)
            else:
                flush_cleaner_success_batch(account_name, "failed", pending_report_files)
    except Exception as e:
        logger.error(f"❌ 清道夫崩溃: {e}")
        report_status(account_name, "failed")

def task_router(payload: TaskPayload):
    try:
        if payload.task_type == "clean":
            run_cleaner(payload.account_name, payload.dest_name, list(payload.files))
        else:
            run_uploader(payload.account_name, payload.files, payload.upload_method)
    except ReportStatusError as e:
        logger.error(f"❌ 状态回执失败: {e}")
    except Exception as e:
        logger.error(f"❌ 任务路由未捕获异常: {e}")
        try:
            report_status(payload.account_name, "failed")
        except ReportStatusError as report_error:
            logger.error(f"❌ 补偿失败回执也未送达: {report_error}")
    finally:
        if payload.task_type == "clean":
            with active_cleaner_accounts_lock:
                active_cleaner_accounts.discard(payload.account_name)
        else:
            with active_upload_accounts_lock:
                active_upload_accounts.discard(payload.account_name)


def start_detached_task(payload: TaskPayload) -> None:
    thread = threading.Thread(
        target=task_router,
        args=(payload,),
        daemon=True,
        name=f"baicai-{payload.task_type}-{payload.account_name}",
    )
    thread.start()

@app.post("/task/upload", dependencies=[Depends(verify_token)])
async def receive_task(payload: TaskPayload):
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
        with active_upload_accounts_lock:
            if payload.account_name in active_upload_accounts:
                logger.warning(
                    "⏸️ 账号 [%s] 已有上传任务在本节点运行，拒绝重复启动新的 rclone。",
                    payload.account_name,
                )
                raise HTTPException(status_code=409, detail="account already uploading on this worker")
            if len(active_upload_accounts) >= MAX_CONCURRENT_UPLOADS_PER_WORKER:
                logger.warning(
                    "⏸️ 本节点已有 %s 个上传 rclone 在运行，达到本地并发上限 %s，拒绝新上传任务。",
                    len(active_upload_accounts),
                    MAX_CONCURRENT_UPLOADS_PER_WORKER,
                )
                raise HTTPException(status_code=429, detail="worker upload concurrency limit reached")
            active_upload_accounts.add(payload.account_name)
    try:
        start_detached_task(payload)
    except Exception:
        if payload.task_type == "clean":
            with active_cleaner_accounts_lock:
                active_cleaner_accounts.discard(payload.account_name)
        else:
            with active_upload_accounts_lock:
                active_upload_accounts.discard(payload.account_name)
        raise
    return {"status": "Accepted", "mode": payload.upload_method}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
