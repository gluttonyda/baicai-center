#!/usr/bin/env python3
"""维护期 hold 安装助手：用固定本地镜像重建并“创建”指定服务的容器，从不启动。

流程（任一步失败即停止，后续步骤一律不做）：
  1. 预检：compose 求值后每个目标服务 restart=no、无 build、镜像为固定 tag 且本地镜像 ID 等于预期、
     hold 配置正确；每个目标服务恰好一个旧容器（没有就是首次创建，不属于维护升级）且已停止；
     Worker 的两个上传并发值在旧容器里显式存在，最终 compose 逐字相同。
  2. 留证：旧容器日志与少量非敏感 inspect 字段写入 evidence-dir（目录 700、文件 600，只留在本机）。
  3. 创建：docker compose ... up --no-start --no-build --pull never --no-deps <services>。
  4. 验收：目标容器为 created、StartedAt 为零值、RestartPolicy=no、镜像 ID 精确、挂载/端口未漂移、
     Worker 并发值与旧容器相同；其它容器没有任何变化。
compose config 与容器环境变量只在内存中检查，从不打印或落盘。
"""
import argparse
import json
import os
import subprocess
import sys

ZERO_TIME = "0001-01-01T00:00:00Z"
PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"
CENTER_FALSE_FLAGS = ("UPLOADER_DISPATCH_ENABLED", "CLEANER_DISPATCH_ENABLED")
CONCURRENCY_KEYS = ("MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY", "MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT")


class HoldError(Exception):
    pass


def _run(args, stdout=None):
    if stdout is None:
        return subprocess.run(args, capture_output=True, text=True, check=False)
    return subprocess.run(args, stdout=stdout, stderr=stdout, check=False)


def _check_hold_config(name, service, role):
    """返回问题列表；只描述哪一项不对，不带取值。"""
    if service is None:
        return [f"{name}: compose 中没有该服务"]
    problems = []
    if "build" in service:
        problems.append(f"{name}: 仍继承 build（override 需要 build: !reset null）")
    if service.get("restart") != "no":
        problems.append(f'{name}: restart 不是 "no"')
    image = service.get("image") or ""
    repo, _, tag = image.rpartition(":")
    if not repo or "/" in tag or tag in ("", "latest"):
        problems.append(f"{name}: image 必须是显式、非 latest 的固定 tag")
    env = service.get("environment") or {}
    if role == "center":
        problems += [f"{name}: {flag} 必须为 false" for flag in CENTER_FALSE_FLAGS if env.get(flag) != "false"]
        problems += [f"{name}: {key} 不能为空" for key in ("WORKER_API_URL_MAP", "WORKER_API_TAILNET_IPS")
                     if not (env.get(key) or "").strip()]
    else:
        center_url = env.get("CENTER_URL") or ""
        if not center_url or center_url.endswith("/") or not (env.get("WORKER_API_URL") or ""):
            problems.append(f"{name}: CENTER_URL/WORKER_API_URL 必须是非空的完整地址（CENTER_URL 不带结尾 /）")
        if env.get("CENTER_API") != center_url + "/api/report_files":
            problems.append(f"{name}: CENTER_API 必须是 CENTER_URL + /api/report_files")
    return problems


def _check_concurrency(name, old_env, final_env):
    """返回问题列表；只报服务名与键名，不带取值。"""
    problems = []
    for key in CONCURRENCY_KEYS:
        if not old_env.get(key):
            # 旧容器没有显式值时进程用的是旧镜像/代码默认值，这里不猜。
            problems.append(f"{name}: {key} 在旧容器里没有显式值，无法证明保留")
        elif final_env.get(key) != old_env[key]:
            problems.append(f"{name}: {key} 与旧容器实际值不一致（最终配置缺失或改了值）")
    return problems


def _env(container):
    return dict(item.partition("=")[::2] for item in container["Config"].get("Env") or [])


def _mounts(container):
    return sorted(
        (m.get("Type"), m.get("Source"), m.get("Destination"), bool(m.get("RW"))) for m in container.get("Mounts") or []
    )


def _summary(container):
    """inspect 里挑出的非敏感字段（不含 Env/Cmd/Labels 之外的配置）。"""
    return {
        "Id": container["Id"],
        "Name": container.get("Name"),
        "Image": container.get("Image"),
        "Service": container["Config"]["Labels"].get(SERVICE_LABEL),
        "Status": container["State"].get("Status"),
        "StartedAt": container["State"].get("StartedAt"),
        "RestartPolicy": container["HostConfig"].get("RestartPolicy"),
        "PortBindings": container["HostConfig"].get("PortBindings"),
        "Mounts": _mounts(container),
    }


def _write_private(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)


def install(opts, run=_run, out=print):
    compose = [opts.docker, "compose", "--project-directory", opts.project_dir]
    if opts.project_name:
        compose += ["-p", opts.project_name]
    for compose_file in opts.file:
        compose += ["-f", compose_file]

    def call(args, what):
        result = run(args)
        if result.returncode != 0:
            # 不转述 stderr：compose 的报错可能带配置片段。
            raise HoldError(f"{what} 失败（exit {result.returncode}）")
        return result.stdout

    def containers():
        ids = call([opts.docker, "ps", "-aq", "--no-trunc"], "列出容器").split()
        found = json.loads(call([opts.docker, "inspect", *ids], "inspect 容器")) if ids else []
        return {c["Id"]: c for c in found}

    config = json.loads(call(compose + ["config", "--format", "json"], "compose 求值（请手工用 config --quiet 排查）"))
    project, services = config.get("name"), config.get("services") or {}
    problems = [p for name in opts.service for p in _check_hold_config(name, services.get(name), opts.role)]
    if problems:
        raise HoldError("hold 配置不满足：" + "；".join(problems))
    for ref in sorted({services[name]["image"] for name in opts.service}):
        image_id = call([opts.docker, "image", "inspect", "--format", "{{.Id}}", ref], "本地镜像检查").strip()
        if image_id != opts.expect_image_id:
            raise HoldError("本地镜像 ID 与预期不一致（同名 tag 可能已被覆盖），拒绝创建")

    def is_target(container):
        labels = container["Config"].get("Labels") or {}
        return labels.get(PROJECT_LABEL) == project and labels.get(SERVICE_LABEL) in opts.service

    before = containers()
    old_targets = [c for c in before.values() if is_target(c)]
    old_by_service = {}
    for name in opts.service:
        found = [c for c in old_targets if c["Config"]["Labels"][SERVICE_LABEL] == name]
        if not found:
            raise HoldError(f"{name}: 旧容器不存在（首次创建不属于维护升级），拒绝创建")
        if len(found) > 1:
            raise HoldError(f"{name}: 旧容器不止一个（重建目标不明确），拒绝创建")
        old_by_service[name] = found[0]
    for container in old_targets:
        if container["State"].get("Running") or container["State"].get("Status") not in ("exited", "created"):
            raise HoldError(f"服务 {container['Config']['Labels'][SERVICE_LABEL]} 的旧容器未停止，拒绝重建")
    if opts.role == "worker":
        problems = [p for name in opts.service for p in _check_concurrency(
            name, _env(old_by_service[name]), services[name].get("environment") or {})]
        if problems:
            raise HoldError("并发保留不满足：" + "；".join(problems))

    os.makedirs(opts.evidence_dir, mode=0o700, exist_ok=True)
    if os.listdir(opts.evidence_dir):
        raise HoldError("evidence-dir 非空，拒绝覆盖已有留证")
    os.chmod(opts.evidence_dir, 0o700)
    for container in old_targets:
        name = f"{container['Config']['Labels'][SERVICE_LABEL]}-{container['Id'][:12]}.log"
        fd = os.open(os.path.join(opts.evidence_dir, name), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as handle:
            if run([opts.docker, "logs", "--timestamps", container["Id"]], stdout=handle).returncode != 0:
                raise HoldError("保存旧容器日志失败，未重建任何容器")
    others_before = {cid: _summary(c) for cid, c in before.items() if not is_target(c)}
    _write_private(os.path.join(opts.evidence_dir, "before.json"), json.dumps(
        {"targets": [_summary(c) for c in old_targets], "others": sorted(others_before)}, indent=2))

    fd = os.open(os.path.join(opts.evidence_dir, "compose-up.log"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        up = run(compose + ["up", "--no-start", "--no-build", "--pull", "never", "--no-deps", *opts.service],
                 stdout=handle)
    if up.returncode != 0:
        raise HoldError(f"compose up --no-start 失败（exit {up.returncode}），输出见 evidence-dir/compose-up.log")

    after = containers()
    problems = []
    for name in opts.service:
        created = [c for c in after.values() if is_target(c) and c["Config"]["Labels"][SERVICE_LABEL] == name]
        if len(created) != 1:
            problems.append(f"{name}: 目标容器数量不是 1")
            continue
        new = _summary(created[0])
        old = old_by_service[name]
        checks = {
            "状态不是 created": new["Status"] != "created",
            "StartedAt 不是零值": new["StartedAt"] != ZERO_TIME,
            "RestartPolicy 不是 no": (new["RestartPolicy"] or {}).get("Name") != "no",
            "镜像 ID 不符": new["Image"] != opts.expect_image_id,
            "挂载漂移": new["Mounts"] != _mounts(old),
            "端口漂移": new["PortBindings"] != old["HostConfig"].get("PortBindings"),
        }
        env = _env(created[0])
        if opts.role == "center":
            checks["派发开关不是 false"] = any(env.get(flag) != "false" for flag in CENTER_FALSE_FLAGS)
        else:
            old_env = _env(old)
            checks["并发值与旧容器不一致"] = any(env.get(key) != old_env[key] for key in CONCURRENCY_KEYS)
        problems += [f"{name}: {label}" for label, failed in checks.items() if failed]
        out(f"{'❌' if any(checks.values()) else '✅'} {name}: created / 未启动 / restart=no / 镜像、挂载、端口与 hold 配置核对")
    others_after = {cid: _summary(c) for cid, c in after.items() if not is_target(c)}
    if others_after != others_before:
        problems.append("非目标容器发生了变化")
    _write_private(os.path.join(opts.evidence_dir, "after.json"), json.dumps(
        {"targets": [_summary(c) for c in after.values() if is_target(c)], "problems": problems}, indent=2))
    if problems:
        raise HoldError("验收未通过：" + "；".join(problems))


def main(argv=None, run=_run):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--project-name")
    parser.add_argument("-f", "--file", action="append", required=True)
    parser.add_argument("--service", action="append", required=True)
    parser.add_argument("--role", choices=("center", "worker"), required=True)
    parser.add_argument("--expect-image-id", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--docker", default="docker")
    opts = parser.parse_args(argv)
    try:
        install(opts, run=run)
    except HoldError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1
    print("✅ hold 创建完成：所有目标容器保持 created，未启动任何容器。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
