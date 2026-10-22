"""deploy/hold 的离线契约测试：注入假 docker，证明安装助手只创建不启动、失败即停、不输出配置/环境。

真实 compose 求值与创建由 Hermes 在发布前执行，这里不调用任何真实 docker。
"""
import copy
import importlib.util
import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HOLD_DIR = ROOT / "deploy" / "hold"
ZERO_TIME = "0001-01-01T00:00:00Z"
SECRET = "SENTINEL-secret-value"
CENTER_IMAGE = "baicai-center:abc123def456-r1-arm64"
WORKER_IMAGE = "baicai-worker:abc123def456-r1-amd64"
NEW_ID = "sha256:" + "a" * 64
OLD_ID = "sha256:" + "b" * 64
OTHER_ID = "c" * 64
DAY = "MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY"
NIGHT = "MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT"
API_CONCURRENCY = {DAY: "3", NIGHT: "3"}
# 现场：Scanner 没跟着 .env 重建，实际值与 worker-api 不同（Gomami-2/US 的 Scanner 是 2/2）。
SCANNER_CONCURRENCY = {DAY: "2", NIGHT: "2"}


@pytest.fixture(scope="module")
def hold():
    spec = importlib.util.spec_from_file_location("hold_install", HOLD_DIR / "hold_install.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _container(cid, service, project, *, status="exited", started="2026-10-01T00:00:00Z",
               image=OLD_ID, restart="unless-stopped", env=None):
    return {
        "Id": cid,
        "Name": f"/{project}-{service}",
        "Image": image,
        "Config": {
            "Image": "old:tag",
            "Env": env if env is not None else [f"DB_PASSWORD={SECRET}"],
            "Labels": {"com.docker.compose.project": project, "com.docker.compose.service": service},
        },
        "State": {"Status": status, "Running": status == "running", "StartedAt": started},
        "HostConfig": {
            "RestartPolicy": {"Name": restart, "MaximumRetryCount": 0},
            "PortBindings": {"8001/tcp": [{"HostIp": "", "HostPort": "8001"}]},
        },
        "Mounts": [{"Type": "bind", "Source": "/data/links", "Destination": "/data/links", "RW": True}],
    }


def _worker_env():
    return {
        "TZ": "Asia/Shanghai",
        "NODE_TOKEN": SECRET,
        "CENTER_URL": "http://100.91.91.80:8080",
        "CENTER_API": "http://100.91.91.80:8080/api/report_files",
        "WORKER_API_URL": "http://100.125.75.111:8001/task/upload",
    }


def worker_config():
    return {
        "name": "baicai-work",
        "services": {
            "worker-api": {
                "image": WORKER_IMAGE,
                "restart": "no",
                "environment": {**_worker_env(), **API_CONCURRENCY},
            },
            "scanner": {"image": WORKER_IMAGE, "restart": "no", "environment": {**_worker_env(), **SCANNER_CONCURRENCY}},
        },
    }


def center_config():
    return {
        "name": "baicai-center",
        "services": {
            "baicai-api": {
                "image": CENTER_IMAGE,
                "restart": "no",
                "environment": {
                    "DB_PASSWORD": SECRET,
                    "UPLOADER_DISPATCH_ENABLED": "false",
                    "CLEANER_DISPATCH_ENABLED": "false",
                    "WORKER_API_URL_MAP": '{"Gomami-1":"http://100.125.75.111:8001/task/upload"}',
                    "WORKER_API_TAILNET_IPS": "100.125.75.111",
                },
            }
        },
    }


class FakeDocker:
    """按 argv 应答的 docker 替身；记录每次调用，`up` 时按 compose 求值结果重建目标容器（只创建）。"""

    def __init__(self, config, images, containers):
        self.config = config
        self.images = images
        self.containers = {c["Id"]: c for c in containers}
        self.calls = []
        self.config_rc = 0
        self.up_rc = 0
        self.after_up = None
        self._serial = 0

    @staticmethod
    def _compose_rest(args):
        index = 2
        while index < len(args) and args[index] in ("--project-directory", "-p", "-f"):
            index += 2
        return args[index:]

    def __call__(self, args, stdout=None):
        self.calls.append(list(args))
        done = lambda rc=0, out="", err="": subprocess.CompletedProcess(args, rc, out, err)
        if args[1] == "compose":
            rest = self._compose_rest(args)
            if rest[0] == "config":
                if self.config_rc:
                    return done(self.config_rc, "", f"error near {SECRET}")
                return done(0, json.dumps(self.config))
            if rest[0] == "up":
                stdout.write(b"Container created\n")
                if self.up_rc:
                    return done(self.up_rc)
                self._create(rest[rest.index("--no-deps") + 1:])
                if self.after_up:
                    self.after_up(self)
                return done(0)
        if args[1:3] == ["image", "inspect"]:
            image_id = self.images.get(args[-1])
            return done(0, image_id + "\n") if image_id else done(1, "", "No such image")
        if args[1] == "ps":
            return done(0, "".join(cid + "\n" for cid in self.containers))
        if args[1] == "inspect":
            return done(0, json.dumps([self.containers[cid] for cid in args[2:]]))
        if args[1] == "logs":
            stdout.write(f"old log line with {SECRET}\n".encode())
            return done(0)
        raise AssertionError(f"unexpected docker call: {args}")

    def _create(self, services):
        project = self.config["name"]
        for service in services:
            old = [c for c in self.containers.values()
                   if c["Config"]["Labels"].get("com.docker.compose.service") == service
                   and c["Config"]["Labels"].get("com.docker.compose.project") == project]
            spec = self.config["services"][service]
            self._serial += 1
            new = _container(
                f"{self._serial:064d}", service, project, status="created", started=ZERO_TIME,
                image=self.images[spec["image"]], restart=spec["restart"],
                env=[f"{key}={value}" for key, value in spec["environment"].items()],
            )
            if old:
                new["Mounts"] = copy.deepcopy(old[0]["Mounts"])
                new["HostConfig"]["PortBindings"] = copy.deepcopy(old[0]["HostConfig"]["PortBindings"])
            for container in old:
                del self.containers[container["Id"]]
            self.containers[new["Id"]] = new

    def verbs(self):
        return [self._compose_rest(c)[0] if c[1] == "compose" else c[1] for c in self.calls]


def worker_fake():
    return FakeDocker(
        worker_config(),
        {WORKER_IMAGE: NEW_ID},
        [
            _container("1" * 64, "worker-api", "baicai-work", env=_old_env(API_CONCURRENCY)),
            _container("2" * 64, "scanner", "baicai-work", env=_old_env(SCANNER_CONCURRENCY)),
            _container(OTHER_ID, "mysql", "unrelated", status="running"),
        ],
    )


def _old_env(concurrency):
    return [f"DB_PASSWORD={SECRET}", *(f"{key}={value}" for key, value in concurrency.items())]


def center_fake():
    return FakeDocker(
        center_config(),
        {CENTER_IMAGE: NEW_ID},
        [_container("3" * 64, "baicai-api", "baicai-center"), _container(OTHER_ID, "mysql", "unrelated", status="running")],
    )


def argv(evidence, role="worker", services=("worker-api", "scanner")):
    args = [
        "--project-dir", "/opt/baicai-worker",
        "-f", "docker-compose.yml", "-f", "/opt/release/hold.yml",
        "--role", role,
        "--expect-image-id", NEW_ID,
        "--evidence-dir", str(evidence),
    ]
    for service in services:
        args += ["--service", service]
    return args


def run_worker(hold, tmp_path, fake=None):
    fake = fake or worker_fake()
    evidence = tmp_path / "evidence"
    return hold.main(argv(evidence), run=fake), fake, evidence


FORBIDDEN_TOKENS = {
    "start", "restart", "run", "exec", "-d", "--detach", "pull", "build", "rm", "down", "stop", "kill",
    "cp", "--force-recreate", "--build", "--always-recreate-deps",
}


def test_worker_hold_creates_targets_without_starting(hold, tmp_path, capsys):
    rc, fake, evidence = run_worker(hold, tmp_path)

    assert rc == 0, capsys.readouterr()
    up_calls = [c for c in fake.calls if c[1] == "compose" and "up" in c]
    assert len(up_calls) == 1
    assert up_calls[0][-8:] == ["up", "--no-start", "--no-build", "--pull", "never", "--no-deps", "worker-api", "scanner"]
    assert set(fake.verbs()) <= {"config", "up", "image", "ps", "inspect", "logs"}
    for call in fake.calls:
        assert not FORBIDDEN_TOKENS & set(call[1:]), call
        assert not any("rclone" in arg or "MIGRATION" in arg for arg in call), call
    created = [c for c in fake.containers.values() if c["Config"]["Labels"]["com.docker.compose.project"] == "baicai-work"]
    assert {c["State"]["Status"] for c in created} == {"created"}
    assert "✅" in capsys.readouterr().out


def test_old_logs_are_saved_privately_before_recreate(hold, tmp_path):
    rc, fake, evidence = run_worker(hold, tmp_path)

    assert rc == 0
    verbs = fake.verbs()
    assert max(i for i, verb in enumerate(verbs) if verb == "logs") < verbs.index("up")
    assert stat.S_IMODE(os.stat(evidence).st_mode) == 0o700
    logs = sorted(evidence.glob("*.log"))
    assert {p.name for p in logs} >= {f"worker-api-{'1' * 12}.log", f"scanner-{'2' * 12}.log"}
    for path in evidence.iterdir():
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600, path


def test_config_and_env_values_never_printed_or_written_outside_logs(hold, tmp_path, capsys):
    rc, fake, evidence = run_worker(hold, tmp_path)

    assert rc == 0
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    for path in evidence.iterdir():
        if path.suffix != ".log":
            assert SECRET not in path.read_text(encoding="utf-8"), path


def test_center_hold_checks_dispatch_flags_in_created_container(hold, tmp_path, capsys):
    fake = center_fake()
    rc = hold.main(argv(tmp_path / "ev", role="center", services=("baicai-api",)), run=fake)

    assert rc == 0, capsys.readouterr()
    assert SECRET not in capsys.readouterr().out


def _mutate(config, service, change):
    change(config["services"][service])
    return config


@pytest.mark.parametrize(
    ("role", "service", "change"),
    [
        ("worker", "worker-api", lambda s: s.update(restart="unless-stopped")),
        ("worker", "scanner", lambda s: s.pop("restart")),
        ("worker", "scanner", lambda s: s.update(build={"context": "."})),
        ("worker", "worker-api", lambda s: s.update(image="baicai-worker:latest")),
        ("worker", "worker-api", lambda s: s.update(image="baicai-worker")),
        ("worker", "scanner", lambda s: s["environment"].update(CENTER_API="http://100.91.91.80:8080/api/report_status")),
        ("worker", "worker-api", lambda s: s["environment"].pop("WORKER_API_URL")),
        ("center", "baicai-api", lambda s: s["environment"].update(UPLOADER_DISPATCH_ENABLED="true")),
        ("center", "baicai-api", lambda s: s["environment"].pop("CLEANER_DISPATCH_ENABLED")),
        ("center", "baicai-api", lambda s: s["environment"].update(WORKER_API_TAILNET_IPS="")),
    ],
    ids=[
        "restart-unless-stopped", "restart-missing", "build-kept", "latest-tag", "no-tag",
        "center-api-wrong", "worker-url-missing", "uploader-on", "cleaner-flag-missing", "tailnet-ips-empty",
    ],
)
def test_preflight_rejects_non_hold_config_before_any_change(hold, tmp_path, capsys, role, service, change):
    fake = worker_fake() if role == "worker" else center_fake()
    _mutate(fake.config, service, change)
    services = ("worker-api", "scanner") if role == "worker" else ("baicai-api",)
    evidence = tmp_path / "evidence"

    rc = hold.main(argv(evidence, role=role, services=services), run=fake)

    assert rc != 0
    assert "up" not in fake.verbs() and "logs" not in fake.verbs()
    assert not evidence.exists()
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err


def test_preflight_rejects_missing_service(hold, tmp_path):
    fake = worker_fake()
    del fake.config["services"]["scanner"]

    assert hold.main(argv(tmp_path / "ev"), run=fake) != 0
    assert "up" not in fake.verbs()


@pytest.mark.parametrize("images", [{}, {WORKER_IMAGE: OLD_ID}], ids=["image-missing", "image-id-mismatch"])
def test_preflight_rejects_missing_or_unexpected_image(hold, tmp_path, images):
    fake = worker_fake()
    fake.images = images

    assert hold.main(argv(tmp_path / "ev"), run=fake) != 0
    assert "up" not in fake.verbs() and "logs" not in fake.verbs()


@pytest.mark.parametrize("status", ["running", "restarting", "paused"])
def test_preflight_requires_old_targets_stopped(hold, tmp_path, status):
    fake = worker_fake()
    fake.containers["1" * 64]["State"].update(Status=status, Running=status != "paused")

    assert hold.main(argv(tmp_path / "ev"), run=fake) != 0
    assert "up" not in fake.verbs() and "logs" not in fake.verbs()


def _container_of(fake, service):
    return next(c for c in fake.containers.values() if c["Config"]["Labels"]["com.docker.compose.service"] == service)


def _drop_old_scanner(fake):
    del fake.containers["2" * 64]


def _duplicate_old_scanner(fake):
    extra = copy.deepcopy(fake.containers["2" * 64])
    extra["Id"] = "d" * 64
    fake.containers[extra["Id"]] = extra


def _drop_old_center(fake):
    del fake.containers["3" * 64]


@pytest.mark.parametrize(
    ("role", "service", "change"),
    [
        ("worker", "scanner", _drop_old_scanner),
        ("worker", "scanner", _duplicate_old_scanner),
        ("center", "baicai-api", _drop_old_center),
    ],
    ids=["worker-old-missing", "worker-old-duplicate", "center-old-missing"],
)
def test_preflight_requires_exactly_one_old_container_per_service(hold, tmp_path, capsys, role, service, change):
    # 缺旧容器 = 首次创建，不属于维护升级；多个旧容器 = 目标不明确。两者都必须在留证与 up 之前拒绝。
    fake = worker_fake() if role == "worker" else center_fake()
    change(fake)
    services = ("worker-api", "scanner") if role == "worker" else ("baicai-api",)
    evidence = tmp_path / "evidence"

    rc = hold.main(argv(evidence, role=role, services=services), run=fake)

    assert rc != 0
    assert "up" not in fake.verbs() and "logs" not in fake.verbs()
    assert not evidence.exists()
    err = capsys.readouterr().err
    assert f"{service}: 旧容器" in err
    assert SECRET not in err


def _edit_env(container, changes):
    """改容器 Env（KEY=VALUE 列表）；值为 None 表示删掉该键。"""
    env = dict(item.partition("=")[::2] for item in container["Config"]["Env"])
    for key, value in changes.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    container["Config"]["Env"] = [f"{key}={value}" for key, value in env.items()]


@pytest.mark.parametrize(
    ("service", "compose_changes", "old_changes"),
    [
        ("worker-api", {DAY: "2", NIGHT: "2"}, {}),
        ("scanner", {DAY: "99", NIGHT: "99"}, {}),
        # 现场 Gomami-2：模板不管 Scanner 时 .env 会把旧 2/2 抬成 3/3；US 会抬成 4/4。
        ("scanner", {DAY: "3", NIGHT: "3"}, {}),
        ("scanner", {DAY: "4", NIGHT: "4"}, {}),
        # 把 API 的值套给 Scanner 同样是漂移。
        ("scanner", dict(API_CONCURRENCY), {}),
        ("worker-api", {NIGHT: "SENTINEL-77"}, {}),
        # 最终配置缺键：会落到新镜像/代码默认值，不能当成“保持不变”。
        ("scanner", {NIGHT: None}, {}),
        # 旧容器缺键或为空：不猜它当时用的镜像/代码默认值。
        ("worker-api", {}, {DAY: None}),
        ("worker-api", {DAY: ""}, {DAY: ""}),
        ("scanner", {DAY: None, NIGHT: None}, {DAY: None, NIGHT: None}),
    ],
    ids=[
        "api-3-to-2", "scanner-2-to-99", "scanner-2-to-3", "scanner-2-to-4", "scanner-gets-api-values",
        "api-night-sentinel", "final-missing-key", "old-missing-key", "both-empty", "both-missing",
    ],
)
def test_preflight_requires_exact_concurrency_preservation(hold, tmp_path, capsys, service, compose_changes,
                                                           old_changes):
    fake = worker_fake()
    environment = fake.config["services"][service]["environment"]
    for key, value in compose_changes.items():
        if value is None:
            environment.pop(key)
        else:
            environment[key] = value
    _edit_env(_container_of(fake, service), old_changes)
    evidence = tmp_path / "evidence"

    rc = hold.main(argv(evidence), run=fake)

    assert rc != 0
    assert "up" not in fake.verbs() and "logs" not in fake.verbs()
    assert not evidence.exists()
    err = capsys.readouterr().err
    assert f"{service}: MAX_CONCURRENT_UPLOADS_PER_WORKER_" in err
    # 只报服务名与键名，不回显任何取值（SECRET 与 SENTINEL-77 都以 SENTINEL 开头）。
    assert "SENTINEL" not in err


@pytest.mark.parametrize(
    ("service", "changes"),
    [("worker-api", {DAY: "2"}), ("scanner", {NIGHT: "3"}), ("scanner", {DAY: None})],
    ids=["api-day-drift", "scanner-night-drift", "scanner-key-dropped"],
)
def test_acceptance_rejects_concurrency_drift_in_created_container(hold, tmp_path, capsys, service, changes):
    fake = worker_fake()
    fake.after_up = lambda f: _edit_env(_container_of(f, service), changes)

    rc = hold.main(argv(tmp_path / "ev"), run=fake)

    assert rc != 0
    err = capsys.readouterr().err
    assert f"{service}: 并发值与旧容器不一致" in err
    assert SECRET not in err
    assert fake.verbs().count("up") == 1


def test_compose_config_failure_stops_without_echoing_stderr(hold, tmp_path, capsys):
    fake = worker_fake()
    fake.config_rc = 1

    assert hold.main(argv(tmp_path / "ev"), run=fake) != 0
    assert fake.verbs() == ["config"]
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err


def test_refuses_non_empty_evidence_dir(hold, tmp_path):
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / "previous.log").write_text("keep me")
    fake = worker_fake()

    assert hold.main(argv(evidence), run=fake) != 0
    assert "up" not in fake.verbs() and "logs" not in fake.verbs()
    assert (evidence / "previous.log").read_text() == "keep me"


def test_compose_up_failure_is_reported_and_nothing_else_runs(hold, tmp_path):
    fake = worker_fake()
    fake.up_rc = 1

    assert hold.main(argv(tmp_path / "ev"), run=fake) != 0
    assert fake.verbs()[-1] == "up"


def _set_state(cid_service, **state):
    def change(fake):
        for container in fake.containers.values():
            if container["Config"]["Labels"]["com.docker.compose.service"] == cid_service:
                container["State"].update(state)
    return change


def _set_field(service, path, value):
    def change(fake):
        for container in fake.containers.values():
            if container["Config"]["Labels"]["com.docker.compose.service"] == service:
                target = container
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
    return change


@pytest.mark.parametrize(
    "after_up",
    [
        _set_state("scanner", Status="running", Running=True),
        _set_state("worker-api", StartedAt="2026-10-22T00:00:00Z"),
        _set_field("worker-api", ("HostConfig", "RestartPolicy"), {"Name": "unless-stopped"}),
        _set_field("scanner", ("Image",), OLD_ID),
        _set_field("worker-api", ("Mounts",), []),
        _set_field("scanner", ("HostConfig", "PortBindings"), {}),
        _set_state("mysql", StartedAt="2026-10-22T00:00:00Z"),
    ],
    ids=["started", "started-at", "restart-policy", "image", "mount-drift", "port-drift", "other-container-touched"],
)
def test_acceptance_fails_when_created_state_is_not_hold(hold, tmp_path, capsys, after_up):
    fake = worker_fake()
    fake.after_up = after_up

    rc = hold.main(argv(tmp_path / "ev"), run=fake)

    assert rc != 0
    assert "❌" in capsys.readouterr().err
    # 验收失败也只报告，不做任何补救动作（不 start、不重试 up）。
    assert fake.verbs().count("up") == 1


def test_acceptance_rejects_center_container_with_dispatch_enabled(hold, tmp_path, capsys):
    fake = center_fake()

    def flip(fake):
        for container in fake.containers.values():
            if container["Config"]["Labels"]["com.docker.compose.service"] == "baicai-api":
                container["Config"]["Env"] = [f"DB_PASSWORD={SECRET}", "UPLOADER_DISPATCH_ENABLED=true",
                                              "CLEANER_DISPATCH_ENABLED=false"]

    fake.after_up = flip

    assert hold.main(argv(tmp_path / "ev", role="center", services=("baicai-api",)), run=fake) != 0
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err


def test_installer_source_has_no_lifecycle_or_business_commands():
    # "build"/"restart" 会作为 compose 配置键出现（检查它们的取值），所以按"实际拼出的 docker 动词"断言。
    import re

    source = (HOLD_DIR / "hold_install.py").read_text(encoding="utf-8")
    docker_verbs = set(re.findall(r'\[opts\.docker, "([a-z-]+)"', source))
    compose_verbs = set(re.findall(r'compose \+ \["([a-z-]+)"', source))
    assert docker_verbs == {"compose", "ps", "inspect", "image", "logs"}
    assert compose_verbs == {"config", "up"}
    for token in ('"start"', '"run"', '"exec"', '"-d"', '"--detach"', '"stop"', '"rm"', '"down"', '"pull"',
                  "shell=True", "rclone", "MIGRATION", "mysql", "--force-recreate", "os.system", "Popen"):
        assert token not in source, token


# ----- override 模板（真实 compose 求值由 Hermes 发布前执行，这里只做静态契约） -----


def _service_block(text, service):
    start = text.index(f"\n  {service}:\n")
    following = [text.find(f"\n  {name}:\n", start + 1) for name in ("worker-api", "scanner", "baicai-api")]
    end = min([pos for pos in following if pos > start] or [len(text)])
    return text[start:end]


@pytest.mark.parametrize(
    ("template", "services", "image_var"),
    [
        ("center.hold.override.yml", ("baicai-api",), "BAICAI_HOLD_CENTER_IMAGE"),
        ("worker.hold.override.yml", ("worker-api", "scanner"), "BAICAI_HOLD_WORKER_IMAGE"),
    ],
)
def test_hold_templates_pin_image_drop_build_and_never_restart(template, services, image_var):
    text = (HOLD_DIR / template).read_text(encoding="utf-8")
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

    for service in services:
        block = _service_block(body, service)
        assert f"image: ${{{image_var}:?" in block
        assert "build: !reset null" in block
        assert 'restart: "no"' in block
    # 只覆盖镜像、业务 URL 与 hold 开关：挂载、端口、env_file、命令、网络保持现场配置。
    for key in ("ports:", "volumes:", "env_file:", "command:", "networks:", "entrypoint:", "latest"):
        assert key not in body, key


def test_center_template_freezes_dispatch_and_requires_exact_tailnet_config():
    body = (HOLD_DIR / "center.hold.override.yml").read_text(encoding="utf-8")

    assert 'UPLOADER_DISPATCH_ENABLED: "false"' in body
    assert 'CLEANER_DISPATCH_ENABLED: "false"' in body
    assert "WORKER_API_URL_MAP: ${BAICAI_HOLD_WORKER_API_URL_MAP:?" in body
    assert "WORKER_API_TAILNET_IPS: ${BAICAI_HOLD_WORKER_API_TAILNET_IPS:?" in body


def test_worker_template_uses_required_tailnet_urls_and_pins_concurrency_per_service():
    text = (HOLD_DIR / "worker.hold.override.yml").read_text(encoding="utf-8")
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

    assert "CENTER_URL: ${BAICAI_HOLD_CENTER_URL:?" in body
    assert "/api/report_files" in body and "CENTER_API: ${BAICAI_HOLD_CENTER_URL:?" in body
    assert "WORKER_API_URL: ${BAICAI_HOLD_WORKER_API_URL:?" in body
    api_block = _service_block(body, "worker-api")
    scanner_block = _service_block(body, "scanner")
    assert "MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY: ${BAICAI_HOLD_UPLOADS_DAY:?" in api_block
    assert "MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT: ${BAICAI_HOLD_UPLOADS_NIGHT:?" in api_block
    # Scanner 必须显式固定自己的旧值，不能落回 .env（现场 .env 已改，旧 Scanner 没跟着重建）。
    assert "MAX_CONCURRENT_UPLOADS_PER_WORKER_DAY: ${BAICAI_HOLD_SCANNER_UPLOADS_DAY:?" in scanner_block
    assert "MAX_CONCURRENT_UPLOADS_PER_WORKER_NIGHT: ${BAICAI_HOLD_SCANNER_UPLOADS_NIGHT:?" in scanner_block
    # 两个服务各用各的变量，API 的值不会被套给 Scanner。
    assert "SCANNER" not in api_block
    assert "BAICAI_HOLD_UPLOADS_" not in scanner_block
    for block in (api_block, scanner_block):
        assert "*baicai-hold-urls" in block
