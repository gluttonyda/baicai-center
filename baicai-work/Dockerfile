FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     TZ=Asia/Shanghai

# 官方 rclone 构建（不是 apt 的 1.60.1：那个版本不接受含 @ 的 remote 名，而现场账号名都带 @）。
# 版本与校验值固定在这里，CI 会断言实际版本与 "@" remote 的可用性，防止悄悄回退。
ARG RCLONE_VERSION=1.65.2
ARG RCLONE_SHA256_AMD64=2155ea2c225272a6f78b2aa4547bb587c40b007586e73b41b31c59edba64f8fe
ARG RCLONE_SHA256_ARM64=71938906831a2fbab00a0519cb8a1f6aaa31425d528df130e60ca371f0dd45ab
ARG RCLONE_SHA256_ARM=6be7a09e3e3bc55af0ee9f00ea17fafdd8a38541ef2de21b8e804729b41af298
ARG TARGETARCH

WORKDIR /app

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl unzip tzdata bash; \
    rm -rf /var/lib/apt/lists/*; \
    # BuildKit 会注入 TARGETARCH；老式 docker build 没有它，用 dpkg 兜底。
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
        amd64) rclone_arch="amd64"; expected_sha="$RCLONE_SHA256_AMD64" ;; \
        arm64|aarch64) rclone_arch="arm64"; expected_sha="$RCLONE_SHA256_ARM64" ;; \
        arm|armhf|armv7l) rclone_arch="arm"; expected_sha="$RCLONE_SHA256_ARM" ;; \
        *) echo "不支持的 CPU 架构: $arch" >&2; exit 1 ;; \
    esac; \
    package="rclone-v${RCLONE_VERSION}-linux-${rclone_arch}.zip"; \
    curl -fsSL -o /tmp/rclone.zip "https://downloads.rclone.org/v${RCLONE_VERSION}/${package}"; \
    echo "${expected_sha}  /tmp/rclone.zip" | sha256sum -c -; \
    unzip -q /tmp/rclone.zip -d /tmp/rclone; \
    install -m 0755 "/tmp/rclone/rclone-v${RCLONE_VERSION}-linux-${rclone_arch}/rclone" /usr/local/bin/rclone; \
    rm -rf /tmp/rclone /tmp/rclone.zip; \
    rclone version | head -1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py scanner.py scanner-loop.sh ./
RUN chmod +x /app/scanner-loop.sh

EXPOSE 8001

CMD ["python", "worker.py"]
