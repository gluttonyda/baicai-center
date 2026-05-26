#!/usr/bin/env bash
set -eu

INTERVAL_SECONDS="${SCANNER_LOOP_SECONDS:-300}"

mkdir -p /opt/baicai-worker/runtime /data/links /data/rclone-cache /data/logs/rclone

while true; do
  echo "[scanner-loop] starting scan at $(date -Iseconds)"
  if ! python /app/scanner.py; then
    echo "[scanner-loop] scanner.py exited with error" >&2
  fi
  echo "[scanner-loop] sleeping ${INTERVAL_SECONDS}s"
  sleep "${INTERVAL_SECONDS}"
done
