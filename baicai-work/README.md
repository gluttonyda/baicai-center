# baicai-work

This directory contains the Worker-side deployment bundle.

## Files

- `worker.py`: receives upload and cleaner tasks from Center
- `scanner.py`: scans local directories and reports files to Center
- `Dockerfile`: image build for Worker runtime
- `docker-compose.yml`: runs `worker-api` and `scanner`
- `.env.example`: Worker-side environment template
- `scanner-loop.sh`: periodic scanner runner inside container

## Quick Start

1. Copy the env template:
   `cp .env.example .env`
2. Adjust at least:
   - `API_TOKEN`
   - `WORKER_NODE_NAME`
   - `WORKER_API_URL`
   - `CENTER_URL`
   - `CENTER_API`
3. Ensure the host has a usable rclone config at `/root/.config/rclone/rclone.conf`
4. Start services:
   `docker compose up -d --build`

## Host Directories

Compose mounts these host paths from the current directory:

- `./links -> /data/links`
- `./rclone-cache -> /data/rclone-cache`
- `./logs/rclone -> /data/logs/rclone`
- `./runtime -> /opt/baicai-worker/runtime`

## Notes

- `worker-api` listens on port `8001`
- `scanner` runs in a loop controlled by `SCANNER_LOOP_SECONDS`
- If Center database was reset, remove the scanner cache before re-scanning:
  `rm -f runtime/scanner_cache.db`
