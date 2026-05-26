#!/bin/bash
set -e

DIR="${DIR:-/opt/baicai-center/baicai-release}"

# 1. 替换 API_TOKEN
find "$DIR" -type f -exec sed -i 's/YOUR_REAL_API_TOKEN/<REDACTED_API_TOKEN>/g' {} +

# 2. 替换 Center 地址
find "$DIR" -type f -exec sed -i 's/center\.example\.com:8080/<REDACTED_CENTER_HOST>:<CENTER_PORT>/g' {} +
find "$DIR" -type f -exec sed -i 's/center\.example\.com/<REDACTED_CENTER_HOST>/g' {} +

# 3. 替换 Worker 地址
find "$DIR" -type f -exec sed -i 's/worker-node-1\.example\.com:8001/<REDACTED_WORKER_HOST>:<WORKER_PORT>/g' {} +
find "$DIR" -type f -exec sed -i 's/worker-node-1\.example\.com/<REDACTED_WORKER_HOST>/g' {} +

# 4. 替换 MySQL 密码和 Host
find "$DIR" -type f -exec sed -i 's/YOUR_REAL_DB_PASSWORD/<REDACTED_DB_PASSWORD>/g' {} +
find "$DIR" -type f -exec sed -i 's/YOUR_REAL_DB_HOST/<REDACTED_DB_HOST>/g' {} +

# 5. 替换 Worker 节点名
find "$DIR" -type f -exec sed -i 's/worker-node-1/<REDACTED_NODE_NAME>/g' {} +

echo "脱敏完成！"
