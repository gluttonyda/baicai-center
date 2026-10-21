FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 运行时使用固定 uid/gid 的非特权用户；依赖仍以 root 安装到系统 site-packages。
RUN groupadd --gid 10001 baicai \
    && useradd --uid 10001 --gid baicai --no-create-home --home-dir /app \
        --shell /usr/sbin/nologin baicai \
    && chown baicai:baicai /app

# 只安装运行时依赖，pytest 等开发依赖在 requirements-dev.txt，不进入生产镜像。
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=baicai:baicai main.py init_sync.py ./

USER baicai

EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
