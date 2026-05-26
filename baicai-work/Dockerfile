FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     TZ=Asia/Shanghai

WORKDIR /app

RUN apt-get update     && apt-get install -y --no-install-recommends rclone ca-certificates tzdata bash     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py scanner.py scanner-loop.sh ./
RUN chmod +x /app/scanner-loop.sh

EXPOSE 8001

CMD ["python", "worker.py"]
