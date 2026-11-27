FROM python:3.12-slim

ARG SUPERCRONIC_VERSION=v0.2.33
ARG SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl tzdata && \
    curl -fsSL "$SUPERCRONIC_URL" -o /usr/local/bin/supercronic && \
    chmod +x /usr/local/bin/supercronic && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -m appuser

WORKDIR /app

COPY backend/data/requirements-data.txt /app/

RUN pip install --no-cache-dir -r requirements-data.txt && \
    find /usr/local/lib/python3.12 -name "*.pyc" -delete && \
    find /usr/local/lib/python3.12 -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

COPY backend/data/crontab /app/crontab
COPY backend /app/backend
COPY shared /app/shared

RUN mkdir -p /app/backend/data/logs && chown -R appuser:appgroup /app

USER appuser

ENV TZ=Europe/Skopje \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

CMD ["supercronic", "/app/crontab"]
