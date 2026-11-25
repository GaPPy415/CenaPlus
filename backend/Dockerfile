FROM python:3.12-alpine

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -S appuser -u 1001 -G appgroup

WORKDIR /app

COPY requirements-api.txt /app/

RUN pip install --no-cache-dir -r requirements-api.txt && \
    find /usr/local/lib/python3.12 -name "*.pyc" -delete && \
    find /usr/local/lib/python3.12 -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

COPY backend /app/backend
COPY shared /app/shared

# Change ownership to non-root user
RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "backend.api:app", "--host", "0.0.0.0", "--port", "8000"]

