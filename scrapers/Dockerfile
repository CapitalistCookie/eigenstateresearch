FROM python:3.11-slim AS builder

WORKDIR /app
COPY scrapers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY processor/shared/ ./shared/
COPY scrapers/ ./
COPY scripts/ ./scripts/
ENV PYTHONPATH=/app
CMD ["python", "scheduler.py"]
