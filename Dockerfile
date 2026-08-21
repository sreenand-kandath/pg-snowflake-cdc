FROM python:3.11-slim

# libpq5 is the runtime PostgreSQL client library needed by psycopg2-binary
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Health check used by container orchestrators (Docker, Kubernetes, ACA, ECS, ...)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz')"

EXPOSE 8080

# Run as non-root
RUN useradd -r -u 10001 cdcuser
USER cdcuser

CMD ["python", "-m", "src.main"]
