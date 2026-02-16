# Stage 1: dependencies
FROM python:3.12-slim AS deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: runtime
FROM python:3.12-slim
WORKDIR /app

# Copy installed packages from deps stage
COPY --from=deps /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Create data directory (will be overridden by Railway volume mount)
RUN mkdir -p /app/data

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

# Non-root user
RUN useradd -r -s /bin/false botuser && chown -R botuser:botuser /app
USER botuser

# Health check: verify Telegram API is reachable
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('https://api.telegram.org', timeout=5)" || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
