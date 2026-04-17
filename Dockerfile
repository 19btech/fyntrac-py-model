# ── Stage 1: Build dependencies ──────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Runtime ─────────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Create non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY app/ ./app/

# Set ownership
RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8090

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
