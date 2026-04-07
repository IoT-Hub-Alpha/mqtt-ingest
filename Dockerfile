# Multi-stage build for mqtt-ingest service
# Stage 1: Builder - install all dependencies
FROM python:3.13-slim AS builder

WORKDIR /app

# Install build dependencies (git for git-based packages, build-essential for compilation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements files
COPY mqtt-ingest/requirements.txt mqtt-ingest/requirements-git.txt ./

# Install Python dependencies to virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements-git.txt && \
    pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime - minimal image with only runtime dependencies
FROM python:3.13-slim AS runtime

WORKDIR /app

# Install only runtime dependencies (curl for health checks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set environment to use venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Copy application code
COPY mqtt-ingest/app ./app

# Create non-root user for security
RUN useradd -m -u 1000 mqtt-ingest && chown -R mqtt-ingest:mqtt-ingest /app
USER mqtt-ingest

# Expose HTTP and metrics ports
EXPOSE 8000 9103

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/live || exit 1

# Run FastAPI application with uvicorn
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8038"]
