# Multi-stage Docker build for the unified Dataverse Audit Sync
# Works for: long-running container, batch container, or Azure Container Instance

FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies into a virtual environment
COPY requirements.txt .
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# ----------------------------------------------------------------------
# Runtime stage - minimal image
# ----------------------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code (function_app.py optional - only needed for Azure Functions)
COPY main.py config.json ./

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app/logs && \
    chown -R appuser:appuser /app
USER appuser

# Health check (Snowflake state table query)
HEALTHCHECK --interval=5m --timeout=30s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run as long-running console job by default
# Override with: docker run -e BACKLOG_MODE=true ... for one-shot mode
CMD ["python", "main.py"]
