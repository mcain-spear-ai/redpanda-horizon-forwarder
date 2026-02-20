# For a guaranteed fresh build and horizon-data-core 4.4.0 verification, use:
#   ./scripts/build-and-verify.sh
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_CACHE_DIR=/app/.cache/uv

# System dependencies for ctypes-loaded shared libraries
RUN apt-get update && apt-get install -y \
    libc6 \
    libstdc++6 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Create non-root user
RUN groupadd --gid 1001 appuser && useradd --uid 1001 --gid 1001 --no-create-home appuser

WORKDIR /app

# Copy dependency metadata first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync

# Copy application code
COPY . .

# Fix ownership so appuser can access the app and cache directories
RUN chown -R appuser:appuser /app

USER appuser

CMD ["uv", "run", "main.py"]
