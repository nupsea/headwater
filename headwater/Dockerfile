FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock ./
COPY headwater/ headwater/

# Install dependencies
RUN uv sync --frozen --no-dev

EXPOSE 8000

# Data is mounted via docker-compose volumes
CMD ["uv", "run", "uvicorn", "headwater.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
