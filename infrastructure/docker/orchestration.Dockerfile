FROM python:3.12.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/pipelines/dagster/src:/app/pipelines/warehouse/src" \
    DAGSTER_HOME="/var/lib/titan/dagster"

COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages packages
COPY services services
COPY apps/api apps/api
COPY pipelines pipelines
COPY warehouse warehouse

RUN uv sync --frozen --no-dev --all-packages --group orchestration \
    && mkdir -p /var/lib/titan/dagster

WORKDIR /app
