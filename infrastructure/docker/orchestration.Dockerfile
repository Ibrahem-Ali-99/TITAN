FROM python@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 # 3.12.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/pipelines/dagster/src:/app/pipelines/warehouse/src" \
    DAGSTER_HOME="/var/lib/titan/dagster"

COPY --from=ghcr.io/astral-sh/uv@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded /uv /uvx /bin/ # 0.12.1

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
