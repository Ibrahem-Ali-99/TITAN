FROM python:3.12.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages packages
COPY services services
COPY apps/api apps/api

RUN uv sync --frozen --no-dev --all-packages \
    && useradd --create-home --uid 1000 --shell /usr/sbin/nologin titan \
    && chown -R titan:titan /app

USER titan
WORKDIR /app
