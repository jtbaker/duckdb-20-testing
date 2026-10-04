FROM ghcr.io/astral-sh/uv:0.5.24 AS uv

FROM python:3.12-slim

COPY --from=uv /uv /uvx /usr/local/bin/

ENV PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY poc.py quack_server.py quack_client.py load_energy_market.py load_append_energy.py load_parallel_append.py benchmark_client.py ./

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "quack_server.py"]
