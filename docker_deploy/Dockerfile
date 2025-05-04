FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/app \
    # Add DBT-related environment variables for the build stage
    DBT_PROFILES_DIR=/tmp/dbt_build_profile

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY orpheus_engine ./orpheus_engine
COPY orpheus_engine_dbt ./orpheus_engine_dbt

# Install Python dependencies FIRST
RUN uv sync
RUN uv pip install dagster dbt-core dbt-postgres PyYAML

RUN mkdir -p ${DAGSTER_HOME}

COPY docker_deploy/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

EXPOSE 3000

CMD ["sh", "-c", "uv run dagster-daemon run & uv run dagster-webserver -h 0.0.0.0 -p 3000"]
