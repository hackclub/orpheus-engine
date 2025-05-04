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

# --- DBT Manifest Generation Step ---
# Create a minimal temporary profile for the build stage `dbt parse`
RUN mkdir -p ${DBT_PROFILES_DIR} && \
    echo "\
orpheus_engine_dbt:\n\
  target: prod\n\
  outputs:\n\
    prod:\n\
      type: postgres\n\
      host: placeholder-host\n\
      port: 5432\n\
      user: placeholder-user\n\
      pass: placeholder-pass\n\
      dbname: placeholder-db\n\
      schema: public\n\
      threads: 1\n\
" > ${DBT_PROFILES_DIR}/profiles.yml

# Run dbt parse to generate manifest.json
RUN echo "Running dbt parse..." && \
    cd orpheus_engine_dbt && \
    dbt parse --profiles-dir ${DBT_PROFILES_DIR} --profile orpheus_engine_dbt --target prod && \
    cd .. && \
    echo "dbt parse finished." && \
    # Clean up the temporary build profile directory
    rm -rf ${DBT_PROFILES_DIR}
# --- End DBT Manifest Generation Step ---

RUN mkdir -p ${DAGSTER_HOME}

COPY docker_deploy/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

EXPOSE 3000

CMD ["sh", "-c", "uv run dagster-daemon run & uv run dagster-webserver -h 0.0.0.0 -p 3000"]
