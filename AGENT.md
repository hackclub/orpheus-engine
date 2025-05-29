# AGENT.md

## Commands
- **Install**: `uv sync --extra dev` or `pip install -e ".[dev]"`
- **Start Dagster**: `uv run dagster dev` (opens http://localhost:3000)
- **Test all**: `uv run pytest orpheus_engine_tests`
- **Test single**: `uv run pytest orpheus_engine_tests/test_assets.py::test_function_name`
- **All commands**: Must be prefixed with `uv run` (e.g., `uv run dagster dev`)

## Code Style
- **Language**: Python with Dagster framework, using uv for dependencies
- **Imports**: `import dagster as dg`, `import polars as pl` (not pandas), standard library first
- **Naming**: snake_case for all variables, functions, assets; descriptive asset groups/keys
- **Types**: Use type hints with typing module; pydantic BaseModel for structured data
- **Assets**: Use `@asset` decorator with `group_name`, `key_prefix`, `compute_kind` parameters
- **Resources**: Inherit from `ConfigurableResource`; use `EnvVar()` for environment variables
- **Error handling**: Custom exceptions per service; comprehensive try/except with chaining (`from e`)
- **DataFrames**: Use Polars (`pl.DataFrame`) as primary data processing library (NOT pandas - AI often confuses these)
- **Environment**: Use `.env` file for local development; `DAGSTER_ENV=development` locally

## Project Structure
- Main code: `orpheus_engine/` (assets, resources, definitions)
- Tests: `orpheus_engine_tests/`
- Config: `pyproject.toml` (not setup.py)
