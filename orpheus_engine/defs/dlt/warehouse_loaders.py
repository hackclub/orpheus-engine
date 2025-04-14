import dlt
import polars as pl
import re
from dagster import (
    asset, AssetIn, Definitions, EnvVar, DagsterLogManager, AssetKey,
    AssetExecutionContext # Use AssetExecutionContext for dlt_assets
)
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator

# Asset Key for the upstream loops export asset
# Assuming it's defined in orpheus_engine.defs.loops.definitions
# Adjust the import path if necessary
try:
    # Attempt to import the specific asset to get its key
    from ..loops.definitions import loops_contacts_export
    LOOPS_EXPORT_ASSET_KEY = loops_contacts_export.key
    print(f"Successfully imported loops_contacts_export key: {LOOPS_EXPORT_ASSET_KEY}")
except (ImportError, AttributeError):
    # Fallback if the structure is different or asset not found easily
    LOOPS_EXPORT_ASSET_KEY = AssetKey(["loops_contacts_export"])
    print(f"Warning: Could not dynamically import loops_contacts_export key. Using fallback: {LOOPS_EXPORT_ASSET_KEY}")


# --- Helper Functions ---

def to_snake_case(name: str) -> str:
    """Converts lowerCamelCase or UpperCamelCase to snake_case."""
    if not name: # Handle empty string case
        return name
    # Insert underscore before capital letters preceded by a lowercase letter or number
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insert underscore before capital letters preceded by another capital letter but followed by a lowercase letter
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def rename_columns_to_snake_case(df: pl.DataFrame) -> pl.DataFrame:
    """Renames all columns in a Polars DataFrame to snake_case."""
    new_columns = {col: to_snake_case(col) for col in df.columns}
    return df.rename(new_columns)


# --- Dagster Assets ---

@asset(
    ins={"raw_loops_df": AssetIn(key=LOOPS_EXPORT_ASSET_KEY)},
    group_name="dlt_staging", # Group for preprocessing steps
    compute_kind="python"
)
def loops_audience_prepared(context, raw_loops_df: pl.DataFrame) -> pl.DataFrame:
    """
    Takes the raw Loops contacts export DataFrame and renames columns
    to snake_case for loading into the warehouse.
    """
    log: DagsterLogManager = context.log
    if raw_loops_df.is_empty():
        log.warning("Input DataFrame is empty. Skipping processing.")
        return raw_loops_df # dlt can handle empty DataFrames

    log.info(f"Received DataFrame with {raw_loops_df.height} rows and columns: {raw_loops_df.columns}")
    log.info("Renaming columns to snake_case...")
    loops_df = rename_columns_to_snake_case(raw_loops_df)
    log.info(f"Renamed columns: {loops_df.columns}")
    context.add_output_metadata({"columns_renamed": loops_df.columns})
    return loops_df

@asset(
    ins={"prepared_df": AssetIn(key=loops_audience_prepared.key)},
    group_name="dlt_load_warehouse",
    compute_kind="dlt",
)
def loops_audience_dlt_load(
    context: AssetExecutionContext,
    prepared_df: pl.DataFrame,
    dlt: DagsterDltResource
):
    """
    Loads the snake_cased Loops audience data into warehouse_coolify.loops.audience
    using dlt with replace disposition. Handles schema changes automatically.
    Requires the 'dlt' resource configured. dlt automatically picks up credentials
    from environment variables like:
    DESTINATION__POSTGRES__CREDENTIALS=postgresql://user:pass@host:port/db
    """
    log: DagsterLogManager = context.log

    if prepared_df.is_empty():
        log.warning("Input DataFrame is empty. Skipping dlt load.")
        return

    log.info(f"Initiating dlt pipeline load to Postgres schema 'loops', table 'audience'.")

    # Define the pipeline configuration
    pipeline = dlt.pipeline(
        pipeline_name="loops_audience_warehouse",
        destination="postgres",
        dataset_name="loops"
    )

    log.info(f"Running dlt load via dlt.run() into dataset 'loops', table 'audience' with write_disposition='replace'")

    # Use the dlt resource's run method. It handles execution and logging.
    yield from dlt.run(
        context=context,
        pipeline=pipeline,
        data=prepared_df,
        table_name="audience",
        write_disposition="replace"
    )
    # No explicit return or logging after yield from dlt.run is needed.
    # Remove the logging code below that accesses load_info

    # REMOVED the logging block that accessed load_info
    # REMOVED the explicit return DltOutput statement

    # Removed the previous try/except and manual logging/raising logic

    # Log details about the load
    if load_info.has_failed_jobs:
        log.error("dlt pipeline finished with failed jobs.")
        # Raise exception to fail the Dagster asset run
        failed_jobs = [job.job_file_info for job in load_info.load_packages[0].jobs["failed_jobs"]]
        raise Exception(f"dlt load job failed. Failed jobs: {failed_jobs}")
    else:
        log.info("dlt pipeline completed successfully.")
        # Example: Logging metrics from the first load package if it exists
        if load_info.load_packages:
             job_metrics = load_info.load_packages[0].metrics.get("job_metrics", [])
             if job_metrics:
                 metrics = job_metrics[0] # Take the first job's metrics
                 log.info(f"dlt Load Metrics: Total Records={metrics.get('total_records')}, Inserted={metrics.get('inserted_records')}")
             else:
                 log.info("No job metrics found in dlt load package.")
        else:
             log.info("No load packages found in dlt load info.")

    return DltOutput(load_info=load_info.asdict(), asset_key=context.asset_key) 