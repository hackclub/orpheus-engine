import dlt
from dlt.destinations import postgres # Import the specific destination class

import os
from typing import Iterable, Dict, Any
import orpheus_engine.defs.loops.definitions as loops_defs
import polars as pl # <-- Add polars import

from dagster import AssetExecutionContext, asset, Output, MetadataValue
# Removed DagsterDltResource, dlt_assets imports as they are no longer used

# --- Helper Function for Destination ---
def warehouse_coolify_destination() -> postgres:
    """Creates and returns a configured postgres destination instance for DLT."""
    creds = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not creds:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set or empty.")
    # Initialize the destination with credentials
    # DLT handles the connection details internally
    return dlt.destinations.postgres(credentials=creds)

# --- DLT Asset: Loads Data into Warehouse using DLT ---
@asset(
    compute_kind="dlt", # Tagging the compute type for UI clarity
    group_name="dlt"    # Organizing asset in the UI
)
def loops_audience_in_warehouse(
    context: AssetExecutionContext,
    loops_contacts_export: pl.DataFrame # <-- Depend directly on the Polars DF asset
):
    """
    Loads Loops contacts data from the upstream asset 'loops_contacts_export'
    into the data warehouse (Postgres) using dlt.
    """
    pipeline_name = "loops_to_warehouse"
    dataset_name = "loops" # This will typically be the schema name in Postgres
    table_name = "audience" # This will be the table name within the schema

    context.log.info(f"Starting DLT pipeline '{pipeline_name}' to load data.")
    context.log.info(f"Destination: Postgres, Dataset (Schema): '{dataset_name}', Table: '{table_name}'")

    # Configure the dlt pipeline.
    # Destination details (credentials) are fetched by the helper function.
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=warehouse_coolify_destination(),
        dataset_name=dataset_name,
        progress="log" # Optional: display progress logs
    )

    # Run the pipeline, passing the data received from the upstream asset.
    # Convert Polars DataFrame to an iterator of dicts for DLT
    data_iterator = loops_contacts_export.iter_rows(named=True)
    try:
        load_info = pipeline.run(
            data=data_iterator, # <-- Use the iterator from the DataFrame
            table_name=table_name,
            write_disposition="replace", # Options: "append", "replace", "merge"
            primary_key="email" # Specify primary key for potential merging or indexing
        )

        context.log.info(f"DLT pipeline run finished successfully.")
        # Pretty-print the load info for detailed logs
        context.log.info(f"Load Info: {load_info}") # Use the __str__ representation for concise logging

        # Return Dagster metadata about the DLT load operation
        return Output(
            value=None, # This asset doesn't produce data for other Dagster assets
            metadata={
                "dlt_pipeline_name": MetadataValue.text(pipeline_name),
                "dlt_dataset_name": MetadataValue.text(dataset_name),
                "dlt_table_name": MetadataValue.text(table_name),
                "dlt_load_id": MetadataValue.text(load_id_str), # <-- Use extracted load_id
                "write_disposition": MetadataValue.text("replace"),
                "first_run": MetadataValue.bool(load_info.first_run)
            }
        )

    except Exception as e:
        context.log.error(f"DLT pipeline '{pipeline_name}' failed: {e}")
        # Re-raise the exception to mark the Dagster asset run as failed
        raise

# --- Removed old code ---
# The previous @dlt.source and @dlt_assets definitions have been removed.
