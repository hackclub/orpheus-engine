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
def loops_audience(
    context: AssetExecutionContext,
    loops_processed_audience: pl.DataFrame # <-- Changed dependency
):
    """
    Loads Loops contacts data from the upstream asset 'loops_processed_audience'
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
    data_iterator = loops_processed_audience.iter_rows(named=True)
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
                "write_disposition": MetadataValue.text("replace"),
                "first_run": MetadataValue.bool(load_info.first_run)
            }
        )

    except Exception as e:
        context.log.error(f"DLT pipeline '{pipeline_name}' failed: {e}")
        # Re-raise the exception to mark the Dagster asset run as failed
        raise

@asset(
    compute_kind="dlt",
    group_name="dlt",
    description="Loads analytics_hack_clubbers data into the warehouse.analytics schema."
)
def analytics_hack_clubbers_warehouse(
    context: AssetExecutionContext,
    analytics_hack_clubbers: pl.DataFrame
):
    """
    Loads the processed analytics_hack_clubbers data into the warehouse.analytics schema
    using dlt.
    """
    pipeline_name = "analytics_to_warehouse"
    dataset_name = "analytics"  # This will be the schema name in Postgres
    table_name = "hack_clubbers"  # This will be the table name within the schema

    context.log.info(f"Starting DLT pipeline '{pipeline_name}' to load data.")
    context.log.info(f"Destination: Postgres, Dataset (Schema): '{dataset_name}', Table: '{table_name}'")

    # Configure the dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=warehouse_coolify_destination(),
        dataset_name=dataset_name,
        progress="log"
    )

    # Convert Polars DataFrame to an iterator of dicts for DLT
    data_iterator = analytics_hack_clubbers.iter_rows(named=True)
    try:
        load_info = pipeline.run(
            data=data_iterator,
            table_name=table_name,
            write_disposition="replace",
            primary_key="email"  # Assuming email is the primary key
        )

        context.log.info(f"DLT pipeline run finished successfully.")
        context.log.info(f"Load Info: {load_info}")

        return Output(
            value=None,
            metadata={
                "dlt_pipeline_name": MetadataValue.text(pipeline_name),
                "dlt_dataset_name": MetadataValue.text(dataset_name),
                "dlt_table_name": MetadataValue.text(table_name),
                "write_disposition": MetadataValue.text("replace"),
                "first_run": MetadataValue.bool(load_info.first_run),
                "num_records": MetadataValue.int(analytics_hack_clubbers.height)
            }
        )

    except Exception as e:
        context.log.error(f"DLT pipeline '{pipeline_name}' failed: {e}")
        raise
