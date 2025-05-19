import dlt
from dlt.destinations import postgres # Import the specific destination class

import os
from typing import Iterable, Dict, Any
import orpheus_engine.defs.loops.definitions as loops_defs
import polars as pl # <-- Add polars import

from dagster import AssetExecutionContext, asset, Output, MetadataValue, AssetIn
# Removed DagsterDltResource, dlt_assets imports as they are no longer used

from ..airtable.generated_ids import AirtableIDs

# --- Helper Function for Destination ---
def warehouse_coolify_destination() -> postgres:
    """Creates and returns a configured postgres destination instance for DLT."""
    creds = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not creds:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set or empty.")
    # Initialize the destination with credentials
    # DLT handles the connection details internally
    return dlt.destinations.postgres(credentials=creds)

def create_airtable_sync_assets(
    base_name: str,
    tables: list[str],
    description: str = None
):
    """
    Creates DLT assets for syncing multiple Airtable tables to the warehouse.
    
    Args:
        base_name: The name of the Airtable base (e.g., 'neighborhood')
        tables: List of table names to sync (e.g., ['neighbors', 'projects'])
        description: Optional custom description for the assets
    """
    assets = []
    
    for table_name in tables:
        asset_name = f"{base_name}_{table_name}_warehouse"
        group_name = f"dlt_airtable_{base_name}"
        
        @asset(
            name=asset_name,
            compute_kind="dlt",
            group_name=group_name,
            description=description or f"Loads {base_name}.{table_name} data into the warehouse.airtable_{base_name} schema.",
            ins={
                table_name: AssetIn(key_prefix=["airtable", base_name])
            }
        )
        def sync_asset(
            context: AssetExecutionContext,
            **kwargs
        ):
            pipeline_name = f"{base_name}_to_warehouse"
            dataset_name = f"airtable_{base_name}"
            table_name_warehouse = table_name

            # Get the input DataFrame
            df = kwargs[table_name]

            # Get field mappings from generated_ids
            base_class = getattr(AirtableIDs, base_name)
            table_class = getattr(base_class, table_name)
            field_mappings = {
                getattr(table_class, field_name): field_name
                for field_name in dir(table_class)
                if not field_name.startswith('_') and field_name != 'TABLE_ID'
            }

            # Filter mappings to only include columns that exist in the DataFrame
            existing_columns = set(df.columns)
            valid_mappings = {
                old_name: new_name 
                for old_name, new_name in field_mappings.items() 
                if old_name in existing_columns
            }

            context.log.info(f"Found {len(valid_mappings)} columns to rename out of {len(existing_columns)} total columns")
            context.log.info(f"Columns to rename: {valid_mappings}")

            # Rename columns using the filtered mappings
            renamed_df = df.rename(valid_mappings)

            context.log.info(f"Starting DLT pipeline '{pipeline_name}' to load data.")
            context.log.info(f"Destination: Postgres, Dataset (Schema): '{dataset_name}', Table: '{table_name_warehouse}'")
            context.log.info(f"Columns after renaming: {renamed_df.columns}")

            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination=warehouse_coolify_destination(),
                dataset_name=dataset_name,
                progress="log"
            )

            data_iterator = renamed_df.iter_rows(named=True)
            try:
                load_info = pipeline.run(
                    data=data_iterator,
                    table_name=table_name_warehouse,
                    write_disposition="replace",
                    primary_key="id"  # Always use Airtable record ID
                )

                context.log.info(f"DLT pipeline run finished successfully.")
                context.log.info(f"Load Info: {load_info}")

                return Output(
                    value=None,
                    metadata={
                        "dlt_pipeline_name": MetadataValue.text(pipeline_name),
                        "dlt_dataset_name": MetadataValue.text(dataset_name),
                        "dlt_table_name": MetadataValue.text(table_name_warehouse),
                        "write_disposition": MetadataValue.text("replace"),
                        "first_run": MetadataValue.bool(load_info.first_run),
                        "num_records": MetadataValue.int(renamed_df.height),
                        "columns": MetadataValue.text(", ".join(renamed_df.columns))
                    }
                )

            except Exception as e:
                context.log.error(f"DLT pipeline '{pipeline_name}' failed: {e}")
                raise

        # Set the function name to match the asset name
        sync_asset.__name__ = asset_name
        assets.append(sync_asset)
    
    return assets

neighborhood_assets = create_airtable_sync_assets(
    base_name="neighborhood",
    tables=["neighbors", "hackatime_projects"],
    description="Loads neighborhood.neighbors and neighborhood.hackatime_projects data into the warehouse.neighborhood schema."
)

shipwrecked_assets = create_airtable_sync_assets(
    base_name="shipwrecked",
    tables=["rsvps"],
    description="Loads shipwrecked.rsvps data into the warehouse.shipwrecked schema."
)

highway_assets = create_airtable_sync_assets(
    base_name="highway",
    tables=["rsvps"],
    description="Loads highway.rsvps data into the warehouse.highway schema."
)

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
