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
    description: str = None,
    source_suffix: str = "",
    warehouse_dataset_name: str = None,
    source_base_name: str = None
):
    """
    Creates DLT assets for syncing multiple Airtable tables to the warehouse.
    
    Args:
        base_name: The name to use for asset naming (e.g., 'unified_ysws')
        tables: List of table names to sync (e.g., ['neighbors', 'projects'])
        description: Optional custom description for the assets
        source_suffix: Optional suffix for source asset names (e.g., '_refresh')
        warehouse_dataset_name: Override for warehouse dataset name (default: f"airtable_{base_name}")
        source_base_name: Override for the source base name in input dependencies (default: base_name)
    """
    assets_list = []  # Renamed from 'assets' to avoid potential module name conflict
    
    # Use source_base_name if provided, otherwise use base_name
    actual_source_base_name = source_base_name if source_base_name else base_name
    
    for table_name in tables:
        # Create a factory function to properly capture the table_name
        def create_sync_asset_for_table(specific_table_name):
            """Factory function that creates an asset function for a specific table"""
            @asset(
                name=f"{base_name}_{specific_table_name}_warehouse",
                compute_kind="dlt",
                group_name=f"dlt_airtable_{base_name}",
                description=description or f"Loads {base_name}.{specific_table_name} data into the warehouse.airtable_{base_name} schema.",
                ins={
                    f"{specific_table_name}{source_suffix}": AssetIn(key_prefix=["airtable", actual_source_base_name])
                }
            )
            def sync_asset(context: AssetExecutionContext, **kwargs):
                # Use specific_table_name that was captured by the factory function closure
                pipeline_name_base = f"{base_name}_to_warehouse" 
                dataset_name = warehouse_dataset_name if warehouse_dataset_name else f"airtable_{base_name}"
                table_name_warehouse = specific_table_name

                # Get the input DataFrame (use the suffixed key)
                input_key = f"{specific_table_name}{source_suffix}"
                df = kwargs[input_key]

                # Get field mappings from generated_ids - use actual_source_base_name for AirtableIDs lookup
                base_class = getattr(AirtableIDs, actual_source_base_name) 
                table_class = getattr(base_class, specific_table_name)
                field_mappings = {
                    getattr(table_class, field_name): field_name
                    for field_name in dir(table_class)
                    if not field_name.startswith('_') and field_name != 'TABLE_ID'
                }

                existing_columns = set(df.columns)

                # Build present mappings only for columns actually in the DataFrame
                present_mappings = {
                    old_name: new_name
                    for old_name, new_name in field_mappings.items()
                    if old_name in existing_columns
                }

                # Columns that will remain unchanged after renaming
                non_renamed_columns = existing_columns - set(present_mappings.keys())

                # 'taken' are names that will exist after rename for sure (cannot collide with these)
                taken = set(non_renamed_columns)

                # Group mappings by their desired target name
                from collections import defaultdict
                groups = defaultdict(list)
                for old_name, new_name in present_mappings.items():
                    groups[new_name].append(old_name)

                rename_map = {}

                def next_unique(base: str) -> str:
                    # Generate a unique name by adding numeric suffix if needed
                    candidate = base
                    i = 2
                    while candidate in taken:
                        candidate = f"{base}_{i}"
                        i += 1
                    taken.add(candidate)
                    return candidate

                # Prefix for resolving collisions, using the table name to keep it readable
                collision_prefix = f"{specific_table_name}_"

                # Build a deterministic, collision-free mapping
                for target_name, old_names in sorted(groups.items(), key=lambda kv: kv[0]):
                    old_names = sorted(old_names)  # deterministic ordering
                    if len(old_names) == 1 and target_name not in taken:
                        # No collision: use the nice name directly
                        rename_map[old_names[0]] = target_name
                        taken.add(target_name)
                    else:
                        # Collision with existing column or multiple IDs mapping to the same target name
                        # Apply prefix, and number if more than one
                        for idx, old in enumerate(old_names, start=1):
                            base = f"{collision_prefix}{target_name}" if target_name in taken or len(old_names) > 1 else target_name
                            # If there are multiple olds for the same target, make them stable with numeric suffix
                            if len(old_names) > 1:
                                base = f"{base}_{idx}"
                            unique = next_unique(base)
                            rename_map[old] = unique

                context.log.info(f"Resolved rename map for '{specific_table_name}' (total {len(rename_map)}): {rename_map}")

                # Sanity check before renaming
                context.log.info(f"Before renaming: {df.columns}")
                renamed_df = df.rename(rename_map) if rename_map else df
                context.log.info(f"After renaming: {renamed_df.columns}")
                
                # Assert no duplicates
                assert len(renamed_df.columns) == len(set(renamed_df.columns)), "Duplicate column names still present!"
                
                # Sanitize string columns to remove/escape problematic characters
                context.log.info("Sanitizing string columns to remove problematic characters for SQL")
                for col in renamed_df.columns:
                    if renamed_df[col].dtype == pl.Utf8:
                        # Replace control characters with spaces
                        renamed_df = renamed_df.with_columns(
                            pl.col(col)
                            .str.replace_all(r'[\n\r\t\v\f\x00-\x08\x0b-\x1f\x7f]', ' ', literal=False)
                            .alias(col)
                        )
                        # Remove any remaining non-printable or problematic Unicode characters
                        # Keep only standard printable ASCII and common Unicode
                        renamed_df = renamed_df.with_columns(
                            pl.col(col)
                            .str.replace_all(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', literal=False)
                            .alias(col)
                        )
                        # Trim excessive whitespace that may have been created
                        renamed_df = renamed_df.with_columns(
                            pl.col(col)
                            .str.replace_all(r'\s+', ' ', literal=False)
                            .str.strip_chars()
                            .alias(col)
                        )
                
                # Make DLT pipeline name unique per table to avoid state conflicts
                dlt_pipeline_name = f"{pipeline_name_base}_{specific_table_name}"

                context.log.info(f"Starting DLT pipeline '{dlt_pipeline_name}' to load data.")
                context.log.info(f"Destination: Postgres, Dataset (Schema): '{dataset_name}', Table: '{table_name_warehouse}'")
                context.log.info(f"Columns after renaming: {renamed_df.columns}")

                try:
                    pipeline = dlt.pipeline(
                        pipeline_name=dlt_pipeline_name, # Use unique pipeline name
                        destination=warehouse_coolify_destination(),
                        dataset_name=dataset_name,
                        progress="log",
                        pipelines_dir=".dlt_pipelines"  # Consistent working directory
                    )
                except Exception as e:
                    context.log.warning(f"Pipeline creation failed, likely due to corrupted state: {e}")
                    # Drop the corrupted pipeline and recreate
                    import shutil
                    import os
                    pipeline_dir = f".dlt_pipelines/{dlt_pipeline_name}"
                    if os.path.exists(pipeline_dir):
                        shutil.rmtree(pipeline_dir)
                        context.log.info(f"Removed corrupted pipeline directory: {pipeline_dir}")
                    
                    pipeline = dlt.pipeline(
                        pipeline_name=dlt_pipeline_name,
                        destination=warehouse_coolify_destination(),
                        dataset_name=dataset_name,
                        progress="log",
                        pipelines_dir=".dlt_pipelines"
                    )

                data_iterator = renamed_df.iter_rows(named=True)
                try:
                    load_info = pipeline.run(
                        data=data_iterator,
                        table_name=table_name_warehouse,
                        write_disposition="replace",
                        primary_key="id"
                    )

                    context.log.info(f"DLT pipeline run finished successfully for table '{specific_table_name}'.")
                    context.log.info(f"Load Info for '{specific_table_name}': {load_info}")

                    return Output(
                        value=None,
                        metadata={
                            "dlt_pipeline_name": MetadataValue.text(dlt_pipeline_name),
                            "dlt_dataset_name": MetadataValue.text(dataset_name),
                            "dlt_table_name": MetadataValue.text(table_name_warehouse),
                            "write_disposition": MetadataValue.text("replace"),
                            "first_run": MetadataValue.bool(load_info.first_run),
                            "num_records": MetadataValue.int(renamed_df.height),
                            "columns": MetadataValue.text(", ".join(renamed_df.columns))
                        }
                    )

                except Exception as e:
                    error_str = str(e)
                    # Check if this is a schema already exists error (race condition from parallel runs)
                    if "pg_namespace_nspname_index" in error_str and "already exists" in error_str:
                        context.log.warning(f"Schema already exists (race condition from parallel runs). Retrying...")
                        # Just retry - the schema already exists which is what we want
                        data_iterator = renamed_df.iter_rows(named=True)
                        load_info = pipeline.run(
                            data=data_iterator,
                            table_name=table_name_warehouse,
                            write_disposition="replace",
                            primary_key="id"
                        )
                        
                        context.log.info(f"DLT pipeline retry succeeded after schema creation race condition.")
                        return Output(
                            value=None,
                            metadata={
                                "dlt_pipeline_name": MetadataValue.text(dlt_pipeline_name),
                                "dlt_dataset_name": MetadataValue.text(dataset_name),
                                "dlt_table_name": MetadataValue.text(table_name_warehouse),
                                "write_disposition": MetadataValue.text("replace"),
                                "first_run": MetadataValue.bool(load_info.first_run),
                                "num_records": MetadataValue.int(renamed_df.height),
                                "columns": MetadataValue.text(", ".join(renamed_df.columns)),
                                "recovered_from_schema_race": MetadataValue.bool(True)
                            }
                        )
                    # Check if this is a corrupted load package error (unterminated quoted string, SQL syntax errors)
                    elif "unterminated quoted string" in error_str or "LoadClientJobRetry" in error_str:
                        context.log.warning(f"DLT pipeline '{dlt_pipeline_name}' has corrupted load package. Clearing pipeline state and retrying...")
                        # Clear the entire pipeline directory to remove corrupted load packages
                        import shutil
                        import os
                        pipeline_dir = f".dlt_pipelines/{dlt_pipeline_name}"
                        if os.path.exists(pipeline_dir):
                            shutil.rmtree(pipeline_dir)
                            context.log.info(f"Removed corrupted pipeline directory: {pipeline_dir}")
                        
                        # Recreate pipeline and retry with sanitized data
                        pipeline = dlt.pipeline(
                            pipeline_name=dlt_pipeline_name,
                            destination=warehouse_coolify_destination(),
                            dataset_name=dataset_name,
                            progress="log",
                            pipelines_dir=".dlt_pipelines"
                        )
                        
                        # Regenerate iterator (old one was consumed)
                        data_iterator = renamed_df.iter_rows(named=True)
                        load_info = pipeline.run(
                            data=data_iterator,
                            table_name=table_name_warehouse,
                            write_disposition="replace",
                            primary_key="id"
                        )
                        
                        context.log.info(f"DLT pipeline retry succeeded after clearing corrupted state.")
                        return Output(
                            value=None,
                            metadata={
                                "dlt_pipeline_name": MetadataValue.text(dlt_pipeline_name),
                                "dlt_dataset_name": MetadataValue.text(dataset_name),
                                "dlt_table_name": MetadataValue.text(table_name_warehouse),
                                "write_disposition": MetadataValue.text("replace"),
                                "first_run": MetadataValue.bool(load_info.first_run),
                                "num_records": MetadataValue.int(renamed_df.height),
                                "columns": MetadataValue.text(", ".join(renamed_df.columns)),
                                "recovered_from_corruption": MetadataValue.bool(True)
                            }
                        )
                    else:
                        context.log.error(f"DLT pipeline '{dlt_pipeline_name}' failed: {e}")
                        raise
            
            # Set the function name to match the asset name
            sync_asset.__name__ = f"{base_name}_{specific_table_name}_warehouse"
            return sync_asset
        
        # Create the asset function for this specific table and add it to our list
        asset_function = create_sync_asset_for_table(table_name)
        assets_list.append(asset_function)
    
    return assets_list

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

athena_award_assets = create_airtable_sync_assets(
    base_name="athena_award",
    tables=["registered_users", "email_slack_invites", "free_sticker_form", "projects"],
    description="Loads athena_award.registered_users, athena_award.email_slack_invites, athena_award.free_sticker_form, and athena_award.projects data into the warehouse.athena_award schema."
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
