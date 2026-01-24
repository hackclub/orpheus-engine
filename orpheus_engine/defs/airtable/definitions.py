import re
import polars as pl
from dagster import (
    Definitions,
    # load_assets_from_modules, # Removed as assets are generated here
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    AssetKey,
)

# Import the resource and config models
from .resources import AirtableResource
from .config import AirtableServiceConfig, AirtableBaseConfig, AirtableTableConfig

# --- Generated IDs Handling (Copied from old assets.py) ---
try:
    from .generated_ids import AirtableIDs
except ImportError:
    print("WARNING: Could not import generated_ids.py. Run the generation script.")
    class AirtableIDs: # Dummy class for fallback
        class analytics_hack_clubbers: # Use snake_case
             base_id = "UNKNOWN"
             class hack_clubbers: table_id = "UNKNOWN"
             class field_mapping_rules: table_id = "UNKNOWN"
             class program_mapping_rules: table_id = "UNKNOWN"
        # Add other dummy bases/tables if necessary based on config

# --- Helper Function (Copied from old assets.py) ---
def sanitize_name(name: str) -> str:
    """Converts a potentially non-pythonic name to a valid snake_case identifier."""
    s = re.sub(r'\W|^(?=\d)', '_', name)
    s = re.sub('([A-Z]+)', r'_\1', s).lower().strip('_')
    s = re.sub(r'_+', '_', s)
    return s

# --- Airtable Configuration ---
# Define your Airtable structure here
airtable_config = AirtableServiceConfig(
    bases={
        "analytics_hack_clubbers": AirtableBaseConfig(
            base_id="appCGB6LccMzwkJZg",
            tables={
                "hack_clubbers": AirtableTableConfig(
                    table_id="tbluKHJi8hmg34uOF"
                ),
                "field_mapping_rules": AirtableTableConfig(
                    table_id="tbl7SkwqGYtFl4pe4"
                ),
                "program_mapping_rules": AirtableTableConfig(
                    table_id="tbl02flnwdqn8NcgG"
                ),
                "programs": AirtableTableConfig(
                    table_id="tblo0e0B4UQ4edi31"
                ),
            }
        ),
        "neighborhood": AirtableBaseConfig(
            base_id="appnsN4MzbnfMY0ai",
            tables={
                "neighbors": AirtableTableConfig(
                    table_id="tblTGpK7QgwsqT8t0"
                ),
                "hackatime_projects": AirtableTableConfig(
                    table_id="tblIqliBgKvoNT3uD"
                )
            }
        ),
        "shipwrecked": AirtableBaseConfig(
            base_id="appcfQcNlm6vXUOad",
            tables={
                "rsvps": AirtableTableConfig(
                    table_id="tblGW44enlxcYjDjS"
                )
            }
        ),
        "highway": AirtableBaseConfig(
            base_id="appuDQSHCdCHyOrxw",
            tables={
                "rsvps": AirtableTableConfig(
                    table_id="tblhGTc3WX9nYzU18"
                )
            }
        ),
        "athena_award": AirtableBaseConfig(
            base_id="appwSxpT4lASsosUI",
            tables={
                "registered_users": AirtableTableConfig(
                    table_id="tblIk0A5f9eqSng1h"
                ),
                "email_slack_invites": AirtableTableConfig(
                    table_id="tblmiCs66Oy9ux6eN"
                ),
                "free_sticker_form": AirtableTableConfig(
                    table_id="tblBOgmyC9RVK98wD"
                ),
                "projects": AirtableTableConfig(
                    table_id="tblZqHeeelwGkOkRr"
                )
            }
        ),
        "unified_ysws_projects_db": AirtableBaseConfig(
            base_id="app3A5kJwYqxMLOgh",
            tables={
                "approved_projects": AirtableTableConfig(
                    table_id="tblzWWGUYHVH7Zyqf"
                ),
                "ysws_programs": AirtableTableConfig(
                    table_id="tblrGi9RARJy1A0c5"
                ),
                "ysws_authors": AirtableTableConfig(
                    table_id="tblRf1BQs5H8298gW"
                ),
                "nps": AirtableTableConfig(
                    table_id="tblQpkS0I9V2ixBD0"
                ),
                "ysws_project_mentions": AirtableTableConfig(
                    table_id="tbl5SU7OcPeZMaDOs"
                ),
                "ysws_project_mention_searches": AirtableTableConfig(
                    table_id="tblfU7k0cgzysujpH"
                ),
                "ysws_spot_checks": AirtableTableConfig(
                    table_id="tbltWKtnaXRVJDEo3"
                ),
                "ysws_spot_check_sessions": AirtableTableConfig(
                    table_id="tblJCBhT1pYJcrQjV"
                )
            }
        ),
        "slack_nps": AirtableBaseConfig(
            base_id="appvetUwHQTgkNel3",
            tables={
                "nps": AirtableTableConfig(
                    table_id="tbl9iohVeaOd6ovDI"
                )
            }
        ),
        "campfire_flagship": AirtableBaseConfig(
            base_id="appPtca0Gx3KGw7oJ",
            tables={
                "rsvps": AirtableTableConfig(
                    table_id="tblOgqbg3Y7Wdu4k9"
                ),
                "hour_estimation": AirtableTableConfig(
                    table_id="tblJ8oDsRaXyTNlN6"
                ),
                "cool_game_hour_reduction": AirtableTableConfig(
                    table_id="tblPXLNU7inCr55KC"
                )
            }
        ),
        "campfire": AirtableBaseConfig(
            base_id="appNV5AaqOvyDryyQ",
            tables={
                "event": AirtableTableConfig(
                    table_id="tbl5O03v6jl2FfWu4"
                ),
                "organizer": AirtableTableConfig(
                    table_id="tbldqdX0voIpuM56s"
                ),
                "hcb": AirtableTableConfig(
                    table_id="tblGt0gBpeoZ6oei7"
                ),
                "regions": AirtableTableConfig(
                    table_id="tblVx5rx6PzH7x6EJ"
                ),
                "rsvp": AirtableTableConfig(
                    table_id="tbl6zIZaoGnD3j6bb"
                ),
                "regional_managers": AirtableTableConfig(
                    table_id="tblnCXwP5OVDczHsf"
                ),
                "organizer_interest": AirtableTableConfig(
                    table_id="tblC3nSz8kpczayGR"
                ),
                "daydream_events": AirtableTableConfig(
                    table_id="tblriNbP6J6W2tEob"
                ),
                "participants": AirtableTableConfig(
                    table_id="tblfan2dxx0YOH7US"
                ),
                "venues": AirtableTableConfig(
                    table_id="tblNBEkm5m75NfeBv"
                ),
                "jay": AirtableTableConfig(
                    table_id="tbluALNKnREkLlLWM"
                ),
                "us_based_clubs": AirtableTableConfig(
                    table_id="tblXLQDcVRkbyHqIN"
                )
            }
        )
    }
)

# --- Dynamic Asset Generation ---


def create_airtable_assets(
    base_name: str,
    tables: list[str],
    deps: list = None,
    suffix: str = ""
):
    """
    Creates Airtable source assets for specific tables with optional dependencies and suffix.
    
    Args:
        base_name: The name of the Airtable base (e.g., 'unified_ysws_db')
        tables: List of table names to create assets for
        deps: Optional list of AssetKeys or asset dependencies
        suffix: Optional suffix to add to asset names (e.g., '_refresh')
    """
    assets_list = []
    
    for table_name in tables:
        def create_airtable_asset_for_table(specific_table_name):
            """Factory function that creates an Airtable asset for a specific table"""
            
            sanitized_base_name = sanitize_name(base_name)
            sanitized_table_name = sanitize_name(specific_table_name)
            asset_function_name = f"airtable_{base_name}_{specific_table_name}{suffix}"
            
            # Get Base and Table IDs from config
            try:
                base_id_for_log = airtable_config.bases[base_name].base_id
                table_id_for_log = airtable_config.bases[base_name].tables[specific_table_name].table_id
            except KeyError:
                base_id_for_log = "UNKNOWN (config error)"
                table_id_for_log = "UNKNOWN (config error)"
            
            @asset(
                name=f"{specific_table_name}{suffix}",
                key_prefix=["airtable", base_name],
                group_name=f"airtable_{sanitized_base_name}{suffix}",
                description=f"{'Refreshed ' if suffix else ''}Airtable data from '{specific_table_name}' in base '{base_name}'{' after processing' if suffix else ''}.",
                compute_kind="airtable",
                deps=deps or []
            )
            def dynamic_refresh_airtable_asset(
                context: AssetExecutionContext, airtable: AirtableResource
            ) -> Output[pl.DataFrame]:
                """Dynamically generated refresh Airtable asset."""
                
                context.log.info(f"{'Re-querying' if suffix else 'Fetching'} Airtable {'for fresh ' if suffix else ''}data from base '{base_name}' (ID: {base_id_for_log}), table '{specific_table_name}' (ID: {table_id_for_log})...")
                
                try:
                    df = airtable.get_all_records_as_polars(
                        context=context,
                        base_key=base_name,
                        table_key=specific_table_name,
                    )
                    
                    context.log.info(f"Successfully fetched {df.height} records with {df.width} columns{' (refreshed)' if suffix else ''}.")
                    
                    # Generate metadata
                    preview_limit = 5
                    if df.height > 0:
                        preview_df = df.head(preview_limit)
                        try:
                            preview_metadata = MetadataValue.md(preview_df.to_pandas().to_markdown(index=False))
                        except Exception:
                            preview_metadata = MetadataValue.text(str(preview_df))
                    else:
                        preview_metadata = MetadataValue.text("DataFrame is empty.")
                    
                    metadata = {
                        "base_key": MetadataValue.text(base_name),
                        "table_key": MetadataValue.text(specific_table_name),
                        "base_id": MetadataValue.text(base_id_for_log),
                        "table_id": MetadataValue.text(table_id_for_log),
                        "num_records": MetadataValue.int(df.height),
                        "num_columns": MetadataValue.int(df.width),
                        "columns": MetadataValue.text(", ".join(df.columns)),
                        "preview": preview_metadata,
                    }
                    
                    # Add refresh-specific metadata if this is a refresh asset
                    if suffix:
                        metadata["refresh_type"] = MetadataValue.text("post_processing_refresh")
                    
                    return Output(value=df, metadata=metadata)
                    
                except Exception as e:
                    context.log.error(f"Failed to fetch refreshed data from Airtable ({base_name}.{specific_table_name}): {e}", exc_info=True)
                    raise
            
            dynamic_refresh_airtable_asset.__name__ = asset_function_name
            return dynamic_refresh_airtable_asset
        
        # Create the asset function for this specific table
        asset_function = create_airtable_asset_for_table(table_name)
        assets_list.append(asset_function)
    
    return assets_list

# Generate all assets by iterating through the config using the factory function
all_airtable_assets = []
for base_k, base_conf in airtable_config.bases.items():
    # Get all table names for this base
    table_names = list(base_conf.tables.keys())
    # Create assets for this base using the factory function
    base_assets = create_airtable_assets(
        base_name=base_k,
        tables=table_names
    )
    all_airtable_assets.extend(base_assets)

# --- Dagster Definitions ---
defs = Definitions(
    assets=all_airtable_assets, # Use the generated list of assets
    resources={
        # Define the key "airtable" and instantiate the resource with config
        "airtable": AirtableResource(config=airtable_config),
    },
)