import re
import polars as pl
from dagster import (
    Definitions,
    # load_assets_from_modules, # Removed as assets are generated here
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
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
        )
    }
)

# --- Dynamic Asset Generation ---

def _create_airtable_asset(base_key: str, table_key: str):
    """Factory function to create a Dagster asset for a specific Airtable table."""

    sanitized_base_key = sanitize_name(base_key)
    sanitized_table_key = sanitize_name(table_key)
    asset_function_name = f"airtable_{base_key}_{table_key}"

    # --- Get Base and Table IDs directly from config for logging/metadata ---
    try:
        base_id_for_log = airtable_config.bases[base_key].base_id
        table_id_for_log = airtable_config.bases[base_key].tables[table_key].table_id
    except KeyError:
        # This shouldn't happen if the factory is called correctly based on the config
        print(f"ERROR: Could not find config for {base_key} or {table_key} in airtable_config.")
        base_id_for_log = "UNKNOWN (config error)"
        table_id_for_log = "UNKNOWN (config error)"
    # --- End ID fetching ---

    @asset(
        name=table_key,
        key_prefix=["airtable", base_key],
        group_name=f"airtable_{sanitized_base_key}",
        description=f"Fetches all records from the Airtable table '{table_key}' in base '{base_key}'.",
        compute_kind="airtable",
    )
    def dynamic_airtable_asset(
        context: AssetExecutionContext, airtable: AirtableResource
    ) -> Output[pl.DataFrame]:
        """Dynamically generated Dagster asset for an Airtable table."""

        # We still attempt to load generated IDs for potential downstream use, but don't rely on it for base/table IDs
        try:
            BaseIDs = getattr(AirtableIDs, sanitized_base_key, None)
            TableIDs = getattr(BaseIDs, sanitized_table_key, None) if BaseIDs else None
            if not TableIDs:
                context.log.warning(f"Could not load generated field IDs structure for {base_key}.{table_key}. Downstream use of field constants may fail.")
        except NameError: # Catch if AirtableIDs itself isn't defined due to import error
             context.log.warning(f"AirtableIDs class not found. Could not load generated field IDs structure for {base_key}.{table_key}. Downstream use of field constants may fail.")

        # Use the IDs fetched directly from config
        context.log.info(f"Fetching data from Airtable base '{base_key}' (ID: {base_id_for_log}), table '{table_key}' (ID: {table_id_for_log})...")

        try:
            df = airtable.get_all_records_as_polars(
                context=context,
                base_key=base_key,
                table_key=table_key,
            )

            context.log.info(f"Successfully fetched {df.height} records with {df.width} columns.")
            context.log.info(f"Columns returned (Field IDs): {', '.join(df.columns)}")

            # --- Metadata Generation ---
            preview_limit = 5
            if df.height > 0:
                preview_df = df.head(preview_limit)
                try:
                    preview_metadata = MetadataValue.md(preview_df.to_pandas().to_markdown(index=False))
                except Exception as md_err:
                    context.log.warning(f"Could not generate markdown preview: {md_err}")
                    preview_metadata = MetadataValue.text(str(preview_df))
            else:
                preview_metadata = MetadataValue.text("DataFrame is empty.")

            # Use IDs fetched directly from config for metadata
            metadata = {
                "base_key": MetadataValue.text(base_key),
                "table_key": MetadataValue.text(table_key),
                "base_id": MetadataValue.text(base_id_for_log),
                "table_id": MetadataValue.text(table_id_for_log),
                "num_records": MetadataValue.int(df.height),
                "num_columns": MetadataValue.int(df.width),
                "columns": MetadataValue.text(", ".join(df.columns)),
                "preview": preview_metadata,
            }

            return Output(value=df, metadata=metadata)

        except Exception as e:
            context.log.error(f"Failed to fetch data from Airtable ({base_key}.{table_key}): {e}", exc_info=True)
            raise

    dynamic_airtable_asset.__name__ = asset_function_name
    return dynamic_airtable_asset

# Generate all assets by iterating through the config
all_airtable_assets = []
for base_k, base_conf in airtable_config.bases.items():
    for table_k, _ in base_conf.tables.items():
        # Create and collect the asset function
        asset_fn = _create_airtable_asset(base_k, table_k)
        all_airtable_assets.append(asset_fn)

# --- Dagster Definitions ---
defs = Definitions(
    assets=all_airtable_assets, # Use the generated list of assets
    resources={
        # Define the key "airtable" and instantiate the resource with config
        "airtable": AirtableResource(config=airtable_config),
    },
)