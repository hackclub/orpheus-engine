# orpheus-engine/orpheus_engine/defs/airtable/assets.py
import polars as pl
from dagster import asset, AssetExecutionContext, Output, MetadataValue

# Import the resource we just defined
from orpheus_engine.defs.airtable.resources import AirtableResource

# Define constants for your specific Airtable base and table
AIRTABLE_BASE_ID = "appCGB6LccMzwkJZg"
AIRTABLE_TABLE_ID = "tbluKHJi8hmg34uOF" # Using ID as requested
AIRTABLE_TABLE_NAME = "hack_clubbers" # For description/metadata

@asset(
    group_name="airtable",
    description=f"Fetches all records from the Airtable table '{AIRTABLE_TABLE_NAME}' ({AIRTABLE_TABLE_ID}) in base {AIRTABLE_BASE_ID}.",
    compute_kind="airtable",
)
def hack_clubbers_airtable(context: AssetExecutionContext, airtable: AirtableResource) -> Output[pl.DataFrame]:
    """
    Dagster asset that retrieves all records from the specified Airtable table
    and returns them as a Polars DataFrame.
    """
    context.log.info(f"Fetching data from Airtable base '{AIRTABLE_BASE_ID}', table '{AIRTABLE_TABLE_ID}'...")

    try:
        df = airtable.get_all_records_as_polars(
            base_id=AIRTABLE_BASE_ID,
            table_id_or_name=AIRTABLE_TABLE_ID,
            # Optionally specify fields=["Field Name 1", "Field Name 2"] if you don't need all columns
            # Optionally add a formula="FIND('keyword', {Your Field Name})"
        )

        context.log.info(f"Successfully fetched {df.height} records with {df.width} columns.")

        # Generate preview data safely
        preview_limit = 5
        preview_df = df.head(preview_limit)
        try:
            # Convert preview to JSON string for metadata, handling potential errors
            preview_json = preview_df.write_json(row_oriented=True, pretty=False)
            preview_metadata = MetadataValue.json(json.loads(preview_json)) # Dagster expects dict/list
        except Exception as json_err:
            context.log.warning(f"Could not serialize preview to JSON: {json_err}")
            # Fallback to simpler text representation if JSON fails
            preview_metadata = MetadataValue.text(str(preview_df))


        return Output(
            value=df,
            metadata={
                "base_id": AIRTABLE_BASE_ID,
                "table_id": AIRTABLE_TABLE_ID,
                "num_records": df.height,
                "num_columns": df.width,
                "columns": MetadataValue.text(", ".join(df.columns)),
                "preview": preview_metadata,
            }
        )

    except Exception as e:
        context.log.error(f"Failed to fetch data from Airtable: {e}")
        raise # Re-raise the exception to mark the asset run as failed