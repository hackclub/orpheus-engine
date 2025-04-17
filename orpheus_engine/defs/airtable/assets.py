# orpheus-engine/orpheus_engine/defs/airtable/assets.py
import polars as pl
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import json # Need json for metadata preview
import re # Need re for sanitize_name helper

# Import the resource
from orpheus_engine.defs.airtable.resources import AirtableResource
# Import the generated IDs
try:
    from .generated_ids import AirtableIDs
except ImportError:
    # Provide a fallback or raise a more informative error if the file MUST exist
    print("WARNING: Could not import generated_ids.py. Run the generation script.")
    # Define a dummy class so the rest of the code doesn't break immediately
    class AirtableIDs:
        class HACK_CLUB_STUFF:
            BASE_ID = "UNKNOWN"
            class HACK_CLUBBERS:
                TABLE_ID = "UNKNOWN"
                # Add dummy field IDs if needed for testing before generation
                # EMAIL = "fldXXXXXXXXXXXXXX"
        # Add other dummy base/table structures if necessary

# Helper function from script - needed for getattr access
def sanitize_name(name: str) -> str:
    """Converts a potentially non-pythonic name to a valid UPPER_SNAKE_CASE identifier."""
    # Replace non-alphanumeric characters with underscores
    s = re.sub(r'\W|^(?=\d)', '_', name)
    # Convert camelCase/PascalCase to snake_case (simplified)
    s = re.sub('([A-Z]+)', r'_\1', s).strip('_')
    # Handle multiple underscores
    s = re.sub(r'_+', '_', s)
    return s.upper()


# Define constants for LOGICAL keys used in the resource configuration
# These should match the keys you used in airtable/definitions.py
BASE_KEY_HACK_CLUB = "analytics_hack_clubbers"
TABLE_KEY_CLUBBERS = "hack_clubbers"

@asset(
    group_name="airtable",
    description=f"Fetches all records from the Airtable table '{TABLE_KEY_CLUBBERS}' in base '{BASE_KEY_HACK_CLUB}'.",
    compute_kind="airtable",
    key_prefix=["airtable", BASE_KEY_HACK_CLUB] # Optional: Organize assets in UI/Lineage
)
def hack_clubbers_airtable(context: AssetExecutionContext, airtable: AirtableResource) -> Output[pl.DataFrame]:
    """
    Dagster asset that retrieves all records from the configured Airtable table
    using logical keys and returns them as a Polars DataFrame.
    """
    # Access generated IDs using the structure defined in the script
    # Use getattr for safer access in case script hasn't run or keys changed
    BaseIDs = getattr(AirtableIDs, sanitize_name(BASE_KEY_HACK_CLUB), None)
    TableIDs = getattr(BaseIDs, sanitize_name(TABLE_KEY_CLUBBERS), None) if BaseIDs else None

    if not TableIDs:
        context.log.warning(f"Could not find generated IDs for {BASE_KEY_HACK_CLUB}.{TABLE_KEY_CLUBBERS}. Check config and run generation script. Using fallback dummy IDs.")
        # Use the dummy class defined above if import failed
        BaseIDs = getattr(AirtableIDs, sanitize_name(BASE_KEY_HACK_CLUB))
        TableIDs = getattr(BaseIDs, sanitize_name(TABLE_KEY_CLUBBERS))
        table_id_for_log = TableIDs.TABLE_ID
        base_id_for_log = BaseIDs.BASE_ID
    else:
        table_id_for_log = getattr(TableIDs, 'TABLE_ID', 'UNKNOWN')
        base_id_for_log = getattr(BaseIDs, 'BASE_ID', 'UNKNOWN')

    context.log.info(f"Fetching data from Airtable base '{BASE_KEY_HACK_CLUB}' (ID: {base_id_for_log}), table '{TABLE_KEY_CLUBBERS}' (ID: {table_id_for_log})...")

    try:
        # Fetch using logical keys, pass context
        df = airtable.get_all_records_as_polars(
            context=context,
            base_key=BASE_KEY_HACK_CLUB,
            table_key=TABLE_KEY_CLUBBERS,
            # Optionally specify fields=["Field Name 1", "Field Name 2"] if you don't need all columns
            # Optionally add a formula="FIND('keyword', {Your Field Name})"
        )

        context.log.info(f"Successfully fetched {df.height} records with {df.width} columns.")
        context.log.info(f"Columns returned (Field Names): {', '.join(df.columns)}")

        # Example: Using a generated ID constant (assuming it exists)
        # This demonstrates how downstream code uses the stable IDs
        email_field_id = getattr(TableIDs, 'EMAIL', None) # Safely get EMAIL ID
        if email_field_id:
            # We fetched by *name*, so the column *name* corresponding to the ID is needed
            # This currently requires knowing the name ('Email' in this example)
            # A future enhancement could involve fetching the schema here or in the resource
            # to create a name -> id mapping if needed for complex renaming/selection.
            email_column_name = 'Email' # Assuming the field name is 'Email'

            if email_column_name in df.columns:
                 context.log.info(f"Accessed email column ('{email_column_name}' linked to ID {email_field_id})")
                 # Example selection using the name, knowing it maps to EMAIL ID
                 # emails_series = df[email_column_name]

                 # Example Renaming (if you want DF keyed by ID downstream):
                 # try:
                 #     df = df.rename({email_column_name: email_field_id})
                 #     context.log.info(f"Renamed column '{email_column_name}' to ID '{email_field_id}'")
                 # except Exception as rename_err:
                 #     context.log.warning(f"Could not rename column '{email_column_name}' to '{email_field_id}': {rename_err}")
            else:
                 context.log.warning(f"Column '{email_column_name}' (expected for ID {email_field_id}) not found in DataFrame. Available columns: {df.columns}")
        elif TableIDs is not None: # Only log if TableIDs were expected but EMAIL missing
             context.log.info("EMAIL field ID constant not found in generated IDs for this table.")

        # --- Metadata Generation --- 
        preview_limit = 5
        # Ensure preview doesn't fail on empty dataframe
        if df.height > 0:
            preview_df = df.head(preview_limit)
            try:
                preview_metadata = MetadataValue.md(preview_df.to_pandas().to_markdown(index=False))
            except Exception as md_err:
                context.log.warning(f"Could not generate markdown preview: {md_err}")
                preview_metadata = MetadataValue.text(str(preview_df))
        else:
            preview_metadata = MetadataValue.text("DataFrame is empty.")


        # Add base/table IDs to metadata from constants if available
        metadata = {
            "base_key": BASE_KEY_HACK_CLUB,
            "table_key": TABLE_KEY_CLUBBERS,
            "base_id": base_id_for_log,
            "table_id": table_id_for_log,
            "num_records": df.height,
            "num_columns": df.width,
            "columns": MetadataValue.text(", ".join(df.columns)),
            "preview": preview_metadata,
        }

        return Output(value=df, metadata=metadata)

    except Exception as e:
        context.log.error(f"Failed to fetch data from Airtable: {e}", exc_info=True)
        raise # Re-raise the exception to mark the asset run as failed


# Add more assets here for other tables by defining new functions
# and using different BASE_KEY/TABLE_KEY constants
# e.g., marketing_contacts_airtable, operations_tasks_airtable