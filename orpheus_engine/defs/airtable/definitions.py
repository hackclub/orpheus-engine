from dagster import Definitions, load_assets_from_modules

# Import the resource and asset modules
from . import assets # Keep this if you have assets defined here
from .resources import AirtableResource
from .config import AirtableServiceConfig, AirtableBaseConfig, AirtableTableConfig # Import config

# Define your Airtable structure here
# Replace with your actual Base IDs, Table IDs, and desired logical names
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
            }
        )
    }
)

# Load assets defined in the assets module
airtable_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=airtable_assets,
    resources={
        # Define the key "airtable" and instantiate the resource with config
        "airtable": AirtableResource(config=airtable_config),
    },
)