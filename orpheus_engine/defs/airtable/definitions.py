from dagster import Definitions, load_assets_from_modules

# Import the resource and asset modules
from . import assets, resources

# Load assets defined in the assets module
airtable_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=airtable_assets,
    resources={
        # Define the key "airtable" and instantiate the resource
        "airtable": resources.AirtableResource(),
    },
)