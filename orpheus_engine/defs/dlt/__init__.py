import dlt
from dlt.destinations import postgres # Import the specific destination class

import os
from orpheus_engine.defs.loops.definitions import loops_contacts_export

@dlt.resource(name="audience")
def loops_audience_resource():
    yield loops_contacts_export()


def warehouse_coolify_destination() -> postgres:
    """Creates and returns a configured postgres destination instance."""
    creds = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not creds:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set or empty.")

    return dlt.destinations.postgres(credentials=creds)

pipeline = dlt.pipeline(
    pipeline_name="loops_to_warehouse",
    destination=warehouse_coolify_destination(),
    dataset_name="loops" # Schema name
)
