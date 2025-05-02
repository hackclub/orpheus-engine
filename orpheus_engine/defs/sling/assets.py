from dagster import EnvVar, AssetExecutionContext
from dagster_sling import SlingResource, SlingConnectionResource, sling_assets

# --- Define Connections ---

# 1. Source Connection (Hackatime Database)
source_db_connection = SlingConnectionResource(
    name="HACKATIME_DB",  # This name MUST match the 'source' key in replication_config
    type="postgres",
    connection_string=EnvVar("HACKATIME_COOLIFY_URL"),
)

# 2. Target Connection (Warehouse Database)
warehouse_db_connection = SlingConnectionResource(
    name="WAREHOUSE_DB",  # This name MUST match the 'target' key in replication_config
    type="postgres",
    connection_string=EnvVar("WAREHOUSE_COOLIFY_URL"),
)

# --- Create Sling Resource ---
sling_replication_resource = SlingResource(
    connections=[
        source_db_connection,
        warehouse_db_connection,
    ]
)

# --- Define Replication Configuration ---
hackatime_replication_config = {
    "source": "HACKATIME_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hackatime.{stream_table}",
    },

    "streams": {
        "public.*": None,
    },

    "public.leaderboard_entries": {
        "mode": "incremental",
        "primary_key": ["id"],
        "update_key": ["updated_at"],
    },

    "public.heartbeats": {
        "mode": "incremental",
        "primary_key": ["id"],
        "update_key": ["updated_at"],
    }
}

@sling_assets(replication_config=hackatime_replication_config)
def hackatime_sling_assets(context: AssetExecutionContext, sling: SlingResource):
    """
    Dagster asset definition for replicating Hackatime public schema
    to the Warehouse hackatime schema using Sling.
    """
    context.log.info(f"Starting Sling replication defined in config: {hackatime_replication_config}")

    # Execute the replication job defined by the config
    yield from sling.replicate(context=context)

    # Stream and log raw Sling output for debugging
    context.log.info("Streaming Sling raw logs...")
    for row in sling.stream_raw_logs():
        context.log.info(row)
    context.log.info("Sling replication finished.")
