from dagster import EnvVar, AssetExecutionContext
from dagster_sling import SlingResource, SlingConnectionResource, sling_assets, DagsterSlingTranslator
from typing import Mapping, Any
import dagster as dg

# --- Define Custom Translator ---
class HackatimeSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """Overrides asset spec to set hackatime asset keys."""
        # We create the default asset spec using super()
        default_spec = super().get_asset_spec(stream_definition)
        # Override the key with hackatime prefix
        return default_spec.replace_attributes(
            key=dg.AssetKey(["hackatime_warehouse_mirror"])
        )

class HcerPublicGithubDataSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """Overrides asset spec to set hcer-public-github-data asset keys."""
        # We create the default asset spec using super()
        default_spec = super().get_asset_spec(stream_definition)
        # Override the key with hcer-public-github-data prefix
        return default_spec.replace_attributes(
            key=dg.AssetKey(["hcer_public_github_data_warehouse_mirror"])
        )

# --- Define Connections ---

# 1. Source Connection (Hackatime Database)
hackatime_db_connection = SlingConnectionResource(
    name="HACKATIME_DB",  # This name MUST match the 'source' key in replication_config
    type="postgres",
    connection_string=EnvVar("HACKATIME_COOLIFY_URL"),
)

hcer_public_github_data_connection = SlingConnectionResource(
    name="HCER_PUBLIC_GITHUB_DATA_DB",
    type="postgres",
    connection_string=EnvVar("HCER_PUBLIC_GITHUB_DATA_COOLIFY_URL"),
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
        hackatime_db_connection,
        hcer_public_github_data_connection,
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
        "update_key": ["updated_at"]
    }
}

@sling_assets(
    replication_config=hackatime_replication_config,
    dagster_sling_translator=HackatimeSlingTranslator()
)
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

# --- Define Replication Configuration ---
hcer_public_github_data_replication_config = {
    "source": "HCER_PUBLIC_GITHUB_DATA_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hcer_public_github_data.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

@sling_assets(
    replication_config=hcer_public_github_data_replication_config,
    dagster_sling_translator=HcerPublicGithubDataSlingTranslator()
)
def hcer_public_github_data_sling_assets(context: AssetExecutionContext, sling: SlingResource):
    """
    Dagster asset definition for replicating hcer-public-github-datapublic schema
    to the Warehouse hackatime schema using Sling.
    """
    context.log.info(f"Starting Sling replication defined in config: {hcer_public_github_data_replication_config}")

    # Execute the replication job defined by the config
    yield from sling.replicate(context=context)

    # Stream and log raw Sling output for debugging
    context.log.info("Streaming Sling raw logs...")
    for row in sling.stream_raw_logs():
        context.log.info(row)
    context.log.info("Sling replication finished.")
