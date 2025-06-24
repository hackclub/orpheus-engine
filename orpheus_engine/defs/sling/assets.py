from dagster import EnvVar, AssetExecutionContext
from dagster_sling import SlingResource, SlingConnectionResource, sling_assets, DagsterSlingTranslator
from typing import Mapping, Any
import dagster as dg

# --- Define Custom Translator ---
class HackatimeSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """
        Return a unique key for each stream while keeping a single top‑level
        prefix: hackatime_warehouse_mirror/<schema>/<table>
        """
        default_spec = super().get_asset_spec(stream_definition)
        new_key_path = ["hackatime_warehouse_mirror", *default_spec.key.path]
        new_asset_key = dg.AssetKey(new_key_path)
        return default_spec.replace_attributes(key=new_asset_key)

class HcerPublicGithubDataSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """
        Return a unique key for each stream while keeping a single top‑level
        prefix: hcer_public_github_data_warehouse_mirror/<schema>/<table>
        """
        default_spec = super().get_asset_spec(stream_definition)
        new_key_path = ["hcer_public_github_data_warehouse_mirror", *default_spec.key.path]
        new_asset_key = dg.AssetKey(new_key_path)
        return default_spec.replace_attributes(key=new_asset_key)

class JourneySlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """
        Return a unique key for each stream while keeping a single top‑level
        prefix: journey_warehouse_mirror/<schema>/<table>
        """
        default_spec = super().get_asset_spec(stream_definition)
        new_key_path = ["journey_warehouse_mirror", *default_spec.key.path]
        new_asset_key = dg.AssetKey(new_key_path)
        return default_spec.replace_attributes(key=new_asset_key)

class ShipwreckedTheBaySlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """
        Return a unique key for each stream while keeping a single top‑level
        prefix: shipwrecked_the_bay_warehouse_mirror/<schema>/<table>
        """
        default_spec = super().get_asset_spec(stream_definition)
        new_key_path = ["shipwrecked_the_bay_warehouse_mirror", *default_spec.key.path]
        new_asset_key = dg.AssetKey(new_key_path)
        return default_spec.replace_attributes(key=new_asset_key)

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

shipwrecked_the_bay_db_connection = SlingConnectionResource(
    name="SHIPWRECKED_THE_BAY_DB",
    type="postgres",
    connection_string=EnvVar("SHIPWRECKED_THE_BAY_COOLIFY_URL"),
)

journey_db_connection = SlingConnectionResource(
    name="JOURNEY_DB",
    type="postgres",
    connection_string=EnvVar("JOURNEY_COOLIFY_URL"),
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
        shipwrecked_the_bay_db_connection,
        journey_db_connection,
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
        "public.*": {
            "exclude": [
                "pg_stat_statements",
                "pg_stat_statements_info"
            ]
        },
        "public.leaderboard_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.heartbeats": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at"
        }
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

# --- Journey Database Replication Configuration ---
journey_replication_config = {
    "source": "JOURNEY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "journey.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

@sling_assets(
    replication_config=journey_replication_config,
    dagster_sling_translator=JourneySlingTranslator()
)
def journey_sling_assets(context: AssetExecutionContext, sling: SlingResource):
    """
    Dagster asset definition for replicating Journey public schema
    to the Warehouse journey schema using Sling.
    """
    context.log.info(f"Starting Sling replication defined in config: {journey_replication_config}")

    # Execute the replication job defined by the config
    yield from sling.replicate(context=context)

    # Stream and log raw Sling output for debugging
    context.log.info("Streaming Sling raw logs...")
    for row in sling.stream_raw_logs():
        context.log.info(row)
    context.log.info("Sling replication finished.")

# --- Shipwrecked The Bay Database Replication Configuration ---
shipwrecked_the_bay_replication_config = {
    "source": "SHIPWRECKED_THE_BAY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "shipwrecked_the_bay.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

@sling_assets(
    replication_config=shipwrecked_the_bay_replication_config,
    dagster_sling_translator=ShipwreckedTheBaySlingTranslator()
)
def shipwrecked_the_bay_sling_assets(context: AssetExecutionContext, sling: SlingResource):
    """
    Dagster asset definition for replicating Shipwrecked The Bay public schema
    to the Warehouse shipwrecked_the_bay schema using Sling.
    """
    context.log.info(f"Starting Sling replication defined in config: {shipwrecked_the_bay_replication_config}")

    # Execute the replication job defined by the config
    yield from sling.replicate(context=context)

    # Stream and log raw Sling output for debugging
    context.log.info("Streaming Sling raw logs...")
    for row in sling.stream_raw_logs():
        context.log.info(row)
    context.log.info("Sling replication finished.")
