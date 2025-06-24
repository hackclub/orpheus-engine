from dagster import EnvVar, AssetExecutionContext, Nothing
from dagster_sling import SlingResource, SlingConnectionResource
from typing import Mapping, Any
import dagster as dg

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

summer_of_making_2025_db_connection = SlingConnectionResource(
    name="SUMMER_OF_MAKING_2025_DB",
    type="postgres",
    connection_string=EnvVar("SUMMER_OF_MAKING_2025_COOLIFY_URL"),
)

hackatime_legacy_db_connection = SlingConnectionResource(
    name="HACKATIME_LEGACY_DB",
    type="postgres",
    connection_string=EnvVar("HACKATIME_LEGACY_COOLIFY_URL"),
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
        summer_of_making_2025_db_connection,
        hackatime_legacy_db_connection,
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

# --- Summer of Making 2025 Database Replication Configuration ---
summer_of_making_2025_replication_config = {
    "source": "SUMMER_OF_MAKING_2025_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "summer_of_making_2025.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

# --- Hackatime Legacy Database Replication Configuration ---
hackatime_legacy_replication_config = {
    "source": "HACKATIME_LEGACY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hackatime_legacy.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

# --- Single Assets per Database ---

@dg.asset(
    name="hackatime_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime → warehouse Sling replication")

    # Iterate through the generator **without yielding** its events.
    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    # Optionally attach run‑level metadata
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hcer_public_github_data_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hcer_public_github_data_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire HCER Public GitHub Data DB → warehouse in a single shot."""
    context.log.info("Starting HCER Public GitHub Data → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hcer_public_github_data_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="journey_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def journey_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Journey DB → warehouse in a single shot."""
    context.log.info("Starting Journey → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=journey_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="shipwrecked_the_bay_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def shipwrecked_the_bay_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Shipwrecked The Bay DB → warehouse in a single shot."""
    context.log.info("Starting Shipwrecked The Bay → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=shipwrecked_the_bay_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="summer_of_making_2025_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def summer_of_making_2025_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Summer of Making 2025 DB → warehouse in a single shot."""
    context.log.info("Starting Summer of Making 2025 → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=summer_of_making_2025_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hackatime_legacy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_legacy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime Legacy DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime Legacy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_legacy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
