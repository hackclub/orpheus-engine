import dagster as dg

# Import definition modules that export defs
import orpheus_engine.defs.airtable.definitions as airtable_defs
import orpheus_engine.defs.dbt.definitions as dbt_defs
import orpheus_engine.defs.dlt.definitions as dlt_defs
import orpheus_engine.defs.loops.definitions as loops_defs
import orpheus_engine.defs.loops_campaign_and_metrics_export.definitions as loops_campaign_defs
import orpheus_engine.defs.slack.definitions as slack_defs
import orpheus_engine.defs.sling.definitions as sling_defs
import orpheus_engine.defs.unified_ysws_db.definitions as ysws_defs
import orpheus_engine.defs.agh_fulfillment_zenventory.definitions as zenventory_defs
import orpheus_engine.defs.slack_users_sync.definitions as slack_users_sync_defs
import orpheus_engine.schedules as schedules

# Import analytics asset separately (it doesn't export defs)
from orpheus_engine.defs.analytics.definitions import analytics_hack_clubbers

# Import the DuckLake row hash asset factory and sync asset
from orpheus_engine.defs.ducklake.definitions import create_warehouse_row_hashes_asset, ducklake_sync

# Import shared exclusion list (single source of truth)
from orpheus_engine.schedules import EXCLUDED_FROM_MAIN_JOB


def _build_definitions() -> dg.Definitions:
    """
    Build the final Definitions object with dynamic DuckLake dependencies.
    Using a function to avoid having multiple Definitions at module scope.
    """
    # First, merge all definitions EXCEPT ducklake
    base_defs = dg.Definitions.merge(
        airtable_defs.defs,
        dbt_defs.defs,
        dlt_defs.defs,
        loops_defs.defs,
        loops_campaign_defs.defs,
        slack_defs.defs,
        slack_users_sync_defs.defs,
        sling_defs.defs,
        ysws_defs.defs,
        zenventory_defs.defs,
        schedules.defs,
        dg.Definitions(assets=[analytics_hack_clubbers])
    )

    # Dynamically discover all asset keys from the merged definitions
    # Handle both single assets (.key) and multi-assets (.keys)
    # Exclude the same assets that are excluded from materialize_all_assets_job
    all_asset_keys = []
    for asset in base_defs.assets:
        # Multi-assets have multiple keys, single assets have one
        if hasattr(asset, 'keys'):
            for key in asset.keys:
                if key not in EXCLUDED_FROM_MAIN_JOB:
                    all_asset_keys.append(key)
        else:
            if asset.key not in EXCLUDED_FROM_MAIN_JOB:
                all_asset_keys.append(asset.key)

    # Create the DuckLake row hash asset with dependencies matching materialize_all_assets_job
    warehouse_row_hashes = create_warehouse_row_hashes_asset(dep_asset_keys=all_asset_keys)

    # Final merged definitions including the DuckLake assets
    # - warehouse_row_hashes: computes row hashes (depends on all other assets)
    # - ducklake_sync: syncs to DuckLake (depends on warehouse_row_hashes)
    return dg.Definitions.merge(
        base_defs,
        dg.Definitions(assets=[warehouse_row_hashes, ducklake_sync])
    )


# Only one Definitions object at module scope
defs = _build_definitions()