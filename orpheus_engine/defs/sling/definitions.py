from dagster import Definitions
from .assets import (
    hackatime_warehouse_mirror,
    hcer_public_github_data_warehouse_mirror,
    journey_warehouse_mirror,
    shipwrecked_the_bay_warehouse_mirror,
    summer_of_making_2025_warehouse_mirror,
    hackatime_legacy_warehouse_mirror,
    flavortown_warehouse_mirror,
    hack_club_the_game_warehouse_mirror,
    blueprint_warehouse_mirror,
    stasis_warehouse_mirror,
    fallout_warehouse_mirror,
    horizons_warehouse_mirror,
    flavortown_ahoy_warehouse_mirror,
    hcb_warehouse_mirror,
    auth_warehouse_mirror,  # absolute minimum permissions for monthly active stats
    sling_replication_resource,
)

defs = Definitions(
    assets=[
        hackatime_warehouse_mirror,
        hcer_public_github_data_warehouse_mirror,
        journey_warehouse_mirror,
        shipwrecked_the_bay_warehouse_mirror,
        summer_of_making_2025_warehouse_mirror,
        hackatime_legacy_warehouse_mirror,
        flavortown_warehouse_mirror,
        hack_club_the_game_warehouse_mirror,
        blueprint_warehouse_mirror,
        stasis_warehouse_mirror,
        fallout_warehouse_mirror,
        horizons_warehouse_mirror,
        flavortown_ahoy_warehouse_mirror,
        hcb_warehouse_mirror,
        auth_warehouse_mirror,
    ],
    resources={
        "sling": sling_replication_resource,
    },
)
