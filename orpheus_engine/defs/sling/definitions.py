from dagster import Definitions
from .assets import (
    hackatime_warehouse_mirror,
    hcer_public_github_data_warehouse_mirror,
    journey_warehouse_mirror,
    shipwrecked_the_bay_warehouse_mirror,
    summer_of_making_2025_warehouse_mirror,
    sling_replication_resource,
)

defs = Definitions(
    assets=[
        hackatime_warehouse_mirror,
        hcer_public_github_data_warehouse_mirror,
        journey_warehouse_mirror,
        shipwrecked_the_bay_warehouse_mirror,
        summer_of_making_2025_warehouse_mirror,
    ],
    resources={
        "sling": sling_replication_resource,
    },
)
