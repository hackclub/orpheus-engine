from dagster import Definitions
from .assets import hackatime_sling_assets, hcer_public_github_data_sling_assets, journey_sling_assets, shipwrecked_the_bay_sling_assets, sling_replication_resource

defs = Definitions(
    assets=[
        hackatime_sling_assets,
        hcer_public_github_data_sling_assets,
        journey_sling_assets,
        shipwrecked_the_bay_sling_assets,
    ],
    resources={
        "sling": sling_replication_resource,
    },
)
