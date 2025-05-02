from dagster import Definitions
from .assets import hackatime_sling_assets, sling_replication_resource

defs = Definitions(
    assets=[
        hackatime_sling_assets,
    ],
    resources={
        "sling": sling_replication_resource,
    },
)
