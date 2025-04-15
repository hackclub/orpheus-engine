from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource
from orpheus_engine.defs.dlt import assets

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "dlt": DagsterDltResource(),
    },
)