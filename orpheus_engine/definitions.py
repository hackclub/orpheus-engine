import dagster as dg
from dagster_aws.s3 import S3PickleIOManager, S3Resource

import orpheus_engine.defs

s3_resource = S3Resource(
    # Credentials will be picked up from environment variables
    # region_name can be explicitly set here or via AWS_REGION env var
    # region_name=dg.EnvVar("AWS_REGION")
)

s3_io_manager = S3PickleIOManager(
    s3_resource=s3_resource,
    # Bucket/prefix config comes from dagster.yaml 'resources.s3_io_manager.config'
)

other_defs = dg.Definitions.merge(
    dg.load_assets_from_package_module(orpheus_engine.assets),
    dg.load_assets_from_package_module(orpheus_engine.assets.dlt),
    dg.Definitions(schedules=[orpheus_engine.defs.sync_loops_schedule]),
)

defs = dg.Definitions(
    assets=other_defs.assets,
    schedules=other_defs.schedules,
    sensors=other_defs.sensors,
    jobs=other_defs.jobs,
    resources={
        "s3_io_manager": s3_io_manager,
        "s3": s3_resource,
        **orpheus_engine.defs.RESOURCES_PROD,
    },
)
