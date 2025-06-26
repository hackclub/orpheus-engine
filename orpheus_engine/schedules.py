import dagster as dg

# 1. "All assets" job
materialize_all_assets_job = dg.define_asset_job(
    name="materialize_all_assets_job",                     # pick any name you like
    selection="*",
)

# 2. Every 6 hours schedule (30 minutes past every 6 hours, NY time)
hourly_materialize_schedule = dg.ScheduleDefinition(
    name="hourly_materialize_schedule",
    job=materialize_all_assets_job,
    cron_schedule="30 */6 * * *",                           # minute hour day month weekday
    execution_timezone="America/New_York",                 # keeps logs in local time
)

# 3. Wrap in a Definitions so Dagster can find it
defs = dg.Definitions(
    jobs=[materialize_all_assets_job],
    schedules=[hourly_materialize_schedule],
) 