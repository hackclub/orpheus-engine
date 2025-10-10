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

# 3. Unified YSWS refresh and warehouse assets job
# Select the refresh assets and warehouse assets explicitly using key() method
materialize_unified_ysws_job = dg.define_asset_job(
    name="materialize_unified_ysws_job",
    selection=(
        # Airtable refresh assets (with key_prefix path syntax)
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/approved_projects_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/ysws_programs_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/ysws_authors_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/nps_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/ysws_project_mentions_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/ysws_project_mention_searches_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/ysws_spot_checks_refresh") |
        dg.AssetSelection.keys("airtable/unified_ysws_projects_db/ysws_spot_check_sessions_refresh") |
        # Warehouse assets (no key_prefix)
        dg.AssetSelection.keys("unified_ysws_approved_projects_warehouse") |
        dg.AssetSelection.keys("unified_ysws_ysws_programs_warehouse") |
        dg.AssetSelection.keys("unified_ysws_ysws_authors_warehouse") |
        dg.AssetSelection.keys("unified_ysws_nps_warehouse") |
        dg.AssetSelection.keys("unified_ysws_ysws_project_mentions_warehouse") |
        dg.AssetSelection.keys("unified_ysws_ysws_project_mention_searches_warehouse") |
        dg.AssetSelection.keys("unified_ysws_ysws_spot_checks_warehouse") |
        dg.AssetSelection.keys("unified_ysws_ysws_spot_check_sessions_warehouse")
    ),
)

# 4. Every 15 minutes schedule for unified YSWS assets
unified_ysws_15min_schedule = dg.ScheduleDefinition(
    name="unified_ysws_15min_schedule",
    job=materialize_unified_ysws_job,
    cron_schedule="*/15 * * * *",                          # every 15 minutes
    execution_timezone="America/New_York",                 # keeps logs in local time
)

# 5. Wrap in a Definitions so Dagster can find it
defs = dg.Definitions(
    jobs=[materialize_all_assets_job, materialize_unified_ysws_job],
    schedules=[hourly_materialize_schedule, unified_ysws_15min_schedule],
) 