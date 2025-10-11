import dagster as dg

# Import definition modules that export defs
import orpheus_engine.defs.airtable.definitions as airtable_defs
import orpheus_engine.defs.dbt.definitions as dbt_defs
import orpheus_engine.defs.dlt.definitions as dlt_defs
import orpheus_engine.defs.loops.definitions as loops_defs
import orpheus_engine.defs.sling.definitions as sling_defs
import orpheus_engine.defs.unified_ysws_db.definitions as ysws_defs
import orpheus_engine.schedules as schedules

# Import analytics asset separately (it doesn't export defs)
from orpheus_engine.defs.analytics.definitions import analytics_hack_clubbers

# Merge all definitions
defs = dg.Definitions.merge(
    airtable_defs.defs,
    dbt_defs.defs,
    dlt_defs.defs,
    loops_defs.defs,
    sling_defs.defs,
    ysws_defs.defs,
    schedules.defs,
    dg.Definitions(assets=[analytics_hack_clubbers])
)