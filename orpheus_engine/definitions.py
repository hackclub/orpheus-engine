import dagster as dg

import orpheus_engine.defs
import orpheus_engine.defs.dbt.definitions as dbt_defs
import orpheus_engine.schedules as schedules

defs = dg.Definitions.merge(
    dg.components.load_defs(orpheus_engine.defs),
    dbt_defs.defs,
    schedules.defs
)