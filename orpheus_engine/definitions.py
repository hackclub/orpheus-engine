import dagster as dg

import orpheus_engine.defs
import orpheus_engine.schedules as schedules

defs = dg.Definitions.merge(
    dg.components.load_defs(orpheus_engine.defs),
    schedules.defs,
)