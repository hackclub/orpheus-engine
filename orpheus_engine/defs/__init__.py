import dagster as dg

import orpheus_engine.defs.loops.definitions as loops_defs

defs = dg.Definitions.merge(
    loops_defs.defs
)