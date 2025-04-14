import dagster as dg
import orpheus_engine.defs

defs = dg.Definitions.merge(
    dg.components.load_defs(orpheus_engine.defs)
)
