import dagster as dg
import orpheus_engine.defs
from dagster import EnvVar # Import EnvVar if defining other resources like loops_session_token
# Import the necessary resource class
from dagster_dlt import DagsterDltResource

# Create the final Definitions object by merging:
# 1. Definitions loaded automatically from submodules
# 2. A Definitions object containing explicitly defined resources
defs = dg.Definitions.merge(
    # Load definitions from submodules directly within the merge call
    dg.components.load_defs(orpheus_engine.defs),
    # Define explicit resources directly within the merge call
    dg.Definitions(
        resources={
            "dlt": DagsterDltResource(),
            # Add other explicit resources here if needed
            # e.g., "loops_session_token": EnvVar("LOOPS_SESSION_TOKEN"),
        }
    )
)

# Removed intermediate variable assignments for loaded_defs and explicit_resources
