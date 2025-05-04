import os
import yaml
import tempfile
import logging
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional

from dagster import Definitions, AssetExecutionContext, EnvVar
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator

# --- Configuration ---
# Assume the dbt project is in a directory named 'orpheus_engine_dbt'
# located four levels up from this file's directory. Adjust if needed.
DBT_ROOT_DIR = Path(__file__).joinpath("..", "..", "..", "..").resolve()
DBT_PROJECT_DIR = DBT_ROOT_DIR / "orpheus_engine_dbt"
DBT_PROFILE_NAME = "orpheus_engine_dbt" # Should match profile name in generated profiles.yml
DBT_TARGET_NAME = "prod"                # The target name within the profile

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def parse_postgres_url(db_url: str) -> dict:
    """Parses a PostgreSQL URL into dbt profile components."""
    if not db_url:
        raise ValueError("Database URL is empty or not provided.")
    parsed = urlparse(db_url)
    if parsed.scheme not in ["postgres", "postgresql"]:
        raise ValueError(f"Invalid URL scheme: {parsed.scheme}. Expected 'postgresql'.")

    # Basic validation for essential parts
    if not parsed.username:
        logger.warning("Database URL is missing username.")
    if not parsed.password:
        logger.warning("Database URL is missing password.") # Be careful logging this
    if not parsed.path or parsed.path == "/":
         raise ValueError("Database URL is missing the database name path (e.g., '/mydatabase').")

    return {
        "type": "postgres",
        "host": parsed.hostname or "localhost", # Provide default if missing
        "port": parsed.port or 5432,           # Provide default if missing
        "user": parsed.username,
        "pass": parsed.password,               # Use 'pass' for dbt profiles
        "dbname": parsed.path[1:],             # Extract dbname (remove leading '/')
        "schema": "public",                    # Default schema, dbt models can override
        "connect_timeout": 15,                 # Increased default timeout slightly
        # "keepalives_idle": 60, # Optional: configure keepalives if needed
        "threads": 4, # Sensible default, can be overridden
    }

def create_dbt_profiles_yaml(target_name: str, profile_name: str, db_config: dict) -> str:
    """Creates the YAML content string for profiles.yml."""
    profiles_content = {
        profile_name: {                 # Top level key is the profile name
            "target": target_name,      # Default target
            "outputs": {
                target_name: db_config  # Target config under 'outputs'
            }
        }
    }
    return yaml.dump(profiles_content, default_flow_style=False)

# Global variable to store the temporary directory path, generated once
# We use a function scoped variable trick to ensure it runs only once.
_temp_profiles_dir = None

def get_or_create_temporary_profiles_dir() -> Optional[str]:
    """
    Lazily creates a temporary directory containing profiles.yml based on
    the WAREHOUSE_COOLIFY_URL environment variable. Returns the directory path.

    Returns None if the environment variable is missing or invalid.
    Caches the result to avoid recreation on subsequent calls within the same process.
    """
    global _temp_profiles_dir
    if _temp_profiles_dir is not None:
        # Return cached path if already created (or None if creation failed before)
        # Add a check if the directory still exists, though cleanup is OS/user dependent
        if _temp_profiles_dir is False or (_temp_profiles_dir and not os.path.exists(_temp_profiles_dir)):
             logger.warning(f"Temporary profiles directory {_temp_profiles_dir} no longer exists. Re-creation attempt depends on code location reload.")
             # Mark as failed or allow potential recreation if logic permits
             _temp_profiles_dir = False # Mark as unusable
             return None
        return _temp_profiles_dir if _temp_profiles_dir else None


    warehouse_url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        logger.error(
            "WAREHOUSE_COOLIFY_URL environment variable is not set. "
            "Cannot configure dbt connection. Skipping dbt setup."
        )
        _temp_profiles_dir = False # Mark as failed permanently for this run
        return None

    # Clean up the URL - remove any newlines or extra whitespace
    warehouse_url = warehouse_url.replace('\n', '').replace('\r', '').strip()
    
    try:
        logger.info("Attempting to parse WAREHOUSE_COOLIFY_URL...")
        db_config = parse_postgres_url(warehouse_url)
        logger.info("Successfully parsed database URL.")

        profiles_yaml_content = create_dbt_profiles_yaml(
            target_name=DBT_TARGET_NAME,
            profile_name=DBT_PROFILE_NAME,
            db_config=db_config
        )

        # Create a temporary directory managed by tempfile
        # Note: This directory might be cleaned up depending on OS and process lifecycle.
        # For long-running Dagster deployments (like dagster-daemon or webserver),
        # relying on temp dirs created at startup can be fragile.
        # Consider alternative configuration methods if this becomes an issue.
        tmp_dir = tempfile.mkdtemp(prefix="dagster_dbt_profiles_")
        profiles_path = os.path.join(tmp_dir, "profiles.yml")

        # Write the profiles content to profiles.yml
        with open(profiles_path, "w") as f:
            f.write(profiles_yaml_content)

        logger.info(f"Generated temporary dbt profiles.yml at: {profiles_path}")
        # logger.debug(f"Profiles content:\n{profiles_yaml_content}") # Avoid logging secrets

        _temp_profiles_dir = tmp_dir # Cache the successful path
        return _temp_profiles_dir

    except ValueError as e:
        logger.error(f"ERROR: Failed to configure dbt profile: {e}")
        _temp_profiles_dir = False # Mark as failed permanently for this run
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during profile creation: {e}", exc_info=True)
        _temp_profiles_dir = False
        return None


# --- Create Resources and Assets ---

# Attempt to get/create the profiles directory path
# This will only run the creation logic once per process load.
DBT_PROFILES_DIR_PATH = get_or_create_temporary_profiles_dir()

# Define the DbtProject helper
# It helps manage paths and can generate the manifest in development
# Ensure the project_dir points to your actual dbt project location
try:
    # First set up a DbtCliResource for parsing/manifest generation
    dbt_cli_for_parsing = DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=DBT_PROFILES_DIR_PATH,
        profile=DBT_PROFILE_NAME,
        target=DBT_TARGET_NAME,
    ) if DBT_PROFILES_DIR_PATH else None
    
    # Only create the DbtProject if we have a valid profiles directory
    if dbt_cli_for_parsing:
        dbt_project = DbtProject(
            project_dir=DBT_PROJECT_DIR,
            # The DbtProject needs a profiles_dir for prepare_if_dev to work properly
            profiles_dir=DBT_PROFILES_DIR_PATH,
        )
        
        # prepare manifest.json during runtime. note: we may need to change this
        # to preparing at build time for prod if we scale beyond a single node
        # setup
        dbt_project.preparer.prepare(dbt_project)
        DBT_MANIFEST_PATH = dbt_project.manifest_path
        logger.info(f"Using dbt manifest path: {DBT_MANIFEST_PATH}")
        dbt_project_valid = True
    else:
        logger.warning("Skipping DbtProject initialization due to missing profiles directory")
        dbt_project = None
        DBT_MANIFEST_PATH = None
        dbt_project_valid = False
except Exception as e:
    logger.error(f"Failed to initialize DbtProject: {e}", exc_info=True)
    DBT_MANIFEST_PATH = None # Indicate failure
    dbt_project_valid = False
    dbt_project = None


# Define the dbt assets using the @dbt_assets decorator
# Only define assets if the profile and project setup were successful
if DBT_PROFILES_DIR_PATH and dbt_project_valid and DBT_MANIFEST_PATH:
    @dbt_assets(
        manifest=DBT_MANIFEST_PATH,
        # dagster_dbt_translator=DagsterDbtTranslator(), # Optional: Customize asset keys, groups etc.
        # select="tag:my_tag", # Optional: Load only a subset of assets
        # exclude="config.materialized:ephemeral", # Optional: Exclude certain models
    )
    def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        """
        Loads dbt models defined in the project manifest as Dagster assets.
        Executes `dbt build` for the selected assets.
        """
        # The DbtCliResource `dbt` will use the profile and target configured below.
        # `dbt.cli` smartly passes selection context to dbt.
        yield from dbt.cli(["build"], context=context).stream()
        # You can add more commands or metadata fetching:
        # .fetch_row_counts()
        # .fetch_column_metadata()

    # Define the DbtCliResource
    # It needs the path to the temporary profiles directory created earlier.
    dbt_resource = DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR), # Pass the project dir as string
        profiles_dir=DBT_PROFILES_DIR_PATH, # Use the dynamically generated profiles dir path
        profile=DBT_PROFILE_NAME,           # Explicitly set profile name
        target=DBT_TARGET_NAME,             # Explicitly set target name
        # global_config_flags=["--log-level", "debug"] # Optional: Add global dbt flags
    )

    dagster_assets = [my_dbt_assets]
    dagster_resources = {"dbt": dbt_resource}

else:
    logger.warning(
        "Skipping dbt asset definition due to profile generation "
        "or dbt project initialization failure. Check logs above."
    )
    # Define empty lists/dicts if setup failed, so Dagster can still load
    dagster_assets = []
    dagster_resources = {}


# --- Define Dagster Definitions ---
defs = Definitions(
    assets=dagster_assets,
    resources=dagster_resources,
    # Add schedules or sensors if needed
    # schedules=[ScheduleDefinition(...)],
)

# Example of defining a job and schedule (optional)
# if dagster_assets:
#     dbt_basic_job = define_asset_job("dbt_basic_job", selection=dagster_assets)
#     dbt_daily_schedule = ScheduleDefinition(
#         job=dbt_basic_job,
#         cron_schedule="@daily",
#         job_name="daily_dbt_job"
#     )
#
#     defs = Definitions(
#         assets=dagster_assets,
#         resources=dagster_resources,
#         schedules=[dbt_daily_schedule],
#         jobs=[dbt_basic_job]
#     ) 