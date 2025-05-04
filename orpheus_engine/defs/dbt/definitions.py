import os
import sys
import subprocess
from pathlib import Path
import tempfile
import yaml
import json
import importlib.util

from dagster import Definitions, asset, AssetKey

# Define the paths to the dbt project and profiles
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "..", "orpheus_engine_dbt").resolve()
DBT_PROFILE_NAME = "dagster_managed_profile"
DBT_TARGET_NAME = "dagster_prod"

# Create a function to parse the warehouse URL
def parse_postgres_url(db_url: str) -> dict:
    from urllib.parse import urlparse
    parsed = urlparse(db_url)
    return {
        "type": "postgres",
        "host": parsed.hostname,
        "port": parsed.port,
        "user": parsed.username,
        "password": parsed.password,
        "dbname": parsed.path[1:] if parsed.path else None,
        "schema": "public",
    }

# Create a function to create the profiles.yml file
def create_profiles_dir():
    # Create a temporary directory to store the profiles
    tmp_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
    
    # Construct the profiles content using the warehouse URL
    warehouse_url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is required")
    
    profiles_content = {
        DBT_PROFILE_NAME: {
            "target": DBT_TARGET_NAME,
            "outputs": {
                DBT_TARGET_NAME: parse_postgres_url(warehouse_url)
            }
        }
    }
    
    # Write the profiles content to a profiles.yml file
    profiles_path = os.path.join(tmp_dir, "profiles.yml")
    with open(profiles_path, "w") as f:
        yaml.dump(profiles_content, f)
    
    return tmp_dir

# Create a simple implementation of a dbt asset using direct SQL execution
@asset(
    name="daily_activity",
    key_prefix=["dbt", "hackatime_analytics"],
    deps=[AssetKey(["postgres", "hackatime", "heartbeats"])],
    compute_kind="dbt",
    description="Daily activity metrics from Hackatime data"
)
def daily_activity_asset(context):
    """Transform Hackatime heartbeats into daily activity metrics using SQL from dbt model."""
    context.log.info(f"Running dbt model for daily activity metrics")
    
    # Get the path to the dbt SQL file
    sql_file_path = os.path.join(
        DBT_PROJECT_DIR, 
        "models", 
        "hackatime_analytics", 
        "daily_activity.sql"
    )
    
    # Read the SQL content from the file
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Parse out the actual SQL (removing dbt config section and macros)
    # This is a simplified approach - a real implementation would use dbt's parsing
    context.log.info(f"Extracted SQL from dbt model file")
    
    # Get warehouse connection details directly
    warehouse_url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is required")
    
    context.log.info(f"Connecting to warehouse database")
    
    # Execute the SQL directly against the database
    # We'll use psycopg2 since we're targeting Postgres
    try:
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        
        # Parse the URL to get connection details
        conn_details = parse_postgres_url(warehouse_url)
        
        # Connect to the database
        conn = psycopg2.connect(
            host=conn_details["host"],
            port=conn_details["port"],
            user=conn_details["user"],
            password=conn_details["password"],
            dbname=conn_details["dbname"]
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # Create the target schema if it doesn't exist
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS hackatime_analytics")
        
        # Execute the modified SQL from the dbt model
        # This is a simplified version - in real implementation we'd need to parse
        # and replace dbt macros, especially the source() references
        context.log.info("Creating hackatime_analytics.daily_activity table")
        with conn.cursor() as cur:
            # A simplified SQL query based on the dbt model, but directly using table names
            # instead of dbt macros
            sql = """
            -- Create or replace the daily_activity table
            DROP TABLE IF EXISTS hackatime_analytics.daily_activity;
            CREATE TABLE hackatime_analytics.daily_activity AS
            
            WITH params AS (
                SELECT
                    120::int            AS hb_timeout,          -- 2‑minute cap
                    'America/New_York'  AS tz                   -- reporting zone
            ),
            
            -- Normalize time to seconds & apply scopes
            clean_hb AS (
                SELECT
                    hb.*,
                    CASE
                        WHEN hb.time > 32503680000 * 1000  THEN hb.time / 1000000   -- µs → s
                        WHEN hb.time > 32503680000         THEN hb.time / 1000      -- ms → s
                        ELSE                                    hb.time             -- already s
                    END::double precision AS ts_sec
                -- Direct reference to source table instead of using dbt source() macro
                FROM hackatime.heartbeats hb
                WHERE hb.category = 'coding'
            ),
            
            valid_hb AS (
                SELECT *
                FROM clean_hb
                WHERE ts_sec BETWEEN 0 AND 253402300799
            ),
            
            -- Per‑day capped seconds
            hb_daily AS (
                SELECT
                    user_id,
                    (to_timestamp(ts_sec) AT TIME ZONE p.tz)::date AS activity_date,
                    SUM( LEAST(ts_sec - COALESCE(prev_ts, ts_sec), p.hb_timeout) ) AS seconds_day
                FROM (
                    SELECT
                        user_id,
                        ts_sec,
                        LAG(ts_sec) OVER (PARTITION BY user_id ORDER BY ts_sec) AS prev_ts
                    FROM valid_hb
                ) x
                CROSS JOIN params p
                GROUP BY user_id, activity_date
            ),
            
            -- Per‑day language list
            lang_daily AS (
                SELECT
                    user_id,
                    (to_timestamp(ts_sec) AT TIME ZONE p.tz)::date AS activity_date,
                    STRING_AGG(DISTINCT language, ', ' ORDER BY language) AS languages
                FROM valid_hb
                CROSS JOIN params p
                GROUP BY user_id, activity_date
            )
            
            -- Final projection
            SELECT
                d.user_id                                       AS hackatime_user_id,
                d.activity_date,
                ROUND(d.seconds_day::numeric / 3600, 2)         AS hackatime_hours,
                COALESCE(l.languages, '')                       AS languages_used
            FROM       hb_daily d
            LEFT JOIN  lang_daily l
                ON  l.user_id       = d.user_id
                AND  l.activity_date = d.activity_date
            ORDER BY   d.activity_date DESC,
                    d.user_id
            """
            cur.execute(sql)
        
        # Close the connection
        conn.close()
        
        context.log.info("Successfully created hackatime_analytics.daily_activity table")
        return None
    except Exception as e:
        context.log.error(f"Error executing SQL: {str(e)}")
        raise

# Define the Dagster definitions
defs = Definitions(
    assets=[daily_activity_asset],
    resources={}
) 