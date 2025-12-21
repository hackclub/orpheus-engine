"""
Slack Analytics Assets

Fetches member and channel analytics from Slack and stores them in PostgreSQL.
Uses incremental loading - finds the most recent data and fetches forward,
or backfills from today backwards on first run.
"""

import csv
import io
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import psycopg2
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

from .resources import SlackAnalyticsResource, SlackAnalyticsApiError


# Schema definitions for table creation
MEMBER_ANALYTICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS slack.member_analytics (
    enterprise_id TEXT,
    team_id TEXT,
    date DATE NOT NULL,
    user_id TEXT NOT NULL,
    email_address TEXT,
    enterprise_employee_number TEXT,
    is_guest BOOLEAN,
    is_billable_seat BOOLEAN,
    is_active BOOLEAN,
    is_active_ios BOOLEAN,
    is_active_android BOOLEAN,
    is_active_desktop BOOLEAN,
    reactions_added_count INTEGER,
    messages_posted_count INTEGER,
    channel_messages_posted_count INTEGER,
    files_added_count INTEGER,
    is_active_apps BOOLEAN,
    is_active_workflows BOOLEAN,
    is_active_slack_connect BOOLEAN,
    total_calls_count INTEGER,
    slack_calls_count INTEGER,
    slack_huddles_count INTEGER,
    search_count INTEGER,
    date_claimed BIGINT,
    PRIMARY KEY (date, user_id)
);
"""

CHANNEL_ANALYTICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS slack.public_channel_analytics (
    enterprise_id TEXT,
    team_id TEXT,
    date DATE NOT NULL,
    channel_id TEXT NOT NULL,
    originating_team JSONB,
    date_created BIGINT,
    date_last_active BIGINT,
    total_members_count INTEGER,
    full_members_count INTEGER,
    guest_member_count INTEGER,
    messages_posted_count INTEGER,
    messages_posted_by_members_count INTEGER,
    members_who_viewed_count INTEGER,
    members_who_posted_count INTEGER,
    reactions_added_count INTEGER,
    visibility TEXT,
    channel_type TEXT,
    is_shared_externally BOOLEAN,
    shared_with JSONB,
    externally_shared_with_organizations JSONB,
    PRIMARY KEY (date, channel_id)
);
"""

CHANNEL_METADATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS slack.public_channel_metadata (
    channel_id TEXT PRIMARY KEY,
    name TEXT,
    topic TEXT,
    description TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

CHANNEL_METADATA_COLUMNS = ["channel_id", "name", "topic", "description"]

# Column order for COPY operations
MEMBER_ANALYTICS_COLUMNS = [
    "enterprise_id", "team_id", "date", "user_id", "email_address",
    "enterprise_employee_number", "is_guest", "is_billable_seat", "is_active",
    "is_active_ios", "is_active_android", "is_active_desktop",
    "reactions_added_count", "messages_posted_count", "channel_messages_posted_count",
    "files_added_count", "is_active_apps", "is_active_workflows",
    "is_active_slack_connect", "total_calls_count", "slack_calls_count",
    "slack_huddles_count", "search_count", "date_claimed"
]

CHANNEL_ANALYTICS_COLUMNS = [
    "enterprise_id", "team_id", "date", "channel_id", "originating_team",
    "date_created", "date_last_active", "total_members_count", "full_members_count",
    "guest_member_count", "messages_posted_count", "messages_posted_by_members_count",
    "members_who_viewed_count", "members_who_posted_count", "reactions_added_count",
    "visibility", "channel_type", "is_shared_externally", "shared_with",
    "externally_shared_with_organizations"
]


def get_db_connection():
    """Get a connection to the warehouse database."""
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def ensure_schema_exists(conn):
    """Ensure the slack schema exists."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS slack;")
    conn.commit()


def get_max_date(conn, table_name: str) -> Optional[date]:
    """Get the maximum date from a table, or None if table doesn't exist or is empty."""
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT MAX(date) FROM {table_name};")
            result = cur.fetchone()
            return result[0] if result and result[0] else None
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            return None


def get_min_date(conn, table_name: str) -> Optional[date]:
    """Get the minimum date from a table, or None if table doesn't exist or is empty."""
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT MIN(date) FROM {table_name};")
            result = cur.fetchone()
            return result[0] if result and result[0] else None
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            return None


def normalize_email(email: str) -> str:
    """Normalize email address by lowercasing and stripping whitespace."""
    if email is None:
        return None
    return email.lower().strip()


def records_to_csv(
    records: List[Dict[str, Any]], 
    columns: List[str],
    normalize_email_field: bool = False
) -> io.StringIO:
    """Convert records to CSV format for COPY."""
    import json
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    for record in records:
        row = []
        for col in columns:
            value = record.get(col)
            # Normalize email addresses if requested
            if normalize_email_field and col == "email_address" and value is not None:
                value = normalize_email(value)
            
            if value is None:
                row.append('')
            elif isinstance(value, (dict, list)):
                row.append(json.dumps(value))
            elif isinstance(value, bool):
                row.append('t' if value else 'f')
            else:
                row.append(str(value))
        writer.writerow(row)
    
    output.seek(0)
    return output


def copy_records_to_table(
    conn,
    table_name: str,
    records: List[Dict[str, Any]],
    columns: List[str],
    target_date: date,
    log,
    normalize_email_field: bool = False
):
    """Delete existing data for the date and COPY new records."""
    with conn.cursor() as cur:
        # Delete existing data for this date
        cur.execute(f"DELETE FROM {table_name} WHERE date = %s;", (target_date,))
        deleted = cur.rowcount
        if deleted > 0:
            log.info(f"Deleted {deleted} existing records for {target_date}")
        
        # COPY new data
        csv_data = records_to_csv(records, columns, normalize_email_field=normalize_email_field)
        columns_str = ", ".join(columns)
        cur.copy_expert(
            f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT csv)",
            csv_data
        )
        log.info(f"Copied {len(records)} records for {target_date}")
    
    conn.commit()


@asset(
    compute_kind="slack_api",
    group_name="slack",
    description="Fetches public channel analytics from Slack and stores in slack.public_channel_analytics"
)
def slack_public_channel_analytics(
    context: AssetExecutionContext,
    slack_analytics: SlackAnalyticsResource,
) -> Output[None]:
    """
    Incrementally fetches Slack public channel analytics.
    
    - If data exists: fetches from max_date + 1 forward to today
    - If no data: backfills from today backwards until file_not_found
    """
    log = context.log
    
    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        
        # Create table if needed
        with conn.cursor() as cur:
            cur.execute(CHANNEL_ANALYTICS_SCHEMA)
        conn.commit()
        
        max_date = get_max_date(conn, "slack.public_channel_analytics")
        min_date = get_min_date(conn, "slack.public_channel_analytics")
        
        total_records = 0
        days_processed = 0
        
        if max_date:
            # Forward fill: fetch from max_date + 1 to today
            log.info(f"Found existing data up to {max_date}, fetching newer data...")
            
            current_date = max_date + timedelta(days=1)
            today = date.today()
            
            while current_date <= today:
                log.info(f"Fetching channel analytics for {current_date}...")
                try:
                    records = slack_analytics.get_channel_analytics(current_date)
                    copy_records_to_table(
                        conn,
                        "slack.public_channel_analytics",
                        records,
                        CHANNEL_ANALYTICS_COLUMNS,
                        current_date,
                        log
                    )
                    total_records += len(records)
                    days_processed += 1
                    current_date += timedelta(days=1)
                except SlackAnalyticsApiError as e:
                    if "file_not_yet_available" in str(e) or "file_not_found" in str(e):
                        log.info(f"Data not available for {current_date}, stopping forward fill")
                        break
                    raise
        else:
            # Initial backfill: start from today, go backwards
            log.info("No existing data found, starting backfill from today...")
            
            current_date = date.today()
            found_first_success = False
            
            while True:
                log.info(f"Fetching channel analytics for {current_date}...")
                try:
                    records = slack_analytics.get_channel_analytics(current_date)
                    found_first_success = True
                    copy_records_to_table(
                        conn,
                        "slack.public_channel_analytics",
                        records,
                        CHANNEL_ANALYTICS_COLUMNS,
                        current_date,
                        log
                    )
                    total_records += len(records)
                    days_processed += 1
                    current_date -= timedelta(days=1)
                except SlackAnalyticsApiError as e:
                    error_str = str(e)
                    if "file_not_found" in error_str:
                        if found_first_success:
                            log.info(f"Reached end of available data at {current_date}")
                            break
                        else:
                            log.info(f"Data not available for {current_date}, trying earlier...")
                            current_date -= timedelta(days=1)
                            continue
                    elif "file_not_yet_available" in error_str:
                        log.info(f"Data not yet available for {current_date}, trying earlier...")
                        current_date -= timedelta(days=1)
                        continue
                    raise
        
        return Output(
            value=None,
            metadata={
                "total_records": MetadataValue.int(total_records),
                "days_processed": MetadataValue.int(days_processed),
                "table": MetadataValue.text("slack.public_channel_analytics"),
            }
        )
    finally:
        conn.close()


@asset(
    compute_kind="slack_api",
    group_name="slack",
    description="Fetches member analytics from Slack and stores in slack.member_analytics"
)
def slack_member_analytics(
    context: AssetExecutionContext,
    slack_analytics: SlackAnalyticsResource,
) -> Output[None]:
    """
    Incrementally fetches Slack member analytics.
    
    - If data exists: fetches from max_date + 1 forward to today
    - If no data: backfills from today backwards until file_not_found
    
    Note: Requires the org's "Display email addresses" setting to be enabled.
    """
    log = context.log
    
    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        
        # Create table if needed
        with conn.cursor() as cur:
            cur.execute(MEMBER_ANALYTICS_SCHEMA)
        conn.commit()
        
        max_date = get_max_date(conn, "slack.member_analytics")
        min_date = get_min_date(conn, "slack.member_analytics")
        
        total_records = 0
        days_processed = 0
        
        if max_date:
            # Forward fill: fetch from max_date + 1 to today
            log.info(f"Found existing data up to {max_date}, fetching newer data...")
            
            current_date = max_date + timedelta(days=1)
            today = date.today()
            
            while current_date <= today:
                log.info(f"Fetching member analytics for {current_date}...")
                try:
                    records = slack_analytics.get_member_analytics(current_date)
                    copy_records_to_table(
                        conn,
                        "slack.member_analytics",
                        records,
                        MEMBER_ANALYTICS_COLUMNS,
                        current_date,
                        log,
                        normalize_email_field=True
                    )
                    total_records += len(records)
                    days_processed += 1
                    current_date += timedelta(days=1)
                except SlackAnalyticsApiError as e:
                    if "file_not_yet_available" in str(e) or "file_not_found" in str(e):
                        log.info(f"Data not available for {current_date}, stopping forward fill")
                        break
                    raise
        else:
            # Initial backfill: start from today, go backwards
            log.info("No existing data found, starting backfill from today...")
            
            current_date = date.today()
            found_first_success = False
            
            while True:
                log.info(f"Fetching member analytics for {current_date}...")
                try:
                    records = slack_analytics.get_member_analytics(current_date)
                    found_first_success = True
                    copy_records_to_table(
                        conn,
                        "slack.member_analytics",
                        records,
                        MEMBER_ANALYTICS_COLUMNS,
                        current_date,
                        log,
                        normalize_email_field=True
                    )
                    total_records += len(records)
                    days_processed += 1
                    current_date -= timedelta(days=1)
                except SlackAnalyticsApiError as e:
                    error_str = str(e)
                    if "file_not_found" in error_str:
                        if found_first_success:
                            log.info(f"Reached end of available data at {current_date}")
                            break
                        else:
                            log.info(f"Data not available for {current_date}, trying earlier...")
                            current_date -= timedelta(days=1)
                            continue
                    elif "file_not_yet_available" in error_str:
                        log.info(f"Data not yet available for {current_date}, trying earlier...")
                        current_date -= timedelta(days=1)
                        continue
                    raise
        
        return Output(
            value=None,
            metadata={
                "total_records": MetadataValue.int(total_records),
                "days_processed": MetadataValue.int(days_processed),
                "table": MetadataValue.text("slack.member_analytics"),
            }
        )
    finally:
        conn.close()


@asset(
    compute_kind="slack_api",
    group_name="slack",
    description="Fetches channel metadata (names, topics, descriptions) from Slack and upserts to slack.public_channel_metadata"
)
def slack_public_channel_metadata(
    context: AssetExecutionContext,
    slack_analytics: SlackAnalyticsResource,
) -> Output[None]:
    """
    Fetches metadata for all public Slack channels and upserts to the database.
    
    This includes channel names, topics, and descriptions - useful for joining
    with analytics data which only contains channel IDs.
    
    Uses bulk COPY to a staging table, then upserts to the main table.
    """
    log = context.log
    
    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        
        # Create main table if needed
        with conn.cursor() as cur:
            cur.execute(CHANNEL_METADATA_SCHEMA)
        conn.commit()
        
        log.info("Fetching channel metadata from Slack API...")
        records = slack_analytics.get_channel_metadata()
        log.info(f"Retrieved metadata for {len(records)} channels")
        
        if not records:
            log.warning("No channel metadata returned from API")
            return Output(
                value=None,
                metadata={
                    "total_channels": MetadataValue.int(0),
                    "table": MetadataValue.text("slack.public_channel_metadata"),
                }
            )
        
        with conn.cursor() as cur:
            # Create temp staging table
            cur.execute("""
                CREATE TEMP TABLE channel_metadata_staging (
                    channel_id TEXT,
                    name TEXT,
                    topic TEXT,
                    description TEXT
                ) ON COMMIT DROP
            """)
            
            # Bulk COPY to staging
            log.info("Bulk loading to staging table...")
            csv_data = records_to_csv(records, CHANNEL_METADATA_COLUMNS)
            cur.copy_expert(
                "COPY channel_metadata_staging (channel_id, name, topic, description) FROM STDIN WITH (FORMAT csv)",
                csv_data
            )
            log.info(f"Loaded {len(records)} records to staging")
            
            # Upsert from staging to main table
            log.info("Upserting to main table...")
            cur.execute("""
                INSERT INTO slack.public_channel_metadata (channel_id, name, topic, description, updated_at)
                SELECT channel_id, name, topic, description, NOW()
                FROM channel_metadata_staging
                ON CONFLICT (channel_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    topic = EXCLUDED.topic,
                    description = EXCLUDED.description,
                    updated_at = NOW()
            """)
            upserted = cur.rowcount
        
        conn.commit()
        log.info(f"Upserted {upserted} channel metadata records")
        
        return Output(
            value=None,
            metadata={
                "total_channels": MetadataValue.int(len(records)),
                "table": MetadataValue.text("slack.public_channel_metadata"),
            }
        )
    finally:
        conn.close()


# Create the resource instance
slack_analytics_resource = SlackAnalyticsResource()

defs = Definitions(
    assets=[slack_public_channel_analytics, slack_member_analytics, slack_public_channel_metadata],
    resources={
        "slack_analytics": slack_analytics_resource,
    },
)

