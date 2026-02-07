"""
Slack Analytics Assets

Fetches member and channel analytics from Slack and stores them in PostgreSQL.
Uses incremental loading - finds the most recent data and fetches forward,
or backfills from today backwards on first run.
"""

import csv
import io
import json
import os
from datetime import date, timedelta
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

from .resources import SlackAnalyticsResource, SlackAnalyticsApiError, SlackRateLimitError


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
    date_claimed TIMESTAMP WITH TIME ZONE,
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
    date_created TIMESTAMP WITH TIME ZONE,
    date_last_active TIMESTAMP WITH TIME ZONE,
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

# Fields that contain Unix timestamps and should be converted to timestamps
UNIX_TIMESTAMP_FIELDS = {"date_claimed", "date_created", "date_last_active"}

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

MEMBER_METADATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS slack.member_metadata (
    user_id TEXT PRIMARY KEY,
    team_id TEXT,
    enterprise_id TEXT,
    name TEXT,
    real_name TEXT,
    display_name TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    title TEXT,
    phone TEXT,
    tz TEXT,
    tz_label TEXT,
    tz_offset INTEGER,
    is_admin BOOLEAN,
    is_owner BOOLEAN,
    is_primary_owner BOOLEAN,
    is_restricted BOOLEAN,
    is_ultra_restricted BOOLEAN,
    is_bot BOOLEAN,
    is_app_user BOOLEAN,
    is_email_confirmed BOOLEAN,
    has_2fa BOOLEAN,
    deleted BOOLEAN,
    color TEXT,
    status_text TEXT,
    status_emoji TEXT,
    avatar_hash TEXT,
    image_original TEXT,
    profile_fields JSONB,
    updated_at TIMESTAMP WITH TIME ZONE,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

# Columns for bulk COPY (excludes profile_fields which is enriched separately)
MEMBER_METADATA_COLUMNS = [
    "user_id", "team_id", "enterprise_id", "name", "real_name", "display_name", "first_name", "last_name",
    "email", "title", "phone", "tz", "tz_label", "tz_offset",
    "is_admin", "is_owner", "is_primary_owner", "is_restricted", "is_ultra_restricted",
    "is_bot", "is_app_user", "is_email_confirmed", "has_2fa", "deleted", "color",
    "status_text", "status_emoji", "avatar_hash", "image_original", "updated_at"
]

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

AUDIT_LOGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS slack.audit_logs (
    id TEXT PRIMARY KEY,
    date_create TIMESTAMP WITH TIME ZONE NOT NULL,
    action TEXT NOT NULL,
    ip_address TEXT,
    actor JSONB,
    entity JSONB,
    context JSONB,
    details JSONB,
    app JSONB,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_date_create ON slack.audit_logs (date_create);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON slack.audit_logs (action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_ip_address ON slack.audit_logs (ip_address);
"""

_AUDIT_LOGS_SYNC_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS slack._audit_logs_sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

AUDIT_LOGS_COLUMNS = [
    "id", "date_create", "action", "ip_address", "actor", "entity", "context", "details", "app"
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


def unix_timestamp_to_iso(timestamp: int) -> Optional[str]:
    """Convert Unix timestamp to ISO 8601 string, or None if timestamp is 0 or invalid."""
    if timestamp is None or timestamp == 0:
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    except (ValueError, OSError):
        return None


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
            
            # Convert Unix timestamps to ISO 8601 strings
            if col in UNIX_TIMESTAMP_FIELDS and value is not None:
                value = unix_timestamp_to_iso(value)
            
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


def _transform_user_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a raw Slack user record to match our schema."""
    profile = record.get("profile", {})
    enterprise_user = record.get("enterprise_user", {})
    return {
        "user_id": record.get("id"),
        "team_id": record.get("team_id"),
        "enterprise_id": enterprise_user.get("enterprise_id"),
        "name": record.get("name"),
        "real_name": record.get("real_name"),
        "display_name": profile.get("display_name"),
        "first_name": profile.get("first_name"),
        "last_name": profile.get("last_name"),
        "email": profile.get("email"),
        "title": profile.get("title"),
        "phone": profile.get("phone"),
        "tz": record.get("tz"),
        "tz_label": record.get("tz_label"),
        "tz_offset": record.get("tz_offset"),
        "is_admin": record.get("is_admin"),
        "is_owner": record.get("is_owner"),
        "is_primary_owner": record.get("is_primary_owner"),
        "is_restricted": record.get("is_restricted"),
        "is_ultra_restricted": record.get("is_ultra_restricted"),
        "is_bot": record.get("is_bot"),
        "is_app_user": record.get("is_app_user"),
        "is_email_confirmed": record.get("is_email_confirmed"),
        "has_2fa": record.get("has_2fa"),
        "deleted": record.get("deleted"),
        "color": record.get("color"),
        "status_text": profile.get("status_text"),
        "status_emoji": profile.get("status_emoji"),
        "avatar_hash": profile.get("avatar_hash"),
        "image_original": profile.get("image_original"),
        "updated_at": unix_timestamp_to_iso(record.get("updated")) if record.get("updated") else None,
    }


def _upsert_user_batch(conn, records: List[Dict[str, Any]], log):
    """Upsert a batch of user records to the database."""
    if not records:
        return 0
    
    transformed = [_transform_user_record(r) for r in records]
    
    with conn.cursor() as cur:
        # Create temp staging table
        cur.execute("""
            CREATE TEMP TABLE member_metadata_staging (
                user_id TEXT,
                team_id TEXT,
                enterprise_id TEXT,
                name TEXT,
                real_name TEXT,
                display_name TEXT,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                title TEXT,
                phone TEXT,
                tz TEXT,
                tz_label TEXT,
                tz_offset INTEGER,
                is_admin BOOLEAN,
                is_owner BOOLEAN,
                is_primary_owner BOOLEAN,
                is_restricted BOOLEAN,
                is_ultra_restricted BOOLEAN,
                is_bot BOOLEAN,
                is_app_user BOOLEAN,
                is_email_confirmed BOOLEAN,
                has_2fa BOOLEAN,
                deleted BOOLEAN,
                color TEXT,
                status_text TEXT,
                status_emoji TEXT,
                avatar_hash TEXT,
                image_original TEXT,
                updated_at TIMESTAMP WITH TIME ZONE
            )
        """)
        
        # COPY to staging
        csv_data = records_to_csv(transformed, MEMBER_METADATA_COLUMNS)
        columns_str = ", ".join(MEMBER_METADATA_COLUMNS)
        cur.copy_expert(
            f"COPY member_metadata_staging ({columns_str}) FROM STDIN WITH (FORMAT csv)",
            csv_data
        )
        
        # Upsert from staging (NOT touching profile_fields)
        cur.execute("""
            INSERT INTO slack.member_metadata (
                user_id, team_id, enterprise_id, name, real_name, display_name, first_name, last_name,
                email, title, phone, tz, tz_label, tz_offset,
                is_admin, is_owner, is_primary_owner, is_restricted, is_ultra_restricted,
                is_bot, is_app_user, is_email_confirmed, has_2fa, deleted, color,
                status_text, status_emoji, avatar_hash, image_original, updated_at, fetched_at
            )
            SELECT 
                user_id, team_id, enterprise_id, name, real_name, display_name, first_name, last_name,
                email, title, phone, tz, tz_label, tz_offset,
                is_admin, is_owner, is_primary_owner, is_restricted, is_ultra_restricted,
                is_bot, is_app_user, is_email_confirmed, has_2fa, deleted, color,
                status_text, status_emoji, avatar_hash, image_original, updated_at, NOW()
            FROM member_metadata_staging
            ON CONFLICT (user_id) DO UPDATE SET
                team_id = EXCLUDED.team_id,
                enterprise_id = EXCLUDED.enterprise_id,
                name = EXCLUDED.name,
                real_name = EXCLUDED.real_name,
                display_name = EXCLUDED.display_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                title = EXCLUDED.title,
                phone = EXCLUDED.phone,
                tz = EXCLUDED.tz,
                tz_label = EXCLUDED.tz_label,
                tz_offset = EXCLUDED.tz_offset,
                is_admin = EXCLUDED.is_admin,
                is_owner = EXCLUDED.is_owner,
                is_primary_owner = EXCLUDED.is_primary_owner,
                is_restricted = EXCLUDED.is_restricted,
                is_ultra_restricted = EXCLUDED.is_ultra_restricted,
                is_bot = EXCLUDED.is_bot,
                is_app_user = EXCLUDED.is_app_user,
                is_email_confirmed = EXCLUDED.is_email_confirmed,
                has_2fa = EXCLUDED.has_2fa,
                deleted = EXCLUDED.deleted,
                color = EXCLUDED.color,
                status_text = EXCLUDED.status_text,
                status_emoji = EXCLUDED.status_emoji,
                avatar_hash = EXCLUDED.avatar_hash,
                image_original = EXCLUDED.image_original,
                updated_at = EXCLUDED.updated_at,
                fetched_at = NOW()
        """)
        upserted = cur.rowcount
        
        # Drop staging table
        cur.execute("DROP TABLE member_metadata_staging")
    
    conn.commit()
    return upserted


@asset(
    compute_kind="slack_api",
    group_name="slack",
    description="Fetches member metadata from Slack and upserts to slack.member_metadata (excludes profile_fields)"
)
def slack_member_metadata(
    context: AssetExecutionContext,
    slack_analytics: SlackAnalyticsResource,
) -> Output[None]:
    """
    Fetches metadata for all Slack members and upserts to the database.
    
    1. Lists all teams via admin.teams.list
    2. For each team, fetches users via users.list (1000/page)
    3. Upserts each page as it's fetched (interruptable)
    
    Does NOT update profile_fields column - that's handled by slack_member_metadata_enrichment.
    """
    log = context.log
    
    # Ensure table exists
    conn = get_db_connection()
    ensure_schema_exists(conn)
    with conn.cursor() as cur:
        cur.execute(MEMBER_METADATA_SCHEMA)
    conn.commit()
    conn.close()
    
    # Step 1: Get all teams
    log.info("Step 1: Listing teams via admin.teams.list...")
    teams = slack_analytics.get_teams()
    team_ids = [t["id"] for t in teams]
    log.info(f"Found {len(team_ids)} teams: {team_ids}")
    
    # Step 2: For each team, fetch and upsert users page by page
    log.info("Step 2: Fetching and upserting users from each team...")
    total_users = 0
    seen_user_ids = set()
    
    for team_idx, team_id in enumerate(team_ids):
        log.info(f"  Team {team_id} ({team_idx+1}/{len(team_ids)})...")
        page = 0
        
        for batch in slack_analytics.get_users_for_team_paginated(team_id):
            page += 1
            
            # Filter duplicates (users can be in multiple teams)
            new_users = []
            for user in batch:
                uid = user.get("id")
                if uid and uid not in seen_user_ids:
                    seen_user_ids.add(uid)
                    new_users.append(user)
            
            if new_users:
                # Fresh connection for each batch
                conn = get_db_connection()
                try:
                    upserted = _upsert_user_batch(conn, new_users, log)
                    total_users += len(new_users)
                    log.info(f"    Page {page}: {len(batch)} fetched, {len(new_users)} new, upserted {upserted}. Total: {total_users}")
                finally:
                    conn.close()
    
    log.info(f"Done! Total users upserted: {total_users}")
    
    return Output(
        value=None,
        metadata={
            "total_members": MetadataValue.int(total_users),
            "table": MetadataValue.text("slack.member_metadata"),
        }
    )


@asset(
    compute_kind="slack_api",
    group_name="slack",
    deps=["slack_member_metadata"],
    description="Enriches member metadata with profile_fields from users.profile.get API"
)
def slack_member_metadata_enrichment(
    context: AssetExecutionContext,
    slack_analytics: SlackAnalyticsResource,
) -> Output[None]:
    """
    Enriches slack.member_metadata with custom profile fields.
    
    Iterates through all users and calls users.profile.get to fetch their
    custom profile fields, updating each row immediately (interruptable).
    
    Profile fields are stored with human-readable labels instead of Slack field IDs.
    """
    import json
    import time
    
    log = context.log
    
    # Step 1: Get field ID -> label mapping from team.profile.get
    log.info("Fetching profile field definitions...")
    field_id_to_label = slack_analytics.get_profile_field_definitions()
    log.info(f"Found {len(field_id_to_label)} profile field definitions")
    
    # Step 2: Get all user IDs
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM slack.member_metadata WHERE deleted = false OR deleted IS NULL")
        user_ids = [row[0] for row in cur.fetchall()]
    conn.close()
    
    log.info(f"Enriching profile fields for {len(user_ids)} users...")
    
    enriched_count = 0
    error_count = 0
    
    for i, user_id in enumerate(user_ids):
        # Retry loop for rate limiting - keep trying until success or non-rate-limit error
        while True:
            try:
                profile = slack_analytics.get_user_profile(user_id)
                raw_fields = profile.get("fields", {})
                
                # Only update if there are fields
                if raw_fields:
                    # Convert field IDs to labels
                    labeled_fields = {}
                    for field_id, field_data in raw_fields.items():
                        label = field_id_to_label.get(field_id, field_id)
                        labeled_fields[label] = field_data.get("value", "")
                    
                    # Fresh connection for each update
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE slack.member_metadata SET profile_fields = %s WHERE user_id = %s",
                                (json.dumps(labeled_fields), user_id)
                            )
                        conn.commit()
                        enriched_count += 1
                        log.info(f"Enriched user {user_id} with {len(labeled_fields)} fields: {list(labeled_fields.keys())}")
                    finally:
                        conn.close()
                
                break  # Success, exit retry loop
                
            except SlackRateLimitError as e:
                log.warning(f"Rate limited, sleeping {e.retry_after}s before retrying user {user_id}...")
                time.sleep(e.retry_after)
                # Continue the while loop to retry this user
            except SlackAnalyticsApiError as e:
                error_count += 1
                log.warning(f"Error enriching user {user_id}: {e}")
                break
            except Exception as e:
                error_count += 1
                log.warning(f"Error enriching user {user_id}: {e}")
                break
        
        # Log progress every 1000 users
        if (i + 1) % 1000 == 0:
            log.info(f"Progress: {i + 1}/{len(user_ids)} processed, {enriched_count} enriched, {error_count} errors")
    
    log.info(f"Done! {enriched_count} users enriched, {error_count} errors")
    
    return Output(
        value=None,
        metadata={
            "total_users": MetadataValue.int(len(user_ids)),
            "enriched_count": MetadataValue.int(enriched_count),
            "error_count": MetadataValue.int(error_count),
            "table": MetadataValue.text("slack.member_metadata"),
        }
    )


def _json_or_none(value) -> Optional[str]:
    """Serialize a value to JSON string, or None if the value is None."""
    return json.dumps(value) if value is not None else None


def _transform_audit_log_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a raw Slack audit log entry to match our schema."""
    date_create = entry.get("date_create")
    if date_create is not None:
        date_create = datetime.fromtimestamp(date_create, tz=timezone.utc).isoformat()

    context = entry.get("context")

    return {
        "id": entry.get("id"),
        "date_create": date_create,
        "action": entry.get("action"),
        "ip_address": context.get("ip_address") if context else None,
        "actor": _json_or_none(entry.get("actor")),
        "entity": _json_or_none(entry.get("entity")),
        "context": _json_or_none(context),
        "details": _json_or_none(entry.get("details")),
        "app": _json_or_none(entry.get("app")),
    }


def _upsert_audit_logs_batch(conn, entries: List[Dict[str, Any]], log) -> int:
    """Upsert a batch of audit log entries using a staging table. Returns rows inserted."""
    if not entries:
        return 0

    transformed = [_transform_audit_log_entry(e) for e in entries]

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE audit_logs_staging (
                id TEXT,
                date_create TIMESTAMP WITH TIME ZONE,
                action TEXT,
                ip_address TEXT,
                actor JSONB,
                entity JSONB,
                context JSONB,
                details JSONB,
                app JSONB
            ) ON COMMIT DROP
        """)

        csv_data = records_to_csv(transformed, AUDIT_LOGS_COLUMNS)
        columns_str = ", ".join(AUDIT_LOGS_COLUMNS)
        cur.copy_expert(
            f"COPY audit_logs_staging ({columns_str}) FROM STDIN WITH (FORMAT csv)",
            csv_data
        )

        cur.execute("""
            INSERT INTO slack.audit_logs (id, date_create, action, ip_address, actor, entity, context, details, app, fetched_at)
            SELECT id, date_create, action, ip_address, actor, entity, context, details, app, NOW()
            FROM audit_logs_staging
            ON CONFLICT (id) DO NOTHING
        """)
        inserted = cur.rowcount

    conn.commit()
    return inserted


def _get_backfill_complete(conn) -> bool:
    """Check if audit logs backfill has completed."""
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT value FROM slack._audit_logs_sync_state WHERE key = 'backfill_complete'")
            row = cur.fetchone()
            return row is not None and row[0] == "true"
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            return False


def _set_backfill_complete(conn, complete: bool):
    """Set the backfill_complete flag in sync state."""
    value = "true" if complete else "false"
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO slack._audit_logs_sync_state (key, value, updated_at)
            VALUES ('backfill_complete', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (value,))
    conn.commit()


def _get_high_water(conn) -> Optional[int]:
    """Get the max date_create as a Unix timestamp, or None if table is empty."""
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT EXTRACT(EPOCH FROM MAX(date_create))::bigint FROM slack.audit_logs")
            row = cur.fetchone()
            return row[0] if row and row[0] else None
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            return None


@asset(
    compute_kind="slack_api",
    group_name="slack",
    description="Fetches Slack Audit Logs (Enterprise Grid) and stores in slack.audit_logs with incremental sync"
)
def slack_audit_logs(
    context: AssetExecutionContext,
    slack_analytics: SlackAnalyticsResource,
) -> Output[None]:
    """
    Incrementally fetches Slack Audit Logs and upserts to the database.

    - Initial backfill: pages through all history (newest first), resumable on interruption
    - Incremental: fetches only events newer than the high-water mark
    - Uses ON CONFLICT DO NOTHING to avoid duplicates
    """
    log = context.log

    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        with conn.cursor() as cur:
            cur.execute(AUDIT_LOGS_SCHEMA)
            cur.execute(_AUDIT_LOGS_SYNC_STATE_SCHEMA)
        conn.commit()

        backfill_complete = _get_backfill_complete(conn)
        total_inserted = 0
        total_fetched = 0
        pages = 0

        if backfill_complete:
            high_water = _get_high_water(conn)
            if high_water:
                log.info(f"Incremental mode: fetching events after {datetime.fromtimestamp(high_water, tz=timezone.utc)}")
            else:
                log.info("Incremental mode but table is empty, fetching all events")
        else:
            high_water = None
            log.info("Backfill mode: fetching all audit log history...")

        cursor = None
        while True:
            entries, next_cursor = slack_analytics.get_audit_logs_page(
                oldest=high_water,
                cursor=cursor,
            )
            pages += 1
            total_fetched += len(entries)

            if entries:
                inserted = _upsert_audit_logs_batch(conn, entries, log)
                total_inserted += inserted
                log.info(f"Page {pages}: fetched {len(entries)}, inserted {inserted} new. Total: {total_inserted}")

            if not next_cursor:
                if not backfill_complete:
                    _set_backfill_complete(conn, True)
                    log.info("Backfill complete!")
                break
            cursor = next_cursor

        log.info(f"Done! Fetched {total_fetched} events across {pages} pages, inserted {total_inserted} new")

        return Output(
            value=None,
            metadata={
                "total_fetched": MetadataValue.int(total_fetched),
                "total_inserted": MetadataValue.int(total_inserted),
                "pages": MetadataValue.int(pages),
                "table": MetadataValue.text("slack.audit_logs"),
            }
        )
    finally:
        conn.close()


# Create the resource instance
slack_analytics_resource = SlackAnalyticsResource()

defs = Definitions(
    assets=[
        slack_public_channel_analytics,
        slack_member_analytics,
        slack_public_channel_metadata,
        slack_member_metadata,
        slack_member_metadata_enrichment,
        slack_audit_logs,
    ],
    resources={
        "slack_analytics": slack_analytics_resource,
    },
)

