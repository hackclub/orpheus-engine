"""
Slack Users Sync Asset

Syncs Slack user metadata from the PostgreSQL warehouse (slack.member_metadata)
to Airtable. Runs after the slack_member_metadata asset completes.

Uses a smart diff approach:
1. Fetch existing Airtable records
2. Fetch source data from PostgreSQL
3. Compute sync plan
4. Delete stale + duplicate records
5. Update changed records
6. Create new records
"""

import os
import time
from datetime import datetime, timezone
from itertools import chain
from typing import Any, Callable, Dict, List, Tuple

import psycopg2
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

from ..airtable.resources import AirtableResource
from ..airtable.generated_ids import AirtableIDs

# Alias for convenience
SlackUsersTable = AirtableIDs.synced_data_warehouse_slack_users.slack_users

# Record ID for the sync stats linked record
SYNC_STATS_RECORD_ID = "recGe0Ta2b3zskKn0"

# Fields to compare for diff (excludes last_synced_at which changes every run,
# and slack_id which is the match key)
DIFF_FIELDS = {
    SlackUsersTable.email,
    SlackUsersTable.display_name,
    SlackUsersTable.profile_real_name,
    SlackUsersTable.timezone,
    SlackUsersTable.avatar_url,
    SlackUsersTable.user_type,
    SlackUsersTable.is_bot,
    SlackUsersTable.is_guest,
    SlackUsersTable.is_admin,
    SlackUsersTable.is_owner,
    SlackUsersTable.has_signed_in,
    SlackUsersTable.is_active,
    SlackUsersTable.first_sign_in_at,
}


def get_db_connection():
    """Get a connection to the warehouse database."""
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def determine_user_type(pg_user: Dict[str, Any]) -> str:
    """Determine the user type based on flags."""
    if pg_user.get("is_bot"):
        return "Bot"
    if pg_user.get("is_app_user"):
        return "App"
    if pg_user.get("is_owner") or pg_user.get("is_primary_owner"):
        return "Owner"
    if pg_user.get("is_admin"):
        return "Admin"
    if pg_user.get("is_ultra_restricted"):
        return "Single-Channel Guest"
    if pg_user.get("is_restricted"):
        return "Multi-Channel Guest"
    return "Member"


def transform_pg_user_to_airtable(pg_user: Dict[str, Any], sync_time: str) -> Dict[str, Any]:
    """
    Transform a PostgreSQL user record to Airtable field format.

    Maps PostgreSQL columns to Airtable field IDs.
    """
    fields = {}

    # Required field: Slack ID (primary key for matching)
    if pg_user.get("user_id"):
        fields[SlackUsersTable.slack_id] = pg_user["user_id"]

    # Email - lowercase and strip, use placeholder for bots
    if pg_user.get("email"):
        fields[SlackUsersTable.email] = pg_user["email"].strip().lower()
    elif pg_user.get("is_bot"):
        fields[SlackUsersTable.email] = "bot-user@hackclub.com"

    # Display name
    if pg_user.get("display_name"):
        fields[SlackUsersTable.display_name] = pg_user["display_name"]

    # Real name (profile_real_name in Airtable)
    if pg_user.get("real_name"):
        fields[SlackUsersTable.profile_real_name] = pg_user["real_name"]

    # Timezone
    if pg_user.get("tz"):
        fields[SlackUsersTable.timezone] = pg_user["tz"]

    # Avatar URL (image_original in PostgreSQL)
    if pg_user.get("image_original"):
        fields[SlackUsersTable.avatar_url] = pg_user["image_original"]

    # User Type (single select)
    fields[SlackUsersTable.user_type] = determine_user_type(pg_user)

    # Boolean fields - only set if True (Airtable checkboxes default to unchecked)
    if pg_user.get("is_bot"):
        fields[SlackUsersTable.is_bot] = True

    if pg_user.get("is_restricted"):  # Maps to is_guest in Airtable
        fields[SlackUsersTable.is_guest] = True

    if pg_user.get("is_admin"):
        fields[SlackUsersTable.is_admin] = True

    if pg_user.get("is_owner") or pg_user.get("is_primary_owner"):
        fields[SlackUsersTable.is_owner] = True

    # Has Signed In - true if date_claimed exists
    if pg_user.get("date_claimed"):
        fields[SlackUsersTable.has_signed_in] = True

    # Is Active - from latest analytics
    if pg_user.get("is_active"):
        fields[SlackUsersTable.is_active] = True

    # First Sign In At - from date_claimed in member_analytics
    if pg_user.get("date_claimed"):
        date_claimed = pg_user["date_claimed"]
        if isinstance(date_claimed, datetime):
            fields[SlackUsersTable.first_sign_in_at] = date_claimed.isoformat()
        else:
            fields[SlackUsersTable.first_sign_in_at] = str(date_claimed)

    # Sync Stats (linked record)
    fields[SlackUsersTable.sync_stats] = [SYNC_STATS_RECORD_ID]

    # Last Synced At - consistent across all records in this run
    fields[SlackUsersTable.last_synced_at] = sync_time

    return fields


def _normalize_val(val: Any) -> Any:
    """
    Normalize field values for comparison.

    Handles format differences between what we send and what Airtable returns:
    - Single select: {"id": "selXXX", "name": "Member"} → "Member"
    - Linked records: [{"id": "recXXX", ...}] → ["recXXX"]
    - Datetimes: "2025-06-20T04:59:48.000Z" → "2025-06-20T04:59:48+00:00"
    """
    if isinstance(val, dict) and "name" in val:
        # Single select field - extract just the name
        return val["name"]
    if isinstance(val, list):
        # Linked records or other list fields - normalize each element
        return [
            item["id"] if isinstance(item, dict) and "id" in item else item
            for item in val
        ]
    # Normalize ISO datetime strings to a canonical format
    # Airtable returns "2025-06-20T04:59:48.000Z", Python sends "2025-06-20T04:59:48+00:00"
    if isinstance(val, str) and len(val) >= 19 and val[10:11] == "T":
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt.isoformat()
        except ValueError:
            pass
    return val


def fields_differ(desired: Dict[str, Any], existing: Dict[str, Any]) -> bool:
    """
    Compare desired fields with existing Airtable fields.

    Only compares DIFF_FIELDS (excludes last_synced_at).
    Handles Airtable's convention where unchecked checkboxes and empty fields
    are absent from the record rather than set to False/None.
    """
    for field_id in DIFF_FIELDS:
        desired_val = _normalize_val(desired.get(field_id))
        existing_val = _normalize_val(existing.get(field_id))

        # Normalize: treat None and absent the same way
        # For booleans, Airtable omits False/unchecked fields
        if desired_val is None and existing_val is None:
            continue
        if desired_val != existing_val:
            return True

    return False


def _fmt_duration(seconds: float) -> str:
    """Format seconds into a human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m{secs}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h{mins}m{secs}s"


def _batch_operation(
    operation: Callable,
    records: List[Any],
    batch_size: int,
    log: Any,
    verb: str,
) -> Tuple[int, int]:
    """
    Generic batch processor with 15-second progress logging.

    Args:
        operation: callable to invoke on each batch (e.g., table.batch_create)
        records: list of records/IDs to process
        batch_size: number of records per API call
        log: dagster logger
        verb: past tense for logging (e.g., "Created", "Updated", "Deleted")

    Returns:
        Tuple of (successfully processed count, error count).
    """
    total = len(records)
    if total == 0:
        return (0, 0)

    processed = 0
    failed_records = 0
    start = time.monotonic()
    last_log = start

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        try:
            operation(batch)
            processed += len(batch)
        except Exception as e:
            failed_records += len(batch)
            log.error(f"  Batch {i // batch_size + 1} ({len(batch)} records) failed: {e}")

        now = time.monotonic()
        if now - last_log >= 15:
            elapsed = now - start
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (total - processed) / rate if rate > 0 else 0
            pct = processed / total * 100
            log.info(
                f"  [{verb}] {processed:,}/{total:,} ({pct:.1f}%) | "
                f"{rate:.0f} records/s | "
                f"elapsed {_fmt_duration(elapsed)} | "
                f"~{_fmt_duration(remaining)} remaining"
                + (f" | {failed_records} failed" if failed_records else "")
            )
            last_log = now

    elapsed = time.monotonic() - start
    rate = processed / elapsed if elapsed > 0 else 0
    log.info(
        f"  [{verb}] Done: {processed:,}/{total:,} in {_fmt_duration(elapsed)} ({rate:.0f} records/s)"
        + (f" | {failed_records} failed" if failed_records else "")
    )
    return (processed, failed_records)


@asset(
    name="sync_slack_users_to_airtable",
    compute_kind="airtable_sync",
    group_name="slack_users_sync",
    deps=["slack_member_metadata"],
    description="Syncs Slack user metadata from PostgreSQL warehouse to Airtable (intelligent diff sync)"
)
def sync_slack_users_to_airtable(
    context: AssetExecutionContext,
    airtable: AirtableResource,
) -> Output[None]:
    """
    Syncs Slack users from the warehouse to Airtable.

    Uses a smart diff approach to minimize API calls:
    - Deletes records that no longer exist in source
    - Updates only records where fields have actually changed
    - Creates new records
    """
    log = context.log
    sync_start = time.monotonic()
    sync_time = datetime.now(timezone.utc).isoformat()
    total_errors = 0

    # Step 1: Fetch all existing Airtable records (all fields for diffing)
    log.info("=" * 60)
    log.info("STEP 1/6: Fetching existing Airtable records...")
    log.info("=" * 60)
    step_start = time.monotonic()
    table = airtable.get_table("synced_data_warehouse_slack_users", "slack_users")

    # Maps slack_user_id -> (airtable_record_id, fields_dict)
    existing_by_slack_id: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    duplicate_record_ids: list[str] = []

    existing_records = []
    last_log = time.monotonic()
    for page in table.iterate(use_field_ids=True, page_size=100):
        existing_records.extend(page)
        now = time.monotonic()
        if now - last_log >= 15:
            elapsed = now - step_start
            rate = len(existing_records) / elapsed if elapsed > 0 else 0
            log.info(
                f"  [Fetching] {len(existing_records):,} records so far | "
                f"{rate:.0f} records/s | "
                f"elapsed {_fmt_duration(elapsed)}"
            )
            last_log = now
    fetch_time = time.monotonic() - step_start

    for record in existing_records:
        fields = record.get("fields", {})
        slack_id = fields.get(SlackUsersTable.slack_id)
        if slack_id:
            if slack_id in existing_by_slack_id:
                duplicate_record_ids.append(record["id"])
            else:
                existing_by_slack_id[slack_id] = (record["id"], fields)

    log.info(
        f"Fetched {len(existing_records):,} records in {_fmt_duration(fetch_time)} | "
        f"{len(existing_by_slack_id):,} unique Slack users | "
        f"{len(duplicate_record_ids):,} duplicates"
    )

    # Step 2: Fetch all Slack users from PostgreSQL
    log.info("=" * 60)
    log.info("STEP 2/6: Fetching Slack users from PostgreSQL...")
    log.info("=" * 60)
    step_start = time.monotonic()
    source_users: Dict[str, Dict[str, Any]] = {}
    conn = get_db_connection()
    try:
        with conn.cursor(name="slack_users_sync") as cur:
            cur.itersize = 50000
            cur.execute("""
                SELECT m.user_id, m.real_name, m.display_name, m.email,
                       m.tz, m.image_original,
                       m.is_admin, m.is_owner, m.is_primary_owner,
                       m.is_restricted, m.is_ultra_restricted,
                       m.is_bot, m.is_app_user,
                       a.date_claimed,
                       a.is_active
                FROM slack.member_metadata m
                LEFT JOIN (
                    SELECT DISTINCT ON (user_id) user_id, date_claimed, is_active
                    FROM slack.member_analytics
                    ORDER BY user_id, date DESC
                ) a ON m.user_id = a.user_id
                WHERE m.deleted = false OR m.deleted IS NULL
            """)
            # Named cursors defer description until first fetch
            first_row = cur.fetchone()
            if first_row is not None:
                columns = [desc[0] for desc in cur.description]
                for row in chain([first_row], cur):
                    user_data = dict(zip(columns, row))
                    user_id = user_data.get("user_id")
                    if user_id:
                        source_users[user_id] = user_data
    finally:
        conn.close()

    pg_time = time.monotonic() - step_start
    log.info(f"Fetched {len(source_users):,} active users from PostgreSQL in {_fmt_duration(pg_time)}")

    # Step 3: Compute sync plan (with full diff)
    log.info("=" * 60)
    log.info("STEP 3/6: Computing sync plan...")
    log.info("=" * 60)
    step_start = time.monotonic()
    source_user_ids = set(source_users.keys())
    existing_user_ids = set(existing_by_slack_id.keys())

    to_create_ids = source_user_ids - existing_user_ids
    to_check_update = source_user_ids & existing_user_ids
    to_delete = existing_user_ids - source_user_ids

    # Diff existing records upfront to get exact counts
    records_to_create = []
    records_to_update = []
    skipped_count = 0

    for uid in to_create_ids:
        records_to_create.append(transform_pg_user_to_airtable(source_users[uid], sync_time))

    for uid in to_check_update:
        desired_fields = transform_pg_user_to_airtable(source_users[uid], sync_time)
        record_id, existing_fields = existing_by_slack_id[uid]

        if fields_differ(desired_fields, existing_fields):
            records_to_update.append({
                "id": record_id,
                "fields": desired_fields
            })
        else:
            skipped_count += 1

    diff_time = time.monotonic() - step_start
    log.info(f"Sync plan computed in {_fmt_duration(diff_time)}:")
    log.info(f"  To delete:    {len(to_delete):,} stale + {len(duplicate_record_ids):,} duplicates")
    log.info(f"  To update:    {len(records_to_update):,}")
    log.info(f"  To create:    {len(records_to_create):,}")
    log.info(f"  Unchanged:    {skipped_count:,}")

    # Step 4: Delete stale + duplicate records
    log.info("=" * 60)
    log.info("STEP 4/6: Deleting stale and duplicate records...")
    log.info("=" * 60)
    record_ids_to_delete = [existing_by_slack_id[uid][0] for uid in to_delete] + duplicate_record_ids
    if record_ids_to_delete:
        deleted_count, errs = _batch_operation(table.batch_delete, record_ids_to_delete, 10, log, "Deleted")
        total_errors += errs
    else:
        log.info("Nothing to delete")
        deleted_count = 0

    # Step 5: Update changed records
    log.info("=" * 60)
    log.info("STEP 5/6: Updating changed records...")
    log.info("=" * 60)
    if records_to_update:
        updated_count, errs = _batch_operation(table.batch_update, records_to_update, 10, log, "Updated")
        total_errors += errs
    else:
        log.info("Nothing to update")
        updated_count = 0

    # Step 6: Create new records
    log.info("=" * 60)
    log.info("STEP 6/6: Creating new records...")
    log.info("=" * 60)
    if records_to_create:
        created_count, errs = _batch_operation(table.batch_create, records_to_create, 10, log, "Created")
        total_errors += errs
    else:
        log.info("Nothing to create")
        created_count = 0

    # Final summary
    total_time = time.monotonic() - sync_start
    log.info("=" * 60)
    log.info("SYNC COMPLETE")
    log.info("=" * 60)
    log.info(f"  Created:   {created_count:,}")
    log.info(f"  Updated:   {updated_count:,}")
    log.info(f"  Unchanged: {skipped_count:,}")
    log.info(f"  Deleted:   {deleted_count:,}")
    log.info(f"  Errors:    {total_errors}")
    log.info(f"  Total time: {_fmt_duration(total_time)}")
    log.info("=" * 60)

    return Output(
        value=None,
        metadata={
            "source_users_count": MetadataValue.int(len(source_users)),
            "existing_airtable_records": MetadataValue.int(len(existing_by_slack_id)),
            "created_count": MetadataValue.int(created_count),
            "updated_count": MetadataValue.int(updated_count),
            "skipped_unchanged": MetadataValue.int(skipped_count),
            "deleted_count": MetadataValue.int(deleted_count),
            "errors": MetadataValue.int(total_errors),
            "airtable_base": MetadataValue.text("synced_data_warehouse_slack_users"),
            "airtable_table": MetadataValue.text("slack_users"),
        }
    )


# Create Definitions for this module
defs = Definitions(
    assets=[sync_slack_users_to_airtable],
    resources={},  # Uses the shared airtable resource from the main definitions
)
