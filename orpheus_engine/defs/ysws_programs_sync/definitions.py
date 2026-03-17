"""
YSWS Programs Sync: weighted_referral_count

Computes weighted referral count per YSWS program (capped_hours / 10) using
approved project activity since 2026-03-01, and upserts it to the ysws_programs
Airtable table.

Only counts hours from approved projects where:
- The project was approved on or after 2026-03-01
- The user's first Loops event matches the program's prefix
- The project is linked to that same program

Hours are capped at 10 per (user, program) pair.
weighted_referral_count = sum(min(user_hours, 10)) / 10
"""

import math
import os
from typing import Any, Dict, List

import psycopg2
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

from ..airtable.resources import AirtableResource

# ─── CONFIGURE THIS ───────────────────────────────────────────────────────────
# After creating the "Weighted Referral Count" field in the ysws_programs
# Airtable table, run scripts/generate_airtable_ids.py and paste the field ID:
WEIGHTED_REFERRAL_COUNT_FIELD_ID = "fldkzELMtOTOn7kek"  # ysws_programs: Weighted Referral Count
# ─────────────────────────────────────────────────────────────────────────────

ACTIVITY_START_DATE = "2026-03-01"

# Computes weighted_referral_count (capped_hours / 10) per program.
# Uses a LEFT JOIN from all_programs so every program gets a value —
# programs with no qualifying activity get 0 rather than being omitted.
_SQL = f"""
WITH programs_with_activity AS (
  SELECT DISTINCT y.value AS program_id
  FROM airtable_unified_ysws_projects_db.approved_projects__ysws y
  JOIN airtable_unified_ysws_projects_db.approved_projects p
    ON p._dlt_id = y._dlt_parent_id
  WHERE p.approved_at >= '{ACTIVITY_START_DATE}'
),
program_base AS (
  SELECT
    yp.id AS program_id,
    NULLIF(BTRIM(LOWER(yp.sign_up_stats_override_prefix)), '') AS override_prefix,
    REGEXP_REPLACE(LOWER(yp.name), '[^a-z0-9]+', '', 'g') AS normalized_name,
    CASE
      WHEN ARRAY_LENGTH(
        REGEXP_SPLIT_TO_ARRAY(
          REGEXP_REPLACE(LOWER(yp.name), '[^a-z0-9]+', ' ', 'g'),
          '\\s+'
        ),
        1
      ) >= 2 THEN (
        SELECT STRING_AGG(LEFT(part, 1), '')
        FROM UNNEST(
          REGEXP_SPLIT_TO_ARRAY(
            REGEXP_REPLACE(LOWER(yp.name), '[^a-z0-9]+', ' ', 'g'),
            '\\s+'
          )
        ) AS part
        WHERE part <> ''
      )
    END AS acronym_prefix
  FROM airtable_unified_ysws_projects_db.ysws_programs yp
  JOIN programs_with_activity pwa ON pwa.program_id = yp.id
),
program_prefixes AS (
  SELECT DISTINCT program_id, prefix
  FROM (
    SELECT program_id, override_prefix AS prefix
    FROM program_base
    WHERE override_prefix IS NOT NULL

    UNION ALL

    SELECT program_id, normalized_name AS prefix
    FROM program_base
    WHERE normalized_name IS NOT NULL
      AND LENGTH(normalized_name) >= 3

    UNION ALL

    SELECT program_id, acronym_prefix AS prefix
    FROM program_base
    WHERE acronym_prefix IS NOT NULL
      AND LENGTH(acronym_prefix) >= 3
  ) t
  WHERE prefix IS NOT NULL AND prefix <> ''
),
loops_events AS (
  SELECT
    CASE
      WHEN POSITION('@' IN LOWER(BTRIM(email))) > 0 THEN
        SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)), '@', 1), '+', 1)
        || '@' ||
        SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
      ELSE SPLIT_PART(LOWER(BTRIM(email)), '+', 1)
    END AS normalized_email,
    LOWER(event) AS event_name,
    event_date
  FROM public_unified_analytics.events
  WHERE email IS NOT NULL
    AND source_system = 'loops'
),
first_loops_at AS (
  SELECT normalized_email, MIN(event_date) AS first_loops_event_at
  FROM loops_events
  GROUP BY normalized_email
),
first_loops_events AS (
  SELECT DISTINCT fl.normalized_email, le.event_name
  FROM first_loops_at fl
  JOIN loops_events le
    ON le.normalized_email = fl.normalized_email
   AND le.event_date = fl.first_loops_event_at
),
new_users_by_program AS (
  SELECT DISTINCT pp.program_id, fle.normalized_email
  FROM first_loops_events fle
  JOIN program_prefixes pp
    ON fle.event_name LIKE pp.prefix || '%'
),
recent_project_rows AS (
  SELECT DISTINCT
    y.value AS program_id,
    CASE
      WHEN POSITION('@' IN LOWER(BTRIM(ap.email_trimmed_lowercased))) > 0 THEN
        SPLIT_PART(
          SPLIT_PART(LOWER(BTRIM(ap.email_trimmed_lowercased)), '@', 1),
          '+',
          1
        )
        || '@' ||
        SPLIT_PART(LOWER(BTRIM(ap.email_trimmed_lowercased)), '@', 2)
      ELSE SPLIT_PART(LOWER(BTRIM(ap.email_trimmed_lowercased)), '+', 1)
    END AS normalized_email,
    COALESCE(ap.hours_spent, 0) AS hours_spent
  FROM airtable_unified_ysws_projects_db.approved_projects ap
  JOIN airtable_unified_ysws_projects_db.approved_projects__ysws y
    ON y._dlt_parent_id = ap._dlt_id
  WHERE ap.approved_at >= '{ACTIVITY_START_DATE}'
),
project_hours_by_user AS (
  SELECT
    n.program_id,
    n.normalized_email,
    SUM(rpr.hours_spent) AS raw_hours
  FROM new_users_by_program n
  JOIN recent_project_rows rpr
    ON rpr.program_id = n.program_id
   AND rpr.normalized_email = n.normalized_email
  GROUP BY n.program_id, n.normalized_email
),
program_totals AS (
  SELECT
    program_id,
    SUM(LEAST(raw_hours, 10)) AS capped_hours
  FROM project_hours_by_user
  GROUP BY program_id
),
all_programs AS (
  SELECT id AS program_id
  FROM airtable_unified_ysws_projects_db.ysws_programs
)
SELECT
  ap.program_id,
  COALESCE(ROUND((pt.capped_hours / 10.0)::numeric, 2), 0.0) AS weighted_referral_count
FROM all_programs ap
LEFT JOIN program_totals pt ON pt.program_id = ap.program_id
"""


def _get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def _is_valid_number(value: Any) -> bool:
    """Returns True only for finite real numbers. Rejects strings, None, NaN, Inf."""
    if not isinstance(value, (int, float)):
        return False
    return not (math.isnan(value) or math.isinf(value))


@asset(
    name="unified_ysws_ysws_programs_weighted_referral_count",
    compute_kind="airtable_sync",
    group_name="unified_ysws_db_processing",
    deps=["unified_ysws_approved_projects_warehouse", "unified_ysws_ysws_programs_warehouse"],
    description=(
        f"Computes weighted_referral_count (capped project hours / 10) per YSWS program "
        f"from approved project activity since {ACTIVITY_START_DATE} "
        f"and upserts it to the ysws_programs Airtable table."
    ),
)
def sync_ysws_programs_weighted_referral_count(
    context: AssetExecutionContext,
    airtable: AirtableResource,
) -> Output[None]:
    if WEIGHTED_REFERRAL_COUNT_FIELD_ID.startswith("FILL_IN"):
        raise ValueError(
            "WEIGHTED_REFERRAL_COUNT_FIELD_ID is not configured. "
            "Create the 'Weighted Referral Count' field in the ysws_programs Airtable table, "
            "run scripts/generate_airtable_ids.py, and update the constant in this file."
        )

    log = context.log

    # Step 1: Query warehouse for computed values
    log.info(f"Querying warehouse for weighted_referral_count (activity since {ACTIVITY_START_DATE})...")
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_SQL)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    log.info(f"Query returned {len(rows)} program rows")

    # Step 2: Validate and build update records — never write junk data
    updates: List[Dict[str, Any]] = []
    skipped_junk = 0

    for row in rows:
        row_dict = dict(zip(columns, row))
        program_id = row_dict.get("program_id")
        value = row_dict.get("weighted_referral_count")

        if not program_id or not isinstance(program_id, str):
            log.warning(f"Skipping row with invalid program_id: {row_dict}")
            skipped_junk += 1
            continue

        # psycopg2 returns NUMERIC as decimal.Decimal — convert to float
        if hasattr(value, "__float__"):
            value = float(value)

        if not _is_valid_number(value):
            log.warning(f"Skipping program {program_id}: non-numeric value {value!r}")
            skipped_junk += 1
            continue

        updates.append({
            "id": program_id,
            WEIGHTED_REFERRAL_COUNT_FIELD_ID: value,
        })

    log.info(
        f"Built {len(updates)} valid update records "
        f"({skipped_junk} skipped for invalid values)"
    )

    if not updates:
        log.warning("No valid records to update — aborting to avoid writing junk data")
        return Output(
            value=None,
            metadata={
                "programs_queried": MetadataValue.int(len(rows)),
                "updates_written": MetadataValue.int(0),
                "skipped_junk": MetadataValue.int(skipped_junk),
                "activity_start_date": MetadataValue.text(ACTIVITY_START_DATE),
            },
        )

    # Step 3: Batch upsert to Airtable
    result = airtable.batch_update_records(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="ysws_programs",
        records=updates,
    )

    log.info(f"Airtable update complete: {result}")

    return Output(
        value=None,
        metadata={
            "programs_queried": MetadataValue.int(len(rows)),
            "updates_written": MetadataValue.int(result.get("successful", 0)),
            "updates_failed": MetadataValue.int(result.get("failed", 0)),
            "skipped_junk": MetadataValue.int(skipped_junk),
            "activity_start_date": MetadataValue.text(ACTIVITY_START_DATE),
        },
    )


defs = Definitions(
    assets=[sync_ysws_programs_weighted_referral_count],
    resources={},
)
