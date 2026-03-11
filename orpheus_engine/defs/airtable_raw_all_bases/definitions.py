"""
Airtable Raw All Bases Sync

Discovers all Airtable bases accessible by the API token and syncs their
complete contents into the `airtable_raw_all_bases` PostgreSQL schema.

Two tables:
  - bases: One row per BASE with full schema as JSONB
    (base_id PK, base_name, schema JSONB, _synced_at)
  - records: One row per RECORD with fields as JSONB (field names as keys)
    (base_id + table_id + record_id composite PK, fields JSONB, _synced_at)

Architecture:
  - 5 bases download concurrently via a ThreadPoolExecutor
  - Downloaded data is pushed into a write queue (thread-safe)
  - The write queue flushes to PostgreSQL every 10k rows
  - After all bases are processed, a final flush writes remaining rows
  - Stale rows (with _synced_at < run start) are deleted
"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from pyairtable import Api as AirtableApi
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)


SCHEMA_NAME = "airtable_raw_all_bases"
FLUSH_THRESHOLD = 10_000
CONCURRENT_BASES = 5

BASES_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.bases (
    base_id TEXT PRIMARY KEY,
    base_name TEXT,
    schema JSONB,
    _synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

RECORDS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.records (
    base_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    fields JSONB,
    _synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (base_id, table_id, record_id)
);
"""


def get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def get_airtable_api() -> AirtableApi:
    token = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    if not token:
        raise ValueError("AIRTABLE_PERSONAL_ACCESS_TOKEN environment variable is not set")
    return AirtableApi(token)


def ensure_schema_and_tables(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(SCHEMA_NAME)
        ))
        cur.execute(BASES_DDL)
        cur.execute(RECORDS_DDL)
    conn.commit()


def _clean_field_value(value: Any) -> Any:
    if isinstance(value, dict):
        if "specialValue" in value or "error" in value:
            return None
    return value


def _clean_record_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _clean_field_value(v) for k, v in fields.items()}


def fetch_with_retry(fn, *, max_attempts=6, log=None):
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "RATE_LIMIT" in error_str.upper():
                if attempt < max_attempts - 1:
                    wait = min(2 ** attempt * 2, 30)
                    if log:
                        log.warning(f"Rate limited, waiting {wait}s (attempt {attempt + 1}/{max_attempts})")
                    time.sleep(wait)
                    continue
            raise


def _build_schema_json(schema) -> List[Dict]:
    """Build a JSON-serializable schema list from a pyairtable BaseSchema."""
    tables = []
    for table_schema in schema.tables:
        fields = []
        for field in table_schema.fields:
            fields.append({
                "id": field.id,
                "name": field.name,
                "type": field.type,
            })
        tables.append({
            "table_id": table_schema.id,
            "table_name": table_schema.name,
            "fields": fields,
        })
    return tables


class WriteQueue:
    """Thread-safe queue that accumulates rows and flushes to PostgreSQL in batches."""

    def __init__(self, run_ts: datetime, log):
        self._lock = threading.Lock()
        self._bases_buf: List[Tuple] = []
        self._records_buf: List[Tuple] = []
        self._run_ts = run_ts
        self._log = log
        self._total_bases_flushed = 0
        self._total_records_flushed = 0

    def push_base(self, row: Tuple):
        """Push a single base row (base_id, base_name, schema_json)."""
        with self._lock:
            self._bases_buf.append(row)
            if len(self._bases_buf) >= 100:
                self._flush_bases()

    def push_records(self, rows: List[Tuple]):
        with self._lock:
            self._records_buf.extend(rows)
            if len(self._records_buf) >= FLUSH_THRESHOLD:
                self._flush_records()

    def flush_all(self):
        with self._lock:
            self._flush_bases()
            self._flush_records()

    def _flush_bases(self):
        if not self._bases_buf:
            return
        rows = self._bases_buf
        self._bases_buf = []
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {SCHEMA_NAME}.bases
                        (base_id, base_name, schema, _synced_at)
                    VALUES %s
                    ON CONFLICT (base_id) DO UPDATE SET
                        base_name = EXCLUDED.base_name,
                        schema = EXCLUDED.schema,
                        _synced_at = EXCLUDED._synced_at
                    """,
                    [(r[0], r[1], r[2], self._run_ts) for r in rows],
                    template="(%s, %s, %s::jsonb, %s)",
                    page_size=100,
                )
            conn.commit()
            self._total_bases_flushed += len(rows)
            self._log.info(f"Flushed {len(rows)} bases (total: {self._total_bases_flushed})")
        finally:
            conn.close()

    def _flush_records(self):
        if not self._records_buf:
            return
        rows = self._records_buf
        self._records_buf = []
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {SCHEMA_NAME}.records
                        (base_id, table_id, record_id, fields, _synced_at)
                    VALUES %s
                    ON CONFLICT (base_id, table_id, record_id) DO UPDATE SET
                        fields = EXCLUDED.fields,
                        _synced_at = EXCLUDED._synced_at
                    """,
                    [(r[0], r[1], r[2], r[3], self._run_ts) for r in rows],
                    template="(%s, %s, %s, %s::jsonb, %s)",
                    page_size=500,
                )
            conn.commit()
            self._total_records_flushed += len(rows)
            self._log.info(f"Flushed {len(rows)} records (total: {self._total_records_flushed})")
        finally:
            conn.close()


def process_base(base, queue: WriteQueue, log) -> Dict[str, int]:
    """Process a single Airtable base: fetch schema + records, push to queue."""
    base_id = base.id
    base_name = base.name
    stats = {"tables": 0, "fields": 0, "records": 0, "table_errors": 0}

    try:
        schema = fetch_with_retry(lambda: base.schema(), log=log)
    except Exception as e:
        log.warning(f"Failed to fetch schema for {base_name} ({base_id}): {e}")
        raise

    # Build and push base metadata (one row per base)
    schema_json = _build_schema_json(schema)
    for t in schema_json:
        stats["tables"] += 1
        stats["fields"] += len(t["fields"])
    queue.push_base((base_id, base_name, json.dumps(schema_json)))

    # Fetch records for each table
    for table_schema in schema.tables:
        table_id = table_schema.id
        table_name = table_schema.name

        try:
            table = base.table(table_id)
            records = fetch_with_retry(
                lambda t=table: t.all(),
                log=log,
            )
        except Exception as e:
            log.warning(f"  [{base_name}] Failed to fetch records from {table_name} ({table_id}): {e}")
            stats["table_errors"] += 1
            continue

        data_rows = []
        for record in records:
            fields = _clean_record_fields(record.get("fields", {}))
            data_rows.append((base_id, table_id, record["id"], json.dumps(fields)))

        stats["records"] += len(data_rows)
        queue.push_records(data_rows)
        log.info(f"  [{base_name}] {table_name}: {len(data_rows)} records")

    return stats


def delete_stale_rows(conn, run_ts: datetime) -> Tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {SCHEMA_NAME}.bases WHERE _synced_at < %s",
            (run_ts,),
        )
        deleted_bases = cur.rowcount
        cur.execute(
            f"DELETE FROM {SCHEMA_NAME}.records WHERE _synced_at < %s",
            (run_ts,),
        )
        deleted_records = cur.rowcount
    conn.commit()
    return deleted_bases, deleted_records


@asset(
    compute_kind="airtable_api",
    group_name="airtable_raw_all_bases",
    description="Syncs all Airtable bases, tables, fields, and records into airtable_raw_all_bases schema (bases + records tables)",
)
def airtable_raw_all_bases_sync(
    context: AssetExecutionContext,
) -> Output[None]:
    """
    Discovers all Airtable bases and syncs their contents to PostgreSQL.

    1. Records a run timestamp
    2. Lists all bases via the Airtable API
    3. Downloads 5 bases concurrently, pushing rows to a write queue
    4. Write queue flushes to PostgreSQL every 10k rows
    5. Final flush after all bases are done
    6. Deletes any rows with _synced_at < run timestamp (stale data)
    """
    log = context.log
    api = get_airtable_api()

    conn = get_db_connection()
    try:
        ensure_schema_and_tables(conn)
    finally:
        conn.close()

    run_ts = datetime.now(timezone.utc)
    log.info(f"Run timestamp: {run_ts.isoformat()}")

    log.info("Discovering all Airtable bases...")
    bases = api.bases()
    log.info(f"Found {len(bases)} bases")

    queue = WriteQueue(run_ts, log)

    total_tables = 0
    total_records = 0
    total_fields = 0
    base_errors = 0
    table_errors = 0
    bases_done = 0

    with ThreadPoolExecutor(max_workers=CONCURRENT_BASES) as executor:
        future_to_base = {
            executor.submit(process_base, base, queue, log): base
            for base in bases
        }

        for future in as_completed(future_to_base):
            base = future_to_base[future]
            bases_done += 1

            try:
                stats = future.result()
                total_tables += stats["tables"]
                total_fields += stats["fields"]
                total_records += stats["records"]
                table_errors += stats["table_errors"]
            except Exception as e:
                log.warning(f"Base {base.name} ({base.id}) failed: {e}")
                base_errors += 1

            if bases_done % 50 == 0:
                log.info(
                    f"Progress: {bases_done}/{len(bases)} bases done, "
                    f"{total_tables} tables, {total_records} records"
                )

    log.info("Final flush of remaining queued rows...")
    queue.flush_all()

    log.info("Cleaning up stale data...")
    conn = get_db_connection()
    try:
        deleted_bases, deleted_records = delete_stale_rows(conn, run_ts)
    finally:
        conn.close()
    log.info(f"Deleted {deleted_bases} stale bases, {deleted_records} stale records")

    log.info(
        f"Sync complete: {len(bases)} bases, {total_tables} tables, "
        f"{total_fields} fields, {total_records} records"
    )

    return Output(
        value=None,
        metadata={
            "bases": MetadataValue.int(len(bases)),
            "tables": MetadataValue.int(total_tables),
            "fields": MetadataValue.int(total_fields),
            "records": MetadataValue.int(total_records),
            "base_errors": MetadataValue.int(base_errors),
            "table_errors": MetadataValue.int(table_errors),
            "deleted_bases": MetadataValue.int(deleted_bases),
            "deleted_records": MetadataValue.int(deleted_records),
        },
    )


defs = Definitions(
    assets=[airtable_raw_all_bases_sync],
)
