"""
DuckLake row hash computation asset.

This module is part of the DuckLake sync infrastructure. It runs after all other
assets complete to compute xxHash-based row hashes for change detection.

Uses the pg_rowhash extension to add _row_hash (bytea) and _row_hash_at (timestamptz)
columns to every table in the warehouse, enabling efficient incremental sync to DuckLake.
"""

import os
import logging
from typing import List, Tuple, Sequence

import dagster as dg
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schemas to exclude from DuckLake row hash computation
EXCLUDED_SCHEMAS = frozenset({
    "pg_catalog",
    "information_schema",
    "pg_toast",
    "pg_temp_1",
    "pg_toast_temp_1",
})

# Tables to exclude (schema.table format)
EXCLUDED_TABLES = frozenset({
    # Add any specific tables to skip here
})


def get_warehouse_connection():
    """Get a connection to the warehouse database."""
    conn_string = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable not set")
    # Clean up the URL
    conn_string = conn_string.replace('\n', '').replace('\r', '').strip()
    return psycopg2.connect(conn_string)


def get_all_tables(conn) -> List[Tuple[str, str]]:
    """Get all tables in all schemas (excluding system schemas)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN %s
            ORDER BY schemaname, tablename
        """, (tuple(EXCLUDED_SCHEMAS),))
        tables = cur.fetchall()
    return [(schema, table) for schema, table in tables 
            if f"{schema}.{table}" not in EXCLUDED_TABLES]


def ensure_rowhash_extension(conn, log):
    """Ensure the rowhash extension is installed."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'rowhash'")
        if not cur.fetchone():
            log.info("Installing rowhash extension...")
            cur.execute("CREATE EXTENSION IF NOT EXISTS rowhash")
            conn.commit()
            log.info("rowhash extension installed")
        else:
            log.info("rowhash extension already installed")


def compute_row_hash_for_table(
    conn,
    schema: str,
    table: str,
    batch_size: int,
    log,
) -> dict:
    """
    Compute row hashes for a single table.
    
    Returns a dict with statistics about the operation.
    """
    target_table = f'"{schema}"."{table}"'
    stats = {
        "schema": schema,
        "table": table,
        "total_rows": 0,
        "rows_updated": 0,
        "batches": 0,
        "skipped": False,
        "error": None,
    }
    
    try:
        with conn.cursor() as cur:
            # Add hash columns if they don't exist
            cur.execute(f"""
                ALTER TABLE {target_table} 
                ADD COLUMN IF NOT EXISTS _row_hash bytea,
                ADD COLUMN IF NOT EXISTS _row_hash_at timestamptz
            """)
            conn.commit()
            
            # Count total rows
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
            stats["total_rows"] = cur.fetchone()[0]
            
            if stats["total_rows"] == 0:
                log.info(f"  {target_table}: empty table, skipping")
                stats["skipped"] = True
                return stats
            
            # Process in batches
            total_updated = 0
            batch_count = 0
            
            while True:
                batch_count += 1
                
                # Update rows where hash differs or is null
                cur.execute(f"""
                    WITH to_update AS (
                        SELECT ctid
                        FROM {target_table} t
                        WHERE _row_hash IS DISTINCT FROM rowhash_128b(t.*)
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {target_table} t SET 
                        _row_hash = rowhash_128b(t.*),
                        _row_hash_at = now()
                    FROM to_update
                    WHERE t.ctid = to_update.ctid
                """, (batch_size,))
                
                batch_updated = cur.rowcount
                total_updated += batch_updated
                conn.commit()
                
                if batch_updated == 0:
                    break
                
                if batch_count % 10 == 0:
                    log.info(f"  {target_table}: batch {batch_count}, "
                            f"{total_updated}/{stats['total_rows']} rows")
            
            stats["rows_updated"] = total_updated
            stats["batches"] = batch_count
            
            log.info(f"  {target_table}: {total_updated} rows updated in {batch_count} batches")
            
    except psycopg2.Error as e:
        stats["error"] = str(e)
        log.error(f"  {target_table}: error - {e}")
        conn.rollback()
    
    return stats


def _warehouse_row_hashes_impl(context: dg.AssetExecutionContext) -> dg.Output[None]:
    """
    Implementation of DuckLake row hash computation.
    Separated from the asset decorator to allow dynamic dependency injection.
    """
    log = context.log
    batch_size = 100_000
    
    log.info("Starting DuckLake row hash computation...")
    
    conn = get_warehouse_connection()
    try:
        # Ensure extension is available
        ensure_rowhash_extension(conn, log)
        
        # Get all tables
        tables = get_all_tables(conn)
        log.info(f"Found {len(tables)} tables to process")
        
        # Track statistics
        total_tables = len(tables)
        tables_processed = 0
        tables_with_errors = 0
        tables_skipped = 0
        total_rows_scanned = 0
        total_rows_updated = 0
        
        # Process each table
        for schema, table in tables:
            tables_processed += 1
            log.info(f"[{tables_processed}/{total_tables}] Processing {schema}.{table}...")
            
            stats = compute_row_hash_for_table(
                conn, schema, table, batch_size, log
            )
            
            # Always count total rows (even for errors/skipped)
            total_rows_scanned += stats["total_rows"]
            
            if stats["error"]:
                tables_with_errors += 1
            elif stats["skipped"]:
                tables_skipped += 1
            else:
                total_rows_updated += stats["rows_updated"]
        
        log.info("=" * 60)
        log.info("DuckLake row hash computation complete!")
        log.info(f"  Tables processed: {tables_processed}")
        log.info(f"  Tables skipped (empty): {tables_skipped}")
        log.info(f"  Tables with errors: {tables_with_errors}")
        log.info(f"  Total rows scanned: {total_rows_scanned:,}")
        log.info(f"  Total rows updated: {total_rows_updated:,}")
        
        return dg.Output(
            None,
            metadata={
                "tables_processed": tables_processed,
                "tables_skipped": tables_skipped,
                "tables_with_errors": tables_with_errors,
                "total_rows_scanned": total_rows_scanned,
                "total_rows_updated": total_rows_updated,
            },
        )
        
    finally:
        conn.close()


def create_warehouse_row_hashes_asset(
    dep_asset_keys: Sequence[dg.AssetKey] = (),
) -> dg.AssetsDefinition:
    """
    Factory function to create the warehouse_row_hashes asset with dynamic dependencies.
    
    Args:
        dep_asset_keys: List of asset keys this asset should depend on.
                       If empty, the asset has no dependencies.
    
    Returns:
        The warehouse_row_hashes asset with the specified dependencies.
    """
    @dg.asset(
        name="warehouse_row_hashes",
        group_name="ducklake",
        compute_kind="postgres",
        description=(
            f"DuckLake sync preparation: computes xxHash-based row hashes for all tables. "
            f"Adds _row_hash and _row_hash_at columns for efficient incremental sync to DuckLake. "
            f"Depends on {len(dep_asset_keys)} other assets."
        ),
        deps=list(dep_asset_keys) if dep_asset_keys else None,
    )
    def warehouse_row_hashes(context: dg.AssetExecutionContext) -> dg.Output[None]:
        """
        DuckLake row hash computation for the warehouse database.
        
        Runs after all data ingestion assets complete, ensuring row hashes
        are computed on the latest data. Uses the pg_rowhash extension
        (xxHash) to enable efficient incremental sync to DuckLake.
        """
        return _warehouse_row_hashes_impl(context)
    
    return warehouse_row_hashes


# For standalone testing/development, create asset with no dependencies
# The main definitions.py will create the real asset with all dependencies
warehouse_row_hashes = create_warehouse_row_hashes_asset()

# Note: defs is intentionally NOT exported here.
# The asset is created dynamically in the main definitions.py with proper dependencies.

