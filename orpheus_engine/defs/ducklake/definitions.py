"""
DuckLake row hash computation asset.

This module is part of the DuckLake sync infrastructure. It runs after all other
assets complete to compute xxHash-based row hashes for change detection.

Uses the pg_rowhash extension to add _row_hash (bytea) and _row_hash_at (timestamptz)
columns to every table in the warehouse, enabling efficient incremental sync to DuckLake.

Processing is parallelized across multiple workers based on Postgres max_parallel_workers.
"""

import os
import re
import time
import signal
import logging
import threading
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import Any, List, Tuple, Sequence, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import dagster as dg
import psycopg2

# Module-level logger - Dagster automatically captures standard logging
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

# =============================================================================
# DEBUG: Limit sync to specific tables matching these regex patterns
# Set to None to sync all tables, or a list of regex patterns to filter
# Pattern matches against "schema.table" format
# =============================================================================
DEBUG_SYNC_TABLE_PATTERNS: list[str] | None = None
# Set to None to disable debug filtering and sync all tables:
# DEBUG_SYNC_TABLE_PATTERNS = None
# =============================================================================

# Progress update interval in seconds
PROGRESS_UPDATE_INTERVAL = 5.0

# Batch size for row hash updates
BATCH_SIZE = 100_000

# Fraction of Postgres max_parallel_workers to use (0.0-1.0)
WORKER_FRACTION = 0.5


@dataclass
class TableProgress:
    """Progress tracking for a single table."""
    schema: str
    table: str
    status: str = "pending"  # pending, running, completed, error, skipped
    total_rows: int = 0
    rows_updated: int = 0
    rows_remaining: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    rows_per_second: float = 0.0
    eta_seconds: Optional[float] = None
    error: Optional[str] = None
    worker_id: Optional[int] = None
    
    @property
    def table_name(self) -> str:
        return f"{self.schema}.{self.table}"
    
    @property
    def elapsed_seconds(self) -> float:
        if self.start_time is None:
            return 0.0
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time


@dataclass
class GlobalProgress:
    """Thread-safe global progress tracker."""
    tables: Dict[str, TableProgress] = field(default_factory=dict)
    tables_completed: List[str] = field(default_factory=list)
    tables_just_completed: List[str] = field(default_factory=list)  # Since last report
    total_tables: int = 0
    total_rows_scanned: int = 0
    total_rows_updated: int = 0
    start_time: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)
    stop_monitoring: bool = False
    stop_workers: bool = False  # Signal workers to terminate
    terminated: bool = False  # True if terminated early (vs completed normally)
    active_connections: Dict[int, Any] = field(default_factory=dict)  # worker_id -> connection
    
    def register_connection(self, worker_id: int, conn):
        """Register an active database connection for a worker."""
        with self.lock:
            self.active_connections[worker_id] = conn
    
    def unregister_connection(self, worker_id: int):
        """Unregister a worker's database connection."""
        with self.lock:
            self.active_connections.pop(worker_id, None)
    
    def cancel_all_queries(self):
        """Cancel all active database queries to allow workers to exit quickly."""
        with self.lock:
            for worker_id, conn in list(self.active_connections.items()):
                try:
                    conn.cancel()
                except Exception:
                    pass  # Connection might already be closed
    
    def update_table(self, table_name: str, **kwargs):
        """Thread-safe update of table progress."""
        with self.lock:
            if table_name in self.tables:
                for key, value in kwargs.items():
                    setattr(self.tables[table_name], key, value)
    
    def mark_completed(self, table_name: str, rows_updated: int, total_rows: int, error: str = None):
        """Mark a table as completed."""
        with self.lock:
            if table_name in self.tables:
                progress = self.tables[table_name]
                progress.status = "error" if error else "completed"
                progress.end_time = time.time()
                progress.rows_updated = rows_updated
                progress.error = error
                self.tables_completed.append(table_name)
                self.tables_just_completed.append(table_name)
                self.total_rows_scanned += total_rows
                self.total_rows_updated += rows_updated
    
    def mark_skipped(self, table_name: str, total_rows: int = 0):
        """Mark a table as skipped (no rows need updating)."""
        with self.lock:
            if table_name in self.tables:
                progress = self.tables[table_name]
                progress.status = "skipped"
                progress.end_time = time.time()
                self.tables_completed.append(table_name)
                self.tables_just_completed.append(table_name)
                # Still count the rows we checked even if none needed updating
                self.total_rows_scanned += total_rows
    
    def get_snapshot(self) -> dict:
        """Get a thread-safe snapshot of current progress."""
        with self.lock:
            running = []
            pending_count = 0
            completed_count = len(self.tables_completed)
            error_count = 0
            skipped_count = 0
            just_completed = self.tables_just_completed.copy()
            self.tables_just_completed.clear()
            
            # Track rows from running tables
            running_rows_total = 0
            running_rows_updated = 0
            
            for name, progress in self.tables.items():
                if progress.status == "running":
                    running.append({
                        "name": name,
                        "worker_id": progress.worker_id,
                        "total_rows": progress.total_rows,
                        "rows_updated": progress.rows_updated,
                        "rows_remaining": progress.rows_remaining,
                        "rows_per_second": progress.rows_per_second,
                        "eta_seconds": progress.eta_seconds,
                        "elapsed": progress.elapsed_seconds,
                    })
                    # Add running tables' rows to totals
                    running_rows_total += progress.total_rows
                    running_rows_updated += progress.rows_updated
                elif progress.status == "pending":
                    pending_count += 1
                elif progress.status == "error":
                    error_count += 1
                elif progress.status == "skipped":
                    skipped_count += 1
            
            return {
                "total_tables": self.total_tables,
                "completed_count": completed_count,
                "pending_count": pending_count,
                "running_count": len(running),
                "error_count": error_count,
                "skipped_count": skipped_count,
                "running": running,
                "just_completed": just_completed,
                # Include completed + running tables' rows
                "total_rows_scanned": self.total_rows_scanned + running_rows_total,
                "total_rows_updated": self.total_rows_updated + running_rows_updated,
                "elapsed": time.time() - self.start_time,
            }


def get_warehouse_connection():
    """Get a connection to the warehouse database."""
    conn_string = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable not set")
    conn_string = conn_string.replace('\n', '').replace('\r', '').strip()
    return psycopg2.connect(conn_string)


def get_postgres_worker_count(conn) -> int:
    """Get the number of parallel workers to use based on Postgres settings.
    
    Uses WORKER_FRACTION of max_parallel_workers (minimum 1 worker).
    """
    with conn.cursor() as cur:
        cur.execute("SHOW max_parallel_workers")
        max_workers = int(cur.fetchone()[0])
        
        worker_count = int(max_workers * WORKER_FRACTION)
        return max(worker_count, 1)  # At least 1 worker


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


def ensure_rowhash_extension(conn):
    """Ensure the rowhash extension is installed."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'rowhash'")
        if not cur.fetchone():
            cur.execute("CREATE EXTENSION IF NOT EXISTS rowhash")
            conn.commit()


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def format_rate(rate: float) -> str:
    """Format rows per second with appropriate unit."""
    if rate >= 1_000_000:
        return f"{rate/1_000_000:.1f}M/s"
    elif rate >= 1_000:
        return f"{rate/1_000:.1f}K/s"
    else:
        return f"{rate:.0f}/s"


def progress_monitor(progress: GlobalProgress, log):
    """
    Monitoring thread that prints consolidated progress every PROGRESS_UPDATE_INTERVAL seconds.
    """
    while not progress.stop_monitoring:
        time.sleep(PROGRESS_UPDATE_INTERVAL)
        
        if progress.stop_monitoring:
            break
        
        snapshot = progress.get_snapshot()
        
        # Calculate overall progress percentage
        total_tables = snapshot['total_tables']
        completed_count = snapshot['completed_count']
        overall_pct = (completed_count / total_tables * 100) if total_tables > 0 else 0
        
        # Calculate rates - use scanned rate as primary metric (more meaningful when most rows are up-to-date)
        elapsed = snapshot['elapsed']
        avg_scan_rate = snapshot['total_rows_scanned'] / elapsed if elapsed > 0 else 0
        avg_update_rate = snapshot['total_rows_updated'] / elapsed if elapsed > 0 else 0
        # Current rate = sum of all running workers' rates (update rate)
        current_update_rate = sum(r['rows_per_second'] for r in snapshot['running'])
        
        # Build overall progress bar
        bar_width = 40
        filled = int(bar_width * overall_pct / 100)
        overall_bar = "█" * filled + "░" * (bar_width - filled)
        
        # Build progress report
        lines = []
        lines.append("")
        lines.append("=" * 80)
        lines.append(f"DUCKLAKE ROW HASH PROGRESS - {format_duration(snapshot['elapsed'])} elapsed")
        lines.append("=" * 80)
        
        # Overall progress bar with scan rate (primary) and update rate (if any updates happening)
        rate_info = f"Scan: {format_rate(avg_scan_rate)}"
        if snapshot['total_rows_updated'] > 0 or current_update_rate > 0:
            rate_info += f" | Update: {format_rate(current_update_rate)} (avg {format_rate(avg_update_rate)})"
        lines.append(f"Overall: [{overall_bar}] {overall_pct:5.1f}% | {rate_info}")
        lines.append("")
        
        # Summary line
        lines.append(
            f"Tables: {snapshot['completed_count']}/{snapshot['total_tables']} done | "
            f"{snapshot['running_count']} running | "
            f"{snapshot['pending_count']} pending | "
            f"{snapshot['skipped_count']} skipped | "
            f"{snapshot['error_count']} errors"
        )
        lines.append(
            f"Rows: {snapshot['total_rows_scanned']:,} scanned | "
            f"{snapshot['total_rows_updated']:,} updated"
        )
        
        # Just completed tables
        if snapshot['just_completed']:
            lines.append("")
            lines.append(f"✓ Just completed: {', '.join(snapshot['just_completed'][:5])}"
                        + (f" (+{len(snapshot['just_completed'])-5} more)" 
                           if len(snapshot['just_completed']) > 5 else ""))
        
        # Currently running tables with detailed progress (sorted by worker_id)
        if snapshot['running']:
            lines.append("")
            lines.append("Currently running:")
            for r in sorted(snapshot['running'], key=lambda x: x['worker_id']):
                pct = (r['rows_updated'] / r['total_rows'] * 100) if r['total_rows'] > 0 else 0
                eta_str = format_duration(r['eta_seconds']) if r['eta_seconds'] else "calculating..."
                rate_str = format_rate(r['rows_per_second']) if r['rows_per_second'] > 0 else "starting..."
                
                # Progress bar
                bar_width = 20
                filled = int(bar_width * pct / 100)
                bar = "█" * filled + "░" * (bar_width - filled)
                
                lines.append(
                    f"  Worker {r['worker_id']:2d} │ {r['name'][:40]:<40} │ "
                    f"[{bar}] {pct:5.1f}% │ {rate_str:>8} │ "
                    f"{r['rows_updated']:,}/{r['total_rows']:,} │ ETA: {eta_str}"
                )
        
        lines.append("=" * 80)
        
        # Log all lines
        for line in lines:
            log.info(line)


def worker_process_table(
    worker_id: int,
    schema: str,
    table: str,
    progress: GlobalProgress,
) -> dict:
    """
    Worker function to process a single table.
    Each worker gets its own database connection.
    """
    table_name = f"{schema}.{table}"
    stats = {
        "schema": schema,
        "table": table,
        "total_rows": 0,
        "rows_updated": 0,
        "batches": 0,
        "skipped": False,
        "error": None,
    }
    
    # Update progress: starting
    progress.update_table(
        table_name,
        status="running",
        worker_id=worker_id,
        start_time=time.time(),
    )
    
    conn = None
    try:
        conn = get_warehouse_connection()
        # Register connection so it can be cancelled on termination
        progress.register_connection(worker_id, conn)
        target_table = f'"{schema}"."{table}"'
        
        with conn.cursor() as cur:
            # Add hash columns if they don't exist
            cur.execute(f"""
                ALTER TABLE {target_table} 
                ADD COLUMN IF NOT EXISTS _row_hash bytea,
                ADD COLUMN IF NOT EXISTS _row_hash_at timestamptz
            """)
            conn.commit()
            
            # Count total rows needing update (where hash differs or is null)
            cur.execute(f"""
                SELECT COUNT(*) FROM {target_table} t
                WHERE _row_hash IS DISTINCT FROM rowhash_128b(t.*)
            """)
            rows_needing_update = cur.fetchone()[0]
            
            # Also get total rows for reference
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
            stats["total_rows"] = cur.fetchone()[0]
            
            # Update progress with row counts
            progress.update_table(
                table_name,
                total_rows=rows_needing_update,
                rows_remaining=rows_needing_update,
            )
            
            if rows_needing_update == 0:
                stats["skipped"] = True
                progress.mark_skipped(table_name, total_rows=stats["total_rows"])
                return stats
            
            # Process in batches
            total_updated = 0
            batch_count = 0
            batch_start_time = time.time()
            recent_updates = []  # Track recent batch sizes for rate calculation
            terminated_early = False
            
            while True:
                # Check for termination signal before each batch
                if progress.stop_workers:
                    terminated_early = True
                    break
                
                batch_count += 1
                batch_time = time.time()
                
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
                """, (BATCH_SIZE,))
                
                batch_updated = cur.rowcount
                total_updated += batch_updated
                conn.commit()
                
                if batch_updated == 0:
                    break
                
                # Calculate rate (rows per second) using rolling window
                batch_duration = time.time() - batch_time
                if batch_duration > 0:
                    recent_updates.append((batch_updated, batch_duration))
                    # Keep last 10 batches for averaging
                    if len(recent_updates) > 10:
                        recent_updates.pop(0)
                
                # Calculate rolling average rate
                if recent_updates:
                    total_batch_rows = sum(u[0] for u in recent_updates)
                    total_batch_time = sum(u[1] for u in recent_updates)
                    rows_per_second = total_batch_rows / total_batch_time if total_batch_time > 0 else 0
                else:
                    rows_per_second = 0
                
                # Calculate remaining and ETA
                rows_remaining = rows_needing_update - total_updated
                eta_seconds = rows_remaining / rows_per_second if rows_per_second > 0 else None
                
                # Update progress
                progress.update_table(
                    table_name,
                    rows_updated=total_updated,
                    rows_remaining=rows_remaining,
                    rows_per_second=rows_per_second,
                    eta_seconds=eta_seconds,
                )
            
            stats["rows_updated"] = total_updated
            stats["batches"] = batch_count
            stats["terminated"] = terminated_early
            
            # Mark completed (or terminated)
            if terminated_early:
                progress.mark_completed(table_name, total_updated, stats["total_rows"], error="Terminated by user")
            else:
                progress.mark_completed(table_name, total_updated, stats["total_rows"])
            
    except psycopg2.Error as e:
        # Check if this was a query cancellation (from termination signal)
        error_msg = str(e)
        if "cancel" in error_msg.lower() or progress.stop_workers:
            stats["error"] = "Terminated by user"
            stats["terminated"] = True
            progress.mark_completed(table_name, stats.get("rows_updated", 0), stats["total_rows"], error="Terminated by user")
        else:
            stats["error"] = error_msg
            progress.mark_completed(table_name, 0, stats["total_rows"], error=error_msg)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass  # Connection might be in bad state after cancel
    except Exception as e:
        error_msg = "Terminated by user" if progress.stop_workers else str(e)
        stats["error"] = error_msg
        progress.mark_completed(table_name, 0, stats["total_rows"], error=error_msg)
    finally:
        # Unregister and close connection
        progress.unregister_connection(worker_id)
        if conn:
            conn.close()
    
    return stats


def worker_loop(
    worker_id: int,
    table_queue: Queue,
    progress: GlobalProgress,
    results: list,
    results_lock: threading.Lock,
):
    """
    Worker loop that continuously pulls tables from the queue and processes them.
    Exits gracefully when progress.stop_workers is set.
    """
    while True:
        # Check for termination signal
        if progress.stop_workers:
            break
        
        try:
            schema, table = table_queue.get(timeout=1.0)
        except Empty:
            # Check if we should stop (either termination or queue empty)
            if progress.stop_workers or table_queue.empty():
                break
            continue
        
        try:
            stats = worker_process_table(worker_id, schema, table, progress)
            with results_lock:
                results.append(stats)
        finally:
            table_queue.task_done()


def _warehouse_row_hashes_impl(context: dg.AssetExecutionContext) -> dg.Output[None]:
    """
    Implementation of DuckLake row hash computation with parallel workers.
    """
    log = context.log
    
    log.info("Starting DuckLake row hash computation...")
    log.info("")
    
    # Get initial connection to query settings and tables
    conn = get_warehouse_connection()
    try:
        # Ensure extension is available
        ensure_rowhash_extension(conn)
        log.info("✓ rowhash extension verified")
        
        # Get number of workers
        num_workers = get_postgres_worker_count(conn)
        log.info(f"✓ Using {num_workers} parallel workers ({WORKER_FRACTION:.0%} of Postgres max_parallel_workers)")
        
        # Get all tables
        tables = get_all_tables(conn)
        log.info(f"✓ Found {len(tables)} tables to process")
        log.info("")
        
    finally:
        conn.close()
    
    if not tables:
        log.info("No tables to process!")
        return dg.Output(
            None,
            metadata={
                "tables_processed": 0,
                "tables_skipped": 0,
                "tables_with_errors": 0,
                "total_rows_scanned": 0,
                "total_rows_updated": 0,
                "num_workers": num_workers,
            },
        )
    
    # Initialize progress tracker
    progress = GlobalProgress(
        total_tables=len(tables),
        start_time=time.time(),
    )
    
    # Initialize table progress entries
    for schema, table in tables:
        table_name = f"{schema}.{table}"
        progress.tables[table_name] = TableProgress(schema=schema, table=table)
    
    # Create work queue
    table_queue = Queue()
    for schema, table in tables:
        table_queue.put((schema, table))
    
    # Results collection
    results = []
    results_lock = threading.Lock()
    
    # Set up signal handler for graceful termination
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)
    
    def handle_termination(signum, frame):
        """Handle termination signal by stopping all workers gracefully."""
        log.warning("")
        log.warning("=" * 80)
        log.warning("TERMINATION SIGNAL RECEIVED - Stopping all workers...")
        log.warning("=" * 80)
        progress.stop_workers = True
        progress.terminated = True
        # Cancel all active database queries so workers can exit immediately
        progress.cancel_all_queries()
        # Clear the queue so workers don't pick up new tables
        while not table_queue.empty():
            try:
                table_queue.get_nowait()
                table_queue.task_done()
            except Empty:
                break
    
    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)
    
    try:
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=progress_monitor,
            args=(progress, log),
            daemon=True,
        )
        monitor_thread.start()
        
        log.info(f"Starting {num_workers} worker threads...")
        log.info("Progress updates every 5 seconds...")
        log.info("")
        
        # Start worker threads
        workers = []
        for worker_id in range(num_workers):
            t = threading.Thread(
                target=worker_loop,
                args=(worker_id, table_queue, progress, results, results_lock),
            )
            t.start()
            workers.append(t)
        
        # Wait for all workers to complete
        for t in workers:
            t.join()
        
        # Stop monitoring
        progress.stop_monitoring = True
        time.sleep(0.5)  # Give monitor thread time to exit cleanly
    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGTERM, original_sigterm)
        signal.signal(signal.SIGINT, original_sigint)
    
    # Calculate final statistics
    final_snapshot = progress.get_snapshot()
    
    tables_processed = final_snapshot['completed_count']
    tables_skipped = final_snapshot['skipped_count']
    tables_with_errors = final_snapshot['error_count']
    tables_pending = final_snapshot['pending_count']
    total_rows_scanned = final_snapshot['total_rows_scanned']
    total_rows_updated = final_snapshot['total_rows_updated']
    total_time = final_snapshot['elapsed']
    was_terminated = progress.terminated
    
    # Final summary
    log.info("")
    log.info("=" * 80)
    if was_terminated:
        log.warning("DUCKLAKE ROW HASH COMPUTATION TERMINATED!")
        log.warning(f"  Tables NOT processed: {tables_pending}")
    else:
        log.info("DUCKLAKE ROW HASH COMPUTATION COMPLETE!")
    log.info("=" * 80)
    log.info(f"  Workers used:         {num_workers}")
    log.info(f"  Total time:           {format_duration(total_time)}")
    log.info(f"  Tables processed:     {tables_processed}")
    log.info(f"  Tables skipped:       {tables_skipped}")
    log.info(f"  Tables with errors:   {tables_with_errors}")
    log.info(f"  Total rows scanned:   {total_rows_scanned:,}")
    log.info(f"  Total rows updated:   {total_rows_updated:,}")
    if total_time > 0:
        log.info(f"  Scan throughput:      {format_rate(total_rows_scanned / total_time)}")
        if total_rows_updated > 0:
            log.info(f"  Update throughput:    {format_rate(total_rows_updated / total_time)}")
    log.info("=" * 80)
    
    # Log any errors (excluding termination errors which are expected)
    non_termination_errors = [
        (name, tbl.error) for name, tbl in progress.tables.items()
        if tbl.error and tbl.error != "Terminated by user"
    ]
    if non_termination_errors:
        log.warning("")
        log.warning("Tables with errors:")
        for name, error in non_termination_errors:
            log.warning(f"  {name}: {error}")
    
    return dg.Output(
        None,
        metadata={
            "tables_processed": tables_processed,
            "tables_skipped": tables_skipped,
            "tables_with_errors": tables_with_errors,
            "tables_pending": tables_pending,
            "total_rows_scanned": total_rows_scanned,
            "total_rows_updated": total_rows_updated,
            "num_workers": num_workers,
            "total_seconds": total_time,
            "terminated": was_terminated,
        },
    )


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
            f"Parallelized across multiple workers. Depends on {len(dep_asset_keys)} other assets."
        ),
        deps=list(dep_asset_keys) if dep_asset_keys else None,
    )
    def warehouse_row_hashes(context: dg.AssetExecutionContext) -> dg.Output[None]:
        """
        DuckLake row hash computation for the warehouse database.
        
        Runs after all data ingestion assets complete, ensuring row hashes
        are computed on the latest data. Uses the pg_rowhash extension
        (xxHash) to enable efficient incremental sync to DuckLake.
        
        Processing is parallelized based on Postgres max_parallel_workers.
        """
        return _warehouse_row_hashes_impl(context)
    
    return warehouse_row_hashes


# For standalone testing/development, create asset with no dependencies
# The main definitions.py will create the real asset with all dependencies
warehouse_row_hashes = create_warehouse_row_hashes_asset()

# Note: defs is intentionally NOT exported here.
# The asset is created dynamically in the main definitions.py with proper dependencies.


# =============================================================================
# DUCKLAKE SYNC ASSET
# =============================================================================
# Syncs data from PostgreSQL warehouse to DuckLake using partition-based
# hash comparison for efficient change detection at scale (200M+ rows).
# =============================================================================

import duckdb

# Columns to exclude from sync (metadata columns)
SYNC_EXCLUDED_COLUMNS = frozenset({"_row_hash", "_row_hash_at"})


@dataclass
class SyncTableProgress:
    """Progress tracking for a single table sync."""
    schema: str
    table: str
    status: str = "pending"  # pending, syncing, completed, error, skipped
    phase: str = ""  # Current phase: downloading, diffing, deleting, updating, inserting, loading
    rows_deleted: int = 0
    rows_updated: int = 0
    rows_inserted: int = 0
    current_rows: int = 0  # Rows processed so far in current operation
    total_rows: int = 0  # Total rows to process in current operation
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    phase_start_time: Optional[float] = None  # When current phase started
    error: Optional[str] = None
    worker_id: Optional[int] = None
    
    @property
    def table_name(self) -> str:
        return f"{self.schema}.{self.table}"
    
    @property
    def elapsed_seconds(self) -> float:
        if self.start_time is None:
            return 0.0
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time
    
    @property
    def phase_elapsed_seconds(self) -> float:
        if self.phase_start_time is None:
            return 0.0
        return time.time() - self.phase_start_time
    
    @property
    def rows_per_second(self) -> float:
        elapsed = self.phase_elapsed_seconds
        if elapsed <= 0 or self.current_rows <= 0:
            return 0.0
        return self.current_rows / elapsed
    
    @property
    def eta_seconds(self) -> Optional[float]:
        if self.total_rows <= 0 or self.current_rows <= 0:
            return None
        rps = self.rows_per_second
        if rps <= 0:
            return None
        remaining = self.total_rows - self.current_rows
        return remaining / rps
    
    @property
    def progress_pct(self) -> float:
        if self.total_rows <= 0:
            return 0.0
        return min(100.0, (self.current_rows / self.total_rows) * 100)


@dataclass  
class SyncGlobalProgress:
    """Thread-safe global progress tracker for DuckLake sync."""
    tables: Dict[str, SyncTableProgress] = field(default_factory=dict)
    tables_completed: List[str] = field(default_factory=list)
    tables_just_completed: List[str] = field(default_factory=list)
    total_tables: int = 0
    total_rows_deleted: int = 0
    total_rows_updated: int = 0
    total_rows_inserted: int = 0
    start_time: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)
    stop_monitoring: bool = False
    stop_workers: bool = False
    terminated: bool = False
    active_duck_connections: Dict[int, Any] = field(default_factory=dict)  # worker_id -> duckdb connection
    
    def register_duck_connection(self, worker_id: int, conn):
        """Register an active DuckDB connection for a worker."""
        with self.lock:
            self.active_duck_connections[worker_id] = conn
    
    def unregister_duck_connection(self, worker_id: int):
        """Unregister a worker's DuckDB connection."""
        with self.lock:
            self.active_duck_connections.pop(worker_id, None)
    
    def interrupt_all_queries(self):
        """Interrupt all active DuckDB queries to allow workers to exit immediately."""
        with self.lock:
            for worker_id, conn in list(self.active_duck_connections.items()):
                try:
                    conn.interrupt()
                except Exception:
                    pass  # Connection might already be closed
    
    def update_table(self, table_name: str, **kwargs):
        """Thread-safe update of table progress."""
        with self.lock:
            if table_name in self.tables:
                for key, value in kwargs.items():
                    setattr(self.tables[table_name], key, value)
    
    def set_phase(self, table_name: str, phase: str, total_rows: int = 0):
        """Set the current phase and reset progress counters."""
        with self.lock:
            if table_name in self.tables:
                progress = self.tables[table_name]
                progress.phase = phase
                progress.total_rows = total_rows
                progress.current_rows = 0
                progress.phase_start_time = time.time()
    
    def update_progress(self, table_name: str, current_rows: int):
        """Update the current row count for progress tracking."""
        with self.lock:
            if table_name in self.tables:
                self.tables[table_name].current_rows = current_rows
    
    def mark_completed(self, table_name: str, rows_deleted: int, rows_inserted: int, 
                       rows_updated: int = 0, error: str = None):
        """Mark a table as completed."""
        with self.lock:
            if table_name in self.tables:
                progress = self.tables[table_name]
                progress.status = "error" if error else "completed"
                progress.end_time = time.time()
                progress.rows_deleted = rows_deleted
                progress.rows_updated = rows_updated
                progress.rows_inserted = rows_inserted
                progress.error = error
                self.tables_completed.append(table_name)
                self.tables_just_completed.append(table_name)
                self.total_rows_deleted += rows_deleted
                self.total_rows_updated += rows_updated
                self.total_rows_inserted += rows_inserted
    
    def mark_skipped(self, table_name: str):
        """Mark a table as skipped (already in sync)."""
        with self.lock:
            if table_name in self.tables:
                progress = self.tables[table_name]
                progress.status = "skipped"
                progress.end_time = time.time()
                self.tables_completed.append(table_name)
                self.tables_just_completed.append(table_name)
    
    def get_snapshot(self) -> dict:
        """Get a thread-safe snapshot of current progress."""
        with self.lock:
            running = []
            pending_count = 0
            completed_count = 0
            error_count = 0
            skipped_count = 0
            just_completed = self.tables_just_completed.copy()
            self.tables_just_completed.clear()
            
            running_rows_deleted = 0
            running_rows_updated = 0
            running_rows_inserted = 0
            
            for name, progress in self.tables.items():
                if progress.status == "syncing":
                    running.append({
                        "name": name,
                        "worker_id": progress.worker_id,
                        "status": progress.status,
                        "phase": progress.phase,
                        "rows_deleted": progress.rows_deleted,
                        "rows_updated": progress.rows_updated,
                        "rows_inserted": progress.rows_inserted,
                        "current_rows": progress.current_rows,
                        "total_rows": progress.total_rows,
                        "progress_pct": progress.progress_pct,
                        "rows_per_second": progress.rows_per_second,
                        "eta_seconds": progress.eta_seconds,
                        "elapsed": progress.elapsed_seconds,
                    })
                    running_rows_deleted += progress.rows_deleted
                    running_rows_updated += progress.rows_updated
                    running_rows_inserted += progress.rows_inserted
                elif progress.status == "pending":
                    pending_count += 1
                elif progress.status == "completed":
                    completed_count += 1
                elif progress.status == "error":
                    error_count += 1
                elif progress.status == "skipped":
                    skipped_count += 1
            
            return {
                "total_tables": self.total_tables,
                "completed_count": completed_count + skipped_count + error_count,
                "pending_count": pending_count,
                "running_count": len(running),
                "error_count": error_count,
                "skipped_count": skipped_count,
                "running": running,
                "just_completed": just_completed,
                "total_rows_deleted": self.total_rows_deleted + running_rows_deleted,
                "total_rows_updated": self.total_rows_updated + running_rows_updated,
                "total_rows_inserted": self.total_rows_inserted + running_rows_inserted,
                "elapsed": time.time() - self.start_time,
            }


def escape_sql_string(s: str) -> str:
    """Escape single quotes in a string for SQL interpolation."""
    return s.replace("'", "''")


def get_ducklake_connection() -> duckdb.DuckDBPyConnection:
    """
    Get a connection to DuckLake via DuckDB.
    
    Configures DuckDB with:
    - DuckLake extension
    - PostgreSQL extension (for catalog)
    - S3 credentials from environment
    - PostgreSQL catalog with S3 data storage
    
    Environment variables:
    - DUCKLAKE_S3_URL: Full S3 endpoint URL (e.g., https://xxx.r2.cloudflarestorage.com)
    """
    catalog_url = os.environ.get("DUCKLAKE_CATALOG_DB_URL")
    s3_bucket = os.environ.get("DUCKLAKE_S3_BUCKET")
    s3_prefix = os.environ.get("DUCKLAKE_S3_PREFIX", "ducklake_data")
    s3_url = os.environ.get("DUCKLAKE_S3_URL")
    s3_key_id = os.environ.get("DUCKLAKE_S3_KEY_ID")
    s3_secret = os.environ.get("DUCKLAKE_S3_SECRET")
    
    if not catalog_url:
        raise ValueError("DUCKLAKE_CATALOG_DB_URL environment variable not set")
    if not s3_bucket:
        raise ValueError("DUCKLAKE_S3_BUCKET environment variable not set")
    if not s3_url:
        raise ValueError("DUCKLAKE_S3_URL environment variable not set")
    if not s3_key_id or not s3_secret:
        raise ValueError("DUCKLAKE_S3_KEY_ID and DUCKLAKE_S3_SECRET must be set")
    
    conn = duckdb.connect()
    
    # Load required extensions (install only if not already installed)
    for ext in ["ducklake", "postgres", "httpfs"]:
        try:
            conn.execute(f"LOAD {ext}")
        except (duckdb.CatalogException, duckdb.IOException):
            # Extension not installed or not found on disk, try to install it
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
    
    # Configure S3 endpoint (strip protocol prefix)
    s3_endpoint = s3_url.replace("https://", "").replace("http://", "").rstrip("/")
    
    conn.execute(f"""
        CREATE SECRET ducklake_s3 (
            TYPE S3,
            KEY_ID '{escape_sql_string(s3_key_id)}',
            SECRET '{escape_sql_string(s3_secret)}',
            ENDPOINT '{escape_sql_string(s3_endpoint)}',
            USE_SSL true,
            URL_STYLE path
        )
    """)
    
    # S3 path for data storage
    s3_data_path = f"s3://{s3_bucket}/{s3_prefix}"
    
    # Attach DuckLake with PostgreSQL catalog
    # Syntax: ducklake:postgres:connection_string
    # Convert postgres:// URL to libpq format for DuckDB postgres extension
    # postgres://user:pass@host:port/dbname?params -> dbname=... host=... user=... password=... port=...
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(catalog_url)
    
    # Build libpq connection string
    libpq_parts = []
    if parsed.path and parsed.path != '/':
        libpq_parts.append(f"dbname={parsed.path.lstrip('/')}")
    if parsed.hostname:
        libpq_parts.append(f"host={parsed.hostname}")
    if parsed.port:
        libpq_parts.append(f"port={parsed.port}")
    if parsed.username:
        libpq_parts.append(f"user={parsed.username}")
    if parsed.password:
        libpq_parts.append(f"password={parsed.password}")
    # Handle query params like sslmode
    if parsed.query:
        params = parse_qs(parsed.query)
        for key, values in params.items():
            libpq_parts.append(f"{key}={values[0]}")
    
    libpq_string = " ".join(libpq_parts)
    
    conn.execute(f"""
        ATTACH 'ducklake:postgres:{libpq_string}' AS ducklake (
            DATA_PATH '{s3_data_path}'
        )
    """)
    
    # Increase retry count for high-concurrency scenarios
    conn.execute("SET ducklake_max_retry_count = 100")
    
    return conn


def get_table_columns_pg(conn, schema: str, table: str) -> List[Tuple[str, str]]:
    """
    Get column names and types from PostgreSQL table.
    Returns list of (column_name, column_type) tuples.
    Excludes _row_hash and _row_hash_at columns.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = []
        for col_name, data_type, udt_name in cur.fetchall():
            if col_name in SYNC_EXCLUDED_COLUMNS:
                continue
            # Map PostgreSQL types to DuckDB types
            duck_type = pg_type_to_duckdb(data_type, udt_name)
            columns.append((col_name, duck_type))
        return columns


def pg_type_to_duckdb(data_type: str, udt_name: str) -> str:
    """Map PostgreSQL data types to DuckDB types."""
    # Handle array types
    if data_type == "ARRAY":
        base_type = udt_name.lstrip("_")  # _int4 -> int4
        inner_type = pg_type_to_duckdb(base_type, base_type)
        return f"{inner_type}[]"
    
    type_map = {
        # Numeric types
        "smallint": "SMALLINT",
        "int2": "SMALLINT",
        "integer": "INTEGER",
        "int4": "INTEGER",
        "bigint": "BIGINT",
        "int8": "BIGINT",
        "decimal": "DECIMAL",
        "numeric": "DECIMAL",
        "real": "REAL",
        "float4": "REAL",
        "double precision": "DOUBLE",
        "float8": "DOUBLE",
        
        # Character types
        "character varying": "VARCHAR",
        "varchar": "VARCHAR",
        "character": "VARCHAR",
        "char": "VARCHAR",
        "text": "VARCHAR",
        "name": "VARCHAR",
        
        # Binary types
        "bytea": "BLOB",
        
        # Boolean
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        
        # Date/time types
        "date": "DATE",
        "time": "TIME",
        "time without time zone": "TIME",
        "time with time zone": "TIME",
        "timetz": "TIME",
        "timestamp": "TIMESTAMP",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMPTZ",
        "timestamptz": "TIMESTAMPTZ",
        "interval": "INTERVAL",
        
        # UUID
        "uuid": "UUID",
        
        # JSON
        "json": "JSON",
        "jsonb": "JSON",
        
        # Network types (store as VARCHAR in DuckDB)
        "inet": "VARCHAR",
        "cidr": "VARCHAR",
        "macaddr": "VARCHAR",
        "macaddr8": "VARCHAR",
        
        # Other types
        "oid": "UINTEGER",
        "regclass": "VARCHAR",
        "regtype": "VARCHAR",
    }
    
    # Try exact match first
    if data_type.lower() in type_map:
        return type_map[data_type.lower()]
    if udt_name.lower() in type_map:
        return type_map[udt_name.lower()]
    
    # Default to VARCHAR for unknown types
    return "VARCHAR"


def normalize_duckdb_type(conn: duckdb.DuckDBPyConnection, dtype: str) -> str:
    """
    Normalize a type name to DuckDB's canonical form.
    Uses DuckDB itself to resolve type aliases (e.g., TIMESTAMPTZ -> TIMESTAMP WITH TIME ZONE).
    """
    try:
        result = conn.execute(f"SELECT typeof(CAST(NULL AS {dtype}))").fetchone()
        return result[0] if result else dtype
    except Exception:
        # If the type can't be cast (e.g., already canonical or invalid), return as-is
        return dtype


def get_table_columns_ducklake(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> List[Tuple[str, str]]:
    """
    Get column names and types from DuckLake table.
    Returns list of (column_name, column_type) tuples, or empty list if table doesn't exist.
    """
    try:
        result = conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = 'ducklake' 
              AND table_schema = '{schema}'
              AND table_name = '{table}'
            ORDER BY ordinal_position
        """).fetchall()
        return [(row[0], row[1]) for row in result]
    except Exception:
        return []


def table_exists_in_ducklake(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Check if a table exists in DuckLake."""
    try:
        result = conn.execute(f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_catalog = 'ducklake'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
        """).fetchone()
        return result is not None
    except Exception:
        return False


def ensure_schema_exists_ducklake(conn: duckdb.DuckDBPyConnection, schema: str):
    """Ensure schema exists in DuckLake."""
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS ducklake."{schema}"')


def sync_table_schema(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    pk_columns: List[str],
) -> Tuple[bool, List[str], List[dict], dict]:
    """
    Sync table schema from PostgreSQL to DuckLake.
    Creates table if it doesn't exist, handles column additions, removals, and type changes.
    
    Returns (was_created, columns_to_backfill, changes, timing_info).
    
    columns_to_backfill: list of column names that need to be populated from source
    changes: list of schema change dicts with action, column, reason/type info
    """
    table_name = f"{schema}.{table}"
    timing = {"total": 0.0, "ensure_schema": 0.0, "get_src_cols": 0.0, "check_exists": 0.0, "create_or_alter": 0.0}
    total_start = time.time()
    changes = []
    columns_to_backfill = []
    
    # Ensure schema exists
    t0 = time.time()
    ensure_schema_exists_ducklake(duck_conn, schema)
    timing["ensure_schema"] = time.time() - t0
    
    # Get source columns
    t0 = time.time()
    src_columns = get_table_columns_pg(pg_conn, schema, table)
    timing["get_src_cols"] = time.time() - t0
    if not src_columns:
        raise ValueError(f"No columns found in source table {schema}.{table}")
    
    # Check if table exists in DuckLake
    t0 = time.time()
    exists = table_exists_in_ducklake(duck_conn, schema, table)
    timing["check_exists"] = time.time() - t0
    
    t0 = time.time()
    if not exists:
        # Create table with all columns plus _row_hash
        col_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in src_columns])
        col_defs += ", _row_hash BLOB"
        
        duck_conn.execute(f"""
            CREATE TABLE ducklake."{schema}"."{table}" ({col_defs})
        """)
        timing["create_or_alter"] = time.time() - t0
        timing["total"] = time.time() - total_start
        # Return with a special change entry to indicate table was created
        changes.append({"action": "create", "column": "*", "type": f"{len(src_columns)} columns"})
        return True, [], changes, timing
    
    # Table exists - get destination columns for comparison
    dst_columns = get_table_columns_ducklake(duck_conn, schema, table)
    src_map = {name: dtype for name, dtype in src_columns}
    dst_map = {name: dtype for name, dtype in dst_columns}
    
    # Normalize both source and destination types to DuckDB canonical form for comparison
    src_map_normalized = {name: normalize_duckdb_type(duck_conn, dtype) for name, dtype in src_columns}
    dst_map_normalized = {name: normalize_duckdb_type(duck_conn, dtype) for name, dtype in dst_columns}
    
    # Collect columns to drop (removed or type changed)
    columns_to_drop = []
    for col_name, dst_type in dst_columns:
        if col_name == "_row_hash":
            continue
            
        if col_name not in src_map:
            # Column removed from source
            columns_to_drop.append(col_name)
            changes.append({"action": "drop", "column": col_name, "reason": "removed from source"})
            
        elif src_map_normalized[col_name] != dst_map_normalized[col_name]:
            # Type changed - drop and will re-add below
            columns_to_drop.append(col_name)
            changes.append({
                "action": "drop", 
                "column": col_name, 
                "reason": f"type changed: {dst_type} -> {src_map[col_name]}"
            })
    
    # Collect columns to add (new or type changed)
    columns_to_add = []
    for col_name, src_type in src_columns:
        if col_name not in dst_map or col_name in columns_to_drop:
            columns_to_add.append((col_name, src_type))
            changes.append({"action": "add", "column": col_name, "type": src_type})
            columns_to_backfill.append(col_name)
    
    # Ensure _row_hash column exists (required for sync)
    if "_row_hash" not in dst_map:
        columns_to_add.append(("_row_hash", "BLOB"))
        changes.append({"action": "add", "column": "_row_hash", "type": "BLOB"})
    
    # Execute all schema changes in a single transaction to avoid conflicts
    if columns_to_drop or columns_to_add:
        # Drop columns (DuckDB requires one ALTER per statement)
        for col in columns_to_drop:
            duck_conn.execute(f'ALTER TABLE ducklake."{schema}"."{table}" DROP COLUMN "{col}"')
        
        # Add columns (DuckDB requires one ALTER per statement)
        for col, dtype in columns_to_add:
            duck_conn.execute(f'ALTER TABLE ducklake."{schema}"."{table}" ADD COLUMN "{col}" {dtype}')
    
    timing["create_or_alter"] = time.time() - t0
    timing["total"] = time.time() - total_start
    
    return False, columns_to_backfill, changes, timing


def backfill_columns(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    columns: List[str],
    pk_columns: List[str],
) -> Tuple[int, dict]:
    """
    Populate specific columns from source for existing rows.
    Uses PK join if available, otherwise joins on _row_hash.
    
    Returns (rows_updated, timing_info).
    """
    if not columns:
        return 0, {"total": 0.0}
    
    table_name = f"{schema}.{table}"
    timing = {"total": 0.0, "attach": 0.0, "update": 0.0}
    total_start = time.time()
    
    # Ensure warehouse is attached
    t0 = time.time()
    ensure_warehouse_attached(duck_conn)
    timing["attach"] = time.time() - t0
    
    # Build SET clause for just the changed columns
    set_clause = ", ".join([f'"{col}" = src."{col}"' for col in columns])
    
    # Build join condition: PK if available, otherwise _row_hash
    if pk_columns:
        join_condition = " AND ".join([f'dst."{pk}" = src."{pk}"' for pk in pk_columns])
    else:
        join_condition = "dst._row_hash = src._row_hash"
    
    t0 = time.time()
    duck_conn.execute(f"""
        UPDATE ducklake."{schema}"."{table}" dst
        SET {set_clause}
        FROM warehouse."{schema}"."{table}" src
        WHERE {join_condition}
    """)
    timing["update"] = time.time() - t0
    
    result = duck_conn.execute("SELECT changes()").fetchone()
    rows_updated = result[0] if result else 0
    
    timing["total"] = time.time() - total_start
    timing["join_type"] = "PK" if pk_columns else "_row_hash"
    timing["columns"] = columns
    
    return rows_updated, timing


def ensure_warehouse_attached(duck_conn: duckdb.DuckDBPyConnection):
    """
    Ensure the PostgreSQL warehouse is attached to DuckDB for direct queries.
    """
    try:
        duck_conn.execute("SELECT 1 FROM warehouse.information_schema.tables LIMIT 1")
    except:
        warehouse_url = os.environ.get("WAREHOUSE_COOLIFY_URL")
        if not warehouse_url:
            raise ValueError("WAREHOUSE_COOLIFY_URL environment variable not set")
        warehouse_url = warehouse_url.replace('\n', '').replace('\r', '').strip()
        duck_conn.execute(f"ATTACH '{warehouse_url}' AS warehouse (TYPE postgres, READ_ONLY)")


def get_primary_key_columns(pg_conn, schema: str, table: str) -> List[str]:
    """
    Get the primary key column names for a table.
    Returns empty list if no primary key exists.
    """
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """, (f'"{schema}"."{table}"',))
        return [row[0] for row in cur.fetchall()]


def download_sync_metadata(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    progress: Optional[SyncGlobalProgress] = None,
) -> Tuple[List[str], int, dict]:
    """
    Download lightweight sync metadata from PostgreSQL into a temp DuckDB table.
    
    Downloads: primary key columns (if any) + _row_hash + _row_hash_at
    
    Returns (pk_columns, row_count, timing_info).
    """
    table_name = f"{schema}.{table}"
    timing = {"total": 0.0, "get_pk": 0.0, "count_src": 0.0, "drop_temp": 0.0, "attach": 0.0, "download": 0.0, "count": 0.0}
    total_start = time.time()
    
    # Get primary key columns
    t0 = time.time()
    pk_columns = get_primary_key_columns(pg_conn, schema, table)
    timing["get_pk"] = time.time() - t0
    
    # Count rows in source first for progress tracking
    t0 = time.time()
    with pg_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE _row_hash IS NOT NULL')
        expected_rows = cur.fetchone()[0]
    timing["count_src"] = time.time() - t0
    
    # Set download phase with expected row count
    if progress:
        progress.set_phase(table_name, "downloading", expected_rows)
    
    # Build column list for download
    if pk_columns:
        pk_cols_quoted = ", ".join([f'"{c}"' for c in pk_columns])
        select_cols = f"{pk_cols_quoted}, _row_hash, _row_hash_at"
    else:
        select_cols = "_row_hash, _row_hash_at"
    
    # Drop temp table if exists
    t0 = time.time()
    duck_conn.execute("DROP TABLE IF EXISTS _sync_metadata")
    timing["drop_temp"] = time.time() - t0
    
    # Ensure warehouse is attached
    t0 = time.time()
    ensure_warehouse_attached(duck_conn)
    timing["attach"] = time.time() - t0
    
    # Download metadata directly from PostgreSQL into DuckDB temp table
    t0 = time.time()
    duck_conn.execute(f"""
        CREATE TEMP TABLE _sync_metadata AS
        SELECT {select_cols}
        FROM warehouse."{schema}"."{table}"
        WHERE _row_hash IS NOT NULL
    """)
    timing["download"] = time.time() - t0
    
    # Get row count
    t0 = time.time()
    row_count = duck_conn.execute("SELECT COUNT(*) FROM _sync_metadata").fetchone()[0]
    timing["count"] = time.time() - t0
    
    # Update progress with actual downloaded rows
    if progress:
        progress.update_progress(table_name, row_count)
    
    timing["total"] = time.time() - total_start
    
    pk_info = f"PK={pk_columns}" if pk_columns else "no PK"
    logger.info(
        f"  [{table_name}] DOWNLOAD:\n"
        f"    {row_count:,} rows, {pk_info}\n"
        f"    download={timing['download']*1000:.0f}ms, total={timing['total']*1000:.0f}ms"
    )
    
    return pk_columns, row_count, timing


def compute_sync_diff(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    pk_columns: List[str],
) -> Tuple[dict, dict]:
    """
    Compare _sync_metadata (from PostgreSQL) with DuckLake table.
    
    If PK columns exist, compares by PK (enables updates).
    Otherwise, compares by _row_hash only.
    
    Returns (diff_info, timing_info).
    diff_info contains:
    - src_count: rows in source
    - dst_count: rows in destination  
    - to_insert: count of rows to insert (PK/hash in source, not in dest)
    - to_update: count of rows to update (PK in both, hash differs) - only when PK exists
    - to_delete: count of rows to delete (PK/hash in dest, not in source)
    - in_sync: count of rows already in sync
    - change_pct: percentage of source that needs changes
    - pk_columns: the PK columns used for comparison
    """
    table_name = f"{schema}.{table}"
    timing = {"total": 0.0, "src_count": 0.0, "dst_count": 0.0, "count_insert": 0.0, "count_update": 0.0, "count_delete": 0.0}
    total_start = time.time()
    
    # Source count (from temp table)
    t0 = time.time()
    src_count = duck_conn.execute("SELECT COUNT(*) FROM _sync_metadata").fetchone()[0]
    timing["src_count"] = time.time() - t0
    
    # Destination count
    t0 = time.time()
    try:
        dst_count = duck_conn.execute(
            f'SELECT COUNT(*) FROM ducklake."{schema}"."{table}"'
        ).fetchone()[0]
    except:
        dst_count = 0
    timing["dst_count"] = time.time() - t0
    
    if dst_count == 0:
        timing["total"] = time.time() - total_start
        logger.info(f"  [{table_name}] DIFF: dest empty, full insert of {src_count:,} rows, total={timing['total']*1000:.0f}ms")
        return {
            "src_count": src_count,
            "dst_count": 0,
            "to_insert": src_count,
            "to_update": 0,
            "to_delete": 0,
            "in_sync": 0,
            "change_pct": 100.0,
            "pk_columns": pk_columns,
        }, timing
    
    # Build join condition based on PK or hash
    if pk_columns:
        pk_join = " AND ".join([f'src."{c}" = dst."{c}"' for c in pk_columns])
        
        # Count rows to INSERT (PK in source, not in dest)
        t0 = time.time()
        to_insert = duck_conn.execute(f"""
            SELECT COUNT(*) FROM _sync_metadata src
            WHERE NOT EXISTS (
                SELECT 1 FROM ducklake."{schema}"."{table}" dst
                WHERE {pk_join}
            )
        """).fetchone()[0]
        timing["count_insert"] = time.time() - t0
        
        # Count rows to UPDATE (PK in both, hash differs)
        t0 = time.time()
        to_update = duck_conn.execute(f"""
            SELECT COUNT(*) FROM _sync_metadata src
            INNER JOIN ducklake."{schema}"."{table}" dst ON {pk_join}
            WHERE src._row_hash IS DISTINCT FROM dst._row_hash
        """).fetchone()[0]
        timing["count_update"] = time.time() - t0
        
        # Count rows to DELETE (PK in dest, not in source)
        t0 = time.time()
        to_delete = duck_conn.execute(f"""
            SELECT COUNT(*) FROM ducklake."{schema}"."{table}" dst
            WHERE NOT EXISTS (
                SELECT 1 FROM _sync_metadata src
                WHERE {pk_join}
            )
        """).fetchone()[0]
        timing["count_delete"] = time.time() - t0
        
    else:
        # Hash-only comparison (no updates possible)
        
        # Count rows to insert (hash in source, not in dest)
        t0 = time.time()
        to_insert = duck_conn.execute(f"""
            SELECT COUNT(*) FROM _sync_metadata src
            WHERE NOT EXISTS (
                SELECT 1 FROM ducklake."{schema}"."{table}" dst
                WHERE dst._row_hash = src._row_hash
            )
        """).fetchone()[0]
        timing["count_insert"] = time.time() - t0
        
        to_update = 0  # No updates without PK
        
        # Count rows to delete (hash in dest, not in source)
        t0 = time.time()
        to_delete = duck_conn.execute(f"""
            SELECT COUNT(*) FROM ducklake."{schema}"."{table}" dst
            WHERE NOT EXISTS (
                SELECT 1 FROM _sync_metadata src
                WHERE src._row_hash = dst._row_hash
            )
        """).fetchone()[0]
        timing["count_delete"] = time.time() - t0
    
    # Rows in sync
    in_sync = src_count - to_insert - to_update
    
    # Calculate change percentage
    total_changes = to_insert + to_update + to_delete
    change_pct = (total_changes / max(src_count, 1)) * 100
    
    timing["total"] = time.time() - total_start
    
    compare_type = "PK" if pk_columns else "hash"
    logger.info(
        f"  [{table_name}] DIFF ({compare_type}):\n"
        f"    src={src_count:,}, dst={dst_count:,}\n"
        f"    insert={to_insert:,}, update={to_update:,}, delete={to_delete:,}, change={change_pct:.1f}%\n"
        f"    times: ins={timing['count_insert']*1000:.0f}ms, upd={timing['count_update']*1000:.0f}ms, "
        f"del={timing['count_delete']*1000:.0f}ms, total={timing['total']*1000:.0f}ms"
    )
    
    return {
        "src_count": src_count,
        "dst_count": dst_count,
        "to_insert": to_insert,
        "to_update": to_update,
        "to_delete": to_delete,
        "in_sync": in_sync,
        "change_pct": change_pct,
        "pk_columns": pk_columns,
    }, timing


class TerminatedException(Exception):
    """Raised when sync is terminated by user."""
    pass


def check_termination(progress: Optional[SyncGlobalProgress]):
    """Check if sync has been terminated and raise exception if so."""
    if progress and progress.stop_workers:
        raise TerminatedException("Sync terminated by user")


def sync_table_incremental(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    column_names: List[str],
    full_resync_threshold: float = 80.0,
    progress: Optional[SyncGlobalProgress] = None,
) -> Tuple[int, int, int, bool, dict]:
    """
    Incrementally sync a table from PostgreSQL to DuckLake.
    
    Strategy:
    1. Download lightweight metadata (PK + _row_hash + _row_hash_at)
    2. Compare with DuckLake to determine changes (by PK if available, else by hash)
    3. If change % > threshold, do full resync
    4. Otherwise, sync just the changes using UPDATE for changed rows (if PK exists)
    
    Returns (rows_deleted, rows_updated, rows_inserted, was_full_resync, timing_info).
    
    Raises TerminatedException if sync is cancelled.
    """
    table_name = f"{schema}.{table}"
    timing = {
        "total": 0.0,
        "download_metadata": 0.0,
        "compute_diff": 0.0,
        "clear_dest": 0.0,
        "bulk_load": 0.0,
        "dedup_check": 0.0,
        "dedup_delete": 0.0,
        "delete_op": 0.0,
        "update_op": 0.0,
        "insert_op": 0.0,
        "cleanup": 0.0,
    }
    total_start = time.time()
    
    # Step 1: Download metadata (includes progress tracking)
    check_termination(progress)
    t0 = time.time()
    pk_columns, src_count, download_timing = download_sync_metadata(pg_conn, duck_conn, schema, table, progress=progress)
    timing["download_metadata"] = time.time() - t0
    
    # Step 2: Compute diff (PK-aware)
    check_termination(progress)
    if progress:
        progress.set_phase(table_name, "diffing", src_count)
    t0 = time.time()
    diff, diff_timing = compute_sync_diff(duck_conn, schema, table, pk_columns)
    timing["compute_diff"] = time.time() - t0
    
    # Step 3: Decide full vs incremental sync
    check_termination(progress)
    if diff['dst_count'] == 0 or diff['change_pct'] >= full_resync_threshold:
        # Full resync
        logger.info(f"  [{table_name}] FULL RESYNC: change_pct={diff['change_pct']:.1f}% >= {full_resync_threshold}%")
        
        # Clear destination
        t0 = time.time()
        if diff['dst_count'] > 0:
            if progress:
                progress.set_phase(table_name, "clearing", diff['dst_count'])
            duck_conn.execute(f'DELETE FROM ducklake."{schema}"."{table}"')
        timing["clear_dest"] = time.time() - t0
        
        # Bulk load
        check_termination(progress)
        t0 = time.time()
        rows_inserted, bulk_timing = bulk_load_table(pg_conn, duck_conn, schema, table, column_names, progress=progress)
        timing["bulk_load"] = time.time() - t0
        
        # Cleanup temp table
        t0 = time.time()
        duck_conn.execute("DROP TABLE IF EXISTS _sync_metadata")
        timing["cleanup"] = time.time() - t0
        
        timing["total"] = time.time() - total_start
        
        logger.info(
            f"  [{table_name}] SYNC COMPLETE (full):\n"
            f"    {rows_inserted:,} rows loaded\n"
            f"    times: download={timing['download_metadata']*1000:.0f}ms, diff={timing['compute_diff']*1000:.0f}ms, "
            f"clear={timing['clear_dest']*1000:.0f}ms, load={timing['bulk_load']*1000:.0f}ms\n"
            f"    TOTAL={timing['total']:.1f}s"
        )
        
        return diff['dst_count'], 0, rows_inserted, True, timing
    
    # Step 3.5: Clean up duplicate PKs in destination (if PK exists)
    dups_removed = 0
    if pk_columns and diff['dst_count'] > 0:
        pk_cols_quoted = ", ".join([f'"{c}"' for c in pk_columns])
        
        # Count PKs with duplicates
        t0 = time.time()
        dup_count = duck_conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_cols_quoted}
                FROM ducklake."{schema}"."{table}"
                GROUP BY {pk_cols_quoted}
                HAVING COUNT(*) > 1
            ) sub
        """).fetchone()[0]
        timing["dedup_check"] = time.time() - t0
        
        if dup_count > 0:
            logger.info(f"  [{table_name}] DEDUP: cleaning up {dup_count} duplicate PKs...")
            
            # Delete all but one row per PK using rowid
            # DuckLake uses rowid internally - find duplicates and delete extras
            t0 = time.time()
            duck_conn.execute(f"""
                DELETE FROM ducklake."{schema}"."{table}"
                WHERE rowid IN (
                    SELECT rowid FROM (
                        SELECT rowid,
                               ROW_NUMBER() OVER (PARTITION BY {pk_cols_quoted} ORDER BY _row_hash) as rn
                        FROM ducklake."{schema}"."{table}"
                    ) sub
                    WHERE rn > 1
                )
            """)
            timing["dedup_delete"] = time.time() - t0
            
            dups_removed = dup_count
            logger.info(f"  [{table_name}] DEDUP: cleaned {dups_removed} duplicates in {timing['dedup_delete']*1000:.0f}ms")
    
    # Step 3.6: Clean up duplicate hashes in destination (hash-only mode, no PK)
    elif not pk_columns and diff['dst_count'] > 0:
        # Count hashes with duplicates
        t0 = time.time()
        dup_count = duck_conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT _row_hash
                FROM ducklake."{schema}"."{table}"
                GROUP BY _row_hash
                HAVING COUNT(*) > 1
            ) sub
        """).fetchone()[0]
        timing["dedup_check"] = time.time() - t0
        
        if dup_count > 0:
            logger.info(f"  [{table_name}] DEDUP (hash): cleaning up {dup_count} duplicate hashes...")
            
            # Delete all but one row per hash using rowid
            t0 = time.time()
            duck_conn.execute(f"""
                DELETE FROM ducklake."{schema}"."{table}"
                WHERE rowid IN (
                    SELECT rowid FROM (
                        SELECT rowid,
                               ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _row_hash) as rn
                        FROM ducklake."{schema}"."{table}"
                    ) sub
                    WHERE rn > 1
                )
            """)
            timing["dedup_delete"] = time.time() - t0
            
            dups_removed = dup_count
            logger.info(f"  [{table_name}] DEDUP (hash): cleaned {dups_removed} duplicates in {timing['dedup_delete']*1000:.0f}ms")
    
    # Step 4: Incremental sync
    check_termination(progress)
    rows_deleted = 0
    rows_updated = 0
    rows_inserted = 0
    
    # Ensure warehouse is attached for data fetch
    ensure_warehouse_attached(duck_conn)
    
    # Build column list
    cols_quoted = ", ".join([f'"{c}"' for c in column_names])
    cols_with_hash = cols_quoted + ", _row_hash"
    
    if pk_columns:
        # PK-based sync: DELETE, UPDATE, INSERT
        pk_join_meta = " AND ".join([f'src."{c}" = meta."{c}"' for c in pk_columns])
        pk_join_dst = " AND ".join([f'src."{c}" = dst."{c}"' for c in pk_columns])
        pk_cols_quoted = ", ".join([f'"{c}"' for c in pk_columns])
        
        # DELETE rows where PK is in dest but not in source
        check_termination(progress)
        if diff['to_delete'] > 0:
            if progress:
                progress.set_phase(table_name, "deleting", diff['to_delete'])
            t0 = time.time()
            pk_join_del = " AND ".join([f'dst."{c}" = meta."{c}"' for c in pk_columns])
            duck_conn.execute(f"""
                DELETE FROM ducklake."{schema}"."{table}" dst
                WHERE NOT EXISTS (
                    SELECT 1 FROM _sync_metadata meta
                    WHERE {pk_join_del}
                )
            """)
            timing["delete_op"] = time.time() - t0
            rows_deleted = diff['to_delete']
            if progress:
                progress.update_progress(table_name, rows_deleted)
            logger.info(
                f"  [{table_name}] DELETE (PK): {rows_deleted:,} rows in {timing['delete_op']*1000:.0f}ms "
                f"({rows_deleted/(timing['delete_op']+0.001):.0f} rows/sec)"
            )
        
        # UPDATE rows where PK is in both but hash differs
        check_termination(progress)
        if diff['to_update'] > 0:
            if progress:
                progress.set_phase(table_name, "updating", diff['to_update'])
            t0 = time.time()
            
            # Build SET clause for all columns
            set_clause = ", ".join([f'"{c}" = src."{c}"' for c in column_names])
            set_clause += ', _row_hash = src._row_hash'
            
            duck_conn.execute(f"""
                UPDATE ducklake."{schema}"."{table}" dst
                SET {set_clause}
                FROM warehouse."{schema}"."{table}" src
                WHERE {pk_join_dst}
                AND dst._row_hash IS DISTINCT FROM src._row_hash
                AND EXISTS (
                    SELECT 1 FROM _sync_metadata meta
                    WHERE {pk_join_meta}
                )
            """)
            timing["update_op"] = time.time() - t0
            rows_updated = diff['to_update']
            if progress:
                progress.update_progress(table_name, rows_updated)
            logger.info(
                f"  [{table_name}] UPDATE (PK): {rows_updated:,} rows in {timing['update_op']*1000:.0f}ms "
                f"({rows_updated/(timing['update_op']+0.001):.0f} rows/sec)"
            )
        
        # INSERT rows where PK is in source but not in dest
        check_termination(progress)
        if diff['to_insert'] > 0:
            if progress:
                progress.set_phase(table_name, "inserting", diff['to_insert'])
            t0 = time.time()
            
            duck_conn.execute(f"""
                INSERT INTO ducklake."{schema}"."{table}" ({cols_with_hash})
                SELECT {cols_with_hash}
                FROM warehouse."{schema}"."{table}" src
                WHERE src._row_hash IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM _sync_metadata meta
                    WHERE {pk_join_meta}
                )
                AND NOT EXISTS (
                    SELECT 1 FROM ducklake."{schema}"."{table}" dst
                    WHERE {pk_join_dst}
                )
            """)
            timing["insert_op"] = time.time() - t0
            rows_inserted = diff['to_insert']
            if progress:
                progress.update_progress(table_name, rows_inserted)
            logger.info(
                f"  [{table_name}] INSERT (PK): {rows_inserted:,} rows in {timing['insert_op']*1000:.0f}ms "
                f"({rows_inserted/(timing['insert_op']+0.001):.0f} rows/sec)"
            )
    
    else:
        # Hash-only sync: DELETE and INSERT (no updates)
        
        # Delete rows not in source
        check_termination(progress)
        if diff['to_delete'] > 0:
            if progress:
                progress.set_phase(table_name, "deleting", diff['to_delete'])
            t0 = time.time()
            duck_conn.execute(f"""
                DELETE FROM ducklake."{schema}"."{table}" dst
                WHERE NOT EXISTS (
                    SELECT 1 FROM _sync_metadata src
                    WHERE src._row_hash = dst._row_hash
                )
            """)
            timing["delete_op"] = time.time() - t0
            rows_deleted = diff['to_delete']
            if progress:
                progress.update_progress(table_name, rows_deleted)
            logger.info(
                f"  [{table_name}] DELETE (hash): {rows_deleted:,} rows in {timing['delete_op']*1000:.0f}ms "
                f"({rows_deleted/(timing['delete_op']+0.001):.0f} rows/sec)"
            )
        
        # Insert new rows
        check_termination(progress)
        if diff['to_insert'] > 0:
            if progress:
                progress.set_phase(table_name, "inserting", diff['to_insert'])
            t0 = time.time()
            
            # Insert with deduplication
            duck_conn.execute(f"""
                INSERT INTO ducklake."{schema}"."{table}" ({cols_with_hash})
                SELECT {cols_with_hash}
                FROM (
                    SELECT {cols_with_hash},
                           ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _row_hash) as rn
                    FROM warehouse."{schema}"."{table}" src
                    WHERE src._row_hash IS NOT NULL
                    AND EXISTS (
                        SELECT 1 FROM _sync_metadata meta
                        WHERE meta._row_hash = src._row_hash
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM ducklake."{schema}"."{table}" dst
                        WHERE dst._row_hash = src._row_hash
                    )
                ) sub
                WHERE rn = 1
            """)
            timing["insert_op"] = time.time() - t0
            # Get actual inserted count (may differ from to_insert due to dedup)
            result = duck_conn.execute("SELECT changes()").fetchone()
            rows_inserted = result[0] if result else diff['to_insert']
            if progress:
                progress.update_progress(table_name, rows_inserted)
            logger.info(
                f"  [{table_name}] INSERT (hash): {rows_inserted:,} rows in {timing['insert_op']*1000:.0f}ms "
                f"({rows_inserted/(timing['insert_op']+0.001):.0f} rows/sec)"
            )
    
    # Cleanup temp table
    t0 = time.time()
    duck_conn.execute("DROP TABLE IF EXISTS _sync_metadata")
    timing["cleanup"] = time.time() - t0
    
    timing["total"] = time.time() - total_start
    
    # Summary line
    ops_summary = []
    if rows_deleted > 0:
        ops_summary.append(f"del={rows_deleted:,}")
    if rows_updated > 0:
        ops_summary.append(f"upd={rows_updated:,}")
    if rows_inserted > 0:
        ops_summary.append(f"ins={rows_inserted:,}")
    ops_str = ", ".join(ops_summary) if ops_summary else "no changes"
    
    logger.info(
        f"  [{table_name}] SYNC COMPLETE (incr):\n"
        f"    {ops_str}\n"
        f"    times: download={timing['download_metadata']*1000:.0f}ms, diff={timing['compute_diff']*1000:.0f}ms, "
        f"del={timing['delete_op']*1000:.0f}ms, upd={timing['update_op']*1000:.0f}ms, ins={timing['insert_op']*1000:.0f}ms\n"
        f"    TOTAL={timing['total']:.1f}s"
    )
    
    return rows_deleted, rows_updated, rows_inserted, False, timing


def bulk_load_table(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    column_names: List[str],
    batch_size: int = 10000,  # Not used anymore, kept for API compatibility
    progress: Optional[SyncGlobalProgress] = None,
) -> Tuple[int, dict]:
    """
    Bulk load all rows from PostgreSQL to DuckLake using DuckDB's native PostgreSQL integration.
    Much faster than row-by-row inserts.
    
    Returns (total_rows_inserted, timing_info).
    """
    table_name = f"{schema}.{table}"
    timing = {"total": 0.0, "count_src": 0.0, "attach": 0.0, "insert": 0.0, "verify": 0.0}
    total_start = time.time()
    
    cols_quoted = ", ".join([f'"{c}"' for c in column_names])
    cols_with_hash = cols_quoted + ", _row_hash"
    
    # Count total rows for progress
    t0 = time.time()
    with pg_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE _row_hash IS NOT NULL')
        total_rows = cur.fetchone()[0]
    timing["count_src"] = time.time() - t0
    
    if total_rows == 0:
        timing["total"] = time.time() - total_start
        logger.info(f"  [{table_name}] BULK LOAD: 0 rows (empty source)")
        return 0, timing
    
    # Get warehouse connection string
    warehouse_url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable not set")
    warehouse_url = warehouse_url.replace('\n', '').replace('\r', '').strip()
    
    # Check if warehouse is already attached
    t0 = time.time()
    try:
        duck_conn.execute("SELECT 1 FROM warehouse.information_schema.tables LIMIT 1")
    except:
        # Attach PostgreSQL warehouse directly to DuckDB for fast bulk loading
        duck_conn.execute(f"ATTACH '{warehouse_url}' AS warehouse (TYPE postgres, READ_ONLY)")
    timing["attach"] = time.time() - t0
    
    # Set phase for progress tracking
    if progress:
        progress.set_phase(table_name, "loading", total_rows)
    
    # Bulk insert directly from PostgreSQL - much faster than row-by-row
    t0 = time.time()
    insert_sql = f"""
        INSERT INTO ducklake."{schema}"."{table}" ({cols_with_hash})
        SELECT {cols_with_hash} 
        FROM warehouse."{schema}"."{table}"
        WHERE _row_hash IS NOT NULL
    """
    
    duck_conn.execute(insert_sql)
    timing["insert"] = time.time() - t0
    
    # Verify count
    t0 = time.time()
    result = duck_conn.execute(f'SELECT COUNT(*) FROM ducklake."{schema}"."{table}"').fetchone()
    rows_inserted = result[0] if result else 0
    timing["verify"] = time.time() - t0
    
    # Mark progress as complete
    if progress:
        progress.update_progress(table_name, rows_inserted)
    
    timing["total"] = time.time() - total_start
    
    rows_per_sec = rows_inserted / (timing["insert"] + 0.001)
    logger.info(
        f"  [{table_name}] BULK LOAD:\n"
        f"    {rows_inserted:,} rows in {timing['insert']*1000:.0f}ms ({rows_per_sec:.0f} rows/sec)\n"
        f"    verify={timing['verify']*1000:.0f}ms, total={timing['total']:.1f}s"
    )
    
    return rows_inserted, timing


def sync_table(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    progress: SyncGlobalProgress,
    worker_id: int,
) -> dict:
    """
    Sync a single table from PostgreSQL to DuckLake.
    
    Strategy:
    1. Download lightweight metadata (PK + _row_hash + _row_hash_at) from PostgreSQL
    2. Compare with DuckLake to determine changes
    3. If change % > threshold, do full resync
    4. Otherwise, sync just the changes (delete stale, insert new)
    """
    table_name = f"{schema}.{table}"
    table_start = time.time()
    
    stats = {
        "schema": schema,
        "table": table,
        "rows_deleted": 0,
        "rows_inserted": 0,
        "rows_backfilled": 0,
        "error": None,
        "created": False,
        "full_resync": False,
        "schema_changes": [],
        "timing": {},
    }
    
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"[Worker {worker_id}] STARTING: {table_name}")
        logger.info(f"{'='*60}")
        
        # Update progress: starting
        progress.update_table(
            table_name,
            status="syncing",
            worker_id=worker_id,
            start_time=time.time(),
        )
        
        # Get PK columns first (needed for schema sync and backfill)
        t0 = time.time()
        pk_columns = get_primary_key_columns(pg_conn, schema, table)
        stats["timing"]["get_pk"] = time.time() - t0
        
        # Sync schema - returns columns that need backfill
        progress.set_phase(table_name, "schema sync")
        t0 = time.time()
        was_created, columns_to_backfill, schema_changes, schema_timing = sync_table_schema(
            pg_conn, duck_conn, schema, table, pk_columns
        )
        stats["created"] = was_created
        stats["schema_changes"] = schema_changes
        stats["timing"]["schema"] = time.time() - t0
        
        # Log schema changes
        for change in schema_changes:
            if change["action"] == "drop":
                logger.info(f"  [{table_name}] SCHEMA: {change['action']} {change['column']} ({change.get('reason', '')})")
            else:
                logger.info(f"  [{table_name}] SCHEMA: {change['action']} {change['column']} ({change.get('type', '')})")
        
        # Backfill new/changed columns if needed (skip if table was just created)
        if columns_to_backfill and not was_created:
            progress.set_phase(table_name, f"backfill {len(columns_to_backfill)} cols")
            t0 = time.time()
            rows_backfilled, backfill_timing = backfill_columns(
                duck_conn, schema, table, columns_to_backfill, pk_columns
            )
            stats["rows_backfilled"] = rows_backfilled
            stats["timing"]["backfill"] = time.time() - t0
            logger.info(
                f"  [{table_name}] BACKFILL ({backfill_timing.get('join_type', 'unknown')}): "
                f"updated {rows_backfilled:,} rows for columns: {columns_to_backfill}"
            )
        
        # Get column names for data sync
        t0 = time.time()
        columns = get_table_columns_pg(pg_conn, schema, table)
        column_names = [col[0] for col in columns]
        stats["timing"]["get_columns"] = time.time() - t0
        
        # Smart incremental sync with metadata comparison
        rows_deleted, rows_updated, rows_inserted, was_full_resync, sync_timing = sync_table_incremental(
            pg_conn, duck_conn, schema, table, column_names, progress=progress
        )
        
        stats["rows_deleted"] = rows_deleted
        stats["rows_updated"] = rows_updated
        stats["rows_inserted"] = rows_inserted
        stats["full_resync"] = was_full_resync
        stats["timing"]["sync"] = sync_timing
        
        table_total = time.time() - table_start
        stats["timing"]["total"] = table_total
        
        if rows_deleted == 0 and rows_updated == 0 and rows_inserted == 0:
            # Table is already in sync
            progress.mark_skipped(table_name)
            stats["skipped"] = True
            logger.info(f"[Worker {worker_id}] DONE: {table_name} - already in sync ({table_total:.1f}s total)")
        else:
            progress.mark_completed(
                table_name,
                rows_deleted=rows_deleted,
                rows_inserted=rows_inserted,
                rows_updated=rows_updated,
            )
            sync_type = "FULL" if was_full_resync else "INCREMENTAL"
            logger.info(f"[Worker {worker_id}] DONE: {table_name} - {sync_type} sync complete ({table_total:.1f}s total)")
        
        logger.info(f"{'='*60}\n")
    
    except (TerminatedException, duckdb.InterruptException):
        # Sync was terminated/interrupted by user
        logger.info(f"[Worker {worker_id}] TERMINATED: {table_name}")
        stats["error"] = "Terminated by user"
        stats["terminated"] = True
        stats["timing"]["total"] = time.time() - table_start
        progress.mark_completed(table_name, 0, 0, error="Terminated by user")
        logger.info(f"{'='*60}\n")
        # Re-raise so the worker loop knows to stop
        raise
        
    except Exception as e:
        logger.exception(f"[Worker {worker_id}] ERROR: {table_name}")
        stats["error"] = str(e)
        stats["timing"]["total"] = time.time() - table_start
        progress.mark_completed(table_name, 0, 0, error=str(e))
        logger.info(f"{'='*60}\n")
    
    return stats


def sync_worker_loop(
    worker_id: int,
    table_queue: Queue,
    progress: SyncGlobalProgress,
    results: list,
    results_lock: threading.Lock,
):
    """
    Worker loop for syncing tables to DuckLake.
    Each worker maintains its own connections.
    """
    pg_conn = None
    duck_conn = None
    
    try:
        pg_conn = get_warehouse_connection()
        duck_conn = get_ducklake_connection()
        
        # Register connection so it can be interrupted on termination
        progress.register_duck_connection(worker_id, duck_conn)
        
        while True:
            if progress.stop_workers:
                break
            
            try:
                schema, table = table_queue.get(timeout=1.0)
            except Empty:
                if progress.stop_workers or table_queue.empty():
                    break
                continue
            
            try:
                stats = sync_table(pg_conn, duck_conn, schema, table, progress, worker_id)
                with results_lock:
                    results.append(stats)
            except (TerminatedException, duckdb.InterruptException):
                # Query was interrupted/terminated - stop this worker
                break
            finally:
                table_queue.task_done()
    finally:
        progress.unregister_duck_connection(worker_id)
        if pg_conn:
            pg_conn.close()
        if duck_conn:
            duck_conn.close()


def sync_progress_monitor(progress: SyncGlobalProgress, log):
    """
    Monitoring thread for DuckLake sync progress.
    """
    while not progress.stop_monitoring:
        time.sleep(PROGRESS_UPDATE_INTERVAL)
        
        if progress.stop_monitoring:
            break
        
        snapshot = progress.get_snapshot()
        
        # Calculate overall progress
        total_tables = snapshot['total_tables']
        completed_count = snapshot['completed_count']
        overall_pct = (completed_count / total_tables * 100) if total_tables > 0 else 0
        
        # Build progress bar
        bar_width = 40
        filled = int(bar_width * overall_pct / 100)
        overall_bar = "█" * filled + "░" * (bar_width - filled)
        
        lines = []
        lines.append("")
        lines.append("=" * 80)
        lines.append(f"DUCKLAKE SYNC PROGRESS - {format_duration(snapshot['elapsed'])} elapsed")
        lines.append("=" * 80)
        lines.append(f"Overall: [{overall_bar}] {overall_pct:5.1f}%")
        lines.append("")
        
        lines.append(
            f"Tables: {snapshot['completed_count']}/{snapshot['total_tables']} done | "
            f"{snapshot['running_count']} running | "
            f"{snapshot['pending_count']} pending | "
            f"{snapshot['skipped_count']} in-sync | "
            f"{snapshot['error_count']} errors"
        )
        lines.append(
            f"Rows: {snapshot['total_rows_deleted']:,} deleted | "
            f"{snapshot['total_rows_updated']:,} updated | "
            f"{snapshot['total_rows_inserted']:,} inserted"
        )
        
        if snapshot['just_completed']:
            lines.append("")
            lines.append(f"✓ Just completed: {', '.join(snapshot['just_completed'][:5])}"
                        + (f" (+{len(snapshot['just_completed'])-5} more)" 
                           if len(snapshot['just_completed']) > 5 else ""))
        
        if snapshot['running']:
            lines.append("")
            lines.append("Currently running:")
            for r in sorted(snapshot['running'], key=lambda x: x['worker_id']):
                elapsed_str = f"{r['elapsed']:.0f}s" if r['elapsed'] < 60 else f"{r['elapsed']/60:.1f}m"
                
                # Build progress info
                phase = r.get('phase', '')
                current = r.get('current_rows', 0)
                total = r.get('total_rows', 0)
                rps = r.get('rows_per_second', 0)
                eta = r.get('eta_seconds')
                pct = r.get('progress_pct', 0)
                
                # Build progress bar (10 chars)
                bar_width = 10
                if total > 0:
                    filled = int(bar_width * pct / 100)
                    progress_bar = "█" * filled + "░" * (bar_width - filled)
                    eta_str = format_duration(eta) if eta and eta > 0 else "..."
                    progress_str = f"[{progress_bar}] {current:,}/{total:,} │ {rps:,.0f}/s │ ETA: {eta_str}"
                else:
                    # Show animated spinner for phases without row counts
                    spinner_frames = ["◐", "◓", "◑", "◒"]
                    spinner_idx = int(r['elapsed'] * 2) % len(spinner_frames)
                    spinner = spinner_frames[spinner_idx]
                    phase_display = phase if phase else "starting"
                    progress_str = f"{spinner} {phase_display}..."
                
                lines.append(
                    f"  Worker {r['worker_id']:2d} │ {r['name'][:35]:<35} │ {elapsed_str:>6} │ {progress_str}"
                )
        
        lines.append("=" * 80)
        
        for line in lines:
            log.info(line)


def _ducklake_sync_impl(context: dg.AssetExecutionContext) -> dg.Output[None]:
    """
    Implementation of DuckLake sync with partition-based change detection.
    """
    log = context.log
    
    log.info("Starting DuckLake sync...")
    log.info("")
    
    # Verify connections
    log.info("Verifying connections...")
    
    pg_conn = get_warehouse_connection()
    try:
        tables = get_all_tables(pg_conn)
        log.info(f"✓ PostgreSQL warehouse: {len(tables)} tables found")
    finally:
        pg_conn.close()
    
    # Apply debug filter if enabled
    if DEBUG_SYNC_TABLE_PATTERNS is not None:
        log.warning("=" * 60)
        log.warning("DEBUG MODE: Filtering tables by patterns")
        log.warning(f"  Patterns: {DEBUG_SYNC_TABLE_PATTERNS}")
        log.warning("=" * 60)
        
        original_count = len(tables)
        filtered_tables = []
        for schema, table in tables:
            table_name = f"{schema}.{table}"
            for pattern in DEBUG_SYNC_TABLE_PATTERNS:
                if re.match(pattern, table_name):
                    filtered_tables.append((schema, table))
                    break
        tables = filtered_tables
        log.warning(f"  Filtered: {original_count} -> {len(tables)} tables")
        log.warning("")
    
    duck_conn = get_ducklake_connection()
    try:
        duck_conn.execute("SELECT 1")
        log.info("✓ DuckLake connection verified")
    finally:
        duck_conn.close()
    
    log.info("")
    
    if not tables:
        log.info("No tables to sync!")
        return dg.Output(
            None,
            metadata={
                "tables_synced": 0,
                "tables_skipped": 0,
                "tables_with_errors": 0,
                "total_rows_deleted": 0,
                "total_rows_inserted": 0,
            },
        )
    
    # Get worker count (use same logic as row hash computation)
    pg_conn = get_warehouse_connection()
    try:
        num_workers = get_postgres_worker_count(pg_conn)
    finally:
        pg_conn.close()
    
    # Pre-create all unique schemas to avoid transaction conflicts between workers
    unique_schemas = sorted(set(schema for schema, _ in tables))
    if unique_schemas:
        log.info(f"Pre-creating {len(unique_schemas)} schemas: {', '.join(unique_schemas)}")
        duck_conn = get_ducklake_connection()
        try:
            for schema in unique_schemas:
                try:
                    duck_conn.execute(f'CREATE SCHEMA IF NOT EXISTS ducklake."{schema}"')
                    log.info(f"  ✓ Schema: {schema}")
                except Exception as e:
                    # Schema might already exist from a previous run
                    log.warning(f"  Schema {schema}: {str(e)[:50]}")
        finally:
            duck_conn.close()
        log.info("")
    
    # Initialize progress tracker
    progress = SyncGlobalProgress(
        total_tables=len(tables),
        start_time=time.time(),
    )
    
    for schema, table in tables:
        table_name = f"{schema}.{table}"
        progress.tables[table_name] = SyncTableProgress(schema=schema, table=table)
    
    # Create work queue
    table_queue = Queue()
    for schema, table in tables:
        table_queue.put((schema, table))
    
    results = []
    results_lock = threading.Lock()
    
    # Set up signal handlers
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)
    
    def handle_termination(signum, frame):
        log.warning("")
        log.warning("=" * 80)
        log.warning("TERMINATION SIGNAL RECEIVED - Stopping sync...")
        log.warning("=" * 80)
        progress.stop_workers = True
        progress.terminated = True
        # Interrupt all active DuckDB queries so workers exit immediately
        progress.interrupt_all_queries()
        # Clear the queue so workers don't pick up new tables
        while not table_queue.empty():
            try:
                table_queue.get_nowait()
                table_queue.task_done()
            except Empty:
                break
    
    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)
    
    try:
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=sync_progress_monitor,
            args=(progress, log),
            daemon=True,
        )
        monitor_thread.start()
        
        log.info(f"Starting {num_workers} sync workers ({WORKER_FRACTION:.0%} of Postgres max_parallel_workers)...")
        log.info("Progress updates every 5 seconds...")
        log.info("")
        
        # Start workers
        workers = []
        for worker_id in range(num_workers):
            t = threading.Thread(
                target=sync_worker_loop,
                args=(worker_id, table_queue, progress, results, results_lock),
            )
            t.start()
            workers.append(t)
        
        # Wait for completion with periodic timeout to allow signal handling
        while any(t.is_alive() for t in workers):
            for t in workers:
                t.join(timeout=0.5)
            # If terminated, break out to allow cleanup
            if progress.terminated:
                # Give workers a moment to finish current operations
                for t in workers:
                    t.join(timeout=2.0)
                break
        
        progress.stop_monitoring = True
        time.sleep(0.5)
    finally:
        signal.signal(signal.SIGTERM, original_sigterm)
        signal.signal(signal.SIGINT, original_sigint)
    
    # Final statistics
    final_snapshot = progress.get_snapshot()
    
    tables_synced = sum(1 for t in progress.tables.values() if t.status == "completed")
    tables_skipped = final_snapshot['skipped_count']
    tables_with_errors = final_snapshot['error_count']
    total_deleted = final_snapshot['total_rows_deleted']
    total_updated = final_snapshot['total_rows_updated']
    total_inserted = final_snapshot['total_rows_inserted']
    total_time = final_snapshot['elapsed']
    was_terminated = progress.terminated
    
    # Aggregate schema changes and backfill stats from results
    total_schema_changes = 0
    total_rows_backfilled = 0
    tables_with_schema_changes = []
    for result in results:
        schema_changes = result.get("schema_changes", [])
        if schema_changes:
            total_schema_changes += len(schema_changes)
            tables_with_schema_changes.append(f"{result['schema']}.{result['table']}")
        total_rows_backfilled += result.get("rows_backfilled", 0)
    
    # Final summary
    log.info("")
    log.info("=" * 80)
    if was_terminated:
        log.warning("DUCKLAKE SYNC TERMINATED!")
    else:
        log.info("DUCKLAKE SYNC COMPLETE!")
    log.info("=" * 80)
    log.info(f"  Workers used:           {num_workers}")
    log.info(f"  Total time:             {format_duration(total_time)}")
    log.info(f"  Tables synced:          {tables_synced}")
    log.info(f"  Tables already in sync: {tables_skipped}")
    log.info(f"  Tables with errors:     {tables_with_errors}")
    log.info(f"  Rows deleted:           {total_deleted:,}")
    log.info(f"  Rows updated:           {total_updated:,}")
    log.info(f"  Rows inserted:          {total_inserted:,}")
    if total_schema_changes > 0:
        log.info(f"  Schema changes:         {total_schema_changes} (in {len(tables_with_schema_changes)} tables)")
        log.info(f"  Rows backfilled:        {total_rows_backfilled:,}")
    log.info("=" * 80)
    
    # Log schema changes
    if tables_with_schema_changes:
        log.info("")
        log.info("Tables with schema changes:")
        for table_name in tables_with_schema_changes:
            log.info(f"  {table_name}")
    
    # Log errors
    errors = [(name, t.error) for name, t in progress.tables.items() if t.error]
    if errors:
        log.warning("")
        log.warning("Tables with errors:")
        for name, error in errors:
            log.warning(f"  {name}: {error}")
    
    return dg.Output(
        None,
        metadata={
            "tables_synced": tables_synced,
            "tables_skipped": tables_skipped,
            "tables_with_errors": tables_with_errors,
            "total_rows_deleted": total_deleted,
            "total_rows_updated": total_updated,
            "total_rows_inserted": total_inserted,
            "total_schema_changes": total_schema_changes,
            "total_rows_backfilled": total_rows_backfilled,
            "tables_with_schema_changes": len(tables_with_schema_changes),
            "num_workers": num_workers,
            "total_seconds": total_time,
            "terminated": was_terminated,
        },
    )


@dg.asset(
    name="ducklake_sync",
    group_name="ducklake",
    compute_kind="duckdb",
    deps=[dg.AssetKey("warehouse_row_hashes")],
    description=(
        "Syncs PostgreSQL warehouse to DuckLake data lake. "
        "Uses _row_hash for efficient incremental sync. "
        "Handles inserts, deletions, modifications, and schema changes."
    ),
)
def ducklake_sync(context: dg.AssetExecutionContext) -> dg.Output[None]:
    """
    DuckLake sync: Incrementally syncs warehouse data to DuckLake.
    
    Uses _row_hash for efficient change detection:
    - New/modified rows have different _row_hash values
    - Deleted rows have _row_hash values that no longer exist in source
    
    Depends on warehouse_row_hashes to ensure all rows have computed hashes.
    """
    return _ducklake_sync_impl(context)
