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
import threading
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import List, Tuple, Sequence, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import dagster as dg
import psycopg2

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
DEBUG_SYNC_TABLE_PATTERNS: list[str] | None = [
    r"^loops\.audience$",  # Only sync loops.audience for testing
]
# Set to None to disable debug filtering and sync all tables:
# DEBUG_SYNC_TABLE_PATTERNS = None
# =============================================================================

# Progress update interval in seconds
PROGRESS_UPDATE_INTERVAL = 5.0

# Batch size for row hash updates
BATCH_SIZE = 100_000


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
    active_connections: Dict[int, any] = field(default_factory=dict)  # worker_id -> connection
    
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
    """Get the number of parallel workers available in Postgres."""
    with conn.cursor() as cur:
        # Get max_parallel_workers setting
        cur.execute("SHOW max_parallel_workers")
        max_workers = int(cur.fetchone()[0])
        
        # Also check max_parallel_workers_per_gather for reference
        cur.execute("SHOW max_parallel_workers_per_gather")
        per_gather = int(cur.fetchone()[0])
        
        # Use the smaller of max_parallel_workers or a reasonable cap
        # We don't want to overwhelm the database
        worker_count = min(max_workers, 8)  # Cap at 8 workers
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
        log.info(f"✓ Using {num_workers} parallel workers (based on Postgres max_parallel_workers)")
        
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
import hashlib
from typing import Set, Any

# Columns to exclude from sync (metadata columns)
SYNC_EXCLUDED_COLUMNS = frozenset({"_row_hash", "_row_hash_at"})


@dataclass
class SyncTableProgress:
    """Progress tracking for a single table sync."""
    schema: str
    table: str
    status: str = "pending"  # pending, syncing, completed, error, skipped
    rows_deleted: int = 0
    rows_inserted: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
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
class SyncGlobalProgress:
    """Thread-safe global progress tracker for DuckLake sync."""
    tables: Dict[str, SyncTableProgress] = field(default_factory=dict)
    tables_completed: List[str] = field(default_factory=list)
    tables_just_completed: List[str] = field(default_factory=list)
    total_tables: int = 0
    total_rows_deleted: int = 0
    total_rows_inserted: int = 0
    start_time: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)
    stop_monitoring: bool = False
    stop_workers: bool = False
    terminated: bool = False
    
    def update_table(self, table_name: str, **kwargs):
        """Thread-safe update of table progress."""
        with self.lock:
            if table_name in self.tables:
                for key, value in kwargs.items():
                    setattr(self.tables[table_name], key, value)
    
    def mark_completed(self, table_name: str, rows_deleted: int, rows_inserted: int, 
                       error: str = None):
        """Mark a table as completed."""
        with self.lock:
            if table_name in self.tables:
                progress = self.tables[table_name]
                progress.status = "error" if error else "completed"
                progress.end_time = time.time()
                progress.rows_deleted = rows_deleted
                progress.rows_inserted = rows_inserted
                progress.error = error
                self.tables_completed.append(table_name)
                self.tables_just_completed.append(table_name)
                self.total_rows_deleted += rows_deleted
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
            running_rows_inserted = 0
            
            for name, progress in self.tables.items():
                if progress.status == "syncing":
                    running.append({
                        "name": name,
                        "worker_id": progress.worker_id,
                        "status": progress.status,
                        "rows_deleted": progress.rows_deleted,
                        "rows_inserted": progress.rows_inserted,
                        "elapsed": progress.elapsed_seconds,
                    })
                    running_rows_deleted += progress.rows_deleted
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
                "total_rows_inserted": self.total_rows_inserted + running_rows_inserted,
                "elapsed": time.time() - self.start_time,
            }


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
    
    # Install and load required extensions
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    
    # Configure S3 endpoint (strip protocol prefix)
    s3_endpoint = s3_url.replace("https://", "").replace("http://", "").rstrip("/")
    
    conn.execute(f"""
        CREATE SECRET ducklake_s3 (
            TYPE S3,
            KEY_ID '{s3_key_id}',
            SECRET '{s3_secret}',
            ENDPOINT '{s3_endpoint}',
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
) -> bool:
    """
    Sync table schema from PostgreSQL to DuckLake.
    Creates table if it doesn't exist, adds new columns if needed.
    Returns True if table was created, False if it already existed.
    """
    # Ensure schema exists
    ensure_schema_exists_ducklake(duck_conn, schema)
    
    # Get source columns
    src_columns = get_table_columns_pg(pg_conn, schema, table)
    if not src_columns:
        raise ValueError(f"No columns found in source table {schema}.{table}")
    
    # Check if table exists in DuckLake
    if not table_exists_in_ducklake(duck_conn, schema, table):
        # Create table with all columns plus _row_hash
        col_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in src_columns])
        col_defs += ", _row_hash BLOB"
        
        duck_conn.execute(f"""
            CREATE TABLE ducklake."{schema}"."{table}" ({col_defs})
        """)
        return True
    
    # Table exists - check for new columns
    dst_columns = get_table_columns_ducklake(duck_conn, schema, table)
    dst_col_names = {col[0] for col in dst_columns}
    
    for col_name, col_type in src_columns:
        if col_name not in dst_col_names:
            duck_conn.execute(f"""
                ALTER TABLE ducklake."{schema}"."{table}" 
                ADD COLUMN "{col_name}" {col_type}
            """)
    
    # Ensure _row_hash column exists (required for sync)
    if "_row_hash" not in dst_col_names:
        duck_conn.execute(f"""
            ALTER TABLE ducklake."{schema}"."{table}" 
            ADD COLUMN "_row_hash" BLOB
        """)
    
    return False


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
) -> Tuple[List[str], int]:
    """
    Download lightweight sync metadata from PostgreSQL into a temp DuckDB table.
    
    Downloads: primary key columns (if any) + _row_hash + _row_hash_at
    
    Returns (pk_columns, row_count).
    """
    import sys
    table_name = f"{schema}.{table}"
    
    print(f"    [download_sync_metadata] Starting for {table_name}", flush=True)
    
    # Get primary key columns
    pk_columns = get_primary_key_columns(pg_conn, schema, table)
    print(f"    [download_sync_metadata] PK columns: {pk_columns}", flush=True)
    
    # Build column list for download
    if pk_columns:
        pk_cols_quoted = ", ".join([f'"{c}"' for c in pk_columns])
        select_cols = f"{pk_cols_quoted}, _row_hash, _row_hash_at"
    else:
        select_cols = "_row_hash, _row_hash_at"
    print(f"    [download_sync_metadata] Select cols: {select_cols[:100]}...", flush=True)
    
    # Drop temp table if exists
    print(f"    [download_sync_metadata] Dropping temp table...", flush=True)
    duck_conn.execute("DROP TABLE IF EXISTS _sync_metadata")
    
    # Ensure warehouse is attached
    print(f"    [download_sync_metadata] Ensuring warehouse attached...", flush=True)
    ensure_warehouse_attached(duck_conn)
    
    # Download metadata directly from PostgreSQL into DuckDB temp table
    print(f"    [download_sync_metadata] Downloading metadata from PG...", flush=True)
    start = time.time()
    duck_conn.execute(f"""
        CREATE TEMP TABLE _sync_metadata AS
        SELECT {select_cols}
        FROM warehouse."{schema}"."{table}"
        WHERE _row_hash IS NOT NULL
    """)
    elapsed = time.time() - start
    print(f"    [download_sync_metadata] Download completed in {elapsed:.1f}s", flush=True)
    
    # Get row count
    row_count = duck_conn.execute("SELECT COUNT(*) FROM _sync_metadata").fetchone()[0]
    print(f"    [download_sync_metadata] Downloaded {row_count:,} rows", flush=True)
    
    return pk_columns, row_count


def compute_sync_diff(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    pk_columns: List[str],
) -> dict:
    """
    Compare _sync_metadata (from PostgreSQL) with DuckLake table.
    
    If PK columns exist, compares by PK (enables updates).
    Otherwise, compares by _row_hash only.
    
    Returns dict with:
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
    print(f"    [compute_sync_diff] Starting for {table_name}", flush=True)
    
    # Source count (from temp table)
    print(f"    [compute_sync_diff] Counting source rows...", flush=True)
    src_count = duck_conn.execute("SELECT COUNT(*) FROM _sync_metadata").fetchone()[0]
    print(f"    [compute_sync_diff] Source: {src_count:,}", flush=True)
    
    # Destination count
    print(f"    [compute_sync_diff] Counting dest rows...", flush=True)
    try:
        dst_count = duck_conn.execute(
            f'SELECT COUNT(*) FROM ducklake."{schema}"."{table}"'
        ).fetchone()[0]
    except:
        dst_count = 0
    print(f"    [compute_sync_diff] Dest: {dst_count:,}", flush=True)
    
    if dst_count == 0:
        print(f"    [compute_sync_diff] Dest empty, returning full insert", flush=True)
        return {
            "src_count": src_count,
            "dst_count": 0,
            "to_insert": src_count,
            "to_update": 0,
            "to_delete": 0,
            "in_sync": 0,
            "change_pct": 100.0,
            "pk_columns": pk_columns,
        }
    
    # Build join condition based on PK or hash
    if pk_columns:
        pk_join = " AND ".join([f'src."{c}" = dst."{c}"' for c in pk_columns])
        print(f"    [compute_sync_diff] Using PK join: {pk_join[:80]}...", flush=True)
        
        # Count rows to INSERT (PK in source, not in dest)
        print(f"    [compute_sync_diff] Counting rows to insert...", flush=True)
        start = time.time()
        to_insert = duck_conn.execute(f"""
            SELECT COUNT(*) FROM _sync_metadata src
            WHERE NOT EXISTS (
                SELECT 1 FROM ducklake."{schema}"."{table}" dst
                WHERE {pk_join}
            )
        """).fetchone()[0]
        print(f"    [compute_sync_diff] To insert: {to_insert:,} ({time.time()-start:.1f}s)", flush=True)
        
        # Count rows to UPDATE (PK in both, hash differs)
        print(f"    [compute_sync_diff] Counting rows to update...", flush=True)
        start = time.time()
        to_update = duck_conn.execute(f"""
            SELECT COUNT(*) FROM _sync_metadata src
            INNER JOIN ducklake."{schema}"."{table}" dst ON {pk_join}
            WHERE src._row_hash != dst._row_hash
        """).fetchone()[0]
        print(f"    [compute_sync_diff] To update: {to_update:,} ({time.time()-start:.1f}s)", flush=True)
        
        # Count rows to DELETE (PK in dest, not in source)
        print(f"    [compute_sync_diff] Counting rows to delete...", flush=True)
        start = time.time()
        to_delete = duck_conn.execute(f"""
            SELECT COUNT(*) FROM ducklake."{schema}"."{table}" dst
            WHERE NOT EXISTS (
                SELECT 1 FROM _sync_metadata src
                WHERE {pk_join}
            )
        """).fetchone()[0]
        print(f"    [compute_sync_diff] To delete: {to_delete:,} ({time.time()-start:.1f}s)", flush=True)
        
    else:
        # Hash-only comparison (no updates possible)
        print(f"    [compute_sync_diff] Using hash-only comparison (no PK)", flush=True)
        
        # Count rows to insert (hash in source, not in dest)
        print(f"    [compute_sync_diff] Counting rows to insert...", flush=True)
        start = time.time()
        to_insert = duck_conn.execute(f"""
            SELECT COUNT(*) FROM _sync_metadata src
            WHERE NOT EXISTS (
                SELECT 1 FROM ducklake."{schema}"."{table}" dst
                WHERE dst._row_hash = src._row_hash
            )
        """).fetchone()[0]
        print(f"    [compute_sync_diff] To insert: {to_insert:,} ({time.time()-start:.1f}s)", flush=True)
        
        to_update = 0  # No updates without PK
        
        # Count rows to delete (hash in dest, not in source)
        print(f"    [compute_sync_diff] Counting rows to delete...", flush=True)
        start = time.time()
        to_delete = duck_conn.execute(f"""
            SELECT COUNT(*) FROM ducklake."{schema}"."{table}" dst
            WHERE NOT EXISTS (
                SELECT 1 FROM _sync_metadata src
                WHERE src._row_hash = dst._row_hash
            )
        """).fetchone()[0]
        print(f"    [compute_sync_diff] To delete: {to_delete:,} ({time.time()-start:.1f}s)", flush=True)
    
    # Rows in sync
    in_sync = src_count - to_insert - to_update
    
    # Calculate change percentage
    total_changes = to_insert + to_update + to_delete
    change_pct = (total_changes / max(src_count, 1)) * 100
    
    print(f"    [compute_sync_diff] Done: insert={to_insert}, update={to_update}, delete={to_delete}, in_sync={in_sync:,}, change={change_pct:.1f}%", flush=True)
    
    return {
        "src_count": src_count,
        "dst_count": dst_count,
        "to_insert": to_insert,
        "to_update": to_update,
        "to_delete": to_delete,
        "in_sync": in_sync,
        "change_pct": change_pct,
        "pk_columns": pk_columns,
    }


def sync_table_incremental(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    column_names: List[str],
    full_resync_threshold: float = 80.0,
) -> Tuple[int, int, int, bool]:
    """
    Incrementally sync a table from PostgreSQL to DuckLake.
    
    Strategy:
    1. Download lightweight metadata (PK + _row_hash + _row_hash_at)
    2. Compare with DuckLake to determine changes (by PK if available, else by hash)
    3. If change % > threshold, do full resync
    4. Otherwise, sync just the changes using UPDATE for changed rows (if PK exists)
    
    Returns (rows_deleted, rows_updated, rows_inserted, was_full_resync).
    """
    table_name = f"{schema}.{table}"
    
    # Step 1: Download metadata
    pk_columns, src_count = download_sync_metadata(pg_conn, duck_conn, schema, table)
    
    # Step 2: Compute diff (PK-aware)
    diff = compute_sync_diff(duck_conn, schema, table, pk_columns)
    
    print(f"  {table_name}: src={diff['src_count']:,}, dst={diff['dst_count']:,}", flush=True)
    print(f"  {table_name}: insert={diff['to_insert']:,}, update={diff['to_update']:,}, "
          f"delete={diff['to_delete']:,}, change={diff['change_pct']:.1f}%", flush=True)
    
    # Step 3: Decide full vs incremental sync
    if diff['dst_count'] == 0 or diff['change_pct'] >= full_resync_threshold:
        # Full resync
        print(f"  → Full resync (change_pct={diff['change_pct']:.1f}% >= {full_resync_threshold}%)", flush=True)
        
        # Clear destination
        if diff['dst_count'] > 0:
            duck_conn.execute(f'DELETE FROM ducklake."{schema}"."{table}"')
        
        # Bulk load
        rows_inserted = bulk_load_table(pg_conn, duck_conn, schema, table, column_names)
        
        # Cleanup temp table
        duck_conn.execute("DROP TABLE IF EXISTS _sync_metadata")
        
        return diff['dst_count'], 0, rows_inserted, True
    
    # Step 3.5: Clean up duplicate PKs in destination (if PK exists)
    dups_removed = 0
    if pk_columns and diff['dst_count'] > 0:
        pk_cols_quoted = ", ".join([f'"{c}"' for c in pk_columns])
        
        # Count PKs with duplicates
        dup_count = duck_conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_cols_quoted}
                FROM ducklake."{schema}"."{table}"
                GROUP BY {pk_cols_quoted}
                HAVING COUNT(*) > 1
            ) sub
        """).fetchone()[0]
        
        if dup_count > 0:
            print(f"  → Cleaning up {dup_count} duplicate PKs (keeping 1 each)...", flush=True)
            
            # Delete all but one row per PK using rowid
            # DuckLake uses rowid internally - find duplicates and delete extras
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
            
            dups_removed = dup_count
            print(f"  → Cleaned up {dups_removed} duplicate PKs", flush=True)
    
    # Step 4: Incremental sync
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
        if diff['to_delete'] > 0:
            print(f"  → Deleting {diff['to_delete']:,} rows (by PK)...", flush=True)
            pk_join_del = " AND ".join([f'dst."{c}" = meta."{c}"' for c in pk_columns])
            duck_conn.execute(f"""
                DELETE FROM ducklake."{schema}"."{table}" dst
                WHERE NOT EXISTS (
                    SELECT 1 FROM _sync_metadata meta
                    WHERE {pk_join_del}
                )
            """)
            rows_deleted = diff['to_delete']
        
        # UPDATE rows where PK is in both but hash differs
        if diff['to_update'] > 0:
            print(f"  → Updating {diff['to_update']:,} rows (by PK)...", flush=True)
            
            # Build SET clause for all columns
            set_clause = ", ".join([f'"{c}" = src."{c}"' for c in column_names])
            set_clause += ', _row_hash = src._row_hash'
            
            duck_conn.execute(f"""
                UPDATE ducklake."{schema}"."{table}" dst
                SET {set_clause}
                FROM warehouse."{schema}"."{table}" src
                WHERE {pk_join_dst}
                AND dst._row_hash != src._row_hash
                AND EXISTS (
                    SELECT 1 FROM _sync_metadata meta
                    WHERE {pk_join_meta}
                )
            """)
            rows_updated = diff['to_update']
        
        # INSERT rows where PK is in source but not in dest
        if diff['to_insert'] > 0:
            print(f"  → Inserting {diff['to_insert']:,} rows (by PK)...", flush=True)
            
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
            rows_inserted = diff['to_insert']
    
    else:
        # Hash-only sync: DELETE and INSERT (no updates)
        
        # Delete rows not in source
        if diff['to_delete'] > 0:
            print(f"  → Deleting {diff['to_delete']:,} rows (by hash)...", flush=True)
            duck_conn.execute(f"""
                DELETE FROM ducklake."{schema}"."{table}" dst
                WHERE NOT EXISTS (
                    SELECT 1 FROM _sync_metadata src
                    WHERE src._row_hash = dst._row_hash
                )
            """)
            rows_deleted = diff['to_delete']
        
        # Insert new rows
        if diff['to_insert'] > 0:
            print(f"  → Inserting {diff['to_insert']:,} rows (by hash)...", flush=True)
            
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
            rows_inserted = diff['to_insert']
    
    # Cleanup temp table
    duck_conn.execute("DROP TABLE IF EXISTS _sync_metadata")
    
    return rows_deleted, rows_updated, rows_inserted, False


def bulk_load_table(
    pg_conn,
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    column_names: List[str],
    batch_size: int = 10000,  # Not used anymore, kept for API compatibility
) -> int:
    """
    Bulk load all rows from PostgreSQL to DuckLake using DuckDB's native PostgreSQL integration.
    Much faster than row-by-row inserts.
    
    Returns total rows inserted.
    """
    cols_quoted = ", ".join([f'"{c}"' for c in column_names])
    cols_with_hash = cols_quoted + ", _row_hash"
    
    # Count total rows for progress
    with pg_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE _row_hash IS NOT NULL')
        total_rows = cur.fetchone()[0]
    
    if total_rows == 0:
        return 0
    
    # Get warehouse connection string
    warehouse_url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable not set")
    warehouse_url = warehouse_url.replace('\n', '').replace('\r', '').strip()
    
    # Check if warehouse is already attached
    try:
        duck_conn.execute("SELECT 1 FROM warehouse.information_schema.tables LIMIT 1")
    except:
        # Attach PostgreSQL warehouse directly to DuckDB for fast bulk loading
        duck_conn.execute(f"ATTACH '{warehouse_url}' AS warehouse (TYPE postgres, READ_ONLY)")
    
    # Bulk insert directly from PostgreSQL - much faster than row-by-row
    insert_sql = f"""
        INSERT INTO ducklake."{schema}"."{table}" ({cols_with_hash})
        SELECT {cols_with_hash} 
        FROM warehouse."{schema}"."{table}"
        WHERE _row_hash IS NOT NULL
    """
    
    duck_conn.execute(insert_sql)
    
    # Verify count
    result = duck_conn.execute(f'SELECT COUNT(*) FROM ducklake."{schema}"."{table}"').fetchone()
    return result[0] if result else 0


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
    stats = {
        "schema": schema,
        "table": table,
        "rows_deleted": 0,
        "rows_inserted": 0,
        "error": None,
        "created": False,
        "full_resync": False,
    }
    
    try:
        # Update progress: starting
        progress.update_table(
            table_name,
            status="syncing",
            worker_id=worker_id,
            start_time=time.time(),
        )
        
        # Sync schema (create table or add new columns)
        stats["created"] = sync_table_schema(pg_conn, duck_conn, schema, table)
        
        # Get column names for data sync
        columns = get_table_columns_pg(pg_conn, schema, table)
        column_names = [col[0] for col in columns]
        
        # Smart incremental sync with metadata comparison
        rows_deleted, rows_updated, rows_inserted, was_full_resync = sync_table_incremental(
            pg_conn, duck_conn, schema, table, column_names
        )
        
        stats["rows_deleted"] = rows_deleted
        stats["rows_updated"] = rows_updated
        stats["rows_inserted"] = rows_inserted
        stats["full_resync"] = was_full_resync
        
        if rows_deleted == 0 and rows_updated == 0 and rows_inserted == 0:
            # Table is already in sync
            progress.mark_skipped(table_name)
            stats["skipped"] = True
        else:
            progress.mark_completed(
                table_name,
                rows_deleted=rows_deleted,
                rows_inserted=rows_inserted,
            )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        stats["error"] = str(e)
        progress.mark_completed(table_name, 0, 0, error=str(e))
    
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
            finally:
                table_queue.task_done()
    finally:
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
            f"Changes: {snapshot['total_partitions_changed']:,} partitions | "
            f"{snapshot['total_rows_deleted']:,} deleted | "
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
                lines.append(
                    f"  Worker {r['worker_id']:2d} │ {r['name'][:40]:<40} │ "
                    f"{r['status']:<12} │ "
                    f"partitions: {r['partitions_synced']}/{r['partitions_changed']} │ "
                    f"del: {r['rows_deleted']:,} ins: {r['rows_inserted']:,}"
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
        
        log.info(f"Starting {num_workers} sync workers...")
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
        
        # Wait for completion
        for t in workers:
            t.join()
        
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
    total_inserted = final_snapshot['total_rows_inserted']
    total_time = final_snapshot['elapsed']
    was_terminated = progress.terminated
    
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
    log.info(f"  Rows inserted:          {total_inserted:,}")
    log.info("=" * 80)
    
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
            "total_rows_inserted": total_inserted,
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
