"""
DuckLake row hash computation asset.

This module is part of the DuckLake sync infrastructure. It runs after all other
assets complete to compute xxHash-based row hashes for change detection.

Uses the pg_rowhash extension to add _row_hash (bytea) and _row_hash_at (timestamptz)
columns to every table in the warehouse, enabling efficient incremental sync to DuckLake.

Processing is parallelized across multiple workers based on Postgres max_parallel_workers.
"""

import os
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
