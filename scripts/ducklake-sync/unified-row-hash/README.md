# Unified Row Hash for PostgreSQL

Fast row-level hashing using xxHash (xxh3_64) for detecting changes in PostgreSQL tables. Designed for syncing to data lakes by identifying changed rows.

## Performance

Benchmarks on `ghcr.io/hackclub/warehouse-postgres:17` with a realistic table containing 30+ columns:
- Text types (VARCHAR, TEXT, CHAR)
- Numeric types (SMALLINT, INTEGER, BIGINT, DECIMAL, NUMERIC, REAL, DOUBLE PRECISION)
- Boolean, Date/Time types (DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL)
- JSON types (JSON, JSONB, nested JSONB)
- Array types (INTEGER[], TEXT[])
- Network types (INET, CIDR, MACADDR)
- Geometric (POINT), Binary (BYTEA), UUID

### Optimized Results (using `xxh3_64b` bytea output)

| Rows | Baseline (hex) | bytea | bytea + parallel | Speedup |
|------|---------------|-------|------------------|---------|
| 100K | 89ms | 90ms | 82ms | 1.1x |
| 1M | 487ms | 180ms | 155ms | **3.1x** |
| 2M | 932ms | 235ms | 181ms | **5.2x** |

**Key insight:** Using `xxh3_64b` (bytea) instead of `xxh3_64` (hex string) is 2.7x faster.

**Extrapolated for 10B rows (parallel):** ~15 minutes

### Storage Size Comparison

| Format | Bytes/Row | 1M Rows | 10B Rows |
|--------|-----------|---------|----------|
| hex (varchar 16) | 60 | 58 MB | ~560 GB |
| bytea (8 bytes) | 52 | 50 MB | ~484 GB |
| bigint (8 bytes) | 44 | 43 MB | ~410 GB |

**Recommendation:** Use `bigint` for storage (smallest), `bytea` for computation (fastest).

### Optimization Tips

1. **Use `xxh3_64b` (bytea)** - avoids hex string conversion overhead
2. **Enable parallelism** - `SET max_parallel_workers_per_gather = 8`
3. **Tune parallel costs** - `SET parallel_tuple_cost = 0.001; SET parallel_setup_cost = 10`
4. **Use COPY for export** - `COPY (...) TO PROGRAM 'gzip > file.gz'`
5. **Multi-session parallelism** - run N sessions over disjoint PK ranges for 10B+ rows

## Usage

### Setup

```sql
-- Enable xxhash extension (must be installed)
CREATE EXTENSION IF NOT EXISTS xxhash;

-- Load the helper functions
\i row_hash.sql
```

### Basic Row Hashing

```sql
-- Hash all rows in a table (fastest approach - use bytea output)
SELECT id, xxh3_64b(t.*::text) as row_hash 
FROM my_table t;

-- With parallel execution for large tables
SET max_parallel_workers_per_gather = 8;
SET parallel_tuple_cost = 0.001;
SET parallel_setup_cost = 10;

SELECT id, xxh3_64b(t.*::text) as row_hash 
FROM my_table t;

-- Export to compressed CSV (for 10B+ rows)
COPY (
    SELECT id, encode(xxh3_64b(t.*::text), 'hex') as row_hash 
    FROM my_table t
) TO PROGRAM 'gzip > /tmp/row_hashes.csv.gz' WITH (FORMAT csv, HEADER);
```

### Using Helper Functions

```sql
-- Generate a hash query for any table
SELECT generate_row_hash_query('public', 'my_table', ARRAY['id']);
-- Returns: SELECT id::text as pk, xxh3_64(t.*::text) as row_hash FROM public.my_table t

-- For composite primary keys
SELECT generate_row_hash_query('public', 'my_table', ARRAY['id1', 'id2']);

-- Get table-level checksum (for quick drift detection)
SELECT * FROM table_hash_summary('public', 'my_table');
-- Returns: total_rows, xor_checksum, computed_at
```

### Export to CSV

```sql
-- Export hashes to CSV file
SELECT export_row_hashes_to_csv(
    'public',           -- schema
    'my_table',         -- table
    ARRAY['id'],        -- primary key column(s)
    '/tmp/hashes.csv'   -- output path
);
```

## How It Works

1. **Row to Text**: Each row is cast to its text representation (`t.*::text`)
2. **xxHash**: The text is hashed using `xxh3_64()` - the fastest xxHash variant
3. **Output**: Returns a 16-character hex string (64-bit hash)

### Why xxh3_64?

- **Extremely fast**: ~10GB/s on modern CPUs
- **Low collision rate**: 64-bit provides 1 in 18 quintillion collision probability
- **Parallel-safe**: Can leverage PostgreSQL parallel query execution

## Change Detection Workflow

1. **Initial sync**: Export all `(pk, row_hash)` pairs to your data lake
2. **Incremental sync**:
   - Compute hashes for source table
   - Compare with stored hashes in data lake
   - Rows with different hashes have changed
   - New PKs are inserts, missing PKs are deletes

```sql
-- Example: Find changed rows
WITH current_hashes AS (
    SELECT id::text as pk, xxh3_64(t.*::text) as row_hash
    FROM my_table t
),
stored_hashes AS (
    SELECT pk, row_hash FROM my_data_lake_hashes
)
SELECT c.pk
FROM current_hashes c
LEFT JOIN stored_hashes s ON c.pk = s.pk
WHERE s.row_hash IS NULL OR s.row_hash != c.row_hash;
```

## Running Benchmarks

```bash
./benchmark.sh
```

Requires Docker. Uses `ghcr.io/hackclub/warehouse-postgres:17`.

## Files

- `row_hash.sql` - PostgreSQL functions for row hashing
- `benchmark.sh` - Benchmark script with timing measurements
- `README.md` - This file
