# Row Hash for PostgreSQL Change Detection

Self-contained SQL script to compute row hashes for CDC/change detection. Stores hashes in `_row_hash` (uuid) and `_row_hash_at` (timestamptz) columns.

## Usage

Edit `row_hash.sql` to set `target_table`, then run in your PostgreSQL client:

```sql
\i row_hash.sql
```

The script:
1. Adds `_row_hash` and `_row_hash_at` columns if missing
2. Dynamically builds column list (excludes `_row_hash*` columns)
3. Updates only rows where hash changed

## Performance (100-column table, mixed types including JSONB)

**Hash Computation (SELECT only):**
| Rows | Time | Throughput |
|------|------|------------|
| 100K | 30ms | 3.4 M rows/sec |
| 500K | 325ms | 1.5 M rows/sec |
| 1M | 610ms | **1.6 M rows/sec** |
| 2M | 2.1s | 0.96 M rows/sec |

**UPDATE with hash storage:**
| Rows | Initial (all NULL) | Re-run (no changes) |
|------|-------------------|---------------------|
| 100K | 4.3s | 3.1s |
| 500K | 22.8s | 18.4s |
| 1M | 47.2s | 36.4s |
| 2M | 98.5s | 72.0s |

*Tested on Docker postgres:17 with 4GB memory, 100 columns (10 UUIDs, 20 integers, 10 floats, 20 timestamps/dates, 20 text columns, 10 booleans, 5 JSONB, 4 numerics)*

## Key Optimization

`ROW(col1, col2, ...)::text` is **8x faster** than `row_to_json()::jsonb`:
- jsonb approach: ~8s for 100K rows with 122 columns
- ROW() approach: ~1s for same data

## Storage Overhead

~25 bytes per row:
- `_row_hash` (uuid): 16 bytes
- `_row_hash_at` (timestamptz): 8 bytes
- Column overhead: ~1 byte

## Files

- `row_hash.sql` - Main script (copy-paste into psql)
- `benchmark_100col.sh` - Performance benchmark with 100-column table
