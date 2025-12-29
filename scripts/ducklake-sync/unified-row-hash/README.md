# Row Hash Function

Simple SQL function to hash table rows for change detection, excluding `_row_hash*` columns to avoid circular dependencies.

## Usage

```sql
-- Load the function
\i row_hash.sql

-- Hash a row
SELECT row_hash(t.*) FROM my_table t;

-- Store hash in the table itself
UPDATE my_table SET _row_hash = row_hash(my_table.*);
```

## Performance (10M rows)

| Function | Sequential | Parallel (4 workers) |
|----------|-----------|---------------------|
| row_hash | 510ms | 150ms |
| md5(row::text) | 497ms | 141ms |

~6% overhead vs raw md5 for the column exclusion logic.

## How it works

Uses `row_to_json(row)::jsonb - '_row_hash' - '_row_hash_at'` to exclude hash columns, then `md5()`. The `row_to_json` approach is 2x faster than `to_jsonb` directly.

No custom extension needed - works on any PostgreSQL 12+.
