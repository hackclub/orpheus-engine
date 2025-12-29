#!/bin/bash
set -e

# Benchmark script for row_hash function
# Uses standard postgres image - no custom extension needed

CONTAINER_NAME="rowhash-benchmark"
PG_PASSWORD="benchmark"
PG_USER="postgres"
PG_DB="benchmark"

ROW_COUNTS=(1000000 10000000)

cleanup() {
    echo "Cleaning up..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

trap cleanup EXIT

run_sql() {
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c "$1"
}

run_timed_sql() {
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c "\\timing" -c "$1"
}

echo "=== row_hash Benchmark ==="
echo "Starting PostgreSQL container..."

docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DB" \
    --shm-size=2g \
    postgres:17

echo "Waiting for PostgreSQL..."
for i in {1..30}; do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then
        echo "PostgreSQL is ready!"
        break
    fi
    sleep 1
done

# Create the row_hash function
echo "Creating row_hash function..."
run_sql "
CREATE OR REPLACE FUNCTION row_hash(r anyelement)
RETURNS text AS \$\$
  SELECT md5((
    to_jsonb(r) 
    - '_row_hash' 
    - '_row_hash_at'
  )::text)
\$\$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
"

echo ""
echo "=== Running Benchmarks ==="

for row_count in "${ROW_COUNTS[@]}"; do
    echo ""
    echo "=========================================="
    echo "Testing with $row_count rows"
    echo "=========================================="
    
    run_sql "
DROP TABLE IF EXISTS benchmark_table;
CREATE TABLE benchmark_table (
    id BIGSERIAL PRIMARY KEY,
    uuid_col UUID DEFAULT gen_random_uuid(),
    text_col TEXT,
    float_col DOUBLE PRECISION,
    timestamp_col TIMESTAMPTZ DEFAULT now(),
    jsonb_col JSONB,
    _row_hash TEXT,
    _row_hash_at TIMESTAMPTZ
);

INSERT INTO benchmark_table (text_col, float_col, jsonb_col)
SELECT 
    md5(random()::text),
    random() * 1000000,
    jsonb_build_object('id', i, 'value', random())
FROM generate_series(1, $row_count) i;
ANALYZE benchmark_table;
"
    
    echo "Table populated"
    echo ""

    echo "1. row_hash - sequential"
    run_timed_sql "
SET max_parallel_workers_per_gather = 0;
SELECT COUNT(*) FROM (SELECT row_hash(t.*) FROM benchmark_table t) x;
"

    echo ""
    echo "2. row_hash - parallel (4 workers)"
    run_timed_sql "
SET max_parallel_workers_per_gather = 4;
SELECT COUNT(*) FROM (SELECT row_hash(t.*) FROM benchmark_table t) x;
"

    echo ""
    echo "3. md5(row::text) baseline - sequential"
    run_timed_sql "
SET max_parallel_workers_per_gather = 0;
SELECT COUNT(*) FROM (SELECT md5(t.*::text) FROM benchmark_table t) x;
"

    echo ""
    echo "4. Verify hash excludes _row_hash columns"
    run_sql "
UPDATE benchmark_table SET _row_hash = row_hash(benchmark_table.*) WHERE id <= 5;
UPDATE benchmark_table SET _row_hash_at = now() WHERE id <= 5;
SELECT id, _row_hash = row_hash(t.*) as hash_stable FROM benchmark_table t WHERE id <= 5;
"
done

echo ""
echo "=== Benchmark Complete ==="
