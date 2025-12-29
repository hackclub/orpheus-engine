#!/bin/bash
set -e

# Benchmark script for row hashing performance
# Uses ghcr.io/hackclub/warehouse-postgres:17

CONTAINER_NAME="row-hash-benchmark"
PG_PASSWORD="benchmark"
PG_USER="postgres"
PG_DB="benchmark"

# Row counts to test
ROW_COUNTS=(100000 1000000)

cleanup() {
    echo "Cleaning up..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

trap cleanup EXIT

run_sql() {
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c "$1"
}

run_timed_sql() {
    local start=$(date +%s.%N)
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c "$1"
    local end=$(date +%s.%N)
    local elapsed=$(echo "$end - $start" | bc)
    echo "Time: ${elapsed}s"
}

echo "=== Row Hash Benchmark ==="
echo "Starting PostgreSQL container..."

docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DB" \
    -p 5433:5432 \
    --shm-size=1g \
    ghcr.io/hackclub/warehouse-postgres:17

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then
        echo "PostgreSQL is ready!"
        break
    fi
    sleep 1
done

# Check if xxhash extension is available
echo "Checking xxhash extension..."
run_sql "CREATE EXTENSION IF NOT EXISTS xxhash;"

# Load the row hash functions
echo "Loading row hash functions..."
docker exec -i "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" < row_hash.sql

# Create benchmark table
echo "Creating benchmark table..."
run_sql "
DROP TABLE IF EXISTS benchmark_table;
CREATE TABLE benchmark_table (
    id BIGSERIAL PRIMARY KEY,
    uuid_col UUID DEFAULT gen_random_uuid(),
    text_col TEXT,
    int_col INTEGER,
    float_col DOUBLE PRECISION,
    timestamp_col TIMESTAMPTZ DEFAULT now(),
    json_col JSONB
);
"

echo ""
echo "=== Running Benchmarks ==="
echo ""

for row_count in "${ROW_COUNTS[@]}"; do
    echo "--- Testing with $row_count rows ---"
    
    # Clear and populate table (use DROP/CREATE for cleaner state)
    # Include many PostgreSQL types to simulate real-world tables
    run_sql "
DROP TABLE IF EXISTS benchmark_table;
CREATE TABLE benchmark_table (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- UUID
    uuid_col UUID DEFAULT gen_random_uuid(),
    
    -- Text types
    varchar_col VARCHAR(255),
    text_col TEXT,
    char_col CHAR(10),
    
    -- Numeric types
    smallint_col SMALLINT,
    int_col INTEGER,
    bigint_col BIGINT,
    decimal_col DECIMAL(18,6),
    numeric_col NUMERIC(20,8),
    real_col REAL,
    float_col DOUBLE PRECISION,
    
    -- Boolean
    bool_col BOOLEAN,
    
    -- Date/Time types
    date_col DATE,
    time_col TIME,
    timetz_col TIMETZ,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ DEFAULT now(),
    interval_col INTERVAL,
    
    -- JSON types
    json_col JSON,
    jsonb_col JSONB,
    jsonb_nested JSONB,
    
    -- Array types
    int_array INTEGER[],
    text_array TEXT[],
    
    -- Network types
    inet_col INET,
    cidr_col CIDR,
    macaddr_col MACADDR,
    
    -- Geometric types
    point_col POINT,
    
    -- Binary
    bytea_col BYTEA
);

INSERT INTO benchmark_table (
    varchar_col, text_col, char_col,
    smallint_col, int_col, bigint_col, decimal_col, numeric_col, real_col, float_col,
    bool_col,
    date_col, time_col, timetz_col, timestamp_col, interval_col,
    json_col, jsonb_col, jsonb_nested,
    int_array, text_array,
    inet_col, cidr_col, macaddr_col,
    point_col, bytea_col
)
SELECT 
    -- Text types
    md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    substring(md5(random()::text) from 1 for 10),
    
    -- Numeric types
    (random() * 32767)::smallint,
    (random() * 1000000)::integer,
    (random() * 1000000000000)::bigint,
    (random() * 1000000)::decimal(18,6),
    (random() * 100000000)::numeric(20,8),
    random()::real,
    random() * 1000000,
    
    -- Boolean
    random() > 0.5,
    
    -- Date/Time types
    '2020-01-01'::date + (random() * 1500)::int,
    ('00:00:00'::time + (random() * 86400) * interval '1 second'),
    ('00:00:00+00'::timetz + (random() * 86400) * interval '1 second'),
    '2020-01-01 00:00:00'::timestamp + (random() * 1500) * interval '1 day' + (random() * 86400) * interval '1 second',
    (random() * 1000)::int * interval '1 day' + (random() * 86400)::int * interval '1 second',
    
    -- JSON types
    json_build_object('key', md5(random()::text), 'value', random(), 'active', random() > 0.5),
    jsonb_build_object('id', (random()*1000)::int, 'name', md5(random()::text), 'score', random()),
    jsonb_build_object(
        'user', jsonb_build_object('id', (random()*1000)::int, 'email', md5(random()::text) || '@example.com'),
        'metadata', jsonb_build_object('tags', jsonb_build_array('tag1', 'tag2', md5(random()::text))),
        'settings', jsonb_build_object('enabled', random() > 0.5, 'threshold', random())
    ),
    
    -- Array types
    ARRAY[(random()*100)::int, (random()*100)::int, (random()*100)::int],
    ARRAY[md5(random()::text), md5(random()::text)],
    
    -- Network types
    ('192.168.' || (random()*255)::int || '.' || (random()*255)::int)::inet,
    ('10.' || (random()*255)::int || '.0.0/16')::cidr,
    (lpad(to_hex((random()*255)::int), 2, '0') || ':' ||
     lpad(to_hex((random()*255)::int), 2, '0') || ':' ||
     lpad(to_hex((random()*255)::int), 2, '0') || ':' ||
     lpad(to_hex((random()*255)::int), 2, '0') || ':' ||
     lpad(to_hex((random()*255)::int), 2, '0') || ':' ||
     lpad(to_hex((random()*255)::int), 2, '0'))::macaddr,
    
    -- Geometric
    point(random() * 180 - 90, random() * 360 - 180),
    
    -- Binary
    decode(md5(random()::text), 'hex')
FROM generate_series(1, $row_count);
ANALYZE benchmark_table;
"
    
    echo "Table populated with $row_count rows"
    
    # Benchmark 1: Direct xxh3_64 (hex output) - baseline
    echo ""
    echo "Benchmark 1: xxh3_64(row::text) - hex string output (baseline)"
    run_timed_sql "
SELECT COUNT(*) FROM (
    SELECT id, xxh3_64(t.*::text) as row_hash 
    FROM benchmark_table t
) x;
"

    # Benchmark 2: xxh3_64b (bytea) - avoids hex conversion
    echo ""
    echo "Benchmark 2: xxh3_64b(row::text) - bytea output (faster)"
    run_timed_sql "
SELECT COUNT(*) FROM (
    SELECT id, xxh3_64b(t.*::text) as row_hash 
    FROM benchmark_table t
) x;
"

    # Benchmark 3: Parallel + bytea
    echo ""
    echo "Benchmark 3: Parallel (4 workers) + bytea"
    run_timed_sql "
SET max_parallel_workers_per_gather = 4;
SELECT COUNT(*) FROM (
    SELECT id, xxh3_64b(t.*::text) as row_hash 
    FROM benchmark_table t
) x;
"

    # Benchmark 4: Aggressive parallel (no JIT - causes stability issues)
    echo ""
    echo "Benchmark 4: Aggressive parallel (8 workers, tuned costs)"
    run_timed_sql "
SET max_parallel_workers_per_gather = 8;
SET parallel_tuple_cost = 0.001;
SET parallel_setup_cost = 10;
SELECT COUNT(*) FROM (
    SELECT id, xxh3_64b(t.*::text) as row_hash 
    FROM benchmark_table t
) x;
"

    # Benchmark 5: EXPLAIN ANALYZE (limit output to avoid memory issues)
    echo ""
    echo "Benchmark 5: EXPLAIN ANALYZE (parallel, limited)"
    run_sql "
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0.001;
SET parallel_setup_cost = 10;
EXPLAIN (ANALYZE, COSTS OFF)
SELECT COUNT(*) FROM (
    SELECT id, xxh3_64b(t.*::text) as row_hash 
    FROM benchmark_table t
) x;
"

    # Benchmark 6: XOR checksum (for quick table comparison)
    echo ""
    echo "Benchmark 6: Table XOR checksum (for drift detection)"
    run_timed_sql "SELECT * FROM table_hash_summary('public', 'benchmark_table');"

    # Benchmark 7: Storage size comparison
    echo ""
    echo "Benchmark 7: Storage size comparison"
    run_sql "
-- Create temp tables to measure storage
DROP TABLE IF EXISTS hash_storage_hex;
DROP TABLE IF EXISTS hash_storage_bytea;
DROP TABLE IF EXISTS hash_storage_bigint;

CREATE UNLOGGED TABLE hash_storage_hex AS
SELECT id, xxh3_64(t.*::text) as row_hash FROM benchmark_table t;

CREATE UNLOGGED TABLE hash_storage_bytea AS
SELECT id, xxh3_64b(t.*::text) as row_hash FROM benchmark_table t;

CREATE UNLOGGED TABLE hash_storage_bigint AS
SELECT id, ('x' || xxh3_64(t.*::text))::bit(64)::bigint as row_hash FROM benchmark_table t;

-- Report sizes
SELECT 'hex (varchar 16)' as format,
       pg_size_pretty(pg_total_relation_size('hash_storage_hex')) as total_size,
       pg_total_relation_size('hash_storage_hex') / $row_count as bytes_per_row;

SELECT 'bytea (8 bytes)' as format,
       pg_size_pretty(pg_total_relation_size('hash_storage_bytea')) as total_size,
       pg_total_relation_size('hash_storage_bytea') / $row_count as bytes_per_row;

SELECT 'bigint (8 bytes)' as format,
       pg_size_pretty(pg_total_relation_size('hash_storage_bigint')) as total_size,
       pg_total_relation_size('hash_storage_bigint') / $row_count as bytes_per_row;

DROP TABLE hash_storage_hex;
DROP TABLE hash_storage_bytea;
DROP TABLE hash_storage_bigint;
"

    echo ""
    echo "=========================================="
done

echo ""
echo "=== Benchmark Complete ==="
