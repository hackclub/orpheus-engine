#!/bin/bash
set -e

# Benchmark: 100-column table with assorted types
# Measures throughput in millions of rows per second

CONTAINER_NAME="rowhash-bench-100col"
PG_PASSWORD="testbench"
PG_USER="postgres"
PG_DB="benchmark"

ROW_COUNTS=(100000 500000 1000000 2000000)

cleanup() {
    echo "Cleaning up..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

trap cleanup EXIT

run_sql() {
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c "$1"
}

run_sql_file() {
    docker exec -i "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB"
}

echo "=== 100-Column Row Hash Benchmark ==="
echo "Starting PostgreSQL container..."

docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DB" \
    --shm-size=2g \
    --memory=4g \
    postgres:17 \
    -c shared_buffers=512MB \
    -c work_mem=128MB

echo "Waiting for PostgreSQL..."
for i in {1..30}; do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then
        echo "PostgreSQL is ready!"
        break
    fi
    sleep 1
done

# Verify postgres is ready
run_sql "SELECT 1;"

echo ""
echo "=== Running Benchmarks ==="

for row_count in "${ROW_COUNTS[@]}"; do
    echo ""
    echo "=========================================="
    echo "Testing with $row_count rows (100 columns)"
    echo "=========================================="
    
    # Create 100-column table with assorted types
    run_sql_file <<'EOSQL'
DROP TABLE IF EXISTS bench_100col;

CREATE TABLE bench_100col (
    id BIGSERIAL PRIMARY KEY,
    
    -- 10 UUIDs
    uuid_1 UUID DEFAULT gen_random_uuid(), uuid_2 UUID DEFAULT gen_random_uuid(),
    uuid_3 UUID DEFAULT gen_random_uuid(), uuid_4 UUID DEFAULT gen_random_uuid(),
    uuid_5 UUID DEFAULT gen_random_uuid(), uuid_6 UUID DEFAULT gen_random_uuid(),
    uuid_7 UUID DEFAULT gen_random_uuid(), uuid_8 UUID DEFAULT gen_random_uuid(),
    uuid_9 UUID DEFAULT gen_random_uuid(), uuid_10 UUID DEFAULT gen_random_uuid(),
    
    -- 10 integers
    int_1 INTEGER, int_2 INTEGER, int_3 INTEGER, int_4 INTEGER, int_5 INTEGER,
    int_6 INTEGER, int_7 INTEGER, int_8 INTEGER, int_9 INTEGER, int_10 INTEGER,
    
    -- 10 bigints
    bigint_1 BIGINT, bigint_2 BIGINT, bigint_3 BIGINT, bigint_4 BIGINT, bigint_5 BIGINT,
    bigint_6 BIGINT, bigint_7 BIGINT, bigint_8 BIGINT, bigint_9 BIGINT, bigint_10 BIGINT,
    
    -- 10 floats
    float_1 DOUBLE PRECISION, float_2 DOUBLE PRECISION, float_3 DOUBLE PRECISION,
    float_4 DOUBLE PRECISION, float_5 DOUBLE PRECISION, float_6 DOUBLE PRECISION,
    float_7 DOUBLE PRECISION, float_8 DOUBLE PRECISION, float_9 DOUBLE PRECISION,
    float_10 DOUBLE PRECISION,
    
    -- 10 timestamps
    ts_1 TIMESTAMPTZ DEFAULT now(), ts_2 TIMESTAMPTZ DEFAULT now(), ts_3 TIMESTAMPTZ DEFAULT now(),
    ts_4 TIMESTAMPTZ DEFAULT now(), ts_5 TIMESTAMPTZ DEFAULT now(), ts_6 TIMESTAMPTZ DEFAULT now(),
    ts_7 TIMESTAMPTZ DEFAULT now(), ts_8 TIMESTAMPTZ DEFAULT now(), ts_9 TIMESTAMPTZ DEFAULT now(),
    ts_10 TIMESTAMPTZ DEFAULT now(),
    
    -- 10 dates
    date_1 DATE DEFAULT CURRENT_DATE, date_2 DATE DEFAULT CURRENT_DATE, date_3 DATE DEFAULT CURRENT_DATE,
    date_4 DATE DEFAULT CURRENT_DATE, date_5 DATE DEFAULT CURRENT_DATE, date_6 DATE DEFAULT CURRENT_DATE,
    date_7 DATE DEFAULT CURRENT_DATE, date_8 DATE DEFAULT CURRENT_DATE, date_9 DATE DEFAULT CURRENT_DATE,
    date_10 DATE DEFAULT CURRENT_DATE,
    
    -- 10 short text columns
    text_short_1 TEXT, text_short_2 TEXT, text_short_3 TEXT, text_short_4 TEXT, text_short_5 TEXT,
    text_short_6 TEXT, text_short_7 TEXT, text_short_8 TEXT, text_short_9 TEXT, text_short_10 TEXT,
    
    -- 10 medium text columns (varchar 255)
    text_med_1 VARCHAR(255), text_med_2 VARCHAR(255), text_med_3 VARCHAR(255),
    text_med_4 VARCHAR(255), text_med_5 VARCHAR(255), text_med_6 VARCHAR(255),
    text_med_7 VARCHAR(255), text_med_8 VARCHAR(255), text_med_9 VARCHAR(255),
    text_med_10 VARCHAR(255),
    
    -- 10 booleans
    bool_1 BOOLEAN, bool_2 BOOLEAN, bool_3 BOOLEAN, bool_4 BOOLEAN, bool_5 BOOLEAN,
    bool_6 BOOLEAN, bool_7 BOOLEAN, bool_8 BOOLEAN, bool_9 BOOLEAN, bool_10 BOOLEAN,
    
    -- 5 JSONB columns (varying complexity)
    jsonb_simple JSONB,      -- simple key-value
    jsonb_nested JSONB,      -- nested object
    jsonb_array JSONB,       -- array
    jsonb_large JSONB,       -- larger object with many keys
    jsonb_mixed JSONB,       -- mixed types
    
    -- 4 numeric columns (precise decimals)
    numeric_1 NUMERIC(18,4), numeric_2 NUMERIC(18,4), numeric_3 NUMERIC(18,4), numeric_4 NUMERIC(18,4),
    
    -- Row hash columns (excluded from hash)
    _row_hash UUID,
    _row_hash_at TIMESTAMPTZ
);
EOSQL

    # Insert test data in batches to avoid OOM
    batch_size=100000
    inserted=0
    while [ $inserted -lt $row_count ]; do
        remaining=$((row_count - inserted))
        if [ $remaining -gt $batch_size ]; then
            this_batch=$batch_size
        else
            this_batch=$remaining
        fi
        
        run_sql "
INSERT INTO bench_100col (
    int_1, int_2, int_3, int_4, int_5, int_6, int_7, int_8, int_9, int_10,
    bigint_1, bigint_2, bigint_3, bigint_4, bigint_5, bigint_6, bigint_7, bigint_8, bigint_9, bigint_10,
    float_1, float_2, float_3, float_4, float_5, float_6, float_7, float_8, float_9, float_10,
    text_short_1, text_short_2, text_short_3, text_short_4, text_short_5,
    text_short_6, text_short_7, text_short_8, text_short_9, text_short_10,
    text_med_1, text_med_2, text_med_3, text_med_4, text_med_5,
    text_med_6, text_med_7, text_med_8, text_med_9, text_med_10,
    bool_1, bool_2, bool_3, bool_4, bool_5, bool_6, bool_7, bool_8, bool_9, bool_10,
    jsonb_simple, jsonb_nested, jsonb_array, jsonb_large, jsonb_mixed,
    numeric_1, numeric_2, numeric_3, numeric_4
)
SELECT
    -- integers
    (random()*1000000)::int, (random()*1000000)::int, (random()*1000000)::int,
    (random()*1000000)::int, (random()*1000000)::int, (random()*1000000)::int,
    (random()*1000000)::int, (random()*1000000)::int, (random()*1000000)::int,
    (random()*1000000)::int,
    -- bigints
    (random()*10000000000)::bigint, (random()*10000000000)::bigint, (random()*10000000000)::bigint,
    (random()*10000000000)::bigint, (random()*10000000000)::bigint, (random()*10000000000)::bigint,
    (random()*10000000000)::bigint, (random()*10000000000)::bigint, (random()*10000000000)::bigint,
    (random()*10000000000)::bigint,
    -- floats
    random()*1000000, random()*1000000, random()*1000000, random()*1000000, random()*1000000,
    random()*1000000, random()*1000000, random()*1000000, random()*1000000, random()*1000000,
    -- short text (32 chars)
    md5(random()::text), md5(random()::text), md5(random()::text), md5(random()::text), md5(random()::text),
    md5(random()::text), md5(random()::text), md5(random()::text), md5(random()::text), md5(random()::text),
    -- medium text (100+ chars)
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    md5(random()::text) || md5(random()::text) || md5(random()::text),
    -- booleans
    random() > 0.5, random() > 0.5, random() > 0.5, random() > 0.5, random() > 0.5,
    random() > 0.5, random() > 0.5, random() > 0.5, random() > 0.5, random() > 0.5,
    -- jsonb columns
    jsonb_build_object('id', i, 'value', random()),
    jsonb_build_object('user', jsonb_build_object('id', i, 'name', 'user_' || i, 'active', random() > 0.5)),
    jsonb_build_array(i, i+1, i+2, i+3, i+4),
    jsonb_build_object('a', i, 'b', random(), 'c', 'text_' || i, 'd', random() > 0.5,
                       'e', i*2, 'f', random()*100, 'g', 'data_' || i, 'h', now()::text),
    jsonb_build_object('number', i, 'float', random(), 'string', md5(i::text), 
                       'bool', random() > 0.5, 'array', jsonb_build_array(1,2,3)),
    -- numerics
    (random()*100000)::numeric(18,4), (random()*100000)::numeric(18,4),
    (random()*100000)::numeric(18,4), (random()*100000)::numeric(18,4)
FROM generate_series(1, $this_batch) i;
"
        inserted=$((inserted + this_batch))
        echo "  Inserted $inserted / $row_count rows..."
    done

    run_sql "ANALYZE bench_100col;"
    
    echo "Table populated with $row_count rows"
    
    # Get actual column count (excluding _row_hash*)
    run_sql "SELECT COUNT(*) as column_count FROM information_schema.columns 
             WHERE table_name = 'bench_100col' AND column_name NOT LIKE '_row_hash%';"
    
    echo ""
    echo "--- Benchmark: md5(ROW(cols)::text)::uuid ---"
    
    # Run benchmark using \timing
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c "
-- Build column list
CREATE TEMP TABLE _bench_cols AS
SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) as cols
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'bench_100col'
  AND column_name NOT LIKE '_row_hash%';
"

    # Get column count for display
    col_count=$(docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -tAc "
        SELECT COUNT(*) FROM information_schema.columns 
        WHERE table_name = 'bench_100col' AND column_name NOT LIKE '_row_hash%';
    ")
    
    echo "Columns: $col_count"
    echo ""
    
    echo "1. SELECT hash computation (read-only baseline):"
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c '\timing on' -c "
SELECT COUNT(*) as hashed_rows FROM (
    SELECT md5(ROW(
        id, uuid_1, uuid_2, uuid_3, uuid_4, uuid_5, uuid_6, uuid_7, uuid_8, uuid_9, uuid_10,
        int_1, int_2, int_3, int_4, int_5, int_6, int_7, int_8, int_9, int_10,
        bigint_1, bigint_2, bigint_3, bigint_4, bigint_5, bigint_6, bigint_7, bigint_8, bigint_9, bigint_10,
        float_1, float_2, float_3, float_4, float_5, float_6, float_7, float_8, float_9, float_10,
        ts_1, ts_2, ts_3, ts_4, ts_5, ts_6, ts_7, ts_8, ts_9, ts_10,
        date_1, date_2, date_3, date_4, date_5, date_6, date_7, date_8, date_9, date_10,
        text_short_1, text_short_2, text_short_3, text_short_4, text_short_5,
        text_short_6, text_short_7, text_short_8, text_short_9, text_short_10,
        text_med_1, text_med_2, text_med_3, text_med_4, text_med_5,
        text_med_6, text_med_7, text_med_8, text_med_9, text_med_10,
        bool_1, bool_2, bool_3, bool_4, bool_5, bool_6, bool_7, bool_8, bool_9, bool_10,
        jsonb_simple, jsonb_nested, jsonb_array, jsonb_large, jsonb_mixed,
        numeric_1, numeric_2, numeric_3, numeric_4
    )::text)::uuid as hash
    FROM bench_100col
) x;
"
    
    echo ""
    echo "2. UPDATE all rows (initial hash population):"
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c '\timing on' -c "
UPDATE bench_100col SET 
    _row_hash = md5(ROW(
        id, uuid_1, uuid_2, uuid_3, uuid_4, uuid_5, uuid_6, uuid_7, uuid_8, uuid_9, uuid_10,
        int_1, int_2, int_3, int_4, int_5, int_6, int_7, int_8, int_9, int_10,
        bigint_1, bigint_2, bigint_3, bigint_4, bigint_5, bigint_6, bigint_7, bigint_8, bigint_9, bigint_10,
        float_1, float_2, float_3, float_4, float_5, float_6, float_7, float_8, float_9, float_10,
        ts_1, ts_2, ts_3, ts_4, ts_5, ts_6, ts_7, ts_8, ts_9, ts_10,
        date_1, date_2, date_3, date_4, date_5, date_6, date_7, date_8, date_9, date_10,
        text_short_1, text_short_2, text_short_3, text_short_4, text_short_5,
        text_short_6, text_short_7, text_short_8, text_short_9, text_short_10,
        text_med_1, text_med_2, text_med_3, text_med_4, text_med_5,
        text_med_6, text_med_7, text_med_8, text_med_9, text_med_10,
        bool_1, bool_2, bool_3, bool_4, bool_5, bool_6, bool_7, bool_8, bool_9, bool_10,
        jsonb_simple, jsonb_nested, jsonb_array, jsonb_large, jsonb_mixed,
        numeric_1, numeric_2, numeric_3, numeric_4
    )::text)::uuid,
    _row_hash_at = now()
WHERE _row_hash IS NULL;
"

    echo ""
    echo "3. UPDATE no changes (re-run after hashes populated):"
    docker exec "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DB" -c '\timing on' -c "
UPDATE bench_100col SET 
    _row_hash = md5(ROW(
        id, uuid_1, uuid_2, uuid_3, uuid_4, uuid_5, uuid_6, uuid_7, uuid_8, uuid_9, uuid_10,
        int_1, int_2, int_3, int_4, int_5, int_6, int_7, int_8, int_9, int_10,
        bigint_1, bigint_2, bigint_3, bigint_4, bigint_5, bigint_6, bigint_7, bigint_8, bigint_9, bigint_10,
        float_1, float_2, float_3, float_4, float_5, float_6, float_7, float_8, float_9, float_10,
        ts_1, ts_2, ts_3, ts_4, ts_5, ts_6, ts_7, ts_8, ts_9, ts_10,
        date_1, date_2, date_3, date_4, date_5, date_6, date_7, date_8, date_9, date_10,
        text_short_1, text_short_2, text_short_3, text_short_4, text_short_5,
        text_short_6, text_short_7, text_short_8, text_short_9, text_short_10,
        text_med_1, text_med_2, text_med_3, text_med_4, text_med_5,
        text_med_6, text_med_7, text_med_8, text_med_9, text_med_10,
        bool_1, bool_2, bool_3, bool_4, bool_5, bool_6, bool_7, bool_8, bool_9, bool_10,
        jsonb_simple, jsonb_nested, jsonb_array, jsonb_large, jsonb_mixed,
        numeric_1, numeric_2, numeric_3, numeric_4
    )::text)::uuid,
    _row_hash_at = now()
WHERE _row_hash IS DISTINCT FROM md5(ROW(
        id, uuid_1, uuid_2, uuid_3, uuid_4, uuid_5, uuid_6, uuid_7, uuid_8, uuid_9, uuid_10,
        int_1, int_2, int_3, int_4, int_5, int_6, int_7, int_8, int_9, int_10,
        bigint_1, bigint_2, bigint_3, bigint_4, bigint_5, bigint_6, bigint_7, bigint_8, bigint_9, bigint_10,
        float_1, float_2, float_3, float_4, float_5, float_6, float_7, float_8, float_9, float_10,
        ts_1, ts_2, ts_3, ts_4, ts_5, ts_6, ts_7, ts_8, ts_9, ts_10,
        date_1, date_2, date_3, date_4, date_5, date_6, date_7, date_8, date_9, date_10,
        text_short_1, text_short_2, text_short_3, text_short_4, text_short_5,
        text_short_6, text_short_7, text_short_8, text_short_9, text_short_10,
        text_med_1, text_med_2, text_med_3, text_med_4, text_med_5,
        text_med_6, text_med_7, text_med_8, text_med_9, text_med_10,
        bool_1, bool_2, bool_3, bool_4, bool_5, bool_6, bool_7, bool_8, bool_9, bool_10,
        jsonb_simple, jsonb_nested, jsonb_array, jsonb_large, jsonb_mixed,
        numeric_1, numeric_2, numeric_3, numeric_4
    )::text)::uuid;
"

    echo ""
done

echo ""
echo "=== Benchmark Complete ==="
