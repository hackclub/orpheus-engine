-- Unified Row Hash Script using xxHash (xxh3_64)
-- Purpose: Generate unique hashes for all rows in a table for change detection
-- Usage: Set the variables below and run this script

-- Configuration (modify these for your table)
-- Example: \set target_table 'public.my_table'
-- Example: \set primary_key_column 'id'

-- Function to hash an entire row using xxh3_64 (fastest xxHash variant)
-- Converts the row to text representation and hashes it
CREATE OR REPLACE FUNCTION hash_row_xxh3(row_data anyelement)
RETURNS varchar(16) AS $$
BEGIN
    RETURN xxh3_64(row_data::text);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Alternative: Direct SQL approach for maximum performance
-- This avoids PL/pgSQL overhead by using pure SQL

-- Generate row hashes for a table
-- Returns: primary_key, row_hash
-- The hash is computed over the entire row converted to text

-- Example query pattern (replace with actual table/pk):
-- SELECT id, xxh3_64(t.*::text) as row_hash FROM my_table t;

-- For tables with composite primary keys, concatenate them:
-- SELECT id1 || '|' || id2 as pk, xxh3_64(t.*::text) as row_hash FROM my_table t;

-- Batch processing for very large tables (10B+ rows)
-- Use COPY for fastest export of hashes

-- Create a reusable function to generate hash query for any table
CREATE OR REPLACE FUNCTION generate_row_hash_query(
    p_schema_name text,
    p_table_name text,
    p_pk_columns text[]  -- Array of primary key column names
)
RETURNS text AS $$
DECLARE
    pk_expr text;
    full_table_name text;
BEGIN
    full_table_name := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);
    
    -- Build primary key expression
    IF array_length(p_pk_columns, 1) = 1 THEN
        pk_expr := quote_ident(p_pk_columns[1]) || '::text';
    ELSE
        pk_expr := 'concat_ws(''|'', ' || 
            array_to_string(
                ARRAY(SELECT quote_ident(col) || '::text' FROM unnest(p_pk_columns) col),
                ', '
            ) || ')';
    END IF;
    
    RETURN format(
        'SELECT %s as pk, xxh3_64(t.*::text) as row_hash FROM %s t',
        pk_expr,
        full_table_name
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- Optimized batch hash export using COPY
-- This is the fastest way to export hashes for comparison
CREATE OR REPLACE FUNCTION export_row_hashes_to_csv(
    p_schema_name text,
    p_table_name text,
    p_pk_columns text[],
    p_output_path text
)
RETURNS void AS $$
DECLARE
    hash_query text;
BEGIN
    hash_query := generate_row_hash_query(p_schema_name, p_table_name, p_pk_columns);
    
    EXECUTE format(
        'COPY (%s) TO %L WITH (FORMAT CSV, HEADER)',
        hash_query,
        p_output_path
    );
END;
$$ LANGUAGE plpgsql;

-- Parallel hash computation for very large tables
-- Uses parallel query execution when available
CREATE OR REPLACE FUNCTION compute_row_hashes_parallel(
    p_schema_name text,
    p_table_name text,
    p_pk_columns text[],
    p_batch_size bigint DEFAULT 1000000
)
RETURNS TABLE(pk text, row_hash varchar(16)) AS $$
DECLARE
    hash_query text;
BEGIN
    -- Enable parallel query
    SET LOCAL max_parallel_workers_per_gather = 8;
    SET LOCAL parallel_tuple_cost = 0.001;
    SET LOCAL parallel_setup_cost = 100;
    
    hash_query := generate_row_hash_query(p_schema_name, p_table_name, p_pk_columns);
    
    RETURN QUERY EXECUTE hash_query;
END;
$$ LANGUAGE plpgsql;

-- Streaming hash computation with cursor for memory efficiency
-- Useful when you need to process hashes row by row
CREATE OR REPLACE FUNCTION stream_row_hashes(
    p_schema_name text,
    p_table_name text,
    p_pk_columns text[]
)
RETURNS SETOF record AS $$
DECLARE
    hash_query text;
    cur REFCURSOR;
    rec record;
BEGIN
    hash_query := generate_row_hash_query(p_schema_name, p_table_name, p_pk_columns);
    
    OPEN cur FOR EXECUTE hash_query;
    LOOP
        FETCH cur INTO rec;
        EXIT WHEN NOT FOUND;
        RETURN NEXT rec;
    END LOOP;
    CLOSE cur;
END;
$$ LANGUAGE plpgsql;

-- Convert bytea hash to bigint for XOR operations
CREATE OR REPLACE FUNCTION bytea_to_bigint(b bytea)
RETURNS bigint AS $$
BEGIN
    RETURN get_byte(b, 0)::bigint << 56 |
           get_byte(b, 1)::bigint << 48 |
           get_byte(b, 2)::bigint << 40 |
           get_byte(b, 3)::bigint << 32 |
           get_byte(b, 4)::bigint << 24 |
           get_byte(b, 5)::bigint << 16 |
           get_byte(b, 6)::bigint << 8 |
           get_byte(b, 7)::bigint;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Count and hash summary for quick table comparison
-- Uses XOR aggregation which is order-independent and memory-efficient
CREATE OR REPLACE FUNCTION table_hash_summary(
    p_schema_name text,
    p_table_name text
)
RETURNS TABLE(
    total_rows bigint,
    xor_checksum bigint,
    computed_at timestamptz
) AS $$
DECLARE
    full_table_name text;
BEGIN
    full_table_name := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);
    
    -- Use bit_xor on hash values - order independent and O(1) memory
    -- Uses xxh3_64b (bytea) for speed
    RETURN QUERY EXECUTE format(
        'SELECT 
            COUNT(*)::bigint as total_rows,
            bit_xor(bytea_to_bigint(xxh3_64b(t.*::text))) as xor_checksum,
            now() as computed_at
         FROM %s t',
        full_table_name
    );
END;
$$ LANGUAGE plpgsql;
