-- Row hash computation using pg_rowhash extension (xxHash on binary data)
-- Requires: CREATE EXTENSION rowhash;
-- rowhash_128b automatically excludes _row_hash* columns

DO $$
DECLARE
  target_table text := 'public.my_table';  -- <=== CHANGE THIS
  batch_size int := 100000;
  
  max_workers int;
  total_rows bigint;
  batch_count int := 0;
  batch_updated bigint;
  total_updated bigint := 0;
  start_ts timestamptz;
  batch_start timestamptz;
  elapsed_ms numeric;
  total_elapsed_sec numeric;
  eta_sec numeric;
  eta_text text;
BEGIN
  start_ts := clock_timestamp();
  
  -- Verify rowhash extension is installed
  IF NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'rowhash') THEN
    RAISE EXCEPTION 'rowhash extension not installed. Run: CREATE EXTENSION rowhash;';
  END IF;
  
  -- Maximize parallelism
  SELECT current_setting('max_parallel_workers')::int INTO max_workers;
  EXECUTE format('SET max_parallel_workers_per_gather = %s', max_workers);
  SET parallel_tuple_cost = 0.001;
  SET parallel_setup_cost = 100;
  SET min_parallel_table_scan_size = '1MB';
  
  RAISE NOTICE 'Parallelism: % workers available', max_workers;
  
  -- Add hash columns if they don't exist
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _row_hash bytea', target_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _row_hash_at timestamptz', target_table);
  
  -- Count total rows
  EXECUTE format('SELECT COUNT(*) FROM %s', target_table) INTO total_rows;
  RAISE NOTICE 'Table: % (% rows)', target_table, to_char(total_rows, 'FM999,999,999,999');
  RAISE NOTICE 'Processing in batches of %...', batch_size;
  
  -- Process in batches
  LOOP
    batch_start := clock_timestamp();
    batch_count := batch_count + 1;
    
    EXECUTE format('
      WITH to_update AS (
        SELECT ctid
        FROM %s t
        WHERE _row_hash IS DISTINCT FROM rowhash_128b(t.*)
        LIMIT %s
        FOR UPDATE SKIP LOCKED
      )
      UPDATE %s t SET 
        _row_hash = rowhash_128b(t.*),
        _row_hash_at = now()
      FROM to_update
      WHERE t.ctid = to_update.ctid',
      target_table, batch_size, target_table);
    
    GET DIAGNOSTICS batch_updated = ROW_COUNT;
    total_updated := total_updated + batch_updated;
    
    elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - batch_start)) * 1000;
    total_elapsed_sec := EXTRACT(EPOCH FROM (clock_timestamp() - start_ts));
    
    -- Calculate ETA
    IF total_updated > 0 THEN
      eta_sec := (total_rows - total_updated)::numeric * total_elapsed_sec / total_updated;
      eta_text := '';
      IF eta_sec >= 3600 THEN
        eta_text := floor(eta_sec / 3600)::text || 'h ';
        eta_sec := eta_sec - floor(eta_sec / 3600) * 3600;
      END IF;
      IF eta_sec >= 60 OR eta_text != '' THEN
        eta_text := eta_text || floor(eta_sec / 60)::text || 'm ';
        eta_sec := eta_sec - floor(eta_sec / 60) * 60;
      END IF;
      eta_text := eta_text || floor(eta_sec)::text || 's';
    ELSE
      eta_text := '?';
    END IF;
    
    RAISE NOTICE 'Batch %: % rows in % ms (% rows/sec) - Total: %/% - ETA: %', 
      to_char(batch_count, 'FM999,999'),
      to_char(batch_updated, 'FM999,999,999'),
      to_char(round(elapsed_ms::numeric, 0), 'FM999,999'),
      to_char(CASE WHEN elapsed_ms > 0 THEN round(batch_updated / (elapsed_ms / 1000), 0) ELSE 0 END, 'FM999,999,999'),
      to_char(total_updated, 'FM999,999,999,999'),
      to_char(total_rows, 'FM999,999,999,999'),
      eta_text;
    
    EXIT WHEN batch_updated = 0;
    
    PERFORM pg_sleep(0.01);
  END LOOP;
  
  elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - start_ts)) * 1000;
  
  RAISE NOTICE '=== Complete: % rows in %.1f sec (% rows/sec avg) ===', 
    to_char(total_updated, 'FM999,999,999,999'), elapsed_ms / 1000,
    to_char(CASE WHEN elapsed_ms > 0 THEN round(total_updated / (elapsed_ms / 1000), 0) ELSE 0 END, 'FM999,999,999');
END $$;
