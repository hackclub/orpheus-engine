DO $$
DECLARE
  target_table text := 'public.my_table';  -- <=== CHANGE THIS
  cols text;
  max_workers int;
  row_count bigint;
  updated_count bigint;
  start_ts timestamptz;
  elapsed_ms numeric;
BEGIN
  -- Maximize parallelism for this session
  SELECT current_setting('max_parallel_workers')::int INTO max_workers;
  EXECUTE format('SET max_parallel_workers_per_gather = %s', max_workers);
  SET parallel_tuple_cost = 0.001;
  SET parallel_setup_cost = 100;
  SET min_parallel_table_scan_size = '1MB';
  SET min_parallel_index_scan_size = '512kB';
  
  RAISE NOTICE 'Parallelism: % workers available', max_workers;
  
  -- Add hash columns if they don't exist
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _row_hash uuid', target_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _row_hash_at timestamptz', target_table);
  
  -- Get all columns except _row_hash*
  SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
  INTO cols
  FROM information_schema.columns
  WHERE table_schema = split_part(target_table, '.', 1)
    AND table_name = split_part(target_table, '.', 2)
    AND column_name NOT LIKE '_row_hash%';
  
  -- Get row count for progress reporting
  EXECUTE format('SELECT COUNT(*) FROM %s', target_table) INTO row_count;
  RAISE NOTICE 'Table: % (% rows, % columns)', target_table, row_count, 
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = split_part(target_table, '.', 1)
       AND table_name = split_part(target_table, '.', 2)
       AND column_name NOT LIKE '_row_hash%');
  
  start_ts := clock_timestamp();
  
  -- Update hashes for changed rows
  EXECUTE format('
    UPDATE %s SET 
      _row_hash = md5(ROW(%s)::text)::uuid,
      _row_hash_at = now()
    WHERE _row_hash IS NULL 
       OR _row_hash IS DISTINCT FROM md5(ROW(%s)::text)::uuid',
    target_table, cols, cols);
  
  GET DIAGNOSTICS updated_count = ROW_COUNT;
  elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - start_ts)) * 1000;
  
  RAISE NOTICE 'Updated % of % rows in % ms (% rows/sec)', 
    updated_count, row_count, round(elapsed_ms::numeric, 0),
    CASE WHEN elapsed_ms > 0 THEN round(updated_count / (elapsed_ms / 1000), 0) ELSE 0 END;
END $$;
