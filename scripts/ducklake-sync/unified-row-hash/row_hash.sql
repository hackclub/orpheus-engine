DO $$
DECLARE
  target_table text := 'public.my_table';  -- <=== CHANGE THIS
  cols text;
BEGIN
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
  
  -- Update hashes for changed rows
  EXECUTE format('
    UPDATE %s SET 
      _row_hash = md5(ROW(%s)::text)::uuid,
      _row_hash_at = now()
    WHERE _row_hash IS NULL 
       OR _row_hash IS DISTINCT FROM md5(ROW(%s)::text)::uuid',
    target_table, cols, cols);
  
  RAISE NOTICE 'Done: %', target_table;
END $$;
