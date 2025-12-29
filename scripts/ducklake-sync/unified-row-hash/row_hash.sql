-- row_hash: Hash a row excluding _row_hash* columns
-- Uses row_to_json -> jsonb for fast key removal, then md5
-- No custom extension needed - works on PostgreSQL 12+

CREATE OR REPLACE FUNCTION row_hash(r anyelement)
RETURNS text AS $$
  SELECT md5((row_to_json(r)::jsonb - '_row_hash' - '_row_hash_at')::text)
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION row_hash(anyelement) IS 
'Hash a row using md5, excluding _row_hash and _row_hash_at columns. Returns 32-char hex string.';
