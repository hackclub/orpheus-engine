-- rowhash extension
-- Fast row hashing for PostgreSQL using xxHash
-- Hashes rows directly from binary representation (2-3x faster than text conversion)

\echo Use "CREATE EXTENSION rowhash" to load this file. \quit

-- 128-bit hash (collision-safe for 1B+ rows) - returns bytea (16 bytes)
CREATE OR REPLACE FUNCTION rowhash_128b(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_rowhash_xxh3_128b'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- 128-bit hash - returns hex string (32 chars)
CREATE OR REPLACE FUNCTION rowhash_128(record)
RETURNS varchar(32)
AS 'MODULE_PATHNAME', 'pg_rowhash_xxh3_128'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- 64-bit hash (for tables < 1B rows) - returns bytea (8 bytes)
CREATE OR REPLACE FUNCTION rowhash_64b(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_rowhash_xxh3_64b'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Convenience: cast bytea hash to UUID for indexing
-- Usage: rowhash_128b(t.*)::uuid (requires the 16-byte bytea)
COMMENT ON FUNCTION rowhash_128b(record) IS 
'Hash a row using xxh3_128. Returns 16-byte bytea. 2-3x faster than xxh3_128b(row::text). Safe for 1B+ rows.';

COMMENT ON FUNCTION rowhash_128(record) IS 
'Hash a row using xxh3_128. Returns 32-char hex string. 2-3x faster than xxh3_128(row::text). Safe for 1B+ rows.';

COMMENT ON FUNCTION rowhash_64b(record) IS 
'Hash a row using xxh3_64. Returns 8-byte bytea. Use only for tables < 1B rows.';
