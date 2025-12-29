/*
 * pg_rowhash - Fast row hashing for PostgreSQL
 * 
 * Hashes entire rows (tuples) from their binary representation,
 * automatically excluding columns that start with "_row_hash" to avoid
 * circular dependencies when storing hash values in the same table.
 * 
 * Uses xxHash (XXH3) - extremely fast non-cryptographic hash.
 * - 128-bit version for collision-safe hashing of tables with 1B+ rows
 * - 64-bit version for smaller tables
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#define XXH_INLINE_ALL
#include "xxhash.h"

PG_MODULE_MAGIC;

/*
 * Per-function cache to avoid repeated tuple descriptor lookups.
 * Stored in fn_extra and reused across all rows in a query.
 * 
 * Note: We keep a pinned reference to the TupleDesc. The pin is released
 * via a memory context callback when the context is reset/deleted.
 */
typedef struct RowHashCache
{
    MemoryContextCallback callback;
    Oid         tupType;
    int32       tupTypmod;
    TupleDesc   tupdesc;       /* pinned reference, released on context reset */
    int         natts;

    /* Reusable deformed-tuple storage */
    Datum      *values;
    bool       *nulls;

    /* Precomputed list of columns to hash (excludes dropped and _row_hash* cols) */
    int         nhashcols;
    int        *hashcols;      /* attribute indices to hash */
    int16      *attlen;
    bool       *attbyval;
} RowHashCache;

/*
 * Memory context callback to release the pinned TupleDesc when the
 * function's memory context is reset/deleted.
 */
static void
rowhash_cache_callback(void *arg)
{
    RowHashCache *cache = (RowHashCache *) arg;
    if (cache->tupdesc)
    {
        ReleaseTupleDesc(cache->tupdesc);
        cache->tupdesc = NULL;
    }
}

/*
 * Check if column name starts with "_row_hash" (case-insensitive)
 */
static bool
is_rowhash_column(const char *name)
{
    return (pg_strncasecmp(name, "_row_hash", 9) == 0);
}

/*
 * Get (and initialize) the cache from fn_extra.
 * Only calls lookup_rowtype_tupdesc when rowtype changes.
 */
static RowHashCache *
get_rowhash_cache(FunctionCallInfo fcinfo, HeapTupleHeader rec)
{
    RowHashCache *cache = (RowHashCache *) fcinfo->flinfo->fn_extra;
    Oid           tupType  = HeapTupleHeaderGetTypeId(rec);
    int32         tupTypmod = HeapTupleHeaderGetTypMod(rec);

    if (cache == NULL)
    {
        MemoryContext oldctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        cache = (RowHashCache *) palloc0(sizeof(RowHashCache));
        fcinfo->flinfo->fn_extra = cache;
        
        /* Register callback to release TupleDesc when context is reset */
        cache->callback.func = rowhash_cache_callback;
        cache->callback.arg = cache;
        MemoryContextRegisterResetCallback(fcinfo->flinfo->fn_mcxt, &cache->callback);
        
        MemoryContextSwitchTo(oldctx);
    }

    /* Refresh if first time or rowtype changed */
    if (cache->tupdesc == NULL ||
        cache->tupType != tupType ||
        cache->tupTypmod != tupTypmod)
    {
        TupleDesc tupdesc;
        int       natts;
        int       i, nhashcols;
        MemoryContext oldctx;

        if (cache->tupdesc)
            ReleaseTupleDesc(cache->tupdesc);

        tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
        natts   = tupdesc->natts;

        cache->tupType   = tupType;
        cache->tupTypmod = tupTypmod;
        cache->tupdesc   = tupdesc;
        cache->natts     = natts;

        oldctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

        if (cache->values)
        {
            pfree(cache->values);
            pfree(cache->nulls);
            pfree(cache->hashcols);
            pfree(cache->attlen);
            pfree(cache->attbyval);
        }

        cache->values   = (Datum *) palloc(natts * sizeof(Datum));
        cache->nulls    = (bool *) palloc(natts * sizeof(bool));
        cache->hashcols = (int *) palloc(natts * sizeof(int));
        cache->attlen   = (int16 *) palloc(natts * sizeof(int16));
        cache->attbyval = (bool *) palloc(natts * sizeof(bool));

        /* Build list of columns to hash, excluding dropped and _row_hash* */
        nhashcols = 0;
        for (i = 0; i < natts; i++)
        {
            Form_pg_attribute att = TupleDescAttr(tupdesc, i);

            if (att->attisdropped)
                continue;

            /* Skip columns starting with _row_hash */
            if (is_rowhash_column(NameStr(att->attname)))
                continue;

            cache->hashcols[nhashcols] = i;
            cache->attlen[nhashcols]   = att->attlen;
            cache->attbyval[nhashcols] = att->attbyval;
            nhashcols++;
        }
        cache->nhashcols = nhashcols;

        MemoryContextSwitchTo(oldctx);
    }

    return cache;
}

/*
 * Hash a single datum into the 128-bit state
 */
static inline void
hash_datum_128(XXH3_state_t *state, Datum value, bool isnull, int16 typlen, bool typbyval)
{
    if (isnull)
    {
        static const char null_marker[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
        XXH3_128bits_update(state, null_marker, sizeof(null_marker));
        return;
    }

    if (typbyval)
    {
        XXH3_128bits_update(state, &value, sizeof(Datum));
    }
    else if (typlen == -1)
    {
        struct varlena *val = PG_DETOAST_DATUM_PACKED(value);
        char *data = VARDATA_ANY(val);
        int len = VARSIZE_ANY_EXHDR(val);
        
        XXH3_128bits_update(state, &len, sizeof(int));
        XXH3_128bits_update(state, data, len);
        
        if ((Pointer)val != DatumGetPointer(value))
            pfree(val);
    }
    else if (typlen == -2)
    {
        char *str = DatumGetCString(value);
        int len = strlen(str);
        XXH3_128bits_update(state, &len, sizeof(int));
        XXH3_128bits_update(state, str, len);
    }
    else
    {
        XXH3_128bits_update(state, DatumGetPointer(value), typlen);
    }
}

/*
 * Hash a single datum into the 64-bit state
 */
static inline void
hash_datum_64(XXH3_state_t *state, Datum value, bool isnull, int16 typlen, bool typbyval)
{
    if (isnull)
    {
        static const char null_marker[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
        XXH3_64bits_update(state, null_marker, sizeof(null_marker));
        return;
    }

    if (typbyval)
    {
        XXH3_64bits_update(state, &value, sizeof(Datum));
    }
    else if (typlen == -1)
    {
        struct varlena *val = PG_DETOAST_DATUM_PACKED(value);
        char *data = VARDATA_ANY(val);
        int len = VARSIZE_ANY_EXHDR(val);
        
        XXH3_64bits_update(state, &len, sizeof(int));
        XXH3_64bits_update(state, data, len);
        
        if ((Pointer)val != DatumGetPointer(value))
            pfree(val);
    }
    else if (typlen == -2)
    {
        char *str = DatumGetCString(value);
        int len = strlen(str);
        XXH3_64bits_update(state, &len, sizeof(int));
        XXH3_64bits_update(state, str, len);
    }
    else
    {
        XXH3_64bits_update(state, DatumGetPointer(value), typlen);
    }
}

/*
 * rowhash_128b(record) -> bytea (16 bytes)
 * 
 * 128-bit xxHash of row, excluding columns starting with "_row_hash".
 * Use for tables with 1B+ rows where collision safety matters.
 */
PG_FUNCTION_INFO_V1(pg_rowhash_xxh3_128b);

Datum
pg_rowhash_xxh3_128b(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    RowHashCache   *cache;
    HeapTupleData   tuple;
    XXH3_state_t    state;
    XXH128_hash_t   hash;
    bytea          *result;
    int             i;

    cache = get_rowhash_cache(fcinfo, rec);

    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    heap_deform_tuple(&tuple, cache->tupdesc, cache->values, cache->nulls);

    XXH3_128bits_reset(&state);

    for (i = 0; i < cache->nhashcols; i++)
    {
        int   attnum = cache->hashcols[i];
        Datum value  = cache->values[attnum];
        bool  isnull = cache->nulls[attnum];

        /* Include column position for structure awareness */
        XXH3_128bits_update(&state, &attnum, sizeof(int));
        hash_datum_128(&state, value, isnull, cache->attlen[i], cache->attbyval[i]);
    }

    hash = XXH3_128bits_digest(&state);

    result = (bytea *) palloc(VARHDRSZ + 16);
    SET_VARSIZE(result, VARHDRSZ + 16);
    memcpy(VARDATA(result), &hash.high64, 8);
    memcpy(VARDATA(result) + 8, &hash.low64, 8);
    PG_RETURN_BYTEA_P(result);
}

/*
 * rowhash_128(record) -> varchar(32)
 * 
 * Same as rowhash_128b but returns hex string.
 */
PG_FUNCTION_INFO_V1(pg_rowhash_xxh3_128);

Datum
pg_rowhash_xxh3_128(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    RowHashCache   *cache;
    HeapTupleData   tuple;
    XXH3_state_t    state;
    XXH128_hash_t   hash;
    char           *hexstr;
    int             i;

    cache = get_rowhash_cache(fcinfo, rec);

    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    heap_deform_tuple(&tuple, cache->tupdesc, cache->values, cache->nulls);

    XXH3_128bits_reset(&state);

    for (i = 0; i < cache->nhashcols; i++)
    {
        int   attnum = cache->hashcols[i];
        Datum value  = cache->values[attnum];
        bool  isnull = cache->nulls[attnum];

        XXH3_128bits_update(&state, &attnum, sizeof(int));
        hash_datum_128(&state, value, isnull, cache->attlen[i], cache->attbyval[i]);
    }

    hash = XXH3_128bits_digest(&state);

    hexstr = palloc(33);
    snprintf(hexstr, 33, "%016llx%016llx", 
             (unsigned long long)hash.high64, 
             (unsigned long long)hash.low64);
    
    PG_RETURN_VARCHAR_P(cstring_to_text(hexstr));
}

/*
 * rowhash_64b(record) -> bytea (8 bytes)
 * 
 * 64-bit xxHash - faster, smaller. Use for tables < 1B rows.
 */
PG_FUNCTION_INFO_V1(pg_rowhash_xxh3_64b);

Datum
pg_rowhash_xxh3_64b(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    RowHashCache   *cache;
    HeapTupleData   tuple;
    XXH3_state_t    state;
    XXH64_hash_t    hash;
    bytea          *result;
    int             i;

    cache = get_rowhash_cache(fcinfo, rec);

    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    heap_deform_tuple(&tuple, cache->tupdesc, cache->values, cache->nulls);

    XXH3_64bits_reset(&state);

    for (i = 0; i < cache->nhashcols; i++)
    {
        int   attnum = cache->hashcols[i];
        Datum value  = cache->values[attnum];
        bool  isnull = cache->nulls[attnum];

        XXH3_64bits_update(&state, &attnum, sizeof(int));
        hash_datum_64(&state, value, isnull, cache->attlen[i], cache->attbyval[i]);
    }

    hash = XXH3_64bits_digest(&state);

    result = (bytea *) palloc(VARHDRSZ + 8);
    SET_VARSIZE(result, VARHDRSZ + 8);
    memcpy(VARDATA(result), &hash, 8);
    PG_RETURN_BYTEA_P(result);
}
