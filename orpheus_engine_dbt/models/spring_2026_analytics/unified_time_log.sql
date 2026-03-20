{{ config(
    schema='spring_2026_analytics',
    materialized='table'
) }}

-- ============================================================================
-- Unified Time Log across Hack Club Programs (v4 — hourly, UTC)
--
-- Columns: activity_hour (UTC), program_name, user_email, project_name, code_url,
--          logging_method, raw_hours_logged, credited_hours_logged, split_factor, overlap_type,
--          overlapping_programs, hackatime_alias, source_detail, claim_started_at
--
-- Programs & sources:
--   Hackatime-based: stasis, flavortown, horizons, sleepover, hack_club_the_game
--   Custom logging:  stasis (work_sessions), blueprint (journal_entries),
--                    flavortown (post_devlogs), hack_club_the_game (project_reviews)
--
-- Key features:
--   1. Email normalization — strips plus-addressing (user+tag@x.com -> user@x.com)
--   2. Bad alias filtering — excludes junk hackatime project names
--   3. Claim start date enforcement — only attributes hours after claim date
--   4. Dual overlap detection — GREATEST(hackatime claim count, URL overlap count)
--   5. Flavortown devlog custom time — post_devlogs.duration_seconds
--   6. Proper sleepover joins — via projects__registered_users (not correlated subquery)
--   7. URL-based splitting applied to custom sources too
-- ============================================================================

WITH program_start_dates AS (
    SELECT * FROM (VALUES
        ('blueprint',  TIMESTAMP WITH TIME ZONE '2025-10-17 00:00:00+00'),
        ('flavortown', TIMESTAMP WITH TIME ZONE '2025-12-24 00:00:00+00'),
        ('sleepover',  TIMESTAMP WITH TIME ZONE '2026-01-20 00:00:00+00'),
        ('horizons',   TIMESTAMP WITH TIME ZONE '2026-02-20 00:00:00+00'),
        ('stasis',     TIMESTAMP WITH TIME ZONE '2026-03-04 00:00:00+00'),
        ('hack_club_the_game', TIMESTAMP WITH TIME ZONE '2026-01-22 00:00:00+00')
    ) AS t(program_name, program_start_date)
),

bad_aliases AS (
    SELECT alias FROM (VALUES
        (''), ('other'), ('<<last_project>>'), ('projects'), ('.wakatime'), ('.vscode')
    ) AS t(alias)
),

-- ============================================================
-- 1. HACKATIME HOURLY: raw coding hours grouped by (hour, user, alias)
-- ============================================================
hackatime_hourly AS (
    SELECT
        DATE_TRUNC('hour', hpa.activity_time) AS activity_hour,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(hpa.hackatime_first_email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(hpa.hackatime_first_email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(hpa.hackatime_first_email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(hpa.hackatime_first_email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hpa.project_name)) AS hackatime_alias,
        SUM(hpa.hackatime_hours) AS raw_hours_logged
    FROM {{ ref('hourly_project_activity') }} hpa
    WHERE hpa.hackatime_first_email IS NOT NULL
      AND hpa.project_name IS NOT NULL
      AND hpa.hackatime_hours > 0
      AND hpa.activity_time <= NOW()
    GROUP BY 1, 2, 3
),

-- ============================================================
-- 2. CUSTOM TIME SOURCES (non-hackatime)
-- ============================================================
stasis_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', ws."createdAt") AS activity_hour,
        'stasis'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj."githubRepo"), '') AS code_url,
        SUM(ws."hoursClaimed")::numeric AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('stasis.work_session.hoursClaimed; sessions=' || COUNT(*)::text) AS source_detail
    FROM {{ source('stasis', 'work_session') }} ws
    JOIN {{ source('stasis', 'project') }} proj ON proj.id = ws."projectId"
    JOIN {{ source('stasis', 'user') }} u ON u.id = proj."userId"
    WHERE ws."hoursClaimed" > 0
    GROUP BY 1, 2, 3, 4, 5
),

blueprint_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', je.created_at) AS activity_hour,
        'blueprint'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_link), '') AS code_url,
        ROUND(SUM(je.duration_seconds)::numeric / 3600.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('blueprint.journal_entries; entries=' || COUNT(*)::text) AS source_detail
    FROM {{ source('blueprint', 'journal_entries') }} je
    JOIN {{ source('blueprint', 'projects') }} proj ON proj.id = je.project_id
    JOIN {{ source('blueprint', 'users') }} u ON u.id = je.user_id
    WHERE je.duration_seconds > 0
    GROUP BY 1, 2, 3, 4, 5
),

flavortown_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', COALESCE(po.created_at, pd.created_at)) AS activity_hour,
        'flavortown'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        ROUND(SUM(pd.duration_seconds)::numeric / 3600.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('flavortown.post_devlogs; posts=' || COUNT(*)::text) AS source_detail
    FROM {{ source('flavortown', 'post_devlogs') }} pd
    JOIN {{ source('flavortown', 'posts') }} po
        ON po.postable_type = 'Post::Devlog' AND po.postable_id = pd.id
    JOIN {{ source('flavortown', 'projects') }} proj ON proj.id = po.project_id
    JOIN {{ source('flavortown', 'users') }} u ON u.id = po.user_id
    WHERE pd.duration_seconds > 0
    GROUP BY 1, 2, 3, 4, 5
),

hack_club_the_game_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', pr.created_at AT TIME ZONE 'UTC') AS activity_hour,
        'hack_club_the_game'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_link), '') AS code_url,
        ROUND(SUM(pr.approved_seconds)::numeric / 3600.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('hack_club_the_game.project_reviews.approved_seconds; reviews=' || COUNT(*)::text) AS source_detail
    FROM {{ source('hack_club_the_game', 'project_reviews') }} pr
    JOIN {{ source('hack_club_the_game', 'projects') }} proj ON proj.id = pr.project_id
    JOIN {{ source('hack_club_the_game', 'users') }} u ON u.id = proj.user_id
    WHERE pr.review_type = 'approval'
      AND pr.approved_seconds > 0
      AND pr.deleted_at IS NULL
    GROUP BY 1, 2, 3, 4, 5
),

-- ============================================================
-- 3. HACKATIME CLAIMS: per-program alias -> project mappings
-- ============================================================
stasis_ht_claims AS (
    SELECT 'stasis'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp."hackatimeProject")) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj."githubRepo"), '') AS code_url,
        hp."createdAt" AS claim_start_ts
    FROM {{ source('stasis', 'hackatime_project') }} hp
    JOIN {{ source('stasis', 'project') }} proj ON proj.id = hp."projectId"
    JOIN {{ source('stasis', 'user') }} u ON u.id = proj."userId"
    WHERE hp."hackatimeProject" IS NOT NULL AND hp."hackatimeProject" != ''
),

flavortown_ht_claims AS (
    SELECT 'flavortown'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(uhp.name)) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        uhp.created_at AS claim_start_ts
    FROM {{ source('flavortown', 'user_hackatime_projects') }} uhp
    JOIN {{ source('flavortown', 'users') }} u ON u.id = uhp.user_id
    JOIN {{ source('flavortown', 'projects') }} proj ON proj.id = uhp.project_id
    WHERE uhp.name IS NOT NULL AND uhp.name != '' AND uhp.project_id IS NOT NULL
),

horizons_ht_claims AS (
    SELECT 'horizons'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.project_title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        proj.created_at AS claim_start_ts
    FROM {{ source('horizons', 'projects') }} proj
    JOIN {{ source('horizons', 'users') }} u ON u.user_id = proj.user_id
    CROSS JOIN LATERAL unnest(proj.now_hackatime_projects::text[]) AS alias(alias_text)
    WHERE proj.now_hackatime_projects IS NOT NULL
      AND proj.now_hackatime_projects != '{}'
),

hack_club_the_game_ht_claims AS (
    SELECT 'hack_club_the_game'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp.name)) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_link), '') AS code_url,
        hp.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('hack_club_the_game', 'hackatime_projects') }} hp
    JOIN {{ source('hack_club_the_game', 'projects') }} proj ON proj.id = hp.project_id
    JOIN {{ source('hack_club_the_game', 'users') }} u ON u.id = hp.user_id
    WHERE hp.name IS NOT NULL AND hp.name != '' AND hp.project_id IS NOT NULL
),

sleepover_code_urls AS (
    SELECT y.userid,
           LOWER(BTRIM(y.project)) AS project_name_key,
           (ARRAY_AGG(NULLIF(BTRIM(y.code_url), '')
                      ORDER BY y.automation_first_submitted_at DESC NULLS LAST,
                               y.id DESC))[1] AS code_url
    FROM {{ source('airtable_sleepover', 'ysws_project_submission') }} y
    WHERE y.userid IS NOT NULL AND y.userid != ''
      AND y.project IS NOT NULL AND y.project != ''
      AND y.code_url IS NOT NULL AND y.code_url != ''
    GROUP BY 1, 2
),

sleepover_ht_claims AS (
    SELECT 'sleepover'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(ru.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(ru.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(ru.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(ru.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.name AS project_name,
        scu.code_url,
        proj.created_at AS claim_start_ts
    FROM {{ source('airtable_sleepover', 'projects') }} proj
    JOIN {{ source('airtable_sleepover', 'projects__registered_users') }} pr
        ON pr._dlt_parent_id = proj._dlt_id
    JOIN {{ source('airtable_sleepover', 'registered_users') }} ru
        ON ru.id = pr.value
    LEFT JOIN sleepover_code_urls scu
        ON scu.userid = proj.userid
        AND scu.project_name_key = LOWER(BTRIM(proj.name))
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(proj.hackatime_name::jsonb) AS alias_text
        WHERE proj.hackatime_name ~ '^\s*\[.*\]\s*$'
        UNION ALL
        SELECT proj.hackatime_name AS alias_text
        WHERE NOT (proj.hackatime_name ~ '^\s*\[.*\]\s*$')
    ) AS alias
    WHERE proj.hackatime_name IS NOT NULL AND proj.hackatime_name != ''
),

-- ============================================================
-- 4. MERGE CLAIMS & FILTER BAD ALIASES
-- ============================================================
all_claims_raw AS (
    SELECT * FROM stasis_ht_claims
    UNION ALL SELECT * FROM flavortown_ht_claims
    UNION ALL SELECT * FROM horizons_ht_claims
    UNION ALL SELECT * FROM sleepover_ht_claims
    UNION ALL SELECT * FROM hack_club_the_game_ht_claims
),

all_claims AS (
    SELECT
        c.program_name,
        c.user_email,
        c.hackatime_alias,
        COALESCE(
            MIN(c.project_name) FILTER (WHERE NULLIF(BTRIM(c.code_url), '') IS NOT NULL),
            MIN(c.project_name)
        ) AS project_name,
        MIN(c.code_url) FILTER (
            WHERE NULLIF(BTRIM(c.code_url), '') IS NOT NULL
        ) AS code_url,
        MIN(c.claim_start_ts) AS claim_start_ts
    FROM all_claims_raw c
    LEFT JOIN bad_aliases b ON b.alias = c.hackatime_alias
    WHERE c.hackatime_alias IS NOT NULL AND c.hackatime_alias != ''
      AND b.alias IS NULL
    GROUP BY 1, 2, 3
),

-- ============================================================
-- 5. URL-BASED CROSS-PROGRAM OVERLAP
-- ============================================================
all_project_urls AS (
    SELECT DISTINCT user_email,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(code_url, '\.git$', ''), '/+$', '')) AS norm_url,
        program_name
    FROM all_claims
    WHERE code_url IS NOT NULL AND code_url != ''
    UNION
    SELECT DISTINCT user_email,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(code_url, '\.git$', ''), '/+$', '')) AS norm_url,
        program_name
    FROM (
        SELECT user_email, code_url, program_name FROM stasis_custom_hourly WHERE code_url IS NOT NULL
        UNION ALL
        SELECT user_email, code_url, program_name FROM blueprint_custom_hourly WHERE code_url IS NOT NULL
        UNION ALL
        SELECT user_email, code_url, program_name FROM flavortown_custom_hourly WHERE code_url IS NOT NULL
        UNION ALL
        SELECT user_email, code_url, program_name FROM hack_club_the_game_custom_hourly WHERE code_url IS NOT NULL
    ) cust
),

url_overlap AS (
    SELECT user_email, norm_url,
        COUNT(DISTINCT program_name) AS num_programs,
        STRING_AGG(DISTINCT program_name, ', ' ORDER BY program_name) AS programs
    FROM all_project_urls
    WHERE norm_url IS NOT NULL AND norm_url != ''
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT program_name) > 1
),

-- ============================================================
-- 6. MATCH HACKATIME HOURS -> CLAIMS (only after claim_start_ts)
-- ============================================================
hackatime_matches AS (
    SELECT
        hd.activity_hour,
        ac.program_name,
        hd.user_email,
        ac.project_name,
        ac.code_url,
        hd.hackatime_alias,
        hd.raw_hours_logged,
        ac.claim_start_ts AS claim_started_at
    FROM hackatime_hourly hd
    JOIN all_claims ac
        ON ac.user_email = hd.user_email
        AND ac.hackatime_alias = hd.hackatime_alias
        AND hd.activity_hour >= DATE_TRUNC('hour', ac.claim_start_ts)
),

ht_claim_counts AS (
    SELECT activity_hour, user_email, hackatime_alias,
        COUNT(DISTINCT program_name) AS ht_claim_count,
        STRING_AGG(DISTINCT program_name, ', ' ORDER BY program_name) AS ht_claimed_programs
    FROM hackatime_matches
    GROUP BY 1, 2, 3
),

hackatime_split_hourly AS (
    SELECT
        hm.activity_hour,
        hm.program_name,
        hm.user_email,
        hm.project_name,
        hm.code_url,
        'hackatime'::text AS logging_method,
        hm.raw_hours_logged,
        ROUND((hm.raw_hours_logged / GREATEST(hcc.ht_claim_count, COALESCE(uo.num_programs, 1)))::numeric, 4) AS credited_hours_logged,
        GREATEST(hcc.ht_claim_count, COALESCE(uo.num_programs, 1))::smallint AS split_factor,
        CASE
            WHEN hcc.ht_claim_count > 1 AND COALESCE(uo.num_programs, 1) > 1 THEN 'both'
            WHEN hcc.ht_claim_count > 1 THEN 'hackatime_alias'
            WHEN COALESCE(uo.num_programs, 1) > 1 THEN 'code_url'
            ELSE 'none'
        END AS overlap_type,
        CASE
            WHEN GREATEST(hcc.ht_claim_count, COALESCE(uo.num_programs, 1)) > 1
            THEN (
                SELECT ARRAY_AGG(DISTINCT p ORDER BY p)
                FROM unnest(
                    string_to_array(COALESCE(hcc.ht_claimed_programs, ''), ', ')
                    || string_to_array(COALESCE(uo.programs, ''), ', ')
                ) AS p
                WHERE p != ''
            )
        END AS overlapping_programs,
        hm.hackatime_alias,
        NULL::text AS source_detail,
        hm.claim_started_at
    FROM hackatime_matches hm
    JOIN ht_claim_counts hcc
        ON hcc.activity_hour = hm.activity_hour
        AND hcc.user_email = hm.user_email
        AND hcc.hackatime_alias = hm.hackatime_alias
    LEFT JOIN url_overlap uo
        ON hm.code_url IS NOT NULL AND hm.code_url != ''
        AND hm.user_email = uo.user_email
        AND LOWER(REGEXP_REPLACE(REGEXP_REPLACE(hm.code_url, '\.git$', ''), '/+$', '')) = uo.norm_url
),

-- Apply URL-based splitting to custom sources too
custom_with_url_split AS (
    SELECT
        c.activity_hour,
        c.program_name,
        c.user_email,
        c.project_name,
        c.code_url,
        c.logging_method,
        c.raw_hours_logged,
        ROUND((c.raw_hours_logged / COALESCE(uo.num_programs, 1))::numeric, 4) AS credited_hours_logged,
        COALESCE(uo.num_programs, 1)::smallint AS split_factor,
        CASE WHEN COALESCE(uo.num_programs, 1) > 1 THEN 'code_url' ELSE 'none' END AS overlap_type,
        CASE WHEN COALESCE(uo.num_programs, 1) > 1
             THEN string_to_array(uo.programs, ', ')
        END AS overlapping_programs,
        NULL::text AS hackatime_alias,
        c.source_detail,
        NULL::timestamptz AS claim_started_at
    FROM (
        SELECT * FROM stasis_custom_hourly
        UNION ALL SELECT * FROM blueprint_custom_hourly
        UNION ALL SELECT * FROM flavortown_custom_hourly
        UNION ALL SELECT * FROM hack_club_the_game_custom_hourly
    ) c
    LEFT JOIN url_overlap uo
        ON c.code_url IS NOT NULL AND c.code_url != ''
        AND c.user_email = uo.user_email
        AND LOWER(REGEXP_REPLACE(REGEXP_REPLACE(c.code_url, '\.git$', ''), '/+$', '')) = uo.norm_url
)

-- ============================================================
-- 7. FINAL UNION
-- ============================================================
SELECT
    h.activity_hour,
    h.program_name,
    h.user_email,
    h.project_name,
    h.code_url,
    h.logging_method,
    h.raw_hours_logged,
    h.credited_hours_logged,
    h.split_factor,
    h.overlap_type,
    h.overlapping_programs,
    h.hackatime_alias,
    h.source_detail,
    h.claim_started_at
FROM hackatime_split_hourly h
JOIN program_start_dates psd ON psd.program_name = h.program_name
WHERE h.credited_hours_logged > 0
  AND h.activity_hour >= psd.program_start_date

UNION ALL

SELECT
    c.activity_hour,
    c.program_name,
    c.user_email,
    c.project_name,
    c.code_url,
    c.logging_method,
    c.raw_hours_logged,
    c.credited_hours_logged,
    c.split_factor,
    c.overlap_type,
    c.overlapping_programs,
    c.hackatime_alias,
    c.source_detail,
    c.claim_started_at
FROM custom_with_url_split c
JOIN program_start_dates psd ON psd.program_name = c.program_name
WHERE c.credited_hours_logged > 0
  AND c.activity_hour >= psd.program_start_date

ORDER BY activity_hour DESC, program_name, user_email, project_name
