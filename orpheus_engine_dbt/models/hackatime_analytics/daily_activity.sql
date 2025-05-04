{{ config(
    schema='hackatime_analytics',
    materialized='table'
) }}

WITH params AS (
    SELECT
        120::int            AS hb_timeout,          -- 2‑minute cap
        'America/New_York'  AS tz                   -- reporting zone
),

-- Normalise time to seconds & apply scopes
clean_hb AS (
    SELECT
        hb.*,
        CASE
            WHEN hb.time > 32503680000 * 1000  THEN hb.time / 1000000   -- µs → s
            WHEN hb.time > 32503680000         THEN hb.time / 1000      -- ms → s
            ELSE                                    hb.time             -- already s
        END::double precision AS ts_sec
    -- Use the dbt source function to reference the upstream table
    FROM {{ source('hackatime_raw', 'heartbeats') }} hb
    WHERE hb.category = 'coding'                               -- coding_only
),

valid_hb AS (
    SELECT *
    FROM clean_hb
    WHERE ts_sec BETWEEN 0 AND 253402300799                     -- with_valid_timestamps
),

-- Per‑day capped seconds
hb_daily AS (
    SELECT
        user_id,
        (to_timestamp(ts_sec) AT TIME ZONE p.tz)::date AS activity_date,
        SUM( LEAST(ts_sec - COALESCE(prev_ts, ts_sec), p.hb_timeout) ) AS seconds_day
    FROM (
        SELECT
            user_id,
            ts_sec,
            LAG(ts_sec) OVER (PARTITION BY user_id ORDER BY ts_sec) AS prev_ts
        FROM valid_hb
    ) x
    CROSS JOIN params p
    GROUP BY user_id, activity_date
),

-- Per‑day language list
lang_daily AS (
    SELECT
        user_id,
        (to_timestamp(ts_sec) AT TIME ZONE p.tz)::date AS activity_date,
        STRING_AGG(DISTINCT language, ', ' ORDER BY language) AS languages
    FROM valid_hb
    CROSS JOIN params p
    GROUP BY user_id, activity_date
)

-- Final projection
SELECT
    d.user_id                                       AS hackatime_user_id,
    d.activity_date,
    ROUND(d.seconds_day::numeric / 3600, 2)         AS hackatime_hours,
    COALESCE(l.languages, '')                       AS languages_used
FROM       hb_daily d
LEFT JOIN  lang_daily l
    ON  l.user_id       = d.user_id
    AND  l.activity_date = d.activity_date
ORDER BY   d.activity_date DESC,
        d.user_id 