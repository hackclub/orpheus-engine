{{ config(
    schema='hackatime_legacy_analytics',
    materialized='table'
) }}

WITH
/* 0️⃣  Session-wide constants */
params AS (
    SELECT
        120::int            AS hb_timeout,                       -- cap an idle gap at 2 min
        'America/New_York'  AS tz,                               -- reporting zone
        (EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC'
                             - INTERVAL '100 year')))::double precision -- super long for prod, shorten to speed up debug queries
                           AS cutoff_utc                         -- last hour (unix-seconds)
),

/* 1️⃣  Legacy heartbeats use timestamp(3) without time zone, convert to unix seconds */
hb_sec AS (
    SELECT
        hb.*,
        EXTRACT(EPOCH FROM hb."time")::double precision AS ts_sec
    FROM {{ source('hackatime_legacy_raw', 'heartbeats') }} hb
    WHERE hb.category = 'coding'                -- matches the partial-index predicate
),

/* 2️⃣  Keep only rows from the last hour – SELECT *only* heartbeat columns */
recent_hb AS (
    SELECT h.*                                   -- <- no columns from params, avoids duplicates
    FROM   hb_sec h
    JOIN   params p ON TRUE
    WHERE  h.ts_sec >= p.cutoff_utc
      AND  h.ts_sec <= 253402300799              -- sanity upper bound
),

/* 3️⃣  Gap to previous beat per user (needs hb_timeout & tz just once) */
hb_with_gaps AS (
    SELECT
        h.*,
        to_timestamp(h.ts_sec) AT TIME ZONE p.tz                       AS local_ts,
        LAG(h.ts_sec) OVER (PARTITION BY h.user_id ORDER BY h.ts_sec)  AS prev_ts,
        p.hb_timeout
    FROM recent_hb h
    CROSS JOIN params p
),

/* 4️⃣  Drop each capped-gap second into its hour bucket */
hb_hourly AS (
    SELECT
        user_id,
        project                                                      AS project_name,
        DATE_TRUNC('hour', local_ts)::timestamptz                    AS activity_time,
        LEAST(GREATEST(ts_sec - COALESCE(prev_ts, ts_sec), 0),
              hb_timeout)                                            AS delta_sec,
        language,
        editor,
        entity
    FROM hb_with_gaps
),

/* 5️⃣  Aggregate per user × project × hour, keep only active hours */
sec_per_hour AS (
    SELECT
        user_id,
        project_name,
        activity_time,
        SUM(delta_sec) AS seconds_hour,
        STRING_AGG(DISTINCT language, ', ' ORDER BY language)        AS languages_used,
        STRING_AGG(
            DISTINCT regexp_replace(entity, '.*/', ''), ', '
            ORDER BY regexp_replace(entity, '.*/', '')
        ) AS files_edited,
        STRING_AGG(DISTINCT editor, ', ' ORDER BY editor)            AS editors_used
    FROM hb_hourly
    GROUP BY user_id, project_name, activity_time
    HAVING SUM(delta_sec) > 0
),

/* 6️⃣  Email from users table (different from current hackatime) */
emails AS (
    SELECT 
           id AS user_id,
           LOWER(email) AS hackatime_first_email
    FROM hackatime_legacy.users
    WHERE email IS NOT NULL
)

/* 7️⃣  Final result */
SELECT
    s.user_id                                 AS hackatime_user_id,
    e.hackatime_first_email,
    s.project_name,
    s.activity_time,                          -- tz-aware hour start
    ROUND(LEAST(s.seconds_hour::numeric / 3600, 1), 2) AS hackatime_hours,
    s.languages_used,
    s.files_edited,
    s.editors_used
FROM sec_per_hour s
LEFT JOIN emails  e ON e.user_id = s.user_id
ORDER BY s.activity_time DESC,
         s.user_id,
         s.project_name
