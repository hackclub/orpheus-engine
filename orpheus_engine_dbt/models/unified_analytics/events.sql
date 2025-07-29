{{ config(
    schema='unified_analytics',
    materialized='table'
) }}

-- Unified events table for monthly active user calculations across Hack Club platforms

WITH loops_events AS (
    SELECT *
    FROM {{ source('loops', 'audience') }}
    WHERE user_group = 'Hack Clubber'
    AND subscribed = true
),

-- Extract date-based events from loops audience data
loops_unpivoted AS (
    SELECT
        le.email,
        e.key AS column_name,
        -- Extract date from the value (handles both date and datetime formats)
        CASE
            WHEN e.value ~ '^\d{4}-\d{2}-\d{2}'
            THEN (substring(e.value::text FROM '^\d{4}-\d{2}-\d{2}'))::date
        END AS event_date,
        -- Convert column name to camelCase
        lower(left(replace(initcap(replace(e.key,'_',' ')),' ',''),1)) ||
        substr(replace(initcap(replace(e.key,'_',' ')),' ',''),2) AS event,
        -- Determine event_type based on whether it ends with 'ApprovedAt'
        CASE
            WHEN e.key LIKE '%_approved_at'
            THEN 'ship'
            ELSE 'loops'
        END AS event_type
    FROM loops_events le,
         LATERAL jsonb_each_text(to_jsonb(le) - 'birthday') AS e(key, value)
    WHERE
        -- Match date or datetime patterns
        e.value ~ '^\d{4}-\d{2}-\d{2}'
        -- Filter out null dates
        AND e.value IS NOT NULL
        AND e.value != ''
        -- Filter out columns that start with 'calculated'
        AND e.key NOT LIKE 'calculated%'
        -- Filter out specific unwanted events
        AND e.key != 'high_seas_last_synced_from_airtable'
),

-- Clean loops events
loops_final AS (
    SELECT
        event_date,
        email,
        event,
        event_type,
        'loops' AS source_system
    FROM loops_unpivoted
    WHERE event_date IS NOT NULL
        AND event_date >= '2024-01-01'  -- Filter events older than Jan 1, 2024
        AND event_date <= CURRENT_DATE + INTERVAL '1 day'  -- Filter future events, allow 1 day for timezone differences
),

-- Hackatime activity events aggregated by day
hackatime_events AS (
    SELECT
        DATE(activity_time) AS event_date,
        hackatime_first_email AS email,
        'hackatimeActivity' AS event,
        'hackatime' AS event_type,
        'hackatime' AS source_system
    FROM {{ ref('hourly_project_activity') }}
    WHERE hackatime_first_email IS NOT NULL
        AND activity_time IS NOT NULL
        AND hackatime_hours > 0
        AND DATE(activity_time) >= '2024-01-01'  -- Filter events older than Jan 1, 2024
        AND DATE(activity_time) <= CURRENT_DATE + INTERVAL '1 day'  -- Filter future events, allow 1 day for timezone differences
    GROUP BY DATE(activity_time), hackatime_first_email
),

-- Hackatime Legacy activity events aggregated by day
hackatime_legacy_events AS (
    SELECT
        DATE(activity_time) AS event_date,
        hackatime_first_email AS email,
        'hackatimeLegacyActivity' AS event,
        'hackatime_legacy' AS event_type,
        'hackatimeLegacy' AS source_system
    FROM {{ ref('hackatime_legacy_hourly_project_activity') }}
    WHERE hackatime_first_email IS NOT NULL
        AND activity_time IS NOT NULL
        AND hackatime_hours > 0
        AND DATE(activity_time) >= '2024-01-01'  -- Filter events older than Jan 1, 2024
        AND DATE(activity_time) <= CURRENT_DATE + INTERVAL '1 day'  -- Filter future events, allow 1 day for timezone differences
    GROUP BY DATE(activity_time), hackatime_first_email
)

-- Union all events
SELECT
    event_date::date AS event_date,
    LOWER(email) AS email,
    event,
    event_type,
    source_system
FROM loops_final

UNION ALL

SELECT
    event_date::date AS event_date,
    LOWER(email) AS email,
    event,
    event_type,
    source_system
FROM hackatime_events

UNION ALL

SELECT
    event_date::date AS event_date,
    LOWER(email) AS email,
    event,
    event_type,
    source_system
FROM hackatime_legacy_events

ORDER BY email, event_date DESC, event
