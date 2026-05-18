{{ config(
    schema='unified_analytics',
    alias='hack_clubbers',
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_unified_hack_clubbers_email ON {{ this }} (email);",
        "CREATE INDEX IF NOT EXISTS idx_unified_hack_clubbers_first_meaningful_event_at ON {{ this }} (first_meaningful_event_at);"
    ]
) }}

WITH events_normalized AS (
    SELECT
        e.event_date,
        lower(btrim(e.email)) AS email,
        CASE
            WHEN POSITION('@' IN lower(btrim(e.email))) > 0 THEN
                split_part(split_part(lower(btrim(e.email)), '@', 1), '+', 1)
                || '@' ||
                split_part(lower(btrim(e.email)), '@', 2)
            ELSE split_part(lower(btrim(e.email)), '+', 1)
        END AS email_join_key,
        e.event,
        e.event_type,
        e.source_system
    FROM {{ ref('events') }} AS e
    WHERE NULLIF(btrim(e.email), '') IS NOT NULL
),

loops_profiles_raw AS (
    SELECT
        lower(btrim(a.email)) AS email,
        CASE
            WHEN POSITION('@' IN lower(btrim(a.email))) > 0 THEN
                split_part(split_part(lower(btrim(a.email)), '@', 1), '+', 1)
                || '@' ||
                split_part(lower(btrim(a.email)), '@', 2)
            ELSE split_part(lower(btrim(a.email)), '+', 1)
        END AS email_join_key,
        NULLIF(btrim(a.first_name), '') AS first_name,
        NULLIF(btrim(a.last_name), '') AS last_name,
        a.subscribed AS loops_subscribed,
        NULLIF(btrim(a.calculated_gender_best_known), '') AS gender_best_known,
        a.calculated_geocoded_latitude AS latitude,
        a.calculated_geocoded_longitude AS longitude,
        a.user_group,
        a.updated_at
    FROM {{ source('loops', 'audience') }} AS a
    WHERE NULLIF(btrim(a.email), '') IS NOT NULL
),

base_users AS (
    SELECT DISTINCT email, email_join_key
    FROM events_normalized

    UNION

    SELECT DISTINCT email, email_join_key
    FROM loops_profiles_raw
    WHERE user_group = 'Hack Clubber'
),

loops_subscription AS (
    SELECT
        email_join_key,
        bool_or(loops_subscribed) AS loops_subscribed
    FROM loops_profiles_raw
    GROUP BY email_join_key
),

loops_profiles_ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY email_join_key
            ORDER BY
                (user_group = 'Hack Clubber') DESC,
                updated_at DESC NULLS LAST,
                email ASC
        ) AS rn
    FROM loops_profiles_raw
),

loops_profiles AS (
    SELECT
        email_join_key,
        first_name,
        last_name,
        gender_best_known,
        latitude,
        longitude
    FROM loops_profiles_ranked
    WHERE rn = 1
),

project_metrics AS (
    SELECT
        CASE
            WHEN POSITION('@' IN lower(btrim(ap.email_trimmed_lowercased))) > 0 THEN
                split_part(split_part(lower(btrim(ap.email_trimmed_lowercased)), '@', 1), '+', 1)
                || '@' ||
                split_part(lower(btrim(ap.email_trimmed_lowercased)), '@', 2)
            ELSE split_part(lower(btrim(ap.email_trimmed_lowercased)), '+', 1)
        END AS email_join_key,
        SUM(
            COALESCE(
                ap.ysws_weighted_project_contribution_per_author,
                ap.ysws_weighted_project_contribution,
                0
            )
        )::double precision AS weighted_projects_count
    FROM {{ source('unified_ysws', 'approved_projects') }} AS ap
    WHERE NULLIF(btrim(ap.email_trimmed_lowercased), '') IS NOT NULL
      AND ap.approved_at IS NOT NULL
    GROUP BY 1
),

raw_first_events AS (
    SELECT *
    FROM (
        SELECT
            e.*,
            row_number() OVER (
                PARTITION BY e.email
                ORDER BY e.event_date ASC, e.source_system ASC, e.event_type ASC, e.event ASC
            ) AS rn
        FROM events_normalized AS e
    ) AS ranked
    WHERE rn = 1
),

raw_first_events_with_window AS (
    SELECT
        *,
        CASE
            WHEN source_system = 'hackClubAuth' THEN INTERVAL '30 days'
            ELSE INTERVAL '24 hours'
        END AS attribution_window,
        CASE
            WHEN source_system = 'hackClubAuth' THEN 'auth_30d_window'
            ELSE '24h_conflict_window'
        END AS attribution_window_label
    FROM raw_first_events
),

first_event_candidates AS (
    SELECT
        r.email,
        r.event_date AS raw_first_event_at,
        r.event AS raw_first_event,
        r.attribution_window_label,
        e.event_date,
        e.event,
        e.event_type,
        e.source_system,
        event_override.source_system_priority AS event_override_priority,
        source_default.source_system_priority AS source_default_priority,
        COALESCE(
            event_override.source_system_priority,
            source_default.source_system_priority,
            100
        ) AS event_priority,
        COALESCE(
            event_override.first_meaningful_event_justification,
            source_default.first_meaningful_event_justification,
            'preferred_unprioritized_source_system'
        ) AS priority_justification
    FROM raw_first_events_with_window AS r
    JOIN events_normalized AS e
      ON e.email = r.email
     AND e.event_date >= r.event_date
     AND e.event_date < r.event_date + r.attribution_window
    LEFT JOIN {{ ref('event_priorities') }} AS event_override
      ON event_override.priority_scope = 'event_override'
     AND event_override.source_system = e.source_system
     AND event_override.event = e.event
    LEFT JOIN {{ ref('event_priorities') }} AS source_default
      ON source_default.priority_scope = 'source_system_default'
     AND source_default.source_system = e.source_system
     AND source_default.event IS NULL
),

first_meaningful_events AS (
    SELECT *
    FROM (
        SELECT
            c.*,
            row_number() OVER (
                PARTITION BY c.email
                ORDER BY
                    c.event_priority ASC,
                    c.event_date ASC,
                    c.source_system ASC,
                    c.event_type ASC,
                    c.event ASC
            ) AS rn
        FROM first_event_candidates AS c
    ) AS ranked
    WHERE rn = 1
),

first_meaningful_with_justification AS (
    SELECT
        f.email,
        f.event AS first_meaningful_event,
        f.event_date AS first_meaningful_event_at,
        CASE
            WHEN f.event = f.raw_first_event
              AND f.event_date = f.raw_first_event_at
                THEN 'raw_first_event'
            ELSE f.priority_justification || '_' || f.attribution_window_label
        END AS first_meaningful_event_justification
    FROM first_meaningful_events AS f
)

SELECT
    lp.first_name,
    lp.last_name,
    u.email,
    ls.loops_subscribed,
    lp.gender_best_known,
    f.first_meaningful_event,
    f.first_meaningful_event_at,
    COALESCE(f.first_meaningful_event_justification, 'no_observed_event') AS first_meaningful_event_justification,
    lp.latitude,
    lp.longitude,
    COALESCE(pm.weighted_projects_count, 0::double precision) AS weighted_projects_count
FROM base_users AS u
LEFT JOIN loops_profiles AS lp
  ON lp.email_join_key = u.email_join_key
LEFT JOIN loops_subscription AS ls
  ON ls.email_join_key = u.email_join_key
LEFT JOIN first_meaningful_with_justification AS f
  ON f.email = u.email
LEFT JOIN project_metrics AS pm
  ON pm.email_join_key = u.email_join_key
ORDER BY u.email
