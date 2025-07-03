{{ config(materialized='table', schema='loops_analytics') }}

/* ---------------------------------------------------------------------------
   HACK CLUBBERS ONLY (subscribed + either user_group or source says so)
--------------------------------------------------------------------------- */
WITH base AS (
    SELECT *
    FROM   {{ source('loops', 'audience') }} a
    WHERE  a.subscribed
      AND (a.user_group = 'Hack Clubber')
),

/* ---------------------------------------------------------------------------
   1.  Explode each row into (column_name, event_date, event_camel) triples.
       We skip created_at & birthday; keep only strings that look like YYYY-MM-DD.
--------------------------------------------------------------------------- */
events AS (
    SELECT
        b.email,
        e.key                                                             AS column_name,
        (substring(e.value::text FROM '^\d{4}-\d{2}-\d{2}'))::date        AS event_date,
        lower(left(replace(initcap(replace(e.key,'_',' ')),' ',''),1)) ||
        substr(replace(initcap(replace(e.key,'_',' ')),' ',''),2)         AS event_camel
    FROM   base b,
           LATERAL jsonb_each_text(to_jsonb(b) - 'created_at' - 'birthday') AS e(key,value)
    WHERE  e.value ~ '^\d{4}-\d{2}-\d{2}'          -- keep date-shaped values only
),

/* ---------------------------------------------------------------------------
   2.  Earliest dated event per user  →  sign_up_at
--------------------------------------------------------------------------- */
first_event AS (
    SELECT  email, MIN(event_date) AS sign_up_at
    FROM    events
    GROUP   BY email
),

/* ---------------------------------------------------------------------------
   3.  All events that occurred **on** sign_up_at  →  sign_up_day_events
--------------------------------------------------------------------------- */
sign_up_day_events AS (
    SELECT  e.email,
            STRING_AGG(e.event_camel, ',' ORDER BY e.event_camel)
                    AS sign_up_day_events
    FROM    events e
    JOIN    first_event fe
      ON    fe.email = e.email
     AND    fe.sign_up_at = e.event_date
    GROUP   BY e.email
),

/* ---------------------------------------------------------------------------
   4.  Overview: newest-to-oldest lines "eventName eventDate" (E'\n'-separated)
--------------------------------------------------------------------------- */
events_overview AS (
    SELECT  email,
            STRING_AGG(
              event_camel || ' ' || event_date::text,
              E'\n'
              ORDER BY event_date DESC, event_camel)      AS events_overview
    FROM    events
    GROUP   BY email
)

/* ---------------------------------------------------------------------------
   FINAL RESULT
--------------------------------------------------------------------------- */
SELECT
    fe.email,
    fe.sign_up_at,
    sud.sign_up_day_events,
    evo.events_overview
FROM        first_event        fe
JOIN        sign_up_day_events sud USING (email)
JOIN        events_overview    evo USING (email)
ORDER BY    fe.email
