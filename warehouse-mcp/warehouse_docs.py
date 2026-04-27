"""Client-facing documentation snippets for warehouse MCP tools."""

UNIFIED_ANALYTICS_EVENTS_GUIDANCE = """Use public_unified_analytics.events for signups, signup sources, and user lifecycle/event history questions. Treat the first event for a normalized email address as that user's Hack Club signup: rank rows by event_date ascending within lower(btrim(email)), keep row_number() = 1, and count those first rows. The first row's source_system, event_type, and event are the signup source fields. Do not use loops.audience.source to determine where users came from; it is not the signup source of truth. For subscribed-only signup metrics, filter to subscribed rows before ranking. Avoid unpivoting loops.audience for signup counts unless you specifically need raw Loops profile fields."""


UNIFIED_ANALYTICS_EVENTS_SQL_EXAMPLES = """Example total signups by day:
WITH first_events AS (
  SELECT *
  FROM (
    SELECT
      lower(btrim(email)) AS email,
      event_date,
      event,
      event_type,
      source_system,
      row_number() OVER (
        PARTITION BY lower(btrim(email))
        ORDER BY event_date ASC, source_system ASC, event_type ASC, event ASC
      ) AS rn
    FROM public_unified_analytics.events
    WHERE NULLIF(btrim(email), '') IS NOT NULL
  ) ranked
  WHERE rn = 1
)
SELECT event_date::date AS signup_date, count(*) AS signups
FROM first_events
GROUP BY 1
ORDER BY 1;

Example signup sources:
WITH first_events AS (
  SELECT *
  FROM (
    SELECT
      lower(btrim(email)) AS email,
      event_date,
      event,
      event_type,
      source_system,
      row_number() OVER (
        PARTITION BY lower(btrim(email))
        ORDER BY event_date ASC, source_system ASC, event_type ASC, event ASC
      ) AS rn
    FROM public_unified_analytics.events
    WHERE NULLIF(btrim(email), '') IS NOT NULL
  ) ranked
  WHERE rn = 1
)
SELECT source_system, event_type, event, count(*) AS signups
FROM first_events
GROUP BY 1, 2, 3
ORDER BY signups DESC;"""
