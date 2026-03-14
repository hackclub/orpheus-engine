{{ config(
    schema='finance_2026_analytics',
    materialized='table'
) }}

WITH fivetran_syncs AS (
  SELECT
    c.connection_name,
    MAX(l.time_stamp) AS last_sync_completed_at
  FROM {{ source('fivetran_log', 'log') }} l
  JOIN {{ source('fivetran_log', 'connection') }} c ON c.connection_id = l.connection_id
  WHERE c.connection_name LIKE 'finance_2026.%'
    AND l.message_event = 'sync_end'
  GROUP BY c.connection_name
)

SELECT
  REPLACE(connection_name, 'finance_2026.', '') AS source_table,
  NOW() AS last_materialized_at,
  last_sync_completed_at AS last_fivetran_synced_at
FROM fivetran_syncs
