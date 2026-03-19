{{ config(
    schema='spring_2026_analytics',
    materialized='table'
) }}

-- Metadata for Spring 2026 Analytics
-- Tracks: program start dates, last dbt materialization, and last data sync per program

WITH program_info AS (
  SELECT * FROM (VALUES
    ('blueprint',  DATE '2025-10-17'),
    ('flavortown', DATE '2025-12-24'),
    ('sleepover',  DATE '2026-01-20'),
    ('horizons',   DATE '2026-02-20'),
    ('stasis',     DATE '2026-03-04')
  ) AS t(program_name, program_start_date)
),

-- Last sync timestamps per program (max updated_at from representative tables)
stasis_sync AS (
  SELECT MAX("updatedAt") AS last_synced_at FROM {{ source('stasis', 'user') }}
),
flavortown_sync AS (
  SELECT MAX(updated_at) AS last_synced_at FROM {{ source('flavortown', 'users') }}
),
horizons_sync AS (
  SELECT MAX(updated_at) AS last_synced_at FROM {{ source('horizons', 'users') }}
),
blueprint_sync AS (
  SELECT MAX(updated_at) AS last_synced_at FROM {{ source('blueprint', 'users') }}
),
sleepover_sync AS (
  SELECT MAX(inserted_at) AS last_synced_at FROM {{ source('airtable_sleepover', '_dlt_loads') }}
),

sync_times AS (
  SELECT 'stasis' AS program_name, last_synced_at FROM stasis_sync
  UNION ALL
  SELECT 'flavortown', last_synced_at FROM flavortown_sync
  UNION ALL
  SELECT 'horizons', last_synced_at FROM horizons_sync
  UNION ALL
  SELECT 'blueprint', last_synced_at FROM blueprint_sync
  UNION ALL
  SELECT 'sleepover', last_synced_at FROM sleepover_sync
)

SELECT
  pi.program_name,
  pi.program_start_date,
  NOW() AS last_materialized_at,
  st.last_synced_at AS last_source_synced_at,
  'equal_split' AS dedup_method,
  'When a session is claimed by multiple programs (via matching hackatime project alias or matching code URL), raw_hours are divided equally among them. credited_hours = raw_hours / split_factor.' AS dedup_description
FROM program_info pi
LEFT JOIN sync_times st ON st.program_name = pi.program_name
ORDER BY pi.program_start_date
