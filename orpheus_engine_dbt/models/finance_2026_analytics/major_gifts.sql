{{ config(
    schema='finance_2026_analytics',
    materialized='table'
) }}

SELECT
  internal_name AS donor,
  amount,
  COALESCE(
    CASE
      WHEN NULLIF(received_at, '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        THEN TO_DATE(received_at, 'MM/DD/YYYY')
      WHEN NULLIF(received_at, '') ~ '^\d{1,2}/\d{1,2}/\d{2}$'
        THEN TO_DATE(received_at, 'MM/DD/YY')
    END,
    CASE
      WHEN NULLIF(leadership_flagged_donation_at, '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        THEN TO_DATE(leadership_flagged_donation_at, 'MM/DD/YYYY')
      WHEN NULLIF(leadership_flagged_donation_at, '') ~ '^\d{1,2}/\d{1,2}/\d{2}$'
        THEN TO_DATE(leadership_flagged_donation_at, 'MM/DD/YY')
    END
  ) AS date,
  CASE
    WHEN received_at IS NOT NULL AND received_at != '' THEN 'Received'
    WHEN leadership_flagged_donation_at IS NOT NULL AND leadership_flagged_donation_at != '' THEN 'Awaiting Receipt'
    ELSE 'Unknown'
  END AS status,
  donor_origin AS country,
  _fivetran_synced AS source_synced_at
FROM {{ source('finance_2026', 'major_gifts') }}
ORDER BY date DESC
