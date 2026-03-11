{{ config(
    schema='finance_2026_analytics',
    materialized='table'
) }}

WITH fee_revenue AS (
  SELECT
    DATE_TRUNC('month', date) AS month,
    SUM(amount_cents) / 100.0 AS hcb_fee_revenue
  FROM {{ source('hcb', 'canonical_transactions') }}
  WHERE hcb_code ILIKE 'HCB-702%'
    AND (amount_cents > 0 OR date > '2024-02-26')
    AND EXTRACT(YEAR FROM date) = 2026
  GROUP BY DATE_TRUNC('month', date)
),

major_gifts_received AS (
  SELECT
    DATE_TRUNC('month', TO_DATE(received_at, 'MM/DD/YYYY')) AS month,
    SUM(amount) AS major_gift_total
  FROM {{ source('finance_2026', 'major_gifts') }}
  WHERE received_at IS NOT NULL
  GROUP BY DATE_TRUNC('month', TO_DATE(received_at, 'MM/DD/YYYY'))
),

revenue_awaiting_receipt AS (
  SELECT
    DATE_TRUNC('month', TO_DATE(leadership_flagged_donation_at, 'MM/DD/YYYY')) AS month,
    SUM(amount) AS awaiting_total
  FROM {{ source('finance_2026', 'major_gifts') }}
  WHERE received_at IS NULL
    AND leadership_flagged_donation_at IS NOT NULL
  GROUP BY DATE_TRUNC('month', TO_DATE(leadership_flagged_donation_at, 'MM/DD/YYYY'))
),

monthly AS (
  SELECT
    TO_DATE(month, 'Mon YYYY') AS month,
    expenses AS total_expenses,
    revenue_other,
    revenue_hq_interest
  FROM {{ source('finance_2026', 'monthly_finances') }}
),

hcb_exp AS (
  SELECT
    TO_DATE(month, 'Mon YYYY') AS month,
    hcb_expenses,
    hcb_revenue_from_bank_interest AS hcb_interest,
    hcb_revenue_from_grants AS hcb_grants
  FROM {{ source('finance_2026', 'hcb_expense_reporting') }}
  WHERE EXTRACT(YEAR FROM TO_DATE(month, 'Mon YYYY')) = 2026
)

SELECT
  TO_CHAR(COALESCE(m.month, f.month, g.month, h.month), 'Mon YYYY') AS month,

  COALESCE(f.hcb_fee_revenue, 0)
    + COALESCE(g.major_gift_total, 0)
    + COALESCE(m.revenue_other, 0)
    + COALESCE(m.revenue_hq_interest, 0)
    + COALESCE(h.hcb_interest, 0)
    + COALESCE(h.hcb_grants, 0)
    AS revenue_total,

  COALESCE(f.hcb_fee_revenue, 0)
    + COALESCE(h.hcb_interest, 0)
    + COALESCE(h.hcb_grants, 0)
    AS revenue_hcb,

  COALESCE(g.major_gift_total, 0) AS revenue_major_gifts,
  COALESCE(m.revenue_other, 0) AS revenue_other,
  COALESCE(m.revenue_hq_interest, 0) + COALESCE(h.hcb_interest, 0) AS revenue_interest,
  COALESCE(a.awaiting_total, 0) AS revenue_awaiting_receipt,

  COALESCE(h.hcb_expenses, 0) AS expenses_hcb,
  COALESCE(m.total_expenses, 0) - COALESCE(h.hcb_expenses, 0) AS expenses_everything_else,
  COALESCE(m.total_expenses, 0) AS expenses_total,

  COALESCE(f.hcb_fee_revenue, 0)
    + COALESCE(g.major_gift_total, 0)
    + COALESCE(m.revenue_other, 0)
    + COALESCE(m.revenue_hq_interest, 0)
    + COALESCE(h.hcb_interest, 0)
    + COALESCE(h.hcb_grants, 0)
    - COALESCE(m.total_expenses, 0)
    AS net_revenue

FROM monthly m
FULL OUTER JOIN fee_revenue f ON m.month = f.month
FULL OUTER JOIN major_gifts_received g ON COALESCE(m.month, f.month) = g.month
FULL OUTER JOIN hcb_exp h ON COALESCE(m.month, f.month, g.month) = h.month
FULL OUTER JOIN revenue_awaiting_receipt a ON COALESCE(m.month, f.month, g.month, h.month) = a.month

WHERE EXTRACT(YEAR FROM COALESCE(m.month, f.month, g.month, h.month)) = 2026
ORDER BY COALESCE(m.month, f.month, g.month, h.month)
