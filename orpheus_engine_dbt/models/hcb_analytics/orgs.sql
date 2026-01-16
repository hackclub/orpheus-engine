{{ config(
    schema='hcb_analytics',
    materialized='table'
) }}

/*
    Organizations

    All HCB organizations with comprehensive metadata.
    Use is_hq flag to filter for Hack Club HQ organizations.

    HQ plan types:
    - Event::Plan::HackClubHQ - Core HQ accounts
    - Event::Plan::Internal - Internal operational accounts
    - Event::Plan::HackClubAffiliate - HQ programs, YSWS, affiliated projects

    Balance calculation matches HCB API exactly:
    - balance_cents = settled_balance + pending_outgoing + fronted_incoming
    - settled_balance: sum of canonical_transactions on MAIN LEDGER (subledger_id IS NULL)
    - pending_outgoing: sum of unsettled pending transactions (negative amounts, not settled, not declined)
    - fronted_incoming: for events with can_front_balance=true, uses sum_fronted_amount logic:
        * Groups fronted pending by hcb_code
        * Subtracts settled amounts for same hcb_code
        * Uses max(pending - settled, 0) for each hcb_code

    Note on card grants: Card grants are funded via self-disbursements which create
    +X and -X canonical transactions that net to zero. Grant spending becomes card
    transactions in canonical_transactions. Therefore, grants are NOT deducted from
    balance_cents - they're tracked separately in card_grants_* columns for analysis.

    Note: fee_balance_cents tracks HCB's collected fee revenue, NOT outstanding
    fees to deduct from the org balance.
*/

WITH plan_info AS (
    -- Get active plan for each event
    SELECT DISTINCT ON (event_id)
        event_id,
        type AS plan_type,
        CASE
            WHEN type = 'Event::Plan::HackClubHQ' THEN 'Core HQ'
            WHEN type = 'Event::Plan::Internal' THEN 'Internal'
            WHEN type = 'Event::Plan::HackClubAffiliate' THEN 'Affiliate'
            WHEN type = 'Event::Plan::SalaryAccount' THEN 'Salary'
            ELSE 'Standard'
        END AS plan_category,
        type IN (
            'Event::Plan::HackClubHQ',
            'Event::Plan::Internal',
            'Event::Plan::HackClubAffiliate'
        ) AS is_hq,
        created_at AS plan_created_at,
        inactive_at AS plan_inactive_at
    FROM {{ source('hcb', 'event_plans') }}
    WHERE aasm_state = 'active'
    ORDER BY event_id, created_at DESC
),

organizer_counts AS (
    -- Count active organizers per event
    SELECT
        event_id,
        COUNT(*) AS organizer_count,
        COUNT(*) FILTER (WHERE is_signee = true) AS signatory_count
    FROM {{ source('hcb', 'organizer_positions') }}
    WHERE deleted_at IS NULL
    GROUP BY event_id
),

transaction_stats AS (
    -- Aggregate transaction stats per event (settled transactions on MAIN LEDGER only)
    SELECT
        cem.event_id,
        COUNT(*) AS transaction_count,
        SUM(ct.amount_cents) AS settled_balance_cents,
        SUM(CASE WHEN ct.amount_cents > 0 THEN ct.amount_cents ELSE 0 END) AS total_inflow_cents,
        SUM(CASE WHEN ct.amount_cents < 0 THEN ct.amount_cents ELSE 0 END) AS total_outflow_cents,
        MIN(ct.date) AS first_transaction_date,
        MAX(ct.date) AS last_transaction_date
    FROM {{ source('hcb', 'canonical_event_mappings') }} cem
    JOIN {{ source('hcb', 'canonical_transactions') }} ct
        ON ct.id = cem.canonical_transaction_id
    WHERE cem.subledger_id IS NULL  -- Main ledger only
    GROUP BY cem.event_id
),

pending_outgoing AS (
    -- Unsettled pending transactions (only outgoing/negative amounts)
    -- Excludes settled and declined pending transactions
    -- Main ledger only (subledger_id IS NULL)
    SELECT
        cpem.event_id,
        SUM(cpt.amount_cents) AS pending_outgoing_cents
    FROM {{ source('hcb', 'canonical_pending_transactions') }} cpt
    JOIN {{ source('hcb', 'canonical_pending_event_mappings') }} cpem
        ON cpem.canonical_pending_transaction_id = cpt.id
    LEFT JOIN {{ source('hcb', 'canonical_pending_settled_mappings') }} cpsm
        ON cpsm.canonical_pending_transaction_id = cpt.id
    LEFT JOIN {{ source('hcb', 'canonical_pending_declined_mappings') }} cpdm
        ON cpdm.canonical_pending_transaction_id = cpt.id
    WHERE cpem.subledger_id IS NULL  -- Main ledger only
      AND cpt.amount_cents < 0       -- Outgoing only
      AND cpsm.id IS NULL            -- Not settled
      AND cpdm.id IS NULL            -- Not declined
    GROUP BY cpem.event_id
),

-- Fronted incoming: for events with can_front_balance=true
-- Uses sum_fronted_amount logic: max(pending - settled, 0) per hcb_code
fronted_pending_by_hcb_code AS (
    -- Group fronted pending transactions by event and hcb_code
    SELECT
        cpem.event_id,
        cpt.hcb_code,
        SUM(cpt.amount_cents) AS pending_sum
    FROM {{ source('hcb', 'canonical_pending_transactions') }} cpt
    JOIN {{ source('hcb', 'canonical_pending_event_mappings') }} cpem
        ON cpem.canonical_pending_transaction_id = cpt.id
    LEFT JOIN {{ source('hcb', 'canonical_pending_declined_mappings') }} cpdm
        ON cpdm.canonical_pending_transaction_id = cpt.id
    WHERE cpem.subledger_id IS NULL  -- Main ledger only
      AND cpt.amount_cents > 0       -- Incoming only
      AND cpt.fronted = true         -- Fronted only
      AND cpdm.id IS NULL            -- Not declined (note: NOT checking settled here)
    GROUP BY cpem.event_id, cpt.hcb_code
),

settled_by_hcb_code AS (
    -- Get settled amounts for the same hcb_codes that have fronted pending
    SELECT
        cem.event_id,
        ct.hcb_code,
        SUM(ct.amount_cents) AS settled_sum
    FROM {{ source('hcb', 'canonical_transactions') }} ct
    JOIN {{ source('hcb', 'canonical_event_mappings') }} cem
        ON cem.canonical_transaction_id = ct.id
    WHERE cem.subledger_id IS NULL
      AND ct.hcb_code IN (SELECT DISTINCT hcb_code FROM fronted_pending_by_hcb_code)
    GROUP BY cem.event_id, ct.hcb_code
),

fronted_incoming AS (
    -- Calculate fronted incoming: max(pending - settled, 0) per hcb_code, then sum
    SELECT
        fp.event_id,
        SUM(GREATEST(fp.pending_sum - COALESCE(sb.settled_sum, 0), 0)) AS fronted_incoming_cents
    FROM fronted_pending_by_hcb_code fp
    LEFT JOIN settled_by_hcb_code sb
        ON sb.event_id = fp.event_id AND sb.hcb_code = fp.hcb_code
    GROUP BY fp.event_id
),

fee_balance AS (
    -- Outstanding fees per event
    SELECT
        event_id,
        SUM(amount_cents_as_decimal)::bigint AS fee_balance_cents
    FROM {{ source('hcb', 'fees') }}
    WHERE reason IS NOT NULL  -- Active fees (not voided)
    GROUP BY event_id
),

card_grants_outstanding AS (
    -- Active card grants with deposited disbursements
    -- These represent funds reserved for user card spending
    -- Status: 0=pending invite, 1=active, 2=completed/expired
    -- Only count grants with deposited disbursements (funds actually moved)
    SELECT
        cg.event_id,
        SUM(cg.amount_cents) AS card_grants_cents,
        COUNT(*) AS card_grants_count,
        SUM(CASE WHEN cg.status = 1 THEN cg.amount_cents ELSE 0 END) AS active_grants_cents,
        COUNT(*) FILTER (WHERE cg.status = 1) AS active_grants_count,
        SUM(CASE WHEN cg.status = 2 THEN cg.amount_cents ELSE 0 END) AS completed_grants_cents,
        COUNT(*) FILTER (WHERE cg.status = 2) AS completed_grants_count
    FROM {{ source('hcb', 'card_grants') }} cg
    JOIN {{ source('hcb', 'disbursements') }} d ON d.id = cg.disbursement_id
    WHERE d.aasm_state = 'deposited'
    GROUP BY cg.event_id
),

card_stats AS (
    -- Count cards per event
    -- card_type enum: 0=virtual, 1=physical
    SELECT
        event_id,
        COUNT(*) AS total_cards,
        COUNT(*) FILTER (WHERE stripe_status = 'active') AS active_cards,
        COUNT(*) FILTER (WHERE card_type = 1) AS physical_cards,
        COUNT(*) FILTER (WHERE card_type = 0) AS virtual_cards
    FROM {{ source('hcb', 'stripe_cards') }}
    GROUP BY event_id
),

disbursement_stats AS (
    -- Disbursements sent and received
    SELECT
        e.id AS event_id,
        SUM(CASE WHEN d.event_id = e.id THEN d.amount ELSE 0 END) AS disbursements_received_cents,
        SUM(CASE WHEN d.source_event_id = e.id THEN d.amount ELSE 0 END) AS disbursements_sent_cents,
        COUNT(*) FILTER (WHERE d.event_id = e.id) AS disbursements_received_count,
        COUNT(*) FILTER (WHERE d.source_event_id = e.id) AS disbursements_sent_count
    FROM {{ source('hcb', 'events') }} e
    LEFT JOIN {{ source('hcb', 'disbursements') }} d
        ON d.event_id = e.id OR d.source_event_id = e.id
    WHERE d.aasm_state = 'deposited'
    GROUP BY e.id
),

point_of_contact AS (
    -- Get point of contact info
    SELECT
        id AS user_id,
        email AS poc_email,
        full_name AS poc_name
    FROM {{ source('hcb', 'users') }}
)

SELECT
    -- Event identifiers
    e.id AS event_id,
    e.slug,
    e.name,
    e.short_name,

    -- HQ classification
    COALESCE(pi.is_hq, false) AS is_hq,
    pi.plan_type,
    pi.plan_category,

    -- Status flags
    e.aasm_state,
    e.deleted_at IS NOT NULL AS is_deleted,
    e.financially_frozen,
    e.demo_mode,
    e.is_public,
    e.is_indexable,

    -- Risk & fees
    e.risk_level,
    e.fee_waiver_eligible,
    e.fee_waiver_applied,
    e.can_front_balance,

    -- Organizational info
    e.description,
    e.website,
    e.country,
    e.postal_code,

    -- Point of contact
    e.point_of_contact_id,
    poc.poc_email,
    poc.poc_name,

    -- Parent relationship (for sub-accounts)
    e.parent_id,
    parent.name AS parent_name,
    parent.slug AS parent_slug,

    -- Organizer stats
    COALESCE(oc.organizer_count, 0) AS organizer_count,
    COALESCE(oc.signatory_count, 0) AS signatory_count,

    -- Financial stats (balance matches HCB API: settled + pending_outgoing)
    COALESCE(ts.transaction_count, 0) AS transaction_count,
    COALESCE(ts.settled_balance_cents, 0) AS settled_balance_cents,
    COALESCE(po.pending_outgoing_cents, 0) AS pending_outgoing_cents,
    COALESCE(fb.fee_balance_cents, 0) AS fee_revenue_cents,  -- HCB's collected fee revenue
    COALESCE(fi.fronted_incoming_cents, 0) AS fronted_incoming_cents,
    COALESCE(ts.settled_balance_cents, 0) + COALESCE(po.pending_outgoing_cents, 0) + CASE WHEN e.can_front_balance THEN COALESCE(fi.fronted_incoming_cents, 0) ELSE 0 END AS balance_cents,
    ROUND((COALESCE(ts.settled_balance_cents, 0) + COALESCE(po.pending_outgoing_cents, 0) + CASE WHEN e.can_front_balance THEN COALESCE(fi.fronted_incoming_cents, 0) ELSE 0 END) / 100.0, 2) AS balance_dollars,
    COALESCE(ts.total_inflow_cents, 0) AS total_inflow_cents,
    COALESCE(ts.total_outflow_cents, 0) AS total_outflow_cents,
    ts.first_transaction_date,
    ts.last_transaction_date,

    -- Card stats
    COALESCE(cs.total_cards, 0) AS total_cards,
    COALESCE(cs.active_cards, 0) AS active_cards,
    COALESCE(cs.physical_cards, 0) AS physical_cards,
    COALESCE(cs.virtual_cards, 0) AS virtual_cards,

    -- Card grant stats (funds issued to users for card spending)
    COALESCE(cgo.card_grants_cents, 0) AS card_grants_total_cents,
    COALESCE(cgo.card_grants_count, 0) AS card_grants_total_count,
    COALESCE(cgo.active_grants_cents, 0) AS card_grants_active_cents,
    COALESCE(cgo.active_grants_count, 0) AS card_grants_active_count,
    COALESCE(cgo.completed_grants_cents, 0) AS card_grants_completed_cents,
    COALESCE(cgo.completed_grants_count, 0) AS card_grants_completed_count,

    -- Disbursement stats
    COALESCE(ds.disbursements_received_cents, 0) AS disbursements_received_cents,
    COALESCE(ds.disbursements_sent_cents, 0) AS disbursements_sent_cents,
    COALESCE(ds.disbursements_received_count, 0) AS disbursements_received_count,
    COALESCE(ds.disbursements_sent_count, 0) AS disbursements_sent_count,

    -- Donation settings
    e.donation_page_enabled,
    e.donation_tiers_enabled,

    -- Discord integration
    e.discord_guild_id,
    e.discord_channel_id,

    -- Timestamps
    e.created_at,
    e.activated_at,
    e.updated_at,
    e.deleted_at,
    pi.plan_created_at,

    -- Treasury/operational account flags (exclude from spending analysis)
    e.slug IN ('hcb-sweeps', 'noevent', 'reimbursement-clearinghouse', 'clearing') AS is_treasury_account

FROM {{ source('hcb', 'events') }} e
LEFT JOIN plan_info pi ON pi.event_id = e.id
LEFT JOIN organizer_counts oc ON oc.event_id = e.id
LEFT JOIN transaction_stats ts ON ts.event_id = e.id
LEFT JOIN pending_outgoing po ON po.event_id = e.id
LEFT JOIN fee_balance fb ON fb.event_id = e.id
LEFT JOIN fronted_incoming fi ON fi.event_id = e.id
LEFT JOIN card_stats cs ON cs.event_id = e.id
LEFT JOIN card_grants_outstanding cgo ON cgo.event_id = e.id
LEFT JOIN disbursement_stats ds ON ds.event_id = e.id
LEFT JOIN point_of_contact poc ON poc.user_id = e.point_of_contact_id
LEFT JOIN {{ source('hcb', 'events') }} parent ON parent.id = e.parent_id

WHERE e.deleted_at IS NULL  -- Exclude soft-deleted events

ORDER BY balance_cents DESC NULLS LAST
