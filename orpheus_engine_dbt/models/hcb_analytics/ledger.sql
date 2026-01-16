{{ config(
    schema='hcb_analytics',
    materialized='table'
) }}

/*
    Ledger

    All HCB financial transactions with rich metadata.
    Use is_hq flag to filter for Hack Club HQ transactions.

    HCB Code patterns:
    - HCB-500-{id} - Disbursements (internal transfers)
    - HCB-200-{id} - ACH transfers
    - HCB-100-{id} - Donations
    - HCB-600-{id} - Stripe card transactions
    - HCB-300-{id} - Checks
    - HCB-000-{id} - Raw/other transactions
    - GRANT-{id} - Card grants (funds issued to user cards)

    Card grants are shown as negative amounts (outflow) representing funds
    reserved for user card spending. Grant status:
    - 0 = pending (invite sent, not activated)
    - 1 = active (funds available on card)
    - 2 = completed/expired
*/

WITH plan_info AS (
    -- Get active plan for each event to determine is_hq
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
        ) AS is_hq
    FROM {{ source('hcb', 'event_plans') }}
    WHERE aasm_state = 'active'
    ORDER BY event_id, created_at DESC
),

-- Get all transactions
all_transactions AS (
    SELECT
        ct.id AS transaction_id,
        ct.date AS transaction_date,
        ct.amount_cents,
        ct.memo,
        ct.friendly_memo,
        ct.custom_memo,
        ct.hcb_code,
        ct.transaction_source_type,
        ct.transaction_source_id,
        ct.created_at AS transaction_created_at,
        ct.updated_at AS transaction_updated_at,
        cem.event_id,
        cem.user_id AS transacting_user_id,
        cem.subledger_id
    FROM {{ source('hcb', 'canonical_transactions') }} ct
    JOIN {{ source('hcb', 'canonical_event_mappings') }} cem
        ON cem.canonical_transaction_id = ct.id
),

-- Parse HCB code to get transaction type
parsed_codes AS (
    SELECT
        *,
        CASE
            WHEN hcb_code LIKE 'HCB-500-%' THEN 'disbursement'
            WHEN hcb_code LIKE 'HCB-200-%' THEN 'ach_transfer'
            WHEN hcb_code LIKE 'HCB-100-%' THEN 'donation'
            WHEN hcb_code LIKE 'HCB-600-%' THEN 'card_transaction'
            WHEN hcb_code LIKE 'HCB-300-%' THEN 'check'
            WHEN hcb_code LIKE 'HCB-000-%' THEN 'raw_transaction'
            ELSE 'other'
        END AS transaction_type,
        -- Extract ID from HCB code for joining
        CASE
            WHEN hcb_code ~ 'HCB-\d{3}-\d+' THEN
                CAST(SPLIT_PART(hcb_code, '-', 3) AS BIGINT)
            ELSE NULL
        END AS hcb_code_id
    FROM all_transactions
),

-- Disbursement metadata
disbursement_meta AS (
    SELECT
        d.id,
        d.name AS disbursement_name,
        d.aasm_state AS disbursement_state,
        d.source_event_id,
        src.name AS source_org_name,
        src.slug AS source_org_slug,
        d.event_id AS dest_org_id,
        dest.name AS dest_org_name,
        dest.slug AS dest_org_slug,
        d.requested_by_id,
        req.full_name AS requested_by_name,
        d.fulfilled_by_id,
        ful.full_name AS fulfilled_by_name,
        d.scheduled_on
    FROM {{ source('hcb', 'disbursements') }} d
    LEFT JOIN {{ source('hcb', 'events') }} src ON src.id = d.source_event_id
    LEFT JOIN {{ source('hcb', 'events') }} dest ON dest.id = d.event_id
    LEFT JOIN {{ source('hcb', 'users') }} req ON req.id = d.requested_by_id
    LEFT JOIN {{ source('hcb', 'users') }} ful ON ful.id = d.fulfilled_by_id
),

-- ACH transfer metadata
ach_meta AS (
    SELECT
        id,
        recipient_name AS ach_recipient_name,
        recipient_email AS ach_recipient_email,
        payment_for AS ach_payment_for,
        aasm_state AS ach_state
    FROM {{ source('hcb', 'ach_transfers') }}
),

-- Donation metadata
donation_meta AS (
    SELECT
        id,
        name AS donor_name,
        email AS donor_email,
        message AS donation_message,
        anonymous AS is_anonymous_donation,
        aasm_state AS donation_state
    FROM {{ source('hcb', 'donations') }}
),

-- Wire metadata
wire_meta AS (
    SELECT
        id,
        recipient_name AS wire_recipient_name,
        memo AS wire_memo,
        aasm_state AS wire_state
    FROM {{ source('hcb', 'wires') }}
),

-- Check metadata (both legacy and Increase)
check_meta AS (
    SELECT
        id,
        recipient_name AS check_recipient_name,
        memo AS check_memo,
        'increase' AS check_source
    FROM {{ source('hcb', 'increase_checks') }}
    UNION ALL
    SELECT
        id,
        NULL AS check_recipient_name,  -- Legacy checks don't have recipient_name
        memo AS check_memo,
        'legacy' AS check_source
    FROM {{ source('hcb', 'checks') }}
),

-- Org info for the transaction's org
org_info AS (
    SELECT
        e.id AS org_id,
        e.name AS org_name,
        e.slug AS org_slug,
        e.slug IN ('hcb-sweeps', 'noevent', 'reimbursement-clearinghouse', 'clearing') AS is_treasury_account
    FROM {{ source('hcb', 'events') }} e
),

-- User info for transacting user
user_info AS (
    SELECT
        id AS user_id,
        email AS transacting_user_email,
        full_name AS transacting_user_name
    FROM {{ source('hcb', 'users') }}
),

-- Detect internal transfers (same hcb_code appears in multiple events)
internal_transfers AS (
    SELECT hcb_code
    FROM all_transactions
    GROUP BY hcb_code
    HAVING COUNT(DISTINCT event_id) > 1
),

-- Card grants as ledger entries
card_grant_entries AS (
    SELECT
        cg.id AS grant_id,
        'GRANT-' || cg.id::text AS hcb_code,
        'card_grant' AS transaction_type,
        -cg.amount_cents AS amount_cents,  -- Negative = outflow from org
        cg.created_at::date AS transaction_date,
        cg.created_at AS transaction_created_at,
        'Grant to ' || COALESCE(u.full_name, SPLIT_PART(cg.email, '@', 1)) AS display_memo,
        cg.purpose AS raw_memo,
        NULL::text AS friendly_memo,
        NULL::text AS custom_memo,
        cg.event_id,
        cg.sent_by_id AS transacting_user_id,
        cg.subledger_id,
        -- Grant-specific fields
        cg.status AS grant_status,
        CASE cg.status
            WHEN 0 THEN 'pending'
            WHEN 1 THEN 'active'
            WHEN 2 THEN 'completed'
            ELSE 'unknown'
        END AS grant_status_name,
        cg.user_id AS grant_recipient_id,
        cg.email AS grant_recipient_email,
        u.full_name AS grant_recipient_name,
        cg.stripe_card_id,
        cg.disbursement_id,
        d.aasm_state AS disbursement_state
    FROM {{ source('hcb', 'card_grants') }} cg
    LEFT JOIN {{ source('hcb', 'users') }} u ON u.id = cg.user_id
    LEFT JOIN {{ source('hcb', 'disbursements') }} d ON d.id = cg.disbursement_id
    WHERE d.aasm_state = 'deposited'  -- Only include grants with settled disbursements
),

-- Main ledger from canonical transactions
canonical_ledger AS (
SELECT
    -- Transaction identifiers
    pc.transaction_id,
    pc.hcb_code,
    pc.transaction_type,

    -- Amounts
    pc.amount_cents,
    ROUND(pc.amount_cents / 100.0, 2) AS amount_dollars,
    CASE WHEN pc.amount_cents > 0 THEN 'inflow' ELSE 'outflow' END AS flow_direction,

    -- Dates
    pc.transaction_date,
    pc.transaction_created_at,

    -- Memos (use custom > friendly > raw)
    COALESCE(pc.custom_memo, pc.friendly_memo, pc.memo) AS display_memo,
    pc.memo AS raw_memo,
    pc.friendly_memo,
    pc.custom_memo,

    -- Org info
    pc.event_id AS org_id,
    oi.org_name,
    oi.org_slug,

    -- HQ classification
    COALESCE(pi.is_hq, false) AS is_hq,
    pi.plan_type,
    pi.plan_category,
    oi.is_treasury_account,

    -- User who made the transaction
    pc.transacting_user_id,
    ui.transacting_user_email,
    ui.transacting_user_name,

    -- Internal transfer flag
    it.hcb_code IS NOT NULL AS is_internal_transfer,

    -- Transaction source details
    pc.transaction_source_type,
    pc.transaction_source_id,

    -- Disbursement details (HCB-500)
    dm.disbursement_name,
    dm.disbursement_state,
    dm.source_org_name,
    dm.source_org_slug,
    dm.dest_org_name,
    dm.dest_org_slug,
    dm.requested_by_name,
    dm.fulfilled_by_name,

    -- ACH details (HCB-200)
    am.ach_recipient_name,
    am.ach_recipient_email,
    am.ach_payment_for,
    am.ach_state,

    -- Donation details (HCB-100)
    dnm.donor_name,
    dnm.donor_email,
    dnm.donation_message,
    dnm.is_anonymous_donation,
    dnm.donation_state,

    -- Wire details
    wm.wire_recipient_name,
    wm.wire_memo,
    wm.wire_state,

    -- Check details (HCB-300)
    cm.check_recipient_name,
    cm.check_memo,
    cm.check_source,

    -- Consolidated vendor/counterparty name
    COALESCE(
        -- For disbursements, show source or dest based on flow
        CASE
            WHEN pc.transaction_type = 'disbursement' AND pc.amount_cents > 0 THEN dm.source_org_name
            WHEN pc.transaction_type = 'disbursement' AND pc.amount_cents < 0 THEN dm.dest_org_name
            ELSE NULL
        END,
        am.ach_recipient_name,
        CASE WHEN dnm.is_anonymous_donation THEN 'Anonymous Donor' ELSE dnm.donor_name END,
        wm.wire_recipient_name,
        cm.check_recipient_name,
        pc.friendly_memo
    ) AS counterparty_name,

    -- Subledger (budget category)
    pc.subledger_id

FROM parsed_codes pc
LEFT JOIN org_info oi ON oi.org_id = pc.event_id
LEFT JOIN plan_info pi ON pi.event_id = pc.event_id
LEFT JOIN user_info ui ON ui.user_id = pc.transacting_user_id
LEFT JOIN internal_transfers it ON it.hcb_code = pc.hcb_code
-- Join vendor tables based on transaction type and hcb_code_id
LEFT JOIN disbursement_meta dm
    ON pc.transaction_type = 'disbursement' AND dm.id = pc.hcb_code_id
LEFT JOIN ach_meta am
    ON pc.transaction_type = 'ach_transfer' AND am.id = pc.hcb_code_id
LEFT JOIN donation_meta dnm
    ON pc.transaction_type = 'donation' AND dnm.id = pc.hcb_code_id
LEFT JOIN wire_meta wm
    ON pc.transaction_source_type = 'Wire' AND wm.id = pc.transaction_source_id
LEFT JOIN check_meta cm
    ON pc.transaction_type = 'check' AND cm.id = pc.hcb_code_id
)

-- Combine canonical transactions with card grants
SELECT
    transaction_id,
    hcb_code,
    transaction_type,
    amount_cents,
    amount_dollars,
    flow_direction,
    transaction_date,
    transaction_created_at,
    display_memo,
    raw_memo,
    friendly_memo,
    custom_memo,
    org_id,
    org_name,
    org_slug,
    is_hq,
    plan_type,
    plan_category,
    is_treasury_account,
    transacting_user_id,
    transacting_user_email,
    transacting_user_name,
    is_internal_transfer,
    transaction_source_type,
    transaction_source_id,
    disbursement_name,
    disbursement_state,
    source_org_name,
    source_org_slug,
    dest_org_name,
    dest_org_slug,
    requested_by_name,
    fulfilled_by_name,
    ach_recipient_name,
    ach_recipient_email,
    ach_payment_for,
    ach_state,
    donor_name,
    donor_email,
    donation_message,
    is_anonymous_donation,
    donation_state,
    wire_recipient_name,
    wire_memo,
    wire_state,
    check_recipient_name,
    check_memo,
    check_source,
    counterparty_name,
    subledger_id,
    -- Card grant fields (NULL for canonical transactions)
    NULL::integer AS grant_status,
    NULL::text AS grant_status_name,
    NULL::bigint AS grant_recipient_id,
    NULL::text AS grant_recipient_email,
    NULL::text AS grant_recipient_name,
    NULL::bigint AS stripe_card_id
FROM canonical_ledger

UNION ALL

-- Card grants as ledger entries
SELECT
    cge.grant_id AS transaction_id,
    cge.hcb_code,
    cge.transaction_type,
    cge.amount_cents,
    ROUND(cge.amount_cents / 100.0, 2) AS amount_dollars,
    'outflow' AS flow_direction,
    cge.transaction_date,
    cge.transaction_created_at,
    cge.display_memo,
    cge.raw_memo,
    cge.friendly_memo,
    cge.custom_memo,
    cge.event_id AS org_id,
    oi.org_name,
    oi.org_slug,
    COALESCE(pi.is_hq, false) AS is_hq,
    pi.plan_type,
    pi.plan_category,
    oi.is_treasury_account,
    cge.transacting_user_id,
    su.transacting_user_email,
    su.transacting_user_name,
    false AS is_internal_transfer,
    'CardGrant' AS transaction_source_type,
    cge.grant_id AS transaction_source_id,
    NULL AS disbursement_name,
    cge.disbursement_state,
    NULL AS source_org_name,
    NULL AS source_org_slug,
    NULL AS dest_org_name,
    NULL AS dest_org_slug,
    NULL AS requested_by_name,
    NULL AS fulfilled_by_name,
    NULL AS ach_recipient_name,
    NULL AS ach_recipient_email,
    NULL AS ach_payment_for,
    NULL AS ach_state,
    NULL AS donor_name,
    NULL AS donor_email,
    NULL AS donation_message,
    NULL::boolean AS is_anonymous_donation,
    NULL AS donation_state,
    NULL AS wire_recipient_name,
    NULL AS wire_memo,
    NULL AS wire_state,
    NULL AS check_recipient_name,
    NULL AS check_memo,
    NULL AS check_source,
    cge.grant_recipient_name AS counterparty_name,
    cge.subledger_id,
    -- Card grant specific fields
    cge.grant_status,
    cge.grant_status_name,
    cge.grant_recipient_id,
    cge.grant_recipient_email,
    cge.grant_recipient_name,
    cge.stripe_card_id
FROM card_grant_entries cge
LEFT JOIN org_info oi ON oi.org_id = cge.event_id
LEFT JOIN plan_info pi ON pi.event_id = cge.event_id
LEFT JOIN user_info su ON su.user_id = cge.transacting_user_id

ORDER BY transaction_date DESC, transaction_id DESC
