{{ config(
    schema='hcb_analytics',
    materialized='table'
) }}

/*
    User Positions

    Links users to organizations with their role and permissions.
    Join with users and orgs models for full details.

    Role levels:
    - 5 = reader (view-only access)
    - 25 = member (basic access)
    - 100 = manager (full access, default for org creators)
*/

SELECT
    id AS position_id,
    user_id,
    event_id AS org_id,

    -- Role
    role AS role_level,
    CASE role
        WHEN 5 THEN 'reader'
        WHEN 25 THEN 'member'
        WHEN 100 THEN 'manager'
        ELSE 'unknown'
    END AS role_name,

    -- Position metadata
    is_signee,
    fiscal_sponsorship_contract_id,

    -- Status
    deleted_at IS NULL AS is_active,

    -- Timestamps
    created_at,
    updated_at,
    deleted_at

FROM {{ source('hcb', 'organizer_positions') }}

ORDER BY org_id, role DESC, created_at
