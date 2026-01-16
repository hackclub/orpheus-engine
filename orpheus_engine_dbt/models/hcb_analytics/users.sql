{{ config(
    schema='hcb_analytics',
    materialized='table'
) }}

/*
    Users

    All HCB users with key profile information.

    Access levels:
    - Higher values = more platform-wide access
    - Most users have default access level
*/

SELECT
    -- Identifiers
    id AS user_id,
    email,
    slug,

    -- Profile info
    full_name,
    preferred_name,

    -- Platform access
    access_level,
    teenager AS is_teenager,

    -- Timestamps
    created_at,
    updated_at

FROM {{ source('hcb', 'users') }}

ORDER BY created_at DESC
