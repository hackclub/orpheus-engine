{{ config(
    schema='unified_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_events_ranking ON {{ this }} (event_type, event_date, email);"
) }}

WITH loops_events AS (
    SELECT *
    FROM {{ source('loops', 'audience') }}
    WHERE user_group = 'Hack Clubber'
      AND subscribed = true
),

-- Extract datetime (prefer precise timestamp if present; otherwise midnight UTC)
loops_unpivoted AS (
    SELECT
        le.email,
        e.key AS column_name,
        CASE
            -- ISO-like datetime (accepts space or 'T', optional seconds/ms, optional Z/offset)
            WHEN e.value ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?$'
              THEN (e.value)::timestamptz
            -- Date only -> midnight UTC
            WHEN e.value ~ '^\d{4}-\d{2}-\d{2}$'
              THEN ((e.value)::date)::timestamp AT TIME ZONE 'UTC'
        END AS event_date,
        -- camelCase event name
        lower(left(replace(initcap(replace(e.key,'_',' ')),' ',''),1)) ||
        substr(replace(initcap(replace(e.key,'_',' ')),' ',''),2) AS event,
        CASE WHEN e.key LIKE '%_approved_at' THEN 'ship' ELSE 'loops' END AS event_type
    FROM loops_events le,
         LATERAL jsonb_each_text(to_jsonb(le) - 'birthday') AS e(key, value)
    WHERE e.value IS NOT NULL
      AND e.value <> ''
      AND (
            e.value ~ '^\d{4}-\d{2}-\d{2}$'
         OR e.value ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}'
      )
      AND e.key NOT LIKE 'calculated%'
      AND e.key <> 'high_seas_last_synced_from_airtable'
      AND e.key NOT LIKE '%row_hash_at'
),

loops_final AS (
    SELECT
        event_date,
        email,
        event,
        event_type,
        'loops' AS source_system
    FROM loops_unpivoted
    WHERE event_date IS NOT NULL
      AND event_date::date >= DATE '2024-01-01'
      AND event_date::date <= (CURRENT_DATE + INTERVAL '1 day')
      AND event <> 'createdAt'
),

/* -------------------- Hackatime (preserve hourly timestamps; multiple per day) -------------------- */
hackatime_events AS (
    SELECT
        (activity_time AT TIME ZONE 'UTC') AS event_date,
        hackatime_first_email AS email,
        'hackatimeActivity' AS event,
        'hackatime' AS event_type,
        'hackatime' AS source_system
    FROM {{ ref('hourly_project_activity') }}
    WHERE hackatime_first_email IS NOT NULL
      AND activity_time IS NOT NULL
      AND hackatime_hours > 0
      AND activity_time::date >= DATE '2024-01-01'
      AND activity_time::date <= (CURRENT_DATE + INTERVAL '1 day')
),

/* -------------------- Hackatime Legacy (preserve hourly timestamps; multiple per day) -------------------- */
hackatime_legacy_events AS (
    SELECT
        (activity_time AT TIME ZONE 'UTC') AS event_date,
        hackatime_first_email AS email,
        'hackatimeLegacyActivity' AS event,
        'hackatime_legacy' AS event_type,
        'hackatimeLegacy' AS source_system
    FROM {{ ref('hackatime_legacy_hourly_project_activity') }}
    WHERE hackatime_first_email IS NOT NULL
      AND activity_time IS NOT NULL
      AND hackatime_hours > 0
      AND activity_time::date >= DATE '2024-01-01'
      AND activity_time::date <= (CURRENT_DATE + INTERVAL '1 day')
),

/* -------------------- HCB seen (preserve original timestamp) -------------------- */
hcb_seen_events AS (
    SELECT
        (ush.period_start_at AT TIME ZONE 'UTC') AS event_date,
        u.email AS email,
        'hcbSeenAt' AS event,
        'hcb' AS event_type,
        'hcb' AS source_system
    FROM {{ source('hcb', 'user_seen_at_histories') }} AS ush
    JOIN {{ source('hcb', 'users') }} AS u
      ON u.id = ush.user_id
    WHERE u.teenager = true
      AND u.email IS NOT NULL
      AND u.email <> ''
      AND ush.period_start_at IS NOT NULL
      AND ush.period_start_at::date >= DATE '2024-01-01'
      AND ush.period_start_at::date <= (CURRENT_DATE + INTERVAL '1 day')
),

/* -------------------- Slack "seen" events -------------------- */
slack_parsed AS (
  SELECT
    s."User ID" AS user_id,
    /* Always set slackSeenAt to 11:59 PM for the day */
    CASE
      WHEN btrim(s."Last active (UTC)") ~ '^[A-Za-z]{3} \d{1,2}, \d{4}(\s+\d{1,2}:\d{2}(:\d{2})?)?$'
        THEN
          COALESCE(
            -- Try with HH24:MI:SS
            (to_date(btrim(s."Last active (UTC)"), 'Mon DD, YYYY HH24:MI:SS'))::timestamp AT TIME ZONE 'UTC' + INTERVAL '23 hours 59 minutes',
            -- Try with HH24:MI
            (to_date(btrim(s."Last active (UTC)"), 'Mon DD, YYYY HH24:MI'))::timestamp AT TIME ZONE 'UTC' + INTERVAL '23 hours 59 minutes',
            -- Fallback to date-only at 11:59 PM
            (to_date(btrim(s."Last active (UTC)"), 'Mon DD, YYYY'))::timestamp AT TIME ZONE 'UTC' + INTERVAL '23 hours 59 minutes'
          )
      WHEN btrim(s."Last active (UTC)") = '' THEN NULL
      ELSE (to_date(btrim(s."Last active (UTC)"), 'Mon DD, YYYY'))::timestamp AT TIME ZONE 'UTC' + INTERVAL '23 hours 59 minutes'
    END AS event_date,
    NULLIF(btrim(s.email), '') AS s_email
  FROM {{ source('slack', 'member_analytics_csv') }} AS s
  UNION ALL
  SELECT
    s.user_id AS user_id,
    (s.date::timestamp AT TIME ZONE 'UTC') + INTERVAL '23 hours 59 minutes' AS event_date,
    NULLIF(btrim(s.email_address), '') AS s_email
  FROM {{ source('slack', 'member_analytics') }} AS s
  WHERE s.date IS NOT NULL
    AND s.is_active = true
),

slack_joined_people AS (
  SELECT
    p.slack_id,
    NULLIF(btrim(p.email), '') AS p_email,
    p.updated_at
  FROM {{ source('loops', 'audience') }} AS p
),

slack_with_email AS (
  SELECT
    p.event_date,
    COALESCE(j.p_email, p.s_email) AS email,
    p.user_id
  FROM slack_parsed AS p
  LEFT JOIN slack_joined_people AS j
    ON j.slack_id = p.user_id
  WHERE p.event_date IS NOT NULL
),

slack_ranked AS (
  SELECT
    event_date,
    email,
    row_number() OVER (
      PARTITION BY email, event_date::date
      ORDER BY event_date DESC
    ) AS rn
  FROM slack_with_email
  WHERE email IS NOT NULL
),

slack_events AS (
  SELECT
    event_date,
    email,
    'slackSeenAt' AS event,
    'slack' AS event_type,
    'slack' AS source_system
  FROM slack_ranked
  WHERE rn = 1
    AND event_date::date >= DATE '2024-01-01'
    AND event_date::date <= (CURRENT_DATE + INTERVAL '1 day')
),

/* -------------------- Hack Club Auth events -------------------- */
/* Absolute minimum permissions - only columns needed to generate events for monthly
   active stats (e.g. "logged in at", "enabled 2fa"). No tokens or secrets.
   Trust levels: 0=hq_official, 1=community_untrusted, 2=community_trusted */
auth_events AS (
  SELECT
    (a.created_at AT TIME ZONE 'UTC') AS event_date,
    i.primary_email AS email,
    CASE
      WHEN a.key = 'oauth_token.create' THEN 'loggedIn - ' || COALESCE(app.name, 'unknown')
      WHEN a.key = 'identity.saml_login' THEN 'loggedIn - ' || COALESCE(SUBSTRING(a.parameters FROM ':name: ([^\n]+)'), 'SAML')
      WHEN a.key = 'identity_session.create' THEN 'sessionCreated'
      WHEN a.key = 'identity_session.sign_out' THEN 'signedOut'
      WHEN a.key = 'identity_session.revoke' THEN 'sessionRevoked'
      WHEN a.key = 'identity.update' THEN 'profileUpdated'
      WHEN a.key = 'identity.revoke_all_sessions' THEN 'allSessionsRevoked'
      WHEN a.key = 'identity.generate_backup_codes' THEN 'backupCodesGenerated'
      WHEN a.key = 'identity.regenerate_backup_codes' THEN 'backupCodesRegenerated'
      WHEN a.key = 'identity.use_backup_code' THEN 'backupCodeUsed'
      WHEN a.key = 'identity_totp.create' THEN '2faEnabled'
      WHEN a.key = 'identity_webauthn_credential.create' THEN 'passkeyAdded'
      WHEN a.key = 'identity_webauthn_credential.destroy' THEN 'passkeyRemoved'
      WHEN a.key = 'verification_document_verification.create' THEN 'idSubmitted'
      WHEN a.key = 'verification_aadhaar_verification.create' THEN 'aadhaarStarted'
      WHEN a.key = 'verification_aadhaar_verification.create_link' THEN 'aadhaarLinkCreated'
      WHEN a.key = 'verification_aadhaar_verification.data_received' THEN 'aadhaarReceived'
      WHEN a.key = 'address.create' THEN 'addressAdded'
      WHEN a.key = 'address.update' THEN 'addressUpdated'
      WHEN a.key = 'address.destroy' THEN 'addressDeleted'
      WHEN a.key = 'oauth_token.revoke' THEN 'tokenRevoked'
      WHEN a.key = 'program.create' THEN 'oauthAppCreated'
      WHEN a.key = 'program.update' THEN 'oauthAppUpdated'
      WHEN a.key = 'program.destroy' THEN 'oauthAppDeleted'
      ELSE a.key
    END AS event,
    -- auth_hq = HQ apps (trust_level = 0), auth_community = community apps (trust_level > 0)
    CASE
      WHEN a.key = 'oauth_token.create' AND app.trust_level > 0 THEN 'auth_community'
      WHEN a.key = 'oauth_token.create' AND (app.trust_level = 0 OR app.trust_level IS NULL) THEN 'auth_hq'
      ELSE 'auth_hq'  -- non-login events default to auth_hq
    END AS event_type,
    'hackClubAuth' AS source_system
  FROM {{ source('auth', 'activities') }} a
  JOIN {{ source('auth', 'identities') }} i ON a.owner_id = i.id AND a.owner_type = 'Identity'
  LEFT JOIN {{ source('auth', 'oauth_access_tokens') }} t ON a.trackable_type = 'OAuthToken' AND a.trackable_id = t.id
  LEFT JOIN {{ source('auth', 'oauth_applications') }} app ON t.application_id = app.id
  WHERE a.owner_type = 'Identity'
    AND i.primary_email IS NOT NULL
    AND i.primary_email <> ''
    AND a.created_at::date >= DATE '2024-01-01'
    AND a.created_at::date <= (CURRENT_DATE + INTERVAL '1 day')
)

-- -------------------- Union all events --------------------
SELECT event_date, lower(email) AS email, event, event_type, source_system FROM loops_final
UNION ALL
SELECT event_date, lower(email) AS email, event, event_type, source_system FROM hackatime_events
UNION ALL
SELECT event_date, lower(email) AS email, event, event_type, source_system FROM hackatime_legacy_events
UNION ALL
SELECT event_date, lower(email) AS email, event, event_type, source_system FROM hcb_seen_events
UNION ALL
SELECT event_date, lower(email) AS email, event, event_type, source_system FROM slack_events
UNION ALL
SELECT event_date, lower(email) AS email, event, event_type, source_system FROM auth_events
ORDER BY email, event_date DESC, event
