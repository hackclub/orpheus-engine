{{ config(
    schema='unified_analytics',
    materialized='table'
) }}

SELECT *
FROM (
    VALUES
        (
            'source_system_default',
            'loops',
            NULL::text,
            10,
            'preferred_loops_source_system',
            'Loops is the highest-priority source system because it contains program, mailing list, and profile origin signals.'
        ),
        (
            'event_override',
            'loops',
            'slackJoinedAt',
            30,
            'preferred_loops_slack_joined_event_override',
            'Loops slackJoinedAt is lowered to Slack priority because it represents Slack/community entry rather than a program signup.'
        ),
        (
            'source_system_default',
            'hcb',
            NULL::text,
            20,
            'preferred_hcb_source_system',
            'HCB activity is prioritized as a direct product origin signal.'
        ),
        (
            'source_system_default',
            'slack',
            NULL::text,
            30,
            'preferred_slack_source_system',
            'Slack activity is prioritized as a community origin signal.'
        ),
        (
            'source_system_default',
            'hackatime',
            NULL::text,
            40,
            'preferred_hackatime_source_system',
            'Current Hackatime coding activity.'
        ),
        (
            'source_system_default',
            'hackatimeLegacy',
            NULL::text,
            50,
            'preferred_hackatime_legacy_source_system',
            'Legacy Hackatime coding activity.'
        ),
        (
            'source_system_default',
            'hackClubAuth',
            NULL::text,
            60,
            'preferred_hack_club_auth_source_system',
            'Hack Club Auth is prioritized after product and community systems because it captures account and app login activity.'
        )
) AS priorities (
    priority_scope,
    source_system,
    event,
    source_system_priority,
    first_meaningful_event_justification,
    priority_description
)
