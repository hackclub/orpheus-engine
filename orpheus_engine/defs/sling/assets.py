from dagster import EnvVar, AssetExecutionContext, Nothing
from dagster_sling import SlingResource, SlingConnectionResource
from typing import Mapping, Any
from urllib.parse import urlparse, parse_qs
import dagster as dg
import ipaddress
import os
import base64


def _validate_sslmode_disable_is_tailscale(env_var_name: str) -> None:
    """
    Validates that if a connection URL uses sslmode=disable, the host must be a
    Tailscale IP (100.64.0.0/10 CGNAT range). This prevents accidentally disabling
    SSL for public-facing databases.
    """
    url = os.getenv(env_var_name, "")
    if not url:
        return

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    # Check if sslmode=disable is set
    sslmode = query_params.get("sslmode", [None])[0]
    if sslmode != "disable":
        return

    # Validate host is a Tailscale IP (100.64.0.0/10)
    host = parsed.hostname
    if not host:
        return

    try:
        ip = ipaddress.ip_address(host)
        tailscale_range = ipaddress.ip_network("100.64.0.0/10")
        if ip not in tailscale_range:
            raise ValueError(
                f"{env_var_name}: sslmode=disable is only allowed for Tailscale IPs "
                f"(100.64.0.0/10). Got host: {host}"
            )
    except ValueError as e:
        if "does not appear to be an IPv4 or IPv6 address" in str(e):
            # Host is a hostname, not an IP - sslmode=disable not allowed
            raise ValueError(
                f"{env_var_name}: sslmode=disable is only allowed for Tailscale IPs "
                f"(100.64.0.0/10), not hostnames. Got host: {host}"
            )
        raise


# Validate all sling connection URLs - sslmode=disable only allowed for Tailscale IPs
_SLING_CONNECTION_URL_ENV_VARS = [
    "HACKATIME_COOLIFY_URL",
    "HCER_PUBLIC_GITHUB_DATA_COOLIFY_URL",
    "SHIPWRECKED_THE_BAY_COOLIFY_URL",
    "JOURNEY_COOLIFY_URL",
    "SUMMER_OF_MAKING_2025_COOLIFY_URL",
    "HACKATIME_LEGACY_COOLIFY_URL",
    "FLAVORTOWN_COOLIFY_URL",
    "FLAVORTOWN_AHOY_COOLIFY_URL",
    "WAREHOUSE_COOLIFY_URL",
]

for _env_var in _SLING_CONNECTION_URL_ENV_VARS:
    _validate_sslmode_disable_is_tailscale(_env_var)

# --- Define Connections ---

# 1. Source Connection (Hackatime Database)
hackatime_db_connection = SlingConnectionResource(
    name="HACKATIME_DB",  # This name MUST match the 'source' key in replication_config
    type="postgres",
    connection_string=EnvVar("HACKATIME_COOLIFY_URL"),
)

hcer_public_github_data_connection = SlingConnectionResource(
    name="HCER_PUBLIC_GITHUB_DATA_DB",
    type="postgres",
    connection_string=EnvVar("HCER_PUBLIC_GITHUB_DATA_COOLIFY_URL"),
)

shipwrecked_the_bay_db_connection = SlingConnectionResource(
    name="SHIPWRECKED_THE_BAY_DB",
    type="postgres",
    connection_string=EnvVar("SHIPWRECKED_THE_BAY_COOLIFY_URL"),
)

journey_db_connection = SlingConnectionResource(
    name="JOURNEY_DB",
    type="postgres",
    connection_string=EnvVar("JOURNEY_COOLIFY_URL"),
)

summer_of_making_2025_db_connection = SlingConnectionResource(
    name="SUMMER_OF_MAKING_2025_DB",
    type="postgres",
    connection_string=EnvVar("SUMMER_OF_MAKING_2025_COOLIFY_URL"),
)

hackatime_legacy_db_connection = SlingConnectionResource(
    name="HACKATIME_LEGACY_DB",
    type="postgres",
    connection_string=EnvVar("HACKATIME_LEGACY_COOLIFY_URL"),
)

flavortown_db_connection = SlingConnectionResource(
    name="FLAVORTOWN_DB",
    type="postgres",
    connection_string=EnvVar("FLAVORTOWN_COOLIFY_URL"),
)

flavortown_ahoy_db_connection = SlingConnectionResource(
    name="FLAVORTOWN_AHOY_DB",
    type="postgres",
    connection_string=EnvVar("FLAVORTOWN_AHOY_COOLIFY_URL"),
)

# Auth DB connection - absolute minimum permissions to generate events for monthly
# active stats (e.g. "logged in at", "created oauth app"). No tokens or secrets.
def _get_auth_ssh_private_key() -> str:
    """Decode base64-encoded SSH private key from env var."""
    key_b64 = os.getenv("AUTH_SSH_PRIVATE_KEY_B64", "")
    if not key_b64:
        return ""
    return base64.b64decode(key_b64).decode("utf-8")

auth_db_connection = SlingConnectionResource(
    name="AUTH_DB",
    type="postgres",
    host=EnvVar("AUTH_DB_HOST"),
    port=EnvVar("AUTH_DB_PORT"),
    database=EnvVar("AUTH_DB_DATABASE"),
    user=EnvVar("AUTH_DB_USER"),
    password=EnvVar("AUTH_DB_PASSWORD"),
    sslmode="disable",  # SSL not needed through SSH tunnel
    ssh_tunnel=EnvVar("AUTH_SSH_TUNNEL"),
    ssh_private_key=_get_auth_ssh_private_key(),
)

def _get_hcb_ssh_private_key() -> str:
    """Decode base64-encoded SSH private key from env var."""
    key_b64 = os.getenv("HCB_SSH_PRIVATE_KEY_B64", "")
    if not key_b64:
        return ""
    return base64.b64decode(key_b64).decode("utf-8")

hcb_db_connection = SlingConnectionResource(
    name="HCB_DB",
    type="postgres",
    host=EnvVar("HCB_DB_HOST"),
    port=EnvVar("HCB_DB_PORT"),
    database=EnvVar("HCB_DB_DATABASE"),
    user=EnvVar("HCB_DB_USER"),
    password=EnvVar("HCB_DB_PASSWORD"),
    ssh_tunnel=EnvVar("HCB_SSH_TUNNEL"),
    ssh_private_key=_get_hcb_ssh_private_key(),
)

# 2. Target Connection (Warehouse Database)
warehouse_db_connection = SlingConnectionResource(
    name="WAREHOUSE_DB",  # This name MUST match the 'target' key in replication_config
    type="postgres",
    connection_string=EnvVar("WAREHOUSE_COOLIFY_URL"),
)

# --- Create Sling Resource ---
sling_replication_resource = SlingResource(
    connections=[
        hackatime_db_connection,
        hcer_public_github_data_connection,
        shipwrecked_the_bay_db_connection,
        journey_db_connection,
        summer_of_making_2025_db_connection,
        hackatime_legacy_db_connection,
        flavortown_db_connection,
        flavortown_ahoy_db_connection,
        auth_db_connection,
        hcb_db_connection,
        warehouse_db_connection,
    ]
)

# --- Define Replication Configuration ---
hackatime_replication_config = {
    "source": "HACKATIME_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hackatime.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
        # Large tables configured for incremental sync
        "public.heartbeats": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.leaderboard_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.raw_heartbeat_uploads": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },
    }
}

# --- Define Replication Configuration ---
hcer_public_github_data_replication_config = {
    "source": "HCER_PUBLIC_GITHUB_DATA_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hcer_public_github_data.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

# --- Journey Database Replication Configuration ---
journey_replication_config = {
    "source": "JOURNEY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "journey.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

# --- Shipwrecked The Bay Database Replication Configuration ---
shipwrecked_the_bay_replication_config = {
    "source": "SHIPWRECKED_THE_BAY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "shipwrecked_the_bay.{stream_table}",
    },

    "streams": {
        "public.*": None,
    }
}

# --- Summer of Making 2025 Database Replication Configuration ---
summer_of_making_2025_replication_config = {
    "source": "SUMMER_OF_MAKING_2025_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "summer_of_making_2025.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # Large tables configured for incremental sync
        "public.active_insights_requests": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 87M rows
        },
        "public.active_insights_jobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 8.6M rows
        },
        "public.vote_changes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 290K rows
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",  # 274K rows
        },
        "public.hackatime_projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 192K rows
        },
        "public.view_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 161K rows
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 149K rows
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",  # 46K rows
        },
        "public.devlogs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 48K rows
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 39K rows
        },
        "public.activities": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 30K rows
        },
    }
}

# --- Hackatime Legacy Database Replication Configuration ---
hackatime_legacy_replication_config = {
    "source": "HACKATIME_LEGACY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hackatime_legacy.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # Large tables configured for incremental sync
        "public.heartbeats": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",  # 52M rows - heartbeat timestamp
        },
    }
}

# --- FlavorTown Database Replication Configuration ---
flavortown_replication_config = {
    "source": "FLAVORTOWN_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "flavortown.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # Rails internal tables - disable
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},

        # Tables with id + updated_at - use incremental sync
        "public.action_mailbox_inbound_emails": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.active_insights_jobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.active_insights_requests": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_checks": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboard_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboards": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_credentials": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ledger_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.post_devlogs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.post_ship_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.posts": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_ideas": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_memberships": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.rsvps": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_card_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_items": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_orders": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_hackatime_projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_identities": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_role_assignments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        # Tables without updated_at stay full-refresh (default):
        # - active_storage_attachments, active_storage_blobs, active_storage_variant_records
        # - blazer_audits, versions
    }
}

# --- FlavorTown Ahoy Database Replication Configuration ---
flavortown_ahoy_replication_config = {
    "source": "FLAVORTOWN_AHOY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "flavortown_ahoy.{stream_table}",
    },

    "streams": {
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
    }
}

# --- HCB Database Replication Configuration ---
# For calculating monthly actives and transaction ledger
hcb_replication_config = {
    "source": "HCB_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "incremental",
        "primary_key": ["id"],
        "update_key": "updated_at",
        "object": "hcb.{stream_table}",
    },

    "streams": {
        # --- Users & Activity ---
        "public.users": None,
        "public.user_seen_at_histories": None,
        "public.organizer_positions": None,

        # --- Core Financial Tables ---
        "public.events": None,
        "public.event_plans": None,
        "public.canonical_transactions": None,
        "public.canonical_event_mappings": None,
        "public.canonical_pending_transactions": None,
        "public.canonical_pending_event_mappings": None,
        "public.canonical_pending_settled_mappings": None,
        "public.canonical_pending_declined_mappings": None,
        "public.hcb_codes": None,
        "public.fees": None,

        # --- Payment/Vendor Tables ---
        "public.disbursements": None,
        "public.ach_transfers": None,
        "public.donations": None,
        "public.wires": None,
        "public.checks": None,
        "public.increase_checks": None,

        # --- Card/Authorization Tables ---
        "public.stripe_cards": None,
        "public.stripe_cardholders": None,
        "public.stripe_authorizations": None,
        "public.card_grants": None,

        # --- Tags/Metadata ---
        "public.tags": None,
        "public.event_tags": None,
        "public.hcb_codes_tags": {
            "primary_key": ["hcb_code_id", "tag_id"],  # Join table, no id column
        },

        # --- Receipts ---
        "public.receipts": {
            "select": [
                "id", "user_id", "receiptable_type", "receiptable_id",
                "upload_method", "suggested_memo", "data_extracted",
                "extracted_subtotal_amount_cents", "extracted_total_amount_cents",
                "extracted_date", "extracted_merchant_name", "extracted_merchant_url",
                "extracted_merchant_zip_code", "extracted_currency",
                "textual_content_source", "created_at", "updated_at"
            ],  # Excludes *_ciphertext and *_bidx columns
        },
    }
}

# --- Auth Database Replication Configuration ---
# Absolute minimum permissions - only columns needed to generate events for monthly
# active stats (e.g. "logged in at"). SELECT * is blocked, explicit columns only.
auth_replication_config = {
    "source": "AUTH_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "incremental",
        "primary_key": ["id"],
    },

    "streams": {
        "public.activities": {
            "object": "auth.activities",
            "select": ["id", "owner_id", "owner_type", "key", "trackable_type", "trackable_id", "parameters", "created_at", "updated_at"],
            "update_key": "updated_at",
        },
        "public.identities": {
            "object": "auth.identities",
            "select": ["id", "primary_email", "updated_at"],
            "update_key": "updated_at",
        },
        "public.oauth_access_tokens": {
            "object": "auth.oauth_access_tokens",
            "select": ["id", "application_id", "resource_owner_id", "created_at"],
            "update_key": "created_at",
        },
        "public.oauth_applications": {
            "object": "auth.oauth_applications",
            "select": ["id", "name", "trust_level", "updated_at"],
            "update_key": "updated_at",
        },
    }
}

# --- Single Assets per Database ---

@dg.asset(
    name="hackatime_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime → warehouse Sling replication")

    # Iterate through the generator **without yielding** its events.
    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    # Optionally attach run‑level metadata
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hcer_public_github_data_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hcer_public_github_data_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire HCER Public GitHub Data DB → warehouse in a single shot."""
    context.log.info("Starting HCER Public GitHub Data → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hcer_public_github_data_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="journey_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def journey_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Journey DB → warehouse in a single shot."""
    context.log.info("Starting Journey → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=journey_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="shipwrecked_the_bay_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def shipwrecked_the_bay_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Shipwrecked The Bay DB → warehouse in a single shot."""
    context.log.info("Starting Shipwrecked The Bay → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=shipwrecked_the_bay_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="summer_of_making_2025_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def summer_of_making_2025_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Summer of Making 2025 DB → warehouse in a single shot."""
    context.log.info("Starting Summer of Making 2025 → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=summer_of_making_2025_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hackatime_legacy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_legacy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime Legacy DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime Legacy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_legacy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="flavortown_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def flavortown_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire FlavorTown DB → warehouse in a single shot."""
    context.log.info("Starting FlavorTown → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=flavortown_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="flavortown_ahoy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def flavortown_ahoy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates FlavorTown Ahoy analytics DB → warehouse with incremental sync."""
    context.log.info("Starting FlavorTown Ahoy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=flavortown_ahoy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hcb_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hcb_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates HCB users and user_seen_at_histories → warehouse via SSH tunnel."""
    context.log.info("Starting HCB → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hcb_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="auth_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def auth_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Auth DB tables → warehouse with explicit column selection."""
    context.log.info("Starting Auth → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=auth_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
