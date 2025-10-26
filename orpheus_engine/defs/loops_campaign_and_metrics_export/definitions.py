"""Dagster assets for Loops campaign and metrics export."""

import time
import requests
import polars as pl
import json
import os
import re
import dlt
import asyncio
import unicodedata
from dlt.destinations import postgres
from datetime import datetime
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

# --- Pydantic Models for AI Response Schema ---

class PIIRedaction(BaseModel):
    type: str  # email|phone|address|token|link_placeholder
    original_sample: str
    action: str  # removed|redacted|generalized

class AIAnalysisFlags(BaseModel):
    removed_email_references: bool
    personalization_replaced: bool
    pii_detected: bool
    pii_safe_to_redact: bool
    images_present: bool

class EmailPublishableAnalysisResponse(BaseModel):
    publish: bool
    reason: str
    decision_overridden_due_to_pii: bool
    title: str
    slug: str
    excerpt: str
    tags: List[str]
    content_markdown: str
    content_html: str
    flags: AIAnalysisFlags
    pii_redactions: List[PIIRedaction]
    transform_notes: List[str]

# Loops API Configuration
LOOPS_BASE_URL = "https://app.loops.so/api/trpc"
LOOPS_APP_BASE_URL = "https://app.loops.so"
LOOPS_TITLES_ENDPOINT = "/teams.getTitlesOfEmails"
LOOPS_EMAIL_BY_ID_ENDPOINT = "/emailMessages.getEmailMessageById"
LOOPS_METRICS_ENDPOINT = "/emailMessages.calculateEmailMessageMetrics"
LOOPS_MAILING_LISTS_ENDPOINT = "/mailingLists.fetchAll"
LOOPS_COMPOSE_PATH_TEMPLATE = "/campaigns/{campaignId}/compose"
LOOPS_COMPOSE_QUERYSTRING = (
    "stepName=Compose&page=0&pageSize=20&sortMetricsBy=firstName&"
    "sortMetricsOrder=desc&columnPinning=email"
)

# Regex to extract __NEXT_DATA__ from HTML
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(?P<json>{.*?})</script>',
    re.DOTALL | re.IGNORECASE,
)

# Debug limit for campaigns needing enrichment
# Set to -1 for no limit, or a positive integer to limit the number of campaigns processed
DEBUG_ENRICHMENT_LIMIT = -1

# Debug limit for AI publishable content analysis
# Set to -1 for no limit, or a positive integer to limit the number of campaigns processed
DEBUG_AI_ANALYSIS_LIMIT = 20

# Batch processing configuration for AI requests
AI_BATCH_SIZE = 20

# Common headers for Loops API requests
COMMON_HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'dnt': '1',
    'origin': 'https://app.loops.so',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://app.loops.so/audience',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
}


# --- Assets ---

@asset(
    group_name="loops_campaign_and_metrics_export",
    description="Fetches all campaign names and IDs from Loops via the tRPC API.",
    required_resource_keys={"loops_session_token"},
    compute_kind="loops_trpc",
)
def loops_campaign_names_and_ids(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Fetches campaign names and IDs from Loops using the tRPC teams.getTitlesOfEmails endpoint.
    
    Returns:
        A Polars DataFrame with columns: id, name, emoji
    """
    log = context.log
    session_token = context.resources.loops_session_token.get_value()
    
    url = f"{LOOPS_BASE_URL}{LOOPS_TITLES_ENDPOINT}"
    params = {"input": json.dumps({"json": {}})}
    headers = COMMON_HEADERS.copy()
    cookies = {'__Secure-next-auth.session-token': session_token}
    
    try:
        log.info("Fetching campaign titles from Loops tRPC API...")
        response = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract campaigns from nested response structure
        campaigns = data.get('result', {}).get('data', {}).get('json', {}).get('campaigns', [])
        
        if not campaigns:
            log.warning("No campaigns found in response")
            # Return empty DataFrame with correct schema
            return pl.DataFrame({
                "id": [],
                "name": [],
                "emoji": []
            }, schema={
                "id": pl.Utf8,
                "name": pl.Utf8,
                "emoji": pl.Utf8
            })
        
        log.info(f"Found {len(campaigns)} campaigns")
        
        # Convert to Polars DataFrame
        df = pl.DataFrame([
            {
                "id": campaign.get("id", ""),
                "name": (campaign.get("name") or "").strip(),
                "emoji": (campaign.get("emoji") or "").strip()
            }
            for campaign in campaigns
        ], schema={
            "id": pl.Utf8,
            "name": pl.Utf8,
            "emoji": pl.Utf8
        })
        
        context.add_output_metadata(
            metadata={
                "num_campaigns": df.height,
                "sample_campaigns": MetadataValue.md(
                    "\n".join([f"- {row['emoji']} {row['name']} (`{row['id']}`)" 
                              for row in df.head(10).to_dicts()])
                )
            }
        )
        
        return df
        
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch campaigns from Loops: {e}")
        if 'response' in locals():
            log.error(f"Response status: {response.status_code}")
            log.error(f"Response text: {response.text[:500]}")
        raise RuntimeError(f"Failed to fetch campaigns from Loops: {e}") from e
    except (KeyError, json.JSONDecodeError) as e:
        log.error(f"Failed to parse campaigns response: {e}")
        if 'response' in locals():
            log.error(f"Response text: {response.text[:500]}")
        raise RuntimeError(f"Failed to parse campaigns response: {e}") from e


@asset(
    group_name="loops_campaign_and_metrics_export",
    description="Fetches all mailing lists from Loops via the tRPC API.",
    required_resource_keys={"loops_session_token"},
    compute_kind="loops_trpc",
)
def loops_mailing_lists(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Fetches mailing lists from Loops using the tRPC mailingLists.fetchAll endpoint.
    
    Returns:
        A Polars DataFrame with columns: id, friendly_name, description, is_public, 
        color_scheme, deletion_status, campaigns_json
    """
    log = context.log
    session_token = context.resources.loops_session_token.get_value()
    
    url = f"{LOOPS_BASE_URL}{LOOPS_MAILING_LISTS_ENDPOINT}"
    params = {"input": json.dumps({"json": {}})}
    headers = COMMON_HEADERS.copy()
    cookies = {'__Secure-next-auth.session-token': session_token}
    
    try:
        log.info("Fetching mailing lists from Loops tRPC API...")
        response = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract mailing lists from nested response structure
        mailing_lists = data.get('result', {}).get('data', {}).get('json', [])
        
        if not mailing_lists:
            log.warning("No mailing lists found in response")
            # Return empty DataFrame with correct schema
            return pl.DataFrame({
                "id": [],
                "friendly_name": [],
                "description": [],
                "is_public": [],
                "color_scheme": [],
                "deletion_status": [],
                "campaigns_json": []
            }, schema={
                "id": pl.Utf8,
                "friendly_name": pl.Utf8,
                "description": pl.Utf8,
                "is_public": pl.Boolean,
                "color_scheme": pl.Utf8,
                "deletion_status": pl.Utf8,
                "campaigns_json": pl.Utf8
            })
        
        log.info(f"Found {len(mailing_lists)} mailing lists")
        
        # Convert to Polars DataFrame
        df = pl.DataFrame([
            {
                "id": ml.get("id", ""),
                "friendly_name": (ml.get("friendlyName") or "").strip(),
                "description": (ml.get("description") or "").strip(),
                "is_public": ml.get("isPublic", False),
                "color_scheme": (ml.get("colorScheme") or "").strip(),
                "deletion_status": (ml.get("deletionStatus") or "").strip(),
                "campaigns_json": json.dumps(ml.get("campaigns", []))
            }
            for ml in mailing_lists
        ], schema={
            "id": pl.Utf8,
            "friendly_name": pl.Utf8,
            "description": pl.Utf8,
            "is_public": pl.Boolean,
            "color_scheme": pl.Utf8,
            "deletion_status": pl.Utf8,
            "campaigns_json": pl.Utf8
        })
        
        context.add_output_metadata(
            metadata={
                "num_mailing_lists": df.height,
                "sample_mailing_lists": MetadataValue.md(
                    "\n".join([f"- {row['friendly_name']} (`{row['id']}`) - {row['description'][:50]}..." 
                              if len(row['description']) > 50 else f"- {row['friendly_name']} (`{row['id']}`) - {row['description']}"
                              for row in df.head(10).to_dicts()])
                )
            }
        )
        
        return df
        
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch mailing lists from Loops: {e}")
        if 'response' in locals():
            log.error(f"Response status: {response.status_code}")
            log.error(f"Response text: {response.text[:500]}")
        raise RuntimeError(f"Failed to fetch mailing lists from Loops: {e}") from e
    except (KeyError, json.JSONDecodeError) as e:
        log.error(f"Failed to parse mailing lists response: {e}")
        if 'response' in locals():
            log.error(f"Response text: {response.text[:500]}")
        raise RuntimeError(f"Failed to parse mailing lists response: {e}") from e


# --- Helper Functions ---

def warehouse_coolify_destination() -> postgres:
    """Creates and returns a configured postgres destination instance for DLT."""
    creds = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not creds:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set or empty.")
    return dlt.destinations.postgres(credentials=creds)


def _http_get_html(path: str, session_token: str, querystring: Optional[str] = None) -> str:
    """Helper to fetch HTML from Loops app."""
    if querystring:
        url = f"{LOOPS_APP_BASE_URL}{path}?{querystring}"
    else:
        url = f"{LOOPS_APP_BASE_URL}{path}"
    
    headers = {
        "user-agent": COMMON_HEADERS["user-agent"],
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "upgrade-insecure-requests": "1",
    }
    cookies = {"__Secure-next-auth.session-token": session_token}
    
    resp = requests.get(url, headers=headers, cookies=cookies, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {path}: {resp.text[:500]}")
    return resp.text


def _resolve_email_message_id_from_campaign(session_token: str, campaign_id: str) -> Optional[Dict[str, Any]]:
    """
    Load /campaigns/{campaignId}/compose and parse __NEXT_DATA__ for:
      - props.pageProps.campaign.emailMessage.id
      - props.pageProps.campaign.status
      - props.pageProps.campaign.sendTime (ISO string timestamp)
      - props.pageProps.campaign.mailingListId
      - props.pageProps.campaign.audienceFilter (JSON object)
    
    Returns dict with: email_message_id, status, sent_at_timestamp, mailing_list_id, audience_filter (or None if not found)
    """
    path = LOOPS_COMPOSE_PATH_TEMPLATE.format(campaignId=campaign_id)
    html = _http_get_html(path, session_token, LOOPS_COMPOSE_QUERYSTRING)
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        data = json.loads(m.group("json"))
        page_props = data.get("props", {}).get("pageProps", {})
        campaign_data = page_props.get("campaign", {})
        
        msg_id = campaign_data.get("emailMessage", {}).get("id")
        if not msg_id or not isinstance(msg_id, str):
            return None
        
        status = campaign_data.get("status", "")
        send_time_iso = campaign_data.get("sendTime")  # ISO string like "2025-10-17T19:13:51.021Z"
        mailing_list_id = campaign_data.get("mailingListId") or None
        audience_filter = campaign_data.get("audienceFilter") or None
        
        # Convert ISO string to Unix timestamp in milliseconds
        sent_at_timestamp = None
        if send_time_iso:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(send_time_iso.replace('Z', '+00:00'))
                sent_at_timestamp = int(dt.timestamp() * 1000)  # Convert to milliseconds
            except Exception:
                pass
        
        return {
            "email_message_id": msg_id,
            "status": status,
            "sent_at_timestamp": sent_at_timestamp,  # Unix timestamp in milliseconds
            "mailing_list_id": mailing_list_id,
            "audience_filter": audience_filter  # JSON object
        }
    except Exception:
        pass
    return None


def _fetch_email_message_by_id(session_token: str, email_message_id: str) -> Dict[str, Any]:
    """Fetch email message details from Loops tRPC API."""
    url = f"{LOOPS_BASE_URL}{LOOPS_EMAIL_BY_ID_ENDPOINT}"
    params = {"input": json.dumps({"json": {"id": email_message_id}})}
    headers = COMMON_HEADERS.copy()
    headers.update({
        "accept": "*/*",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    })
    cookies = {"__Secure-next-auth.session-token": session_token}
    
    resp = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for email message API: {resp.text[:500]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from email message API: {e}")


def _fetch_email_html(session_token: str, email_message_id: str) -> Optional[str]:
    """Fetch rendered email HTML from Loops API."""
    url = f"{LOOPS_APP_BASE_URL}/api/emailMessages/{email_message_id}/render-html"
    params = {"theme": "light"}
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "dnt": "1",
        "pragma": "no-cache",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": COMMON_HEADERS["user-agent"],
    }
    cookies = {"__Secure-next-auth.session-token": session_token}
    
    resp = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for email HTML API: {resp.text[:500]}")
    try:
        data = resp.json()
        if data.get("success"):
            return data.get("WYSIWYGemail", "")
        return None
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from email HTML API: {e}")


async def _process_campaign_with_ai(
    campaign_data: Dict[str, Any], 
    ai_client, 
    ai_prompt: str, 
    log
) -> Dict[str, Any]:
    """
    Process a single campaign with AI analysis.
    Returns a record dict with AI analysis results.
    """
    campaign_id = campaign_data["id"]
    campaign_name = campaign_data["name"]
    email_html = campaign_data["email_html"]
    subject = campaign_data["subject"]
    sent_at = campaign_data["sent_at"]
    
    try:
        # Convert sent_at to ISO string if available
        received_at_iso = None
        if sent_at:
            if isinstance(sent_at, str):
                received_at_iso = sent_at
            else:
                received_at_iso = sent_at.isoformat()
        
        # Construct input JSON for AI
        input_json = {
            "html": email_html,
            "fallback_subject": subject or "",
            "received_at": received_at_iso
        }
        
        # Create the full prompt with input
        full_prompt = f"{ai_prompt}\n\nINPUT:\n{json.dumps(input_json, indent=2)}"
        
        log.info(f"Processing AI analysis for campaign {campaign_name} ({campaign_id})")
        
        # Run the AI call in a thread pool to make it truly async
        loop = asyncio.get_event_loop()
        ai_response = await loop.run_in_executor(
            None,
            lambda: ai_client.generate_structured_response(
                prompt=full_prompt,
                response_schema=EmailPublishableAnalysisResponse,
                model="gpt-5"
            )
        )
        
        # Extract key fields
        ai_publishable = ai_response.publish
        ai_slug = ai_response.slug
        ai_content_markdown = ai_response.content_markdown if ai_publishable else ""
        ai_content_html = ai_response.content_html if ai_publishable else ""
        
        # Store full response as JSON
        ai_response_json = json.dumps(ai_response.model_dump())
        
        record = {
            "id": campaign_id,
            "ai_publishable_response_json": ai_response_json,
            "ai_publishable": ai_publishable,
            "ai_publishable_slug": ai_slug,
            "ai_publishable_content_markdown": ai_content_markdown,
            "ai_publishable_content_html": ai_content_html
        }
        
        log.info(f"Successfully analyzed campaign {campaign_name} ({campaign_id}) - Publishable: {ai_publishable}")
        return record
        
    except Exception as e:
        log.error(f"Failed to analyze campaign {campaign_name} ({campaign_id}): {e}")
        return {
            "id": campaign_id,
            "ai_publishable_response_json": None,
            "ai_publishable": None,
            "ai_publishable_slug": None,
            "ai_publishable_content_markdown": None,
            "ai_publishable_content_html": None,
            "error": str(e)
        }


def _html_to_markdown(html: str) -> str:
    """Convert HTML to Markdown using BeautifulSoup + pypandoc."""
    try:
        from bs4 import BeautifulSoup
        import pypandoc
        import re
        
        # First, use BeautifulSoup to clean up the HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove unwanted elements
        for tag in soup.find_all(['style', 'script', 'meta', 'head', 'title', 'noscript']):
            tag.decompose()
        
        # Unwrap ALL tables and their sub-elements to prevent table formatting in output
        for tag in soup.find_all(['table', 'tbody', 'thead', 'tfoot', 'tr', 'td', 'th', 'colgroup', 'col']):
            tag.unwrap()
        
        # Remove unnecessary attributes, but keep href for links
        for tag in soup.find_all(True):
            if tag.name == 'a' and tag.has_attr('href'):
                href = tag['href']
                tag.attrs = {'href': href}
            else:
                tag.attrs = {}
        
        # Get cleaned HTML
        cleaned_html = str(soup)
        
        # Convert to GitHub-Flavored Markdown using pypandoc
        extra_args = [
            "--from=html",
            "--to=gfm",
            "--wrap=none",
        ]
        
        markdown = pypandoc.convert_text(
            cleaned_html,
            to="gfm",
            format="html",
            extra_args=extra_args
        )
        
        # Post-process to remove any remaining HTML tags
        markdown = re.sub(r'<[^>]+>', '', markdown)
        
        # Remove invisible Unicode characters (zero-width spaces, soft hyphens, etc.)
        invisible_chars = [
            '\u200B',  # Zero-width space
            '\u200C',  # Zero-width non-joiner
            '\u200D',  # Zero-width joiner
            '\u200E',  # Left-to-right mark
            '\u200F',  # Right-to-left mark
            '\u00AD',  # Soft hyphen
            '\uFEFF',  # Zero-width no-break space (BOM)
            '\u180E',  # Mongolian vowel separator
            '\u2060',  # Word joiner
            '\u034F',  # Combining grapheme joiner
            '\u061C',  # Arabic letter mark
            '\u115F',  # Hangul choseong filler
            '\u1160',  # Hangul jungseong filler
            '\u17B4',  # Khmer vowel inherent Aq
            '\u17B5',  # Khmer vowel inherent Aa
            '\u3164',  # Hangul filler
        ]
        for char in invisible_chars:
            markdown = markdown.replace(char, '')
        
        # Remove any other zero-width or invisible characters using regex
        markdown = re.sub(r'[\u200B-\u200F\uFEFF\u2060\u180E]', '', markdown)
        
        # Clean up excessive blank lines
        markdown = re.sub(r'\n{3,}', '\n\n', markdown)
        
        # Clean up trailing spaces on lines
        markdown = re.sub(r' +\n', '\n', markdown)
        
        # Clean up excessive spaces
        markdown = re.sub(r' {2,}', ' ', markdown)
        
        return markdown.strip()
    except Exception as e:
        # If conversion fails, return a fallback message
        return f"<!-- Error converting HTML to Markdown: {e} -->\n\n{html[:500]}..."


# --- Campaign Enrichment Assets ---

@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Writes campaign names and IDs to warehouse.loops.campaigns table."
)
def loops_campaigns_to_warehouse(
    context: AssetExecutionContext,
    loops_campaign_names_and_ids: pl.DataFrame
) -> Output[None]:
    """
    Upserts campaign names/IDs to warehouse.loops.campaigns table.
    Only updates id, name, and emoji columns - does not touch other fields.
    """
    log = context.log
    import psycopg2
    from psycopg2.extras import execute_values
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    log.info(f"Upserting {loops_campaign_names_and_ids.height} campaigns to loops.campaigns table")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Ensure schema exists
                cursor.execute("CREATE SCHEMA IF NOT EXISTS loops")
                
                # Create table if it doesn't exist (only with basic fields)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS loops.campaigns (
                        id TEXT PRIMARY KEY,
                        name TEXT,
                        emoji TEXT,
                        content_last_fetched_at TIMESTAMP WITH TIME ZONE,
                        metrics_last_fetched_at TIMESTAMP WITH TIME ZONE
                    )
                """)
                
                # Ensure primary key exists (for tables created without it)
                cursor.execute("""
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = 'campaigns_pkey' 
                            AND conrelid = 'loops.campaigns'::regclass
                        ) THEN 
                            ALTER TABLE loops.campaigns 
                            ADD PRIMARY KEY (id);
                        END IF;
                    END $$;
                """)
                
                # Migrate existing last_enriched_at column if it exists
                cursor.execute("""
                    DO $$ 
                    BEGIN 
                        -- Check if last_enriched_at column exists
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'loops' 
                            AND table_name = 'campaigns' 
                            AND column_name = 'last_enriched_at'
                        ) THEN 
                            -- Rename last_enriched_at to content_last_fetched_at
                            ALTER TABLE loops.campaigns 
                            RENAME COLUMN last_enriched_at TO content_last_fetched_at;
                        END IF;
                        
                        -- Add metrics_last_fetched_at column if it doesn't exist
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'loops' 
                            AND table_name = 'campaigns' 
                            AND column_name = 'metrics_last_fetched_at'
                        ) THEN 
                            ALTER TABLE loops.campaigns 
                            ADD COLUMN metrics_last_fetched_at TIMESTAMP WITH TIME ZONE;
                        END IF;
                    END $$;
                """)
                
                conn.commit()
                
                # Prepare data for upsert
                records = [
                    (row["id"], row["name"], row["emoji"])
                    for row in loops_campaign_names_and_ids.iter_rows(named=True)
                ]
                
                # Upsert - only update name and emoji
                execute_values(
                    cursor,
                    """
                    INSERT INTO loops.campaigns (id, name, emoji)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        emoji = EXCLUDED.emoji
                    """,
                    records,
                    page_size=1000
                )
                
                conn.commit()
                log.info(f"Successfully upserted {len(records)} campaigns")
        
        return Output(
            value=None,
            metadata={
                "num_records": loops_campaign_names_and_ids.height,
                "operation": "upsert (id, name, emoji only)"
            }
        )
    except Exception as e:
        log.error(f"Failed to upsert campaigns: {e}")
        raise


@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Writes mailing lists to warehouse.loops.mailing_lists table."
)
def loops_mailing_lists_to_warehouse(
    context: AssetExecutionContext,
    loops_mailing_lists: pl.DataFrame
) -> Output[None]:
    """
    Upserts mailing lists to warehouse.loops.mailing_lists table.
    """
    log = context.log
    import psycopg2
    from psycopg2.extras import execute_values
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    log.info(f"Upserting {loops_mailing_lists.height} mailing lists to loops.mailing_lists table")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Ensure schema exists
                cursor.execute("CREATE SCHEMA IF NOT EXISTS loops")
                
                # Create table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS loops.mailing_lists (
                        id TEXT PRIMARY KEY,
                        friendly_name TEXT,
                        description TEXT,
                        is_public BOOLEAN,
                        color_scheme TEXT,
                        deletion_status TEXT,
                        campaigns_json JSONB,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                
                conn.commit()
                
                # Prepare data for upsert
                records = [
                    (
                        row["id"],
                        row["friendly_name"],
                        row["description"],
                        row["is_public"],
                        row["color_scheme"],
                        row["deletion_status"],
                        row["campaigns_json"]
                    )
                    for row in loops_mailing_lists.iter_rows(named=True)
                ]
                
                # Upsert - update all fields and timestamp
                execute_values(
                    cursor,
                    """
                    INSERT INTO loops.mailing_lists (
                        id, friendly_name, description, is_public, 
                        color_scheme, deletion_status, campaigns_json, last_updated_at
                    )
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        friendly_name = EXCLUDED.friendly_name,
                        description = EXCLUDED.description,
                        is_public = EXCLUDED.is_public,
                        color_scheme = EXCLUDED.color_scheme,
                        deletion_status = EXCLUDED.deletion_status,
                        campaigns_json = EXCLUDED.campaigns_json,
                        last_updated_at = NOW()
                    """,
                    [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], datetime.now()) for r in records],
                    page_size=1000
                )
                
                conn.commit()
                log.info(f"Successfully upserted {len(records)} mailing lists")
        
        return Output(
            value=None,
            metadata={
                "num_records": loops_mailing_lists.height,
                "operation": "upsert"
            }
        )
    except Exception as e:
        log.error(f"Failed to upsert mailing lists: {e}")
        raise


@asset(
    group_name="loops_campaign_and_metrics_export",
    compute_kind="sql",
    description="Queries warehouse for campaigns that need content enrichment based on sent status and time since last content fetch.",
    deps=["loops_campaigns_to_warehouse"]  # Ensure warehouse is updated first
)
def loops_campaigns_needing_content_fetch(
    context: AssetExecutionContext
) -> pl.DataFrame:
    """
    Queries warehouse for campaigns that need content enrichment.
    Update frequency depends on email status:
    - Not sent yet: every 6 hours
    - Sent < 1 week ago: every 24 hours
    - Sent >= 2 weeks and < 8 weeks ago: every 7 days
    - Sent >= 8 weeks and < 1 year ago: every 30 days
    - Sent >= 1 year ago: every 90 days
    """
    log = context.log
    import psycopg2
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    # Build query with time-based content enrichment logic
    query = """
        SELECT id, name, emoji
        FROM loops.campaigns
        WHERE 
            -- Never enriched
            content_last_fetched_at IS NULL
            OR
            -- Email not sent yet: update every 6 hours
            ((sent_at IS NULL OR status != 'sent') 
             AND content_last_fetched_at < NOW() - INTERVAL '6 hours')
            OR
            -- Sent less than 1 week ago: update every 24 hours
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at >= NOW() - INTERVAL '1 week' 
             AND content_last_fetched_at < NOW() - INTERVAL '24 hours')
            OR
            -- Sent between 2 and 8 weeks ago: update every 7 days
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at < NOW() - INTERVAL '2 weeks' 
             AND sent_at >= NOW() - INTERVAL '8 weeks'
             AND content_last_fetched_at < NOW() - INTERVAL '7 days')
            OR
            -- Sent between 8 weeks and 1 year ago: update monthly (30 days)
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at < NOW() - INTERVAL '8 weeks'
             AND sent_at >= NOW() - INTERVAL '1 year'
             AND content_last_fetched_at < NOW() - INTERVAL '30 days')
            OR
            -- Sent more than 1 year ago: update every 90 days
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at < NOW() - INTERVAL '1 year'
             AND content_last_fetched_at < NOW() - INTERVAL '90 days')
        ORDER BY content_last_fetched_at NULLS FIRST
    """
    
    if DEBUG_ENRICHMENT_LIMIT > 0:
        query += f"\n        LIMIT {DEBUG_ENRICHMENT_LIMIT}"
        log.info(f"Debug mode: Limiting campaigns to {DEBUG_ENRICHMENT_LIMIT}")
    else:
        log.info("No limit on campaigns (processing all that need content enrichment)")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            df = pl.read_database(query, connection=conn)
        
        log.info(f"Found {df.height} campaigns needing content enrichment")
        
        context.add_output_metadata(
            metadata={
                "num_campaigns_needing_content_enrichment": df.height,
                "sample_campaigns": MetadataValue.md(
                    "\n".join([f"- {row['name']} (`{row['id']}`)" 
                              for row in df.head(5).to_dicts()])
                ) if df.height > 0 else MetadataValue.text("No campaigns need content enrichment")
            }
        )
        
        return df
    except Exception as e:
        log.error(f"Failed to query warehouse for campaigns needing content enrichment: {e}")
        raise


@asset(
    group_name="loops_campaign_and_metrics_export",
    compute_kind="sql",
    description="Queries warehouse for campaigns that need metrics enrichment based on sent status and time since last metrics fetch.",
    deps=["loops_campaigns_to_warehouse"]  # Ensure warehouse is updated first
)
def loops_campaigns_needing_metrics_fetch(
    context: AssetExecutionContext
) -> pl.DataFrame:
    """
    Queries warehouse for campaigns that need metrics enrichment.
    Only refresh metrics for sent campaigns (metrics are heavy):
    - Not sent yet: skip (no metrics to fetch)
    - Sent < 1 week ago: every 24 hours
    - Sent >= 2 weeks and < 8 weeks ago: every 7 days
    - Sent >= 8 weeks and < 1 year ago: every 30 days
    - Sent >= 1 year ago: every 90 days
    """
    log = context.log
    import psycopg2
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    # Build query with time-based metrics enrichment logic
    query = """
        SELECT id, name, emoji
        FROM loops.campaigns
        WHERE 
            -- Only process sent campaigns
            sent_at IS NOT NULL AND status = 'sent'
            AND (
                -- Never enriched
                metrics_last_fetched_at IS NULL
                OR
                -- Sent less than 1 week ago: update every 24 hours
                (sent_at >= NOW() - INTERVAL '1 week' 
                 AND metrics_last_fetched_at < NOW() - INTERVAL '24 hours')
                OR
                -- Sent between 2 and 8 weeks ago: update every 7 days
                (sent_at < NOW() - INTERVAL '2 weeks' 
                 AND sent_at >= NOW() - INTERVAL '8 weeks'
                 AND metrics_last_fetched_at < NOW() - INTERVAL '7 days')
                OR
                -- Sent between 8 weeks and 1 year ago: update monthly (30 days)
                (sent_at < NOW() - INTERVAL '8 weeks'
                 AND sent_at >= NOW() - INTERVAL '1 year'
                 AND metrics_last_fetched_at < NOW() - INTERVAL '30 days')
                OR
                -- Sent more than 1 year ago: update every 90 days
                (sent_at < NOW() - INTERVAL '1 year'
                 AND metrics_last_fetched_at < NOW() - INTERVAL '90 days')
            )
        ORDER BY metrics_last_fetched_at NULLS FIRST
    """
    
    if DEBUG_ENRICHMENT_LIMIT > 0:
        query += f"\n        LIMIT {DEBUG_ENRICHMENT_LIMIT}"
        log.info(f"Debug mode: Limiting campaigns to {DEBUG_ENRICHMENT_LIMIT}")
    else:
        log.info("No limit on campaigns (processing all that need metrics enrichment)")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            df = pl.read_database(query, connection=conn)
        
        log.info(f"Found {df.height} campaigns needing metrics enrichment")
        
        context.add_output_metadata(
            metadata={
                "num_campaigns_needing_metrics_enrichment": df.height,
                "sample_campaigns": MetadataValue.md(
                    "\n".join([f"- {row['name']} (`{row['id']}`)" 
                              for row in df.head(5).to_dicts()])
                ) if df.height > 0 else MetadataValue.text("No campaigns need metrics enrichment")
            }
        )
        
        return df
    except Exception as e:
        log.error(f"Failed to query warehouse for campaigns needing metrics enrichment: {e}")
        raise


@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Queries warehouse for sent campaigns that need AI publishable content analysis.",
    deps=["loops_campaign_contents_to_warehouse"]  # Ensure campaign contents are fetched and stored first
)
def loops_campaigns_needing_publishable_content_analysis(
    context: AssetExecutionContext
) -> pl.DataFrame:
    """
    Queries warehouse for sent campaigns that need AI publishable content analysis.
    Only processes sent campaigns that have email_html but haven't been AI-processed yet.
    """
    log = context.log
    import psycopg2
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Ensure ai_publishable_processed_at column exists
                cursor.execute("""
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'loops' 
                            AND table_name = 'campaigns' 
                            AND column_name = 'ai_publishable_processed_at'
                        ) THEN 
                            ALTER TABLE loops.campaigns 
                            ADD COLUMN ai_publishable_processed_at TIMESTAMP WITH TIME ZONE;
                        END IF;
                    END $$;
                """)
                conn.commit()
                log.info("Ensured ai_publishable_processed_at column exists")
            
            # Build query for campaigns needing AI analysis
            query = """
                SELECT id, name, emoji, subject, email_html, sent_at
                FROM loops.campaigns
                WHERE 
                    status ILIKE 'sent'
                    AND email_html IS NOT NULL
                    AND email_html != ''
                    AND ai_publishable_processed_at IS NULL
                ORDER BY sent_at DESC
            """
            
            if DEBUG_AI_ANALYSIS_LIMIT > 0:
                query += f"\n                LIMIT {DEBUG_AI_ANALYSIS_LIMIT}"
                log.info(f"Debug mode: Limiting AI analysis campaigns to {DEBUG_AI_ANALYSIS_LIMIT}")
            else:
                log.info("No limit on campaigns (processing all that need AI analysis)")
            
            df = pl.read_database(query, connection=conn)
        
        # Debug: Check what campaigns exist in the database
        debug_query = """
            SELECT 
                COUNT(*) as total_campaigns,
                COUNT(CASE WHEN status ILIKE 'sent' THEN 1 END) as sent_campaigns,
                COUNT(CASE WHEN email_html IS NOT NULL AND email_html != '' THEN 1 END) as campaigns_with_html,
                COUNT(CASE WHEN status ILIKE 'sent' AND email_html IS NOT NULL AND email_html != '' THEN 1 END) as sent_with_html,
                COUNT(CASE WHEN status ILIKE 'sent' AND email_html IS NOT NULL AND email_html != '' AND ai_publishable_processed_at IS NULL THEN 1 END) as needing_analysis
            FROM loops.campaigns
        """
        
        debug_df = pl.read_database(debug_query, connection=conn)
        debug_row = debug_df.to_dicts()[0]
        
        log.info(f"Database debug info: {debug_row}")
        
        # Debug: Show sample status values
        sample_query = """
            SELECT DISTINCT status, COUNT(*) as count
            FROM loops.campaigns 
            GROUP BY status 
            ORDER BY count DESC
        """
        sample_df = pl.read_database(sample_query, connection=conn)
        log.info(f"Status values in database: {sample_df.to_dicts()}")
        
        # Debug: Show sample campaigns with email_html
        sample_html_query = """
            SELECT id, name, status, 
                   CASE WHEN email_html IS NOT NULL THEN 'has_html' ELSE 'no_html' END as html_status,
                   CASE WHEN ai_publishable_processed_at IS NOT NULL THEN 'processed' ELSE 'not_processed' END as ai_status
            FROM loops.campaigns 
            WHERE email_html IS NOT NULL AND email_html != ''
            ORDER BY sent_at DESC
            LIMIT 5
        """
        sample_html_df = pl.read_database(sample_html_query, connection=conn)
        log.info(f"Sample campaigns with HTML: {sample_html_df.to_dicts()}")
        
        log.info(f"Found {df.height} campaigns needing AI publishable content analysis")
        
        context.add_output_metadata(
            metadata={
                "num_campaigns_needing_ai_analysis": df.height,
                "sample_campaigns": MetadataValue.md(
                    "\n".join([f"- {row['name']} (`{row['id']}`) - {row['subject'][:50]}..." 
                              if len(row['subject']) > 50 else f"- {row['name']} (`{row['id']}`) - {row['subject']}"
                              for row in df.head(5).to_dicts()])
                ) if df.height > 0 else MetadataValue.text("No campaigns need AI analysis")
            }
        )
        
        return df
    except Exception as e:
        log.error(f"Failed to query warehouse for campaigns needing AI analysis: {e}")
        raise


@asset(
    group_name="loops_campaign_and_metrics_export",
    required_resource_keys={"loops_session_token"},
    compute_kind="loops_trpc",
    description="Scrapes email content from Loops compose HTML for campaigns needing content enrichment."
)
def loops_campaign_email_contents(
    context: AssetExecutionContext,
    loops_campaigns_needing_content_fetch: pl.DataFrame
) -> pl.DataFrame:
    """
    Scrapes email content from Loops compose HTML for each campaign.
    Returns DataFrame with campaign ID and email content fields.
    """
    log = context.log
    session_token = context.resources.loops_session_token.get_value()
    
    if loops_campaigns_needing_content_fetch.height == 0:
        log.info("No campaigns need enrichment, returning empty DataFrame")
        return pl.DataFrame({
            "id": [],
            "name": [],
            "emoji": [],
            "status": [],
            "email_message_id": [],
            "subject": [],
            "from_name": [],
            "from_email": [],
            "reply_to_email": [],
            "email_content_json": [],
            "email_html": [],
            "email_markdown": [],
            "created_at": [],
            "sent_at": [],
            "sent_count": [],
            "opens": [],
            "clicks": [],
            "unsubscribes": [],
            "hard_bounces": [],
            "soft_bounces": [],
            "mailing_list_id": [],
            "audience_filter_json": []
        }, schema={
            "id": pl.Utf8,
            "name": pl.Utf8,
            "emoji": pl.Utf8,
            "status": pl.Utf8,
            "email_message_id": pl.Utf8,
            "subject": pl.Utf8,
            "from_name": pl.Utf8,
            "from_email": pl.Utf8,
            "reply_to_email": pl.Utf8,
            "email_content_json": pl.Utf8,
            "email_html": pl.Utf8,
            "email_markdown": pl.Utf8,
            "created_at": pl.Datetime,
            "sent_at": pl.Datetime,
            "sent_count": pl.Int64,
            "opens": pl.Int64,
            "clicks": pl.Int64,
            "unsubscribes": pl.Int64,
            "hard_bounces": pl.Int64,
            "soft_bounces": pl.Int64,
            "mailing_list_id": pl.Utf8,
            "audience_filter_json": pl.Utf8
        })
    
    records = []
    success_count = 0
    error_count = 0
    
    for row in loops_campaigns_needing_content_fetch.iter_rows(named=True):
        campaign_id = row["id"]
        campaign_name = row["name"]
        campaign_emoji = row["emoji"]
        
        try:
            # Step 1: Resolve campaign ID to email message ID (and get status, sent_at)
            campaign_info = _resolve_email_message_id_from_campaign(session_token, campaign_id)
            
            if not campaign_info:
                log.warning(f"Could not resolve email message ID for campaign {campaign_name} ({campaign_id})")
                error_count += 1
                continue
            
            email_message_id = campaign_info["email_message_id"]
            status = campaign_info.get("status", "")
            sent_at_timestamp = campaign_info.get("sent_at_timestamp")
            mailing_list_id = campaign_info.get("mailing_list_id") or None
            audience_filter = campaign_info.get("audience_filter")
            
            # Convert Unix timestamp (milliseconds) to datetime
            if sent_at_timestamp:
                from datetime import datetime
                sent_at = datetime.fromtimestamp(sent_at_timestamp / 1000)
            else:
                sent_at = None
            
            # Step 2: Fetch email message details
            email_data = _fetch_email_message_by_id(session_token, email_message_id)
            
            # Extract data from nested response
            j = email_data.get('result', {}).get('data', {}).get('json', {})
            
            if not j:
                log.warning(f"Empty response for email message {email_message_id}")
                error_count += 1
                continue
            
            # Extract created_at timestamp
            created_at_str = j.get("createdAt")
            if created_at_str:
                from datetime import datetime
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            else:
                created_at = None
            
            # Construct full from_email (fromEmail + "@" + domain)
            from_email_local = j.get("fromEmail") or ""
            sending_domain = j.get("sendingDomain", {}).get("domain", "")
            if from_email_local and sending_domain:
                from_email = f"{from_email_local}@{sending_domain}"
            else:
                from_email = from_email_local  # Fallback to just the local part
            
            # Step 3: Fetch email HTML
            email_html = _fetch_email_html(session_token, email_message_id)
            if not email_html:
                log.warning(f"Could not fetch HTML for email message {email_message_id}")
                email_html = ""
            
            # Step 4: Convert HTML to Markdown
            email_markdown = _html_to_markdown(email_html) if email_html else ""
            
            # Extract fields (include name and emoji to prevent overwriting with NULL)
            record = {
                "id": campaign_id,
                "name": campaign_name,
                "emoji": campaign_emoji,
                "status": status,
                "email_message_id": email_message_id,
                "subject": (j.get("subject") or "").strip(),
                "from_name": (j.get("fromName") or "").strip(),
                "from_email": from_email.strip(),
                "reply_to_email": (j.get("replyToEmail") or "").strip(),
                "email_content_json": json.dumps(j.get("emailContent", {})),
                "email_html": email_html,
                "email_markdown": email_markdown,
                "created_at": created_at,
                "sent_at": sent_at,
                "sent_count": j.get("sentCount"),
                "opens": j.get("opens"),
                "clicks": j.get("clicks"),
                "unsubscribes": j.get("unsubscribes"),
                "hard_bounces": j.get("hardBounces"),
                "soft_bounces": j.get("softBounces"),
                "mailing_list_id": mailing_list_id,
                "audience_filter_json": json.dumps(audience_filter) if audience_filter else None
            }
            
            records.append(record)
            success_count += 1
            log.info(f"Successfully fetched content for campaign {campaign_name} ({campaign_id})")
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            log.error(f"Failed to fetch content for campaign {campaign_name} ({campaign_id}): {e}")
            error_count += 1
    
    log.info(f"Campaign content fetching completed. Success: {success_count}, Errors: {error_count}")
    
    if not records:
        log.warning("No campaign contents were successfully fetched")
        return pl.DataFrame({
            "id": [],
            "name": [],
            "emoji": [],
            "status": [],
            "email_message_id": [],
            "subject": [],
            "from_name": [],
            "from_email": [],
            "reply_to_email": [],
            "email_content_json": [],
            "email_html": [],
            "email_markdown": [],
            "created_at": [],
            "sent_at": [],
            "sent_count": [],
            "opens": [],
            "clicks": [],
            "unsubscribes": [],
            "hard_bounces": [],
            "soft_bounces": [],
            "mailing_list_id": [],
            "audience_filter_json": []
        }, schema={
            "id": pl.Utf8,
            "name": pl.Utf8,
            "emoji": pl.Utf8,
            "status": pl.Utf8,
            "email_message_id": pl.Utf8,
            "subject": pl.Utf8,
            "from_name": pl.Utf8,
            "from_email": pl.Utf8,
            "reply_to_email": pl.Utf8,
            "email_content_json": pl.Utf8,
            "email_html": pl.Utf8,
            "email_markdown": pl.Utf8,
            "created_at": pl.Datetime,
            "sent_at": pl.Datetime,
            "sent_count": pl.Int64,
            "opens": pl.Int64,
            "clicks": pl.Int64,
            "unsubscribes": pl.Int64,
            "hard_bounces": pl.Int64,
            "soft_bounces": pl.Int64,
            "mailing_list_id": pl.Utf8,
            "audience_filter_json": pl.Utf8
        })
    
    df = pl.DataFrame(records)
    
    context.add_output_metadata(
        metadata={
            "num_campaigns_enriched": success_count,
            "num_errors": error_count,
            "sample_subjects": MetadataValue.md(
                "\n".join([f"- {row['subject']}" for row in df.head(5).to_dicts()])
            )
        }
    )
    
    return df


@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Updates loops.campaigns table with enriched content and sets content_last_fetched_at timestamp."
)
def loops_campaign_contents_to_warehouse(
    context: AssetExecutionContext,
    loops_campaign_email_contents: pl.DataFrame
) -> Output[None]:
    """
    Upserts enriched campaign content to loops.campaigns table.
    Updates all enrichment fields and sets content_last_fetched_at = NOW().
    """
    log = context.log
    import psycopg2
    from psycopg2.extras import execute_values
    
    if loops_campaign_email_contents.height == 0:
        log.info("No campaign contents to write to warehouse")
        return Output(
            value=None,
            metadata={
                "num_records": 0,
                "note": "No campaigns were enriched"
            }
        )
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    enrichment_timestamp = datetime.now()
    log.info(f"Upserting {loops_campaign_email_contents.height} enriched campaigns to loops.campaigns table")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Ensure all enrichment columns exist
                enrichment_columns = [
                    ("status", "TEXT"),
                    ("email_message_id", "TEXT"),
                    ("subject", "TEXT"),
                    ("from_name", "TEXT"),
                    ("from_email", "TEXT"),
                    ("reply_to_email", "TEXT"),
                    ("email_content_json", "TEXT"),
                    ("email_html", "TEXT"),
                    ("email_markdown", "TEXT"),
                    ("created_at", "TIMESTAMP WITH TIME ZONE"),
                    ("sent_at", "TIMESTAMP WITH TIME ZONE"),
                    ("sent_count", "INTEGER"),
                    ("opens", "INTEGER"),
                    ("clicks", "INTEGER"),
                    ("unsubscribes", "INTEGER"),
                    ("hard_bounces", "INTEGER"),
                    ("soft_bounces", "INTEGER"),
                    ("mailing_list_id", "TEXT"),
                    ("audience_filter_json", "JSONB"),
                ]
                
                for col_name, col_type in enrichment_columns:
                    cursor.execute(f"""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_schema = 'loops' 
                                AND table_name = 'campaigns' 
                                AND column_name = '{col_name}'
                            ) THEN 
                                ALTER TABLE loops.campaigns 
                                ADD COLUMN {col_name} {col_type};
                            END IF;
                        END $$;
                    """)
                
                conn.commit()
                log.info("Ensured all enrichment columns exist")
                
                # Prepare data for upsert
                records = [
                    (
                        row["id"],
                        row["name"],
                        row["emoji"],
                        row["status"],
                        row["email_message_id"],
                        row["subject"],
                        row["from_name"],
                        row["from_email"],
                        row["reply_to_email"],
                        row["email_content_json"],
                        row["email_html"],
                        row["email_markdown"],
                        row["created_at"],
                        row["sent_at"],
                        row["sent_count"],
                        row["opens"],
                        row["clicks"],
                        row["unsubscribes"],
                        row["hard_bounces"],
                        row["soft_bounces"],
                        row["mailing_list_id"],
                        row["audience_filter_json"],
                        enrichment_timestamp
                    )
                    for row in loops_campaign_email_contents.iter_rows(named=True)
                ]
                
                # Upsert - update all enrichment fields
                execute_values(
                    cursor,
                    """
                    INSERT INTO loops.campaigns (
                        id, name, emoji, status, email_message_id, subject,
                        from_name, from_email, reply_to_email, email_content_json,
                        email_html, email_markdown, created_at, sent_at, sent_count,
                        opens, clicks, unsubscribes, hard_bounces, soft_bounces,
                        mailing_list_id, audience_filter_json,
                        content_last_fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        emoji = EXCLUDED.emoji,
                        status = EXCLUDED.status,
                        email_message_id = EXCLUDED.email_message_id,
                        subject = EXCLUDED.subject,
                        from_name = EXCLUDED.from_name,
                        from_email = EXCLUDED.from_email,
                        reply_to_email = EXCLUDED.reply_to_email,
                        email_content_json = EXCLUDED.email_content_json,
                        email_html = EXCLUDED.email_html,
                        email_markdown = EXCLUDED.email_markdown,
                        created_at = EXCLUDED.created_at,
                        sent_at = EXCLUDED.sent_at,
                        sent_count = EXCLUDED.sent_count,
                        opens = EXCLUDED.opens,
                        clicks = EXCLUDED.clicks,
                        unsubscribes = EXCLUDED.unsubscribes,
                        hard_bounces = EXCLUDED.hard_bounces,
                        soft_bounces = EXCLUDED.soft_bounces,
                        mailing_list_id = EXCLUDED.mailing_list_id,
                        audience_filter_json = EXCLUDED.audience_filter_json,
                        content_last_fetched_at = EXCLUDED.content_last_fetched_at
                    """,
                    records,
                    page_size=100
                )
                
                conn.commit()
                log.info(f"Successfully upserted {len(records)} enriched campaigns")
        
        return Output(
            value=None,
            metadata={
                "num_records": loops_campaign_email_contents.height,
                "enrichment_timestamp": MetadataValue.text(str(enrichment_timestamp)),
                "operation": "upsert (all enrichment fields)"
            }
        )
    except Exception as e:
        log.error(f"Failed to upsert enriched campaigns: {e}")
        raise


# --- AI Publishable Content Analysis Assets ---

@asset(
    group_name="loops_campaign_and_metrics_export",
    required_resource_keys={"ai_client"},
    compute_kind="ai_analysis",
    description="Analyzes campaign emails with AI to determine if they're suitable for blog publication."
)
def loops_campaign_publishable_content(
    context: AssetExecutionContext,
    loops_campaigns_needing_publishable_content_analysis: pl.DataFrame
) -> pl.DataFrame:
    """
    Analyzes campaign emails with AI to determine publishability and clean content.
    Returns DataFrame with AI analysis results for each campaign.
    """
    log = context.log
    ai_client = context.resources.ai_client
    
    if loops_campaigns_needing_publishable_content_analysis.height == 0:
        log.info("No campaigns need AI analysis, returning empty DataFrame")
        return pl.DataFrame({
            "id": [],
            "ai_publishable_response_json": [],
            "ai_publishable": [],
            "ai_publishable_slug": [],
            "ai_publishable_content_markdown": [],
            "ai_publishable_content_html": []
        }, schema={
            "id": pl.Utf8,
            "ai_publishable_response_json": pl.Utf8,
            "ai_publishable": pl.Boolean,
            "ai_publishable_slug": pl.Utf8,
            "ai_publishable_content_markdown": pl.Utf8,
            "ai_publishable_content_html": pl.Utf8
        })
    
    # AI prompt from user specification
    ai_prompt = """
You are an expert email-to-blog converter. You ingest ONE email (often raw HTML) and decide if it should be published as a blog post. If publishable, you return final, ready-to-publish Markdown **and** HTML that **preserves the input HTML’s styling and structure exactly** except for the minimal edits required to (a) match the Markdown’s content and (b) remove email-only/PII content.

If a section is deleted in Markdown, delete the corresponding HTML **section**. Be **layout-smart**: preserve the surrounding rhythm and spacing; delete only what’s necessary and compact leftover spacers (see **Smart deletions & spacer compaction**). If the unsubscribe/footer area needs to be deleted, delete it and its local spacers, but keep the overall layout intact.

## INPUT (JSON you will receive)

```json
{
  "html": "<raw email html here>",
  "fallback_subject": "Subject line if missing in HTML",
  "received_at": "ISO8601 timestamp (optional)"
}
```

## OUTPUT (return EXACTLY ONE JSON object; no prose before/after)

```json
{
  "publish": true,
  "reason": "Short justification (<= 25 words).",
  "decision_overridden_due_to_pii": false,
  "title": "Concise, faithful title",
  "slug": "kebab-case-slug",
  "excerpt": "1–2 sentences from the email, no new info.",
  "tags": ["few","keywords","optional"],
  "content_markdown": "FINAL READY-TO-PUBLISH MARKDOWN",
  "content_html": "<!doctype html>... FINAL, EDITED HTML (IDENTICAL STYLING; CONTENT MATCHES MARKDOWN) ...",
  "flags": {
    "removed_email_references": true,
    "personalization_replaced": true,
    "pii_detected": false,
    "pii_safe_to_redact": true,
    "images_present": true,
    "verified_consistency_of_spacing_in_returned_content_html": true
  },
  "pii_redactions": [
    { "type": "email|phone|address|token|link_placeholder", "original_sample": "…", "action": "removed|redacted|generalized" }
  ],
  "transform_notes": [
    "Bullet list of notable edits."
  ]
}
```

## ORDER OF OPERATIONS (STRICT)

1. **Decide publishability** (see PUBLISH DECISION).
2. **Produce `content_markdown`** first. All content decisions happen here.
3. **Derive `content_html` by minimally editing the original HTML so its visible content exactly matches the Markdown**, while keeping styling and layout identical.

---

## PUBLISH DECISION

* **Publish (`true`)** for substantive content: announcements, essays, stories, launches, event recaps, calls to action, product/game updates, general news.
* **Do NOT publish (`false`)** if primarily: feedback request; apology/correction (“oops”); purely transactional/administrative (unsubscribe, receipts, password resets, billing, delivery confirmations); nonsensical without mail-merge fields; list-maintenance only (“You’re receiving this because…”, “Manage preferences…”).
* If publication is blocked due to essential PII that cannot be generalized, set:

  * `"publish": false`
  * `"decision_overridden_due_to_pii": true`
  * `"content_markdown": ""`
  * `"content_html": ""`
  * include concrete findings in `"pii_redactions"`.

## PII RULE (strict)

* Detect PII: emails, phone numbers, postal addresses, account/order numbers, private/tokenized links.
* If PII is **not essential**: remove or generalize and proceed; record in `"pii_redactions"`; set `"pii_detected": true` and `"pii_safe_to_redact": true`.
* If PII is **essential** (e.g., private address/phone required to attend; non-public contact): **do not publish** (as above).

---

## TRANSFORM: “VERBATIM BUT BLOG-SAFE”

Keep the writer’s wording and **visual intent**.

### Markdown rules

* Minimal, clean, semantic headings; standard link/image syntax; **no inline CSS** in Markdown.
* The Markdown’s content and ordering must match the kept HTML content (images/links preserved).
* Replace personalization (e.g., “Hi {first_name}” → “Hi all”).
* Remove email-only references and unsubscribe/footer content.

### HTML rules (IDENTICAL STYLING; CONTENT PARITY WITH MARKDOWN)

**Start from the original input HTML.** Everywhere not edited must remain **byte-for-byte the same** (same classes/IDs, inline styles, media queries, tables, attribute order, comments, whitespace, and DOM order).

**Allowed edits ONLY:**

1. **Text edits** needed to align wording with Markdown (no style/structure changes).
2. **Link cleanup:** keep button/link appearance; strip tracking params (`utm_*`, `mc_*`, `gclid`, `fbclid`, per-recipient tokens like `?e=…`, `token=…`). If stripping breaks the URL, replace with a safe public base URL or remove per PII rules.
3. **Image handling:** keep meaningful images and their exact attributes; remove 1×1 pixels/invisible spacers; update `alt` only to remove PII/placeholders.
4. **Email-only content removal:** remove blocks like “You’re receiving this email because…”, “Manage/Update preferences”, “Unsubscribe”, “View in browser”, ESP footers, CAN-SPAM address blocks, tracking pixels.
5. **Personalization → blog-appropriate:** “Hi {first_name}” / “Hello {{ first_name }}” → “Hi all” (or drop if fluff), editing only text nodes.
6. **Scripts:** remove `<script>` tags. **Keep `<link>`/`<style>` (including Google Fonts) unchanged** unless inside a fully deleted subtree.

---

## SMART DELETIONS & SPACER COMPACTION (to avoid broken flow)

**Goal:** Remove unwanted content **without leaving holes** or breaking rhythm—especially around removed list-maintenance lines near the top.

### A) Two-pass keep map

* **Pass 1 (KEEP):** Map Markdown blocks to HTML by matching normalized visible text. Mark nodes containing kept text as **KEEP**.
* **Pass 2 (CANDIDATE):** Nodes not in KEEP that match email-only/PII patterns become **CANDIDATE** for deletion.

### B) Smallest-ancestor deletion with stoplist

Delete the **smallest enclosing ancestor** whose descendants are all CANDIDATE or **pure spacers** (see C) and which is not one of the **stoplist** ancestors:

* Column container (e.g., `.mj-column-per-*`, `td.column-container`, `.styled-primary-column`, or equivalent).
* The immediate `<table role="presentation">` that also contains other KEEP siblings.
* `<body>`, `<html>`, or global wrappers.

### C) Spacer detection (conservative)

A node is a **spacer** *only if*:

* Rendered text is empty/whitespace/`&nbsp;`, **and**
* It contains no meaningful images (≤2×2 px or `display:none` images are non-meaningful), **and**
* It’s clearly for padding/spacing (empty `<tr>`, `<td>`, `<div>`, single empty `<span>`, or a table that becomes empty after deletions).

### D) **Adjacent spacer collapse (deterministic)**

After any deletion or text edit, for each container (e.g., the primary column/table body):

* Find **runs of consecutive spacers** between two KEEP blocks; **reduce the run to exactly one spacer**.
* **Preference when choosing which spacer to keep (pick the first that applies):**

  1. Keep the spacer **outside** the deleted subtree.
  2. Keep the spacer with **greater explicit vertical padding** (inline `padding-top/bottom` or CSS height).
  3. Otherwise keep the **later** spacer in document order.
* If a spacer is both **leading** (at the start of the column) or **trailing** (at the end), keep **at most one** there.

### E) **Spacer content sanitization (no visual change)**

To avoid extra blank lines caused by spacer text nodes:

* If a preserved spacer’s container (`td`/`div`) already provides vertical padding (inline style with `padding-top` or `padding-bottom` ≥ 8px), **remove inner `&nbsp;`/whitespace** from that spacer (leave the element and its padding intact).
* Do **not** alter styles, classes, or structure; only clear the inner text for that spacer container.

### F) Shared-separator preservation

If a spacer sits **between two KEEP blocks** that originally had a single separator, ensure **exactly one** remains after edits (neither zero nor two).

### G) Valid table cleanup (surgical)

* Remove any now-empty `<tr>`; if a `<table>` becomes empty, remove it.
* Keep MSO conditionals balanced; if their contents were fully removed, delete the empty conditional pair.

### H) Guardrails (don’t over-delete)

* Never delete the greeting/opening line or the first two substantive paragraphs **unless** Markdown removed them.
* Never delete a container that also holds any KEEP node.
* Prefer editing link/text over removing the enclosing block if the block is otherwise KEEP.

---

## TITLE, SLUG, EXCERPT, TAGS

* **Title**: `<title>`, else first visible `<h1>`, else infer from opening lines (no added hype).
* **Slug**: kebab-case, ASCII only, collapse multiple hyphens, ≤ 80 chars.
* **Excerpt**: 1–2 sentences from the body, no new info/CTAs.
* **Tags**: 3–6 concise keywords (optional).

---

## VALIDATION (self-check before returning)

* Return **ONE** JSON object only; no extra text.
* If `"publish": false` → `"content_markdown"` and `"content_html"` are empty strings.
* All URLs valid; tracking params removed; no stray encoded quotes.
* Flags reflect reality (`images_present` true only if ≥1 meaningful image remains).
* No duplicate keys/objects.
* **Table integrity:** no empty `<tr>/<td>` shells; no empty wrapper tables; conditional comments balanced.
* **Flow continuity:** no obvious blank holes where email-only sections were; between any two KEEP blocks there is **at most one** spacer.
* **HTML fidelity:** aside from the explicitly allowed edits and smart, minimal deletions/compaction above, the HTML is identical to the input (same styling, classes, IDs, structure, attribute order, comments, and whitespace).

**Proceed with the above. Output exactly and only the final JSON object.**
"""
    
    # Apply debug limit if set
    campaigns_to_process = loops_campaigns_needing_publishable_content_analysis
    if DEBUG_AI_ANALYSIS_LIMIT > 0:
        campaigns_to_process = campaigns_to_process.head(DEBUG_AI_ANALYSIS_LIMIT)
        log.info(f"Debug mode: Processing only first {DEBUG_AI_ANALYSIS_LIMIT} campaigns")
    
    # Convert to list of dicts for processing
    campaign_list = campaigns_to_process.to_dicts()
    total_campaigns = len(campaign_list)
    
    log.info(f"Starting batch processing of {total_campaigns} campaigns in batches of {AI_BATCH_SIZE}")
    
    async def process_batches():
        records = []
        success_count = 0
        error_count = 0
        
        # Process campaigns in batches
        for i in range(0, total_campaigns, AI_BATCH_SIZE):
            batch = campaign_list[i:i + AI_BATCH_SIZE]
            batch_num = (i // AI_BATCH_SIZE) + 1
            total_batches = (total_campaigns + AI_BATCH_SIZE - 1) // AI_BATCH_SIZE
            
            log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} campaigns)")
            
            # Create async tasks for this batch
            tasks = [
                _process_campaign_with_ai(campaign_data, ai_client, ai_prompt, log)
                for campaign_data in batch
            ]
            
            # Process batch in parallel using asyncio.gather
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    if isinstance(result, Exception):
                        error_count += 1
                        log.error(f"Batch processing error: {result}")
                    elif result.get("error"):
                        error_count += 1
                    else:
                        records.append(result)
                        success_count += 1
                
                log.info(f"Batch {batch_num} completed. Success: {success_count}, Errors: {error_count}")
                    
            except Exception as e:
                log.error(f"Batch {batch_num} failed: {e}")
                error_count += len(batch)
        
        log.info(f"AI analysis completed. Success: {success_count}, Errors: {error_count}")
        return records, success_count, error_count
    
    # Run the async batch processing
    records, success_count, error_count = asyncio.run(process_batches())
    
    if not records:
        log.warning("No campaigns were successfully analyzed")
        return pl.DataFrame({
            "id": [],
            "ai_publishable_response_json": [],
            "ai_publishable": [],
            "ai_publishable_slug": [],
            "ai_publishable_content_markdown": [],
            "ai_publishable_content_html": []
        }, schema={
            "id": pl.Utf8,
            "ai_publishable_response_json": pl.Utf8,
            "ai_publishable": pl.Boolean,
            "ai_publishable_slug": pl.Utf8,
            "ai_publishable_content_markdown": pl.Utf8,
            "ai_publishable_content_html": pl.Utf8
        })
    
    df = pl.DataFrame(records)
    
    context.add_output_metadata(
        metadata={
            "num_campaigns_analyzed": success_count,
            "num_errors": error_count,
            "sample_results": MetadataValue.md(
                "\n".join([f"- {row['id']}: Publishable={row['ai_publishable']}" 
                          for row in df.head(5).to_dicts()])
            )
        }
    )
    
    return df


@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Updates loops.campaigns table with AI publishable content analysis results."
)
def loops_campaign_publishable_content_to_warehouse(
    context: AssetExecutionContext,
    loops_campaign_publishable_content: pl.DataFrame
) -> Output[None]:
    """
    Upserts AI analysis results to loops.campaigns table.
    Updates AI fields and sets ai_publishable_processed_at = NOW().
    """
    log = context.log
    import psycopg2
    from psycopg2.extras import execute_values
    
    if loops_campaign_publishable_content.height == 0:
        log.info("No AI analysis results to write to warehouse")
        return Output(
            value=None,
            metadata={
                "num_records": 0,
                "note": "No campaigns were analyzed"
            }
        )
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    processing_timestamp = datetime.now()
    log.info(f"Upserting {loops_campaign_publishable_content.height} AI analysis results to loops.campaigns table")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Ensure all AI columns exist
                ai_columns = [
                    ("ai_publishable_response_json", "JSONB"),
                    ("ai_publishable", "BOOLEAN"),
                    ("ai_publishable_slug", "TEXT"),
                    ("ai_publishable_content_markdown", "TEXT"),
                    ("ai_publishable_content_html", "TEXT"),
                    ("ai_publishable_processed_at", "TIMESTAMP WITH TIME ZONE"),
                ]
                
                for col_name, col_type in ai_columns:
                    cursor.execute(f"""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_schema = 'loops' 
                                AND table_name = 'campaigns' 
                                AND column_name = '{col_name}'
                            ) THEN 
                                ALTER TABLE loops.campaigns 
                                ADD COLUMN {col_name} {col_type};
                            END IF;
                        END $$;
                    """)
                
                conn.commit()
                log.info("Ensured all AI columns exist")
                
                # Helper function to remove control characters from strings
                def sanitize_string(value):
                    if value is None:
                        return value
                    if isinstance(value, str):
                        # Remove control characters (Cc) and format characters (Cf) which can cause database issues
                        # Cc = Control (NULL, LF, etc.)
                        # Cf = Format (zero-width spaces, etc.)
                        return ''.join(
                            char for char in value 
                            if unicodedata.category(char) not in ('Cc', 'Cf')
                        )
                    return value
                
                # Helper function to sanitize JSON structures (dicts/lists)
                def sanitize_json_structure(obj):
                    if isinstance(obj, str):
                        return sanitize_string(obj)
                    if isinstance(obj, dict):
                        return {sanitize_json_structure(k): sanitize_json_structure(v) for k, v in obj.items()}
                    if isinstance(obj, list):
                        return [sanitize_json_structure(item) for item in obj]
                    return obj
                
                # Helper function to sanitize JSON string value
                def sanitize_json(value):
                    if value is None:
                        return value
                    if isinstance(value, str):
                        # Check if it's a JSON string that needs parsing
                        stripped = value.strip()
                        if (stripped.startswith('{') and stripped.endswith('}')) or \
                           (stripped.startswith('[') and stripped.endswith(']')):
                            try:
                                # Parse, sanitize recursively, then re-serialize
                                parsed = json.loads(value)
                                sanitized = sanitize_json_structure(parsed)
                                return json.dumps(sanitized)
                            except (json.JSONDecodeError, TypeError):
                                # If it's not valid JSON, just sanitize the string
                                return sanitize_string(value)
                        else:
                            # Not a JSON structure, just sanitize the string
                            return sanitize_string(value)
                    return value
                
                # Prepare data for upsert with debug logging
                records = []
                for idx, row in enumerate(loops_campaign_publishable_content.iter_rows(named=True)):
                    campaign_id = row["id"]
                    
                    # Check for control characters in raw data
                    for field_name in ["ai_publishable_response_json", "ai_publishable_slug", 
                                      "ai_publishable_content_markdown", "ai_publishable_content_html"]:
                        raw_value = row[field_name]
                        if raw_value and isinstance(raw_value, str):
                            control_chars = []
                            for char in raw_value:
                                cat = unicodedata.category(char)
                                if cat in ('Cc', 'Cf'):
                                    control_chars.append((ord(char), repr(char)))
                            if control_chars:
                                log.warning(f"[Record {idx+1}] {campaign_id}: Found control chars in {field_name}: {control_chars[:5]}")
                    
                    record = (
                        campaign_id,
                        sanitize_json(row["ai_publishable_response_json"]),
                        row["ai_publishable"],
                        sanitize_string(row["ai_publishable_slug"]),
                        sanitize_string(row["ai_publishable_content_markdown"]),
                        sanitize_string(row["ai_publishable_content_html"]),
                        processing_timestamp
                    )
                    
                    # Debug: check sanitized values
                    for i, value in enumerate(record[1:-1]):  # Skip id and timestamp
                        if value and isinstance(value, str):
                            for char in value:
                                cat = unicodedata.category(char)
                                if cat in ('Cc', 'Cf'):
                                    field_names = ["ai_publishable_response_json", "ai_publishable", 
                                                 "ai_publishable_slug", "ai_publishable_content_markdown", 
                                                 "ai_publishable_content_html"]
                                    log.error(f"[Record {idx+1}] {campaign_id}: SANITIZATION FAILED in {field_names[i]}: {ord(char)} (category: {cat})")
                    
                    records.append(record)
                    log.info(f"[Record {idx+1}/{loops_campaign_publishable_content.height}] Prepared {campaign_id}")
                
                log.info(f"Prepared {len(records)} records for upsert")
                
                # Upsert - update all AI fields
                # Insert in batches to help identify problematic records
                batch_size = 100
                inserted_count = 0
                for batch_start in range(0, len(records), batch_size):
                    batch_end = min(batch_start + batch_size, len(records))
                    batch = records[batch_start:batch_end]
                    
                    try:
                        execute_values(
                            cursor,
                            """
                            INSERT INTO loops.campaigns (
                                id, ai_publishable_response_json, ai_publishable,
                                ai_publishable_slug, ai_publishable_content_markdown, ai_publishable_content_html,
                                ai_publishable_processed_at
                            )
                            VALUES %s
                            ON CONFLICT (id) DO UPDATE SET
                                ai_publishable_response_json = EXCLUDED.ai_publishable_response_json,
                                ai_publishable = EXCLUDED.ai_publishable,
                                ai_publishable_slug = EXCLUDED.ai_publishable_slug,
                                ai_publishable_content_markdown = EXCLUDED.ai_publishable_content_markdown,
                                ai_publishable_content_html = EXCLUDED.ai_publishable_content_html,
                                ai_publishable_processed_at = EXCLUDED.ai_publishable_processed_at
                            """,
                            batch,
                            page_size=100
                        )
                        inserted_count += len(batch)
                        log.info(f"Successfully inserted batch {batch_start}-{batch_end-1}")
                    except Exception as e:
                        log.error(f"Failed to insert batch {batch_start}-{batch_end-1}: {e}")
                        # Try to identify the problematic record in this batch
                        for i, record in enumerate(batch):
                            log.error(f"  Record {batch_start + i}: id={record[0]}, "
                                    f"ai_publishable_response_json length={len(record[1]) if record[1] else 0}, "
                                    f"ai_publishable={record[2]}, slug length={len(record[3]) if record[3] else 0}, "
                                    f"markdown length={len(record[4]) if record[4] else 0}, "
                                    f"html length={len(record[5]) if record[5] else 0}")
                        raise
                
                conn.commit()
                log.info(f"Successfully upserted {inserted_count} AI analysis results")
        
        return Output(
            value=None,
            metadata={
                "num_records": loops_campaign_publishable_content.height,
                "processing_timestamp": MetadataValue.text(str(processing_timestamp)),
                "operation": "upsert (AI analysis fields)"
            }
        )
    except Exception as e:
        log.error(f"Failed to upsert AI analysis results: {e}")
        raise


# --- Campaign Metrics Assets ---

def _fetch_campaign_metrics_page(
    session_token: str,
    email_message_id: str,
    page: int = 0,
    page_size: int = 100
) -> Dict[str, Any]:
    """Fetch a single page of campaign metrics from Loops API."""
    url = f"{LOOPS_BASE_URL}{LOOPS_METRICS_ENDPOINT}"
    
    params = {
        "input": json.dumps({
            "json": {
                "emailMessageId": email_message_id,
                "filters": None,
                "page": page,
                "pageSize": page_size,
                "sortOrder": "desc",
                "sortBy": "email"
            }
        })
    }
    
    headers = COMMON_HEADERS.copy()
    cookies = {"__Secure-next-auth.session-token": session_token}
    
    resp = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for metrics API: {resp.text[:500]}")
    
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from metrics API: {e}")


@asset(
    group_name="loops_campaign_and_metrics_export",
    required_resource_keys={"loops_session_token"},
    compute_kind="loops_trpc",
    description="Fetches detailed recipient-level metrics for campaigns needing enrichment."
)
def loops_campaign_recipient_metrics(
    context: AssetExecutionContext,
    loops_campaigns_needing_metrics_fetch: pl.DataFrame
) -> pl.DataFrame:
    """
    Fetches recipient-level metrics for each campaign.
    Returns DataFrame with one row per campaign per recipient.
    """
    log = context.log
    session_token = context.resources.loops_session_token.get_value()
    
    if loops_campaigns_needing_metrics_fetch.height == 0:
        log.info("No campaigns to fetch metrics for, returning empty DataFrame")
        return pl.DataFrame({
            "campaign_id": [],
            "email": [],
            "delivered_at": [],
            "unsubscribed_date": [],
            "opens": [],
            "clicks": [],
            "complaint": [],
            "soft_bounce": [],
            "hard_bounce": [],
            "created_at": []
        }, schema={
            "campaign_id": pl.Utf8,
            "email": pl.Utf8,
            "delivered_at": pl.Datetime,
            "unsubscribed_date": pl.Datetime,
            "opens": pl.Int64,
            "clicks": pl.Int64,
            "complaint": pl.Boolean,
            "soft_bounce": pl.Boolean,
            "hard_bounce": pl.Boolean,
            "created_at": pl.Datetime
        })
    
    all_records = []
    total_recipients = 0
    campaigns_processed = 0
    
    for row in loops_campaigns_needing_metrics_fetch.iter_rows(named=True):
        campaign_id = row["id"]
        campaign_name = row["name"]
        
        try:
            # First, resolve the email_message_id for this campaign
            log.info(f"Resolving email message ID for campaign {campaign_name} ({campaign_id})")
            campaign_info = _resolve_email_message_id_from_campaign(session_token, campaign_id)
            
            if not campaign_info or not campaign_info.get("email_message_id"):
                log.warning(f"Could not resolve email message ID for campaign {campaign_name} ({campaign_id})")
                continue
            
            email_message_id = campaign_info["email_message_id"]
            log.info(f"Fetching metrics for campaign {campaign_name} ({campaign_id})")
            
            # Rate limiting after resolving email_message_id
            time.sleep(0.5)
            
            page = 0
            campaign_recipients = 0
            seen_emails = set()  # Track which emails we've seen for this campaign
            
            while True:
                # Fetch one page of metrics
                response = _fetch_campaign_metrics_page(session_token, email_message_id, page=page)
                
                # Extract emails from response
                emails = (
                    response.get("result", {})
                    .get("data", {})
                    .get("json", {})
                    .get("emailMetrics", {})
                    .get("emails", [])
                )
                
                if not emails:
                    # No more recipients
                    break
                
                # Check if we're getting duplicates (same emails as before)
                current_emails = {email.get("email") for email in emails if email.get("email")}
                new_emails = current_emails - seen_emails
                duplicate_count = len(current_emails) - len(new_emails)
                
                if duplicate_count > 0:
                    log.warning(
                        f"Page {page} has {duplicate_count}/{len(current_emails)} duplicates "
                        f"({duplicate_count/len(current_emails)*100:.1f}% duplicate)"
                    )
                
                if current_emails and not new_emails:
                    log.warning(f"Page {page} returned ONLY duplicate emails, stopping pagination")
                    break
                
                # Process each recipient
                for email_data in emails:
                    # Parse timestamps
                    delivered_at = None
                    if email_data.get("vendorDeliveredAt"):
                        try:
                            delivered_at = datetime.fromisoformat(
                                email_data["vendorDeliveredAt"].replace('Z', '+00:00')
                            )
                        except:
                            pass
                    
                    unsubscribed_date = None
                    if email_data.get("unsubscribedDate"):
                        try:
                            unsubscribed_date = datetime.fromisoformat(
                                email_data["unsubscribedDate"].replace('Z', '+00:00')
                            )
                        except:
                            pass
                    
                    created_at = None
                    if email_data.get("createdAt"):
                        try:
                            created_at = datetime.fromisoformat(
                                email_data["createdAt"].replace('Z', '+00:00')
                            )
                        except:
                            pass
                    
                    record = {
                        "campaign_id": campaign_id,
                        "email": email_data.get("email", ""),
                        "delivered_at": delivered_at,
                        "unsubscribed_date": unsubscribed_date,
                        "opens": email_data.get("vendorOpen", 0),
                        "clicks": email_data.get("vendorClick", 0),
                        "complaint": email_data.get("vendorComplaint", False),
                        "soft_bounce": email_data.get("vendorSoftBounce", False),
                        "hard_bounce": email_data.get("vendorHardBounce", False),
                        "created_at": created_at
                    }
                    
                    # Only add if we haven't seen this email before
                    email_addr = email_data.get("email")
                    if email_addr:
                        if email_addr in seen_emails:
                            # Already processed this email on a previous page
                            log.debug(f"Skipping duplicate email {email_addr}")
                            continue
                        seen_emails.add(email_addr)
                    
                    all_records.append(record)
                    campaign_recipients += 1
                
                # If we got fewer than 100 emails, we've reached the end
                if len(emails) < 100:
                    log.info(f"Received {len(emails)} emails (< 100), pagination complete")
                    break
                
                # Move to next page
                page += 1
                log.info(f"Moving to page {page} (fetched {len(emails)} emails, {campaign_recipients} total for this campaign)")
                
                # Rate limiting
                time.sleep(0.5)
            
            total_recipients += campaign_recipients
            campaigns_processed += 1
            log.info(f"Fetched {campaign_recipients} recipients for campaign {campaign_name}")
            
        except Exception as e:
            log.error(f"Failed to fetch metrics for campaign {campaign_name} ({campaign_id}): {e}")
            continue
    
    log.info(f"Metrics fetching completed. Campaigns: {campaigns_processed}, Total recipients: {total_recipients}")
    
    if not all_records:
        log.warning("No metrics were successfully fetched")
        return pl.DataFrame({
            "campaign_id": [],
            "email": [],
            "delivered_at": [],
            "unsubscribed_date": [],
            "opens": [],
            "clicks": [],
            "complaint": [],
            "soft_bounce": [],
            "hard_bounce": [],
            "created_at": []
        }, schema={
            "campaign_id": pl.Utf8,
            "email": pl.Utf8,
            "delivered_at": pl.Datetime,
            "unsubscribed_date": pl.Datetime,
            "opens": pl.Int64,
            "clicks": pl.Int64,
            "complaint": pl.Boolean,
            "soft_bounce": pl.Boolean,
            "hard_bounce": pl.Boolean,
            "created_at": pl.Datetime
        })
    
    # Create DataFrame with explicit schema to handle None values
    df = pl.DataFrame(all_records, schema={
        "campaign_id": pl.Utf8,
        "email": pl.Utf8,
        "delivered_at": pl.Datetime,
        "unsubscribed_date": pl.Datetime,
        "opens": pl.Int64,
        "clicks": pl.Int64,
        "complaint": pl.Boolean,
        "soft_bounce": pl.Boolean,
        "hard_bounce": pl.Boolean,
        "created_at": pl.Datetime
    })
    
    context.add_output_metadata(
        metadata={
            "num_campaigns": campaigns_processed,
            "num_recipients": total_recipients,
            "avg_recipients_per_campaign": total_recipients / campaigns_processed if campaigns_processed > 0 else 0
        }
    )
    
    return df


@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Writes campaign recipient metrics to warehouse.loops.campaign_metrics table."
)
def loops_campaign_metrics_to_warehouse(
    context: AssetExecutionContext,
    loops_campaign_recipient_metrics: pl.DataFrame
) -> Output[None]:
    """
    Upserts campaign recipient metrics to warehouse.
    Uses composite primary key (campaign_id, contact_id) to prevent duplicates.
    """
    log = context.log
    import psycopg2
    from psycopg2.extras import execute_values
    
    if loops_campaign_recipient_metrics.height == 0:
        log.info("No metrics to write to warehouse")
        return Output(
            value=None,
            metadata={
                "num_records": 0,
                "note": "No metrics were fetched"
            }
        )
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    log.info(f"Upserting {loops_campaign_recipient_metrics.height} metrics to loops.campaign_metrics table")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Create table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS loops.campaign_metrics (
                        campaign_id TEXT NOT NULL,
                        email TEXT NOT NULL,
                        delivered_at TIMESTAMP WITH TIME ZONE,
                        unsubscribed_date TIMESTAMP WITH TIME ZONE,
                        opens INTEGER,
                        clicks INTEGER,
                        complaint BOOLEAN,
                        soft_bounce BOOLEAN,
                        hard_bounce BOOLEAN,
                        created_at TIMESTAMP WITH TIME ZONE,
                        PRIMARY KEY (campaign_id, email)
                    )
                """)
                
                conn.commit()
                
                # Prepare data for upsert
                records = [
                    (
                        row["campaign_id"],
                        row["email"],
                        row["delivered_at"],
                        row["unsubscribed_date"],
                        row["opens"],
                        row["clicks"],
                        row["complaint"],
                        row["soft_bounce"],
                        row["hard_bounce"],
                        row["created_at"]
                    )
                    for row in loops_campaign_recipient_metrics.iter_rows(named=True)
                ]
                
                # Upsert metrics
                execute_values(
                    cursor,
                    """
                    INSERT INTO loops.campaign_metrics (
                        campaign_id, email, delivered_at, unsubscribed_date,
                        opens, clicks, complaint, 
                        soft_bounce, hard_bounce, created_at
                    )
                    VALUES %s
                    ON CONFLICT (campaign_id, email) DO UPDATE SET
                        delivered_at = EXCLUDED.delivered_at,
                        unsubscribed_date = EXCLUDED.unsubscribed_date,
                        opens = EXCLUDED.opens,
                        clicks = EXCLUDED.clicks,
                        complaint = EXCLUDED.complaint,
                        soft_bounce = EXCLUDED.soft_bounce,
                        hard_bounce = EXCLUDED.hard_bounce,
                        created_at = EXCLUDED.created_at
                    """,
                    records,
                    page_size=1000
                )
                
                conn.commit()
                log.info(f"Successfully upserted {len(records)} metrics")
                
                # Update metrics_last_fetched_at timestamp for all campaigns that had metrics written
                campaign_ids = list(set(row["campaign_id"] for row in loops_campaign_recipient_metrics.iter_rows(named=True)))
                if campaign_ids:
                    cursor.execute("""
                        UPDATE loops.campaigns 
                        SET metrics_last_fetched_at = NOW()
                        WHERE id = ANY(%s)
                    """, (campaign_ids,))
                    conn.commit()
                    log.info(f"Updated metrics_last_fetched_at for {len(campaign_ids)} campaigns")
        
        return Output(
            value=None,
            metadata={
                "num_records": loops_campaign_recipient_metrics.height,
                "operation": "upsert with composite key (campaign_id, email)"
            }
        )
    except Exception as e:
        log.error(f"Failed to upsert metrics: {e}")
        raise


@asset(
    compute_kind="sql",
    group_name="loops_campaign_and_metrics_export",
    description="Creates audience mailing list join table by unpivoting mailing list columns from audience table.",
    deps=["loops_audience", "loops_mailing_lists_to_warehouse"]  # Depend on DLT audience asset and mailing lists asset
)
def loops_audience_mailing_lists_to_warehouse(
    context: AssetExecutionContext
) -> Output[None]:
    """
    Creates loops.audience_mailing_lists table by unpivoting mailing list columns.
    Extracts mailing list memberships from audience table and creates normalized join table.
    """
    log = context.log
    import psycopg2
    import tempfile
    import os
    import re
    import csv
    
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set")
    
    log.info("Starting audience mailing lists join table creation")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # Ensure schema exists
                cursor.execute("CREATE SCHEMA IF NOT EXISTS loops")
                
                # Create table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS loops.audience_mailing_lists (
                        email TEXT NOT NULL,
                        mailing_list_id TEXT NOT NULL,
                        PRIMARY KEY (email, mailing_list_id)
                    )
                """)
                
                # Get all mailing lists from the mailing_lists table
                cursor.execute("""
                    SELECT id, friendly_name 
                    FROM loops.mailing_lists 
                    WHERE deletion_status != 'deleted' OR deletion_status IS NULL
                    ORDER BY id
                """)
                
                mailing_lists = cursor.fetchall()
                
                if not mailing_lists:
                    log.warning("No mailing lists found in loops.mailing_lists table")
                    return Output(
                        value=None,
                        metadata={
                            "num_records": 0,
                            "note": "No mailing lists found"
                        }
                    )
                
                log.info(f"Found {len(mailing_lists)} mailing lists")
                
                # Check which mailing list columns actually exist in audience table
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'loops' 
                    AND table_name = 'audience' 
                    AND column_name LIKE 'loops_mailing_list_%'
                    ORDER BY column_name
                """)
                
                existing_columns = {row[0] for row in cursor.fetchall()}
                log.info(f"Found {len(existing_columns)} mailing list columns in audience table")
                
                # Build mapping of mailing list ID to column name
                mailing_list_mapping = {}
                for mailing_list_id, friendly_name in mailing_lists:
                    # Look for any column that starts with loops_mailing_list_{mailing_list_id}_
                    matching_columns = [col for col in existing_columns 
                                     if col.startswith(f"loops_mailing_list_{mailing_list_id}_")]
                    
                    if matching_columns:
                        # Use the first matching column (there should typically be only one)
                        column_name = matching_columns[0]
                        mailing_list_mapping[column_name] = mailing_list_id
                        log.info(f"Found column: {column_name} -> {mailing_list_id} ({friendly_name})")
                    else:
                        log.warning(f"No column found for mailing list: {mailing_list_id} ({friendly_name})")
                        # Log all existing columns for debugging
                        log.debug(f"Available columns: {sorted(existing_columns)}")
                
                if not mailing_list_mapping:
                    log.warning("No valid mailing list columns found")
                    return Output(
                        value=None,
                        metadata={
                            "num_records": 0,
                            "note": "No valid mailing list columns found"
                        }
                    )
                
                log.info(f"Processing {len(mailing_list_mapping)} valid mailing list columns")
                
                # Start transaction
                cursor.execute("BEGIN")
                
                # TRUNCATE table
                cursor.execute("TRUNCATE loops.audience_mailing_lists")
                log.info("Truncated audience_mailing_lists table")
                
                # Process each mailing list column
                total_records = 0
                CHUNK_SIZE = 100_000
                
                for col_name, mailing_list_id in mailing_list_mapping.items():
                    log.info(f"Processing mailing list: {mailing_list_id}")
                    
                    # Query audience data for this mailing list
                    query = f"""
                        SELECT email 
                        FROM loops.audience 
                        WHERE {col_name} = true 
                        AND email IS NOT NULL 
                        AND email != ''
                    """
                    
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    
                    if not rows:
                        log.info(f"No members found for mailing list: {mailing_list_id}")
                        continue
                    
                    # Process in chunks to limit memory usage
                    for i in range(0, len(rows), CHUNK_SIZE):
                        chunk = rows[i:i + CHUNK_SIZE]
                        
                        # Create temporary CSV file
                        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
                            writer = csv.writer(f)
                            for row in chunk:
                                writer.writerow([row[0], mailing_list_id])
                            temp_file = f.name
                        
                        # COPY from file
                        try:
                            with open(temp_file, 'r') as f:
                                cursor.copy_expert(
                                    "COPY loops.audience_mailing_lists (email, mailing_list_id) FROM STDIN WITH CSV",
                                    f
                                )
                            
                            chunk_count = len(chunk)
                            total_records += chunk_count
                            log.info(f"Inserted {chunk_count} records for {mailing_list_id} (chunk {i//CHUNK_SIZE + 1})")
                            
                        finally:
                            # Clean up temp file
                            os.unlink(temp_file)
                
                # Commit transaction
                cursor.execute("COMMIT")
                log.info(f"Successfully created audience mailing lists join table with {total_records} records")
        
        return Output(
            value=None,
            metadata={
                "num_records": total_records,
                "num_mailing_lists": len(mailing_list_mapping),
                "operation": "TRUNCATE + COPY (chunked)"
            }
        )
        
    except Exception as e:
        log.error(f"Failed to create audience mailing lists join table: {e}")
        raise


# Export defs
from dagster import Definitions

defs = Definitions(
    assets=[
        loops_campaign_names_and_ids,
        loops_campaigns_to_warehouse,
        loops_mailing_lists,
        loops_mailing_lists_to_warehouse,
        loops_campaigns_needing_content_fetch,
        loops_campaigns_needing_metrics_fetch,
        loops_campaign_email_contents,
        loops_campaign_contents_to_warehouse,
        loops_campaigns_needing_publishable_content_analysis,
        loops_campaign_publishable_content,
        loops_campaign_publishable_content_to_warehouse,
        loops_campaign_recipient_metrics,
        loops_campaign_metrics_to_warehouse,
        loops_audience_mailing_lists_to_warehouse,
    ]
)
