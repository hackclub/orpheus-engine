"""Dagster assets for Loops campaign and metrics export."""

import time
import requests
import polars as pl
import json
import os
import re
import dlt
from dlt.destinations import postgres
from datetime import datetime
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any, Optional

# Loops API Configuration
LOOPS_BASE_URL = "https://app.loops.so/api/trpc"
LOOPS_APP_BASE_URL = "https://app.loops.so"
LOOPS_TITLES_ENDPOINT = "/teams.getTitlesOfEmails"
LOOPS_EMAIL_BY_ID_ENDPOINT = "/emailMessages.getEmailMessageById"
LOOPS_METRICS_ENDPOINT = "/emailMessages.calculateEmailMessageMetrics"
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
      - props.pageProps.campaign.scheduling.data.timestamp (Unix ms timestamp)
    
    Returns dict with: email_message_id, status, sent_at_timestamp (or None if not found)
    """
    path = LOOPS_COMPOSE_PATH_TEMPLATE.format(campaignId=campaign_id)
    html = _http_get_html(path, session_token, LOOPS_COMPOSE_QUERYSTRING)
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        data = json.loads(m.group("json"))
        campaign_data = (
            data.get("props", {})
                .get("pageProps", {})
                .get("campaign", {})
        )
        
        msg_id = campaign_data.get("emailMessage", {}).get("id")
        if not msg_id or not isinstance(msg_id, str):
            return None
        
        status = campaign_data.get("status", "")
        sent_at_timestamp = campaign_data.get("scheduling", {}).get("data", {}).get("timestamp")
        
        return {
            "email_message_id": msg_id,
            "status": status,
            "sent_at_timestamp": sent_at_timestamp  # Unix timestamp in milliseconds
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
                        last_enriched_at TIMESTAMP WITH TIME ZONE
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
    group_name="loops_campaign_and_metrics_export",
    compute_kind="sql",
    description="Queries warehouse for campaigns that need enrichment based on sent status and time since last enrichment.",
    deps=["loops_campaigns_to_warehouse"]  # Ensure warehouse is updated first
)
def loops_campaigns_needing_enrichment(
    context: AssetExecutionContext
) -> pl.DataFrame:
    """
    Queries warehouse for campaigns that need enrichment.
    Update frequency depends on email status:
    - Not sent yet: every 12 hours
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
    
    # Build query with time-based enrichment logic
    query = """
        SELECT id, name, emoji
        FROM loops.campaigns
        WHERE 
            -- Never enriched
            last_enriched_at IS NULL
            OR
            -- Email not sent yet: update every 12 hours
            ((sent_at IS NULL OR status != 'sent') 
             AND last_enriched_at < NOW() - INTERVAL '12 hours')
            OR
            -- Sent less than 1 week ago: update every 24 hours
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at >= NOW() - INTERVAL '1 week' 
             AND last_enriched_at < NOW() - INTERVAL '24 hours')
            OR
            -- Sent between 2 and 8 weeks ago: update every 7 days
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at < NOW() - INTERVAL '2 weeks' 
             AND sent_at >= NOW() - INTERVAL '8 weeks'
             AND last_enriched_at < NOW() - INTERVAL '7 days')
            OR
            -- Sent between 8 weeks and 1 year ago: update monthly (30 days)
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at < NOW() - INTERVAL '8 weeks'
             AND sent_at >= NOW() - INTERVAL '1 year'
             AND last_enriched_at < NOW() - INTERVAL '30 days')
            OR
            -- Sent more than 1 year ago: update every 90 days
            (sent_at IS NOT NULL AND status = 'sent' 
             AND sent_at < NOW() - INTERVAL '1 year'
             AND last_enriched_at < NOW() - INTERVAL '90 days')
        ORDER BY last_enriched_at NULLS FIRST
    """
    
    if DEBUG_ENRICHMENT_LIMIT > 0:
        query += f"\n        LIMIT {DEBUG_ENRICHMENT_LIMIT}"
        log.info(f"Debug mode: Limiting campaigns to {DEBUG_ENRICHMENT_LIMIT}")
    else:
        log.info("No limit on campaigns (processing all that need enrichment)")
    
    try:
        with psycopg2.connect(conn_string) as conn:
            df = pl.read_database(query, connection=conn)
        
        log.info(f"Found {df.height} campaigns needing enrichment")
        
        context.add_output_metadata(
            metadata={
                "num_campaigns_needing_enrichment": df.height,
                "sample_campaigns": MetadataValue.md(
                    "\n".join([f"- {row['name']} (`{row['id']}`)" 
                              for row in df.head(5).to_dicts()])
                ) if df.height > 0 else MetadataValue.text("No campaigns need enrichment")
            }
        )
        
        return df
    except Exception as e:
        log.error(f"Failed to query warehouse for campaigns needing enrichment: {e}")
        raise


@asset(
    group_name="loops_campaign_and_metrics_export",
    required_resource_keys={"loops_session_token"},
    compute_kind="loops_trpc",
    description="Scrapes email content from Loops compose HTML for campaigns needing enrichment."
)
def loops_campaign_email_contents(
    context: AssetExecutionContext,
    loops_campaigns_needing_enrichment: pl.DataFrame
) -> pl.DataFrame:
    """
    Scrapes email content from Loops compose HTML for each campaign.
    Returns DataFrame with campaign ID and email content fields.
    """
    log = context.log
    session_token = context.resources.loops_session_token.get_value()
    
    if loops_campaigns_needing_enrichment.height == 0:
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
            "soft_bounces": []
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
            "soft_bounces": pl.Int64
        })
    
    records = []
    success_count = 0
    error_count = 0
    
    for row in loops_campaigns_needing_enrichment.iter_rows(named=True):
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
                "soft_bounces": j.get("softBounces")
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
            "soft_bounces": []
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
            "soft_bounces": pl.Int64
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
    description="Updates loops.campaigns table with enriched content and sets last_enriched_at timestamp."
)
def loops_campaign_contents_to_warehouse(
    context: AssetExecutionContext,
    loops_campaign_email_contents: pl.DataFrame
) -> Output[None]:
    """
    Upserts enriched campaign content to loops.campaigns table.
    Updates all enrichment fields and sets last_enriched_at = NOW().
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
                        last_enriched_at
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
                        last_enriched_at = EXCLUDED.last_enriched_at
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
    loops_campaigns_needing_enrichment: pl.DataFrame
) -> pl.DataFrame:
    """
    Fetches recipient-level metrics for each campaign.
    Returns DataFrame with one row per campaign per recipient.
    """
    log = context.log
    session_token = context.resources.loops_session_token.get_value()
    
    if loops_campaigns_needing_enrichment.height == 0:
        log.info("No campaigns to fetch metrics for, returning empty DataFrame")
        return pl.DataFrame({
            "campaign_id": [],
            "email_message_id": [],
            "contact_id": [],
            "email": [],
            "first_name": [],
            "last_name": [],
            "vendor_delivered_at": [],
            "unsubscribed_date": [],
            "vendor_open": [],
            "vendor_click": [],
            "vendor_complaint": [],
            "vendor_soft_bounce": [],
            "vendor_hard_bounce": [],
            "created_at": []
        }, schema={
            "campaign_id": pl.Utf8,
            "email_message_id": pl.Utf8,
            "contact_id": pl.Utf8,
            "email": pl.Utf8,
            "first_name": pl.Utf8,
            "last_name": pl.Utf8,
            "vendor_delivered_at": pl.Datetime,
            "unsubscribed_date": pl.Datetime,
            "vendor_open": pl.Int64,
            "vendor_click": pl.Int64,
            "vendor_complaint": pl.Boolean,
            "vendor_soft_bounce": pl.Boolean,
            "vendor_hard_bounce": pl.Boolean,
            "created_at": pl.Datetime
        })
    
    all_records = []
    total_recipients = 0
    campaigns_processed = 0
    
    for row in loops_campaigns_needing_enrichment.iter_rows(named=True):
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
                    vendor_delivered_at = None
                    if email_data.get("vendorDeliveredAt"):
                        try:
                            vendor_delivered_at = datetime.fromisoformat(
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
                        "email_message_id": email_message_id,
                        "email": email_data.get("email", ""),
                        "first_name": email_data.get("firstName") or "",
                        "last_name": email_data.get("lastName") or "",
                        "vendor_delivered_at": vendor_delivered_at,
                        "unsubscribed_date": unsubscribed_date,
                        "vendor_open": email_data.get("vendorOpen", 0),
                        "vendor_click": email_data.get("vendorClick", 0),
                        "vendor_complaint": email_data.get("vendorComplaint", False),
                        "vendor_soft_bounce": email_data.get("vendorSoftBounce", False),
                        "vendor_hard_bounce": email_data.get("vendorHardBounce", False),
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
            "email_message_id": [],
            "email": [],
            "first_name": [],
            "last_name": [],
            "vendor_delivered_at": [],
            "unsubscribed_date": [],
            "vendor_open": [],
            "vendor_click": [],
            "vendor_complaint": [],
            "vendor_soft_bounce": [],
            "vendor_hard_bounce": [],
            "created_at": []
        }, schema={
            "campaign_id": pl.Utf8,
            "email_message_id": pl.Utf8,
            "email": pl.Utf8,
            "first_name": pl.Utf8,
            "last_name": pl.Utf8,
            "vendor_delivered_at": pl.Datetime,
            "unsubscribed_date": pl.Datetime,
            "vendor_open": pl.Int64,
            "vendor_click": pl.Int64,
            "vendor_complaint": pl.Boolean,
            "vendor_soft_bounce": pl.Boolean,
            "vendor_hard_bounce": pl.Boolean,
            "created_at": pl.Datetime
        })
    
    # Create DataFrame with explicit schema to handle None values
    df = pl.DataFrame(all_records, schema={
        "campaign_id": pl.Utf8,
        "email_message_id": pl.Utf8,
        "email": pl.Utf8,
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "vendor_delivered_at": pl.Datetime,
        "unsubscribed_date": pl.Datetime,
        "vendor_open": pl.Int64,
        "vendor_click": pl.Int64,
        "vendor_complaint": pl.Boolean,
        "vendor_soft_bounce": pl.Boolean,
        "vendor_hard_bounce": pl.Boolean,
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
                        email_message_id TEXT,
                        email TEXT NOT NULL,
                        first_name TEXT,
                        last_name TEXT,
                        vendor_delivered_at TIMESTAMP WITH TIME ZONE,
                        unsubscribed_date TIMESTAMP WITH TIME ZONE,
                        vendor_open INTEGER,
                        vendor_click INTEGER,
                        vendor_complaint BOOLEAN,
                        vendor_soft_bounce BOOLEAN,
                        vendor_hard_bounce BOOLEAN,
                        created_at TIMESTAMP WITH TIME ZONE,
                        PRIMARY KEY (campaign_id, email)
                    )
                """)
                
                conn.commit()
                
                # Prepare data for upsert
                records = [
                    (
                        row["campaign_id"],
                        row["email_message_id"],
                        row["email"],
                        row["first_name"],
                        row["last_name"],
                        row["vendor_delivered_at"],
                        row["unsubscribed_date"],
                        row["vendor_open"],
                        row["vendor_click"],
                        row["vendor_complaint"],
                        row["vendor_soft_bounce"],
                        row["vendor_hard_bounce"],
                        row["created_at"]
                    )
                    for row in loops_campaign_recipient_metrics.iter_rows(named=True)
                ]
                
                # Upsert metrics
                execute_values(
                    cursor,
                    """
                    INSERT INTO loops.campaign_metrics (
                        campaign_id, email_message_id, email, 
                        first_name, last_name, vendor_delivered_at, unsubscribed_date,
                        vendor_open, vendor_click, vendor_complaint, 
                        vendor_soft_bounce, vendor_hard_bounce, created_at
                    )
                    VALUES %s
                    ON CONFLICT (campaign_id, email) DO UPDATE SET
                        email_message_id = EXCLUDED.email_message_id,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        vendor_delivered_at = EXCLUDED.vendor_delivered_at,
                        unsubscribed_date = EXCLUDED.unsubscribed_date,
                        vendor_open = EXCLUDED.vendor_open,
                        vendor_click = EXCLUDED.vendor_click,
                        vendor_complaint = EXCLUDED.vendor_complaint,
                        vendor_soft_bounce = EXCLUDED.vendor_soft_bounce,
                        vendor_hard_bounce = EXCLUDED.vendor_hard_bounce,
                        created_at = EXCLUDED.created_at
                    """,
                    records,
                    page_size=1000
                )
                
                conn.commit()
                log.info(f"Successfully upserted {len(records)} metrics")
        
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


# Export defs
from dagster import Definitions

defs = Definitions(
    assets=[
        loops_campaign_names_and_ids,
        loops_campaigns_to_warehouse,
        loops_campaigns_needing_enrichment,
        loops_campaign_email_contents,
        loops_campaign_contents_to_warehouse,
        loops_campaign_recipient_metrics,
        loops_campaign_metrics_to_warehouse,
    ]
)
