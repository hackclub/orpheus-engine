#!/usr/bin/env python3
"""
Project search using GPT-5 with Brightdata MCP server for web and Reddit data.

Environment variables required:
- OPENAI_API_KEY: Your OpenAI API key
- BRIGHTDATA_API_TOKEN: Your Brightdata API token for MCP server access
- AIRTABLE_PERSONAL_ACCESS_TOKEN: Your Airtable personal access token

Usage:
1. Set the RECORD_ID variable below to an actual Airtable record ID
2. Run the script to automatically fetch project data and perform OSINT research
3. Results will be saved back to Airtable with comprehensive link data

The script fetches from table tblzWWGUYHVH7Zyqf these fields:
- 'Playable URL' -> live_url
- 'Code URL' -> code_url  
- 'First Name' -> first_name
- 'Last Name' -> last_name
- 'Geocoded - Country' -> author_country
"""

import os
import sys
import time
import requests
import json
import io
from datetime import datetime
from typing import List, Optional, Dict, Any, Literal
from contextlib import redirect_stdout

from dotenv import load_dotenv
from openai import OpenAI
from pyairtable import Api
from pydantic import BaseModel, Field


# MCP server configuration
MCP_SERVER_URL = "https://zachmcp.ngrok.io/mcp"


# Improved Pydantic models for structured response
class Engagement(BaseModel):
    primary_label: Literal[
        'upvotes','points','likes','reviews','stars','reactions','votes','users',
        'views','comments','followers','claps','favorites','subscribers','downloads','installs','other'
    ]
    primary_value: int = Field(ge=0)
    breakdown: Dict[str, int] = Field(default_factory=dict)
    sampled_at_utc: str  # ISO 8601 UTC


class Actor(BaseModel):
    handle: Optional[str] = None          # canonical handle if present
    display_name: Optional[str] = None
    role: Literal['author','team','official_account','third_party','unknown'] = 'unknown'
    profile_url: Optional[str] = None
    followers: Optional[int] = None


class Item(BaseModel):
    title: Optional[str] = None
    url: str
    canonical_url: Optional[str] = None
    platform: str
    kind: Literal['post','profile','article','app_listing','video','forum_thread','directory','other'] = 'post'
    lang: Optional[str] = None
    published_at: Optional[str] = None      # YYYY-MM-DD (preferred; never empty after fallback)
    observed_at_utc: str                    # ISO 8601 UTC (required)
    date_source: Optional[Literal['platform_json','html_time','og_meta','microdata','archive','scraped_text','observed_at']] = None
    date_confidence: Optional[Literal['high','medium','low']] = None

    match_basis: Literal['direct_link','exact_phrase','author_reference','profile','listing','other']
    match_confidence: float = Field(ge=0.0, le=1.0)
    evidence: Optional[str] = None

    author: Optional[Actor] = None
    engagement: Optional[Engagement] = None
    external_id: Optional[str] = None

    mentions_hack_club: Optional[bool] = False
    hack_club_prominence: Optional[int] = 0  # 0-100
    is_hack_club_url: Optional[bool] = False  # Whether this URL is published BY Hack Club

    mirror_urls: List[str] = []
    tags: List[str] = []


class Summary(BaseModel):
    platforms_present: Dict[str, int] = Field(default_factory=dict)  # platform -> item count
    totals: Dict[str, int] = Field(default_factory=dict)             # e.g., total_upvotes, total_likes, total_comments
    notes: Optional[str] = None


class Meta(BaseModel):
    now_utc: str
    live_url: Optional[str] = None
    code_url: Optional[str] = None
    authors: List[Actor] = []
    search_complete: bool
    query_budget: Dict[str, int] = Field(default_factory=dict)  # counts of queries/tool calls
    excluded_domains: List[str] = []
    stop_reason: Optional[str] = None


class Diagnostics(BaseModel):
    notes: Optional[str] = None
    attempts: Dict[str, Dict[str, Any]] = Field(default_factory=dict)     # per-platform attempt stats
    date_report: Dict[str, Any] = Field(default_factory=dict)             # counts by date_source/confidence, missing
    excluded: List[str] = []                                              # URLs/domains excluded


class OSINTResponse(BaseModel):
    meta: Meta
    items: List[Item]
    summary: Summary
    diagnostics: Diagnostics


def fetch_project_from_airtable(record_id: str, airtable_token: str) -> Dict[str, Any]:
    """Fetch project details from Airtable."""
    if not airtable_token:
        raise ValueError("No AIRTABLE_PERSONAL_ACCESS_TOKEN provided")
    
    try:
        # Initialize Airtable API
        api = Api(airtable_token)
        base_id = "app3A5kJwYqxMLOgh"
        table_id = "tblzWWGUYHVH7Zyqf"
        table = api.table(base_id, table_id)
        
        # Fetch the record
        record = table.get(record_id)
        fields = record.get('fields', {})
        
        # Extract the required fields
        project_data = {
            "live_url": fields.get('Playable URL', ''),
            "code_url": fields.get('Code URL', ''),
            "first_name": fields.get('First Name', ''),
            "last_name": fields.get('Last Name', ''),
            "author_country": fields.get('Geocoded - Country', ''),
            "record_id": record_id
        }
        
        print(f"✅ Fetched project data:")
        print(f"   🔗 Live URL: {project_data['live_url']}")
        print(f"   💻 Code URL: {project_data['code_url']}")
        print(f"   👤 Author: {project_data['first_name']} {project_data['last_name']}")
        print(f"   🌍 Country: {project_data['author_country']}")
        
        return project_data
        
    except Exception as e:
        raise ValueError(f"Failed to fetch project from Airtable: {e}")


def write_results_to_airtable(project_links: List[dict], record_id: str, airtable_token: str, 
                             prompt: str = "", reasoning_summary: str = "", full_output_json: str = "") -> List[str]:
    """Write project link results to Airtable table."""
    if not airtable_token:
        print("⚠️ No AIRTABLE_PERSONAL_ACCESS_TOKEN provided, skipping Airtable write")
        return []
    
    if not project_links:
        print("ℹ️ No project links to write to Airtable")
        return []
    
    try:
        # Initialize Airtable API
        api = Api(airtable_token)
        base_id = "app3A5kJwYqxMLOgh"
        table_id = "tbl5SU7OcPeZMaDOs"
        table = api.table(base_id, table_id)
        
        # Prepare records for batch creation
        records_to_create = []
        
        for link in project_links:
            # Format the full JSON with 4-space indentation
            full_json = json.dumps(link, indent=4, ensure_ascii=False)
            
            # Prefer published_at; if missing, write the observed date's YYYY-MM-DD
            date_val = link.get("published_at")
            if not date_val and link.get("observed_at_utc"):
                date_val = link["observed_at_utc"][:10]  # YYYY-MM-DD
            
            # Debug date extraction
            print(f"🔍 Date extraction for {link.get('title', 'No title')[:30]}...")
            print(f"   published_at: {link.get('published_at')}")
            print(f"   observed_at_utc: {link.get('observed_at_utc')}")
            print(f"   final date_val: {date_val}")
            
            # Try to map engagement data to the new Airtable fields
            engagement_count = None
            engagement_type = None
            eng = link.get("engagement") or {}
            if eng:
                engagement_count = eng.get("primary_value")
                engagement_type = eng.get("primary_label")
            
            record_data = {
                "Date": date_val,
                "Source": link.get("platform"),
                "Headline": link.get("title"),
                "URL": link.get("canonical_url") or link.get("url"),
                "Engagement Count": engagement_count,
                "Engagement Type": engagement_type,
                "Mentions Hack Club?": link.get("mentions_hack_club", False),
                "Is Hack Club URL?": link.get("is_hack_club_url", False),
                "YSWS Approved Project": [record_id] if record_id else [],
                "Full JSON": full_json,
                "Full Prompt": prompt,
                "Full Prompt Reasoning Summary": reasoning_summary,
                "Full Prompt Output JSON": full_output_json
            }
            records_to_create.append(record_data)
        
        # Write to Airtable in batches (max 10 per batch)
        batch_size = 10
        total_created = 0
        created_ids: List[str] = []
        
        for i in range(0, len(records_to_create), batch_size):
            batch = records_to_create[i:i + batch_size]
            created_records = table.batch_create(batch)
            created_ids.extend([r["id"] for r in created_records])
            total_created += len(created_records)
            print(f"📝 Created {len(created_records)} Airtable records (batch {i//batch_size + 1})")
        
        print(f"✅ Successfully wrote {total_created} records to Airtable")
        return created_ids
        
    except Exception as e:
        print(f"❌ Error writing to Airtable: {e}")
        return []


def write_search_record_to_airtable(
    project_record_id: str,
    mention_ids: List[str],
    full_output_log: str,
    output_json: str,
    prompt: str,
    token_usage: dict,
    total_cost_usd: float,
    airtable_token: str
) -> None:
    """
    Create a "search run" record and link it to the YSWS project +
    mention records.
    """
    if not airtable_token:
        print("⚠️ No AIRTABLE_PERSONAL_ACCESS_TOKEN provided – skipping search-record write")
        return

    try:
        api = Api(airtable_token)
        base_id = "app3A5kJwYqxMLOgh"
        table_id = "tblfU7k0cgzysujpH"          # <- new table
        table = api.table(base_id, table_id)

        record_data = {
            "Project": [project_record_id] if project_record_id else [],
            "Found Project Mentions": mention_ids,                  # link field
            "Output JSON": output_json,
            "Full Output Log": full_output_log,
            "Prompt": prompt,
            "Model": "gpt-5",
            "Non-Cached Input Tokens": token_usage.get("non_cached_tokens", 0),
            "Cached Input Tokens": token_usage.get("cached_tokens", 0),
            "Output Tokens": token_usage.get("output_tokens", 0),
            "Estimated Cost": total_cost_usd
        }

        created_record = table.create(record_data)
        print(f"✅ Search record written to Airtable: {created_record['id']}")
        print(f"💰 Estimated cost: ${total_cost_usd:.4f}")

    except Exception as e:
        print(f"❌ Error writing search record: {e}")


if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) != 2:
        print("❌ Usage: python ysws_mcp_test_clean.py <RECORD_ID>")
        print("   Example: python ysws_mcp_test_clean.py rectC2FMjtHeP4Xy0")
        sys.exit(1)
    
    RECORD_ID = sys.argv[1]
    print(f"🎯 Using record ID: {RECORD_ID}")
    
    # Setup
    load_dotenv()
    API_KEY = os.getenv("OPENAI_API_KEY")
    BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN")
    AIRTABLE_TOKEN = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    
    if not API_KEY:
        raise SystemExit("❌ OPENAI_API_KEY not found in .env")
    
    if not BRIGHTDATA_TOKEN:
        print("⚠️ BRIGHTDATA_API_TOKEN not found - MCP server access will not work")
    else:
        print("✅ Brightdata API token found")
    
    # Set up client
    client = OpenAI(api_key=API_KEY)
    
    # Fetch project data from Airtable
    print(f"📋 Fetching project data for record: {RECORD_ID}")
    try:
        PROJECT_INPUTS = fetch_project_from_airtable(RECORD_ID, AIRTABLE_TOKEN)
    except Exception as e:
        print(f"❌ {e}")
        exit(1)
    
    # Generate schema-based prompt components
    def generate_example_json_from_schema() -> str:
        """Generate an example JSON structure from our Pydantic models."""
        example = OSINTResponse(
            meta=Meta(
                now_utc="2025-08-25T12:00:00Z",
                live_url=PROJECT_INPUTS['live_url'],
                code_url=PROJECT_INPUTS['code_url'],
                authors=[Actor(handle="example_user", display_name="Example User", role="author")],
                search_complete=True,
                query_budget={"google": 10, "reddit": 5},
                stop_reason="complete"
            ),
            items=[
                Item(
                    title="Example Title",
                    url="https://example.com/post", 
                    canonical_url="https://example.com/post",
                    platform="Reddit",
                    kind="post",
                    lang="en",
                    published_at="2025-08-25",
                    observed_at_utc="2025-08-25T12:00:00Z",
                    date_source="platform_json",
                    date_confidence="high",
                    match_basis="direct_link",
                    match_confidence=1.0,
                    evidence="Links to the project repository",
                    author=Actor(handle="example_user", display_name="Example User", role="third_party"),
                    engagement=Engagement(
                        primary_label="upvotes",
                        primary_value=123,
                        breakdown={"upvotes": 123, "comments": 45},
                        sampled_at_utc="2025-08-25T12:00:00Z"
                    ),
                    external_id="123",
                    mirror_urls=[],
                    tags=["launch"]
                )
            ],
            summary=Summary(
                platforms_present={"Hacker News": 1},
                totals={"points": 120, "comments": 45},
                notes=None
            ),
            diagnostics=Diagnostics(
                notes="Found 3 platforms with mentions",
                attempts={"reddit": {"queries_run": 5, "hits_seen": 3, "items_included": 1}},
                date_report={"platform_json": 2, "html_time": 1, "missing": 0}
            )
        )
        return example.model_dump_json(indent=2)
    
    print("🔍 Querying GPT-5 with Brightdata MCP server tools...")
    start_time = time.time()
    
    # Prepare authorization header outside of f-string to avoid nesting
    auth_header = f"Bearer {BRIGHTDATA_TOKEN}"
    
    # Create prompt with variables filled in manually until SDK supports saved prompts
    example_json = generate_example_json_from_schema()
    pydantic_schema = OSINTResponse.model_json_schema()
    
    prompt_content = f"""
You are an OSINT link‑discovery agent. Your job: find where a given project has been shared or discussed online and return a single JSON object that exactly matches the Pydantic schema at the end.

Focus on **viral surface area** (Reddit, Hacker News, X/Twitter, YouTube, Bluesky, Mastodon, Product Hunt, forums, blogs, news, app stores, itch.io, AUR, Chrome Web Store, Firefox Add-ons, etc.) and **official social profiles (for the project, not for the author)** for follower counts. Exclude SEO scrapings and bot mirrors.

Return **JSON only** (no prose, no markdown). Must validate against the attached schema. Make conservative calls; only include links clearly about **this** project.

--------------------------------
INPUTS (provided above by the caller)
- LIVE_URL: {PROJECT_INPUTS['live_url']}
- CODE_URL: {PROJECT_INPUTS['code_url']}
- AUTHOR: {PROJECT_INPUTS['first_name']} {PROJECT_INPUTS['last_name']}
- AUTHOR_COUNTRY: {PROJECT_INPUTS['author_country']}
- MAX_RESULTS: 400

--------------------------------
WHAT COUNTS AS A HIT
Include an item if **any** is true (store a short `evidence` quote/phrase and set `match_basis`):
1) **direct_link** – the page links the LIVE_URL/CODE_URL (or a clear repo‑family variant). `match_confidence=1.0`
2) **exact_phrase** – a distinctive title/slogan/identifier unique to the project. `match_confidence≈0.8`
3) **author_reference** – a known author/team account posts about the project. `match_confidence≈0.6`
4) **profile** – official social profile of the project (for follower counts). `match_basis=profile`

--------------------------------
EXCLUDE (hard)
- Autogenerated directories / SEO mirrors (e.g., AlternativeTo, many "/free‑alternatives/" sites)
- Bot repost accounts (e.g., "HN to Twitter bot" timelines)
- The author asking general **implementation** questions (e.g., StackOverflow) without announcing or showcasing the project
- Pure mirrors of your own site/repo (self‑scope pages); third‑party posts that link to you are allowed

--------------------------------
SEARCH PLAN (iterate up to 5 rounds or until <3 new unique items are found twice in a row)
1) **Fingerprint** LIVE_URL/CODE_URL:
   - Extract title/H1, og:title/description, canonical URL, owner/repo, project name, obvious synonyms/morphology (hyphenation/spaces/case), distinctive phrases, author/handle(s).
   - Add author handles you discover during search to `meta.authors`.
2) **Broad search** with Google **and** Bing queries mixing: project name, repo slug, owner/slug, LIVE_URL host, synonyms, "launch", "show", "WIP", "review", "prototype", etc.
3) **Platform‑native** passes:
   - Reddit: site queries + permalink `.json` + `/user/<handle>/submitted.json` + `/domain/<live-host>/new.json` and `/domain/<code-host>/new.json`.
   - Hacker News: item page or JSON; capture points/comments.
   - X/Twitter, YouTube, Bluesky, Mastodon: fetch permalinks and parse visible `<time datetime>`/embedded JSON for dates/engagement when possible.
   - Product Hunt + App/Play Stores for listings (use reviews/ratings as engagement).
   - AUR (Arch User Repository): AUR packages are valid project distributions - include them with vote counts.
   - Chrome Web Store & Firefox Add-ons: Browser extensions are valid - get user counts, not review counts.
   - Package managers (npm, PyPI, etc.): Valid distributions with download/install metrics.
4) **Author sweep** (run once before stopping): search each known author/official handle on Reddit, X, YouTube for project posts.
5) If a page is gated or fails to load, try an alternative path (old or mobile domains/AMP) or use snippet evidence; record this in `diagnostics.notes`.

Use MCP web tools when available (search engine, platform‑specific fetchers). If a specialized tool fails, fall back to a generic page fetch.

--------------------------------
DATES (keep it simple & consistent)
- `published_at` is **preferred** (YYYY‑MM‑DD), `observed_at_utc` is **required** (ISO UTC).
- Choose the first available:
  1) Platform JSON/LD (e.g., `created_utc`, `publishedAt`, `uploadDate`)
  2) `<time datetime>` or `article:published_time`/`og:updated_time`
  3) Archive earliest snapshot date for the **exact** canonical URL
  4) As a last resort, set `published_at` to `observed_at_utc`'s date with `date_source="observed_at"` and `date_confidence="low"`
- Record `date_source` and `date_confidence`. Never leave `published_at` blank—use the fallback.

--------------------------------
ENGAGEMENT (normalize)
- Parse compact numerals ("1.2K", "3.4M") to integers. No strings.
- Fill `engagement.primary_label`, `engagement.primary_value`, and a `breakdown` dict for all counts found (e.g., `{{"upvotes": 120, "comments": 34}}`).
- `engagement.sampled_at_utc` is required.

**Primary label guidance**:
- Reddit: `upvotes`
- Hacker News: `points`
- X/Twitter: `likes` (also record `retweets`/`reposts` in `breakdown`)
- YouTube: `views` (record `likes`, `comments` too in breakdown)
- Chrome Web Store: `users` (record `rating` if available)
- Firefox Add-ons: `users` (record `rating` if available)
- AUR (Arch User Repository): `votes` (AUR packages are valid and should be included)
- App/Play Store: `reviews` (record `rating` if available)
- Profiles: `followers`

--------------------------------
HACK CLUB URL DETECTION
For each item, determine:
- **mentions_hack_club**: true/false if the content mentions Hack Club organization
- **hack_club_prominence**: 0-100 score for how prominently Hack Club is mentioned
- **is_hack_club_url**: true/false if this URL is published BY Hack Club (examples: summer.hackclub.com, highway.hackclub.com, *.hackclub.com, github.com/hackclub/*, hackclub.slack.com, etc.)

--------------------------------
CANONICALIZATION & DEDUPE
- Use `rel=canonical` > `og:url` > resolved URL (strip tracking).  
- Dedupe by `canonical_url` (keep the highest‑engagement instance). Record alternates in `mirror_urls`.

--------------------------------
DIAGNOSTICS & QUALITY CHECKS (required)
- `diagnostics.attempts` per key platform with: `queries_run[]`, `native_used` (bool), `authors_enumerated` (int), `hits_seen` (int), `items_included` (int).
- `diagnostics.date_report`: counts by `date_source` and `date_confidence`, and `missing` (should be 0).
- If SERPs reveal a platform but no valid permalinks included, add `diagnostics.notes` explaining why (duplicates/ambiguous/removed).
- `meta.stop_reason` = "max_rounds" | "stalled" | "budget" | "complete".

--------------------------------
OUTPUT
- Return JSON ONLY that validates against the schema below.
- If no results: `items: []`, `summary.totals` empty, `meta.search_complete=true`, with an explanatory note in diagnostics.

--------------------------------
PYDANTIC SCHEMA (AUTHORITATIVE)
{json.dumps(pydantic_schema, indent=2)}

--------------------------------
EXAMPLE JSON FORMAT
{example_json}
"""

    # API parameters (MCP server tools with structured output)
    base_params = {
        "model": "gpt-5",
        "input": [
            {"role": "developer", "content": prompt_content}
        ],
        "tools": [
            {
                "type": "mcp",
                "server_label": "web",
                "server_url": MCP_SERVER_URL,
                "require_approval": "never",
                "headers": {
                    "Authorization": auth_header
                }
            }
        ],
        "reasoning": {"effort": "high", "summary": "auto"},
        "text": {"verbosity": "low"},   # keep prose short, budget for search
        "max_output_tokens": 15000
    }
    
    # Make the API call using Responses API with streaming + MCP tools
    log_buffer = io.StringIO()
    
    # Capture execution output (but we'll also print to console for real-time feedback)
    print("💡 Using streaming with MCP tools")
    stream = client.responses.create(**base_params, stream=True)
    
    # Process streaming events
    print(f"\n🔄 Streaming response events...")
    response = None
    reasoning_summary = ""
    full_output_text = ""
    tool_calls = []  # Store all tool call information
    reasoning_content = ""  # Track reasoning content
    reasoning_started_printed = False  # Track if we printed reasoning header
    current_tool_name = "unknown"  # Track current tool being called
    
    # Initialize usage tracking variables
    usage = {}
    total_cost = 0.0
    
    for event in stream:
        event_type = getattr(event, 'type', 'unknown')
        
        if event_type == 'response.created':
            print(f"✅ Response created (ID: {getattr(event, 'id', 'unknown')})")
            response = event
            
        elif event_type == 'response.output_text.delta':
            # Stream the text output as it comes
            text_delta = getattr(event, 'delta', '')
            print(text_delta, end='', flush=True)
            full_output_text += text_delta
            
        elif event_type == 'response.reasoning_summary_text.delta':
            # Accumulate reasoning text and only print if it has content
            reasoning_delta = getattr(event, 'delta', '')
            if reasoning_delta.strip():  # Only process if there's actual content
                reasoning_content += reasoning_delta
                if not reasoning_started_printed:
                    print(f"\n\n🧠 === REASONING ===")
                    reasoning_started_printed = True
                print(reasoning_delta, end='', flush=True)
            
        elif event_type == 'response.mcp_call_arguments.done':
            # MCP tool call arguments completed - use tracked tool name
            empty_args = '{}'
            tool_args = getattr(event, 'arguments', empty_args)
            print(f"\n🔧 {current_tool_name}: {tool_args}")
            
        elif event_type == 'response.mcp_call.completed':
            # MCP tool call completed - show the full response
            print(f"\n📊 {current_tool_name} completed:")
                
        elif event_type == 'response.mcp_call.failed':
            # MCP tool call failed - show the full error
            print(f"\n❌ {current_tool_name} failed:")
            
            # Debug: show the full event object
            print(f"   🔍 Raw event object: {event}")
            print(f"   📋 Event dict: {event.__dict__ if hasattr(event, '__dict__') else 'No __dict__'}")
            
            # Try to access error through model methods if it's a Pydantic object
            if hasattr(event, 'model_dump'):
                event_data = event.model_dump()
                print(f"   💥 Pydantic model data: {json.dumps(event_data, indent=2, default=str)}")
            
            # Try common error field names
            for field_name in ['error', 'message', 'details', 'exception', 'error_message']:
                if hasattr(event, field_name):
                    field_value = getattr(event, field_name)
                    if field_value is not None:
                        print(f"   💥 Found in {field_name}: {json.dumps(field_value, indent=2, default=str) if isinstance(field_value, (dict, list)) else str(field_value)}")
            
        elif event_type == 'response.output_item.added':
            item = getattr(event, 'item', None)
            if item:
                item_type = getattr(item, 'type', 'unknown')
                if item_type == 'reasoning':
                    # Reset reasoning tracking for new reasoning block
                    reasoning_content = ""
                    reasoning_started_printed = False
                elif item_type in ['mcp_call', 'tool_call']:
                    tool_name = getattr(item, 'name', 'unknown')
                    current_tool_name = tool_name  # Track the tool name
                    print(f"\n\n🔧 === CALLING {tool_name.upper()} ===")
                    
        elif event_type == 'response.output_item.done':
            item = getattr(event, 'item', None) 
            if item:
                item_type = getattr(item, 'type', 'unknown')
                if item_type == 'reasoning':
                    # End reasoning only if we had content
                    if reasoning_started_printed:
                        print(f"\n=== REASONING ENDED ===\n")
                elif item_type in ['mcp_call', 'tool_call']:
                    tool_name = getattr(item, 'name', 'unknown')
                    print(f"\n=== {tool_name.upper()} COMPLETED ===\n")
                
        elif event_type == 'response.completed':
            print(f"\n\n✅ Response completed!")
            response = event
            end_time = time.time()
            duration = end_time - start_time
            
            # Extract usage information from the completed response event
            if hasattr(event, 'response') and hasattr(event.response, 'usage') and event.response.usage:
                response_usage = event.response.usage
                print(f"📊 Usage from completed event: {response_usage}")
                
                # Store usage for later use
                input_tokens = getattr(response_usage, 'input_tokens', 0)
                output_tokens = getattr(response_usage, 'output_tokens', 0)
                cached_tokens = 0
                
                # Get cached token count from input_tokens_details
                if hasattr(response_usage, 'input_tokens_details'):
                    cached_tokens = getattr(response_usage.input_tokens_details, 'cached_tokens', 0)
                
                non_cached_tokens = input_tokens - cached_tokens
                
                usage = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cached_tokens": cached_tokens,
                    "non_cached_tokens": non_cached_tokens,
                    "total_tokens": input_tokens + output_tokens,
                }
                
                # Calculate costs using GPT-5 pricing
                non_cached_cost = non_cached_tokens * 1.25 / 1_000_000
                cached_cost = cached_tokens * 0.125 / 1_000_000
                output_cost = output_tokens * 10.0 / 1_000_000
                total_cost = non_cached_cost + cached_cost + output_cost
                print(f"💰 Estimated cost: ${total_cost:.4f}")
            else:
                print("⚠️ No usage information found in completed event")
            
            # Debug: Show full response output array for MCP results
            if hasattr(response, 'output') and response.output:
                print(f"\n🔍 Response output array ({len(response.output)} items):")
                for i, item in enumerate(response.output):
                    item_type = getattr(item, 'type', 'unknown')
                    print(f"   Item {i+1}: type='{item_type}'")
                    
                    if item_type == 'mcp_call':
                        # Show MCP call details
                        tool_name = getattr(item, 'name', 'unknown')
                        tool_result = getattr(item, 'result', None)
                        if tool_result:
                            print(f"   📊 MCP {tool_name} result:")
                            if isinstance(tool_result, dict):
                                result_str = json.dumps(tool_result, indent=4)
                                if len(result_str) > 2000:
                                    print(f"      {result_str[:2000]}\n      ... [truncated]")
                                else:
                                    print(f"      {result_str}")
                            else:
                                print(f"      {str(tool_result)}")
                        else:
                            print(f"   📊 MCP {tool_name}: No result data")
                    
                    elif item_type == 'message':
                        # Show message content
                        if hasattr(item, 'content'):
                            print(f"   📝 Message content preview: {str(item.content)[:200]}...")
                    
                    else:
                        # Show other item types
                        if hasattr(item, 'model_dump'):
                            item_data = item.model_dump()
                            print(f"   📄 {item_type} data: {json.dumps(item_data, indent=2, default=str)}")
            else:
                print(f"\n🔍 No output array found in response")
            
        elif event_type == 'response.failed':
            print(f"\n❌ Response failed: {getattr(event, 'error', 'Unknown error')}")
            end_time = time.time()
            duration = end_time - start_time
            
        elif event_type == 'error':
            print(f"\n❌ Error: {event}")
            end_time = time.time() 
            duration = end_time - start_time
    
    # Final summary
    if not response:
        print("❌ No final response received")
        exit(1)
        
    duration = end_time - start_time if 'end_time' in locals() else 0
    
    print(f"\n📊 Final Summary:")
    print(f"⏱️ Duration: {duration:.2f} seconds")
    
    # Token usage and cost information already extracted during response.completed event
    
    # Print reasoning summary if found
    if reasoning_summary:
        print(f"\n🧠 Reasoning Summary:")
        print("=" * 50)
        print(reasoning_summary)
        print("=" * 50)
    
    print(f"\n📦 Complete Output:")
    final_output = ""
    if full_output_text:
        final_output = full_output_text
        print(full_output_text)
    elif hasattr(response, 'output_text') and response.output_text:
        final_output = response.output_text
        print(response.output_text)
    else:
        print("No output text found")
    
    # Parse output with Pydantic for structured data
    structured_response = None
    if final_output:
        print(f"\n🔧 Parsing output with Pydantic models...")
        try:
            import json
            parsed_json = json.loads(final_output)
            structured_response = OSINTResponse(**parsed_json)
            print("✅ Successfully parsed structured response")
            
            # Show structured summary
            print(f"\n📊 Structured Summary:")
            print(f"   🔗 Found {len(structured_response.items)} items")
            print(f"   🏁 Search complete: {structured_response.meta.search_complete}")
            if structured_response.summary.platforms_present:
                top_platform = max(structured_response.summary.platforms_present.items(), key=lambda kv: kv[1])[0]
                print(f"   🗺️ Top platform by count: {top_platform}")
            if structured_response.items:
                first = structured_response.items[0]
                print(f"   ⭐ Example item: {first.title[:60] if first.title else first.url}")
            
        except json.JSONDecodeError as json_error:
            print(f"⚠️ JSON parsing failed: {json_error}")
            print("🔧 Asking LLM to fix JSON format in existing thread...")
            
            # Get the complete Pydantic schema
            expected_schema = OSINTResponse.model_json_schema()
            
            # Continue the existing conversation to fix JSON
            fix_response = client.responses.create(
                model="gpt-5",
                input=[
                    {"role": "user", "content": f"""
The JSON output you provided failed to parse. Please fix the syntax errors and return valid JSON that matches this exact schema:

JSON Parse Error: {json_error}

Expected Pydantic Schema:
{json.dumps(expected_schema, indent=2)}

Your original output that failed:
{final_output}

Please return ONLY the corrected JSON that validates against the schema above, with no additional text or formatting.
"""}
                ],
                previous_response_id=getattr(response, 'id', None),  # Continue existing thread
                max_output_tokens=len(final_output) + 1000
            )
            
            if hasattr(fix_response, 'output_text') and fix_response.output_text:
                print("✅ Got corrected JSON from LLM")
                try:
                    fixed_json = json.loads(fix_response.output_text)
                    structured_response = OSINTResponse(**fixed_json)
                    print("✅ Successfully parsed corrected structured response")
                    final_output = fix_response.output_text  # Use corrected version
                except Exception as fix_error:
                    print(f"❌ Corrected JSON still failed to parse: {fix_error}")
            
        except Exception as pydantic_error:
            print(f"⚠️ Pydantic validation failed: {pydantic_error}")
            print("🔧 Asking LLM to fix data structure in existing thread...")
            
            # Get the complete Pydantic schema
            expected_schema = OSINTResponse.model_json_schema()
            
            # Continue the existing conversation to fix data structure
            fix_response = client.responses.create(
                model="gpt-5",
                input=[
                    {"role": "user", "content": f"""
Your JSON output has the correct syntax but failed Pydantic validation. Please fix the data structure to match this exact schema:

Pydantic Validation Error: {pydantic_error}

Expected Pydantic Schema:
{json.dumps(expected_schema, indent=2)}

Your original output that failed validation:
{final_output}

Please return ONLY the corrected JSON with the proper data structure that validates against the Pydantic schema above.
"""}
                ],
                previous_response_id=getattr(response, 'id', None),  # Continue existing thread
                max_output_tokens=len(final_output) + 1000
            )
            
            if hasattr(fix_response, 'output_text') and fix_response.output_text:
                print("✅ Got structure-corrected JSON from LLM")
                try:
                    fixed_json = json.loads(fix_response.output_text)
                    structured_response = OSINTResponse(**fixed_json)
                    print("✅ Successfully parsed structure-corrected response")
                    final_output = fix_response.output_text  # Use corrected version
                except Exception as fix_error:
                    print(f"❌ Structure-corrected JSON still failed: {fix_error}")
    
    # Create a basic execution log for record keeping
    full_output_log = f"""Search executed at {datetime.now().isoformat()}
Project: {PROJECT_INPUTS.get('record_id', '')}
Live URL: {PROJECT_INPUTS.get('live_url', '')}
Code URL: {PROJECT_INPUTS.get('code_url', '')}
Author: {PROJECT_INPUTS.get('first_name', '')} {PROJECT_INPUTS.get('last_name', '')}
Duration: {duration:.2f} seconds
Model: gpt-5
Final output length: {len(final_output) if final_output else 0} characters
Structured response items: {len(structured_response.items) if structured_response else 0}
"""
    
    # Write to Airtable using structured data
    mention_ids = []
    if structured_response and AIRTABLE_TOKEN:
        print(f"\n💾 Writing structured results to Airtable...")
        try:
            # Convert Pydantic models to dictionaries for Airtable
            project_items = [item.model_dump() for item in structured_response.items]
            
            if project_items:
                mention_ids = write_results_to_airtable(
                    project_items,
                    PROJECT_INPUTS.get("record_id", ""),
                    AIRTABLE_TOKEN,
                    prompt=prompt_content,  # Use actual full prompt text
                    reasoning_summary=reasoning_content,  # Use accumulated reasoning
                    full_output_json=final_output
                )
            else:
                print("ℹ️ No project links found in structured response")
                mention_ids = []
                
        except Exception as e:
            print(f"❌ Error processing structured Airtable write: {e}")
            mention_ids = []
    elif final_output and AIRTABLE_TOKEN:
        # Fallback to old parsing method if structured parsing failed
        print(f"\n💾 Fallback: Writing raw results to Airtable...")
        try:
            import json
            parsed_output = json.loads(final_output)
            project_links = parsed_output.get("project_links", [])
            if project_links:
                mention_ids = write_results_to_airtable(
                    project_links,
                    PROJECT_INPUTS.get("record_id", ""),
                    AIRTABLE_TOKEN,
                    prompt=prompt_content,  # Use actual full prompt text
                    reasoning_summary=reasoning_content,  # Use accumulated reasoning
                    full_output_json=final_output
                )
            else:
                print("ℹ️ No project_links found in raw output")
                mention_ids = []
        except Exception as e:
            print(f"❌ Error with fallback Airtable write: {e}")
            mention_ids = []
    elif not AIRTABLE_TOKEN:
        print("⚠️ No AIRTABLE_PERSONAL_ACCESS_TOKEN - skipping Airtable write")
        mention_ids = []
    
    # Create search record to track this execution
    if AIRTABLE_TOKEN:
        write_search_record_to_airtable(
            project_record_id=PROJECT_INPUTS.get("record_id", ""),
            mention_ids=mention_ids,
            full_output_log=full_output_log,
            output_json=final_output or "",
            prompt=prompt_content,
            token_usage=usage,
            total_cost_usd=total_cost,
            airtable_token=AIRTABLE_TOKEN
        )
    
    print(f"\n✅ Done at {datetime.now().isoformat()}")
