#!/usr/bin/env python3
"""
Project search using GPT-5 with Brightdata MCP server for web and Reddit data.

Environment variables required:
- OPENAI_API_KEY: Your OpenAI API key
- BRIGHTDATA_API_TOKEN: Your Brightdata API token for MCP server access
- AIRTABLE_PERSONAL_ACCESS_TOKEN: Your Airtable personal access token (optional)
"""

import os
import time
import requests
import json
from datetime import datetime
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv
from openai import OpenAI
from pyairtable import Api
from pydantic import BaseModel


# MCP server configuration
MCP_SERVER_URL = "https://zachmcp.ngrok.io/mcp"


# Pydantic models for structured response
class Engagement(BaseModel):
    upvotes: Optional[int] = None
    comments: Optional[int] = None
    views: Optional[int] = None
    likes: Optional[int] = None


class ProjectLink(BaseModel):
    title: str
    canonical_url: str
    platform: Optional[str] = None
    date: Optional[str] = None  # YYYY-MM-DD
    lang: Optional[str] = None
    match: Optional[str] = None  # direct_link|exact_phrase|author_reference
    match_confidence: Optional[float] = None  # 1.0|0.8|0.6
    evidence: Optional[str] = None
    engagement: Optional[Engagement] = None
    score: Optional[int] = None
    mentions_hack_club: bool = False
    hack_club_prominence: int = 0  # 0-100
    author_handle: Optional[str] = None
    author_name: Optional[str] = None
    author_relationship: Optional[str] = None  # self_post|team_post|third_party
    external_id: Optional[str] = None
    publish_date: Optional[str] = None  # YYYY-MM-DD


class Meta(BaseModel):
    now_utc: str  # YYYY-MM-DDTHH:MM:SSZ
    live_url: str
    code_url: str
    search_complete: bool


class Diagnostics(BaseModel):
    distinctive_terms: List[str] = []
    authors_detected: List[str] = []
    excluded_scopes: List[str] = []


class OSINTResponse(BaseModel):
    meta: Meta
    diagnostics: Diagnostics
    links: List[ProjectLink]


def write_results_to_airtable(project_links: List[dict], record_id: str, airtable_token: str, 
                             prompt: str = "", reasoning_summary: str = "", full_output_json: str = "") -> None:
    """Write project link results to Airtable table."""
    if not airtable_token:
        print("⚠️ No AIRTABLE_PERSONAL_ACCESS_TOKEN provided, skipping Airtable write")
        return
    
    if not project_links:
        print("ℹ️ No project links to write to Airtable")
        return
    
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
            
            record_data = {
                "Date": link.get("date") or link.get("publish_date"),  # YYYY-MM-DD
                "Source": link.get("platform"),  # ex. Hacker News, Reddit
                "Headline": link.get("title"),  # ex. Show HN: Base, an SQLite database editor for macOS
                "URL": link.get("canonical_url") or link.get("url", ""),
                "Upvotes": link.get("engagement", {}).get("upvotes") if link.get("engagement") else None,  # # of upvotes
                "Mentioned Hack Club?": link.get("mentions_hack_club", False),  # true / false
                "Full JSON": full_json,
                "Full Prompt": prompt,
                "Full Prompt Reasoning Summary": reasoning_summary,
                "Full Prompt Output JSON": full_output_json
            }
            records_to_create.append(record_data)
        
        # Write to Airtable in batches (max 10 per batch)
        batch_size = 10
        total_created = 0
        
        for i in range(0, len(records_to_create), batch_size):
            batch = records_to_create[i:i + batch_size]
            created_records = table.batch_create(batch)
            total_created += len(created_records)
            print(f"📝 Created {len(created_records)} Airtable records (batch {i//batch_size + 1})")
        
        print(f"✅ Successfully wrote {total_created} records to Airtable")
        
    except Exception as e:
        print(f"❌ Error writing to Airtable: {e}")


if __name__ == "__main__":
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
    
    # ============================================================================
    # CUSTOMIZE THIS SECTION FOR YOUR PROJECT
    # ============================================================================
    PROJECT_INPUTS = {
        "live_url": "",
        "code_url": "https://github.com/USERNAME/PROJECT",
        "project_name": "Project Name", 
        "author_name": "Author Name",
        "record_id": "YOUR_RECORD_ID"  # Airtable record ID for this project
    }
    # ============================================================================
    
    # Generate schema-based prompt components
    def generate_example_json_from_schema() -> str:
        """Generate an example JSON structure from our Pydantic models."""
        example = OSINTResponse(
            meta=Meta(
                now_utc="2025-08-25T12:00:00Z",
                live_url=PROJECT_INPUTS['live_url'],
                code_url=PROJECT_INPUTS['code_url'],
                search_complete=True
            ),
            diagnostics=Diagnostics(
                distinctive_terms=["term1", "term2"],
                authors_detected=["Name (@handle)"],
                excluded_scopes=["https://github.com/user/repo"]
            ),
            links=[
                ProjectLink(
                    title="Example Title",
                    canonical_url="https://example.com/post",
                    platform="Reddit",
                    date="2025-08-25",
                    lang="en",
                    match="direct_link",
                    match_confidence=1.0,
                    evidence="short quote proving it's THIS project",
                    engagement=Engagement(
                        upvotes=123,
                        comments=45,
                        views=1000,
                        likes=67
                    ),
                    score=85,
                    mentions_hack_club=False,
                    hack_club_prominence=0,
                    author_handle="example_user",
                    author_name="Example User",
                    author_relationship="third_party",
                    external_id="t3_abc123",
                    publish_date="2025-08-25"
                )
            ]
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
You are an expert OSINT-style web researcher. Your job is to uncover EVERY public place a specific project has been shared or discussed online—from major platforms to tiny forums—while returning a clean, consistent JSON result that lets us recompute scores later without re-fetching any pages. Prioritize action: use tools immediately to surface links, limit internal reasoning to brief reflections, and avoid loops by capping iterations at 5 or when <3 new unique links are found in two consecutive iterations. **Exception:** even if the early-stop condition is met, perform one full AUTHOR SWEEP per known author on priority platforms (Reddit/X/YouTube).

############################################
# OBJECTIVE
############################################
Maximize recall: aggressively find valid links across all time periods, including major platforms (Reddit, Hacker News, Twitter/X, YouTube, YouTube Shorts, Stack Overflow, Mastodon/Fediverse, Bluesky, TikTok, Instagram), multilingual/regional sites, mirrors, archives, niche communities, bookmarking services, and cross-posts.

PRIORITY PLATFORMS: Focus heavily on Reddit (all relevant subreddits), Hacker News, Twitter/X, and YouTube as these are primary viral sharing platforms.

Generate queries in multiple languages (translate key terms to the author's country language + Spanish, Japanese, Chinese, Russian, etc., based on global relevance). Pivot intelligently from fingerprints (phrases, filenames, author handles, repo name, slugs). Chain searches from discovered URLs, authors, or related links in evidence/snippets.

Precision is critical: include ONLY links unambiguously referring to THIS exact project (e.g., exclude similar projects without matching fingerprints). When uncertain, exclude.

############################################
# CONSTRAINTS
############################################
- Treat YouTube vs YouTube Shorts separately (Shorts = URLs under /shorts/).
- Return JSON ONLY (no prose/markdown), conforming to the schema at the end.
- Use multiple search engines (Google **and** Bing) plus platform-native search (e.g., Reddit, YouTube, X) for each query family. Do not rely on a single engine.

############################################
# FINGERPRINT & DISCOVERY MINDSET
############################################
1) Deep fingerprint (from LIVE_URL and/or CODE_URL):
- Capture title/H1, meta/og title & description, canonical links, author names/handles, distinctive phrases.
- Build URL VARIANTS/FAMILY:
  • For GitHub/GitLab/Code hosts: include root repo, sub-repos, and slug-family patterns:
    - ROOT: https://github.com/<owner>/<slug>
    - SUBS: any repos whose name **starts with** <slug> or contains <slug> as a token (case-insensitive), discovered from README, submodules, org listing, and links on repo pages (e.g., <slug>-pcb, <slug>-firmware, <slug>-app).
    - OWNER-SCOPED: https://github.com/<owner>/*<slug>* (case-insensitive, tokenized).
  • Generate http/https, www/no-www, trailing slash, and tracking-free variants.
- Build SYNONYMS/MORPHOLOGY lists for project descriptors (store in diagnostics.distinctive_terms):
  • Hyphenation/spacing: e‑ink | eink | "e ink"; e‑paper | epaper | "e paper".
  • Category synonyms: DAP | "digital audio player" | "music player" (example) — generalize to the project domain.
  • Progress verbs: review | WIP | prototype | **finished** | completed | update | "first prototype" | "show & tell" | "build log".
  • Distinctive chips/parts/filenames/handles from README (ICs, SoCs, BOM parts, product nicknames).
- Store in diagnostics: url_variants_used, distinctive_terms, authors_detected.
- If LIVE_URL inaccessible, use CODE_URL, archives, mirrors, or discovered pages.

2) Be aggressive and action-oriented:
- Start broad, then pivot to niche/long-tail/multilingual.
- Follow author profiles, cross-posts, linkbacks; chain from discovered URLs.
- Use web search tools extensively—run multiple parallel searches (Google/Bing/native) with different strategies per iteration.

3) Iterations & stopping:
- Cap at 5 iterations OR when <3 new unique links are found in two consecutive iterations.
- **Exception (MANDATORY):** run one full **AUTHOR SWEEP** per known author on Reddit, X, and YouTube even if stopping criteria are hit.

############################################
# PLATFORM-SPECIFIC SEARCH STRATEGIES
############################################
Reddit (first-class target):
- Run ALL of the following in parallel for 3–5 query shapes each:
  A) Google/Bing `site:reddit.com` queries using (repo-family URLs) OR (synonym phrases) with OR-expansion and quoted/unquoted variants.
  B) **Reddit native search** (`https://www.reddit.com/search/?q=<query>&sort=new`) with combinations of:
     • <project name OR repo slug OR owner/author handle>
     • PLUS synonyms/morphology (e.g., e-ink/eink, DAP/digital audio player)
     • PLUS progress verbs (finished/completed/prototype/update/review/WIP)
  C) **Author Sweep (MANDATORY once author is known):**
     • Enumerate `https://www.reddit.com/user/<handle>/submitted/.json?limit=100` (paginate if needed).
     • Fuzzy match (≥0.7) on title/selftext against (project name, repo-family, synonyms, progress verbs).
     • Include crossposts via `crosspost_parent_list` when present.
- Subreddit expansion (seed list; adapt to domain): `PrintedCircuitBoard, KiCad, electronics, DIY, 3Dprinting, headphones, DigitalAudioPlayer, audiophile, ipod, eink, raspberry_pi, arduino, esp32` (and other topical neighbors).
- Host variants fallback: also try `old.reddit.com` and `np.reddit.com` (search engines sometimes miss/lag).
- Evidence extraction: use the permalink; capture title, author (u/...), body excerpt, and any direct repo links in OP/top comments.

Hacker News:
- `site:news.ycombinator.com` queries for live/code URLs, titles, author names/handles, and distinctive phrases.
- Use HN item JSON for metadata when possible.

Twitter/X:
- Search for (repo-family URLs, project name, author handle, distinctive phrases).
- Enumerate author timeline if handle is known; capture permalinks to tweets and any quoted retweets.

YouTube:
- Search video titles/descriptions for project name, synonyms, and distinctive terms.
- Treat Shorts separately (`/shorts/`).
- Enumerate channel uploads if the author is known.

Other platforms (Stack Overflow/StackExchange, Mastodon/Fediverse, Bluesky, TikTok, Instagram, bookmarking sites like Hatena/Pinboard, regional forums):
- Use `site:` operators + synonyms + progress verbs.
- For author-known platforms, enumerate recent posts where possible.

############################################
# SEARCH QUERY EXAMPLES (DIVERSE RECIPES)
############################################
Direct:
- "<project name>" | "<repo slug>" | "<owner>/<slug>" | "<live URL>"

Author-focused:
- "<author handle> <project>" | "<author handle> <synonym>" | "user:<author handle> <project>"

Platform-specific:
- Reddit (Google/Bing): `site:reddit.com ("<repo-slug>" OR "<owner>/*<slug>*" OR "<project name>") (e-ink OR eink OR "e paper") (DAP OR "digital audio player" OR "music player")`
- Reddit (native): `"<repo-slug>"` OR `"<owner> <slug>"` OR `"u:<authorHandle> <synonym>"` combined with `(finished|completed|prototype|update|review|WIP)`
- HN: `site:news.ycombinator.com <project name OR owner OR slug>`
- X: `<project name>` OR `<slug>` OR `<owner>` OR distinctive phrases, plus URL variants.
- YouTube: `<project name|slug|synonym> review | build | prototype | demo` and filter for `/shorts/` when needed.

Problem/Use-case:
- `"<problem the project solves>" discussion | reddit | alternatives | vs <similar tools>`

Multilingual (examples):
- Spanish/ja/zh/ru + author’s language: translate "<category term(s)>" + "<project name/slug>" + progress verbs.

############################################
# HANDLING INACCESSIBLE PAGES
############################################
If a page fails to load or content is gated:
- Retry immediately via archives: query “wayback machine <URL>” or “archive <URL>” to fetch snapshots.
- If archive fails, query “<URL> cached” or search snippets across engines.
- If still blocked, pivot to related queries (e.g., "<project name> <site> discussion") or multilingual equivalents; extract from snippets if needed.
- Document attempts in diagnostics.notes (e.g., "Page X failed; used Wayback for evidence"). Exhaust options before skipping.

############################################
# STRICT INCLUSION CRITERIA
############################################
Include only if ≥1 condition met (store evidence quote):
(1) **Direct link** to LIVE_URL/CODE_URL **or any repo-family URL variant** → `match_confidence = 1.0`.
(2) **Exact phrase match** to a distinctive, project-unique phrase → `match_confidence = 0.8`.
(3) **Known author + contextual match**: same author handle AND (title/selftext contains ≥1 synonym AND ≥1 progress/descriptor OR falls within the project’s active date window) → `match_confidence = 0.6`.
Reject <0.6 or if matches similar but different projects.

############################################
# SELF-SCOPE EXCLUSIONS (MANDATORY)
############################################
Exclude project's own pages/subpages:
A) LIVE_SCOPE: Exclude origin matching LIVE_URL and paths starting with its path (or entire origin if root).
B) CODE_SCOPE: For GitHub/etc., exclude repo root paths (e.g., `/username/reponame/*`); include platform blogs/explore.
C) Mirrors/archives of excluded pages remain excluded (cite in evidence if useful).
Store scopes in `diagnostics.exclusion_scopes`; include up to 10 excluded examples in `diagnostics.excluded_examples`.

############################################
# CANONICALIZATION, PERMALINKS, DEDUPE
############################################
- Required `canonical_url`: (1) rel="canonical", (2) og:url, (3) resolved URL (tracking stripped).
- Prefer permalinks over listings; use `index_only` if no permalink.
- Dedupe by `canonical_url`; track `mirror_urls` for duplicates/mirrors.

############################################
# PLATFORM CANONICAL NAME MAPPING
############################################
Use consistent `platform` values (e.g., Hacker News, Reddit, X, YouTube (video), YouTube (Shorts), Qiita, Zhihu, Hatena Bookmark, Major Tech Press, DEV Community, Hashnode, Medium, Substack, Personal Blog/Newsletter, Other). Set `platform_detected_from_domain`.

############################################
# METADATA, DATES, NUMERIC RULES
############################################
- `author_handle`: lowercase; `author_name`: as displayed; `author_relationship`: self_post | team_post | third_party.
- `external_id`: platform ID if present.
- Dates: `publish_date` as YYYY-MM-DD. Include `precision` fields; compute relatives from NOW_UTC. Set `source/raw`.
- Engagement: normalized integers (parse “1.2k”). Raw in `score_breakdown.engagement_raw`. Set `observed_at/confidence/extraction_method`.
- Required: `language` (e.g., "en", "ja", "es", "zh-cn").
- `engagement_summary` (string|null) for raw notes (e.g., "115 points, 12 comments").

############################################
# SCORING
############################################
Provide `score_breakdown/score_inputs` for recomputation. Engagement normalization E ∈ [0,1]:
- HN/Reddit/Lobsters: E = 0.7*log1p(upvote_count)/log1p(500) + 0.3*log1p(comment_count)/log1p(200)
- Product Hunt: E = 0.8*log1p(upvote_count)/log1p(1000) + 0.2*log1p(comment_count)/log1p(300)
- YouTube (video): E = 0.6*log1p(view_count)/log1p(1_000_000) + 0.2*log1p(like_count)/log1p(50_000) + 0.2*log1p(comment_count)/log1p(5_000)
- YouTube (Shorts): E = 0.7*log1p(view_count)/log1p(5_000_000) + 0.2*log1p(like_count)/log1p(50_000) + 0.1*log1p(comment_count)/log1p(5_000)
- Hatena/bookmarking: E = log1p(upvote_count)/log1p(500)
- News/Blogs without counters: E = 0
Platform authority W_platform: HN=1.00, Reddit=0.95, Product Hunt=0.90, YouTube(video)=0.85, YouTube(Shorts)=0.80, X=0.75, Bluesky=0.70, Lobsters=0.70, Hatena=0.65, Qiita=0.70, Zhihu=0.65, Major Tech Press=1.00, DEV Community=0.70, Hashnode=0.65, Medium=0.65, Substack=0.65, Personal Blog/Newsletter=0.55, Other=0.60
Originality bonus O: +0.10 if original article/post/video; else 0 (include `originality_rationale`).
Recency factor R: 1.00 if within 365 days of NOW_UTC; else 0.85.
Final score S = round(100 * W_platform * R * (0.9*E + 0.1*O))
Include `version/now_utc/platform_weight/E_components/R_base_date/O_applied`.

############################################
# HACK CLUB PROMINENCE
############################################
0 = not mentioned; 50 = minor (quote); 100 = prominent (quote in justification).

############################################
# CONSISTENCY GUARANTEES
############################################
- Enforce exclusions. Required fields non-null where specified.
- Sort `project_links` by `link_score` DESC, then `publish_date` DESC (null last), then `canonical_url` ASC.
- One per unique canonical; use mirrors for dupes.

############################################
# OUTPUT
############################################
Return JSON ONLY validating against schema. If no results, `project_links: []`, `search_complete: true`, explain in `diagnostics.notes`.

############################################
# INPUTS
############################################
- LIVE_URL: {PROJECT_INPUTS['live_url']}
- CODE_URL: {PROJECT_INPUTS['code_url']}
- MAX_RESULTS: 400

############################################
# CRITICAL: PYDANTIC VALIDATION
############################################
Your response MUST be valid JSON that exactly matches this Pydantic schema. The output will be validated with Pydantic models - any deviation will cause validation failure.

PYDANTIC SCHEMA (AUTHORITATIVE):
{json.dumps(pydantic_schema, indent=2)}

EXAMPLE JSON FORMAT:
{example_json}

VALIDATION REQUIREMENTS:
- Output will be parsed with OSINTResponse(**your_json)
- ALL field names must match the schema exactly (case-sensitive)
- ALL data types must match (str, int, float, bool, list, dict)
- Required fields cannot be null/missing
- Return ONLY valid JSON - no markdown, no prose, no code blocks

Return the JSON response matching this exact Pydantic schema.
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
    
    # Show usage stats if available
    if hasattr(response, 'usage'):
        print(f"📊 Usage: {response.usage}")
        if hasattr(response.usage, 'output_tokens_details'):
            if hasattr(response.usage.output_tokens_details, 'reasoning_tokens'):
                reasoning_tokens = response.usage.output_tokens_details.reasoning_tokens
                print(f"🧠 Reasoning tokens used: {reasoning_tokens:,}")
    
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
            print(f"   🔗 Found {len(structured_response.links)} links")
            print(f"   🏁 Search complete: {structured_response.meta.search_complete}")
            print(f"   🔍 Distinctive terms: {', '.join(structured_response.diagnostics.distinctive_terms[:5])}")
            if structured_response.links:
                top_link = structured_response.links[0]
                print(f"   ⭐ Top link: {top_link.title[:50]}... ({top_link.score} score)")
            
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
    
    # Write to Airtable using structured data
    if structured_response and AIRTABLE_TOKEN:
        print(f"\n💾 Writing structured results to Airtable...")
        try:
            # Convert Pydantic models to dictionaries for Airtable
            project_links = [link.model_dump() for link in structured_response.links]
            
            if project_links:
                write_results_to_airtable(
                    project_links,
                    PROJECT_INPUTS.get("record_id", ""),
                    AIRTABLE_TOKEN,
                    prompt=prompt_content,  # Use actual full prompt text
                    reasoning_summary=reasoning_content,  # Use accumulated reasoning
                    full_output_json=final_output
                )
            else:
                print("ℹ️ No project links found in structured response")
                
        except Exception as e:
            print(f"❌ Error processing structured Airtable write: {e}")
    elif final_output and AIRTABLE_TOKEN:
        # Fallback to old parsing method if structured parsing failed
        print(f"\n💾 Fallback: Writing raw results to Airtable...")
        try:
            import json
            parsed_output = json.loads(final_output)
            project_links = parsed_output.get("project_links", [])
            if project_links:
                write_results_to_airtable(
                    project_links,
                    PROJECT_INPUTS.get("record_id", ""),
                    AIRTABLE_TOKEN,
                    prompt=prompt_content,  # Use actual full prompt text
                    reasoning_summary=reasoning_content,  # Use accumulated reasoning
                    full_output_json=final_output
                )
            else:
                print("ℹ️ No project_links found in raw output")
        except Exception as e:
            print(f"❌ Error with fallback Airtable write: {e}")
    elif not AIRTABLE_TOKEN:
        print("⚠️ No AIRTABLE_PERSONAL_ACCESS_TOKEN - skipping Airtable write")
    
    print(f"\n✅ Done at {datetime.now().isoformat()}")
