#!/usr/bin/env python3
"""
Project search using GPT-5 with Brightdata MCP server for web and Reddit data.

Environment variables required:
- OPENAI_API_KEY: Your OpenAI API key
- BRIGHTDATA_API_TOKEN: Your Brightdata API token for MCP server access
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


# MCP server configuration
MCP_SERVER_URL = "https://zachmcp.ngrok.io/mcp"


def write_results_to_airtable(project_links: List[dict], record_id: str, airtable_token: str, 
                             prompt: str = "", full_output_json: str = "") -> None:
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
                "URL": link.get("url", ""),
                "Mentions Hack Club?": link.get("mentions_hack_club", False),
                "Link Score": link.get("link_score", 0),
                "Full JSON": full_json,
                "YSWS Approved Project": [record_id] if record_id else [],
                "Full Prompt": prompt,
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
        "author_name": "Author Name"
    }
    # ============================================================================
    
    # Create the comprehensive OSINT developer prompt
    PROMPT = f"""
You are an expert OSINT-style web researcher. Your job is to uncover EVERY public place a specific project has been shared or discussed online—from major platforms to tiny forums—while returning a clean, consistent JSON result that lets us recompute scores later without re-fetching any pages. Prioritize action: use tools immediately to surface links, limit internal reasoning to brief reflections, and avoid loops by capping iterations at 5 or when <3 new unique links are found in two consecutive iterations.

############################################
# INPUTS
############################################
- LIVE_URL: {PROJECT_INPUTS['live_url']}
- CODE_URL: {PROJECT_INPUTS['code_url']}
- MAX_RESULTS: 400  # Aim high; stop if iterations yield diminishing returns

############################################
# OBJECTIVE
############################################
Maximize recall: aggressively find valid links across all time periods, including major platforms (Reddit, Hacker News, Twitter/X, YouTube, YouTube Shorts, Stack Overflow, Mastadon / Fediverse, Bluesky, TikTok, Instagram), multilingual/regional sites, mirrors, archives, niche communities, bookmarking services, and cross-posts. 

PRIORITY PLATFORMS: Focus heavily on Reddit (all relevant subreddits), Hacker News, Twitter/X, and YouTube as these are primary viral sharing platforms.

Generate queries in multiple languages (e.g., translate key terms to author's country's language + Spanish, Japanese, Chinese, Russian, etc based on global relevance). Pivot intelligently from fingerprints (phrases, filenames, author handles, repo name, slugs). Chain searches from discovered URLs, authors, or related links in evidence/snippets.

Precision is critical: include ONLY links unambiguously referring to THIS exact project (e.g., exclude similar projects without matching fingerprints). When uncertain, exclude.

############################################
# CONSTRAINTS
############################################
- Treat YouTube vs YouTube Shorts separately (Shorts = URLs under /shorts/).
- Return JSON ONLY (no prose/markdown), conforming to the schema at the end.

############################################
# FINGERPRINT & DISCOVERY MINDSET
############################################
1) Deep fingerprint (from LIVE_URL and/or CODE_URL):
- Capture title/H1, meta/og title & description, canonical links, author names/handles, distinctive phrases
- Generate URL variants (http/https, www/no-www, trailing slash; strip tracking params).
- Include multilingual variants
- Store in diagnostics: url_variants_used, distinctive_terms, authors_detected.
- If LIVE_URL inaccessible, use CODE_URL, archives, or discovered pages.

2) Be aggressive and action-oriented:
- Start broad, pivot to niche/long-tail/multilingual
- Follow author profiles, cross-posts, linkbacks; chain from discovered URLs.
- Act first: use tools for searches/browsing before overthinking.
- Continue until MAX_RESULTS or iterations show <3 new links twice in a row.
- Use web search tools extensively - run multiple parallel searches with different query strategies.
- Don't rely on a single search; try 5-10 different search approaches per iteration.

3) Platform-specific search strategies:
- Reddit: Search for project name, author handle, distinctive phrases. Try both reddit.com in queries and direct Reddit searches. Look in relevant subreddits (r/programming, r/webdev, r/coding, r/github, etc).
- Hacker News: Search for project URLs, titles, author names on news.ycombinator.com.
- Twitter/X: Search for project URLs, @handles, distinctive phrases.
- Use site-specific search operators when beneficial (e.g., "site:reddit.com [project name]", "site:news.ycombinator.com [project]").

############################################
# SEARCH QUERY EXAMPLES
############################################
Generate diverse search queries including:
- Direct project searches: "[project name]", "[repo name]", "[live URL]"  
- Author-focused: "[author name] [project]", "[github handle] project"
- Platform-specific: "site:reddit.com [project name]", "site:news.ycombinator.com [project]"
- Distinctive phrases: "[unique project description]", "[notable features]"
- Problem-solving: "[problem project solves] reddit", "[use case] discussion"
- Cross-references: "[similar tools] vs [project]", "[project] alternative"

############################################
# HANDLING INACCESSIBLE PAGES
############################################
If a page fails to load or content is gated:
- Retry immediately: Query "wayback machine [URL]" or "archive [URL]" to fetch snapshots. If archive fails, query "[URL] cached" or snippets from multiple engines.
- If still blocked, pivot to related queries (e.g., "[project name] [site] discussion") or multilingual equivalents; extract from search snippets if needed.
- Document all attempts in diagnostics.notes (e.g., "Page X failed; used Wayback for evidence").
- Exhaust options before skipping.

############################################
# STRICT INCLUSION CRITERIA
############################################
Include only if ≥1 condition met (store evidence quote):
(1) Direct link to LIVE_URL/CODE_URL (or variant).
(2) Exact match to distinctive, project-unique phrase
(3) Known author explicitly references THIS project (with quote).

match_confidence:
- 1.0 = direct link.
- 0.8 = exact phrase match.
- 0.6 = clear author reference + context.
Reject <0.6 or if matches similar but different projects.

############################################
# SELF-SCOPE EXCLUSIONS (MANDATORY)
############################################
Exclude project's own pages/subpages:
A) LIVE_SCOPE: Exclude origin matching LIVE_URL and paths starting with its path (or entire origin if root).
B) CODE_SCOPE: For GitHub/etc., exclude repo root paths (e.g., /username/reponame/*); include platform blogs/explore.
C) Mirrors/archives of excluded pages remain excluded (cite in evidence if useful).
Store scopes in diagnostics.exclusion_scopes; up to 10 excluded examples in diagnostics.excluded_examples.

############################################
# CANONICALIZATION, PERMALINKS, DEDUPE
############################################
- Required canonical_url: (1) rel="canonical", (2) og:url, (3) resolved URL (tracking stripped).
- Prefer permalinks over listings; use index_only if no permalink.
- Dedupe by canonical_url; use mirror_urls for duplicates/mirrors.

############################################
# PLATFORM CANONICAL NAME MAPPING
############################################
Use consistent platform (e.g., Hacker News, Reddit, X, YouTube with subtype, Qiita, Zhihu, Hatena Bookmark, Major Tech Press by exact name, Personal Blog/Newsletter, Other as fallback). Set platform_detected_from_domain.

############################################
# METADATA, DATES, NUMERIC RULES
############################################
- author_handle: lowercase; author_name: as displayed; author_relationship: self_post | team_post | third_party.
- external_id: platform ID if present.
- Dates: publish_date as YYYY-MM-DD. Use precision fields; compute relatives from NOW_UTC. Set source/raw.
- Engagement: Normalized integers (parse "1.2k"); raw in score_breakdown.engagement_raw. Set observed_at/confidence/extraction_method.
- Required: language (e.g., "en", "ja", "es", "zh-cn").
- New: engagement_summary (string|null) for raw notes (e.g., "115 points, 12 comments").

############################################
# SCORING
############################################
Provide score_breakdown/score_inputs for recomputation. Engagement normalization E ∈ [0,1]:
- HN/Reddit/Lobsters: E = 0.7*log1p(upvote_count)/log1p(500) + 0.3*log1p(comment_count)/log1p(200)
- Product Hunt: E = 0.8*log1p(upvote_count)/log1p(1000) + 0.2*log1p(comment_count)/log1p(300)
- YouTube (video): E = 0.6*log1p(view_count)/log1p(1_000_000) + 0.2*log1p(like_count)/log1p(50_000) + 0.2*log1p(comment_count)/log1p(5_000)
- YouTube Shorts: E = 0.7*log1p(view_count)/log1p(5_000_000) + 0.2*log1p(like_count)/log1p(50_000) + 0.1*log1p(comment_count)/log1p(5_000)
- Hatena/bookmarking: E = log1p(upvote_count)/log1p(500)
- News/Blogs without counters: E = 0

Platform authority W_platform: HN=1.00, Reddit=0.95, Product Hunt=0.90, YouTube(video)=0.85, YouTube(Shorts)=0.80, X=0.75, Bluesky=0.70, Lobsters=0.70, Hatena=0.65, Qiita=0.70, Zhihu=0.65, Major Tech Press=1.00, DEV Community=0.70, Hashnode=0.65, Medium=0.65, Substack=0.65, Personal Blog/Newsletter=0.55, Other=0.60

Originality bonus O: +0.10 if original article/post/video; else 0 (include originality_rationale).
Recency factor R: 1.00 if within 365 days of NOW_UTC; else 0.85.
Final score S = round(100 * W_platform * R * (0.9*E + 0.1*O))

Include version/now_utc/platform_weight/E_components/R_base_date/O_applied.

############################################
# HACK CLUB PROMINENCE
############################################
0 = not mentioned; 50 = minor (quote); 100 = prominent (quote in justification).

############################################
# CONSISTENCY GUARANTEES
############################################
Enforce exclusions. Required fields non-null where specified. Sort project_links by link_score DESC, then publish_date DESC (null last), then canonical_url ASC. One per unique canonical; use mirrors for dupes.

############################################
# OUTPUT
############################################
    print("🔍 Querying GPT-5 with Brightdata MCP server tools...")
    start_time = time.time()
    
    # Prepare authorization header outside of f-string to avoid nesting
    auth_header = f"Bearer {BRIGHTDATA_TOKEN}"
    
    # API parameters (MCP server tools with saved prompt)
    base_params = {
        "model": "gpt-5",
        "prompt": {
            "id": "pmpt_68ab8740fd248190a3fc227568ce249906d120231168c398",
            "version": "6",
            "variables": {
                "live_url": PROJECT_INPUTS['live_url'],
                "code_url": PROJECT_INPUTS['code_url']
            }
        },
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
            
        # Skip logging noisy events that don't provide useful information
    
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
    
    # Try to parse JSON output and write to Airtable
    if final_output and AIRTABLE_TOKEN:
        print(f"\n💾 Writing results to Airtable...")
        try:
            # Try to parse the JSON output
            import json
            parsed_output = json.loads(final_output)
            
            # Extract project links if they exist
            project_links = parsed_output.get("project_links", [])
            if project_links:
                write_results_to_airtable(
                    project_links,
                    PROJECT_INPUTS.get("record_id", ""),
                    AIRTABLE_TOKEN,
                    prompt=f"Saved prompt ID: pmpt_68ab8740fd248190a3fc227568ce249906d120231168c398 v6 with variables: {PROJECT_INPUTS['live_url']}, {PROJECT_INPUTS['code_url']}",
                    full_output_json=final_output
                )
            else:
                print("ℹ️ No project_links found in output to write to Airtable")
                
        except json.JSONDecodeError as e:
            print(f"⚠️ Could not parse output as JSON for Airtable: {e}")
        except Exception as e:
            print(f"❌ Error processing Airtable write: {e}")
    elif not AIRTABLE_TOKEN:
        print("⚠️ No AIRTABLE_PERSONAL_ACCESS_TOKEN - skipping Airtable write")
    
    print(f"\n✅ Done at {datetime.now().isoformat()}")
