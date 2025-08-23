#!/usr/bin/env python3
"""
OSINT-style web research tool using GPT-5 with web search capabilities.

Environment variables required:
- OPENAI_API_KEY: Your OpenAI API key
- AIRTABLE_PERSONAL_ACCESS_TOKEN: Your Airtable personal access token (optional, for writing results)
- OPENAI_ADMIN_KEY: Admin API key for cost tracking (optional)

Customization: Edit the PROJECT_INPUTS dictionary to search for different projects.

Example configurations:
# Manual configuration:
PROJECT_INPUTS = {
    "live_url": "https://example.com",
    "code_url": "https://github.com/user/repo", 
    "project_hints": {
        "record_id": "rec123456789",
        "authors": [{"first_name": "John", "last_name": "Doe", "handles": {"github": "johndoe"}}],
        "author_country": "Canada"
    },
    "now_utc": "2025-08-23T12:00:00Z",
    "max_results": 200
}

# For a project without live URL:
PROJECT_INPUTS = {
    "live_url": "",  # Can be empty if no live demo
    "code_url": "https://github.com/user/cli-tool",
    "project_hints": {
        "record_id": "rec987654321", 
        "authors": [{"first_name": "Jane", "last_name": "Smith", "handles": {"github": "jsmith"}}],
        "author_country": "United Kingdom"
    },
    "now_utc": "2025-08-23T12:00:00Z",
    "max_results": 100
}
"""

import os
import time
import requests
import json
from datetime import datetime
from typing import List, Optional

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel
from pyairtable import Api


class EComponents(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    upvote_count: Optional[int] = None
    comment_count: Optional[int] = None
    like_count: Optional[int] = None
    view_count: Optional[int] = None
    repost_share_count: Optional[int] = None


class EngagementRaw(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    upvote_count: Optional[int] = None
    comment_count: Optional[int] = None
    like_count: Optional[int] = None
    view_count: Optional[int] = None
    repost_share_count: Optional[int] = None


class ScoreInputs(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    E_components: EComponents
    R_base_date: str
    O_applied: float


class ScoreBreakdown(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    platform_weight: float
    engagement_raw: EngagementRaw
    engagement_norm: float
    recency_factor: float
    originality_bonus: float


class Evidence(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    matched_text: str
    where: str
    context_snippet: Optional[str] = None
    selector: Optional[str] = None


class ProjectLink(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    url: str
    canonical_url: str
    platform: Optional[str] = None
    platform_subtype: Optional[str] = None
    content_type: Optional[str] = None
    title: Optional[str] = None
    author_name: Optional[str] = None
    author_handle: Optional[str] = None
    author_relationship: Optional[str] = None
    external_id: Optional[str] = None
    publish_date: str
    publish_date_precision: Optional[str] = None
    publish_date_source: Optional[str] = None
    publish_date_raw: Optional[str] = None
    link_score: int
    score_inputs: ScoreInputs
    score_breakdown: ScoreBreakdown
    originality_rationale: Optional[str] = None
    match_confidence: float
    justification: str
    mentions_hack_club: bool
    hack_club_prominence: int
    hack_club_prominence_justification: str
    is_hack_club_url: bool
    upvote_count: Optional[int] = None
    comment_count: Optional[int] = None
    like_count: Optional[int] = None
    view_count: Optional[int] = None
    repost_share_count: Optional[int] = None
    engagement_summary: Optional[str] = None
    syndication_type: Optional[str] = None
    permalink_status: Optional[str] = None
    mirror_urls: List[str] = []
    archive_urls: List[str] = []
    evidence: List[Evidence] = []
    platform_detected_from_domain: Optional[str] = None
    language: str
    metrics_observed_at: str
    metrics_confidence: float
    extraction_method: str
    collected_at: str


class Handles(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    github: Optional[str] = None


class Author(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    first_name: str
    last_name: str
    handles: Handles


class Project(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    live_url: str
    code_url: str
    canonical_url: Optional[str] = None
    authors: List[Author] = []
    author_country: Optional[str] = None
    approved_at: Optional[str] = None


class QueryIteration(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    iteration: int
    new_links_found: int


class ExclusionScope(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    type: str
    origin: str
    path_prefix: str


class Diagnostics(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    search_date: str
    sources_queried: List[str] = []
    queries_run: List[str] = []
    query_iterations: List[QueryIteration] = []
    pages_scanned: int
    timeframe_considered: Optional[str] = None
    url_variants_used: List[str] = []
    distinctive_terms: List[str] = []
    authors_detected: List[str] = []
    exclusion_scopes: List[ExclusionScope] = []
    excluded_examples: List[str] = []
    notes: Optional[str] = None
    limitations: List[str] = []


class SearchResponse(BaseModel):
    model_config = {"json_schema_extra": {"additionalProperties": False}}
    
    project: Project
    project_links: List[ProjectLink]
    total_found: int
    search_complete: bool
    diagnostics: Diagnostics
    scoring_version: str
    now_utc_used_for_scoring: str


def calculate_detailed_cost(usage_data, model: str, web_search_calls: int = 0, use_flex: bool = True) -> dict:
    """
    Calculate detailed cost breakdown including cached tokens, reasoning tokens, and tool costs.
    Based on August 2025 OpenAI pricing structure.
    """
    
    # Get token counts from usage data (handle different API response formats)
    prompt_tokens = getattr(usage_data, 'input_tokens', 0) or 0
    completion_tokens = getattr(usage_data, 'output_tokens', 0) or 0
    
    # Get detailed token breakdowns
    cached_tokens = 0
    reasoning_tokens = 0
    
    if hasattr(usage_data, 'prompt_tokens_details') and usage_data.prompt_tokens_details:
        cached_tokens = usage_data.prompt_tokens_details.cached_tokens or 0
    
    if hasattr(usage_data, 'completion_tokens_details') and usage_data.completion_tokens_details:
        reasoning_tokens = usage_data.completion_tokens_details.reasoning_tokens or 0
    
    # Calculate non-cached input tokens
    non_cached_tokens = prompt_tokens - cached_tokens
    
    # Model-specific pricing (per 1M tokens) - Standard vs Flex pricing
    pricing = {
        "gpt-5": {
            "standard": {"input": 1.25, "cached": 0.125, "output": 10.00},
            "flex": {"input": 0.625, "cached": 0.0625, "output": 5.00}
        },
        "gpt-5-mini": {
            "standard": {"input": 0.25, "cached": 0.025, "output": 2.00},
            "flex": {"input": 0.125, "cached": 0.0125, "output": 1.00}
        },
        "gpt-5-nano": {
            "standard": {"input": 0.05, "cached": 0.005, "output": 0.40},
            "flex": {"input": 0.025, "cached": 0.0025, "output": 0.20}
        }
    }
    
    # Get pricing tier based on flex processing setting
    tier = "flex" if use_flex else "standard"
    rates = pricing.get(model, pricing["gpt-5"])[tier]
    
    # Calculate token costs
    non_cached_cost = non_cached_tokens * rates["input"] / 1_000_000
    cached_cost = cached_tokens * rates["cached"] / 1_000_000
    output_cost = completion_tokens * rates["output"] / 1_000_000
    
    # Tool costs
    web_search_cost = web_search_calls * 10.00 / 1_000  # $10/1K calls for GPT-5
    
    total_cost = non_cached_cost + cached_cost + output_cost + web_search_cost
    
    return {
        "non_cached_tokens": non_cached_tokens,
        "cached_tokens": cached_tokens,
        "reasoning_tokens": reasoning_tokens,
        "completion_tokens": completion_tokens,
        "non_cached_cost": non_cached_cost,
        "cached_cost": cached_cost,
        "output_cost": output_cost,
        "web_search_cost": web_search_cost,
        "total_cost": total_cost,
        "token_rates": rates,
        "pricing_tier": tier
    }



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


def get_organization_costs(admin_api_key: str) -> float:
    """Get current organization costs using admin API key."""
    headers = {"Authorization": f"Bearer {admin_api_key}"}
    
    # Get costs for today (start_time in seconds since epoch)
    import time
    start_time = int(time.time()) - 86400  # 24 hours ago
    
    try:
        response = requests.get(
            f"https://api.openai.com/v1/organization/costs?start_time={start_time}",
            headers=headers
        )
        response.raise_for_status()
        data = response.json()
        
        # Sum total costs from the response
        total_cost = 0.0
        for item in data.get("data", []):
            total_cost += item.get("amount", 0.0)
        
        return total_cost
    except Exception as e:
        print(f"⚠️ Could not fetch organization costs: {e}")
        return None


if __name__ == "__main__":
    # Setup
    load_dotenv()
    API_KEY = os.getenv("OPENAI_API_KEY")
    ADMIN_KEY = os.getenv("OPENAI_ADMIN_KEY")
    AIRTABLE_TOKEN = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    
    if not API_KEY:
        raise SystemExit("❌ OPENAI_API_KEY not found in .env")

    # Configuration
    USE_FLEX_PROCESSING = False  # Disable flex processing for debugging
    
    # Set up client with appropriate timeout for flex processing
    if USE_FLEX_PROCESSING:
        client = OpenAI(api_key=API_KEY).with_options(timeout=900.0)  # 15 minutes for flex
    else:
        client = OpenAI(api_key=API_KEY)
    
    # Simplified billing test
    if ADMIN_KEY:
        print("✅ Billing API enabled")
    else:
        print("⚠️ Billing tracking disabled (no OPENAI_ADMIN_KEY)")

    # ============================================================================
    # CUSTOMIZE THIS SECTION FOR YOUR PROJECT
    # ============================================================================
    PROJECT_INPUTS = {
    }
    # ============================================================================

    # Generate the OSINT-style web research prompt with configurable inputs
    PROMPT = f"""
You are an expert OSINT-style web researcher. Your job is to uncover EVERY public place a specific project has been shared or discussed online—from major platforms to tiny forums—while returning a clean, consistent JSON result that lets us recompute scores later without re-fetching any pages. Prioritize action: use tools immediately to surface links, limit internal reasoning to brief reflections, and avoid loops by capping iterations at 5 or when <3 new unique links are found in two consecutive iterations.

############################################
# INPUTS
############################################
- LIVE_URL: {PROJECT_INPUTS['live_url']}
- CODE_URL: {PROJECT_INPUTS['code_url']}
- PROJECT_HINTS (JSON): {PROJECT_INPUTS['project_hints']}
- NOW_UTC: {PROJECT_INPUTS['now_utc']}
- MAX_RESULTS: {PROJECT_INPUTS['max_results']}  # Aim high; stop if iterations yield diminishing returns

############################################
# OBJECTIVE
############################################
Maximize recall: aggressively find valid links across all time periods, including major platforms (Reddit, Hacker News, Twitter/X, YouTube, Stack Overflow), multilingual/regional sites (e.g., Qiita, Hatena, VK, cnblogs, Zhihu), mirrors, archives, niche communities, bookmarking services, and cross-posts. 

PRIORITY PLATFORMS: Focus heavily on Reddit (all relevant subreddits), Hacker News, Twitter/X, and YouTube as these are primary viral sharing platforms.

Generate queries in multiple languages (e.g., translate key terms to author's country's language + Spanish, Japanese, Chinese, Russian, etc based on global relevance). Pivot intelligently from fingerprints (phrases, filenames, author handles, repo name, slugs). Chain searches from discovered URLs, authors, or related links in evidence/snippets.

Precision is critical: include ONLY links unambiguously referring to THIS exact project (e.g., exclude similar "Tron" games without matching fingerprints). When uncertain, exclude.

############################################
# CONSTRAINTS
############################################
- Treat YouTube vs YouTube Shorts separately (Shorts = URLs under /shorts/).
- Return JSON ONLY (no prose/markdown), conforming to the schema at the end.

############################################
# FINGERPRINT & DISCOVERY MINDSET
############################################
1) Deep fingerprint (from LIVE_URL and/or CODE_URL):
- Capture title/H1, meta/og title & description, canonical links, author names/handles, distinctive phrases (e.g., "multiplayer lightcycle game over SSH", "ssh sshtron.zachlatta.com", "BrickHack 2").
- Generate URL variants (http/https, www/no-www, trailing slash; strip tracking params).
- Include multilingual variants (e.g., "juego Tron via SSH", "SSHTron ゲーム").
- Store in diagnostics: url_variants_used, distinctive_terms, authors_detected.
- If LIVE_URL inaccessible, use CODE_URL, archives, or discovered pages.

2) Be aggressive and action-oriented:
- Start broad, pivot to niche/long-tail/multilingual (e.g., "SSHTron jeu terminal" for French sites).
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
(2) Exact match to distinctive, project-unique phrase (e.g., "ssh sshtron.zachlatta.com").
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
B) CODE_SCOPE: For GitHub/etc., exclude repo root paths (e.g., /zachlatta/sshtron/*); include platform blogs/explore.
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
Return JSON ONLY validating against schema. If no results, project_links: [], search_complete: true, explain in diagnostics.notes.
    """

    # Get baseline billing
    cost_before = get_organization_costs(ADMIN_KEY) if ADMIN_KEY else None
    
    print("🔍 Querying GPT-5 with web search...")
    start_time = time.time()
    
    # Common API parameters
    base_params = {
        "model": "gpt-5",
        "tools": [
            {
                "type": "web_search_preview",
                "search_context_size": "high"
            },
            {
                "type": "code_interpreter",
                "container": {"type": "auto"}
            }
        ],
        "reasoning": {"effort": "high"},
        "input": PROMPT
    }
    
    if USE_FLEX_PROCESSING:
        print("💡 Using flex processing (50% cost savings, may take longer)")
        
        # Create manual schema for responses.create (supports service_tier)
        schema = SearchResponse.model_json_schema()
        schema["additionalProperties"] = False
        
        # Fix $defs section for strict mode - add optional fields to required
        if "$defs" in schema and "ProjectLink" in schema["$defs"]:
            project_link_def = schema["$defs"]["ProjectLink"]
            all_props = list(project_link_def["properties"].keys())
            project_link_def["required"] = all_props  # Include all fields as required for strict mode
        
        response = client.responses.create(
            **base_params,
            service_tier="flex",
            text={"format": {"type": "json_schema", "name": "search_response", "schema": schema, "strict": True}}
        )
        use_flex_pricing = True
    else:
        print("💡 Using standard processing")
        response = client.responses.parse(**base_params, text_format=SearchResponse)
        use_flex_pricing = False
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Get actual billing
    cost_after = get_organization_costs(ADMIN_KEY) if ADMIN_KEY else None
    
    # Calculate detailed costs with proper token breakdown
    estimated_search_calls = 2  # Conservative estimate
    cost_breakdown = calculate_detailed_cost(response.usage, "gpt-5", estimated_search_calls, use_flex_pricing)
    
    # Calculate actual cost
    actual_cost = (cost_after - cost_before) if (cost_before is not None and cost_after is not None) else None

    print(f"\n📦 Structured Response:")
    # Parse response based on API method used
    try:
        if USE_FLEX_PROCESSING:
            import json
            parsed_data = json.loads(response.output_text)
            structured_response = SearchResponse(**parsed_data)
        else:
            structured_response = response.output_parsed
        
        # Reasoning summary disabled for now
        reasoning_summary = ""
        
        full_output_json = structured_response.model_dump_json(indent=4)
        
        print(structured_response.model_dump_json(indent=2))
        
        # Print reasoning summary if available
        if reasoning_summary:
            print(f"\n🤔 Reasoning Summary:")
            print(reasoning_summary)
        
        # Write results to Airtable
        print(f"\n💾 Writing results to Airtable...")
        project_links_dict = [link.model_dump() for link in structured_response.project_links]
        record_id = PROJECT_INPUTS["project_hints"]["record_id"]
        write_results_to_airtable(
            project_links_dict, 
            record_id, 
            AIRTABLE_TOKEN,
            prompt=PROMPT,
            full_output_json=full_output_json
        )
        
    except Exception as e:
        print(f"Error parsing response: {e}")
        print("Raw response:", getattr(response, 'output_text', 'No output'))

    print(f"\n📊 Detailed Usage & Cost Analysis:")
    print(f"🤖 Model: gpt-5 ({cost_breakdown['pricing_tier']} processing)")
    print(f"⏱️  Duration: {duration:.2f} seconds")
    
    print(f"\n🔢 Token Breakdown:")
    print(f"   Non-cached input: {cost_breakdown['non_cached_tokens']:,} tokens → ${cost_breakdown['non_cached_cost']:.6f}")
    print(f"   Cached input: {cost_breakdown['cached_tokens']:,} tokens → ${cost_breakdown['cached_cost']:.6f}")
    print(f"   Output: {cost_breakdown['completion_tokens']:,} tokens → ${cost_breakdown['output_cost']:.6f}")
    if cost_breakdown['reasoning_tokens'] > 0:
        print(f"   Reasoning: {cost_breakdown['reasoning_tokens']:,} tokens (included in output)")
    
    print(f"\n💰 Cost Breakdown:")
    print(f"   Token costs: ${cost_breakdown['non_cached_cost'] + cost_breakdown['cached_cost'] + cost_breakdown['output_cost']:.6f}")
    print(f"   Web search: ${cost_breakdown['web_search_cost']:.6f} (~{estimated_search_calls} calls)")
    print(f"   📊 Total estimated: ${cost_breakdown['total_cost']:.6f}")
    
    if actual_cost is not None:
        print(f"   💳 Actual cost: ${actual_cost:.6f}")
        if actual_cost > 0 and cost_breakdown['total_cost'] > 0:
            multiplier = actual_cost / cost_breakdown['total_cost']
            print(f"   📈 Cost multiplier: {multiplier:.1f}x")
    else:
        print(f"   💡 Note: Actual costs may be 2-3x higher due to internal web searches")
    
    print(f"✅ Done at {datetime.now().isoformat()}")
