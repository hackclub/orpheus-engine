#!/usr/bin/env python3
"""
Minimal GPT-5 + web-search test script with Pydantic structured output.
"""

import os
import time
import requests
from datetime import datetime
from typing import List

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel


class ProjectLink(BaseModel):
    url: str
    justification: str
    link_score: int  # 0-100 impact score
    publish_date: str
    upvote_count: int = None  # Social media upvotes/likes, or None if not applicable
    comment_count: int = None  # Social media comments/replies, or None if not
    mentions_hack_club: bool
    hack_club_prominence: int  # 0-100 prominence score
    hack_club_prominence_justification: str
    is_hack_club_url: bool


class SearchResponse(BaseModel):
    project_links: List[ProjectLink]
    search_date: str
    total_found: int
    search_complete: bool


def calculate_detailed_cost(usage_data, model: str, web_search_calls: int = 0) -> dict:
    """
    Calculate detailed cost breakdown including cached tokens, reasoning tokens, and tool costs.
    Based on August 2025 OpenAI pricing structure.
    """
    
    # Get token counts from usage data
    prompt_tokens = usage_data.prompt_tokens or 0
    completion_tokens = usage_data.completion_tokens or 0
    
    # Get detailed token breakdowns
    cached_tokens = 0
    reasoning_tokens = 0
    
    if hasattr(usage_data, 'prompt_tokens_details') and usage_data.prompt_tokens_details:
        cached_tokens = usage_data.prompt_tokens_details.cached_tokens or 0
    
    if hasattr(usage_data, 'completion_tokens_details') and usage_data.completion_tokens_details:
        reasoning_tokens = usage_data.completion_tokens_details.reasoning_tokens or 0
    
    # Calculate non-cached input tokens
    non_cached_tokens = prompt_tokens - cached_tokens
    
    # Model-specific pricing (per 1M tokens)
    pricing = {
        "gpt-5": {"input": 1.25, "cached": 0.125, "output": 10.00},
        "gpt-5-mini": {"input": 0.25, "cached": 0.025, "output": 2.00},
        "gpt-5-nano": {"input": 0.05, "cached": 0.005, "output": 0.40}
    }
    
    rates = pricing.get(model, pricing["gpt-5"])  # Default to gpt-5
    
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
        "token_rates": rates
    }


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
    
    if not API_KEY:
        raise SystemExit("❌ OPENAI_API_KEY not found in .env")

    client = OpenAI(api_key=API_KEY)
    
    # Test billing API with simple call first
    if ADMIN_KEY:
        print("🔍 Testing billing API with simple call...")
        cost_before = get_organization_costs(ADMIN_KEY)
        
        # Simple test call
        test_response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": "Say hello"}]
        )
        
        cost_after = get_organization_costs(ADMIN_KEY)
        
        if cost_before is not None and cost_after is not None:
            test_cost = cost_after - cost_before
            print(f"✅ Billing API works! Test cost: ${test_cost:.6f}")
        else:
            print("❌ Billing API not working")
            ADMIN_KEY = None
    else:
        print("⚠️ OPENAI_ADMIN_KEY not found - billing tracking disabled")

    # Configure project URL
    PROJECT_URL = "https://github.com/rsvedant/reelevate.ai"

    # Project viral tracking prompt
    PROMPT = f"""
Here is a project built by a teenager in Hack Club. Find me links to places that project has been shared on the internet in places like Hacker News, Reddit, x.com, and more. Anywhere it could have "gone viral": {PROJECT_URL}

You are an experience researcher. It's crucial you put in the effort to provide comprehensive results.

Make sure you only return results for this project and no other similarly named projects.

For each link return:

- URL
- Justification: Why you included this link
- Link score: From 0-100 how impactful / notable this link is. Ex. Reddit post with 0 upvotes = 0 impact. Reddit / Hacker News / etc post with 50+ upvotes = 100 impact. Re-posted blog post on random site with copied content = 0. Original blog post about the project on theregister.com = 100.
- Publish date: Date (or best guess for date) that the link was published. Must be in ISO 8601 format (YYYY-MM-DD). If the date is not available, put your best estimate.
- Upvote count: If the link is on a social media site like Reddit, Hacker News, x.com, YouTube (including YouTube Shorts), TikTok, or others, include the upvote / like / star count. If it is 0 put 0. If it is not available or applicable, put null. Sometimes it can be hard to get the exact number of upvotes on social media sites. Put in extra effort to get it right.
- Comment count: If the link is on a social media site like Reddit, Hacker News, x.com, YouTube (including YouTube Shorts), TikTok, or others, or a blog with comments, include the comment / reply count. If it is 0 put 0. If it is not available or applicable, put null.
- Whether the link mentions Hack Club (yes / no)
- The prominence of Hack Club in the link. 0 = Hack Club is not mentioned, 50 = Hack Club is mentioned by the author in a comment or there is a positive comment mentioning Hack Club anywhere on the page, 100 = Hack Club is mentioned near the top of the page or has prominence in the page contents (ex. a 1 sentence description of Hack Club or a link to hackclub.com in the post) or there is a very positive comment mentioning Hack Club near the top.
- Hack Club prominence justification: Why you gave the prominence score you did, include exact quotes from the page that mention Hack Club, and explain why you gave the prominence score you did.
- Is Hack Club URL (yes / no): Whether the link is a Hack Club URL (ex. hackclub.com, hackclub.com/xyz, etc.)
    """

    # Get baseline billing if admin key available
    if ADMIN_KEY:
        print("🔍 Getting baseline billing...")
        cost_before = get_organization_costs(ADMIN_KEY)
    
    print("🔍 Querying GPT-5 with web search...")
    start_time = time.time()
    
    response = client.responses.parse(
        model="gpt-5",
        text_format=SearchResponse,
        tools=[{
            "type": "web_search_preview",
            "search_context_size": "high"
        }],
        input=PROMPT
    )
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Get actual billing if admin key available
    if ADMIN_KEY:
        print("🔍 Getting actual billing...")
        cost_after = get_organization_costs(ADMIN_KEY)
    
    # Calculate detailed costs with proper token breakdown
    estimated_search_calls = 2  # Conservative estimate
    cost_breakdown = calculate_detailed_cost(response.usage, "gpt-5", estimated_search_calls)
    
    # Calculate actual cost if available
    if ADMIN_KEY and cost_before is not None and cost_after is not None:
        actual_cost = cost_after - cost_before
    else:
        actual_cost = None

    print(f"\n📦 Structured Response:")
    # output_text is a string, output_parsed is the Pydantic model
    if hasattr(response, 'output_parsed') and response.output_parsed:
        print(response.output_parsed.model_dump_json(indent=2))
    else:
        print("Raw text response:")
        print(response.output_text)

    print(f"\n📊 Detailed Usage & Cost Analysis:")
    print(f"🤖 Model: gpt-5")
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
