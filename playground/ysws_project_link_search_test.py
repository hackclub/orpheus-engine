#!/usr/bin/env python3
"""
Minimal GPT-5 + web-search test script with Pydantic structured output.
"""

import os
import time
import requests
from datetime import datetime, date
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


def get_current_usage(api_key: str) -> float:
    """
    Get current total usage cost in USD for today.
    Note: Billing API requires special permissions that regular API keys don't have.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    today = date.today().isoformat()
    
    try:
        response = requests.get(
            f"https://api.openai.com/v1/dashboard/billing/usage?start_date={today}&end_date={today}",
            headers=headers
        )
        response.raise_for_status()
        data = response.json()
        
        # Sum up all costs for today
        total_cost = 0.0
        for day_data in data.get("data", []):
            total_cost += day_data.get("total_cost", 0.0)
        
        return total_cost
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"⚠️ Billing API access denied (API key lacks billing permissions)")
        else:
            print(f"⚠️ Billing API error: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Could not fetch billing data: {e}")
        return None


if __name__ == "__main__":
    # Setup
    load_dotenv()
    API_KEY = os.getenv("OPENAI_API_KEY")
    if not API_KEY:
        raise SystemExit("❌ OPENAI_API_KEY not found in .env")

    client = OpenAI(api_key=API_KEY)

    # Configure project URL
    PROJECT_URL = "https://github.com/ByteAtATime/flare"

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

    print("🔍 Getting baseline billing...")
    cost_before = get_current_usage(API_KEY)
    
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
    model_used = "gpt-5"
    input_rate = 1.25  # $1.25/1M tokens
    output_rate = 10.00  # $10.00/1M tokens
    search_cost = 0.02  # Estimate ~2 search calls @ $0.01 each
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("🔍 Getting actual billing...")
    cost_after = get_current_usage(API_KEY)
    
    # Calculate costs
    input_tokens = response.usage.input_tokens or 0
    output_tokens = response.usage.output_tokens or 0
    
    # Base token cost 
    token_cost = input_tokens * input_rate / 1_000_000 + output_tokens * output_rate / 1_000_000
    estimated_total = token_cost + search_cost
    
    # Actual cost if billing API worked
    if cost_before is not None and cost_after is not None:
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

    print(f"\n📊 Usage & Cost Analysis:")
    print(f"🤖 Model: {model_used}")
    print(f"⏱️  Duration: {duration:.2f} seconds")
    print(f"🔢 Tokens: {input_tokens:,} in, {output_tokens:,} out")
    print(f"💰 Token cost: ${token_cost:.4f} (${input_rate}/1M in, ${output_rate}/1M out)")
    print(f"🔍 Search cost: ${search_cost:.4f}")
    print(f"📊 Total estimated: ${estimated_total:.4f}")
    
    if actual_cost is not None:
        print(f"💳 Actual cost: ${actual_cost:.4f}")
        if actual_cost > 0 and estimated_total > 0:
            multiplier = actual_cost / estimated_total
            print(f"📈 Cost multiplier: {multiplier:.1f}x")
    else:
        print(f"💳 Actual cost: Not available (billing API requires special permissions)")
        if search_cost > 0:
            print(f"💡 Note: Actual costs may be 2-3x higher due to internal web searches")
    
    print(f"✅ Done at {datetime.now().isoformat()}")
