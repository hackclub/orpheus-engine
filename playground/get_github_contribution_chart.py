#!/usr/bin/env python3
"""
Fetch full GitHub contribution history (the "green squares") for a user
via Hack Club's gh-proxy GraphQL endpoint and write a CSV of
date,count,color.

Usage:
  - Create a .env file with: GH_PROXY_API_KEY=your_key_here
  - python fetch_github_contributions.py zachlatta --out zachlatta_contributions.csv
"""

import os
import sys
import time
import argparse
import datetime as dt
import csv
import requests
from dotenv import load_dotenv

GH_PROXY_URL = "https://gh-proxy.hackclub.com/gh/graphql"
DEFAULT_OUT = "contributions.csv"
USER_AGENT = "orpheus-engine"
# NOTE: Private contributions cannot be requested via an arg; they're hidden unless the token is the user's.

# Simple, backoff-on-429 POST wrapper
def graphql_post(session, api_key, query, variables, max_retries=5):
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    payload = {"query": query, "variables": variables}
    backoff = 1.0
    for attempt in range(max_retries):
        resp = session.post(GH_PROXY_URL, headers=headers, json=payload, timeout=30)
        # Handle rate limiting
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after else backoff
            time.sleep(sleep_s)
            backoff = min(backoff * 2, 30)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(
                f"GraphQL HTTP {resp.status_code}: {resp.text[:500]}"
            )
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]
    raise RuntimeError("Exceeded retry attempts due to rate limiting (429).")


# Query 1: discover contribution years (and createdAt as a fallback)
Q_YEARS = """
query($login: String!) {
  user(login: $login) {
    createdAt
    contributionsCollection {
      contributionYears
    }
  }
}
"""

# Query 2: fetch contribution calendar for a date window (PUBLIC-visible only)
Q_CALENDAR = """
query($login: String!, $from: DateTime!, $to: DateTime!) {
  user(login: $login) {
    contributionsCollection(from: $from, to: $to) {
      contributionCalendar {
        weeks {
          contributionDays {
            date
            contributionCount
            color
          }
        }
      }
      restrictedContributionsCount
    }
  }
}
"""


def iso(dt_obj: dt.datetime) -> str:
    # Ensure UTC Zulu format
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    else:
        dt_obj = dt_obj.astimezone(dt.timezone.utc)
    return dt_obj.isoformat().replace("+00:00", "Z")


def year_bounds(year: int) -> tuple[str, str]:
    start = dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
    end = dt.datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
    return iso(start), iso(end)


def clamp_to_now(end_iso: str) -> str:
    # If the end goes beyond now, clamp to (now + small epsilon) to include today.
    now = dt.datetime.now(dt.timezone.utc)
    end_dt = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    if end_dt > now:
        # Add a minute to be safely beyond "today"
        return iso(now + dt.timedelta(minutes=1))
    return end_iso


def fetch_contribution_years(session, api_key: str, login: str) -> list[int]:
    data = graphql_post(session, api_key, Q_YEARS, {"login": login})
    user = data.get("user")
    if not user:
        raise RuntimeError(f"User '{login}' not found.")
    years = user["contributionsCollection"]["contributionYears"]
    # Fallback: if empty (unlikely), build from createdAt -> current year
    if not years:
        created = dt.datetime.fromisoformat(user["createdAt"].replace("Z", "+00:00"))
        this_year = dt.datetime.now(dt.timezone.utc).year
        years = list(range(created.year, this_year + 1))
    return sorted(years)


def fetch_calendar_for_range(session, api_key: str, login: str, start_iso: str, end_iso: str) -> list[dict]:
    data = graphql_post(
        session,
        api_key,
        Q_CALENDAR,
        {"login": login, "from": start_iso, "to": end_iso},
    )
    coll = data["user"]["contributionsCollection"]
    weeks = coll["contributionCalendar"]["weeks"]
    # Flatten days
    days = []
    for w in weeks:
        for dday in w["contributionDays"]:
            days.append(
                {
                    "date": dday["date"],
                    "count": int(dday["contributionCount"]),
                    "color": dday.get("color") or "",
                }
            )
    return days


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch full GitHub contribution history via gh-proxy GraphQL.")
    parser.add_argument("login", help="GitHub username (e.g., zachlatta)")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"Output CSV path (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    api_key = os.getenv("GH_PROXY_API_KEY")
    if not api_key:
        print("Error: GH_PROXY_API_KEY not set in environment (load from .env)", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()

    # 1) Get the full list of years with contributions
    years = fetch_contribution_years(session, api_key, args.login)
    if not years:
        print("No contribution years found.", file=sys.stderr)
        sys.exit(0)

    # 2) For each year, fetch contributions and merge by date
    #    Using a map to avoid duplicates if date ranges ever overlap.
    by_date = {}

    for year in years:
        start_iso, end_iso = year_bounds(year)
        # Clamp the final year to "now"
        if year == max(years):
            end_iso = clamp_to_now(end_iso)

        days = fetch_calendar_for_range(session, api_key, args.login, start_iso, end_iso)

        for d in days:
            # The API returns each date once per query; across years, there shouldn't be overlap,
            # but we guard anyway and sum counts if a duplicate appears.
            if d["date"] in by_date:
                by_date[d["date"]]["count"] += d["count"]
                # Prefer non-empty color if one is missing
                if not by_date[d["date"]]["color"] and d["color"]:
                    by_date[d["date"]]["color"] = d["color"]
            else:
                by_date[d["date"]] = {"count": d["count"], "color": d["color"]}

    # 3) Write CSV
    rows = sorted(
        ({"date": k, "count": v["count"], "color": v["color"]} for k, v in by_date.items()),
        key=lambda r: r["date"],
    )

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "count", "color"])
        for r in rows:
            writer.writerow([r["date"], r["count"], r["color"]])

    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
