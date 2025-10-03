import hashlib
import polars as pl
import os
import re
import requests
import json
from datetime import datetime, timezone, timedelta
import pytz
import asyncio
import aiohttp
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    Definitions,
    AssetKey,
)
from typing import Dict, Any, List

# Import resources and config
from ..airtable.resources import AirtableResource
from ..airtable.definitions import airtable_config
from ..geocoder.resources import GeocoderResource, GeocodingError
from ..airtable.generated_ids import AirtableIDs
from ..shared.address_utils import build_address_string_from_airtable_row
from ..analytics.definitions import format_airtable_date
from ..dlt.assets import create_airtable_sync_assets
from ..airtable.definitions import create_airtable_assets

# Alias for convenience
UnifiedYSWS = AirtableIDs.unified_ysws_projects_db


def _extract_geocode_details(geocode_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract geocoding details from API response.
    Returns None if required fields are missing.
    """
    if not geocode_result:
        return None
        
    try:
        # Extract coordinates
        lat = geocode_result.get('lat')
        lng = geocode_result.get('lng')
        
        if lat is None or lng is None:
            return None
            
        # Extract country information
        country_name = geocode_result.get('country_name', '')
        country_code = geocode_result.get('country_code', '')
        
        return {
            UnifiedYSWS.approved_projects.geocoded_latitude: float(lat),
            UnifiedYSWS.approved_projects.geocoded_longitude: float(lng),
            UnifiedYSWS.approved_projects.geocoded_country: country_name,
            UnifiedYSWS.approved_projects.geocoded_country_code: country_code,
        }
    except (ValueError, TypeError, KeyError) as e:
        return None


def _build_address_string(row: Dict[str, Any]) -> str:
    """
    Build address string from approved_projects record fields using shared utility.
    Ensures identical formatting to Loops for maximum geocoding cache hits.
    """
    field_ids = {
        "address_line_1": UnifiedYSWS.approved_projects.address_line_1,
        "address_line_2": UnifiedYSWS.approved_projects.address_line_2,
        "city": UnifiedYSWS.approved_projects.city,
        "state_province": UnifiedYSWS.approved_projects.state_province,
        "zip_postal_code": UnifiedYSWS.approved_projects.zip_postal_code,
        "country": UnifiedYSWS.approved_projects.country,
    }
    return build_address_string_from_airtable_row(row, field_ids)


def _extract_hcb_id_from_url(url: str) -> str:
    """
    Extract HCB ID from HCB URLs.
    
    Args:
        url: HCB URL in format https://hcb.hackclub.com/jungle or https://hcb.hackclub.com/jungle/transactions
        
    Returns:
        HCB ID (e.g., 'jungle') or empty string if URL doesn't match format
    """
    if not url:
        return ""
    
    # Pattern to match https://hcb.hackclub.com/ID or https://hcb.hackclub.com/ID/anything
    pattern = r'^https://hcb\.hackclub\.com/([^/]+)(?:/.*)?$'
    match = re.match(pattern, url.strip())
    
    if match:
        return match.group(1)
    
    return ""


def _calculate_archive_hash(code_url: str, playable_url: str, archive_code_url: str = "", archive_live_url: str = "") -> str:
    """
    Calculate archive hash including both source URLs and archive URLs.
    This ensures changes to either source or archive URLs trigger re-archiving.
    """
    hash_string = f"liveurl:{playable_url or ''},codeurl:{code_url or ''},archiveLive:{archive_live_url or ''},archiveCode:{archive_code_url or ''}"
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()


async def _fetch_github_stars_single_async(session: aiohttp.ClientSession, project_id: str, github_repo_url: str, gh_proxy_api_key: str, current_time: datetime, logger=None) -> dict:
    """
    Fetch star count for a single GitHub repository using async/await.
    
    TODO: REFACTOR GH-PROXY INTO DAGSTER RESOURCE
    Currently we're handling gh-proxy API calls directly in this function with hardcoded URLs
    and environment variable access. This should be refactored into a proper Dagster resource
    (similar to AirtableResource) that encapsulates:
    - API key management and validation
    - Base URL configuration  
    - Rate limiting logic and connection pooling
    - Standard error handling and retry logic
    - Consistent logging patterns
    - Reusable across other assets that need GitHub data
    
    The resource should provide methods like:
    - get_repository_info(owner, repo) -> dict
    - get_repositories_bulk(repo_list) -> list[dict] 
    - And handle all the aiohttp session management, auth headers, etc.
    
    Returns:
        Dictionary with project_id and field values for Airtable update
    """
    try:
        # Extract owner/repo from GitHub URL
        url_parts = github_repo_url.replace("https://github.com/", "").split("/")
        if len(url_parts) < 2:
            return {
                "project_id": project_id,
                "success": False,
                "error": f"Invalid GitHub URL format: {github_repo_url}"
            }
            
        owner, repo = url_parts[0], url_parts[1]
        
        # Make API request to gh-proxy
        api_url = f"https://gh-proxy.hackclub.com/gh/repos/{owner}/{repo}"
        
        async with session.get(
            api_url,
            headers={
                "X-API-Key": gh_proxy_api_key,
                "Accept": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            
            if response.status != 200:
                if logger:
                    logger.warning(f"❌ {owner}/{repo}: HTTP {response.status}")
                
                # Set repo_exists based on status code
                if response.status == 404:
                    repo_exists = "Repo 404s"
                else:
                    repo_exists = "Repo 404s"  # Other errors also indicate repo issues
                
                return {
                    "project_id": project_id,
                    "success": False,
                    "repo_exists": repo_exists,
                    "error": f"gh-proxy API returned {response.status} for {owner}/{repo}"
                }
                
            data = await response.json()
            stargazers_count = data.get("stargazers_count", 0)
            language = data.get("language", "")  # Extract primary language
            
            if logger:
                logger.info(f"✅ {owner}/{repo}: {stargazers_count} stars, {language or 'Unknown'} language")
            
            return {
                "project_id": project_id,
                "success": True,
                "repo_exists": "Repo Exists",
                "stars": int(stargazers_count),
                "language": language or "",
                "updated_at": current_time,
                "repo": f"{owner}/{repo}"
            }
        
    except aiohttp.ClientError as e:
        return {
            "project_id": project_id,
            "success": False,
            "repo_exists": "Repo 404s",  # Assume repo doesn't exist for client errors
            "error": f"Request failed for GitHub URL {github_repo_url}: {e}"
        }
    except (KeyError, ValueError, TypeError) as e:
        return {
            "project_id": project_id,
            "success": False,
            "repo_exists": None,  # Unknown if repo exists due to parsing error
            "error": f"Error parsing GitHub data for {github_repo_url}: {e}"
        }
    except Exception as e:
        return {
            "project_id": project_id,
            "success": False,
            "repo_exists": None,  # Unknown if repo exists due to unexpected error
            "error": f"Unexpected error processing GitHub URL {github_repo_url}: {e}"
        }


async def _fetch_github_stars_async_all(projects_data: list, gh_proxy_api_key: str, current_time: datetime, logger=None, max_rps: int = 300) -> list:
    """
    Fetch GitHub star counts using async/await - Python equivalent of Promise.all.
    Processes all requests concurrently up to specified RPS with automatic chunking.
    
    Args:
        projects_data: List of dictionaries with project_id and github_repo_url
        gh_proxy_api_key: API key for gh-proxy
        current_time: Current timestamp for updated_at field
        logger: Optional logger for progress updates
        max_rps: Maximum requests per second (default 300)
    
    Returns:
        List of results from all async API calls
    """
    results = []
    
    # Calculate optimal chunk size and timing for the target RPS
    # Use chunks of 100 or max_rps/3, whichever is smaller for good balance
    chunk_size = min(100, max(1, max_rps // 3))
    chunk_interval = chunk_size / max_rps  # Time between chunks to maintain RPS
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=500, limit_per_host=500),
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        
        for i in range(0, len(projects_data), chunk_size):
            chunk = projects_data[i:i + chunk_size]
            chunk_start_time = time.time()
            
            # Create all async tasks for this chunk (Python equivalent of Promise.all)
            tasks = []
            for project_data in chunk:
                task = _fetch_github_stars_single_async(
                    session,
                    project_data["project_id"],
                    project_data["github_repo_url"],
                    gh_proxy_api_key,
                    current_time,
                    logger
                )
                tasks.append(task)
            
            # Execute all requests concurrently (Promise.all equivalent)
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results and handle any exceptions
            for result in chunk_results:
                if isinstance(result, Exception):
                    if logger:
                        logger.error(f"Async request failed: {result}")
                    results.append({
                        "project_id": "unknown",
                        "success": False,
                        "error": str(result)
                    })
                else:
                    results.append(result)
            
            # Rate limiting: ensure we don't exceed max_rps
            chunk_duration = time.time() - chunk_start_time
            if chunk_duration < chunk_interval and i + chunk_size < len(projects_data):
                await asyncio.sleep(chunk_interval - chunk_duration)
    
    return results


def _fetch_github_stars_parallel_wrapper(projects_data: list, gh_proxy_api_key: str, current_time: datetime, logger=None, max_rps: int = 300) -> list:
    """
    Synchronous wrapper for async GitHub stars fetching.
    """
    return asyncio.run(_fetch_github_stars_async_all(projects_data, gh_proxy_api_key, current_time, logger, max_rps))


def _extract_github_repo_url(code_url: str) -> str:
    """
    Extract GitHub repository URL from various GitHub URL formats.
    Skips pull request URLs, blob URLs (file-specific URLs), and tree URLs (directory-specific URLs) as they are not valid candidates for star counting.
    
    Args:
        code_url: URL that may contain a GitHub repository reference
        
    Returns:
        Base GitHub repository URL (e.g., 'https://github.com/owner/repo') or empty string if not a GitHub URL or is a pull request/blob/tree
        
    Examples:
        https://github.com/zachlatta/sshtron -> https://github.com/zachlatta/sshtron
        https://github.com/zachlatta/sshtron/pull/123 -> "" (skipped)
        https://github.com/zachlatta/sshtron/tree/main -> "" (skipped)
        https://github.com/hackclub/bakebuild/blob/main/cutters/panda.step -> "" (skipped)
        https://github.com/hackclub/browserbuddy/tree/main/submissions/websummarizer -> "" (skipped)
    """
    if not code_url:
        return ""
    
    # Skip pull request URLs
    if '/pull/' in code_url or '/pulls' in code_url:
        return ""
    
    # Skip blob URLs (file-specific URLs)
    if '/blob/' in code_url:
        return ""
    
    # Skip tree URLs (directory-specific URLs)
    if '/tree/' in code_url:
        return ""
    
    # Pattern to match GitHub URLs and extract owner/repo
    pattern = r'^https://github\.com/([^/]+)/([^/]+)(?:/.*)?$'
    match = re.match(pattern, code_url.strip())
    
    if match:
        owner, repo = match.groups()
        return f"https://github.com/{owner}/{repo}"
    
    return ""


def _convert_to_lower_camel_case(name: str) -> str:
    """
    Convert a program name to lowerCamelCase for loops engagement prefix.
    
    Args:
        name: Program name to convert
        
    Returns:
        lowerCamelCase version of the name
    """
    if not name:
        return ""
    
    # Split on spaces and special characters, filter out empty strings
    words = [word for word in name.replace('-', ' ').replace('_', ' ').split() if word]
    
    if not words:
        return ""
    
    # First word lowercase, subsequent words capitalized
    result = words[0].lower()
    for word in words[1:]:
        result += word.capitalize()
    
    return result


@asset(
    group_name="unified_ysws_db_processing",
    description="Prepares approved projects that have GitHub URLs in code_url field for star count retrieval",
    compute_kind="data_preparation",
    deps=[AssetKey(["airtable", "unified_ysws_projects_db", "approved_projects"])],
    required_resource_keys={"airtable"},
)
def approved_projects_repo_stats_candidates(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """
    Loads approved projects and identifies those with GitHub URLs for repository stats processing.
    
    Returns:
        DataFrame with id, code_url, and github_repo_url for projects with valid GitHub URLs
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    
    # Get the approved_projects data from airtable
    projects_df = airtable.get_all_records_as_polars(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="approved_projects",
    )
    
    log.info(f"Processing {projects_df.height} approved projects for GitHub URL detection")
    
    # Get the field IDs
    code_url_field_id = UnifiedYSWS.approved_projects.code_url
    stars_field_id = UnifiedYSWS.approved_projects.repo_star_count
    updated_at_field_id = UnifiedYSWS.approved_projects.repo_stats_last_updated_at
    language_field_id = UnifiedYSWS.approved_projects.repo_language
    exists_field_id = UnifiedYSWS.approved_projects.repo_exists
    
    if code_url_field_id not in projects_df.columns:
        log.warning(f"Code URL field {code_url_field_id} not found in projects data")
        # Return empty DataFrame with correct schema
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                "code_url": pl.Utf8
            }),
            metadata={"num_projects": 0, "num_candidates": 0}
        )
    
    # Calculate 24 hours ago threshold
    twenty_four_hours_ago = datetime.now(timezone.utc) - timedelta(hours=24)
    
    # Filter for projects that have code_url field set
    projects_with_urls = projects_df.filter(
        (pl.col(code_url_field_id).is_not_null()) & 
        (pl.col(code_url_field_id) != "")
    )
    
    # Apply 24-hour freshness filter: only process projects that need updating
    has_updated_at_field = updated_at_field_id in projects_df.columns
    
    if has_updated_at_field:
        # Filter for records that need checking:
        # 1. Updated at field is null/empty, OR
        # 2. Last updated was more than 24 hours ago
        processed_df = projects_with_urls.filter(
            (pl.col(updated_at_field_id).is_null()) |
            (pl.col(updated_at_field_id) == "") |
            (pl.col(updated_at_field_id) < twenty_four_hours_ago.isoformat())
        )
        log.info(f"Applied 24-hour freshness filter: {processed_df.height}/{projects_with_urls.height} projects need repo stats updates")
    else:
        # No existing tracking field, process all projects with code_url (first time setup)
        processed_df = projects_with_urls
        log.info("No existing repo_stats_last_updated_at field found, processing all projects with code_url")
    
    # Return just id and code_url - let repo_stats asset handle GitHub URL detection
    processed_df = processed_df.select([
        pl.col("id"),
        pl.col(code_url_field_id).alias("code_url")
    ])
    
    log.info(f"Found {processed_df.height} projects needing repo stats updates out of {projects_df.height} total projects")
    if processed_df.height > 0:
        log.info(f"Sample candidates: {processed_df.head(3).to_dicts()}")
    
    # Generate preview metadata
    if processed_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(processed_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(processed_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No projects need repo stats updates")
    
    return Output(
        processed_df,
        metadata={
            "num_projects": projects_df.height,
            "num_candidates": processed_df.height,
            "preview": preview_metadata
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Fetches GitHub repository stats for approved projects using gh-proxy API",
    compute_kind="api_request",
)
def approved_projects_repo_stats(
    context: AssetExecutionContext,
    approved_projects_repo_stats_candidates: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Fetches repository stats (stars, language, exists) for GitHub repositories using async/await (up to 100 requests per second).
    
    Returns:
        DataFrame with id, repo_star_count, repo_stats_last_updated_at, repo_language, and repo_exists fields
    """
    log = context.log
    input_df = approved_projects_repo_stats_candidates
    
    if input_df.height == 0:
        log.info("No GitHub URL candidates to process.")
        stars_field_id = UnifiedYSWS.approved_projects.repo_star_count
        updated_at_field_id = UnifiedYSWS.approved_projects.repo_stats_last_updated_at
        language_field_id = UnifiedYSWS.approved_projects.repo_language
        exists_field_id = UnifiedYSWS.approved_projects.repo_exists
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                stars_field_id: pl.Int64,
                updated_at_field_id: pl.Datetime,
                language_field_id: pl.Utf8,
                exists_field_id: pl.Utf8
            }),
            metadata={"num_processed": 0, "num_successful": 0, "num_failed": 0}
        )
    
    # Configurable RPS - easy to adjust here
    max_rps = 100
    log.info(f"Processing GitHub repository stats for {input_df.height} projects in parallel (max {max_rps} req/sec)")
    
    # Get gh-proxy API key from environment
    gh_proxy_api_key = os.getenv("GH_PROXY_API_KEY")
    if not gh_proxy_api_key:
        raise ValueError("GH_PROXY_API_KEY environment variable is required")
    
    current_time = datetime.now(timezone.utc)
    
    # Process each candidate - extract GitHub URLs and classify
    github_projects = []
    non_github_projects = []
    
    for row in input_df.iter_rows(named=True):
        project_id = row.get("id")
        code_url = row.get("code_url")
        
        # Determine if this is a GitHub URL
        github_repo_url = _extract_github_repo_url(code_url) if code_url else ""
        
        if github_repo_url:  # Valid GitHub URL
            github_projects.append({
                "project_id": project_id,
                "github_repo_url": github_repo_url
            })
        else:  # Non-GitHub repo
            non_github_projects.append({
                "project_id": project_id,
                "code_url": code_url
            })
    
    log.info(f"Processing {len(github_projects)} GitHub repos via API and {len(non_github_projects)} non-GitHub repos directly")
    
    # Get GitHub API results
    github_results = []
    if github_projects:
        github_results = _fetch_github_stars_parallel_wrapper(github_projects, gh_proxy_api_key, current_time, log, max_rps)
    
    # Create records for non-GitHub projects (no API calls needed)
    non_github_results = []
    for project in non_github_projects:
        non_github_results.append({
            "project_id": project["project_id"],
            "success": True,  # Successfully processed (just not via API)
            "repo_exists": "Not a GitHub repo",
            "stars": None,
            "language": None,
            "updated_at": current_time
        })
        log.info(f"📝 Project {project['project_id']}: Not a GitHub repo")
    
    # Combine all results
    results = github_results + non_github_results
    
    # Process results and create Airtable records
    stars_field_id = UnifiedYSWS.approved_projects.repo_star_count
    updated_at_field_id = UnifiedYSWS.approved_projects.repo_stats_last_updated_at
    language_field_id = UnifiedYSWS.approved_projects.repo_language
    exists_field_id = UnifiedYSWS.approved_projects.repo_exists
    
    all_records = []
    num_repo_exists_success = 0
    num_repo_404s = 0
    num_not_a_github_repo = 0
    
    for result in results:
        project_id = result["project_id"]
        repo_status = result.get("repo_exists")
        
        all_records.append({
            "id": project_id,
            stars_field_id: result.get("stars"),
            updated_at_field_id: result.get("updated_at", current_time),
            language_field_id: result.get("language"),
            exists_field_id: repo_status
        })
        
        # Count by repo status
        if repo_status == "Repo Exists":
            num_repo_exists_success += 1
        elif repo_status == "Repo 404s":
            num_repo_404s += 1
        elif repo_status == "Not a GitHub repo":
            num_not_a_github_repo += 1
    
    # Create output DataFrame with explicit schema to handle mixed types
    if all_records:
        # Create DataFrame with explicit schema to handle None values properly
        output_df = pl.DataFrame(
            all_records,
            schema={
                "id": pl.Utf8,
                stars_field_id: pl.Int64,
                updated_at_field_id: pl.Datetime,
                language_field_id: pl.Utf8,
                exists_field_id: pl.Utf8
            }
        )
    else:
        output_df = pl.DataFrame(schema={
            "id": pl.Utf8,
            stars_field_id: pl.Int64,
            updated_at_field_id: pl.Datetime,
            language_field_id: pl.Utf8,
            exists_field_id: pl.Utf8
        })
    
    log.info(f"Repository stats processing completed. Repo Exists: {num_repo_exists_success}, Repo 404s: {num_repo_404s}, Not GitHub: {num_not_a_github_repo}")
    
    # Generate preview metadata
    if output_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(output_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(output_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No repository stats processed")
    
    return Output(
        output_df,
        metadata={
            "num_processed": input_df.height,
            "num_repo_exists_success": num_repo_exists_success,
            "num_repo_404s": num_repo_404s,
            "num_not_a_github_repo": num_not_a_github_repo,
            "preview": preview_metadata
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Prepares YSWS programs data for signup analysis by converting names to loops engagement prefixes",
    compute_kind="data_preparation",
    deps=[AssetKey(["airtable", "unified_ysws_projects_db", "ysws_programs"])],
    required_resource_keys={"airtable"},
)
def ysws_programs_sign_up_stats_candidates(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """
    Loads YSWS programs and prepares them for signup analysis.
    
    Returns:
        DataFrame with program id, name, and loops_engagement_prefix (lowerCamelCase)
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    
    # Get the ysws_programs data from airtable
    programs_df = airtable.get_all_records_as_polars(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="ysws_programs",
    )
    
    log.info(f"Processing {programs_df.height} YSWS programs")
    
    # Get the field IDs
    name_field_id = UnifiedYSWS.ysws_programs.name
    override_field_id = UnifiedYSWS.ysws_programs.sign_up_stats_override_prefix
    
    if name_field_id not in programs_df.columns:
        log.error(f"Name field {name_field_id} not found in programs data")
        raise ValueError(f"Missing required field: {name_field_id}")
    
    # Check if override field exists in the data
    has_override_field = override_field_id in programs_df.columns
    log.info(f"Override field present: {has_override_field}")
    
    # Process the data to add loops_engagement_prefix
    if has_override_field:
        # Use override field if set, otherwise convert name to lowerCamelCase
        processed_df = programs_df.with_columns([
            pl.when(
                (pl.col(override_field_id).is_not_null()) & 
                (pl.col(override_field_id) != "")
            ).then(
                pl.col(override_field_id)
            ).otherwise(
                pl.col(name_field_id).map_elements(
                    lambda x: _convert_to_lower_camel_case(x) if x else "",
                    return_dtype=pl.Utf8
                )
            ).alias("loops_engagement_prefix")
        ])
    else:
        # Fallback to name conversion if override field doesn't exist
        processed_df = programs_df.with_columns([
            pl.col(name_field_id).map_elements(
                lambda x: _convert_to_lower_camel_case(x) if x else "",
                return_dtype=pl.Utf8
            ).alias("loops_engagement_prefix")
        ])
    
    # Select final columns and filter
    processed_df = processed_df.select([
        pl.col("id").alias("program_id"),
        pl.col(name_field_id).alias("name"),
        pl.col("loops_engagement_prefix")
    ]).filter(
        # Only include programs with valid names and engagement prefixes
        (pl.col("name").is_not_null()) & 
        (pl.col("name") != "") &
        (pl.col("loops_engagement_prefix") != "")
    )
    
    log.info(f"Prepared {processed_df.height} programs for signup analysis")
    if processed_df.height > 0:
        log.info(f"Sample programs: {processed_df.head(3).to_dicts()}")
    
    # Generate preview metadata
    if processed_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(processed_df.head(5).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(processed_df.head(5)))
    else:
        preview_metadata = MetadataValue.text("No programs found")
    
    return Output(
        processed_df,
        metadata={
            "num_programs": processed_df.height,
            "engagement_prefixes": processed_df["loops_engagement_prefix"].to_list(),
            "preview": preview_metadata
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Prepares YSWS programs with HCB data by extracting HCB IDs from URLs",
    compute_kind="data_preparation",
    deps=[AssetKey(["airtable", "unified_ysws_projects_db", "ysws_programs"])],
    required_resource_keys={"airtable"},
)
def ysws_programs_hcb_candidates(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """
    Loads YSWS programs and extracts HCB IDs from HCB URLs.
    
    Returns:
        DataFrame with id, hcb (URL), and hcb_id (extracted ID)
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    
    # Get the ysws_programs data from airtable
    programs_df = airtable.get_all_records_as_polars(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="ysws_programs",
    )
    
    log.info(f"Processing {programs_df.height} YSWS programs for HCB data")
    
    # Get the HCB field ID
    hcb_field_id = UnifiedYSWS.ysws_programs.hcb
    
    if hcb_field_id not in programs_df.columns:
        log.warning(f"HCB field {hcb_field_id} not found in programs data")
        # Return empty DataFrame with correct schema
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                "hcb": pl.Utf8,
                "hcb_id": pl.Utf8
            }),
            metadata={"num_programs": 0, "num_with_hcb": 0}
        )
    
    # Filter for programs that have HCB field set and extract HCB IDs
    processed_df = programs_df.filter(
        (pl.col(hcb_field_id).is_not_null()) & 
        (pl.col(hcb_field_id) != "")
    ).with_columns([
        pl.col(hcb_field_id).map_elements(
            lambda x: _extract_hcb_id_from_url(x) if x else "",
            return_dtype=pl.Utf8
        ).alias("hcb_id")
    ]).select([
        pl.col("id"),
        pl.col(hcb_field_id).alias("hcb"),
        pl.col("hcb_id")
    ]).filter(
        # Only include programs with valid HCB IDs (skip malformed URLs)
        pl.col("hcb_id") != ""
    )
    
    log.info(f"Found {processed_df.height} programs with valid HCB URLs out of {programs_df.height} total programs")
    if processed_df.height > 0:
        log.info(f"Sample HCB data: {processed_df.head(3).to_dicts()}")
    
    # Generate preview metadata
    if processed_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(processed_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(processed_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No programs with valid HCB URLs found")
    
    return Output(
        processed_df,
        metadata={
            "num_programs": programs_df.height,
            "num_with_hcb": processed_df.height,
            "hcb_ids": processed_df["hcb_id"].to_list(),
            "preview": preview_metadata
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Fetches HCB financial data and calculates total spent from HCB fund",
    compute_kind="api_request",
)
def ysws_programs_hcb_stats(
    context: AssetExecutionContext,
    ysws_programs_hcb_candidates: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Fetches HCB organization data and calculates total spent from HCB fund.
    
    Returns:
        DataFrame with id and total_spent_from_hcb_fund field for Airtable updates
    """
    log = context.log
    input_df = ysws_programs_hcb_candidates
    
    if input_df.height == 0:
        log.info("No HCB candidates to process.")
        total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
        hcb_field_id = UnifiedYSWS.ysws_programs.hcb
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                total_spent_field_id: pl.Float64,
                hcb_field_id: pl.Utf8
            }),
            metadata={"num_processed": 0, "num_successful": 0, "num_failed": 0}
        )
    
    log.info(f"Processing HCB data for {input_df.height} programs")
    
    all_records = []  # Changed from successful_records to include both success and error records
    failed_count = 0
    
    for row in input_df.iter_rows(named=True):
        program_id = row.get("id")
        hcb_id = row.get("hcb_id")
        hcb_url = row.get("hcb")
        
        if not hcb_id:
            log.warning(f"No HCB ID for program {program_id}")
            # Create error record
            total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = UnifiedYSWS.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "No HCB ID extracted" | {hcb_url or "Unknown URL"}'
            })
            failed_count += 1
            continue
            
        try:
            # Make API request to HCB
            api_url = f"https://hcb.hackclub.com/api/v3/organizations/{hcb_id}"
            log.debug(f"Fetching HCB data for {hcb_id}: {api_url}")
            
            response = requests.get(
                api_url,
                headers={"Accept": "application/json"},
                timeout=30
            )
            
            if response.status_code != 200:
                # Try to extract message from JSON response
                try:
                    error_data = response.json()
                    error_message = error_data.get("message", f"HTTP {response.status_code}")
                except:
                    error_message = f"HTTP {response.status_code}"
                
                log.warning(f"HCB API returned {response.status_code} for {hcb_id}: {response.text}")
                # Create error record
                total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
                hcb_field_id = UnifiedYSWS.ysws_programs.hcb
                all_records.append({
                    "id": program_id,
                    total_spent_field_id: None,
                    hcb_field_id: f'ERROR: "{error_message}" | {hcb_url}'
                })
                failed_count += 1
                continue
                
            data = response.json()
            
            # Extract balance information
            balances = data.get("balances", {})
            total_raised = balances.get("total_raised", 0)
            balance_cents = balances.get("balance_cents", 0)
            
            # Calculate total spent: total_raised - balance_cents (convert from cents to dollars)
            total_spent_dollars = (total_raised - balance_cents) / 100.0
            
            # Get Airtable field IDs
            total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = UnifiedYSWS.ysws_programs.hcb
            
            # Add successful record
            all_records.append({
                "id": program_id,
                total_spent_field_id: total_spent_dollars,
                hcb_field_id: None  # No error, so empty
            })
            
            log.info(f"Successfully processed {hcb_id}: total_raised={total_raised}, balance_cents={balance_cents}, total_spent=${total_spent_dollars:.2f}")
            
        except requests.exceptions.RequestException as e:
            log.error(f"Request failed for HCB ID {hcb_id}: {e}")
            # Create error record
            total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = UnifiedYSWS.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "Request failed: {str(e)}" | {hcb_url}'
            })
            failed_count += 1
        except (KeyError, ValueError, TypeError) as e:
            log.error(f"Error parsing HCB data for {hcb_id}: {e}")
            # Create error record
            total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = UnifiedYSWS.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "Data parsing error: {str(e)}" | {hcb_url}'
            })
            failed_count += 1
        except Exception as e:
            log.error(f"Unexpected error processing HCB ID {hcb_id}: {e}")
            # Create error record
            total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = UnifiedYSWS.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "Unexpected error: {str(e)}" | {hcb_url}'
            })
            failed_count += 1
    
    # Create output DataFrame
    total_spent_field_id = UnifiedYSWS.ysws_programs.total_spent_from_hcb_fund
    hcb_field_id = UnifiedYSWS.ysws_programs.hcb
    
    if all_records:
        output_df = pl.DataFrame(all_records)
    else:
        output_df = pl.DataFrame(schema={
            "id": pl.Utf8,
            total_spent_field_id: pl.Float64,
            hcb_field_id: pl.Utf8
        })
    
    successful_count = len([r for r in all_records if r[total_spent_field_id] is not None])
    log.info(f"HCB processing completed. Successful: {successful_count}, Failed: {failed_count}")
    
    # Generate preview metadata
    if output_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(output_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(output_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No HCB data processed successfully")
    
    return Output(
        output_df,
        metadata={
            "num_processed": input_df.height,
            "num_successful": successful_count,
            "num_failed": failed_count,
            "success_rate": round((successful_count / max(input_df.height, 1)) * 100, 2),
            "preview": preview_metadata
        }
    )


def _get_signup_analysis_data(search_terms: List[str]) -> pl.DataFrame:
    """
    Get sign-up analysis data from hack_clubbers for the given search terms.
    
    Args:
        search_terms: List of program engagement prefixes to search for
        
    Returns:
        Polars DataFrame with sign-up analysis results
    """
    # Get warehouse connection URL
    warehouse_url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    
    if not search_terms:
        raise ValueError("No search terms provided")
    
    # Convert search terms to SQL array format, escaping single quotes
    escaped_terms = [term.replace("'", "''") for term in search_terms]
    search_terms_array = "ARRAY['" + "', '".join(escaped_terms) + "']"
    
    query = f"""
    WITH search_terms AS (
        SELECT unnest({search_terms_array}) AS search_term
    )
    SELECT
        st.search_term AS loops_engagement_prefix,
        COUNT(CASE WHEN hc.events_overview LIKE '%' || st.search_term || '%' THEN 1 END) AS sign_ups,
        COUNT(CASE WHEN hc.sign_up_day_events LIKE '%' || st.search_term || '%' THEN 1 END) AS sign_ups_new_to_hack_club
    FROM search_terms st
    CROSS JOIN public_loops_analytics.hack_clubbers hc
    GROUP BY st.search_term
    ORDER BY st.search_term
    """
    
    print(f"DEBUG: Generated SQL query:\n{query}")
    
    # Execute query using polars
    return pl.read_database_uri(query, warehouse_url)


@asset(
    group_name="unified_ysws_db_processing",
    description="Identifies approved projects that need geocoding based on address hash comparison and missing coordinates.",
    compute_kind="address_analysis",
    deps=[AssetKey(["airtable", "unified_ysws_projects_db", "approved_projects"])],
    required_resource_keys={"airtable"},
)
def approved_projects_geocoding_candidates(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """
    Filters approved projects that need geocoding using vectorized Polars operations.
    Much faster than row-by-row iteration for large datasets.
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    
    # Get the approved projects data from airtable
    input_df = airtable.get_all_records_as_polars(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="approved_projects",
    )
    
    log.info(f"Processing {input_df.height} approved projects for geocoding candidates using vectorized operations")
    
    # Use field IDs as variables for cleaner code
    addr1_col = UnifiedYSWS.approved_projects.address_line_1
    addr2_col = UnifiedYSWS.approved_projects.address_line_2
    city_col = UnifiedYSWS.approved_projects.city
    state_col = UnifiedYSWS.approved_projects.state_province
    zip_col = UnifiedYSWS.approved_projects.zip_postal_code
    country_col = UnifiedYSWS.approved_projects.country
    hash_col = UnifiedYSWS.approved_projects.geocoded_address_hash
    lat_col = UnifiedYSWS.approved_projects.geocoded_latitude
    lng_col = UnifiedYSWS.approved_projects.geocoded_longitude
    
    # Check if required address fields exist
    required_fields = [addr1_col, city_col]
    missing_fields = [field for field in required_fields if field not in input_df.columns]
    if missing_fields:
        log.warning(f"Missing required address fields: {missing_fields}")
        return Output(
            pl.DataFrame(schema=input_df.schema),
            metadata={"num_candidates": 0, "reason": "Missing required address fields"}
        )
    
    try:
        # VECTORIZED OPERATIONS - much faster than row iteration!
        df_with_address = input_df.with_columns([
            # Fill null values with empty strings for address construction
            pl.col(addr1_col).fill_null("").alias("addr1_clean"),
            pl.col(addr2_col).fill_null("").alias("addr2_clean") if addr2_col in input_df.columns else pl.lit("").alias("addr2_clean"),
            pl.col(city_col).fill_null("").alias("city_clean"),
            pl.col(state_col).fill_null("").alias("state_clean") if state_col in input_df.columns else pl.lit("").alias("state_clean"),
            pl.col(zip_col).fill_null("").alias("zip_clean") if zip_col in input_df.columns else pl.lit("").alias("zip_clean"),
            pl.col(country_col).fill_null("").alias("country_clean") if country_col in input_df.columns else pl.lit("").alias("country_clean"),
        ]).with_columns([
            # Build address string using vectorized string operations
            pl.format(
                "{}\n{}\n{}, {} {}\n{}",
                pl.col("addr1_clean"),
                pl.col("addr2_clean"), 
                pl.col("city_clean"),
                pl.col("state_clean"),
                pl.col("zip_clean"),
                pl.col("country_clean")
            ).str.strip_chars().alias("address_string")
        ]).with_columns([
            # Calculate SHA256 hash using vectorized operation
            pl.col("address_string")
              .map_elements(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest() if x else "", return_dtype=pl.Utf8)
              .alias("calculated_address_hash")
        ])
        
        # Check if geocoded fields exist in the data
        has_hash_col = hash_col in input_df.columns
        has_lat_col = lat_col in input_df.columns  
        has_lng_col = lng_col in input_df.columns
        
        log.info(f"Geocoded fields present - Hash: {has_hash_col}, Lat: {has_lat_col}, Lng: {has_lng_col}")
        
        # Build filter conditions based on available fields
        base_condition = (
            (pl.col("addr1_clean") != "") &  # Has address line 1
            (pl.col("city_clean") != "") &   # Has city
            (pl.col("address_string") != "")  # Has valid address string
        )
        
        if has_hash_col and has_lat_col and has_lng_col:
            # All geocoded fields exist - check for changes or missing data
            geocoding_condition = (
                (pl.col("calculated_address_hash") != pl.col(hash_col)) |
                pl.col(lat_col).is_null() |
                pl.col(lng_col).is_null()
            )
        elif has_lat_col and has_lng_col:
            # Only coordinate fields exist - check for missing coordinates
            geocoding_condition = pl.col(lat_col).is_null() | pl.col(lng_col).is_null()
        else:
            # No geocoded fields exist - all valid addresses need geocoding
            geocoding_condition = pl.lit(True)
            
        candidates_df = df_with_address.filter(
            base_condition & geocoding_condition
        ).drop([
            # Clean up temporary columns
            "addr1_clean", "addr2_clean", "city_clean", 
            "state_clean", "zip_clean", "country_clean"
        ])
        
        log.info(f"Found {candidates_df.height} candidates out of {input_df.height} records using vectorized operations")
        
        return Output(
            candidates_df,
            metadata={
                "num_input_records": input_df.height,
                "num_candidates": candidates_df.height,
                "candidate_percentage": round((candidates_df.height / max(input_df.height, 1)) * 100, 2),
                "processing_method": "vectorized_polars"
            }
        )
        
    except Exception as e:
        log.error(f"Error in vectorized candidate identification: {e}")
        # Fallback to empty result
        return Output(
            pl.DataFrame(schema=input_df.schema),
            metadata={"num_candidates": 0, "error": str(e)}
        )


@asset(
    group_name="unified_ysws_db_processing",
    description="Identifies approved projects needing URL archiving",
    compute_kind="data_preparation",
    deps=[AssetKey(["airtable", "unified_ysws_projects_db", "approved_projects"])],
    required_resource_keys={"airtable"},
)
def approved_projects_archive_candidates(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """
    Filters approved projects that have URLs but are missing archive URLs.
    Targets projects with code_url/playable_url but no corresponding archive URLs.
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    
    log.info("Fetching approved projects from Airtable")
    input_df = airtable.get_all_records_as_polars(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="approved_projects",
    )
    log.info(f"Retrieved {input_df.height} approved projects from Airtable")
    
    # Field IDs
    code_url_col = UnifiedYSWS.approved_projects.code_url
    playable_url_col = UnifiedYSWS.approved_projects.playable_url
    archive_hash_col = UnifiedYSWS.approved_projects.archive_hash
    approved_at_col = UnifiedYSWS.approved_projects.approved_at
    
    # Calculate archive hash and cutoff date (1 week ago)
    eastern = pytz.timezone('US/Eastern')
    one_week_ago = datetime.now(eastern) - timedelta(weeks=1)
    cutoff_date_str = one_week_ago.astimezone(pytz.UTC).isoformat()
    log.info(f"Archive cutoff date: {cutoff_date_str} (1 week ago)")
    
    log.info("Calculating archive hashes and filtering candidates")
    
    # Get archive URL field IDs for hash calculation
    archive_code_url_col = UnifiedYSWS.approved_projects.archive_code_url
    archive_live_url_col = UnifiedYSWS.approved_projects.archive_live_url
    
    candidates = input_df.with_columns([
        pl.col(code_url_col).fill_null("").alias("code_url_clean"),
        pl.col(playable_url_col).fill_null("").alias("playable_url_clean"),
        pl.col(archive_code_url_col).fill_null("").alias("archive_code_url_clean"),
        pl.col(archive_live_url_col).fill_null("").alias("archive_live_url_clean")
    ]).with_columns([
        pl.concat_str([
            pl.lit("liveurl:"), pl.col("playable_url_clean"),
            pl.lit(",codeurl:"), pl.col("code_url_clean"),
            pl.lit(",archiveLive:"), pl.col("archive_live_url_clean"),
            pl.lit(",archiveCode:"), pl.col("archive_code_url_clean")
        ]).map_elements(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest() if x else "",
            return_dtype=pl.Utf8
        ).alias("calculated_archive_hash")
    ]).filter(
        # Has URLs
        ((pl.col(code_url_col).is_not_null() & (pl.col(code_url_col) != "")) |
         (pl.col(playable_url_col).is_not_null() & (pl.col(playable_url_col) != ""))) &
        # After cutoff date
        (pl.col(approved_at_col) > cutoff_date_str) &
        # Hash changed (URLs changed since last archive) OR never archived before (hash is null/empty)
        ((pl.col(archive_hash_col).is_null()) | 
         (pl.col(archive_hash_col) == "") |
         (pl.col("calculated_archive_hash") != pl.col(archive_hash_col)))
    )
    
    log.info(f"Found {candidates.height} projects needing archiving out of {input_df.height} total")
    
    return Output(
        candidates,
        metadata={
            "num_candidates": candidates.height,
            "total_projects": input_df.height,
            "cutoff_date": cutoff_date_str
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Archives URLs using the new archive.hackclub.com API",
    compute_kind="archive_api",
)
def approved_projects_archived(
    context: AssetExecutionContext,
    approved_projects_archive_candidates: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Archives URLs using the simplified archive.hackclub.com API.
    Returns DataFrame with id and archive hash fields for Airtable updates.
    """
    log = context.log
    input_df = approved_projects_archive_candidates
    
    if input_df.height == 0:
        log.info("No archive candidates to process")
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                UnifiedYSWS.approved_projects.archive_code_url: pl.Utf8,
                UnifiedYSWS.approved_projects.archive_live_url: pl.Utf8,
                UnifiedYSWS.approved_projects.archive_archived_at: pl.Utf8,
                UnifiedYSWS.approved_projects.archive_hash: pl.Utf8,
            }),
            metadata={"num_processed": 0, "num_successful": 0, "num_failed": 0}
        )
    
    log.info(f"Starting archive processing for {input_df.height} projects")
    
    # Field IDs
    code_url_col = UnifiedYSWS.approved_projects.code_url
    playable_url_col = UnifiedYSWS.approved_projects.playable_url
    archive_code_url_col = UnifiedYSWS.approved_projects.archive_code_url
    archive_live_url_col = UnifiedYSWS.approved_projects.archive_live_url
    archive_archived_at_col = UnifiedYSWS.approved_projects.archive_archived_at
    archive_hash_col = UnifiedYSWS.approved_projects.archive_hash
    
    # Archive API setup
    archive_api_key = os.getenv("ARCHIVE_HACKCLUB_COM_API_KEY")
    if not archive_api_key:
        raise ValueError("ARCHIVE_HACKCLUB_COM_API_KEY environment variable is not set")
    
    successful_records = []
    failed_count = 0
    
    for row in input_df.iter_rows(named=True):
        project_id = row.get("id")
        code_url = (row.get(code_url_col) or "").strip()
        playable_url = (row.get(playable_url_col) or "").strip()
        calculated_archive_hash = row.get("calculated_archive_hash", "")
        
        urls_to_archive = []
        if code_url:
            urls_to_archive.append(code_url)
        if playable_url:
            urls_to_archive.append(playable_url)
        
        if not urls_to_archive:
            log.warning(f"No URLs to archive for project {project_id}")
            failed_count += 1
            continue
        
        # Archive each URL and capture the archive URLs
        project_success = True
        archive_code_url = None
        archive_live_url = None
        
        for url in urls_to_archive:
            try:
                log.debug(f"Archiving URL for project {project_id}: {url}")
                response = requests.post(
                    "https://archive.hackclub.com/api/v1/archive",
                    headers={
                        "Authorization": f"Bearer {archive_api_key}",
                        "Content-Type": "application/json"
                    },
                    json={"url": url},
                    timeout=30
                )
                
                if response.status_code == 200:
                    archive_response = response.json()
                    archive_url = archive_response.get("url")
                    
                    if archive_url:
                        # Determine if this is a code URL or live URL
                        if url == code_url:
                            archive_code_url = archive_url
                            log.debug(f"Got archive URL for code: {archive_url}")
                        elif url == playable_url:
                            archive_live_url = archive_url
                            log.debug(f"Got archive URL for live: {archive_url}")
                    else:
                        log.warning(f"Archive API returned success but no URL for {url}")
                        project_success = False
                else:
                    log.warning(f"Archive API returned {response.status_code} for {url}: {response.text}")
                    project_success = False
                    
            except Exception as e:
                log.error(f"Failed to archive {url} for project {project_id}: {e}")
                project_success = False
        
        if project_success:
            # Calculate final hash including the new archive URLs
            final_hash = _calculate_archive_hash(
                code_url, 
                playable_url, 
                archive_code_url or "", 
                archive_live_url or ""
            )
            
            # Record successful archiving with archive URLs and updated hash
            current_time = datetime.now(timezone.utc).isoformat()
            record = {
                "id": project_id,
                archive_archived_at_col: current_time,
                archive_hash_col: final_hash  # Use final hash that includes archive URLs
            }
            
            # Add archive URLs if we got them
            if archive_code_url:
                record[archive_code_url_col] = archive_code_url
            if archive_live_url:
                record[archive_live_url_col] = archive_live_url
                
            successful_records.append(record)
            
            archived_types = []
            if archive_code_url:
                archived_types.append("code")
            if archive_live_url:
                archived_types.append("live")
            log.info(f"Successfully archived {'/'.join(archived_types)} URLs for project {project_id}")
        else:
            failed_count += 1
    
    # Create output DataFrame
    output_schema = {
        "id": pl.Utf8,
        archive_code_url_col: pl.Utf8,
        archive_live_url_col: pl.Utf8,
        archive_archived_at_col: pl.Utf8,
        archive_hash_col: pl.Utf8,
    }
    
    output_df = pl.DataFrame(successful_records, schema=output_schema) if successful_records else pl.DataFrame(schema=output_schema)
    successful_count = len(successful_records)
    
    log.info(f"Archive processing completed. Success: {successful_count}, Failed: {failed_count}")
    
    return Output(
        output_df,
        metadata={
            "num_candidates": input_df.height,
            "num_successful": successful_count,
            "num_failed": failed_count,
            "success_rate": round((successful_count / max(input_df.height, 1)) * 100, 2)
        }
    )





@asset(
    group_name="unified_ysws_db_processing",
    description="Geocodes approved projects that were identified as candidates, returning only newly geocoded records.",
    required_resource_keys={"geocoder_client"},
    compute_kind="hackclub_geocoder",
)
def approved_projects_geocoded(
    context: AssetExecutionContext,
    approved_projects_geocoding_candidates: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Performs geocoding on candidate records and returns successfully geocoded records.
    """
    log = context.log
    geocoder: GeocoderResource = context.resources.geocoder_client
    input_df = approved_projects_geocoding_candidates
    
    if input_df.height == 0:
        log.info("No geocoding candidates found.")
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                UnifiedYSWS.approved_projects.geocoded_latitude: pl.Float64,
                UnifiedYSWS.approved_projects.geocoded_longitude: pl.Float64,
                UnifiedYSWS.approved_projects.geocoded_country: pl.Utf8,
                UnifiedYSWS.approved_projects.geocoded_country_code: pl.Utf8,
                UnifiedYSWS.approved_projects.geocoded_address_hash: pl.Utf8,
            }),
            metadata={"num_candidates": 0, "num_geocoded": 0, "num_errors": 0}
        )
    
    newly_geocoded_records = []
    geocoded_count = 0
    error_count = 0
    geocode_limit = -1  # No limit for now
    
    log.info(f"Starting geocoding process for {input_df.height} candidates")
    
    for row in input_df.iter_rows(named=True):
        if geocode_limit != -1 and geocoded_count >= geocode_limit:
            log.info(f"Reached geocoding limit of {geocode_limit}")
            break
            
        record_id = row.get("id")
        address_string = row.get("address_string")
        calculated_hash = row.get("calculated_address_hash")
        
        if not address_string or not calculated_hash:
            log.warning(f"Missing address string or hash for record {record_id}")
            error_count += 1
            continue
            
        try:
            log.debug(f"Geocoding address for record {record_id}: {address_string.replace('\n', ' ')}")
            geocode_result = geocoder.geocode(address_string)
            
            if not geocode_result:
                log.warning(f"No geocoding results for record {record_id}")
                error_count += 1
                continue
                
            extracted_details = _extract_geocode_details(geocode_result)
            
            if extracted_details:
                geocoded_count += 1
                output_record = {
                    "id": record_id,
                    **extracted_details,
                    UnifiedYSWS.approved_projects.geocoded_address_hash: calculated_hash
                }
                newly_geocoded_records.append(output_record)
                log.info(f"Successfully geocoded record {record_id} ({geocoded_count})")
            else:
                log.warning(f"Could not extract geocode details for record {record_id}")
                error_count += 1
                
        except GeocodingError as e:
            log.error(f"Geocoding failed for record {record_id}: {e}")
            error_count += 1
        except Exception as e:
            log.error(f"Unexpected error geocoding record {record_id}: {e}")
            error_count += 1
    
    log.info(f"Geocoding completed. Success: {geocoded_count}, Errors: {error_count}")
    
    # Create output DataFrame
    output_schema = {
        "id": pl.Utf8,
        UnifiedYSWS.approved_projects.geocoded_latitude: pl.Float64,
        UnifiedYSWS.approved_projects.geocoded_longitude: pl.Float64,
        UnifiedYSWS.approved_projects.geocoded_country: pl.Utf8,
        UnifiedYSWS.approved_projects.geocoded_country_code: pl.Utf8,
        UnifiedYSWS.approved_projects.geocoded_address_hash: pl.Utf8,
    }
    
    if newly_geocoded_records:
        output_df = pl.DataFrame(newly_geocoded_records, schema=output_schema)
    else:
        output_df = pl.DataFrame(schema=output_schema)
    
    # Generate preview metadata
    preview_limit = 10
    if output_df.height > 0:
        preview_df = output_df.head(preview_limit)
        try:
            preview_metadata = MetadataValue.md(preview_df.to_pandas().to_markdown(index=False))
        except Exception as md_err:
            log.warning(f"Could not generate markdown preview: {md_err}")
            preview_metadata = MetadataValue.text(str(preview_df))
    else:
        preview_metadata = MetadataValue.text("No records were geocoded.")
    
    return Output(
        output_df,
        metadata={
            "num_candidates": input_df.height,
            "num_geocoded": geocoded_count,
            "num_errors": error_count,
            "success_rate": round((geocoded_count / max(input_df.height, 1)) * 100, 2),
            "preview": preview_metadata,
            "num_output_records": output_df.height
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Prepares approved projects data for Airtable batch update by merging geocoded and archived data.",
    compute_kind="data_preparation",
)
def approved_projects_prepared_for_update(
    context: AssetExecutionContext,
    approved_projects_geocoded: pl.DataFrame,
    approved_projects_archived: pl.DataFrame,
    approved_projects_repo_stats: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Merges geocoded, archived, and GitHub repository stats data for approved projects and prepares for Airtable batch update.
    
    Checks for conflicts: if multiple inputs provide different non-null values 
    for the same field for a given project ID, an error is raised. 
    Otherwise, the single non-null value (or null) is kept.
    """
    log = context.log
    
    # --- 1. Collect and Filter Input DataFrames ---
    dfs = [approved_projects_geocoded, approved_projects_archived, approved_projects_repo_stats]
    
    input_heights = {
        "geocoded": approved_projects_geocoded.height,
        "archived": approved_projects_archived.height,
        "repo_stats": approved_projects_repo_stats.height,
    }
    log.info(f"Input counts - Geocoded: {input_heights['geocoded']}, Archived: {input_heights['archived']}, Repo Stats: {input_heights['repo_stats']}")

    dfs = [df for df in dfs if not df.is_empty()]

    if not dfs:
        log.warning("All input DataFrames for merging are empty.")
        return Output(
            pl.DataFrame(), 
            metadata={
                "num_output_records": 0,
                "notes": "All input assets were empty.",
                **{f"num_{name}_input_records": h for name, h in input_heights.items()}
            }
        )

    # --- 2. Determine Union of Columns and Target Schema ---
    all_cols = set()
    schemas = {} # Store schema info {col: [non-null dtypes]}
    for df in dfs:
        all_cols.update(df.columns)
        for col, dtype in df.schema.items():
            if col not in schemas: schemas[col] = []
            if dtype != pl.Null: # Only consider non-null types
                schemas[col].append(dtype)

    if "id" not in all_cols:
        raise ValueError("Input DataFrames must contain an 'id' column for merging.")
        
    value_cols = sorted(list(all_cols - {"id"})) 
    required_schema_cols = ["id"] + value_cols

    # Determine the target dtype for each column (use first non-null type found)
    target_schema = {}
    for col in required_schema_cols:
        potential_types = schemas.get(col, [])
        target_type = next((t for t in potential_types if t != pl.Null), pl.Null) 
        target_schema[col] = target_type

    # --- 3. Adjust Schemas and Concatenate ---
    adjusted_dfs = []
    for i, df in enumerate(dfs):
        current_cols = set(df.columns)
        select_exprs = []
        
        for col in required_schema_cols:
            target_type = target_schema[col]
            
            if col in current_cols:
                # Column exists, check if casting is needed
                current_type = df.schema[col]
                if current_type != target_type and target_type != pl.Null:
                    select_exprs.append(pl.col(col).cast(target_type))
                else:
                    select_exprs.append(pl.col(col))
            else:
                # Column is missing, add it with the target type
                if target_type == pl.Null:
                    select_exprs.append(pl.lit(None).alias(col))
                else:
                    select_exprs.append(pl.lit(None).cast(target_type).alias(col))

        adjusted_df = df.select(select_exprs)
        adjusted_dfs.append(adjusted_df)

    # Concatenate using vertical_relaxed strategy
    combined_long = pl.concat(adjusted_dfs, how="vertical_relaxed") 
        
    # --- 4. Group, Aggregate, and Check Conflicts ---
    agg_exprs = []
    conflict_check_cols = []
    for col in value_cols:
        unique_count_expr = pl.col(col).drop_nulls().unique().count()
        first_value_expr = pl.col(col).drop_nulls().first()
        conflict_col_name = f"_{col}_has_conflict_" 

        final_value_expr = (
            pl.when(unique_count_expr == 1)
            .then(first_value_expr)
            .otherwise(pl.lit(None))
            .alias(col)
        )
        
        conflict_flag_expr = (
            (unique_count_expr > 1)
            .alias(conflict_col_name)
        )

        agg_exprs.append(final_value_expr)
        agg_exprs.append(conflict_flag_expr)
        conflict_check_cols.append(conflict_col_name)

    # Perform the aggregation
    grouped = combined_long.group_by("id").agg(agg_exprs)

    # --- 5. Check for Conflicts ---
    conflict_rows = grouped.filter(pl.any_horizontal(pl.col(c) for c in conflict_check_cols))

    if not conflict_rows.is_empty():
        error_details = []
        for row in conflict_rows.head(5).iter_rows(named=True):
            project_id = row["id"]
            conflicting_fields = [
                c.replace("_has_conflict_", "").strip("_") 
                for c in conflict_check_cols if row[c]
            ]
            error_details.append(f"Project ID '{project_id}': Conflicts in fields: {', '.join(conflicting_fields)}")
        
        raise ValueError(f"Conflicting field values found:\n" + "\n".join(error_details))

    # --- 6. Clean and Return ---
    # Remove conflict check columns from the final result
    final_df = grouped.drop(conflict_check_cols)

    log.info(f"Successfully merged {len(dfs)} input DataFrames into {final_df.height} records")

    # Generate preview metadata
    if final_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(final_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(final_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No records prepared for update.")

    return Output(
        final_df,
        metadata={
            "num_records_prepared": final_df.height,
            "num_geocoded_input": input_heights["geocoded"],
            "num_archived_input": input_heights["archived"],
            "num_repo_stats_input": input_heights["repo_stats"],
            "update_columns": list(final_df.columns),
            "preview": preview_metadata
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Updates approved projects in Airtable with newly geocoded data.",
    required_resource_keys={"airtable"},
    compute_kind="airtable_update",
)
def approved_projects_update_status(
    context: AssetExecutionContext,
    approved_projects_prepared_for_update: pl.DataFrame,
) -> Output[None]:
    """
    Performs batch update of approved projects in Airtable with geocoded data.
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    input_df = approved_projects_prepared_for_update
    
    if input_df.height == 0:
        log.info("No records to update in Airtable.")
        return Output(
            None,
            metadata={"updates_attempted": 0, "updates_successful": 0, "updates_failed": 0}
        )
    
    log.info(f"Starting Airtable update for {input_df.height} approved projects")
    
    # Convert DataFrame to list of dictionaries for batch update
    records_to_update = []
    
    # Get datetime columns from DataFrame schema
    datetime_columns = {col for col, dtype in input_df.schema.items() if dtype == pl.Datetime}
    
    for row in input_df.iter_rows(named=True):
        record = {}
        for k, v in row.items():
            if v is not None:
                # Convert datetime columns to ISO format strings for Airtable
                if k in datetime_columns:
                    record[k] = format_airtable_date(v)
                else:
                    record[k] = v
        records_to_update.append(record)
    
    # Perform batch update
    result = airtable.batch_update_records(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="approved_projects",
        records=records_to_update,
    )
    
    successful_count = result['successful']
    failed_count = result['failed']
    total_attempted = len(records_to_update)
    success_rate = round((successful_count / max(total_attempted, 1)) * 100, 2)
    
    log.info(f"Airtable update completed. Successful: {successful_count}, Failed: {failed_count}")
    
    # Fail the asset if there were significant failures (>50% failure rate)
    if failed_count > 0 and success_rate < 50:
        error_msg = f"High failure rate: {failed_count}/{total_attempted} updates failed ({100-success_rate:.1f}% failure rate)"
        log.error(error_msg)
        raise Exception(error_msg)
    
    # Warn if there were any failures but still mostly successful
    if failed_count > 0:
        log.warning(f"Some updates failed: {failed_count}/{total_attempted} records")
    
    return Output(
        None,
        metadata={
            "updates_attempted": total_attempted,
            "updates_successful": successful_count,
            "updates_failed": failed_count,
            "success_rate": success_rate,
            "status": "failed" if success_rate < 50 else ("partial" if failed_count > 0 else "success")
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Calculates sign-up stats for YSWS programs with Airtable field mappings",
    compute_kind="signup_analysis",
    deps=[AssetKey(["loops_analytics", "hack_clubbers"])]
)
def ysws_programs_sign_up_stats(
    context: AssetExecutionContext,
    ysws_programs_sign_up_stats_candidates: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Calculates sign-up statistics for YSWS programs and returns data formatted for Airtable updates.
    
    Returns:
        DataFrame with id, total_sign_ups field, and total_sign_ups_new_to_hack_club field with calculated values
    """
    log = context.log
    
    # Get search terms from the programs data
    search_terms = ysws_programs_sign_up_stats_candidates["loops_engagement_prefix"].to_list()
    programs_data = ysws_programs_sign_up_stats_candidates
    
    log.info(f"Executing sign-up analysis query for {len(search_terms)} search terms: {search_terms}")
    
    try:
        # Get sign-up analysis data
        signup_data = _get_signup_analysis_data(search_terms)
        
        log.info(f"Successfully retrieved sign-up data for {signup_data.height} program(s)")
        log.info(f"Sign-up data preview: {signup_data.to_dicts()}")
        
        # Get Airtable field IDs
        total_signups_field_id = UnifiedYSWS.ysws_programs.total_sign_ups
        total_signups_new_field_id = UnifiedYSWS.ysws_programs.total_sign_ups_new_to_hack_club
        
        # Merge programs data with signup data
        merged_data = programs_data.join(
            signup_data,
            left_on="loops_engagement_prefix",
            right_on="loops_engagement_prefix",
            how="left"
        ).with_columns([
            # Fill nulls with 0 for programs with no signup data
            pl.col("sign_ups").fill_null(0),
            pl.col("sign_ups_new_to_hack_club").fill_null(0)
        ]).select([
            pl.col("program_id").alias("id"),
            pl.col("sign_ups").alias(total_signups_field_id),
            pl.col("sign_ups_new_to_hack_club").alias(total_signups_new_field_id)
        ])
        
        # Calculate totals for metadata
        total_signups = int(merged_data[total_signups_field_id].sum())
        total_new_signups = int(merged_data[total_signups_new_field_id].sum())
        
        # Generate preview metadata
        if merged_data.height > 0:
            try:
                preview_metadata = MetadataValue.md(merged_data.to_pandas().to_markdown(index=False))
            except Exception:
                preview_metadata = MetadataValue.text(str(merged_data))
        else:
            preview_metadata = MetadataValue.text("No signup data found")
        
        return Output(
            merged_data,
            metadata={
                "num_programs_analyzed": len(search_terms),
                "total_signups": total_signups,
                "total_new_signups": total_new_signups,
                "programs_analyzed": search_terms,
                "preview": preview_metadata
            }
        )
        
    except Exception as e:
        log.error(f"Error executing sign-up analysis query: {e}")
        raise


@asset(
    group_name="unified_ysws_db_processing",
    description="Prepares YSWS programs data for Airtable batch update by merging sign-up stats and HCB data.",
    compute_kind="data_preparation",
)
def ysws_programs_prepared_for_update(
    context: AssetExecutionContext,
    ysws_programs_sign_up_stats: pl.DataFrame,
    ysws_programs_hcb_stats: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Merges sign-up stats and HCB data for YSWS programs and prepares for Airtable batch update.
    
    Checks for conflicts: if multiple inputs provide different non-null values 
    for the same field for a given program ID, an error is raised. 
    Otherwise, the single non-null value (or null) is kept.
    """
    log = context.log
    
    # --- 1. Collect and Filter Input DataFrames ---
    dfs = [ysws_programs_sign_up_stats, ysws_programs_hcb_stats]
    
    input_heights = {
        "sign_up_stats": ysws_programs_sign_up_stats.height,
        "hcb_stats": ysws_programs_hcb_stats.height,
    }
    log.info(f"Input counts - Sign-up stats: {input_heights['sign_up_stats']}, HCB stats: {input_heights['hcb_stats']}")

    dfs = [df for df in dfs if not df.is_empty()]

    if not dfs:
        log.warning("All input DataFrames for merging are empty.")
        return Output(
            pl.DataFrame(), 
            metadata={
                "num_output_records": 0,
                "notes": "All input assets were empty.",
                **{f"num_{name}_input_records": h for name, h in input_heights.items()}
            }
        )

    # --- 2. Determine Union of Columns and Target Schema ---
    all_cols = set()
    schemas = {} # Store schema info {col: [non-null dtypes]}
    for df in dfs:
        all_cols.update(df.columns)
        for col, dtype in df.schema.items():
            if col not in schemas: schemas[col] = []
            if dtype != pl.Null: # Only consider non-null types
                schemas[col].append(dtype)

    if "id" not in all_cols:
        raise ValueError("Input DataFrames must contain an 'id' column for merging.")
        
    value_cols = sorted(list(all_cols - {"id"})) 
    required_schema_cols = ["id"] + value_cols

    # Determine the target dtype for each column (use first non-null type found)
    target_schema = {}
    for col in required_schema_cols:
        potential_types = schemas.get(col, [])
        target_type = next((t for t in potential_types if t != pl.Null), pl.Null) 
        target_schema[col] = target_type

    # --- 3. Adjust Schemas and Concatenate ---
    adjusted_dfs = []
    for i, df in enumerate(dfs):
        current_cols = set(df.columns)
        select_exprs = []
        
        for col in required_schema_cols:
            target_type = target_schema[col]
            
            if col in current_cols:
                # Column exists, check if casting is needed
                current_type = df.schema[col]
                if current_type != target_type and target_type != pl.Null:
                    select_exprs.append(pl.col(col).cast(target_type))
                else:
                    select_exprs.append(pl.col(col))
            else:
                # Column is missing, add it with the target type
                if target_type == pl.Null:
                    select_exprs.append(pl.lit(None).alias(col))
                else:
                    select_exprs.append(pl.lit(None).cast(target_type).alias(col))

        adjusted_df = df.select(select_exprs)
        adjusted_dfs.append(adjusted_df)

    # Concatenate using vertical_relaxed strategy
    combined_long = pl.concat(adjusted_dfs, how="vertical_relaxed") 
        
    # --- 4. Group, Aggregate, and Check Conflicts ---
    agg_exprs = []
    conflict_check_cols = []
    for col in value_cols:
        unique_count_expr = pl.col(col).drop_nulls().unique().count()
        first_value_expr = pl.col(col).drop_nulls().first()
        conflict_col_name = f"_{col}_has_conflict_" 

        final_value_expr = (
            pl.when(unique_count_expr == 1)
            .then(first_value_expr)
            .otherwise(pl.lit(None))
            .alias(col)
        )
        
        conflict_flag_expr = (
            (unique_count_expr > 1)
            .alias(conflict_col_name)
        )

        agg_exprs.append(final_value_expr)
        agg_exprs.append(conflict_flag_expr)
        conflict_check_cols.append(conflict_col_name)

    # Perform the aggregation
    grouped = combined_long.group_by("id").agg(agg_exprs)

    # --- 5. Check for Conflicts ---
    conflict_rows = grouped.filter(pl.any_horizontal(pl.col(c) for c in conflict_check_cols))

    if not conflict_rows.is_empty():
        error_details = []
        for row in conflict_rows.head(5).iter_rows(named=True):
            program_id = row["id"]
            conflicting_fields = [
                c.replace("_has_conflict_", "").strip("_") 
                for c in conflict_check_cols if row[c]
            ]
            error_details.append(f"Program ID '{program_id}': Conflicts in fields: {', '.join(conflicting_fields)}")
        
        raise ValueError(f"Conflicting field values found:\n" + "\n".join(error_details))

    # --- 6. Clean and Return ---
    # Remove conflict check columns from the final result
    final_df = grouped.drop(conflict_check_cols)

    log.info(f"Successfully merged {len(dfs)} input DataFrames into {final_df.height} records")

    # Generate preview metadata
    if final_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(final_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(final_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No records prepared for update.")

    return Output(
        final_df,
        metadata={
            "num_records_prepared": final_df.height,
            "num_sign_up_stats_input": input_heights["sign_up_stats"],
            "num_hcb_stats_input": input_heights["hcb_stats"],
            "update_columns": list(final_df.columns),
            "preview": preview_metadata
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Updates YSWS programs in Airtable with merged sign-up stats and HCB data.",
    required_resource_keys={"airtable"},
    compute_kind="airtable_update",
)
def ysws_programs_update_status(
    context: AssetExecutionContext,
    ysws_programs_prepared_for_update: pl.DataFrame,
) -> Output[None]:
    """
    Performs batch update of YSWS programs in Airtable with merged sign-up stats and HCB data.
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    input_df = ysws_programs_prepared_for_update
    
    if input_df.height == 0:
        log.info("No records to update in Airtable.")
        return Output(
            None,
            metadata={"updates_attempted": 0, "updates_successful": 0, "updates_failed": 0}
        )
    
    log.info(f"Starting Airtable update for {input_df.height} YSWS programs")
    
    # Convert DataFrame to list of dictionaries for batch update
    records_to_update = []
    for row in input_df.iter_rows(named=True):
        record = {k: v for k, v in row.items() if v is not None}
        records_to_update.append(record)
    
    # Perform batch update
    result = airtable.batch_update_records(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="ysws_programs",
        records=records_to_update,
    )
    
    successful_count = result['successful']
    failed_count = result['failed']
    total_attempted = len(records_to_update)
    success_rate = round((successful_count / max(total_attempted, 1)) * 100, 2)
    
    log.info(f"Airtable update completed. Successful: {successful_count}, Failed: {failed_count}")
    
    # Fail the asset if there were significant failures (>50% failure rate)
    if failed_count > 0 and success_rate < 50:
        error_msg = f"High failure rate: {failed_count}/{total_attempted} updates failed ({100-success_rate:.1f}% failure rate)"
        log.error(error_msg)
        raise Exception(error_msg)
    
    # Warn if there were any failures but still mostly successful
    if failed_count > 0:
        log.warning(f"Some updates failed: {failed_count}/{total_attempted} records")
    
    return Output(
        None,
        metadata={
            "updates_attempted": total_attempted,
            "updates_successful": successful_count,
            "updates_failed": failed_count,
            "success_rate": success_rate,
            "status": "failed" if success_rate < 50 else ("partial" if failed_count > 0 else "success")
        }
    )


@asset(
    group_name="unified_ysws_db_processing",
    description="Runs parallel YSWS mention searches on projects requesting search",
    required_resource_keys={"airtable"},
    compute_kind="parallel_execution",
    deps=[AssetKey(["approved_projects_update_status"])],
)
def approved_projects_mention_search_batch(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """
    Re-queries approved projects from Airtable (to get fresh formula values after updates),
    finds projects wanting mention search, and runs parallel mention search processes.
    
    Returns:
        DataFrame with search execution results and status
    """
    log = context.log
    airtable: AirtableResource = context.resources.airtable
    
    # Re-query approved projects from Airtable to get fresh formula calculations
    log.info("Re-querying approved projects from Airtable to get fresh formula values")
    projects_df = airtable.get_all_records_as_polars(
        context=context,
        base_key="unified_ysws_projects_db",
        table_key="approved_projects",
    )
    
    # Get field IDs
    wants_search_field = UnifiedYSWS.approved_projects.ysws_project_mentions_project_wants_search
    approved_at_field = UnifiedYSWS.approved_projects.approved_at
    
    # Check if required fields exist
    if wants_search_field not in projects_df.columns:
        log.warning(f"Field '{wants_search_field}' not found in approved_projects")
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8, "status": pl.Utf8, "error": pl.Utf8}),
            metadata={"candidates_found": 0, "reason": "Missing wants_search field"}
        )
    
    if approved_at_field not in projects_df.columns:
        log.warning(f"Field '{approved_at_field}' not found in approved_projects")
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8, "status": pl.Utf8, "error": pl.Utf8}),
            metadata={"candidates_found": 0, "reason": "Missing approved_at field"}
        )
    
    # Filter for projects wanting search, sort by approved_at descending, limit 40
    candidates_df = projects_df.filter(
        (pl.col(wants_search_field) == True) |
        (pl.col(wants_search_field) == "true") |
        (pl.col(wants_search_field) == 1)
    ).sort(
        pl.col(approved_at_field), descending=True
    ).head(40).select([
        pl.col("id"),
        pl.col(approved_at_field)
    ])
    
    log.info(f"Found {candidates_df.height} projects requesting mention search (max 40)")
    
    # Print the selected candidates for visibility
    if candidates_df.height > 0:
        log.info("Selected candidates for mention search:")
        for i, row in enumerate(candidates_df.iter_rows(named=True)):
            log.info(f"  {i+1}. ID: {row['id']}, Approved At: {row.get(approved_at_field, 'N/A')}")
    else:
        log.info("No candidates found - checking why...")
        
        # Debug: Show sample of the wants_search field values
        if wants_search_field in projects_df.columns:
            sample_values = projects_df.select([
                pl.col("id"),
                pl.col(wants_search_field)
            ]).head(5)
            log.info(f"Sample wants_search field values: {sample_values.to_dicts()}")
            
            # Count unique values in wants_search field
            value_counts = projects_df.group_by(wants_search_field).count().sort("count", descending=True)
            log.info(f"Value distribution in wants_search field: {value_counts.to_dicts()}")
        else:
            log.warning(f"wants_search field '{wants_search_field}' not found in projects_df columns")
    
    if candidates_df.height == 0:
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8, "status": pl.Utf8, "error": pl.Utf8}),
            metadata={"candidates_found": 0, "searches_completed": 0, "searches_failed": 0}
        )
    
    # Run parallel mention searches
    log.info(f"Starting parallel mention searches for {candidates_df.height} projects")
    
    # Get the script path relative to the current working directory
    import os
    script_path = os.path.join(os.getcwd(), "scripts", "ysws_project_mention_search.py")
    
    if not os.path.exists(script_path):
        log.error(f"Mention search script not found at: {script_path}")
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8, "status": pl.Utf8, "error": pl.Utf8}),
            metadata={"candidates_found": candidates_df.height, "error": "Script not found"}
        )
    
    # Run searches in parallel using asyncio
    results = asyncio.run(_run_parallel_mention_searches(
        candidates_df["id"].to_list(),
        script_path,
        log
    ))
    
    # Convert results to DataFrame
    results_df = pl.DataFrame(results)
    
    # Count successes and failures
    successful_count = results_df.filter(pl.col("status") == "success").height
    failed_count = results_df.filter(pl.col("status") == "failed").height
    
    log.info(f"Mention search batch completed. Successful: {successful_count}, Failed: {failed_count}")
    
    # Generate preview metadata
    if results_df.height > 0:
        try:
            preview_metadata = MetadataValue.md(results_df.head(10).to_pandas().to_markdown(index=False))
        except Exception:
            preview_metadata = MetadataValue.text(str(results_df.head(10)))
    else:
        preview_metadata = MetadataValue.text("No search results")
    
    return Output(
        results_df,
        metadata={
            "candidates_found": candidates_df.height,
            "searches_completed": successful_count,
            "searches_failed": failed_count,
            "success_rate": round((successful_count / max(candidates_df.height, 1)) * 100, 2),
            "preview": preview_metadata
        }
    )


async def _run_parallel_mention_searches(record_ids: list, script_path: str, logger) -> list:
    """
    Run mention search script in parallel for multiple record IDs using asyncio.
    Similar pattern to _fetch_github_stars_async_all but for subprocess execution.
    
    Args:
        record_ids: List of Airtable record IDs to process
        script_path: Path to the mention search Python script  
        logger: Logger for progress updates
        
    Returns:
        List of results from all subprocess executions
    """
    import asyncio
    import time
    
    results = []
    
    # Process all records in parallel (no chunking for maximum concurrency)
    chunk_size = len(record_ids)  # Process all at once
    
    async def _run_single_search(record_id: str) -> dict:
        """Run mention search for a single record ID with real-time output streaming."""
        try:
            logger.info(f"🚀 Starting search for record {record_id}")
            
            # Create subprocess to run the script
            process = await asyncio.create_subprocess_exec(
                "uv", "run", "python", script_path, record_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=os.getcwd()
            )
            
            async def stream_output(stream, prefix):
                """Stream output from subprocess to logger in real-time."""
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    line_text = line.decode('utf-8').rstrip()
                    if line_text:  # Only log non-empty lines
                        logger.info(f"[{record_id}] {line_text}")
            
            # Start streaming tasks for stdout and stderr
            stdout_task = asyncio.create_task(stream_output(process.stdout, "OUT"))
            stderr_task = asyncio.create_task(stream_output(process.stderr, "ERR"))
            
            # Wait for completion with timeout (increased to 15 minutes for 12-minute runs)
            try:
                await asyncio.wait_for(
                    process.wait(),
                    timeout=900  # 15 minute timeout per search (12 min + buffer)
                )
                
                # Cancel streaming tasks
                stdout_task.cancel()
                stderr_task.cancel()
                
                if process.returncode == 0:
                    logger.info(f"✅ Search completed successfully for record {record_id}")
                    return {
                        "id": record_id,
                        "status": "success",
                        "return_code": process.returncode,
                        "error": None
                    }
                else:
                    logger.warning(f"❌ Search failed for record {record_id} with return code {process.returncode}")
                    return {
                        "id": record_id,
                        "status": "failed", 
                        "return_code": process.returncode,
                        "error": f"Process exited with code {process.returncode}"
                    }
                    
            except asyncio.TimeoutError:
                # Kill the process if it times out
                logger.warning(f"⏰ Search timed out for record {record_id} after 15 minutes")
                process.kill()
                await process.wait()
                stdout_task.cancel()
                stderr_task.cancel()
                return {
                    "id": record_id,
                    "status": "failed",
                    "return_code": -1,
                    "error": "Process timed out after 15 minutes"
                }
                
        except Exception as e:
            logger.error(f"💥 Exception running search for record {record_id}: {e}")
            return {
                "id": record_id,
                "status": "failed",
                "return_code": -1,
                "error": f"Exception: {str(e)}"
            }
    
    # Process in chunks with delays to manage system load
    for i in range(0, len(record_ids), chunk_size):
        chunk = record_ids[i:i + chunk_size]
        chunk_start_time = time.time()
        
        logger.info(f"🔄 Starting all {len(chunk)} searches in parallel")
        
        # Create tasks for this chunk
        tasks = [_run_single_search(record_id) for record_id in chunk]
        
        # Execute chunk in parallel  
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        for result in chunk_results:
            if isinstance(result, Exception):
                logger.error(f"Task exception: {result}")
                results.append({
                    "id": "unknown",
                    "status": "failed",
                    "return_code": -1,
                    "error": str(result)
                })
            else:
                results.append(result)
        
        # No delay needed since we're processing everything in one chunk
    
    return results


@asset(
    group_name="unified_ysws_db_processing",
    description="Stub asset that marks completion of all YSWS processing",
    compute_kind="marker",
)
def unified_ysws_db_processing_done(
    context: AssetExecutionContext,
    approved_projects_mention_search_batch: pl.DataFrame,
    ysws_programs_update_status: None,
) -> Output[None]:
    """
    Marker asset that indicates all YSWS processing is complete.
    Warehouse refresh assets depend on this to ensure they run after all processing.
    """
    context.log.info("All YSWS processing completed successfully")
    
    return Output(
        None,
        metadata={
            "approved_projects_searches": approved_projects_mention_search_batch.height,
            "processing_status": "completed"
        }
    )


# Create refreshed Airtable source assets that re-query after processing
ysws_refresh_airtable_assets = create_airtable_assets(
    base_name="unified_ysws_projects_db",
    tables=[
        "approved_projects", 
        "ysws_programs", 
        "ysws_authors",
        "nps",
        "ysws_project_mentions",
        "ysws_project_mention_searches", 
        "ysws_spot_checks",
        "ysws_spot_check_sessions"
    ],
    deps=[AssetKey(["unified_ysws_db_processing_done"])],
    suffix="_refresh"
)

# Create DLT warehouse assets that use the refreshed Airtable sources
ysws_warehouse_refresh_assets = create_airtable_sync_assets(
    base_name="unified_ysws",
    tables=[
        "approved_projects", 
        "ysws_programs", 
        "ysws_authors",
        "nps",
        "ysws_project_mentions",
        "ysws_project_mention_searches", 
        "ysws_spot_checks",
        "ysws_spot_check_sessions"
    ],
    description="Loads refreshed YSWS data into warehouse after all processing is complete",
    source_suffix="_refresh",  # Use the refreshed Airtable sources
    warehouse_dataset_name="airtable_unified_ysws_projects_db",  # Custom warehouse schema name
    source_base_name="unified_ysws_projects_db"  # Source assets are in unified_ysws_projects_db base
)


# Define the assets for this module
defs = Definitions(
    assets=[
        approved_projects_geocoding_candidates,
        approved_projects_geocoded,
        approved_projects_archive_candidates,
        approved_projects_archived,
        approved_projects_repo_stats_candidates,
        approved_projects_repo_stats,
        approved_projects_prepared_for_update,
        approved_projects_update_status,
        approved_projects_mention_search_batch,
        ysws_programs_sign_up_stats_candidates,
        ysws_programs_sign_up_stats,
        ysws_programs_hcb_candidates,
        ysws_programs_hcb_stats,
        ysws_programs_prepared_for_update,
        ysws_programs_update_status,
        unified_ysws_db_processing_done,
    ] + ysws_refresh_airtable_assets + ysws_warehouse_refresh_assets,
)
