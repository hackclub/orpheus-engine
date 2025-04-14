import time
import requests
import polars as pl
import io
import json
import urllib.parse
import os
from dagster import asset, EnvVar, DagsterLogManager

import dagster as dg

LOOPS_BASE_URL = "https://app.loops.so/api/trpc"

# Define common headers, excluding the cookie which needs the dynamic session token
# Extracted from the provided curl command examples
COMMON_HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'dnt': '1',
    'origin': 'https://app.loops.so',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://app.loops.so/audience', # Simplified referer
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
}

@asset(
    group_name="loops",
    description="Exports all contacts from Loops, polls until completion, downloads the CSV, and returns it as a Polars DataFrame. Uses local cache unless DAGSTER_ENV=production.",
    required_resource_keys={"loops_session_token"},
)
def loops_contacts_export(context) -> pl.DataFrame:
    """
    Fetches the Loops contacts export as a Polars DataFrame using the Loops API.

    Requires the `LOOPS_SESSION_TOKEN` environment variable set via resources.

    Steps:
    1. Initiates an export job via the Loops API.
    2. Polls the API until the export job is complete (every 3 seconds).
    3. Retrieves a presigned S3 URL for the export.
    4. Downloads the CSV from the S3 URL.
    5. Loads the CSV data into a Polars DataFrame.
    """
    log: DagsterLogManager = context.log
    session_token = context.resources.loops_session_token.get_value()
    cookies = {'__Secure-next-auth.session-token': session_token}
    headers = COMMON_HEADERS.copy()

    # --- Cache Check (Upfront) ---
    should_cache = os.getenv("DAGSTER_ENV") != "production"
    cache_file_path = os.path.join(os.path.dirname(__file__), "..", "..", ".cache", "loops", "loops_contacts_export.csv")
    api_filename = None # Keep track of the filename provided by the API later

    if should_cache:
        log.info(f"Checking for cached file at {cache_file_path} (caching enabled unless DAGSTER_ENV=production)")
        if os.path.exists(cache_file_path):
            log.info(f"Cache hit! Loading data from {cache_file_path}")
            try:
                df = pl.read_csv(
                    cache_file_path, # Use the static path
                    infer_schema_length=10000,
                    ignore_errors=False
                )
                log.info(f"Successfully loaded data from cache. DataFrame shape: {df.shape}")
                # Skip download and rest of the try block for cache hit
                context.add_output_metadata(
                    metadata={
                        "num_records": df.height,
                        "columns": ", ".join(df.columns),
                        "loops_job_id": "N/A (cached)", # Indicate cache usage
                        "downloaded_filename": os.path.basename(cache_file_path), # Use cache filename
                        "source": "cache"
                    }
                )
                return df
            except (pl.exceptions.ComputeError, pl.exceptions.NoDataError, pl.exceptions.ShapeError, FileNotFoundError) as e:
                log.error(f"Failed to load data from cache file {cache_file_path}: {e}. Proceeding with API download.")
                # If cache file is corrupted or unreadable, treat as cache miss
        else:
            log.info("Cache miss.")
    else:
        log.info("Production mode (DAGSTER_ENV=production), caching disabled.")
    # --- End Cache Check ---

    # --- API Interaction (Only if cache miss or disabled) ---
    log.info("Proceeding with Loops API interaction...")

    # Step 1: Create the export job
    create_export_url = f"{LOOPS_BASE_URL}/lists.exportContacts"
    create_payload = {"json": {"filter": None, "mailingListId": None}}
    job_id = None
    try:
        log.info("Initiating Loops contact export job...")
        response_create = requests.post(
            create_export_url,
            headers=headers,
            cookies=cookies,
            json=create_payload,
            timeout=30
        )
        response_create.raise_for_status()
        create_data = response_create.json()
        # Navigate through the nested structure to find the ID
        job_id = create_data.get('result', {}).get('data', {}).get('json', {}).get('id')
        if not job_id:
            raise ValueError("Could not extract job ID from response.")
        log.info(f"Started Loops export job with ID: {job_id}")
    except (requests.exceptions.RequestException, KeyError, ValueError, json.JSONDecodeError) as e:
        log.error(f"Failed to create Loops export job: {e}")
        if 'response_create' in locals():
            log.error(f"Response status: {response_create.status_code}")
            log.error(f"Response text: {response_create.text[:500]}") # Log first 500 chars
        raise RuntimeError(f"Failed to create Loops export job: {e}") from e


    # Step 2: Poll for completion
    status_url_template = f"{LOOPS_BASE_URL}/audienceDownload.getAudienceDownload"
    # URL encode the JSON payload for the 'input' query parameter
    status_params = {'input': json.dumps({'json': {'id': job_id}})}
    export_status = None
    max_polls = 100 # Limit polling (100 polls * 3 seconds = 5 minutes)
    poll_count = 0
    poll_interval_seconds = 3

    while export_status != "Complete" and poll_count < max_polls:
        poll_count += 1
        log.info(f"Polling job status ({poll_count}/{max_polls})...")
        try:
            response_status = requests.get(
                status_url_template,
                headers=headers, # Some APIs might require headers even for GET
                cookies=cookies,
                params=status_params,
                timeout=15 # Shorter timeout for status checks
            )
            response_status.raise_for_status()
            status_data = response_status.json()
            export_status = status_data.get('result', {}).get('data', {}).get('json', {}).get('status')
            log.info(f"Current status: {export_status}")
            if export_status != "Complete":
                time.sleep(poll_interval_seconds)
        except (requests.exceptions.RequestException, KeyError, json.JSONDecodeError) as e:
             log.error(f"Failed to get Loops export job status on poll {poll_count}: {e}")
             if 'response_status' in locals():
                 log.error(f"Response status: {response_status.status_code}")
                 log.error(f"Response text: {response_status.text[:500]}")
             # Decide whether to retry or fail
             if poll_count >= max_polls // 2: # If past half the attempts, fail
                 raise RuntimeError(f"Failed to get Loops export job status: {e}") from e
             else:
                 log.warning("Continuing polling despite error...")
                 time.sleep(poll_interval_seconds * 2) # Wait longer after error


    if export_status != "Complete":
        log.error(f"Loops export job {job_id} did not complete after {poll_count} polls ({poll_count * poll_interval_seconds} seconds). Last status: {export_status}")
        raise RuntimeError(f"Loops export job {job_id} timed out.")


    # Step 3: Get the download URL
    sign_url = f"{LOOPS_BASE_URL}/audienceDownload.signs3Url"
    sign_payload = {"json": {"id": job_id}}
    presigned_url = None
    filename = None
    try:
        log.info("Fetching presigned URL for download...")
        response_sign = requests.post(
            sign_url,
            headers=headers,
            cookies=cookies,
            json=sign_payload,
            timeout=30
        )
        response_sign.raise_for_status()
        sign_data = response_sign.json()
        presigned_url = sign_data.get('result', {}).get('data', {}).get('json', {}).get('presignedUrl')
        filename = sign_data.get('result', {}).get('data', {}).get('json', {}).get('filename')
        if not presigned_url or not filename:
             raise ValueError("Could not extract presigned URL or filename from response.")
        log.info(f"Got presigned URL for file: {filename}")
    except (requests.exceptions.RequestException, KeyError, ValueError, json.JSONDecodeError) as e:
        log.error(f"Failed to get presigned URL for Loops export: {e}")
        if 'response_sign' in locals():
            log.error(f"Response status: {response_sign.status_code}")
            log.error(f"Response text: {response_sign.text[:500]}")
        raise RuntimeError(f"Failed to get presigned URL for Loops export: {e}") from e


    # Step 4 & 5: Download the CSV and load into Polars
    try:
        log.info(f"Downloading from presigned URL (truncated): {presigned_url[:100]}...")
        response_download = requests.get(presigned_url, timeout=600) # Allow up to 10 minutes for download
        response_download.raise_for_status()
        csv_content = response_download.content
        log.info(f"Downloaded {len(csv_content)} bytes.")

        # Load into Polars DataFrame
        log.info("Loading downloaded data into Polars DataFrame...")
        # Increase schema inference length to better handle mixed number types
        # without explicit overrides.
        df = pl.read_csv(
            io.BytesIO(csv_content),
            infer_schema_length=10000, # Look at more rows to guess dtypes
            ignore_errors=False
        )
        log.info(f"Successfully loaded data. DataFrame shape: {df.shape}")
        context.add_output_metadata(
            metadata={
                "num_records": df.height,
                "columns": ", ".join(df.columns),
                "loops_job_id": job_id,
                "downloaded_filename": filename or os.path.basename(cache_file_path), # Use API filename if available
                "source": "api"
            }
        )

        # If caching is enabled and cache path is valid, save the downloaded file
        if should_cache and cache_file_path:
            try:
                os.makedirs(os.path.dirname(cache_file_path), exist_ok=True)
                with open(cache_file_path, 'wb') as f:
                    f.write(csv_content)
                log.info(f"Saved downloaded file to cache: {cache_file_path}")
            except OSError as e:
                log.error(f"Failed to write to cache file {cache_file_path}: {e}")

        return df
    except requests.exceptions.RequestException as e:
         log.error(f"Failed to download CSV from presigned URL: {e}")
         raise RuntimeError(f"Failed to download CSV from presigned URL: {e}") from e
    except (pl.exceptions.ComputeError, pl.exceptions.NoDataError, pl.exceptions.ShapeError) as e:
        log.error(f"Failed to parse CSV content into Polars DataFrame: {e}")
        # Maybe log first few bytes if parsing fails?
        log.error(f"First 500 bytes of downloaded content: {csv_content[:500]}")
        raise RuntimeError(f"Failed to parse CSV content into Polars DataFrame: {e}") from e
    except Exception as e: # Catch any other potential errors during download/parse
        log.error(f"An unexpected error occurred during CSV download or parsing: {e}")
        raise RuntimeError(f"An unexpected error occurred during CSV download or parsing: {e}") from e

defs = dg.Definitions(
    assets=[loops_contacts_export],
    resources={
        "loops_session_token": EnvVar("LOOPS_SESSION_TOKEN")
    }
)
