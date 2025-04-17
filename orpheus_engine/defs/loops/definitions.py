import time
import requests
import polars as pl
import io
import json
import urllib.parse
import os
import hashlib
from dagster import asset, EnvVar, DagsterLogManager, AssetExecutionContext, Output, Failure
from typing import List, Dict, Any, Optional
from enum import Enum
from pydantic import BaseModel

import dagster as dg

from orpheus_engine.defs.geocoder.resources import GeocoderResource, GeocodingError
from orpheus_engine.defs.ai.resources import AIResource
from orpheus_engine.defs.genderize.resources import GenderizeResource

# Import the Loops resource and error
from orpheus_engine.defs.loops.resources import LoopsResource, LoopsApiError 

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
def loops_raw_audience(context) -> pl.DataFrame:
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
    cache_file_path = os.path.join(os.path.dirname(__file__), "..", "..", ".cache", "loops", "loops_raw_audience.csv")
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


def _extract_geocode_details(geocode_result_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extracts relevant details from a single Google Maps geocode result."""
    try:
        latitude = geocode_result_item["geometry"]["location"]["lat"]
        longitude = geocode_result_item["geometry"]["location"]["lng"]

        country_name = None
        country_code = None
        for component in geocode_result_item.get("address_components", []):
            if "country" in component.get("types", []):
                country_name = component.get("long_name")
                country_code = component.get("short_name")
                break # Found country component

        if latitude is not None and longitude is not None:
            return {
                "calculatedGeocodedLatitude": latitude,
                "calculatedGeocodedLongitude": longitude,
                "calculatedGeocodedCountryName": country_name,
                "calculatedGeocodedCountryCode": country_code,
            }
    except (KeyError, IndexError, TypeError):
        # Handle cases where the expected structure is missing
        pass
    return None


@asset(
    group_name="loops",
    description="Geocodes contacts from the Loops export if their address has changed or hasn't been geocoded.",
    required_resource_keys={"geocoder_client"},
    compute_kind="google_maps",
)
def loops_geocoded_audience(context: AssetExecutionContext, loops_raw_audience: pl.DataFrame) -> Output[pl.DataFrame]:
    """
    Iterates through contacts, checks if address requires geocoding based on hash comparison,
    attempts geocoding, and returns a DataFrame of newly geocoded contact details.
    """
    log = context.log
    geocoder: GeocoderResource = context.resources.geocoder_client
    input_df = loops_raw_audience

    # Ensure required columns exist
    required_input_cols = ['email', 'addressLine1', 'addressCity']
    # Optional cols used in address string and hash check
    optional_input_cols = ['addressLine2', 'addressState', 'addressZip', 'addressCountry', 'calculatedGeocodedHash']
    missing_required = [col for col in required_input_cols if col not in input_df.columns]
    if missing_required:
        raise Failure(f"Input DataFrame is missing required columns: {missing_required}")

    # Add calculatedGeocodedHash if it's missing, filling with None
    if 'calculatedGeocodedHash' not in input_df.columns:
        log.info("Input DataFrame missing 'calculatedGeocodedHash'. Column will be added.")
        # Use pl.lit(None).cast(pl.Utf8) for correct type hint if needed later
        input_df = input_df.with_columns(pl.lit(None).alias("calculatedGeocodedHash"))

    newly_geocoded_records = []
    processed_count = 0
    geocoded_count = 0 # Counter for successful geocodes
    error_count = 0
    geocode_limit = -1 # Testing limit: -1 means no limit

    if geocode_limit == -1:
        log.info(f"Starting geocoding process for {input_df.height} contacts (no limit).")
    else:
        log.info(f"Starting geocoding process for {input_df.height} contacts (limit: {geocode_limit}).")

    for row in input_df.iter_rows(named=True):
        # --- Apply limit only if it's not -1 ---
        if geocode_limit != -1 and geocoded_count >= geocode_limit:
             log.info(f"Reached geocoding limit of {geocode_limit}. Stopping process.")
             break
        # --- END Limit Check ---

        processed_count += 1
        email = row.get("email")
        address_line1 = row.get("addressLine1")
        address_city = row.get("addressCity")

        # Basic check for required fields - these are essential for the JS format
        if not email or not address_line1 or not address_city:
            continue

        # --- Replicate JS address string construction EXACTLY --- 
        address_line2 = row.get("addressLine2") or ""
        address_state = row.get("addressState") or ""
        # Use addressZipCode to match JS precisely
        address_zipcode = row.get("addressZipCode") or "" 
        address_country = row.get("addressCountry") or ""
        
        # Build line 3 *without* stripping here
        line3 = f"{address_city}, {address_state} {address_zipcode}"

        # Combine all parts with newlines
        address_string = f"{address_line1}\n{address_line2}\n{line3}\n{address_country}"
        
        # Apply strip() *only once* at the end, matching JS .trim()
        address_string = address_string.strip()
        # --- End JS address string construction replication ---

        # Check if the string is empty *after* construction and stripping
        if not address_string:
            continue

        # Hashing (remains the same, as it matches JS crypto)
        address_hash = hashlib.sha256(address_string.encode('utf-8')).hexdigest()
        existing_hash = row.get("calculatedGeocodedHash")

        if address_hash != existing_hash:
            log.info(f"Address hash changed for {email}. Attempting geocode. Hash: {address_hash[:7]}... String: '{address_string.replace('\n', ' ')}'") # Log truncated hash and string
            try:
                geocode_api_result: List[Dict[str, Any]] = geocoder.geocode(address_string)

                if not geocode_api_result:
                    log.warning(f"Geocoding returned no results for address: '{address_string.replace('\n', ' ')}' (Email: {email})")
                    continue

                extracted_details = _extract_geocode_details(geocode_api_result[0])

                if extracted_details:
                    # Increment successful geocode counter BEFORE appending
                    geocoded_count += 1 
                    output_record = {
                        "email": email,
                        **extracted_details,
                        "calculatedGeocodedHash": address_hash
                    }
                    newly_geocoded_records.append(output_record)
                    log.info(f"Successfully geocoded address for {email} ({geocoded_count}/{geocode_limit})") # Log progress towards limit
                else:
                     log.warning(f"Could not extract required details from geocode result for {email}. Result: {geocode_api_result[0]}")
                     error_count += 1

            except GeocodingError as e:
                log.error(f"Geocoding failed for address '{address_string.replace('\n', ' ')}' (Email: {email}): {e}")
                error_count += 1

    log.info(f"Geocoding process completed. Processed: {processed_count}, Newly Geocoded: {geocoded_count}, Errors: {error_count}")

    output_schema = {
        "email": pl.Utf8,
        "calculatedGeocodedLatitude": pl.Float64,
        "calculatedGeocodedLongitude": pl.Float64,
        "calculatedGeocodedCountryName": pl.Utf8,
        "calculatedGeocodedCountryCode": pl.Utf8,
        "calculatedGeocodedHash": pl.Utf8,
    }

    if newly_geocoded_records:
        output_df = pl.DataFrame(newly_geocoded_records, schema=output_schema)
    else:
        output_df = pl.DataFrame({k: [] for k in output_schema.keys()}, schema=output_schema)

    return Output(
        output_df,
        metadata={
            "num_contacts_processed": processed_count,
            "num_contacts_newly_geocoded": geocoded_count,
            "num_geocoding_errors": error_count,
            "num_output_rows": output_df.height,
            "testing_limit_applied": geocode_limit if processed_count > geocoded_count else None # Indicate if limit was likely hit
        }
    )

@asset(
    group_name="loops",
    description="Categorizes gender of contacts that have not been categorized yet.",
    required_resource_keys={"ai_client", "genderize_client"}
)
def loops_gender_categorized_audience(context: AssetExecutionContext, loops_raw_audience: pl.DataFrame) -> Output[pl.DataFrame]:
    """
    Categorizes gender of contacts that have not been categorized yet.
    """
    log = context.log
    ai_client: AIResource = context.resources.ai_client
    genderize_client: GenderizeResource = context.resources.genderize_client
    input_df = loops_raw_audience

    class GenderOption(str, Enum):
        MALE = "male"
        FEMALE = "female"
        NON_BINARY = "non-binary"
    
    class GenderCategorization(BaseModel):
        gender: GenderOption
    
    name = "Zach"
    
    resp = ai_client.generate_structured_response(
        prompt=f"Categorize given name to a gender: {name}",
        response_schema=GenderCategorization
    )

    log.info(f"AI gender categorization response: {resp}")

    gender = genderize_client.get_gender(name)
    log.info(f"genderize gender categorization response: {gender}")


@asset(
    group_name="loops", 
    description="Placeholder asset to combine updates. Currently passes through geocoding results."
)
def loops_audience_prepared_for_update(
    context: AssetExecutionContext,
    loops_geocoded_audience: pl.DataFrame, 
    # gender_categorization_results: pl.DataFrame # Add later
) -> pl.DataFrame:
    """
    Acts as a consolidation point for various contact update sources.
    
    Currently, with only loops_geocoded_audience as input, it simply passes the 
    DataFrame through. The primary key is conceptually 'email'.

    Future logic will perform an outer join on 'email' when other update 
    sources (like gender_categorization_results) are added.
    """
    log = context.log
    log.info(f"Passing through {loops_geocoded_audience.height} geocoding update records.")

    # --- Placeholder for future join logic ---
    # When gender_categorization_results is available:
    # 1. Load/receive gender_categorization_results DataFrame (with 'email').
    # 2. Perform outer join with loops_geocoded_audience on 'email'.
    #    combined = loops_geocoded_audience.join(
    #        gender_categorization_results, # Assuming it has 'email' and update cols
    #        on="email",
    #        how="outer"
    #    )
    # 3. Return the combined DataFrame.
    # ----------------------------------------

    # For now, just return the input DataFrame directly
    combined = loops_geocoded_audience

    log.info(f"Passing through combined updates DataFrame with shape: {combined.shape}")
    context.add_output_metadata({
        "num_update_records": combined.height,
        "columns": combined.columns,
        "preview": combined.head(5).to_dicts() # Preview first 5 rows of the input
    })

    return combined


@asset(
    group_name="loops",
    description="Updates contacts in Loops.so with the latest combined data.",
    required_resource_keys={"loops_client"}, 
    compute_kind="loops_api"
)
def loops_audience_update_status(
    context: AssetExecutionContext, 
    loops_audience_prepared_for_update: pl.DataFrame
) -> Output[None]:
    """
    Iterates through the combined update records and calls the Loops API 
    to update each contact with any non-null fields provided (excluding email).
    Handles API errors on a per-contact basis.
    """
    log = context.log
    loops_client: LoopsResource = context.resources.loops_client
    num_records = loops_audience_prepared_for_update.height

    if num_records == 0:
        log.info("No update records found in loops_audience_prepared_for_update. Nothing to send to Loops.")
        return Output(None, metadata={"updates_attempted": 0, "updates_successful": 0, "updates_failed": 0})

    log.info(f"Starting update process for {num_records} contacts in Loops...")
    
    updated_count = 0
    failed_count = 0

    for row_dict in loops_audience_prepared_for_update.iter_rows(named=True):
        email = row_dict.get("email")
        if not email:
            log.warning(f"Skipping row due to missing email: {row_dict}")
            failed_count += 1
            continue

        # Prepare payload dynamically from all non-null row values, excluding email
        payload_updates = { 
            k: v for k, v in row_dict.items() if k != "email" and v is not None
        }
            
        # Only attempt update if there are actual fields to update
        if not payload_updates:
             log.debug(f"Skipping update for {email} as no new non-null fields were found.")
             continue

        try:
            log.debug(f"Attempting to update contact {email} with payload: {payload_updates}")
            response = loops_client.update_contact(email=email, **payload_updates)
            log.info(f"Successfully updated contact {email}. Response ID: {response.get('id')}")
            updated_count += 1
        except LoopsApiError as e:
            log.error(f"Failed to update contact {email}: {e}")
            failed_count += 1
        except Exception as e:
            log.error(f"An unexpected error occurred while processing update for {email}: {e}", exc_info=True)
            failed_count += 1

    log.info(f"Loops contact update process finished. Successful: {updated_count}, Failed: {failed_count}")

    return Output(
        None,
        metadata={
            "updates_attempted": updated_count + failed_count,
            "updates_successful": updated_count,
            "updates_failed": failed_count
        }
    )


defs = dg.Definitions(
    assets=[loops_raw_audience, loops_geocoded_audience, loops_gender_categorized_audience, loops_audience_prepared_for_update, loops_audience_update_status],
    resources={
        "loops_session_token": EnvVar("LOOPS_SESSION_TOKEN"),
        "geocoder_client": GeocoderResource(),
        "ai_client": AIResource(),
        "genderize_client": GenderizeResource(),
        "loops_client": LoopsResource(),
    }
)
