import time
import requests
import polars as pl
import io
import json
import urllib.parse
import os
import hashlib
from dagster import asset, EnvVar, DagsterLogManager, AssetExecutionContext, Output, Failure, MetadataValue, TableColumn, TableSchema, TableRecord, DagsterInvariantViolationError
from typing import List, Dict, Any, Optional
from enum import Enum
from pydantic import BaseModel
from datetime import date

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
                    infer_schema_length=None, # Infer from all rows
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
            infer_schema_length=None, # Infer from all rows
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

# --- Helper Functions ---

def _calculate_hash(text: str) -> str:
    """Calculates the SHA256 hash of a string."""
    # Ensure None or empty strings hash consistently
    text_to_hash = text or "" 
    return hashlib.sha256(text_to_hash.encode('utf-8')).hexdigest()

# Define Genderize mappings - These match the GenderizeResult enum values from the resource
GENDERIZE_MALE = "male"
GENDERIZE_FEMALE = "female"
GENDERIZE_NEUTRAL = "gender-neutral"
GENDERIZE_ERROR = "error" # Represents API error or null response

def _categorize_gender_of_name(
    name: str, 
    genderize_client: GenderizeResource, 
    log: DagsterLogManager
) -> str:
    """
    Calls the GenderizeResource to get the gender category for a name.
    The resource itself handles probability checks and error mapping.
    Returns one of: 'male', 'female', 'gender-neutral', 'error'.
    """
    if not name:
        log.warning("Attempted to categorize gender for empty name.")
        return GENDERIZE_ERROR 
    
    try:
        # The get_gender method returns a GenderizeResult enum member directly
        # The enum members have string values matching our constants.
        result_enum = genderize_client.get_gender(name) # No country code as requested
        return result_enum.value # Return the string value ('male', 'error', etc.)
            
    except GenderizeApiError as e:
        # Catch specific errors raised by the resource
        log.error(f"Genderize API call failed for name '{name}': {e}")
        return GENDERIZE_ERROR
    except Exception as e: 
        # Catch any other unexpected errors during the call
        log.error(f"Unexpected error during gender categorization for name '{name}': {e}", exc_info=True)
        return GENDERIZE_ERROR

# Define AI response model
class GenderOptionAI(str, Enum):
    MALE = "male"
    FEMALE = "female"
    NONBINARY = "nonbinary"

class BestKnownGenderResponse(BaseModel):
    gender: Optional[GenderOptionAI] = None # Make optional to handle null case
    unableToCategorizeGenderWithGivenInformation: bool

def _determine_best_known_gender(
    first_name_gender: Optional[str], 
    gender_self_reported: Optional[str], 
    ai_client: AIResource, 
    log: DagsterLogManager
) -> Optional[str]:
    """
    Calls AI to determine the best known gender based on inputs, following JS logic.
    """
    # Prepare inputs for prompt, matching JS logic where "error" or "gender-neutral" become empty
    prompt_first_name_gender = first_name_gender if first_name_gender in [GENDERIZE_MALE, GENDERIZE_FEMALE] else ""
    prompt_gender_self_reported = gender_self_reported or "" # Use empty string if None/empty

    # Only proceed if there's some information
    if not prompt_first_name_gender and not prompt_gender_self_reported:
        log.debug("Skipping best known gender determination: no valid inputs.")
        return None # Cannot determine without any input

    options = [o.value for o in GenderOptionAI]
    prompt = f"""
Categorize the user's gender into one of the following options: {', '.join(options)}

User provided information (empty quotes indicate that the user didn't provide that information):

Self-reported gender: "{prompt_gender_self_reported}"
Gender of user's first name: "{prompt_first_name_gender}"

Instructions:

1. If the user self-reported their gender, use this. Self-reported gender always takes precedent over gender of first name.
2. If the user didn't self-report their gender, use the gender of their first name.
3. If unable to determine based on the rules and information, indicate inability to categorize.
"""

    try:
        response = ai_client.generate_structured_response(
            prompt=prompt,
            response_schema=BestKnownGenderResponse
        )
        
        if response.unableToCategorizeGenderWithGivenInformation or response.gender is None:
            log.info(f"AI indicated inability to categorize gender. Inputs: Self='{prompt_gender_self_reported}', First Name='{prompt_first_name_gender}'")
            return None
        
        # Return the string value of the enum
        return response.gender.value 
        
    except Exception as e:
        log.error(f"AI call failed for best known gender determination: {e}", exc_info=True)
        log.error(f"Inputs were: Self='{prompt_gender_self_reported}', First Name='{prompt_first_name_gender}'")
        return None # Return None on AI error


@asset(
    group_name="loops",
    description="Categorizes gender of contacts using Genderize for name and AI for best-known, based on hash checks.",
    required_resource_keys={"ai_client", "genderize_client"}
)
def loops_gender_categorized_audience(context: AssetExecutionContext, loops_raw_audience: pl.DataFrame) -> Output[pl.DataFrame]:
    """
    Identifies contacts needing gender updates based on hash comparisons for full name 
    and best-known gender inputs. Calls Genderize API for name gender and an AI model 
    to determine the best-known gender, prioritizing self-reported data.

    Returns a DataFrame with `email` and the calculated fields for contacts 
    that had at least one field updated.
    """
    log = context.log
    ai_client: AIResource = context.resources.ai_client
    genderize_client: GenderizeResource = context.resources.genderize_client
    input_df = loops_raw_audience.clone() # Clone to avoid modifying the input

    # --- Column Definitions ---
    # Input columns needed (optional are checked/added)
    email_col = "email"
    first_name_col = "firstName"
    last_name_col = "lastName"
    gender_self_reported_col = "genderSelfReported"
    # Hash columns to check against (will be added if missing)
    fn_gender_hash_col = "calculatedFullNameGenderHash"
    best_known_hash_col = "calculatedGenderBestKnownHash"
    # Columns potentially read for best known gender calculation
    existing_fn_gender_col = "calculatedFirstNameGender" # Read if present
    existing_best_known_col = "calculatedGenderBestKnown" # Read if present (less likely needed)

    # Output columns to be generated/updated
    output_fn_gender_col = "calculatedFirstNameGender"
    output_fn_gender_hash_col = "calculatedFullNameGenderHash"
    output_best_known_col = "calculatedGenderBestKnown"
    output_best_known_hash_col = "calculatedGenderBestKnownHash"

    output_schema = {
        email_col: pl.Utf8,
        output_fn_gender_col: pl.Utf8,         # Note: Genderize returns str
        output_fn_gender_hash_col: pl.Utf8,
        output_best_known_col: pl.Utf8,        # Note: AI returns str enum value or None
        output_best_known_hash_col: pl.Utf8,
    }

    # --- Ensure Necessary Columns Exist ---
    required_input_cols = [email_col]
    optional_input_cols = [
        first_name_col, last_name_col, gender_self_reported_col,
        fn_gender_hash_col, best_known_hash_col,
        existing_fn_gender_col, # Needed for best known calc if name not recalc'd
        # existing_best_known_col # Not strictly needed for calculation logic
    ]
    
    missing_required = [col for col in required_input_cols if col not in input_df.columns]
    if missing_required:
        raise Failure(f"Input DataFrame is missing required columns: {missing_required}")

    # Add optional columns if they don't exist, filling with None
    for col in optional_input_cols:
        if col not in input_df.columns:
            log.info(f"Input DataFrame missing optional column '{col}'. Column will be added with null values.")
            # Use appropriate null type (Utf8 for strings/hashes)
            input_df = input_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            
    # --- Pre-computation & Filtering ---
    log.info("Preprocessing data: calculating full names and hashes...")

    # Combine first and last names safely handling nulls
    df_processed = input_df.with_columns(
        pl.concat_str(
            pl.col(first_name_col).fill_null(""),
            pl.col(last_name_col).fill_null(""),
            separator=" "
        ).str.strip_chars().alias("fullName") # Strip whitespace like JS .trim()
    ).with_columns(
        # Calculate current full name hash
        pl.col("fullName").map_elements(_calculate_hash).alias("currentFullNameHash")
    ).with_columns(
        # Determine if full name gender needs update (hash mismatch or empty name)
        needs_fn_gender_update = (pl.col("currentFullNameHash") != pl.col(fn_gender_hash_col)) | pl.col(fn_gender_hash_col).is_null()
    ).with_columns(
        # Step 1: Create the intermediate string needed for the best known hash
        best_known_input_str_expr = pl.concat_str(
            pl.col(existing_fn_gender_col).fill_null(""),
            pl.col(gender_self_reported_col).fill_null(""),
            separator=""
        )
    ).with_columns(
        # Step 2: Use the intermediate string to calculate the hash
        currentBestKnownHash = pl.when(pl.col("best_known_input_str_expr") != "")
                               .then(pl.col("best_known_input_str_expr").map_elements(_calculate_hash))
                               .otherwise(None) # Avoid hashing empty string if both inputs are null/empty
    ).with_columns(
        # Step 3: Now use the hash and other columns to calculate estimates
        # Determine if best known gender *might* need update based on existing data hash comparison
        needs_best_known_update_estimate = (pl.col("currentBestKnownHash") != pl.col(best_known_hash_col)) | pl.col(best_known_hash_col).is_null(),
        # Determine if we *can* calculate best known gender (pre-check)
        can_calculate_best_known_estimate = pl.col(gender_self_reported_col).is_not_null() | 
                                            (pl.col(existing_fn_gender_col).is_not_null() & 
                                             (pl.col(existing_fn_gender_col) != GENDERIZE_ERROR))
    ).drop("best_known_input_str_expr") # Drop intermediate column
    
    # --- Identify Rows Needing Updates (Counts based on pre-calculation) ---
    # Count rows needing first name gender update BEFORE iteration
    rows_needing_fn_gender_update = df_processed.filter(pl.col("needs_fn_gender_update") & (pl.col("fullName") != "")).height
    log.info(f"Identified {rows_needing_fn_gender_update} contacts potentially needing First Name Gender update based on hash.")

    # Count rows potentially needing best known gender update BEFORE iteration
    rows_needing_best_known_update = df_processed.filter(
        pl.col("needs_best_known_update_estimate") & pl.col("can_calculate_best_known_estimate")
    ).height
    log.info(f"Identified {rows_needing_best_known_update} contacts potentially needing Best Known Gender update (estimate based on current data)." )

    # We need to iterate because of the conditional logic and API calls
    update_records = []
    fn_gender_updated_count = 0
    best_known_updated_count = 0
    processed_count = 0
    
    log.info(f"Starting gender processing for {df_processed.height} contacts...")

    for row in df_processed.iter_rows(named=True):
        processed_count += 1
        email = row.get(email_col)
        if not email: 
            log.warning("Skipping row due to missing email.")
            continue
            
        row_updates: Dict[str, Any] = {email_col: email}
        needs_update = False # Flag to track if any update occurred for this row
        
        # --- 1. First Name Gender ---
        current_fn_gender = row.get(existing_fn_gender_col) # Get potentially existing value
        new_fn_gender = current_fn_gender # Default to existing value
        new_fn_gender_hash = row.get(fn_gender_hash_col) # Default to existing hash
        
        if row.get("needs_fn_gender_update") and row.get("fullName"):
            full_name = row.get("fullName")
            current_name_hash = row.get("currentFullNameHash")
            log.debug(f"Processing FN Gender for {email}: Name='{full_name}'")
            calculated_gender = _categorize_gender_of_name(full_name, genderize_client, log)
            
            # Store the calculated gender and the *new* hash if calculation was successful
            # We store the hash regardless of the result of categorization (error/neutral etc)
            new_fn_gender = calculated_gender 
            new_fn_gender_hash = current_name_hash # Use the hash of the name we processed
            row_updates[output_fn_gender_col] = new_fn_gender
            row_updates[output_fn_gender_hash_col] = new_fn_gender_hash
            needs_update = True
            fn_gender_updated_count += 1 # Count attempts/updates based on hash diff
            log.info(f"    FN Gender update for {email}: '{full_name}' -> '{new_fn_gender}' (Hash: {new_fn_gender_hash[:7]}...)")
        else:
             # If no update needed, ensure existing values are in row_updates for potential use later
             row_updates[output_fn_gender_col] = new_fn_gender # Could be None if column was added
             row_updates[output_fn_gender_hash_col] = new_fn_gender_hash # Could be None
            
        # --- 2. Best Known Gender ---
        gender_self_reported = row.get(gender_self_reported_col)
        # Use the *potentially updated* first name gender from this iteration
        fn_gender_for_calc = new_fn_gender 
        
        # Calculate the hash based on current/newly calculated values
        # Handle None by replacing with empty string for hashing, matching JS sha256(`${row.calculatedFirstNameGender}${row.genderSelfReported}`)
        best_known_input_str = f"{fn_gender_for_calc or ''}{gender_self_reported or ''}"
        current_best_known_hash = _calculate_hash(best_known_input_str)
        existing_best_known_hash = row.get(best_known_hash_col)

        needs_best_known_update = (current_best_known_hash != existing_best_known_hash) or existing_best_known_hash is None
        
        # Only update if hash differs AND there is input info (either self-reported or a valid first name gender)
        can_calculate_best_known = gender_self_reported or (fn_gender_for_calc and fn_gender_for_calc != GENDERIZE_ERROR)

        if needs_best_known_update and can_calculate_best_known:
            log.debug(f"Processing Best Known Gender for {email}: FN='{fn_gender_for_calc}', Self='{gender_self_reported}'")
            calculated_best_known = _determine_best_known_gender(
                fn_gender_for_calc,
                gender_self_reported,
                ai_client,
                log
            )
            # Store the calculated best known gender and the *new* hash
            new_best_known_gender = calculated_best_known # Could be None if AI fails/cannot determine
            new_best_known_hash = current_best_known_hash
            row_updates[output_best_known_col] = new_best_known_gender
            row_updates[output_best_known_hash_col] = new_best_known_hash
            needs_update = True
            best_known_updated_count += 1 # Count attempts/updates based on hash diff
            log.info(f"    Best Known Gender update for {email}: -> '{new_best_known_gender}' (Hash: {new_best_known_hash[:7]}...)")
        else:
            # Ensure existing best known hash is added if no update needed
            # (Best known gender value itself isn't strictly needed in output if unchanged)
            if output_best_known_hash_col not in row_updates:
                 row_updates[output_best_known_hash_col] = existing_best_known_hash # Could be None
            # Ensure best known gender field exists even if not updated this round
            if output_best_known_col not in row_updates:
                 row_updates[output_best_known_col] = row.get(existing_best_known_col) # Get existing value if available


        # --- Collect Row if Updated ---
        if needs_update:
            # Ensure all output columns are present, even if value is None
            for col in output_schema:
                if col not in row_updates:
                    row_updates[col] = None # Default missing updates to None
            update_records.append(row_updates)

        if processed_count % 100 == 0:
            log.info(f"Processed {processed_count}/{df_processed.height} contacts...")

    log.info(f"Gender processing finished. Processed: {processed_count}. FN Gender Updates Triggered: {fn_gender_updated_count}. Best Known Updates Triggered: {best_known_updated_count}. Total records with updates: {len(update_records)}")

    # --- Create Output DataFrame ---
    if update_records:
        output_df = pl.DataFrame(update_records, schema=output_schema)
        # Cast columns explicitly to handle potential Nones -> correct Dtype
        output_df = output_df.with_columns([
            pl.col(c).cast(output_schema[c]) for c in output_schema
        ])
    else:
        # Create empty DataFrame with correct schema
        output_df = pl.DataFrame({k: [] for k in output_schema.keys()}, schema=output_schema)

    return Output(
        output_df,
        metadata={
            "num_contacts_processed": processed_count,
            "num_fn_gender_updates_triggered": fn_gender_updated_count,
            "num_best_known_updates_triggered": best_known_updated_count,
            "num_output_rows": output_df.height,
            "output_columns": list(output_schema.keys()),
        }
    )

@asset(
    group_name="loops",
    description="Re-computes age from birthday when it changed or rolled over another year.",
    compute_kind="polars",
)
def loops_age_calculated_audience(
    context: AssetExecutionContext,
    loops_raw_audience: pl.DataFrame,
) -> Output[pl.DataFrame]:
    today = date.today()

    # Ensure calculatedCurrentAge is Int64, handle potential errors
    df = loops_raw_audience.with_columns(
        pl.col("calculatedCurrentAge").cast(pl.Int64, strict=False)
    )

    # Parse birthday column safely: truncate to YYYY-MM-DD then parse
    df = df.with_columns(
        pl.col("birthday")
        .str.slice(0, 10) # Extract first 10 characters (YYYY-MM-DD)
        .str.strptime(pl.Date, format="%Y-%m-%d", strict=False) # Parse the truncated string
        .alias("birthday_parsed")
    )

    # Define the condition for whether the birthday has passed this year using Polars expressions
    birthday_passed_this_year = (
        (pl.col("birthday_parsed").dt.month() < today.month) |
        ((pl.col("birthday_parsed").dt.month() == today.month) & (pl.col("birthday_parsed").dt.day() <= today.day))
    )

    # Compute new age
    df = df.with_columns(
        (
            today.year - pl.col("birthday_parsed").dt.year()
            - pl.when(birthday_passed_this_year).then(0).otherwise(1) # Subtract 1 if birthday hasn't passed yet
        )
        .cast(pl.Int64)
        .alias("new_age")
    )

    # Identify rows that need update:
    # - Birthday must be parseable
    # - AND (current age is null OR new age differs from current age)
    needs_update_filter = (
        pl.col("birthday_parsed").is_not_null() 
        & (
            pl.col("calculatedCurrentAge").is_null() 
            | (pl.col("new_age") != pl.col("calculatedCurrentAge"))
        )
    )
    needs_update_df = df.filter(needs_update_filter)

    # Build output DataFrame with only delta
    out_df = needs_update_df.select(
        pl.col("email"),
        pl.col("new_age").alias("calculatedCurrentAge"),
    )
    # Define the schema for the output table preview
    preview_schema = TableSchema(
        columns=[
            TableColumn("email", "string"),
            TableColumn("calculatedCurrentAge", "integer"),
        ]
    )
    # Convert the list of dicts to a list of TableRecord objects for the preview
    preview_records_list = [TableRecord(rec) for rec in out_df.head(5).to_dicts()]

    # Attach metadata for observability
    metadata = {
        "num_records_processed": loops_raw_audience.height, # Use input height for total processed
        "num_records_updated": needs_update_df.height, # Renamed from num_filtered_for_update
        "num_output_rows": out_df.height,
        "preview": MetadataValue.table(
            records=preview_records_list,
            schema=preview_schema
        ),
    }
    return Output(out_df, metadata=metadata)


@asset(
    group_name="loops",
    description="Combines geocoding, gender, and age updates into a single DataFrame, checking for conflicts.", # Updated description
)
def loops_audience_prepared_for_update(
    context: AssetExecutionContext,
    loops_geocoded_audience: pl.DataFrame, 
    loops_gender_categorized_audience: pl.DataFrame, 
    loops_age_calculated_audience: pl.DataFrame,
) -> Output[pl.DataFrame]: # Changed return type hint to Output
    """
    Combines geocoding, gender, and age update DataFrames based on 'email'.

    Checks for conflicts: if multiple inputs provide different non-null values 
    for the same field for a given email, an error is raised. 
    Otherwise, the single non-null value (or null) is kept.
    """
    log = context.log
    
    # --- 1. Collect and Filter Input DataFrames ---
    dfs: List[pl.DataFrame] = [
        loops_geocoded_audience, 
        loops_gender_categorized_audience, 
        loops_age_calculated_audience
    ]
    
    input_heights = {
        "geocoding": loops_geocoded_audience.height,
        "gender": loops_gender_categorized_audience.height,
        "age": loops_age_calculated_audience.height,
    }
    log.info(f"Input counts - Geo: {input_heights['geocoding']}, Gender: {input_heights['gender']}, Age: {input_heights['age']}")

    dfs = [df for df in dfs if not df.is_empty()]

    if not dfs:
        log.warning("All input DataFrames for merging are empty.")
        # Return empty DataFrame with metadata
        return Output(
            pl.DataFrame(), 
            metadata={                "num_output_records": 0,
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

    if "email" not in all_cols:
        raise DagsterInvariantViolationError("Input DataFrames must contain an 'email' column for merging.")
        
    value_cols = sorted(list(all_cols - {"email"})) 
    required_schema_cols = ["email"] + value_cols

    # Determine the target dtype for each column (use first non-null type found)
    target_schema = {}
    for col in required_schema_cols:
        potential_types = schemas.get(col, [])
        # Find the first non-null type encountered for this column across all DFs
        # Default to pl.Null if the column only ever contained nulls or didn't exist
        target_type = next((t for t in potential_types if t != pl.Null), pl.Null) 
        target_schema[col] = target_type
        log.debug(f"Target type for column '{col}': {target_type}")

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
                    # Cast if types differ and target is not Null
                    log.debug(f"Casting existing column '{col}' from {current_type} to {target_type}")
                    select_exprs.append(pl.col(col).cast(target_type))
                elif current_type == pl.Null and target_type != pl.Null:
                     # Cast Null column to target type if target is known
                     log.debug(f"Casting existing Null column '{col}' to {target_type}")
                     select_exprs.append(pl.col(col).cast(target_type))
                else:
                    # Types match, or target is Null (keep original)
                    select_exprs.append(pl.col(col))
            else:
                # Column is missing, add it with the target type
                log.debug(f"Adding missing column '{col}' with target type {target_type}")
                if target_type == pl.Null:
                    select_exprs.append(pl.lit(None).alias(col)) # Stays Null type
                else:
                    select_exprs.append(pl.lit(None).cast(target_type).alias(col)) # Cast null literal

        # Apply the selections and casts
        adjusted_df = df.select(select_exprs)
        adjusted_dfs.append(adjusted_df)
        log.debug(f"Adjusted schema for DF {i}: {adjusted_df.schema}")

    # Concatenate using relaxed strategy
    try:
        combined_long = pl.concat(adjusted_dfs, how="vertical_relaxed") 
    except Exception as e:
        log.error(f"Error during concatenation: {e}")
        # Log schemas before error for debugging
        for i, adj_df in enumerate(adjusted_dfs):
            log.error(f"Schema of adjusted DF {i} before concat error: {adj_df.schema}")
        raise # Re-raise the error
        
    log.debug(f"Concatenated long DataFrame shape: {combined_long.shape}")
    log.debug(f"Concatenated long DataFrame schema: {combined_long.schema}")

    # --- 4. Group, Aggregate, and Check Conflicts ---
    agg_exprs = []
    conflict_check_cols = []
    for col in value_cols:
        # Expression to count unique non-null values for the current column within the group
        unique_count_expr = pl.col(col).drop_nulls().unique().count()
        
        # Expression to get the first non-null value (which is the unique one if count == 1)
        first_value_expr = pl.col(col).drop_nulls().first()
        
        conflict_col_name = f"_{col}_has_conflict_" 

        # Define the expression for the final column value
        final_value_expr = (
            pl.when(unique_count_expr == 1) # Check if exactly one unique non-null value
            .then(first_value_expr)         # If yes, take that value
            .otherwise(pl.lit(None))        # Otherwise (0 or >1 unique), result is null
            .alias(col)                     # Name it as the original column
            # Ensure the output type matches the target schema, important if all inputs were null
            .cast(target_schema[col], strict=False) 
        )
        
        # Define the expression for the conflict flag
        conflict_flag_expr = (
            (unique_count_expr > 1) # True if more than one unique non-null value
            .alias(conflict_col_name)
        )

        agg_exprs.append(final_value_expr)
        agg_exprs.append(conflict_flag_expr)
        conflict_check_cols.append(conflict_col_name)

    # Perform the aggregation
    grouped = combined_long.group_by("email").agg(agg_exprs)
    log.debug(f"Grouped DataFrame shape (incl. conflict flags): {grouped.shape}")

    # --- 5. Check for Conflicts ---
    conflict_rows = grouped.filter(pl.any_horizontal(pl.col(c) for c in conflict_check_cols))

    if not conflict_rows.is_empty():
        error_details = []
        max_details = 10 
        for row in conflict_rows.head(max_details).iter_rows(named=True):
            email = row["email"]
            conflicting_fields = [
                c.replace("_has_conflict_", "").strip("_") 
                for c in conflict_check_cols if row[c]
            ]
            original_data = combined_long.filter(pl.col("email") == email).select(["email"] + conflicting_fields)
            error_details.append(f"  Email '{email}': Conflicts in fields: {', '.join(conflicting_fields)}\n    Original values:\n{original_data}")
        
        if conflict_rows.height > max_details:
             error_details.append(f"\n  ... and {conflict_rows.height - max_details} more conflicting emails.")

        raise DagsterInvariantViolationError(
            f"Conflicts detected during merge for {conflict_rows.height} emails:\n" + "\n".join(error_details)
        )

    # --- 6. Prepare Final Output ---
    final_df = grouped.select(["email"] + value_cols)
    log.info(f"Successfully merged inputs. Final shape: {final_df.shape}")

    # --- Generate Preview ---
    preview_df = final_df.head(5)
    preview_records = [TableRecord(rec) for rec in preview_df.to_dicts()]
    
    # Dynamically create TableSchema from final_df columns
    preview_schema_cols = []
    for col_name, dtype in final_df.schema.items():
        # Map Polars types to basic Dagster TableColumn types
        if dtype in (pl.Utf8, pl.String): type_str = "string"
        elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64): type_str = "integer"
        elif dtype in (pl.Float32, pl.Float64): type_str = "float"
        elif dtype == pl.Boolean: type_str = "boolean"
        elif dtype == pl.Date: type_str = "date"
        elif dtype == pl.Datetime: type_str = "datetime"
        else: type_str = "string" # Default fallback
        preview_schema_cols.append(TableColumn(col_name, type_str))
        
    preview_schema = TableSchema(columns=preview_schema_cols)
    
    # --- Return Output with Metadata (including preview) ---
    return Output(
        final_df,
        metadata={
            "num_output_records": final_df.height,
            "columns": final_df.columns,
             **{f"num_{name}_input_records": h for name, h in input_heights.items()},
            "preview": MetadataValue.table(
                records=preview_records,
                schema=preview_schema
            )
        }
    )

@asset(
    group_name="loops",
    description="Take the raw Loops export and apply every non-null field "
    "from loops_audience_prepared_for_update to produce an updated audience DF.",
)
def loops_processed_audience(
    context: AssetExecutionContext,
    loops_raw_audience: pl.DataFrame,
    loops_audience_prepared_for_update: pl.DataFrame,
) -> pl.DataFrame:
    # Log sizes
    context.log.info(f"Raw audience rows: {loops_raw_audience.height}")
    context.log.info(
        f"Updates to apply: {loops_audience_prepared_for_update.height}"
    )

    # 1) Left-join raw + updates on email, suffixing update cols with "upd"
    df = loops_raw_audience.join(
        loops_audience_prepared_for_update,
        on="email",
        how="left",
        suffix="upd",
    )

    # 2) For every update-column, coalesce(update, original) → original name
    update_cols = [
        c for c in loops_audience_prepared_for_update.columns if c != "email"
    ]
    for col in update_cols:
        upd = f"{col}_upd"
        if upd in df.columns:
            df = df.with_columns(
                pl.coalesce(pl.col(upd), pl.col(col)).alias(col)
            ).drop(upd)

    # 3) Metadata preview
    preview = df.head(5).to_dicts()
    context.add_output_metadata({
        "num_final_rows": df.height,
        "preview(5 rows)": MetadataValue.json(preview),
    })

    return df


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
    assets=[
        loops_raw_audience, 
        loops_geocoded_audience, 
        loops_gender_categorized_audience, 
        loops_audience_prepared_for_update,
        loops_age_calculated_audience,
        loops_processed_audience,
        loops_audience_update_status
    ],
    resources={
        "loops_session_token": EnvVar("LOOPS_SESSION_TOKEN"),
        "geocoder_client": GeocoderResource(),
        "ai_client": AIResource(),
        "genderize_client": GenderizeResource(),
        "loops_client": LoopsResource(),
    }
)
