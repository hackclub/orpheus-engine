import hashlib
import polars as pl
import os
import re
import requests
import json
import time
from datetime import datetime, timezone
import pytz
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
            AirtableIDs.unified_ysws_db.approved_projects.geocoded_latitude: float(lat),
            AirtableIDs.unified_ysws_db.approved_projects.geocoded_longitude: float(lng),
            AirtableIDs.unified_ysws_db.approved_projects.geocoded_country: country_name,
            AirtableIDs.unified_ysws_db.approved_projects.geocoded_country_code: country_code,
        }
    except (ValueError, TypeError, KeyError) as e:
        return None


def _build_address_string(row: Dict[str, Any]) -> str:
    """
    Build address string from approved_projects record fields using shared utility.
    Ensures identical formatting to Loops for maximum geocoding cache hits.
    """
    field_ids = {
        "address_line_1": AirtableIDs.unified_ysws_db.approved_projects.address_line_1,
        "address_line_2": AirtableIDs.unified_ysws_db.approved_projects.address_line_2,
        "city": AirtableIDs.unified_ysws_db.approved_projects.city,
        "state_province": AirtableIDs.unified_ysws_db.approved_projects.state_province,
        "zip_postal_code": AirtableIDs.unified_ysws_db.approved_projects.zip_postal_code,
        "country": AirtableIDs.unified_ysws_db.approved_projects.country,
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
    description="Prepares YSWS programs data for signup analysis by converting names to loops engagement prefixes",
    compute_kind="data_preparation",
    deps=[AssetKey(["airtable", "unified_ysws_db", "ysws_programs"])],
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
        base_key="unified_ysws_db",
        table_key="ysws_programs",
    )
    
    log.info(f"Processing {programs_df.height} YSWS programs")
    
    # Get the field IDs
    name_field_id = AirtableIDs.unified_ysws_db.ysws_programs.name
    override_field_id = AirtableIDs.unified_ysws_db.ysws_programs.sign_up_stats_override_prefix
    
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
    deps=[AssetKey(["airtable", "unified_ysws_db", "ysws_programs"])],
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
        base_key="unified_ysws_db",
        table_key="ysws_programs",
    )
    
    log.info(f"Processing {programs_df.height} YSWS programs for HCB data")
    
    # Get the HCB field ID
    hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
    
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
        total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
        hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
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
            total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
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
                total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
                hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
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
            total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
            
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
            total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "Request failed: {str(e)}" | {hcb_url}'
            })
            failed_count += 1
        except (KeyError, ValueError, TypeError) as e:
            log.error(f"Error parsing HCB data for {hcb_id}: {e}")
            # Create error record
            total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "Data parsing error: {str(e)}" | {hcb_url}'
            })
            failed_count += 1
        except Exception as e:
            log.error(f"Unexpected error processing HCB ID {hcb_id}: {e}")
            # Create error record
            total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
            hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
            all_records.append({
                "id": program_id,
                total_spent_field_id: None,
                hcb_field_id: f'ERROR: "Unexpected error: {str(e)}" | {hcb_url}'
            })
            failed_count += 1
    
    # Create output DataFrame
    total_spent_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_spent_from_hcb_fund
    hcb_field_id = AirtableIDs.unified_ysws_db.ysws_programs.hcb
    
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
    deps=[AssetKey(["airtable", "unified_ysws_db", "approved_projects"])],
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
        base_key="unified_ysws_db",
        table_key="approved_projects",
    )
    
    log.info(f"Processing {input_df.height} approved projects for geocoding candidates using vectorized operations")
    
    # Use field IDs as variables for cleaner code
    addr1_col = AirtableIDs.unified_ysws_db.approved_projects.address_line_1
    addr2_col = AirtableIDs.unified_ysws_db.approved_projects.address_line_2
    city_col = AirtableIDs.unified_ysws_db.approved_projects.city
    state_col = AirtableIDs.unified_ysws_db.approved_projects.state_province
    zip_col = AirtableIDs.unified_ysws_db.approved_projects.zip_postal_code
    country_col = AirtableIDs.unified_ysws_db.approved_projects.country
    hash_col = AirtableIDs.unified_ysws_db.approved_projects.geocoded_address_hash
    lat_col = AirtableIDs.unified_ysws_db.approved_projects.geocoded_latitude
    lng_col = AirtableIDs.unified_ysws_db.approved_projects.geocoded_longitude
    
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
    deps=[AssetKey(["airtable", "unified_ysws_db", "approved_projects"])],
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
        base_key="unified_ysws_db",
        table_key="approved_projects",
    )
    log.info(f"Retrieved {input_df.height} approved projects from Airtable")
    
    # Field IDs
    code_url_col = AirtableIDs.unified_ysws_db.approved_projects.code_url
    playable_url_col = AirtableIDs.unified_ysws_db.approved_projects.playable_url
    archive_code_url_col = AirtableIDs.unified_ysws_db.approved_projects.archive_code_url
    archive_live_url_col = AirtableIDs.unified_ysws_db.approved_projects.archive_live_url
    archive_hash_col = AirtableIDs.unified_ysws_db.approved_projects.archive_hash
    approved_at_col = AirtableIDs.unified_ysws_db.approved_projects.approved_at
    
    # Calculate archive hash and cutoff date
    eastern = pytz.timezone('US/Eastern')
    cutoff_date_str = eastern.localize(datetime(2025, 7, 8, 16, 0, 0)).astimezone(pytz.UTC).isoformat()
    log.info(f"Archive cutoff date: {cutoff_date_str} (July 8th, 2025 4pm ET)")
    
    log.info("Calculating archive hashes and filtering candidates")
    candidates = input_df.with_columns([
        pl.col(code_url_col).fill_null("").alias("code_url_clean"),
        pl.col(playable_url_col).fill_null("").alias("playable_url_clean")
    ]).with_columns([
        pl.concat_str([
            pl.lit("liveurl:"), pl.col("playable_url_clean"),
            pl.lit(",codeurl:"), pl.col("code_url_clean")
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
        # Hash changed or missing archive URLs
        ((pl.col("calculated_archive_hash") != pl.col(archive_hash_col)) |
         (pl.col(archive_code_url_col).is_null() | (pl.col(archive_code_url_col) == "")) |
         (pl.col(archive_live_url_col).is_null() | (pl.col(archive_live_url_col) == "")))
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
    description="Archives URLs and retrieves archive links for approved projects",
    compute_kind="archive_api",
)
def approved_projects_archived(
    context: AssetExecutionContext,
    approved_projects_archive_candidates: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Sends archive requests and immediately fetches archive URLs for candidate projects.
    Returns DataFrame with id and archive URL fields for Airtable updates.
    """
    log = context.log
    input_df = approved_projects_archive_candidates
    
    if input_df.height == 0:
        log.info("No archive candidates to process")
        return Output(
            pl.DataFrame(schema={
                "id": pl.Utf8,
                AirtableIDs.unified_ysws_db.approved_projects.archive_code_url: pl.Utf8,
                AirtableIDs.unified_ysws_db.approved_projects.archive_live_url: pl.Utf8,
                AirtableIDs.unified_ysws_db.approved_projects.archive_archived_at: pl.Utf8,
                AirtableIDs.unified_ysws_db.approved_projects.archive_hash: pl.Utf8,
            }),
            metadata={"num_processed": 0, "num_successful": 0, "num_failed": 0}
        )
    
    log.info(f"Starting archive processing for {input_df.height} projects")
    
    # Field IDs
    code_url_col = AirtableIDs.unified_ysws_db.approved_projects.code_url
    playable_url_col = AirtableIDs.unified_ysws_db.approved_projects.playable_url
    archive_code_url_col = AirtableIDs.unified_ysws_db.approved_projects.archive_code_url
    archive_live_url_col = AirtableIDs.unified_ysws_db.approved_projects.archive_live_url
    archive_archived_at_col = AirtableIDs.unified_ysws_db.approved_projects.archive_archived_at
    archive_hash_col = AirtableIDs.unified_ysws_db.approved_projects.archive_hash
    
    successful_records = []
    failed_count = 0
    
    # Archive API setup
    archive_base_url = "https://archive.hackclub.com/api/v1"
    archive_api_key = os.getenv("ARCHIVE_HACKCLUB_COM_API_KEY")
    if not archive_api_key:
        raise ValueError("ARCHIVE_HACKCLUB_COM_API_KEY environment variable is not set")
    
    # Process in batches of 10
    all_rows = list(input_df.iter_rows(named=True))
    total_batches = (len(all_rows) + 9) // 10
    log.info(f"Processing {len(all_rows)} projects in {total_batches} batches of 10")
    
    for batch_start in range(0, len(all_rows), 10):
        batch_rows = all_rows[batch_start:batch_start + 10]
        batch_num = (batch_start // 10) + 1
        log.info(f"Processing batch {batch_num}/{total_batches}: {len(batch_rows)} projects")
        
        batch_archive_requests = []
        batch_project_info = []
        
        # Collect URLs and project info for this batch
        for row in batch_rows:
            project_id = row.get("id")
            code_url = (row.get(code_url_col) or "").strip()
            playable_url = (row.get(playable_url_col) or "").strip()
            calculated_archive_hash = row.get("calculated_archive_hash", "")
            
            if not code_url and not playable_url:
                log.warning(f"No URLs to archive for project {project_id}")
                failed_count += 1
                continue
            
            current_time = datetime.now(timezone.utc).isoformat()
            project_info = {
                "id": project_id,
                "code_url": code_url,
                "playable_url": playable_url,
                "timestamp": current_time,
                "archive_hash": calculated_archive_hash
            }
            batch_project_info.append(project_info)
            
            # Add URLs with timestamps for archive request
            if code_url:
                batch_archive_requests.append(code_url + "#" + current_time)
            if playable_url:
                batch_archive_requests.append(playable_url + "#" + current_time)
        
        if not batch_archive_requests:
            log.warning(f"No valid URLs in batch {batch_num}")
            continue
        
        # Send batch archive request (fire-and-forget)
        log.info(f"Sending archive request for {len(batch_archive_requests)} URLs")
        try:
            requests.post(f"{archive_base_url}/cli/add", 
                headers={"Content-Type": "application/json", "X-ArchiveBox-API-Key": archive_api_key},
                json={"urls": batch_archive_requests, "tag": "ysws", "depth": 0, "update": False, 
                      "update_all": False, "index_only": False, "overwrite": False, "init": False, 
                      "extractors": "", "parser": "auto"}, timeout=2)
        except (requests.exceptions.Timeout, requests.exceptions.RequestException):
            log.debug("Archive request sent (expected timeout)")
            pass  # Expected
        
        # Wait and fetch archive URLs
        log.debug("Waiting 5 seconds for archive processing")
        time.sleep(5)
        
        # Get all snapshots and match to our URLs
        log.debug("Fetching archive snapshots to check status")
        try:
            response = requests.get(f"{archive_base_url}/core/snapshots", 
                headers={"accept": "application/json", "X-ArchiveBox-API-Key": archive_api_key},
                params={"with_archiveresults": "false", "limit": "1000", "offset": "0", "page": "0"}, 
                timeout=60)
            
            archive_results = {}
            if response.ok:
                items = response.json().get("items", [])
                log.debug(f"Retrieved {len(items)} snapshots from archive API")
                for item in items:
                    item_url = item.get("url", "")
                    timestamp = item.get("timestamp", "")
                    if item_url and timestamp:
                        for target_url in batch_archive_requests:
                            base_url = target_url.split("#")[0]
                            if item_url == base_url or item_url.rstrip('/') == base_url.rstrip('/'):
                                archive_results[target_url] = f"https://archive.hackclub.com/archive/{timestamp}"
                                break
            else:
                log.warning(f"Failed to fetch archive snapshots: HTTP {response.status_code}")
        except Exception as e:
            log.warning(f"Error fetching archive snapshots: {e}")
            archive_results = {}
        
        # Process results for each project
        batch_success_count = 0
        batch_failed_count = 0
        
        for project_info in batch_project_info:
            project_id = project_info["id"]
            code_url = project_info["code_url"]
            playable_url = project_info["playable_url"]
            timestamp = project_info["timestamp"]
            archive_hash = project_info["archive_hash"]
            
            # Check if we got archive URLs
            archive_code_url = archive_results.get(code_url + "#" + timestamp) if code_url else None
            archive_live_url = archive_results.get(playable_url + "#" + timestamp) if playable_url else None
            
            if archive_code_url or archive_live_url:
                # Success - create record with hash
                result_record = {
                    "id": project_id,
                    archive_archived_at_col: timestamp,
                    archive_hash_col: archive_hash
                }
                if archive_code_url:
                    result_record[archive_code_url_col] = archive_code_url
                if archive_live_url:
                    result_record[archive_live_url_col] = archive_live_url
                
                successful_records.append(result_record)
                batch_success_count += 1
                
                archived_urls = []
                if archive_code_url:
                    archived_urls.append("code")
                if archive_live_url:
                    archived_urls.append("live")
                log.info(f"Successfully archived {'/'.join(archived_urls)} URLs for project {project_id}")
            else:
                # Failed - will retry next run
                log.warning(f"Could not retrieve archive URLs for project {project_id} - will retry next run")
                failed_count += 1
                batch_failed_count += 1
        
        log.info(f"Batch {batch_num} completed: {batch_success_count} successful, {batch_failed_count} failed")
    
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
                AirtableIDs.unified_ysws_db.approved_projects.geocoded_latitude: pl.Float64,
                AirtableIDs.unified_ysws_db.approved_projects.geocoded_longitude: pl.Float64,
                AirtableIDs.unified_ysws_db.approved_projects.geocoded_country: pl.Utf8,
                AirtableIDs.unified_ysws_db.approved_projects.geocoded_country_code: pl.Utf8,
                AirtableIDs.unified_ysws_db.approved_projects.geocoded_address_hash: pl.Utf8,
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
                    AirtableIDs.unified_ysws_db.approved_projects.geocoded_address_hash: calculated_hash
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
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_latitude: pl.Float64,
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_longitude: pl.Float64,
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_country: pl.Utf8,
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_country_code: pl.Utf8,
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_address_hash: pl.Utf8,
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
) -> Output[pl.DataFrame]:
    """
    Merges geocoded and archived data for approved projects and prepares for Airtable batch update.
    
    Checks for conflicts: if multiple inputs provide different non-null values 
    for the same field for a given project ID, an error is raised. 
    Otherwise, the single non-null value (or null) is kept.
    """
    log = context.log
    
    # --- 1. Collect and Filter Input DataFrames ---
    dfs = [approved_projects_geocoded, approved_projects_archived]
    
    input_heights = {
        "geocoded": approved_projects_geocoded.height,
        "archived": approved_projects_archived.height,
    }
    log.info(f"Input counts - Geocoded: {input_heights['geocoded']}, Archived: {input_heights['archived']}")

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
    for row in input_df.iter_rows(named=True):
        record = {k: v for k, v in row.items() if v is not None}
        records_to_update.append(record)
    
    # Perform batch update
    result = airtable.batch_update_records(
        context=context,
        base_key="unified_ysws_db",
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
        total_signups_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_sign_ups
        total_signups_new_field_id = AirtableIDs.unified_ysws_db.ysws_programs.total_sign_ups_new_to_hack_club
        
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
        base_key="unified_ysws_db",
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


# Define the assets for this module
defs = Definitions(
    assets=[
        approved_projects_geocoding_candidates,
        approved_projects_geocoded,
        approved_projects_archive_candidates,
        approved_projects_archived,
        approved_projects_prepared_for_update,
        approved_projects_update_status,
        ysws_programs_sign_up_stats_candidates,
        ysws_programs_sign_up_stats,
        ysws_programs_hcb_candidates,
        ysws_programs_hcb_stats,
        ysws_programs_prepared_for_update,
        ysws_programs_update_status,
    ],
)
