import hashlib
import polars as pl
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
    description="Prepares geocoded approved projects data for Airtable batch update.",
    compute_kind="data_preparation",
)
def approved_projects_prepared_for_update(
    context: AssetExecutionContext,
    approved_projects_geocoded: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Prepares geocoded records for Airtable batch update by formatting the data structure.
    """
    log = context.log
    input_df = approved_projects_geocoded
    
    if input_df.height == 0:
        log.info("No geocoded records to prepare for update.")
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8}),
            metadata={"num_records_prepared": 0}
        )
    
    log.info(f"Preparing {input_df.height} geocoded records for Airtable update")
    
    # The input DataFrame already has the correct structure for updates
    # We just need to ensure the 'id' column is present and properly formatted
    required_columns = ["id"]
    missing_columns = [col for col in required_columns if col not in input_df.columns]
    
    if missing_columns:
        log.error(f"Missing required columns for update: {missing_columns}")
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8}),
            metadata={"num_records_prepared": 0, "error": f"Missing columns: {missing_columns}"}
        )
    
    # Validate that we have actual geocoded data
    geocoded_fields = [
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_latitude,
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_longitude,
        AirtableIDs.unified_ysws_db.approved_projects.geocoded_address_hash,
    ]
    
    # Check if we have the expected geocoded fields
    available_geocoded_fields = [field for field in geocoded_fields if field in input_df.columns]
    
    if not available_geocoded_fields:
        log.error("No geocoded fields found in input DataFrame")
        return Output(
            pl.DataFrame(schema={"id": pl.Utf8}),
            metadata={"num_records_prepared": 0, "error": "No geocoded fields found"}
        )
    
    log.info(f"Available geocoded fields for update: {available_geocoded_fields}")
    
    # Generate preview metadata
    preview_limit = 10
    if input_df.height > 0:
        preview_df = input_df.head(preview_limit)
        try:
            preview_metadata = MetadataValue.md(preview_df.to_pandas().to_markdown(index=False))
        except Exception as md_err:
            log.warning(f"Could not generate markdown preview: {md_err}")
            preview_metadata = MetadataValue.text(str(preview_df))
    else:
        preview_metadata = MetadataValue.text("No records prepared for update.")
    
    return Output(
        input_df,
        metadata={
            "num_records_prepared": input_df.height,
            "geocoded_fields": available_geocoded_fields,
            "update_columns": list(input_df.columns),
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


# Define the assets for this module
defs = Definitions(
    assets=[
        approved_projects_geocoding_candidates,
        approved_projects_geocoded,
        approved_projects_prepared_for_update,
        approved_projects_update_status,
    ],
)
