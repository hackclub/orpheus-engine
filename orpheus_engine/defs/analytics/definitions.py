import polars as pl
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    TableSchema,
    TableColumn,
    TableRecord,
    AssetIn,
)
from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, List
from orpheus_engine.defs.airtable.generated_ids import AirtableIDs

# Constants for filtering and logic
HACK_CLUBBER_USER_GROUP = "Hack Clubber"
YSWS_PROGRAM_KEYWORD = "YSWS" # Used to identify YSWS program names
YSWS_APPROVED_KEYWORD = "approved" # Used to identify YSWS approval engagements
ONLY_LAST_YEAR = True # Mirror JS behavior - filter engagements older than 365 days

def format_airtable_date(dt: Optional[datetime]) -> Optional[str]:
    """Formats datetime objects into ISO 8601 strings for Airtable."""
    if dt is None:
        return None
    # Airtable expects ISO 8601 format, usually with 'Z' for UTC.
    # Polars might parse timezone info, ensure it's handled or assumed UTC if needed.
    if dt.tzinfo is None:
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z') # Add milliseconds and Z
    else:
        # If timezone-aware, convert to UTC and format
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

@asset(
    group_name="analytics",
    description="Processes Loops audience data with field and program mappings to create a comprehensive analytics dataset.",
    compute_kind="polars",
    ins={
        "field_mapping_rules": AssetIn(key_prefix=["airtable", "analytics_hack_clubbers"]),
        "program_mapping_rules": AssetIn(key_prefix=["airtable", "analytics_hack_clubbers"]),
        "programs": AssetIn(key_prefix=["airtable", "analytics_hack_clubbers"]),
    },
)
def analytics_hack_clubbers(
    context: AssetExecutionContext,
    loops_processed_audience: pl.DataFrame,
    field_mapping_rules: pl.DataFrame,
    program_mapping_rules: pl.DataFrame,
    programs: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Processes the Loops audience data by applying field and program mappings to create
    a comprehensive analytics dataset for Hack Clubbers.
    
    Args:
        context: The Dagster execution context
        loops_processed_audience: The processed Loops audience data
        field_mapping_rules: Rules for mapping Loops fields to Hack Clubber fields
        program_mapping_rules: Rules for mapping Loops fields to programs
        programs: The programs reference data
        
    Returns:
        A DataFrame containing the processed analytics data for Hack Clubbers
    """
    log = context.log
    
    # Log input sizes and sample data
    log.info(f"Processing analytics data with {loops_processed_audience.height} rows")
    log.info(f"Sample row columns: {loops_processed_audience.columns}")
    if loops_processed_audience.height > 0:
        sample_row = loops_processed_audience.head(1).to_dicts()[0]
        log.info(f"Sample row data: {sample_row}")
    
    log.info(f"Field mapping rules: {field_mapping_rules.to_dicts()}")
    log.info(f"Program mapping rules: {program_mapping_rules.to_dicts()}")
    log.info(f"Programs DataFrame columns: {programs.columns}")
    log.info(f"Programs sample data: {programs.head(5).to_dicts()}")
    log.info(f"Programs total rows: {programs.height}")

    # --- 1. Load Airtable Field IDs ---
    try:
        # Access the specific nested class for the target table
        HcFields = AirtableIDs.analytics_hack_clubbers.hack_clubbers
    except AttributeError:
        log.error("Could not access AirtableIDs.analytics_hack_clubbers.hack_clubbers. "
                  "Ensure generated_ids.py is up-to-date and defines this structure.")
        raise ValueError("Missing required Airtable Field ID definitions.")

    # --- 2. Prepare Mapping Dictionaries ---
    # Field Mapping: Loops Field Name -> Airtable Field Name
    try:
        field_map_loops_to_airtable_name = {}
        for rule in field_mapping_rules.iter_rows(named=True):
            loops_field = rule.get("fldnjsCqhetjq4wBh")  # This is the field ID for 'Loops.so Field To Map'
            hack_clubber_field = rule.get("fldd3p4ZDWIjjkdKt")  # This is the field ID for 'Hack Clubber Field'
            
            if loops_field and hack_clubber_field:
                field_map_loops_to_airtable_name[loops_field] = hack_clubber_field

        log.info(f"Loaded {len(field_map_loops_to_airtable_name)} field mapping rules")

    except Exception as e:
        log.error(f"Error processing field mapping rules: {e}")
        raise

    # Program Mapping: Loops Field Name -> Airtable Program Record ID
    try:
        program_map_loops_to_program_id = {
            rule['fldgtnrn7tq1dNjDT']: rule['fld6xM9D8pPFankEB'][0]  # These are the field IDs for the mapping fields
            for rule in program_mapping_rules.iter_rows(named=True)
            if rule.get('fldgtnrn7tq1dNjDT') and rule.get('fld6xM9D8pPFankEB') and isinstance(rule.get('fld6xM9D8pPFankEB'), list) and len(rule.get('fld6xM9D8pPFankEB')) > 0
        }
        log.info(f"Loaded {len(program_map_loops_to_program_id)} program mapping rules")

        # Check for unmapped engagement fields - mirror JS logic
        log.info("Checking field mappings:")
        
        # Get all mapped field names from both program and field mappings
        mapped_field_names = set(program_map_loops_to_program_id.keys()) | set(field_map_loops_to_airtable_name.keys())
        
        # Get all fields from the first row that end with 'At', excluding createdAt
        if loops_processed_audience.height > 0:
            first_row = loops_processed_audience.head(1).to_dicts()[0]
            unmapped_timestamp_fields = [
                field for field in first_row.keys()
                if field.endswith('At') and field != 'createdAt' and field not in mapped_field_names
            ]
            
            if unmapped_timestamp_fields:
                for unmapped in sorted(unmapped_timestamp_fields):
                    log.warning(f"  {unmapped} is not mapped!")
            else:
                log.info("  No unmapped fields!")
        
        log.info("")  # Add blank line for readability

    except Exception as e:
        log.error(f"Error processing program mapping rules: {e}")
        raise

    # Program ID -> Program Name (for engagement logic)
    try:
        # Create a mapping of program IDs to names
        program_id_to_name_map = {}
        for row in programs.iter_rows(named=True):
            # The program name is stored in the fldYqdE55u9NgTpTd field
            if row.get('id') and row.get('fldYqdE55u9NgTpTd'):
                program_id_to_name_map[row['id']] = row['fldYqdE55u9NgTpTd']
            else:
                log.warning(f"Program row missing id or name: {row}")
        log.info(f"Loaded {len(program_id_to_name_map)} program names")
    except Exception as e:
        log.error(f"Error processing programs data: {e}")
        raise

    # --- 3. Process Rows ---
    processed_rows_for_df = []
    filtered_out_count = 0
    today = date.today()

    # Check for required columns in input df
    required_cols = ['email', 'userGroup', 'subscribed']
    missing_cols = [col for col in required_cols if col not in loops_processed_audience.columns]
    if missing_cols:
        raise ValueError(f"Input DataFrame loops_processed_audience is missing required columns: {missing_cols}")

    # Apply initial filters using Polars and track detailed stats
    filtered_by_email_null = loops_processed_audience.filter(pl.col('email').is_null()).height
    filtered_by_hack_af = loops_processed_audience.filter(pl.col('email').str.ends_with('@hack.af')).height
    filtered_by_not_subscribed = loops_processed_audience.filter(pl.col('subscribed') != True).height
    filtered_by_wrong_group = loops_processed_audience.filter(pl.col('userGroup') != HACK_CLUBBER_USER_GROUP).height

    filtered_df = loops_processed_audience.filter(
        (pl.col('email').is_not_null()) &
        (~pl.col('email').str.ends_with('@hack.af')) &
        (pl.col('subscribed') == True) &
        (pl.col('userGroup') == HACK_CLUBBER_USER_GROUP)
    )

    # Count initial filtered out records
    filtered_out_count = loops_processed_audience.height - filtered_df.height

    log.info(f"Initial filtering results:")
    log.info(f"  Total records: {loops_processed_audience.height}")
    log.info(f"  Records after filtering: {filtered_df.height}")
    log.info(f"  Filtered out: {filtered_out_count}")
    log.info(f"  Filtered by email being null: {filtered_by_email_null}")
    log.info(f"  Filtered by hack.af domain: {filtered_by_hack_af}")
    log.info(f"  Filtered by not subscribed: {filtered_by_not_subscribed}")
    log.info(f"  Filtered by wrong user group: {filtered_by_wrong_group}")

    # Process all records
    total_engagements = 0
    total_ysws_approvals = 0
    total_records_processed = 0
    total_filtered_by_engagements = 0
    total_filtered_by_year = 0

    for row in filtered_df.iter_rows(named=True):
        email = row.get('email')
        
        # --- Calculate Engagements (Mimic JS) ---
        engagements = []
        airtable_program_ids_for_row = set() # Use set for uniqueness

        for loops_field, program_id in program_map_loops_to_program_id.items():
            engagement_timestamp = row.get(loops_field)
            if engagement_timestamp:
                if isinstance(engagement_timestamp, str):
                    try:
                        # Handle different timestamp formats
                        if 'UTC' in engagement_timestamp:
                            engagement_timestamp = datetime.strptime(engagement_timestamp, '%Y-%m-%d %H:%M:%S UTC')
                        else:
                            engagement_timestamp = datetime.fromisoformat(engagement_timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        continue

                if isinstance(engagement_timestamp, datetime):
                    # Ensure timezone-aware datetime
                    if engagement_timestamp.tzinfo is None:
                        engagement_timestamp = engagement_timestamp.replace(tzinfo=timezone.utc)
                    
                    # Get program name from the mapping
                    program_name = program_id_to_name_map.get(program_id)
                    if program_name:
                        engagements.append({
                            "name": loops_field,
                            "time": engagement_timestamp,
                            "programName": program_name,
                            "programId": program_id
                        })
                        airtable_program_ids_for_row.add(program_id)

        if not engagements:
            total_filtered_by_engagements += 1
            continue

        # Sort engagements: most recent first
        engagements.sort(key=lambda x: x['time'], reverse=True)
        last_engagement_time = engagements[0]['time']

        # Apply ONLY_LAST_YEAR filter
        if ONLY_LAST_YEAR:
            # Ensure we're comparing timezone-aware datetimes
            now = datetime.now(timezone.utc)
            days_since_engagement = (now - last_engagement_time).days
            if days_since_engagement > 365:
                total_filtered_by_year += 1
                continue

        # Count YSWS approvals
        ysws_approvals = [
            e for e in engagements
            if YSWS_PROGRAM_KEYWORD in (e.get('programName') or '')
            and YSWS_APPROVED_KEYWORD in e['name'].lower()
        ]
        
        total_engagements += len(engagements)
        total_ysws_approvals += len(ysws_approvals)
        
        # --- Build Airtable Row Data ---
        airtable_row_data = {}

        # 1. Apply Field Mapping Rules
        for loops_field, airtable_field_name in field_map_loops_to_airtable_name.items():
            if loops_field in row and row[loops_field] is not None:
                value = row[loops_field]
                if isinstance(value, datetime):
                    airtable_row_data[airtable_field_name] = format_airtable_date(value)
                else:
                    airtable_row_data[airtable_field_name] = value

        # 2. Calculate Derived Fields
        first_engagement = engagements[-1]
        last_engagement = engagements[0]

        airtable_row_data['Last Engagement At'] = format_airtable_date(last_engagement['time'])
        airtable_row_data['Last Engagement'] = last_engagement['name']
        airtable_row_data['First Engagement At'] = format_airtable_date(first_engagement['time'])
        airtable_row_data['First Engagement'] = first_engagement['name']
        airtable_row_data['Total Engagements'] = len(engagements)

        # Programs (Linked Record List)
        airtable_row_data['Programs'] = ','.join(sorted(airtable_program_ids_for_row))

        # First Program (Linked Record List)
        airtable_row_data['First Program'] = first_engagement['programId']

        # Second Program (Linked Record List)
        if len(engagements) > 1:
            second_program_engagement = next((e for e in reversed(engagements) if e['programName'] != first_engagement['programName']), None)
            if second_program_engagement:
                airtable_row_data['Second Program'] = second_program_engagement['programId']

        # YSWS Calculations
        if ysws_approvals:
            airtable_row_data['Last YSWS Approved At'] = format_airtable_date(ysws_approvals[0]['time'])
            airtable_row_data['Last YSWS Approved'] = ysws_approvals[0]['name']
            airtable_row_data['YSWS Approved Count'] = len(ysws_approvals)

        # Engagements Overview (Multiline Text)
        airtable_row_data['Engagements Overview'] = "\n".join(
            f"{e['name']} {e['time'].strftime('%Y-%m-%d')}"
            for e in engagements
        )

        # Ensure Birthday is formatted correctly if present
        if 'Birthday' in airtable_row_data:
            bday_val = airtable_row_data['Birthday']
            if isinstance(bday_val, date) and not isinstance(bday_val, datetime):
                airtable_row_data['Birthday'] = bday_val.strftime('%Y-%m-%d')
            elif isinstance(bday_val, datetime):
                airtable_row_data['Birthday'] = bday_val.strftime('%Y-%m-%d')

        # Add the processed row to our list
        processed_rows_for_df.append(airtable_row_data)
        total_records_processed += 1

    # Update total filtered count
    filtered_out_count += total_filtered_by_engagements + total_filtered_by_year

    log.info(f"Final processing results:")
    log.info(f"  Total records processed: {total_records_processed}")
    log.info(f"  Total engagements: {total_engagements}")
    log.info(f"  Total YSWS approvals: {total_ysws_approvals}")
    log.info(f"  Total filtered by no engagements: {total_filtered_by_engagements}")
    log.info(f"  Total filtered by year: {total_filtered_by_year}")
    log.info(f"  Total filtered out: {filtered_out_count}")
    log.info(f"  Records to include: {len(processed_rows_for_df)}")

    # --- 4. Create Final DataFrame ---
    if not processed_rows_for_df:
        log.warning("No rows remaining after filtering. Returning empty DataFrame.")
        final_df = pl.DataFrame()
    else:
        final_df = pl.DataFrame(processed_rows_for_df)

    # --- 5. Metadata & Output ---
    preview_records = [TableRecord(rec) for rec in final_df.head(5).to_dicts()]
    preview_schema_cols = []
    
    for col_name, dtype in final_df.schema.items():
        if dtype in (pl.Utf8, pl.String): type_str = "string"
        elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64): type_str = "integer"
        elif dtype in (pl.Float32, pl.Float64): type_str = "float"
        elif dtype == pl.Boolean: type_str = "boolean"
        elif dtype == pl.Date: type_str = "date"
        elif dtype == pl.Datetime: type_str = "datetime"
        else: type_str = "string"
        preview_schema_cols.append(TableColumn(col_name, type_str))
    
    preview_schema = TableSchema(columns=preview_schema_cols)
    
    return Output(
        final_df,
        metadata={
            "num_input_records": loops_processed_audience.height,
            "num_filtered_out": filtered_out_count,
            "num_output_records": final_df.height,
            "columns": final_df.columns,
            "preview": MetadataValue.table(
                records=preview_records,
                schema=preview_schema
            )
        }
    )
