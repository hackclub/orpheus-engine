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
    
    # Log input sizes
    log.info(f"Processing analytics data with:")
    log.info(f"- Loops audience rows: {loops_processed_audience.height}")
    log.info(f"- Field mapping rules: {field_mapping_rules.height}")
    log.info(f"- Program mapping rules: {program_mapping_rules.height}")
    log.info(f"- Programs: {programs.height}")
    
    # Start with the processed audience data
    df = loops_processed_audience.clone()
    
    # Apply field mappings
    for rule in field_mapping_rules.iter_rows(named=True):
        loops_field = rule.get("loops_so_field_to_map")
        hack_clubber_field = rule.get("hack_clubber_field")
        
        if loops_field in df.columns and hack_clubber_field:
            df = df.with_columns(
                pl.col(loops_field).alias(hack_clubber_field)
            )
    
    # Apply program mappings
    for rule in program_mapping_rules.iter_rows(named=True):
        loops_field = rule.get("loops_so_field_to_map")
        program_field = rule.get("program")
        
        if loops_field in df.columns and program_field:
            # Join with programs table to get program details
            df = df.join(
                programs.select(["id", program_field]),
                left_on=loops_field,
                right_on="id",
                how="left"
            )
    
    # Add metadata
    preview_records = [TableRecord(rec) for rec in df.head(5).to_dicts()]
    preview_schema_cols = []
    
    for col_name, dtype in df.schema.items():
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
        df,
        metadata={
            "num_output_records": df.height,
            "columns": df.columns,
            "preview": MetadataValue.table(
                records=preview_records,
                schema=preview_schema
            )
        }
    )
