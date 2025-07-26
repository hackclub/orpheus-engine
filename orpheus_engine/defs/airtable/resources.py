import polars as pl
from dagster import ConfigurableResource, InitResourceContext, EnvVar, Field, AssetExecutionContext
from pyairtable import Api, Table
from pyairtable.formulas import match
from typing import List, Dict, Any, Optional, Union, Set
import json

# Import the config models
from .config import AirtableServiceConfig, AirtableBaseConfig, AirtableTableConfig

# Import the specific schema model
from pyairtable.models.schema import TableSchema

class AirtableApiError(Exception):
    """Custom exception for errors interacting with the Airtable API."""
    pass

class AirtableResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the Airtable API using pyairtable.
    Requires the AIRTABLE_PERSONAL_ACCESS_TOKEN environment variable.
    Configured with details of the bases and tables to interact with.
    """

    api_key: str = EnvVar("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    # The Pydantic model defines the config schema
    config: AirtableServiceConfig

    _api_client: Optional[Api] = None
    _base_clients: Dict[str, Any] = {} # Could store Base objects if pyairtable had them, use Api for now
    _table_clients: Dict[tuple[str, str], Table] = {} # Cache Table objects: (base_key, table_key) -> Table

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initializes the Airtable API client."""
        try:
            self._api_client = Api(self.api_key)
            # You could pre-initialize table objects here if desired, but lazy init is also fine
        except Exception as e:
            raise AirtableApiError(f"Failed to initialize Airtable client: {e}") from e

    @property
    def client(self) -> Api:
        """Provides access to the initialized pyairtable Api client."""
        if self._api_client is None:
            # This typically shouldn't happen if setup_for_execution runs correctly
            # Re-initialize if needed, or raise error if strictness desired
            try:
                self._api_client = Api(self.api_key)
                # Consider logging a warning if re-initialization happens unexpectedly
            except Exception as e:
                 raise AirtableApiError("Airtable resource is not initialized and failed to re-initialize.") from e
        return self._api_client

    def _get_base_config(self, base_key: str) -> AirtableBaseConfig:
        """Retrieves the configuration for a specific base key."""
        base_config = self.config.bases.get(base_key)
        if not base_config:
            raise AirtableApiError(f"Base key '{base_key}' not found in AirtableResource configuration.")
        return base_config

    def _get_table_config(self, base_key: str, table_key: str) -> AirtableTableConfig:
        """Retrieves the configuration for a specific table key within a base."""
        base_config = self._get_base_config(base_key)
        table_config = base_config.tables.get(table_key)
        if not table_config:
            raise AirtableApiError(f"Table key '{table_key}' not found under base '{base_key}' in configuration.")
        return table_config

    def get_table(self, base_key: str, table_key: str) -> Table:
        """
        Gets a pyairtable Table object for the configured base and table key.
        Uses cached objects for efficiency.
        """
        if (base_key, table_key) in self._table_clients:
            return self._table_clients[(base_key, table_key)]

        base_config = self._get_base_config(base_key)
        table_config = self._get_table_config(base_key, table_key)

        try:
            table = self.client.table(base_config.base_id, table_config.table_id)
            self._table_clients[(base_key, table_key)] = table # Cache it
            return table
        except Exception as e:
            raise AirtableApiError(
                f"Failed to get pyairtable Table object for base '{base_key}' (ID: {base_config.base_id}), "
                f"table '{table_key}' (ID: {table_config.table_id}): {e}"
            ) from e

    def _clean_airtable_value(self, value: Any) -> Any:
        """
        Clean and normalize values from Airtable, handling special cases:
        - Detect and convert special values like {"specialValue": "NaN"} to None
        - Detect and convert error values like {"error": "#ERROR!"} to None
        - Parse any stringified JSON objects
        - Handle other edge cases
        """
        # If it's a string that looks like JSON, try to parse it 
        if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
            try:
                parsed_value = json.loads(value)
                # Check for Airtable special values
                if isinstance(parsed_value, dict):
                    if "specialValue" in parsed_value:
                        # Convert special values to None
                        return None
                    elif "error" in parsed_value:
                        # Convert error values to None
                        return None
                return parsed_value
            except (json.JSONDecodeError, ValueError):
                # Not valid JSON, return as is
                return value
        
        # Handle dict special values directly (not as strings)
        if isinstance(value, dict):
            if "specialValue" in value:
                return None
            elif "error" in value:
                return None
            
        # If it's a list, process each item
        if isinstance(value, list):
            return [self._clean_airtable_value(item) for item in value]
            
        # Return other types unchanged
        return value

    def get_all_records_as_polars(
        self,
        context: AssetExecutionContext,
        base_key: str,
        table_key: str,
        fields: Optional[List[str]] = None,
        formula: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Fetches all records from a specific Airtable table (identified by logical keys)
        and returns them as a Polars DataFrame. Columns will be field *IDs* plus the record 'id'.
        The 'createdTime' column is excluded.

        Handles pagination automatically via pyairtable.

        Args:
            context: The Dagster execution context for logging.
            base_key: The logical key for the base (defined in resource config).
            table_key: The logical key for the table (defined in resource config).
            fields: Optional list of field *names* to retrieve. If None, retrieves all fields.
                    Note: pyairtable filters by name even when returning IDs.
            formula: Optional Airtable formula string to filter records.

        Returns:
            A Polars DataFrame containing the records. Columns are field IDs plus 'id'.

        Raises:
            AirtableApiError: If the API request fails or configuration is invalid.
        """
        table = self.get_table(base_key, table_key)
        table_config = self._get_table_config(base_key, table_key)

        try:
            formula_obj = match(formula) if formula else None
            all_records_raw: List[Dict[str, Any]] = table.all(
                fields=fields,
                formula=formula_obj,
                use_field_ids=True
            )

            if not all_records_raw:
                try:
                    schema_info: TableSchema = self.get_table_schema(base_key, table_key)
                    # Extract field IDs, ensure 'id' is first
                    base_col_ids = [f.id for f in schema_info.fields]
                    col_ids = ["id"] + base_col_ids # Place 'id' first

                    if fields:
                        name_to_id = {f.name: f.id for f in schema_info.fields}
                        # Filter base_col_ids based on requested field names
                        filtered_base_col_ids = []
                        for field_name in fields:
                            if field_name in name_to_id:
                                filtered_base_col_ids.append(name_to_id[field_name])
                            else:
                                context.log.warning(f"Requested field name '{field_name}' not found in schema for {base_key}.{table_key}")
                        # Reconstruct col_ids with 'id' first, then filtered field IDs
                        col_ids = ["id"] + filtered_base_col_ids

                    empty_schema = {col_id: pl.Utf8 for col_id in col_ids}
                    # Ensure the schema dict is ordered correctly if polars doesn't preserve dict order
                    # Creating DF with explicit schema order is safer
                    ordered_empty_schema = {"id": pl.Utf8}
                    ordered_empty_schema.update({col_id: pl.Utf8 for col_id in col_ids if col_id != "id"})
                    return pl.DataFrame(schema=ordered_empty_schema)

                except Exception as schema_err:
                    context.log.warning(f"Could not fetch/process schema to build empty DataFrame for {base_key}.{table_key}: {schema_err}")
                    return pl.DataFrame(schema={"id": pl.Utf8})

            # Get all field IDs from table schema to ensure we include all fields
            try:
                schema_info: TableSchema = self.get_table_schema(base_key, table_key)
                all_fields = set(["id"] + [f.id for f in schema_info.fields])
                
                # Filter fields if specific fields were requested
                if fields:
                    name_to_id = {f.name: f.id for f in schema_info.fields}
                    requested_field_ids = set(["id"])
                    for field_name in fields:
                        if field_name in name_to_id:
                            requested_field_ids.add(name_to_id[field_name])
                        else:
                            context.log.warning(f"Requested field name '{field_name}' not found in schema for {base_key}.{table_key}")
                    all_fields = requested_field_ids
                    
            except Exception as schema_err:
                context.log.warning(f"Could not fetch schema for {base_key}.{table_key}, falling back to record-based field discovery: {schema_err}")
                # Fallback to old behavior
                all_fields = set(["id"])
                for record in all_records_raw:
                    for field_id in record.get("fields", {}).keys():
                        all_fields.add(field_id)
            
            # First pass: Pre-process all records to clean values
            preprocessed_records = []
            
            for record in all_records_raw:
                cleaned_record = {"id": record.get("id")}
                
                # Process all known fields from schema, not just fields with data
                for field_id in all_fields:
                    if field_id == "id":
                        continue
                    value = record.get("fields", {}).get(field_id)
                    # Clean the values (None for missing fields)
                    cleaned_record[field_id] = self._clean_airtable_value(value) if value is not None else None
                
                preprocessed_records.append(cleaned_record)
                
            # Second pass: Identify fields with multi-item lists
            fields_requiring_lists = set()
            
            for record in preprocessed_records:
                for field_id, value in record.items():
                    if isinstance(value, list) and len(value) > 1:
                        fields_requiring_lists.add(field_id)
            
            context.log.info(f"Fields with multi-item lists in {base_key}.{table_key}: {sorted(fields_requiring_lists)}")
            
            # Third pass: Analyze data types for intelligent casting
            field_type_analysis = {}
            
            for field_id in all_fields:
                if field_id in fields_requiring_lists:
                    field_type_analysis[field_id] = "list"
                    continue
                    
                # Analyze non-list fields for best type
                values = []
                for record in preprocessed_records:
                    if field_id in record and record[field_id] is not None:
                        value = record[field_id]
                        if isinstance(value, list):
                            if len(value) == 1:
                                values.append(value[0])
                            elif len(value) == 0:
                                continue  # Skip empty lists
                        else:
                            values.append(value)
                
                if not values:
                    field_type_analysis[field_id] = "string"
                    continue
                
                # Determine best type based on values
                all_numeric = True
                all_int = True
                all_bool = True
                
                for value in values:
                    if value is None:
                        continue
                    
                    # Check if it's boolean
                    if not isinstance(value, bool) and value not in [True, False, 0, 1, "true", "false", "True", "False"]:
                        all_bool = False
                    
                    # Check if it's numeric
                    try:
                        float_val = float(value)
                        if float_val != int(float_val):
                            all_int = False
                    except (ValueError, TypeError):
                        all_numeric = False
                        all_int = False
                        all_bool = False
                        break
                
                if all_bool and len(set(str(v).lower() for v in values if v is not None)) <= 2:
                    field_type_analysis[field_id] = "boolean"
                elif all_int:
                    field_type_analysis[field_id] = "integer"
                elif all_numeric:
                    field_type_analysis[field_id] = "float"
                else:
                    field_type_analysis[field_id] = "string"
            
            context.log.info(f"Field type analysis for {base_key}.{table_key}: {field_type_analysis}")
            
            # Fourth pass: Prepare final records with intelligent type casting
            final_records = []
            for record in preprocessed_records:
                final_record = {}
                
                # Process all known fields to ensure consistency
                for field_id in all_fields:
                    target_type = field_type_analysis[field_id]
                    
                    if field_id not in record:
                        # Field missing in this record
                        if target_type == "list":
                            final_record[field_id] = []
                        else:
                            final_record[field_id] = None
                    else:
                        value = record[field_id]
                        
                        if target_type == "list":
                            # This field requires list type
                            if isinstance(value, list):
                                final_record[field_id] = [str(item) if item is not None else None for item in value]
                            else:
                                final_record[field_id] = [str(value)] if value is not None else []
                        else:
                            # Extract single value from list if needed
                            if isinstance(value, list):
                                if len(value) == 0:
                                    extracted_value = None
                                elif len(value) == 1:
                                    extracted_value = value[0]
                                else:
                                    context.log.warning(f"Unexpected multi-item list for field {field_id}")
                                    extracted_value = value[0]
                            else:
                                extracted_value = value
                            
                            # Cast to target type
                            if extracted_value is None:
                                final_record[field_id] = None
                            elif target_type == "boolean":
                                if isinstance(extracted_value, bool):
                                    final_record[field_id] = extracted_value
                                elif str(extracted_value).lower() in ["true", "1"]:
                                    final_record[field_id] = True
                                elif str(extracted_value).lower() in ["false", "0"]:
                                    final_record[field_id] = False
                                else:
                                    final_record[field_id] = bool(extracted_value)
                            elif target_type == "integer":
                                try:
                                    final_record[field_id] = int(float(extracted_value))
                                except (ValueError, TypeError):
                                    final_record[field_id] = None
                            elif target_type == "float":
                                try:
                                    final_record[field_id] = float(extracted_value)
                                except (ValueError, TypeError):
                                    final_record[field_id] = None
                            else:  # string
                                final_record[field_id] = str(extracted_value) if extracted_value is not None else None
                
                final_records.append(final_record)
            
            # Build schema based on our type analysis
            schema = {}
            for field_id in all_fields:
                target_type = field_type_analysis[field_id]
                if target_type == "list":
                    schema[field_id] = pl.List(pl.Utf8)
                elif target_type == "boolean":
                    schema[field_id] = pl.Boolean
                elif target_type == "integer":
                    schema[field_id] = pl.Int64
                elif target_type == "float":
                    schema[field_id] = pl.Float64
                else:  # string
                    schema[field_id] = pl.Utf8
            
            # Create DataFrame with explicit schema
            context.log.info(f"Creating DataFrame from {len(final_records)} records for {base_key}.{table_key}")
            df = pl.DataFrame(final_records, schema=schema)
            
            # Ensure 'id' column is first
            if "id" in df.columns:
                other_cols = [col for col in df.columns if col != 'id']
                df = df.select(["id"] + other_cols)
            
            return df

        except Exception as e:
            context.log.error(f"Error processing data: {str(e)}")
            raise AirtableApiError(
                f"Failed to fetch records from Airtable base '{base_key}', table '{table_key}' "
                f"(Table ID: {table_config.table_id}): {e}"
            ) from e

    def get_table_schema(self, base_key: str, table_key: str) -> TableSchema:
        """
        Retrieves the schema (including field names and IDs) for a specific table.

        Args:
            base_key: The logical key for the base.
            table_key: The logical key for the table.

        Returns:
            A pyairtable.models.schema.TableSchema object representing the table schema.

        Raises:
            AirtableApiError: If the metadata API request fails.
        """
        base_config = self._get_base_config(base_key)
        table_config = self._get_table_config(base_key, table_key)
        try:
            table = self.client.table(base_config.base_id, table_config.table_id)
            schema_data: TableSchema = table.schema()
            return schema_data
        except Exception as e:
            raise AirtableApiError(
                f"Failed to fetch schema for Airtable base '{base_key}', table '{table_key}' "
                f"(Table ID: {table_config.table_id}): {e}"
            ) from e

    def batch_update_records(
        self,
        context: AssetExecutionContext,
        base_key: str,
        table_key: str,
        records: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """
        Updates multiple records in a specific Airtable table using batch operations.
        
        Args:
            context: The Dagster execution context for logging.
            base_key: The logical key for the base.
            table_key: The logical key for the table.
            records: List of records to update. Each record must have 'id' field and update fields.
            
        Returns:
            Dictionary with counts: {'successful': int, 'failed': int}
            
        Raises:
            AirtableApiError: If the API request fails or configuration is invalid.
        """
        table = self.get_table(base_key, table_key)
        table_config = self._get_table_config(base_key, table_key)
        
        if not records:
            context.log.info("No records to update.")
            return {'successful': 0, 'failed': 0}
            
        successful_count = 0
        failed_count = 0
        
        # Process records in batches (Airtable API limit is 10 records per batch)
        batch_size = 10
        total_records = len(records)
        
        context.log.info(f"Starting batch update of {total_records} records in {base_key}.{table_key}")
        
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_records + batch_size - 1) // batch_size
            
            # Format records for pyairtable batch_update
            formatted_records = []
            batch_failed_count = 0
            
            for record in batch:
                if 'id' not in record:
                    context.log.error(f"Record missing 'id' field: {record}")
                    batch_failed_count += 1
                    continue
                    
                # Separate id from fields
                record_id = record['id']
                fields = {k: v for k, v in record.items() if k != 'id' and v is not None}
                
                if not fields:
                    context.log.warning(f"No fields to update for record {record_id}")
                    batch_failed_count += 1
                    continue
                    
                formatted_records.append({
                    'id': record_id,
                    'fields': fields
                })
            
            if not formatted_records:
                context.log.warning(f"Batch {batch_num}/{total_batches} skipped: no valid records")
                failed_count += len(batch)
                continue
                
            try:
                context.log.debug(f"Processing batch {batch_num}/{total_batches} ({len(formatted_records)} records)")
                
                # Perform batch update
                updated_records = table.batch_update(formatted_records)
                batch_successful_count = len(updated_records)
                successful_count += batch_successful_count
                failed_count += batch_failed_count  # Add any pre-validation failures
                
                context.log.info(f"Batch {batch_num}/{total_batches} completed: {batch_successful_count} successful, {batch_failed_count} failed")
                    
            except Exception as e:
                context.log.error(f"Batch {batch_num}/{total_batches} failed: {e}")
                # All records in this batch failed
                failed_count += len(batch)
                
        context.log.info(f"Batch update completed. Successful: {successful_count}, Failed: {failed_count}")
        
        return {'successful': successful_count, 'failed': failed_count}

    # Optional: Add a helper to rename columns later if needed
    # def rename_df_columns_to_ids(self, df: pl.DataFrame, base_key: str, table_key: str) -> pl.DataFrame: ...