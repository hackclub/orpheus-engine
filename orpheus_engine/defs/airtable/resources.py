import polars as pl
from dagster import ConfigurableResource, InitResourceContext, EnvVar, Field, AssetExecutionContext
from pyairtable import Api, Table
from pyairtable.formulas import match
from typing import List, Dict, Any, Optional

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

            processed_records = []
            for record in all_records_raw:
                flat_record = record.get("fields", {}) # Keys are field IDs
                flat_record["id"] = record.get("id")
                processed_records.append(flat_record)

            df = pl.DataFrame(processed_records)

            # Ensure 'id' column exists and place it first
            if "id" in df.columns:
                other_cols = [col for col in df.columns if col != 'id']
                df = df.select(["id"] + other_cols)
            else:
                # Add 'id' as the first column if it was somehow missing
                context.log.warning(f"'id' column missing from raw records for {base_key}.{table_key}, adding empty column.")
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("id")).select(["id"] + df.columns)

            return df

        except Exception as e:
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

    # Optional: Add a helper to rename columns later if needed
    # def rename_df_columns_to_ids(self, df: pl.DataFrame, base_key: str, table_key: str) -> pl.DataFrame: ...