import polars as pl
from dagster import ConfigurableResource, InitResourceContext, EnvVar, Field, AssetExecutionContext
from pyairtable import Api, Table
from pyairtable.formulas import match
from typing import List, Dict, Any, Optional

# Import the config models
from .config import AirtableServiceConfig, AirtableBaseConfig, AirtableTableConfig

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
        context: AssetExecutionContext, # Add context for logging
        base_key: str,
        table_key: str,
        fields: Optional[List[str]] = None, # Still accepts field *names* here
        formula: Optional[str] = None,
        # Add other pyairtable parameters as needed (e.g., view, max_records, sort)
    ) -> pl.DataFrame:
        """
        Fetches all records from a specific Airtable table (identified by logical keys)
        and returns them as a Polars DataFrame. Columns will be field *names*.

        Handles pagination automatically via pyairtable.

        Args:
            context: The Dagster execution context for logging.
            base_key: The logical key for the base (defined in resource config).
            table_key: The logical key for the table (defined in resource config).
            fields: Optional list of field *names* to retrieve. If None, retrieves all fields.
            formula: Optional Airtable formula string to filter records.

        Returns:
            A Polars DataFrame containing the records. Columns are field names.

        Raises:
            AirtableApiError: If the API request fails or configuration is invalid.
        """
        table = self.get_table(base_key, table_key)
        table_config = self._get_table_config(base_key, table_key) # Get config for metadata

        try:
            formula_obj = match(formula) if formula else None
            all_records_raw: List[Dict[str, Any]] = table.all(
                fields=fields,
                formula=formula_obj
                # Add other params like view=table_config.view if defined
            )

            if not all_records_raw:
                # Attempt to get schema for empty dataframe structure if possible
                try:
                    schema_info_model = self.get_table_schema(base_key, table_key)
                    # Extract names from FieldSchema objects
                    col_names = ["id", "createdTime"] + [f.name for f in schema_info_model.fields]
                    # Use Utf8 as default guess, could refine later
                    empty_schema = {name: pl.Utf8 for name in col_names}
                    # Filter by requested fields if specified
                    if fields:
                         empty_schema = {k:v for k,v in empty_schema.items() if k in ["id", "createdTime"] + fields}

                    return pl.DataFrame(schema=empty_schema)
                except Exception as schema_err:
                    # Fallback if schema fetch fails
                    context.log.warning(f"Could not fetch schema to build empty DataFrame structure for {base_key}.{table_key}: {schema_err}")
                    # Return truly empty DF if schema fails
                    return pl.DataFrame(schema={"id": pl.Utf8, "createdTime": pl.Utf8}) # Ensure base cols exist

            processed_records = []
            for record in all_records_raw:
                flat_record = record.get("fields", {})
                flat_record["id"] = record.get("id")
                flat_record["createdTime"] = record.get("createdTime")
                processed_records.append(flat_record)

            df = pl.DataFrame(processed_records)

            # Ensure standard columns 'id' and 'createdTime' are present
            # They should always be returned by pyairtable's .all() structure, but double-check
            if "id" not in df.columns:
                 df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("id"))
            if "createdTime" not in df.columns:
                 df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("createdTime"))

            return df

        except Exception as e:
            # Catch pyairtable specific errors if possible, otherwise general exception
            raise AirtableApiError(
                f"Failed to fetch records from Airtable base '{base_key}', table '{table_key}' "
                f"(Table ID: {table_config.table_id}): {e}"
            ) from e

    def get_table_schema(self, base_key: str, table_key: str) -> Any: # Return type changed to Any, could be TableSchema
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
            # Use the client to get the base, then table, then schema
            table = self.client.table(base_config.base_id, table_config.table_id)
            # The schema() method on the Table object returns the schema
            return table.schema()
        except Exception as e:
            raise AirtableApiError(
                f"Failed to fetch schema for Airtable base '{base_key}', table '{table_key}' "
                f"(Table ID: {table_config.table_id}): {e}"
            ) from e

    # Optional: Add a helper to rename columns later if needed
    # def rename_df_columns_to_ids(self, df: pl.DataFrame, base_key: str, table_key: str) -> pl.DataFrame: ...