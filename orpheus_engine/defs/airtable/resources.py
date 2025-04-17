import polars as pl
from dagster import ConfigurableResource, InitResourceContext, EnvVar
from pyairtable import Api
from pyairtable.formulas import match
from typing import List, Dict, Any, Optional

class AirtableApiError(Exception):
    """Custom exception for errors interacting with the Airtable API."""
    pass

class AirtableResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the Airtable API using pyairtable.

    Requires the AIRTABLE_PERSONAL_ACCESS_TOKEN environment variable to be set.
    API Reference: https://pyairtable.readthedocs.io/en/latest/api.html
    """

    api_key: str = EnvVar("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    _api_client: Optional[Api] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initializes the Airtable API client."""
        try:
            self._api_client = Api(self.api_key)
        except Exception as e:
            raise AirtableApiError(f"Failed to initialize Airtable client: {e}") from e

    @property
    def client(self) -> Api:
        """Provides access to the initialized pyairtable Api client."""
        if self._api_client is None:
            raise AirtableApiError("Airtable resource is not initialized. Ensure setup_for_execution has run.")
        return self._api_client

    def get_all_records_as_polars(
        self,
        base_id: str,
        table_id_or_name: str,
        fields: Optional[List[str]] = None,
        formula: Optional[str] = None,
        # Add other pyairtable parameters as needed (e.g., view, max_records, sort)
    ) -> pl.DataFrame:
        """
        Fetches all records from a specific Airtable table and returns them as a Polars DataFrame.

        Handles pagination automatically via pyairtable.

        Args:
            base_id: The ID of the Airtable base.
            table_id_or_name: The ID or name of the table within the base.
            fields: Optional list of field names to retrieve. If None, retrieves all fields.
            formula: Optional Airtable formula string to filter records.

        Returns:
            A Polars DataFrame containing the records from the table. Each record's 'id'
            and 'createdTime' are included, along with the contents of the 'fields' dictionary.

        Raises:
            AirtableApiError: If the API request fails.
        """
        try:
            table = self.client.table(base_id, table_id_or_name)
            # Construct formula if provided
            formula_obj = match(formula) if formula else None

            # Fetch all records (pyairtable handles pagination)
            all_records_raw: List[Dict[str, Any]] = table.all(
                fields=fields,
                formula=formula_obj
                # Add other parameters like view, max_records, sort here if needed
            )

            if not all_records_raw:
                # Return an empty DataFrame with placeholder columns if no records found
                # Adjust columns if you know the schema beforehand or retrieve it separately
                # For simplicity, we create a minimal empty frame.
                # If `fields` is provided, use those for schema, otherwise minimal.
                if fields:
                    schema = {field: pl.Utf8 for field in ["id", "createdTime"] + fields} # Guessing Utf8
                else:
                    schema = {"id": pl.Utf8, "createdTime": pl.Utf8} # Minimal schema
                return pl.DataFrame(schema=schema)


            # Process records: flatten the 'fields' dictionary and include 'id' and 'createdTime'
            processed_records = []
            for record in all_records_raw:
                # Start with the fields dictionary
                flat_record = record.get("fields", {})
                # Add the top-level id and createdTime
                flat_record["id"] = record.get("id")
                flat_record["createdTime"] = record.get("createdTime")
                processed_records.append(flat_record)

            # Convert to Polars DataFrame
            df = pl.DataFrame(processed_records)

            # Ensure standard columns 'id' and 'createdTime' are present, even if all records were missing them
            if "id" not in df.columns:
                 df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("id"))
            if "createdTime" not in df.columns:
                 df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("createdTime")) # Could be Datetime if parsed

            # Optional: Explicitly cast 'createdTime' to datetime if desired
            # try:
            #     df = df.with_columns(pl.col("createdTime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%fZ"))
            # except Exception:
            #     # Handle potential parsing errors if format varies
            #     pass

            return df

        except Exception as e:
            # Catch pyairtable specific errors if possible, otherwise general exception
            raise AirtableApiError(
                f"Failed to fetch records from Airtable base '{base_id}', table '{table_id_or_name}': {e}"
            ) from e