# orpheus-engine/orpheus_engine/defs/airtable/config.py
from dagster import Config  # Import dagster.Config
from pydantic import Field # Keep Field for descriptions etc.
from typing import Dict, Optional

class AirtableTableConfig(Config): # Inherit from dagster.Config
    """Configuration for a single Airtable table."""
    table_id: str = Field(..., description="The ID of the Airtable table (e.g., 'tblXXXXXXXXXXXXXX').")
    # Optional: Add other table-specific defaults if needed, like 'view'
    # view: Optional[str] = None

class AirtableBaseConfig(Config): # Inherit from dagster.Config
    """Configuration for a single Airtable base."""
    base_id: str = Field(..., description="The ID of the Airtable base (e.g., 'appXXXXXXXXXXXXXX').")
    tables: Dict[str, AirtableTableConfig] = Field(
        ...,
        description="Dictionary mapping logical table names (e.g., 'contacts', 'projects') to their configuration."
    )

class AirtableServiceConfig(Config): # Inherit from dagster.Config
    """Top-level configuration for the Airtable resource."""
    bases: Dict[str, AirtableBaseConfig] = Field(
        ...,
        description="Dictionary mapping logical base names (e.g., 'marketing_crm', 'operations') to their configuration."
    ) 