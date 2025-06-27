"""
Shared utilities for address string construction and geocoding.
Ensures consistent address formatting across different data sources for maximum cache hits.
"""

def build_standard_address_string(
    address_line1: str, 
    address_line2: str = "", 
    city: str = "", 
    state_province: str = "", 
    zip_postal_code: str = "", 
    country: str = ""
) -> str:
    """
    Build standardized address string for geocoding.
    
    This function replicates the exact address string construction used in the 
    original JavaScript implementation to ensure cache hits across different systems.
    
    Format: "line1\nline2\ncity, state zip\ncountry"
    
    Args:
        address_line1: First line of address
        address_line2: Second line of address (optional)
        city: City name  
        state_province: State or province (optional)
        zip_postal_code: ZIP or postal code (optional)
        country: Country name (optional)
        
    Returns:
        Standardized address string, stripped of leading/trailing whitespace
    """
    # Ensure all inputs are strings and handle None values
    address_line1 = address_line1 or ""
    address_line2 = address_line2 or ""
    city = city or ""
    state_province = state_province or ""
    zip_postal_code = zip_postal_code or ""
    country = country or ""
    
    # Build line 3 *without* stripping here (matches JS logic exactly)
    line3 = f"{city}, {state_province} {zip_postal_code}"
    
    # Combine all parts with newlines
    address_string = f"{address_line1}\n{address_line2}\n{line3}\n{country}"
    
    # Apply strip() *only once* at the end, matching JS .trim()
    return address_string.strip()


def build_address_string_from_loops_row(row: dict) -> str:
    """
    Build address string from Loops data format.
    
    Args:
        row: Dictionary with Loops field names
        
    Returns:
        Standardized address string
    """
    return build_standard_address_string(
        address_line1=row.get("addressLine1", ""),
        address_line2=row.get("addressLine2", ""), 
        city=row.get("addressCity", ""),
        state_province=row.get("addressState", ""),
        zip_postal_code=row.get("addressZipCode", ""),  # Note: addressZipCode not addressZip
        country=row.get("addressCountry", "")
    )


def build_address_string_from_airtable_row(row: dict, field_ids: dict) -> str:
    """
    Build address string from Airtable approved_projects format.
    
    Args:
        row: Dictionary with Airtable field IDs as keys
        field_ids: Dictionary mapping semantic names to field IDs
        
    Returns:
        Standardized address string
    """
    return build_standard_address_string(
        address_line1=row.get(field_ids["address_line_1"], ""),
        address_line2=row.get(field_ids["address_line_2"], ""),
        city=row.get(field_ids["city"], ""),
        state_province=row.get(field_ids["state_province"], ""),
        zip_postal_code=row.get(field_ids["zip_postal_code"], ""),
        country=row.get(field_ids["country"], "")
    )
