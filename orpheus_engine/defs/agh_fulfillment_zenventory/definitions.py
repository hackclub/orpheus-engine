"""
AGH Fulfillment Zenventory Assets

Syncs data from Zenventory (AGH Fulfillment warehouse) to the PostgreSQL warehouse.
Uses incremental upserts based on primary keys to efficiently handle updates.
"""

import io
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2 import sql
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

from .resources import ZenventoryResource


SCHEMA_NAME = "agh_fulfillment_zenventory"

# Table name constants
TABLE_ITEMS = "items"
TABLE_INVENTORY = "inventory"
TABLE_CUSTOMER_ORDERS = "customer_orders"
TABLE_PURCHASE_ORDERS = "purchase_orders"


def get_db_connection() -> "psycopg2.extensions.connection":
    """Get a connection to the warehouse database."""
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def ensure_schema_exists(conn) -> None:
    """Ensure the agh_fulfillment_zenventory schema exists."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME))
        )
    conn.commit()


def get_max_modified_date(conn, table_name: str, date_column: str = "modified_date") -> Optional[datetime]:
    """
    Get the maximum modified date from a table for incremental sync.

    Returns None if the table doesn't exist or is empty.
    """
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT MAX({}) FROM {}.{}").format(
                sql.Identifier(date_column),
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name),
            )
            cur.execute(query)
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return None


def get_open_order_ids(conn, table_name: str = TABLE_CUSTOMER_ORDERS) -> List[int]:
    """
    Get IDs of orders that are not completed and not cancelled.

    These are the only orders that could potentially be modified,
    so we need to re-check them on each sync.
    """
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT id FROM {}.{}
                WHERE completed = false AND cancelled = false
            """).format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name),
            )
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return []


def get_order_stats(conn, table_name: str = TABLE_CUSTOMER_ORDERS) -> Tuple[Optional[int], Optional[int], int]:
    """
    Get min_id, max_id, and total_count from the orders table.

    Returns (min_id, max_id, total_count) or (None, None, 0) if table is empty.
    """
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT MIN(id), MAX(id), COUNT(*) FROM {}.{}").format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name),
            )
            cur.execute(query)
            result = cur.fetchone()
            return result if result[2] > 0 else (None, None, 0)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return (None, None, 0)


def get_open_order_pages(conn, table_name: str = TABLE_CUSTOMER_ORDERS, *, per_page: int) -> List[int]:
    """
    Get the exact pages containing open orders.

    Calculates the actual position of each open order in the sorted table
    and returns the unique page numbers. This is more efficient than
    fetching a contiguous range when open orders are spread across
    non-adjacent pages.

    Returns a sorted list of page numbers (1-indexed).
    """
    # Guard against division by zero
    if per_page <= 0:
        raise ValueError("per_page must be a positive integer")

    try:
        with conn.cursor() as cur:
            # Using parameterized query for per_page to avoid SQL injection
            query = sql.SQL("""
                WITH positions AS (
                    SELECT id, ROW_NUMBER() OVER (ORDER BY id) as position
                    FROM {schema}.{table}
                )
                SELECT DISTINCT CEIL(p.position / %s::float)::int as page
                FROM positions p
                INNER JOIN {schema}.{table} c ON p.id = c.id
                WHERE c.completed = false AND c.cancelled = false
                ORDER BY page
            """).format(
                schema=sql.Identifier(SCHEMA_NAME),
                table=sql.Identifier(table_name),
            )
            cur.execute(query, (per_page,))
            return [row[0] for row in cur.fetchall()]
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return []


def parse_zenventory_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a Zenventory date string to datetime.

    Zenventory dates look like: "2024-02-08T19:43:22-05:00"
    """
    if not date_str:
        return None
    try:
        # Handle ISO format with timezone
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return None


def sanitize_column_name(name: str) -> str:
    """Sanitize column name for PostgreSQL."""
    # Convert camelCase to snake_case
    result = ""
    for i, char in enumerate(name):
        if char.isupper() and i > 0:
            result += "_"
        result += char.lower()
    # Replace any non-alphanumeric chars with underscore
    result = "".join(c if c.isalnum() or c == "_" else "_" for c in result)
    return result


def _needs_csv_quoting(value: str) -> bool:
    """Check if a string value needs to be quoted in CSV."""
    return (
        '"' in value or
        "," in value or
        "\n" in value or
        "\r" in value or
        "\t" in value or
        "\\" in value or
        value.startswith(" ") or
        value.endswith(" ")
    )


def _sanitize_string(value: str) -> str:
    """Sanitize a string for PostgreSQL COPY by removing null bytes."""
    return value.replace("\x00", "")


def records_to_csv(records: List[Dict[str, Any]], columns: List[str]) -> io.StringIO:
    """
    Convert records to CSV format for PostgreSQL COPY.

    Handles NULL values properly by using an unquoted NULL marker (empty field),
    while ensuring actual empty strings are quoted to distinguish them from NULLs.
    Also handles special characters that need quoting (tabs, backslashes, whitespace).
    """
    output = io.StringIO()

    for record in records:
        row_parts = []
        for col in columns:
            value = record.get(col)
            if value is None:
                # Empty unquoted field = NULL in PostgreSQL CSV COPY
                row_parts.append("")
            elif isinstance(value, (dict, list)):
                # JSON values need to be quoted and escaped
                json_str = json.dumps(value)
                # Escape quotes by doubling them and wrap in quotes
                escaped = json_str.replace('"', '""')
                row_parts.append(f'"{escaped}"')
            elif isinstance(value, bool):
                row_parts.append("t" if value else "f")
            elif isinstance(value, (int, float)):
                row_parts.append(str(value))
            elif isinstance(value, str):
                # Remove null bytes that would corrupt COPY
                value = _sanitize_string(value)
                if value == "":
                    # Empty string must be quoted to not be treated as NULL
                    row_parts.append('""')
                elif _needs_csv_quoting(value):
                    # Escape quotes and wrap in quotes
                    escaped = value.replace('"', '""')
                    row_parts.append(f'"{escaped}"')
                else:
                    row_parts.append(value)
            else:
                str_value = _sanitize_string(str(value))
                if _needs_csv_quoting(str_value):
                    escaped = str_value.replace('"', '""')
                    row_parts.append(f'"{escaped}"')
                else:
                    row_parts.append(str_value)
        output.write(",".join(row_parts) + "\n")

    output.seek(0)
    return output


# Schema definitions for tables
ITEMS_SCHEMA = {
    "id": "BIGINT",
    "sku": "TEXT",
    "upc": "TEXT",
    "description": "TEXT",
    "category": "TEXT",
    "client_id": "BIGINT",
    "client_name": "TEXT",
    "base_uom": "TEXT",
    "unit_cost": "DOUBLE PRECISION",
    "lead_time": "INTEGER",
    "default_econ_order": "INTEGER",
    "rrp": "DOUBLE PRECISION",
    "price": "DOUBLE PRECISION",
    "active": "BOOLEAN",
    "kit": "BOOLEAN",
    "assembly": "BOOLEAN",
    "perishable": "BOOLEAN",
    "track_lot": "BOOLEAN",
    "serialized": "BOOLEAN",
    "non_inventory": "BOOLEAN",
    "weight": "DOUBLE PRECISION",
    "storage_length": "DOUBLE PRECISION",
    "storage_width": "DOUBLE PRECISION",
    "storage_height": "DOUBLE PRECISION",
    "storage_volume": "DOUBLE PRECISION",
    "safety_stock": "INTEGER",
    "user_field1": "TEXT",
    "user_field2": "TEXT",
    "user_field3": "TEXT",
    "user_field4": "TEXT",
    "user_field5": "TEXT",
    "user_field6": "TEXT",
    "created_date": "TIMESTAMP WITH TIME ZONE",
    "modified_date": "TIMESTAMP WITH TIME ZONE",
    "notes": "TEXT",
    "additional_fields": "JSONB",
}

# Inventory uses composite primary key (item_id, lot_number) to handle multiple lots per item
INVENTORY_SCHEMA = {
    "item_id": "BIGINT",
    "lot_number": "TEXT",
    "item_sku": "TEXT",
    "item_upc": "TEXT",
    "item_description": "TEXT",
    "client_id": "BIGINT",
    "client_name": "TEXT",
    "in_stock": "INTEGER",
    "expiration_date": "TEXT",
}

CUSTOMER_ORDERS_SCHEMA = {
    "id": "BIGINT",
    "order_number": "TEXT",
    "order_reference": "TEXT",
    "customer_id": "BIGINT",
    "customer_title": "TEXT",
    "customer_name": "TEXT",
    "client_id": "BIGINT",
    "client_name": "TEXT",
    "ordered_date": "TIMESTAMP WITH TIME ZONE",
    "created_date": "TIMESTAMP WITH TIME ZONE",
    "modified_date": "TIMESTAMP WITH TIME ZONE",
    "order_placed": "BOOLEAN",
    "created_by_id": "BIGINT",
    "created_by_name": "TEXT",
    "completed": "BOOLEAN",
    "completed_date": "TIMESTAMP WITH TIME ZONE",
    "cancelled": "BOOLEAN",
    "cancelled_date": "TIMESTAMP WITH TIME ZONE",
    "cancelled_reason": "TEXT",
    "cancelled_by_id": "BIGINT",
    "cancelled_by_name": "TEXT",
    "on_hold": "BOOLEAN",
    "on_hold_until": "TEXT",
    "postage_account": "TEXT",
    "buyer_paid_shipping": "DOUBLE PRECISION",
    "shipping_address_id": "BIGINT",
    "shipping_address_company": "TEXT",
    "shipping_address_name": "TEXT",
    "shipping_address_line1": "TEXT",
    "shipping_address_line2": "TEXT",
    "shipping_address_city": "TEXT",
    "shipping_address_state": "TEXT",
    "shipping_address_postal_code": "TEXT",
    "shipping_address_country": "TEXT",
    "shipping_address_phone": "TEXT",
    "shipping_address_email": "TEXT",
    "billing_address_id": "BIGINT",
    "billing_address_company": "TEXT",
    "billing_address_name": "TEXT",
    "billing_address_line1": "TEXT",
    "billing_address_line2": "TEXT",
    "billing_address_city": "TEXT",
    "billing_address_state": "TEXT",
    "billing_address_postal_code": "TEXT",
    "billing_address_country": "TEXT",
    "ship_via": "TEXT",
    "ship_via_packaging": "TEXT",
    "ship_via_confirmation": "TEXT",
    "dry_ice_weight": "DOUBLE PRECISION",
    "package_sku": "TEXT",
    "ship_from_warehouse_id": "BIGINT",
    "ship_from_warehouse_name": "TEXT",
    "project_number": "TEXT",
    "discount_percentage": "DOUBLE PRECISION",
    "order_source": "TEXT",
    "internal_note": "TEXT",
    "note_from_customer": "TEXT",
    "note_to_customer": "TEXT",
    "notification_email": "TEXT",
    "user_field1": "TEXT",
    "user_field2": "TEXT",
    "user_field3": "TEXT",
    "my_list1": "TEXT",
    "my_list2": "TEXT",
    "tags": "JSONB",
    "items": "JSONB",
}

PURCHASE_ORDERS_SCHEMA = {
    "id": "BIGINT",
    "order_number": "TEXT",
    "supplier_id": "BIGINT",
    "supplier_name": "TEXT",
    "warehouse_id": "BIGINT",
    "warehouse_name": "TEXT",
    "client_id": "BIGINT",
    "client_name": "TEXT",
    "user_id": "BIGINT",
    "user_name": "TEXT",
    "draft": "BOOLEAN",
    "completed": "BOOLEAN",
    "deleted": "BOOLEAN",
    "created_date": "TIMESTAMP WITH TIME ZONE",
    "prepared_date": "TIMESTAMP WITH TIME ZONE",
    "required_by_date": "DATE",
    "completed_date": "TIMESTAMP WITH TIME ZONE",
    "project_number": "TEXT",
    "notes": "TEXT",
    "ship_method": "TEXT",
    "terms": "TEXT",
    "user_field1": "TEXT",
    "user_field2": "TEXT",
    "user_field3": "TEXT",
    "items": "JSONB",
}


def transform_item(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform an item record for database storage."""
    result = {}

    # Flatten client
    client = record.get("client", {}) or {}
    result["client_id"] = client.get("id")
    result["client_name"] = client.get("name")

    # Map other fields with proper naming
    field_mapping = {
        "id": "id",
        "sku": "sku",
        "upc": "upc",
        "description": "description",
        "category": "category",
        "baseUom": "base_uom",
        "unitCost": "unit_cost",
        "leadTime": "lead_time",
        "defaultEconOrder": "default_econ_order",
        "rrp": "rrp",
        "price": "price",
        "active": "active",
        "kit": "kit",
        "assembly": "assembly",
        "perishable": "perishable",
        "trackLot": "track_lot",
        "serialized": "serialized",
        "nonInventory": "non_inventory",
        "weight": "weight",
        "storageLength": "storage_length",
        "storageWidth": "storage_width",
        "storageHeight": "storage_height",
        "storageVolume": "storage_volume",
        "safetyStock": "safety_stock",
        "userField1": "user_field1",
        "userField2": "user_field2",
        "userField3": "user_field3",
        "userField4": "user_field4",
        "userField5": "user_field5",
        "userField6": "user_field6",
        "createdDate": "created_date",
        "modifiedDate": "modified_date",
        "notes": "notes",
        "additionalFields": "additional_fields",
    }

    for api_key, db_key in field_mapping.items():
        if api_key in record:
            result[db_key] = record[api_key]

    return result


def transform_inventory(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform an inventory record for database storage."""
    result = {}

    # Flatten item
    item = record.get("item", {}) or {}
    result["item_id"] = item.get("id")
    result["item_sku"] = item.get("sku")
    result["item_upc"] = item.get("upc")
    result["item_description"] = item.get("description")

    # Flatten client
    client = record.get("client", {}) or {}
    result["client_id"] = client.get("id")
    result["client_name"] = client.get("name")

    # Other fields
    result["in_stock"] = record.get("inStock")
    # Use empty string as default for lot_number to ensure composite key works
    result["lot_number"] = record.get("lotNumber") or ""
    result["expiration_date"] = record.get("expirationDate")

    return result


def transform_customer_order(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a customer order record for database storage."""
    result = {}

    # Flatten nested dicts
    customer = record.get("customer", {}) or {}
    result["customer_id"] = customer.get("id")
    result["customer_title"] = customer.get("title")
    result["customer_name"] = customer.get("name")

    client = record.get("client", {}) or {}
    result["client_id"] = client.get("id")
    result["client_name"] = client.get("name")

    created_by = record.get("createdBy", {}) or {}
    result["created_by_id"] = created_by.get("id")
    result["created_by_name"] = created_by.get("name")

    cancelled_by = record.get("cancelledBy", {}) or {}
    result["cancelled_by_id"] = cancelled_by.get("id")
    result["cancelled_by_name"] = cancelled_by.get("name")

    shipping_addr = record.get("shippingAddress", {}) or {}
    result["shipping_address_id"] = shipping_addr.get("id")
    result["shipping_address_company"] = shipping_addr.get("company")
    result["shipping_address_name"] = shipping_addr.get("name")
    result["shipping_address_line1"] = shipping_addr.get("line1")
    result["shipping_address_line2"] = shipping_addr.get("line2")
    result["shipping_address_city"] = shipping_addr.get("city")
    result["shipping_address_state"] = shipping_addr.get("state")
    result["shipping_address_postal_code"] = shipping_addr.get("postalCode")
    result["shipping_address_country"] = shipping_addr.get("country")
    result["shipping_address_phone"] = shipping_addr.get("phone")
    result["shipping_address_email"] = shipping_addr.get("email")

    billing_addr = record.get("billingAddress", {}) or {}
    result["billing_address_id"] = billing_addr.get("id")
    result["billing_address_company"] = billing_addr.get("company")
    result["billing_address_name"] = billing_addr.get("name")
    result["billing_address_line1"] = billing_addr.get("line1")
    result["billing_address_line2"] = billing_addr.get("line2")
    result["billing_address_city"] = billing_addr.get("city")
    result["billing_address_state"] = billing_addr.get("state")
    result["billing_address_postal_code"] = billing_addr.get("postalCode")
    result["billing_address_country"] = billing_addr.get("country")

    ship_from = record.get("shipFromWarehouse", {}) or {}
    result["ship_from_warehouse_id"] = ship_from.get("id")
    result["ship_from_warehouse_name"] = ship_from.get("name")

    # Map other fields
    field_mapping = {
        "id": "id",
        "orderNumber": "order_number",
        "orderReference": "order_reference",
        "orderedDate": "ordered_date",
        "createdDate": "created_date",
        "modifiedDate": "modified_date",
        "orderPlaced": "order_placed",
        "completed": "completed",
        "completedDate": "completed_date",
        "cancelled": "cancelled",
        "cancelledDate": "cancelled_date",
        "cancelledReason": "cancelled_reason",
        "onHold": "on_hold",
        "onHoldUntil": "on_hold_until",
        "postageAccount": "postage_account",
        "buyerPaidShipping": "buyer_paid_shipping",
        "shipVia": "ship_via",
        "shipViaPackaging": "ship_via_packaging",
        "shipViaConfirmation": "ship_via_confirmation",
        "dryIceWeight": "dry_ice_weight",
        "packageSku": "package_sku",
        "projectNumber": "project_number",
        "discountPercentage": "discount_percentage",
        "orderSource": "order_source",
        "internalNote": "internal_note",
        "noteFromCustomer": "note_from_customer",
        "noteToCustomer": "note_to_customer",
        "notificationEmail": "notification_email",
        "userField1": "user_field1",
        "userField2": "user_field2",
        "userField3": "user_field3",
        "myList1": "my_list1",
        "myList2": "my_list2",
        "tags": "tags",
        "items": "items",
    }

    for api_key, db_key in field_mapping.items():
        if api_key in record:
            value = record[api_key]
            # Handle empty date strings
            if db_key.endswith("_date") and value == "":
                value = None
            result[db_key] = value

    return result


def transform_purchase_order(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a purchase order record for database storage."""
    result = {}

    # Flatten nested dicts
    supplier = record.get("supplier", {}) or {}
    result["supplier_id"] = supplier.get("id")
    result["supplier_name"] = supplier.get("name")

    warehouse = record.get("warehouse", {}) or {}
    result["warehouse_id"] = warehouse.get("id")
    result["warehouse_name"] = warehouse.get("name")

    client = record.get("client", {}) or {}
    result["client_id"] = client.get("id")
    result["client_name"] = client.get("name")

    user = record.get("user", {}) or {}
    result["user_id"] = user.get("id")
    result["user_name"] = user.get("name")

    # Map other fields
    field_mapping = {
        "id": "id",
        "orderNumber": "order_number",
        "draft": "draft",
        "completed": "completed",
        "deleted": "deleted",
        "createdDate": "created_date",
        "preparedDate": "prepared_date",
        "requiredByDate": "required_by_date",
        "completedDate": "completed_date",
        "projectNumber": "project_number",
        "notes": "notes",
        "shipMethod": "ship_method",
        "terms": "terms",
        "userField1": "user_field1",
        "userField2": "user_field2",
        "userField3": "user_field3",
        "items": "items",
    }

    for api_key, db_key in field_mapping.items():
        if api_key in record:
            value = record[api_key]
            # Handle empty date strings
            if db_key.endswith("_date") and value == "":
                value = None
            result[db_key] = value

    return result


def create_table_with_schema(
    conn,
    table_name: str,
    schema: Dict[str, str],
    primary_key: Union[str, List[str]],
) -> None:
    """
    Create a table with the given schema if it doesn't exist.

    Args:
        conn: Database connection
        table_name: Name of the table to create
        schema: Dict mapping column names to PostgreSQL types
        primary_key: Single column name or list of column names for composite key
    """
    columns = []
    for col_name, col_type in schema.items():
        columns.append(
            sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
        )

    # Handle composite primary keys
    if isinstance(primary_key, list):
        pk_cols = sql.SQL(", ").join([sql.Identifier(pk) for pk in primary_key])
    else:
        pk_cols = sql.Identifier(primary_key)

    columns_sql = sql.SQL(",\n        ").join(columns)

    create_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        {columns},
        _synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY ({pk})
    )
    """).format(
        schema=sql.Identifier(SCHEMA_NAME),
        table=sql.Identifier(table_name),
        columns=columns_sql,
        pk=pk_cols,
    )

    with conn.cursor() as cur:
        cur.execute(create_query)
    conn.commit()


def upsert_records(
    conn,
    table_name: str,
    records: List[Dict[str, Any]],
    schema: Dict[str, str],
    primary_key: Union[str, List[str]],
) -> int:
    """
    Upsert records into a table using staging table approach.

    Args:
        conn: Database connection
        table_name: Target table name
        records: List of record dicts to upsert
        schema: Dict mapping column names to PostgreSQL types
        primary_key: Single column name or list of column names for composite key

    Returns:
        The number of records upserted.
    """
    if not records:
        return 0

    columns = list(schema.keys())
    pk_list = primary_key if isinstance(primary_key, list) else [primary_key]

    with conn.cursor() as cur:
        # Create temp staging table with proper identifiers
        staging_name = f"staging_{table_name}"
        staging_columns = sql.SQL(", ").join([
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ))
            for col, typ in schema.items()
        ])
        cur.execute(sql.SQL("""
            CREATE TEMP TABLE {} ({}) ON COMMIT DROP
        """).format(sql.Identifier(staging_name), staging_columns))

        # COPY to staging
        # Our CSV uses empty unquoted fields for NULL, empty quoted ("") for empty strings
        csv_data = records_to_csv(records, columns)
        columns_quoted = sql.SQL(", ").join([sql.Identifier(col) for col in columns])
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
            sql.Identifier(staging_name),
            columns_quoted,
        )
        cur.copy_expert(copy_sql.as_string(conn), csv_data)

        # Build update clause excluding primary key columns
        update_cols = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in columns if col not in pk_list
        ]
        update_cols.append(sql.SQL("_synced_at = NOW()"))
        update_clause = sql.SQL(", ").join(update_cols)

        # Build conflict target
        conflict_cols = sql.SQL(", ").join([sql.Identifier(pk) for pk in pk_list])

        upsert_query = sql.SQL("""
            INSERT INTO {schema}.{table} ({columns}, _synced_at)
            SELECT {columns}, NOW()
            FROM {staging}
            ON CONFLICT ({conflict}) DO UPDATE SET
                {update}
        """).format(
            schema=sql.Identifier(SCHEMA_NAME),
            table=sql.Identifier(table_name),
            columns=columns_quoted,
            staging=sql.Identifier(staging_name),
            conflict=conflict_cols,
            update=update_clause,
        )
        cur.execute(upsert_query)
        upserted = cur.rowcount

    conn.commit()
    return upserted


@asset(
    compute_kind="zenventory_api",
    group_name="agh_fulfillment_zenventory",
    description="Syncs items (products/SKUs) from Zenventory to agh_fulfillment_zenventory.items"
)
def agh_fulfillment_zenventory_items(
    context: AssetExecutionContext,
    zenventory: ZenventoryResource,
) -> Output[None]:
    """
    Fetch items from Zenventory and upsert to the warehouse.

    Uses incremental sync: only upserts records where modifiedDate > max(modified_date) in DB.
    """
    log = context.log

    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        create_table_with_schema(conn, TABLE_ITEMS, ITEMS_SCHEMA, "id")

        # Get max modified_date for incremental sync
        max_modified = get_max_modified_date(conn, TABLE_ITEMS)
        if max_modified:
            log.info(f"Incremental sync: only processing records modified after {max_modified}")
        else:
            log.info("Full sync: no existing data found")

        log.info("Fetching items from Zenventory...")
        raw_items = zenventory.get_all_items()
        log.info(f"Fetched {len(raw_items)} items")

        # Filter to only new/modified records
        if max_modified:
            new_items = []
            for item in raw_items:
                item_modified = parse_zenventory_date(item.get("modifiedDate"))
                if item_modified and item_modified > max_modified:
                    new_items.append(item)
            skipped = len(raw_items) - len(new_items)
            log.info(f"Incremental: {len(new_items)} new/modified, {skipped} unchanged")
            raw_items = new_items
        else:
            skipped = 0

        # Transform and upsert
        transformed = [transform_item(item) for item in raw_items]
        upserted = upsert_records(
            conn, TABLE_ITEMS, transformed, ITEMS_SCHEMA, "id"
        )
        log.info(f"Upserted {upserted} items")

        return Output(
            value=None,
            metadata={
                "total_fetched": MetadataValue.int(len(raw_items) + skipped),
                "total_skipped": MetadataValue.int(skipped),
                "total_upserted": MetadataValue.int(upserted),
                "incremental": MetadataValue.bool(max_modified is not None),
                "table": MetadataValue.text(f"{SCHEMA_NAME}.{TABLE_ITEMS}"),
            }
        )
    finally:
        conn.close()


@asset(
    compute_kind="zenventory_api",
    group_name="agh_fulfillment_zenventory",
    description="Syncs inventory levels from Zenventory to agh_fulfillment_zenventory.inventory"
)
def agh_fulfillment_zenventory_inventory(
    context: AssetExecutionContext,
    zenventory: ZenventoryResource,
) -> Output[None]:
    """
    Fetch all inventory levels from Zenventory and upsert to the warehouse.

    Uses composite primary key (item_id, lot_number) to properly handle
    items with multiple lot numbers.
    """
    log = context.log

    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        # Use composite primary key for inventory
        create_table_with_schema(conn, TABLE_INVENTORY, INVENTORY_SCHEMA, ["item_id", "lot_number"])

        log.info("Fetching inventory from Zenventory...")
        raw_inventory = zenventory.get_all_inventory()
        log.info(f"Fetched {len(raw_inventory)} inventory records")

        # Transform records
        transformed = [transform_inventory(inv) for inv in raw_inventory]

        # Upsert to database with composite key
        upserted = upsert_records(
            conn, TABLE_INVENTORY, transformed, INVENTORY_SCHEMA, ["item_id", "lot_number"]
        )
        log.info(f"Upserted {upserted} inventory records")

        return Output(
            value=None,
            metadata={
                "total_fetched": MetadataValue.int(len(raw_inventory)),
                "total_upserted": MetadataValue.int(upserted),
                "table": MetadataValue.text(f"{SCHEMA_NAME}.{TABLE_INVENTORY}"),
            }
        )
    finally:
        conn.close()


@asset(
    compute_kind="zenventory_api",
    group_name="agh_fulfillment_zenventory",
    description="Syncs customer orders from Zenventory to agh_fulfillment_zenventory.customer_orders"
)
def agh_fulfillment_zenventory_customer_orders(
    context: AssetExecutionContext,
    zenventory: ZenventoryResource,
) -> Output[None]:
    """
    Fetch customer orders from Zenventory and upsert to the warehouse.

    Uses an optimized incremental sync strategy based on the insight that
    completed and cancelled orders are NEVER modified. Only "open" orders
    (not completed, not cancelled) can change.

    Strategy:
    1. Calculate exact pages containing open orders using DB row positions
    2. Calculate pages needed for new orders based on API count vs DB count
    3. Fetch only those specific pages (typically ~70 pages vs 1000+ for full sync)
    4. Filter and upsert only new orders and open orders
    """
    log = context.log

    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        create_table_with_schema(conn, TABLE_CUSTOMER_ORDERS, CUSTOMER_ORDERS_SCHEMA, "id")

        # Get current state for incremental sync
        min_id, max_id, total_orders = get_order_stats(conn)
        open_order_ids = get_open_order_ids(conn) if max_id else []
        open_order_id_set = set(open_order_ids)

        if max_id:
            log.info(f"Incremental sync: {total_orders} orders, max_id={max_id}, {len(open_order_ids)} open orders")
        else:
            log.info("Full sync: no existing data found")

        total_api_calls = 0
        new_orders = []
        refreshed_orders = []
        pages_to_fetch = []

        # Helper for concurrent page fetching
        def fetch_page(page_num):
            return (page_num, zenventory.get_customer_order_page(page_num))

        if max_id and open_order_ids:
            # INCREMENTAL SYNC
            # Get stats from API (count, total_pages, per_page)
            api_count, total_pages, api_per_page = zenventory.get_customer_order_stats()
            total_api_calls += 1
            log.info(f"API has {api_count} orders across {total_pages} pages ({api_per_page}/page)")

            # Calculate exact pages containing open orders
            open_pages = set(get_open_order_pages(conn, per_page=api_per_page))

            # Calculate pages needed for new orders based on API count vs DB count
            new_order_count = max(0, api_count - total_orders)
            new_order_pages_needed = max(1, (new_order_count // api_per_page) + 2)  # +2 buffer
            new_order_pages = set(range(max(1, total_pages - new_order_pages_needed + 1), total_pages + 1))
            log.info(f"Estimated {new_order_count} new orders, fetching {len(new_order_pages)} pages for them")

            # Combine and sort
            pages_to_fetch = sorted(open_pages | new_order_pages)
            log.info(f"Fetching {len(pages_to_fetch)} specific pages ({len(open_pages)} for open orders, {len(new_order_pages)} for new)")

            # Fetch pages concurrently
            all_fetched_orders = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_page, p): p for p in pages_to_fetch}
                for future in as_completed(futures):
                    total_api_calls += 1
                    page_num, orders = future.result()
                    all_fetched_orders.extend(orders)

            log.info(f"Fetched {len(all_fetched_orders)} orders from {len(pages_to_fetch)} pages")

            # Filter to just new orders and open orders
            for order in all_fetched_orders:
                oid = order["id"]
                if oid > max_id:
                    new_orders.append(order)
                elif oid in open_order_id_set:
                    refreshed_orders.append(order)

            log.info(f"Found {len(new_orders)} new orders, {len(refreshed_orders)}/{len(open_order_ids)} open orders refreshed")

            # Warn if we didn't find all open orders (indicates DB/API sync drift)
            missing_count = len(open_order_ids) - len(refreshed_orders)
            if missing_count > 0:
                log.warning(f"Could not find {missing_count} open orders - DB and API may be out of sync")

            # Combine all orders to upsert
            all_orders = new_orders + refreshed_orders

        elif max_id:
            # Have data but no open orders - just check for new orders
            api_count, total_pages, api_per_page = zenventory.get_customer_order_stats()
            total_api_calls += 1

            # Calculate pages needed for new orders
            new_order_count = max(0, api_count - total_orders)
            new_order_pages_needed = max(1, (new_order_count // api_per_page) + 2)
            start_page = max(1, total_pages - new_order_pages_needed + 1)
            pages_to_fetch = list(range(start_page, total_pages + 1))
            log.info(f"No open orders. Estimated {new_order_count} new orders, fetching {len(pages_to_fetch)} pages")

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_page, p): p for p in pages_to_fetch}
                for future in as_completed(futures):
                    total_api_calls += 1
                    page_num, orders = future.result()
                    for order in orders:
                        if order["id"] > max_id:
                            new_orders.append(order)

            log.info(f"Found {len(new_orders)} new orders")
            all_orders = new_orders

        else:
            # FULL SYNC - fetch all orders
            log.info("Performing full sync...")
            total_synced = 0
            for batch in zenventory.get_customer_orders_paginated():
                total_api_calls += 1
                total_synced += len(batch)
                # For full sync, process in batches to avoid memory issues
                transformed = [transform_customer_order(order) for order in batch]
                upsert_records(
                    conn, TABLE_CUSTOMER_ORDERS, transformed, CUSTOMER_ORDERS_SCHEMA, "id"
                )
                if total_synced % 1000 == 0:
                    log.info(f"Progress: {total_synced} orders processed")

            log.info(f"Full sync complete: {total_synced} orders")
            return Output(
                value=None,
                metadata={
                    "sync_type": MetadataValue.text("full"),
                    "total_orders": MetadataValue.int(total_synced),
                    "api_calls": MetadataValue.int(total_api_calls),
                    "table": MetadataValue.text(f"{SCHEMA_NAME}.{TABLE_CUSTOMER_ORDERS}"),
                }
            )

        # Transform and upsert all orders (incremental sync)
        if all_orders:
            transformed = [transform_customer_order(order) for order in all_orders]
            upserted = upsert_records(
                conn, TABLE_CUSTOMER_ORDERS, transformed, CUSTOMER_ORDERS_SCHEMA, "id"
            )
            log.info(f"Upserted {upserted} orders ({len(new_orders)} new, {len(refreshed_orders)} refreshed)")
        else:
            upserted = 0
            log.info("No new or modified orders to upsert")

        return Output(
            value=None,
            metadata={
                "sync_type": MetadataValue.text("incremental"),
                "new_orders": MetadataValue.int(len(new_orders)),
                "open_orders_refreshed": MetadataValue.int(len(refreshed_orders)),
                "total_upserted": MetadataValue.int(upserted),
                "api_calls": MetadataValue.int(total_api_calls),
                "pages_fetched": MetadataValue.int(len(pages_to_fetch) if max_id else 0),
                "table": MetadataValue.text(f"{SCHEMA_NAME}.{TABLE_CUSTOMER_ORDERS}"),
            }
        )
    finally:
        conn.close()


@asset(
    compute_kind="zenventory_api",
    group_name="agh_fulfillment_zenventory",
    description="Syncs purchase orders from Zenventory to agh_fulfillment_zenventory.purchase_orders"
)
def agh_fulfillment_zenventory_purchase_orders(
    context: AssetExecutionContext,
    zenventory: ZenventoryResource,
) -> Output[None]:
    """
    Fetch all purchase orders from Zenventory and upsert to the warehouse.
    """
    log = context.log

    conn = get_db_connection()
    try:
        ensure_schema_exists(conn)
        create_table_with_schema(conn, TABLE_PURCHASE_ORDERS, PURCHASE_ORDERS_SCHEMA, "id")

        log.info("Fetching purchase orders from Zenventory...")
        raw_orders = zenventory.get_all_purchase_orders()
        log.info(f"Fetched {len(raw_orders)} purchase orders")

        # Transform records
        transformed = [transform_purchase_order(order) for order in raw_orders]

        # Upsert to database
        upserted = upsert_records(
            conn, TABLE_PURCHASE_ORDERS, transformed, PURCHASE_ORDERS_SCHEMA, "id"
        )
        log.info(f"Upserted {upserted} purchase orders")

        return Output(
            value=None,
            metadata={
                "total_fetched": MetadataValue.int(len(raw_orders)),
                "total_upserted": MetadataValue.int(upserted),
                "table": MetadataValue.text(f"{SCHEMA_NAME}.{TABLE_PURCHASE_ORDERS}"),
            }
        )
    finally:
        conn.close()


# Create the resource instance
zenventory_resource = ZenventoryResource()

defs = Definitions(
    assets=[
        agh_fulfillment_zenventory_items,
        agh_fulfillment_zenventory_inventory,
        agh_fulfillment_zenventory_customer_orders,
        agh_fulfillment_zenventory_purchase_orders,
    ],
    resources={
        "zenventory": zenventory_resource,
    },
)
