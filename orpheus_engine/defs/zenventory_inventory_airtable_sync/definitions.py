"""
Zenventory Inventory -> Airtable Sync

Reads items and inventory from the PostgreSQL warehouse (written by the
agh_fulfillment_zenventory assets), joins them to compute total in-stock
quantities per SKU, and upserts to Airtable using SKU as the match key.

Also syncs item images from Zenventory's web app (images are publicly
accessible) to populate the Airtable Picture field.

Target: https://airtable.com/appK53aN0fz3sgJ4w/tblvSJMqoXnQyN7co
"""

import os
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import psycopg2
import requests
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)
from pyairtable import Api

from orpheus_engine.defs.agh_fulfillment_zenventory.definitions import (
    agh_fulfillment_labor_costs,
    agh_fulfillment_zenventory_customer_orders,
    agh_fulfillment_zenventory_inventory,
    agh_fulfillment_zenventory_items,
    agh_fulfillment_zenventory_purchase_orders,
    agh_fulfillment_zenventory_shipments,
)

AIRTABLE_BASE_ID = "appK53aN0fz3sgJ4w"
AIRTABLE_TABLE_ID = "tblvSJMqoXnQyN7co"
ZENVENTORY_BASE_URL = "https://app.zenventory.com"

INVENTORY_QUERY = """
    SELECT
        i.id,
        i.sku,
        i.description,
        i.unit_cost,
        COALESCE(SUM(inv.in_stock), 0) AS in_stock
    FROM agh_fulfillment_zenventory.items i
    LEFT JOIN agh_fulfillment_zenventory.inventory inv ON i.id = inv.item_id
    WHERE i.sku IS NOT NULL AND i.sku != ''
    GROUP BY i.id, i.sku, i.description, i.unit_cost
"""

INBOUND_QUERY = """
    SELECT
        po_item->>'sku' AS sku,
        SUM((po_item->>'quantity')::int) AS inbound
    FROM agh_fulfillment_zenventory.purchase_orders po,
         jsonb_array_elements(po.items) AS po_item
    WHERE po.completed = false AND po.deleted = false
    GROUP BY po_item->>'sku'
"""

SHIPMENT_STATS_QUERY = """
    WITH order_skus AS (
        SELECT
            co.order_number,
            item->>'sku' AS sku
        FROM agh_fulfillment_zenventory.customer_orders co,
             jsonb_array_elements(co.items) AS item
        WHERE co.completed = true
    ),
    order_costs AS (
        SELECT
            s.order_number,
            s.country,
            s.shipping_handling,
            lc.labor_cost
        FROM agh_fulfillment_zenventory.shipments s
        LEFT JOIN agh_fulfillment_zenventory.labor_costs lc
            ON s.order_number = lc.order_number
        WHERE s.shipping_handling IS NOT NULL
    )
    SELECT
        os.sku,
        COUNT(*) FILTER (WHERE oc.country != 'US') AS global_shipments,
        COUNT(*) FILTER (WHERE oc.country = 'US') AS usa_shipments,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY COALESCE(oc.shipping_handling, 0) + COALESCE(oc.labor_cost, 0)
        ) FILTER (WHERE oc.country = 'US') AS median_usa_postage_labor,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY COALESCE(oc.shipping_handling, 0) + COALESCE(oc.labor_cost, 0)
        ) FILTER (WHERE oc.country != 'US') AS median_global_postage_labor
    FROM order_skus os
    JOIN order_costs oc ON os.order_number = oc.order_number
    GROUP BY os.sku
"""


def get_zenventory_web_session() -> requests.Session:
    """Log into the Zenventory web app and return an authenticated session."""
    username = os.getenv("ZENVENTORY_WEB_USERNAME")
    password = os.getenv("ZENVENTORY_WEB_PASSWORD")
    if not username or not password:
        raise ValueError("ZENVENTORY_WEB_USERNAME and ZENVENTORY_WEB_PASSWORD must be set")

    s = requests.Session()
    s.get(f"{ZENVENTORY_BASE_URL}/", timeout=10)
    r = s.put(
        f"{ZENVENTORY_BASE_URL}/api/auth/login",
        json={
            "username": username,
            "password": password,
            "supportLogIn": False,
            "refresh": False,
        },
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
        },
        timeout=10,
    )
    if r.status_code != 200 or not r.json().get("success"):
        raise ValueError(f"Zenventory web login failed: {r.status_code} {r.text[:200]}")
    return s


def fetch_item_image_url(
    session: requests.Session, item_id: int
) -> Optional[str]:
    """Fetch the default image URL for a Zenventory item via the web app API."""
    xsrf = session.cookies.get("XSRF-TOKEN", "")
    r = session.get(
        f"{ZENVENTORY_BASE_URL}/api/admin-items/edititem/{item_id}/images",
        headers={"Accept": "application/json", "X-XSRF-TOKEN": xsrf},
        timeout=10,
    )
    if r.status_code != 200:
        return None

    body = r.json().get("body", {})
    default_image = body.get("defaultImage")
    image_folder = body.get("imageFolder")
    if not default_image or not image_folder:
        return None

    return f"{ZENVENTORY_BASE_URL}{image_folder}{default_image}"


def fetch_all_item_images(
    session: requests.Session, item_ids: list[int], log
) -> Dict[int, str]:
    """Fetch image URLs for all items. Returns {item_id: public_zenventory_url}."""
    image_urls = {}
    items_with_images = 0
    items_without_images = 0

    for i, item_id in enumerate(item_ids):
        url = fetch_item_image_url(session, item_id)
        if url:
            image_urls[item_id] = url
            items_with_images += 1
        else:
            items_without_images += 1

        if (i + 1) % 50 == 0:
            log.info(
                f"Image progress: {i + 1}/{len(item_ids)} "
                f"({items_with_images} with images, {items_without_images} without)"
            )
            time.sleep(0.5)
        else:
            time.sleep(0.1)

    log.info(
        f"Images complete: {items_with_images} with images, "
        f"{items_without_images} without"
    )
    return image_urls


@asset(
    compute_kind="zenventory_airtable_sync",
    group_name="zenventory_inventory_airtable_sync",
    deps=[
        agh_fulfillment_zenventory_items,
        agh_fulfillment_zenventory_inventory,
        agh_fulfillment_zenventory_customer_orders,
        agh_fulfillment_zenventory_purchase_orders,
        agh_fulfillment_zenventory_shipments,
        agh_fulfillment_labor_costs,
    ],
    description="Syncs Zenventory inventory counts and images to the Warehouse SKUs Airtable table",
)
def zenventory_inventory_airtable_sync(
    context: AssetExecutionContext,
) -> Output[None]:
    """
    Read items + inventory from the warehouse, fetch image URLs from
    Zenventory's web app, and upsert to the Warehouse SKUs Airtable table.

    Uses SKU as the upsert key. Updates: Name, SKU, In Stock, Unit Cost,
    Inbound, shipment stats, Picture, and Last Synced With Zenventory.
    """
    log = context.log

    # Read joined data from warehouse
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")

    conn = psycopg2.connect(conn_string)
    try:
        with conn.cursor() as cur:
            cur.execute(INVENTORY_QUERY)
            inventory_rows = cur.fetchall()

            cur.execute(INBOUND_QUERY)
            inbound_by_sku = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(SHIPMENT_STATS_QUERY)
            shipment_stats_by_sku = {
                row[0]: {
                    "global_shipments": row[1],
                    "usa_shipments": row[2],
                    "median_usa": row[3],
                    "median_global": row[4],
                }
                for row in cur.fetchall()
            }
    finally:
        conn.close()

    log.info(
        f"Read {len(inventory_rows)} items, "
        f"{len(inbound_by_sku)} inbound SKUs, "
        f"{len(shipment_stats_by_sku)} SKUs with shipment stats"
    )

    # Fetch image URLs from Zenventory web app
    item_ids = [row[0] for row in inventory_rows]
    log.info(f"Fetching images for {len(item_ids)} items...")
    zen_session = get_zenventory_web_session()
    image_urls = fetch_all_item_images(zen_session, item_ids, log)

    item_id_to_sku = {row[0]: row[1] for row in inventory_rows}
    image_by_sku = {
        item_id_to_sku[item_id]: url
        for item_id, url in image_urls.items()
        if item_id in item_id_to_sku
    }

    # Build Airtable records
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for item_id, sku, description, unit_cost, in_stock in inventory_rows:
        fields = {
            "Name (Must Match Poster Requests)": description or sku,
            "SKU": sku,
            "In Stock": in_stock,
            "Last Synced With Zenventory": now,
        }

        if unit_cost is not None and unit_cost > 0:
            fields["Unit Cost"] = round(float(unit_cost), 2)

        inbound = inbound_by_sku.get(sku)
        if inbound:
            fields["Inbound"] = inbound

        stats = shipment_stats_by_sku.get(sku)
        if stats:
            fields["USA Shipments"] = stats["usa_shipments"]
            fields["Global Shipments"] = stats["global_shipments"]
            if stats["median_usa"] is not None:
                fields["Median USA Postage + Labor"] = round(float(stats["median_usa"]), 2)
            if stats["median_global"] is not None:
                fields["Median Global Postage + Labor"] = round(float(stats["median_global"]), 2)

        zen_image_url = image_by_sku.get(sku)
        if zen_image_url:
            fields["Picture"] = [{"url": zen_image_url}]

        records.append({"fields": fields})

    log.info(f"Prepared {len(records)} records for Airtable sync ({len(image_by_sku)} with images)")

    # Sync to Airtable using batch_upsert with SKU as the key
    airtable_token = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    if not airtable_token:
        raise ValueError("AIRTABLE_PERSONAL_ACCESS_TOKEN environment variable is not set")

    api = Api(airtable_token)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

    BATCH_SIZE = 10
    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        table.batch_upsert(batch, key_fields=["SKU"])

        batch_num = (i // BATCH_SIZE) + 1
        log.info(f"Upserted batch {batch_num}/{total_batches} ({len(batch)} records)")

        if i + BATCH_SIZE < len(records):
            time.sleep(0.25)

    log.info(f"Airtable sync complete: {len(records)} records upserted")

    return Output(
        value=None,
        metadata={
            "total_upserted": MetadataValue.int(len(records)),
            "items_with_images": MetadataValue.int(len(image_by_sku)),
            "airtable_table": MetadataValue.text(
                f"https://airtable.com/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
            ),
        },
    )


defs = Definitions(
    assets=[zenventory_inventory_airtable_sync],
)
