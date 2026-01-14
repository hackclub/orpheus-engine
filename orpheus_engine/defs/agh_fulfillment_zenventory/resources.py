"""
Zenventory API Resource

Provides a Dagster resource for interacting with the Zenventory REST API.
Used for syncing AGH Fulfillment warehouse data.
"""

import time
from typing import Any, ClassVar, Dict, Generator, List, Optional

import requests
from dagster import ConfigurableResource, EnvVar, get_dagster_logger
from requests.auth import HTTPBasicAuth


class ZenventoryApiError(Exception):
    """Custom exception for errors interacting with the Zenventory API."""
    pass


class ZenventoryRateLimitError(ZenventoryApiError):
    """Exception raised when Zenventory API returns a rate limit (429) response."""
    def __init__(self, message: str, retry_after: int = 60):
        super().__init__(message)
        self.retry_after = retry_after


class ZenventoryResource(ConfigurableResource):
    """
    A Dagster resource for interacting with the Zenventory REST API.

    Uses HTTP Basic Auth with API Key and API Secret.
    API Reference: https://docs.zenventory.com/
    """

    api_key: str = EnvVar("ZENVENTORY_API_KEY")
    api_secret: str = EnvVar("ZENVENTORY_API_SECRET")

    BASE_URL: ClassVar[str] = "https://app.zenventory.com/rest"
    DEFAULT_PER_PAGE: ClassVar[int] = 100

    def _get_auth(self) -> HTTPBasicAuth:
        """Get HTTP Basic Auth object."""
        return HTTPBasicAuth(self.api_key, self.api_secret)

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Make a GET request to the Zenventory API.

        Args:
            endpoint: API endpoint (e.g., "/items", "/customer-orders")
            params: Query parameters
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries on failure

        Returns:
            Parsed JSON response dict

        Raises:
            ZenventoryApiError: If the request fails after retries
        """
        url = f"{self.BASE_URL}{endpoint}"
        log = get_dagster_logger()

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    auth=self._get_auth(),
                    params=params,
                    timeout=timeout,
                )

                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    if attempt < max_retries - 1:
                        log.warning(f"Rate limited, sleeping {retry_after}s...")
                        time.sleep(retry_after)
                        continue
                    raise ZenventoryRateLimitError(
                        f"Rate limited on {endpoint}",
                        retry_after=retry_after
                    )

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    log.warning(f"Request timeout on {endpoint}, retrying...")
                    time.sleep(5)
                    continue
                raise ZenventoryApiError(f"Request to {endpoint} timed out after {max_retries} attempts")
            except ZenventoryApiError:
                raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    log.warning(f"Request error on {endpoint}: {e}, retrying...")
                    time.sleep(5)
                    continue
                raise ZenventoryApiError(f"Request to {endpoint} failed: {e}") from e

        raise ZenventoryApiError(f"Request to {endpoint} failed after {max_retries} attempts")

    def _paginate(
        self,
        endpoint: str,
        data_key: str,
        per_page: int = DEFAULT_PER_PAGE,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Paginate through all results from an endpoint.

        Args:
            endpoint: API endpoint
            data_key: Key in response containing the data array
            per_page: Number of records per page
            extra_params: Additional query parameters

        Yields:
            Lists of records (one list per page)
        """
        log = get_dagster_logger()
        page = 1

        while True:
            params = {"page": page, "limit": per_page}
            if extra_params:
                params.update(extra_params)

            data = self._make_request(endpoint, params=params)
            meta = data.get("meta", {})
            records = data.get(data_key, [])

            if records:
                yield records

            total_pages = meta.get("totalPages", 1)
            log.info(f"Fetched page {page}/{total_pages} from {endpoint} ({len(records)} records)")

            if page >= total_pages:
                break

            page += 1

    def get_all_items(self) -> List[Dict[str, Any]]:
        """
        Fetch all items (products/SKUs) from Zenventory.

        Returns:
            List of item records with fields like:
            - id, sku, upc, description, category
            - client (dict with id, name)
            - baseUom, unitCost, weight
            - active, kit, assembly, perishable
            - createdDate, modifiedDate
        """
        all_items = []
        for batch in self._paginate("/items", "items"):
            all_items.extend(batch)
        return all_items

    def get_all_inventory(self) -> List[Dict[str, Any]]:
        """
        Fetch all inventory levels from Zenventory.

        Returns:
            List of inventory records with fields like:
            - item (dict with id, sku, upc, description)
            - client (dict with id, name)
            - inStock, lotNumber, expirationDate
        """
        all_inventory = []
        for batch in self._paginate("/inventory/items", "inventory"):
            all_inventory.extend(batch)
        return all_inventory

    def get_all_customer_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all customer orders from Zenventory.

        Returns:
            List of customer order records with fields like:
            - id, orderNumber, orderReference
            - customer (dict), client (dict)
            - orderedDate, createdDate, modifiedDate
            - completed, completedDate, cancelled, cancelledDate
            - shippingAddress (dict), billingAddress (dict)
            - items (list of order items)
        """
        all_orders = []
        for batch in self._paginate("/customer-orders", "customerOrders"):
            all_orders.extend(batch)
        return all_orders

    def get_customer_orders_paginated(
        self,
        per_page: int = DEFAULT_PER_PAGE,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Fetch customer orders in paginated batches.

        Useful for processing large numbers of orders incrementally.

        Yields:
            Lists of customer order records
        """
        yield from self._paginate("/customer-orders", "customerOrders", per_page=per_page)

    def get_all_purchase_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all purchase orders from Zenventory.

        Returns:
            List of purchase order records with fields like:
            - id, orderNumber
            - supplier (dict), warehouse (dict), client (dict)
            - draft, completed, deleted
            - createdDate, preparedDate, requiredByDate, completedDate
            - items (list of PO items)
        """
        all_orders = []
        for batch in self._paginate("/purchase-orders", "purchaseOrders"):
            all_orders.extend(batch)
        return all_orders

    def get_customer_order_by_id(self, order_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single customer order by ID.

        Args:
            order_id: The order ID to fetch

        Returns:
            The order record, or None if not found
        """
        try:
            data = self._make_request(f"/customer-orders/{order_id}")
            return data if data.get("id") else None
        except ZenventoryApiError:
            return None

    def get_customer_orders_from_page(
        self,
        start_page: int,
        per_page: int = DEFAULT_PER_PAGE,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Fetch customer orders starting from a specific page.

        Useful for incremental sync where we only need new orders
        (orders with higher IDs appear on later pages).

        Args:
            start_page: Page number to start from (1-indexed)
            per_page: Number of records per page

        Yields:
            Lists of customer order records
        """
        log = get_dagster_logger()
        page = start_page

        while True:
            params = {"page": page, "limit": per_page}
            data = self._make_request("/customer-orders", params=params)
            meta = data.get("meta", {})
            records = data.get("customerOrders", [])

            if records:
                yield records

            total_pages = meta.get("totalPages", 1)
            log.info(f"Fetched page {page}/{total_pages} ({len(records)} records)")

            if page >= total_pages:
                break

            page += 1

    def get_total_customer_order_pages(self, per_page: int = DEFAULT_PER_PAGE) -> int:
        """
        Get the total number of customer order pages.

        Args:
            per_page: Number of records per page

        Returns:
            Total number of pages
        """
        data = self._make_request("/customer-orders", params={"page": 1, "limit": per_page})
        return data.get("meta", {}).get("totalPages", 1)

    def get_customer_order_stats(self, per_page: int = DEFAULT_PER_PAGE) -> tuple:
        """
        Get customer order pagination stats from the API.

        Returns:
            Tuple of (total_count, total_pages, per_page)
        """
        data = self._make_request("/customer-orders", params={"page": 1, "limit": per_page})
        meta = data.get("meta", {})
        return (
            meta.get("count", 0),
            meta.get("totalPages", 1),
            meta.get("perPage", 20),
        )

    def get_customer_order_page(self, page: int, per_page: int = DEFAULT_PER_PAGE) -> List[Dict[str, Any]]:
        """
        Fetch a single page of customer orders.

        Args:
            page: Page number (1-indexed)
            per_page: Number of records per page

        Returns:
            List of customer order records on that page
        """
        data = self._make_request("/customer-orders", params={"page": page, "limit": per_page})
        return data.get("customerOrders", [])
