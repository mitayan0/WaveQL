"""
Shopify Adapter - Orders, Products, Customers support.

Features:
- Partial predicate pushdown to Shopify REST Admin API
- Support for Orders, Products, Customers, and Inventory
- Private App and Custom App authentication support
- Automatic pagination via Link headers
- Async CRUD support
"""

from __future__ import annotations
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
import pyarrow as pa
try:
    import anyio
except ImportError:
    anyio = None
try:
    import httpx
except ImportError:
    httpx = None

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate

logger = logging.getLogger(__name__)


class ShopifyAdapter(BaseAdapter):
    """
    Shopify adapter for querying the Admin REST API.
    """
    
    adapter_name = "shopify"
    supports_predicate_pushdown = True
    supports_aggregation = True  # Client-side aggregation support
    supports_insert = True
    supports_update = True
    supports_delete = True
    supports_batch = True
    
    API_VERSION = "2024-01"
    
    # Mapping of tables to API endpoints
    OBJECT_MAP = {
        "orders": "orders",
        "order": "orders",
        "products": "products",
        "product": "products",
        "customers": "customers",
        "customer": "customers",
        "collections": "custom_collections",
        "inventory": "inventory_items",
    }

    def __init__(
        self,
        host: str,  # shop-name.myshopify.com
        auth_manager=None,
        schema_cache=None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        self._shop_url = host if "myshopify.com" in host else f"{host}.myshopify.com"
        self._access_token = kwargs.get("api_key") or kwargs.get("oauth_token")
        self._api_version = kwargs.get("api_version", self.API_VERSION)
        self._config = kwargs
    
    def _get_resource(self, table: str) -> str:
        return self.OBJECT_MAP.get(table.lower(), table.lower())

    async def fetch_async(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        """Fetch data from Shopify REST API (async)."""
        resource = self._get_resource(table)
        
        # Smart COUNT optimization: Use dedicated /count.json endpoint for simple COUNT(*)
        if self._is_simple_count(aggregates, group_by):
            return await self._fetch_count_only(resource, predicates, aggregates)
        
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}.json"
        
        # Build query parameters for pushdown
        params = {}
        if limit:
            params["limit"] = min(250, limit) # Shopify max page size
        
        # Shopify REST filtering is limited. We only push down common ones.
        if predicates:
            for pred in predicates:
                if pred.operator == "=":
                    if pred.column == "status":
                        params["status"] = pred.value
                    elif pred.column == "ids":
                        params["ids"] = pred.value
                    elif pred.column == "vendor":
                        params["vendor"] = pred.value
                elif pred.operator == ">=":
                    if pred.column == "updated_at":
                        params["updated_at_min"] = pred.value
                    elif pred.column == "created_at":
                        params["created_at_min"] = pred.value
        
        results = []
        next_url = url
        
        try:
            while next_url:
                response = await self._request_async("GET", next_url, params=params if next_url == url else None)
                data = response.json()
                
                batch = data.get(resource, [])
                results.extend(batch)
                
                if limit and len(results) >= limit:
                    results = results[:limit]
                    break
                
                # Shopify uses Link headers for pagination
                next_url = self._get_next_page_url(response.headers.get("Link"))
                
            if not results:
                return pa.table({})
            
            # Shopify returns nested objects, we flatten them slightly
            result_table = pa.Table.from_pylist(results)
            
            # Apply client-side aggregation if requested
            if aggregates:
                result_table = self._compute_client_side_aggregates(result_table, group_by, aggregates)
            
            return result_table
            
        except Exception as e:
            raise AdapterError(f"Shopify fetch failed: {e}")
    
    def _is_simple_count(self, aggregates: List[Any], group_by: List[str]) -> bool:
        """Check if this is a simple COUNT(*) query without GROUP BY."""
        if not aggregates or group_by:
            return False
        if len(aggregates) != 1:
            return False
        agg = aggregates[0]
        return agg.func.upper() == "COUNT" and (agg.column == "*" or agg.column is None)
    
    async def _fetch_count_only(
        self,
        resource: str,
        predicates: List["Predicate"],
        aggregates: List[Any],
    ) -> pa.Table:
        """
        Optimized COUNT(*) using Shopify's dedicated /count.json endpoint.
        
        Shopify provides /admin/api/{version}/{resource}/count.json for efficient counting.
        """
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}/count.json"
        
        # Build filter params (same logic as fetch)
        params = {}
        if predicates:
            for pred in predicates:
                if pred.operator == "=":
                    if pred.column == "status":
                        params["status"] = pred.value
                elif pred.operator == ">=":
                    if pred.column == "updated_at":
                        params["updated_at_min"] = pred.value
                    elif pred.column == "created_at":
                        params["created_at_min"] = pred.value
        
        try:
            response = await self._request_async("GET", url, params=params if params else None)
            data = response.json()
            
            total = data.get("count", 0)
            
            # Build result with proper alias
            agg = aggregates[0]
            alias = agg.alias or f"COUNT({agg.column})"
            
            logger.debug(f"Shopify COUNT optimization: returned {total} using /count.json endpoint")
            
            return pa.table({alias: [total]})
            
        except Exception as e:
            raise AdapterError(f"Shopify count failed: {e}")

    def fetch(self, *args, **kwargs) -> pa.Table:
        """
        Synchronous fetch using sync httpx directly.
        
        Note: Uses sync httpx here instead of anyio.run() because
        async DNS resolution fails on some Windows configurations.
        """
        return self._fetch_sync(*args, **kwargs)
    
    def _fetch_sync(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        """Synchronous fetch using sync httpx."""
        import httpx
        import time
        
        resource = self._get_resource(table)
        
        # For simple COUNT optimization, still use async path
        if self._is_simple_count(aggregates, group_by):
            return anyio.run(lambda: self._fetch_count_only(resource, predicates, aggregates))
        
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}.json"
        
        # Build query parameters for pushdown
        params = {}
        if limit:
            params["limit"] = min(250, limit)  # Shopify max page size
        
        # Apply predicate pushdown where supported
        if predicates:
            for pred in predicates:
                if pred.operator == "=":
                    if pred.column == "status":
                        params["status"] = pred.value
                    elif pred.column == "ids":
                        params["ids"] = pred.value
                    elif pred.column == "vendor":
                        params["vendor"] = pred.value
                elif pred.operator == ">=":
                    if pred.column == "updated_at":
                        params["updated_at_min"] = pred.value
                    elif pred.column == "created_at":
                        params["created_at_min"] = pred.value
        
        headers = {}
        if self._access_token:
            headers["X-Shopify-Access-Token"] = self._access_token
        
        results = []
        max_retries = 5
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                next_url = url
                with httpx.Client(timeout=30.0) as client:
                    while next_url:
                        response = client.get(next_url, params=params if next_url == url else None, headers=headers)
                        if response.status_code >= 400:
                            raise AdapterError(f"Shopify API error ({response.status_code}): {response.text}")
                        
                        data = response.json()
                        batch = data.get(resource, [])
                        results.extend(batch)
                        
                        if limit and len(results) >= limit:
                            results = results[:limit]
                            break
                        
                        # Shopify uses Link headers for pagination
                        next_url = self._get_next_page_url(response.headers.get("Link"))
                
                result_table = pa.Table.from_pylist(results) if results else pa.table({})
                
                # Apply client-side aggregation if requested
                if aggregates and len(result_table) > 0:
                    result_table = self._compute_client_side_aggregates(result_table, group_by, aggregates)
                
                return result_table
                
            except httpx.ConnectError as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Shopify request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                    results = []
                else:
                    raise AdapterError(f"Shopify fetch failed after {max_retries} attempts: {e}")
            except Exception as e:
                if isinstance(e, AdapterError):
                    raise
                raise AdapterError(f"Shopify fetch failed: {e}")

    def _get_next_page_url(self, link_header: str) -> Optional[str]:
        """Extract 'next' URL from Link header."""
        if not link_header:
            return None
        
        links = link_header.split(",")
        for link in links:
             if 'rel="next"' in link:
                 match = re.search(r'<(.*)>', link)
                 if match:
                     return match.group(1)
        return None

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Synchronous get_schema (runs async)."""
        return anyio.run(lambda: self.get_schema_async(table))

    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Inferred schema from first record (Shopify has no property API like HubSpot)."""
        # We'll fetch 1 record to see the structure
        resource = self._get_resource(table)
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}.json"
        
        try:
            response = await self._request_async("GET", url, params={"limit": 1})
            data = response.json()
            results = data.get(resource, [])
            
            if not results:
                return []
            
            sample = results[0]
            columns = []
            for k, v in sample.items():
                data_type = "string"
                if isinstance(v, bool): data_type = "boolean"
                elif isinstance(v, int): data_type = "integer"
                elif isinstance(v, float): data_type = "double"
                elif isinstance(v, (dict, list)): data_type = "struct"
                
                columns.append(ColumnInfo(name=k, data_type=data_type))
            return columns
        except Exception:
            return []

    async def insert_async(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        resource = self._get_resource(table)
        # Singular form used in POST body
        singular = resource[:-1] if resource.endswith("s") else resource
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}.json"
        
        payload = {singular: values}
        try:
            await self._request_async("POST", url, json=payload)
            return 1
        except Exception as e:
            raise QueryError(f"Shopify insert failed: {e}")

    def insert(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        """Insert a record into Shopify (sync)."""
        return anyio.run(lambda: self.insert_async(table, values, parameters))

    def update(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Update records in Shopify (sync)."""
        return anyio.run(lambda: self.update_async(table, values, predicates, parameters))

    def delete(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Delete records in Shopify (sync)."""
        return anyio.run(lambda: self.delete_async(table, predicates, parameters))

    async def _request_async(self, method: str, url: str, **kwargs) -> Any:
        """Make an HTTP request with retry logic for transient failures."""
        import httpx
        import asyncio
        
        headers = kwargs.get("headers", {})
        if self._access_token:
            headers["X-Shopify-Access-Token"] = self._access_token
        kwargs["headers"] = headers
        
        max_retries = 5
        base_delay = 1.0  # seconds - longer delay for DNS stability
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.request(method, url, **kwargs)
                    if response.status_code >= 400:
                        raise AdapterError(f"Shopify API error ({response.status_code}): {response.text}")
                    return response
            except httpx.ConnectError as e:
                # Retry on connection/DNS errors
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Shopify request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    raise
            except httpx.TimeoutException as e:
                # Retry on timeout
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Shopify request timed out (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    raise AdapterError(f"Shopify request timed out after {max_retries} attempts: {e}")
    
    async def update_async(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        resource = self._get_resource(table)
        singular = resource[:-1] if resource.endswith("s") else resource
        
        object_id = self._extract_id_from_predicates(predicates, "Shopify update")
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}/{object_id}.json"
        
        payload = {singular: values}
        try:
            await self._request_async("PUT", url, json=payload)
            return 1
        except Exception as e:
            raise QueryError(f"Shopify update failed: {e}")

    async def delete_async(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        resource = self._get_resource(table)
        object_id = self._extract_id_from_predicates(predicates, "Shopify delete")
        
        url = f"https://{self._shop_url}/admin/api/{self.API_VERSION}/{resource}/{object_id}.json"
        try:
            await self._request_async("DELETE", url)
            return 1
        except Exception as e:
            raise QueryError(f"Shopify delete failed: {e}")

    def list_tables(self) -> List[str]:
        return list(self.OBJECT_MAP.keys())
