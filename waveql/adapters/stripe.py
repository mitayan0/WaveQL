"""
Stripe Adapter - Payments, Subscriptions, Customers support.

Features:
- Predicate pushdown to Stripe Search API
- Support for Charges, Customers, Invoices, Subscriptions, and Payouts
- Secret Key (Bearer token) authentication
- Cursor-based pagination (starting_after)
- Async CRUD support
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Sequence, TYPE_CHECKING
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


class StripeAdapter(BaseAdapter):
    """
    Stripe adapter for querying the v1 API.
    """
    
    adapter_name = "stripe"
    supports_predicate_pushdown = True
    supports_aggregation = True  # Client-side aggregation support
    supports_insert = True
    supports_update = True
    supports_delete = True
    supports_batch = True
    
    # Tables that support the Search API
    SEARCHABLE_RESOURCES = {"charges", "customers", "invoices", "subscriptions"}
    
    # Mapping of tables to Stripe endpoints
    RESOURCE_MAP = {
        "charges": "charges",
        "charge": "charges",
        "customers": "customers",
        "customer": "customers",
        "invoices": "invoices",
        "invoice": "invoices",
        "subscriptions": "subscriptions",
        "subscription": "subscriptions",
        "payouts": "payouts",
        "payout": "payouts",
        "balance_transactions": "balance_transactions",
        "balance": "balance_transactions",
    }

    def __init__(
        self,
        host: str = "api.stripe.com",
        auth_manager=None,
        schema_cache=None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        self._api_key = kwargs.get("api_key") or kwargs.get("password")
        self._api_version = kwargs.get("api_version")
        self._config = kwargs

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
        """Fetch data from Stripe API (Search or List)."""
        resource = self.RESOURCE_MAP.get(table.lower(), table.lower())
        
        # Smart COUNT optimization for simple COUNT(*) queries
        if self._is_simple_count(aggregates, group_by):
            return await self._fetch_count_only(resource, predicates, aggregates)
        
        # Determine if we can use the Search API
        use_search = resource in self.SEARCHABLE_RESOURCES and predicates
        
        if use_search:
            result_table = await self._fetch_via_search(resource, predicates, limit)
        else:
            result_table = await self._fetch_via_list(resource, predicates, limit)
        
        # Apply client-side aggregation if requested
        if aggregates and len(result_table) > 0:
            result_table = self._compute_client_side_aggregates(result_table, group_by, aggregates)
        
        return result_table
    
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
        Optimized COUNT(*) using Stripe's Search API total_count or minimal fetch.
        
        For searchable resources, Search API returns total_count.
        For others, we do a paginated count with minimal data.
        """
        if resource in self.SEARCHABLE_RESOURCES and predicates:
            # Use Search API's total_count
            query_parts = []
            for pred in predicates:
                if pred.operator == "=":
                    val = f"'{pred.value}'" if isinstance(pred.value, str) else pred.value
                    query_parts.append(f"{pred.column}:{val}")
            
            search_query = " AND ".join(query_parts) if query_parts else "*"
            url = f"https://{self._host}/v1/{resource}/search"
            params = {"query": search_query, "limit": 1}
            
            try:
                response = await self._request_async("GET", url, params=params)
                data = response.json()
                total = data.get("total_count", len(data.get("data", [])))
            except Exception:
                # Fallback: count via list
                total = await self._count_via_list(resource)
        else:
            # Count via list API (no search available)
            total = await self._count_via_list(resource)
        
        # Build result with proper alias
        agg = aggregates[0]
        alias = agg.alias or f"COUNT({agg.column})"
        
        logger.debug(f"Stripe COUNT optimization: returned {total}")
        
        return pa.table({alias: [total]})
    
    async def _count_via_list(self, resource: str) -> int:
        """Count records by paginating through list API with minimal fields."""
        url = f"https://{self._host}/v1/{resource}"
        total = 0
        starting_after = None
        
        while True:
            params = {"limit": 100}
            if starting_after:
                params["starting_after"] = starting_after
            
            try:
                response = await self._request_async("GET", url, params=params)
                data = response.json()
                batch = data.get("data", [])
                total += len(batch)
                
                if not data.get("has_more") or not batch:
                    break
                
                starting_after = batch[-1]["id"]
            except Exception:
                break
        
        return total

    async def _fetch_via_search(self, resource: str, predicates: List["Predicate"], limit: int) -> pa.Table:
        """Fetch using Stripe Search API."""
        query_parts = []
        for pred in predicates:
            if pred.operator == "=":
                val = f"'{pred.value}'" if isinstance(pred.value, str) else pred.value
                query_parts.append(f"{pred.column}:{val}")
            elif pred.operator == ">":
                query_parts.append(f"{pred.column}>{pred.value}")
            elif pred.operator == "<":
                query_parts.append(f"{pred.column}<{pred.value}")
        
        search_query = " AND ".join(query_parts)
        url = f"https://{self._host}/v1/{resource}/search"
        params = {"query": search_query}
        if limit:
            params["limit"] = min(100, limit)
            
        results = []
        try:
            response = await self._request_async("GET", url, params=params)
            data = response.json()
            results = data.get("data", [])
            
            # Pagination for search uses 'next_page' token
            while data.get("has_more") and (not limit or len(results) < limit):
                params["page"] = data.get("next_page")
                response = await self._request_async("GET", url, params=params)
                data = response.json()
                results.extend(data.get("data", []))
                
            if limit: results = results[:limit]
            return pa.Table.from_pylist(results) if results else pa.table({})
        except Exception as e:
            raise AdapterError(f"Stripe search failed: {e}")

    async def _fetch_via_list(self, resource: str, predicates: List["Predicate"], limit: int) -> pa.Table:
        """Fetch using Stripe List API."""
        url = f"https://{self._host}/v1/{resource}"
        params = {}
        if limit:
            params["limit"] = min(100, limit)
            
        results = []
        try:
            while True:
                response = await self._request_async("GET", url, params=params)
                data = response.json()
                batch = data.get("data", [])
                results.extend(batch)
                
                if not data.get("has_more") or (limit and len(results) >= limit):
                    break
                
                # Cursor pagination
                params["starting_after"] = results[-1]["id"]
                
            if limit: results = results[:limit]
            return pa.Table.from_pylist(results) if results else pa.table({})
        except Exception as e:
            raise AdapterError(f"Stripe list failed: {e}")

    def fetch(
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
        """
        Synchronous fetch using sync httpx directly.
        
        Note: Uses sync httpx here instead of anyio.run() because
        async DNS resolution fails on some Windows configurations.
        """
        return self._fetch_sync(
            table=table,
            columns=columns,
            predicates=predicates,
            limit=limit,
            offset=offset,
            order_by=order_by,
            group_by=group_by,
            aggregates=aggregates,
        )
    
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
        
        resource = self.RESOURCE_MAP.get(table.lower(), table.lower())
        
        # For simple COUNT optimization, still use async path (less frequent)
        if self._is_simple_count(aggregates, group_by):
            return anyio.run(lambda: self._fetch_count_only(resource, predicates, aggregates))
        
        url = f"https://{self._host}/v1/{resource}"
        params = {}
        if limit:
            params["limit"] = min(100, limit)
        
        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        
        results = []
        max_retries = 5
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                with httpx.Client(timeout=30.0) as client:
                    current_params = params.copy()
                    while True:
                        response = client.get(url, params=current_params, headers=headers)
                        if response.status_code >= 400:
                            raise AdapterError(f"Stripe API error ({response.status_code}): {response.text}")
                        
                        data = response.json()
                        batch = data.get("data", [])
                        results.extend(batch)
                        
                        if not data.get("has_more") or (limit and len(results) >= limit):
                            break
                        
                        # Cursor pagination
                        current_params["starting_after"] = results[-1]["id"]
                
                if limit:
                    results = results[:limit]
                
                result_table = pa.Table.from_pylist(results) if results else pa.table({})
                
                # Apply client-side aggregation if requested
                if aggregates and len(result_table) > 0:
                    result_table = self._compute_client_side_aggregates(result_table, group_by, aggregates)
                
                return result_table
                
            except httpx.ConnectError as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Stripe request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                    results = []  # Reset for retry
                else:
                    raise AdapterError(f"Stripe fetch failed after {max_retries} attempts: {e}")
            except httpx.NetworkError as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Stripe network error (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                    results = []  # Reset for retry
                else:
                    raise AdapterError(f"Stripe fetch failed after {max_retries} attempts: {e}")
            except httpx.ReadError as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Stripe read error (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                    results = []  # Reset for retry
                else:
                    raise AdapterError(f"Stripe fetch failed after {max_retries} attempts: {e}")
            except httpx.TimeoutException as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Stripe timeout (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                    results = []  # Reset for retry
                else:
                    raise AdapterError(f"Stripe fetch timed out after {max_retries} attempts: {e}")
            except Exception as e:
                if isinstance(e, AdapterError):
                    raise
                raise AdapterError(f"Stripe list failed: {e}")

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Synchronous get_schema (runs async)."""
        return anyio.run(lambda: self.get_schema_async(table))

    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Inferred schema from first record."""
        resource = self.RESOURCE_MAP.get(table.lower(), table.lower())
        url = f"https://{self._host}/v1/{resource}"
        try:
            response = await self._request_async("GET", url, params={"limit": 1})
            data = response.json()
            results = data.get("data", [])
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
        resource = self.RESOURCE_MAP.get(table.lower(), table.lower())
        url = f"https://{self._host}/v1/{resource}"
        try:
            # Stripe uses form-encoded data, not JSON
            await self._request_async("POST", url, data=values)
            return 1
        except Exception as e:
            raise QueryError(f"Stripe insert failed: {e}")

    def insert(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        """Insert a record into Stripe (sync)."""
        return anyio.run(lambda: self.insert_async(table, values, parameters))

    def update(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Update records in Stripe (sync)."""
        return anyio.run(lambda: self.update_async(table, values, predicates, parameters))

    def delete(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Delete records in Stripe (sync)."""
        return anyio.run(lambda: self.delete_async(table, predicates, parameters))

    async def _request_async(self, method: str, url: str, **kwargs) -> Any:
        """Make an HTTP request with retry logic for transient failures."""
        import httpx
        import asyncio
        
        headers = kwargs.get("headers", {})
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        kwargs["headers"] = headers
        
        max_retries = 5
        base_delay = 1.0  # seconds - longer delay for DNS stability
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.request(method, url, **kwargs)
                    if response.status_code >= 400:
                        raise AdapterError(f"Stripe API error ({response.status_code}): {response.text}")
                    return response
            except httpx.ConnectError as e:
                # Retry on connection/DNS errors
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Stripe request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    raise
            except httpx.TimeoutException as e:
                # Retry on timeout
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Stripe request timed out (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    raise AdapterError(f"Stripe request timed out after {max_retries} attempts: {e}")

    async def update_async(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        resource = self.RESOURCE_MAP.get(table.lower(), table.lower())
        object_id = self._extract_id_from_predicates(predicates, "Stripe update")
        
        url = f"https://{self._host}/v1/{resource}/{object_id}"
        try:
            # Stripe uses POST for updates
            await self._request_async("POST", url, data=values)
            return 1
        except Exception as e:
            raise QueryError(f"Stripe update failed: {e}")

    async def delete_async(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        resource = self.RESOURCE_MAP.get(table.lower(), table.lower())
        object_id = self._extract_id_from_predicates(predicates, "Stripe delete")
        
        url = f"https://{self._host}/v1/{resource}/{object_id}"
        try:
            await self._request_async("DELETE", url)
            return 1
        except Exception as e:
            raise QueryError(f"Stripe delete failed: {e}")

    def list_tables(self) -> List[str]:
        return list(self.RESOURCE_MAP.keys())
