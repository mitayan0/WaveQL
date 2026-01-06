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
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
import pyarrow as pa
import anyio

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
    supports_insert = True
    supports_update = True
    supports_delete = True
    
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
        
        # Determine if we can use the Search API
        use_search = resource in self.SEARCHABLE_RESOURCES and predicates
        
        if use_search:
            return await self._fetch_via_search(resource, predicates, limit)
        else:
            return await self._fetch_via_list(resource, predicates, limit)

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

    def fetch(self, *args, **kwargs) -> pa.Table:
        return anyio.run(lambda: self.fetch_async(*args, **kwargs))

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

    async def _request_async(self, method: str, url: str, **kwargs) -> Any:
        import httpx
        headers = kwargs.get("headers", {})
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        kwargs["headers"] = headers
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(method, url, **kwargs)
            if response.status_code >= 400:
                raise AdapterError(f"Stripe API error ({response.status_code}): {response.text}")
            return response

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
