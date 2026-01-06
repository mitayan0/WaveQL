"""
Zendesk Adapter - Tickets, Users, Organizations support.

Features:
- Predicate pushdown to Zendesk Search API (v2)
- Support for Tickets, Users, Organizations, and Groups
- Email/Token and OAuth2 authentication support
- Automatic pagination handling
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


class ZendeskAdapter(BaseAdapter):
    """
    Zendesk adapter for querying the Support API v2.
    """
    
    adapter_name = "zendesk"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    
    # Mapping of tables to Zendesk types
    TYPE_MAP = {
        "tickets": "ticket",
        "ticket": "ticket",
        "users": "user",
        "user": "user",
        "organizations": "organization",
        "organization": "organization",
        "groups": "group",
        "group": "group",
    }

    def __init__(
        self,
        host: str,  # subdomain.zendesk.com
        auth_manager=None,
        schema_cache=None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        self._subdomain = host.split(".")[0]
        self._email = kwargs.get("username")
        self._api_token = kwargs.get("api_key") or kwargs.get("password")
        self._access_token = kwargs.get("oauth_token")
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
        """Fetch data from Zendesk Search API (async)."""
        resource_type = self.TYPE_MAP.get(table.lower(), table.lower())
        
        # Build search query
        query_parts = [f"type:{resource_type}"]
        if predicates:
            for pred in predicates:
                if pred.operator == "=":
                    query_parts.append(f"{pred.column}:{pred.value}")
                elif pred.operator == ">":
                    query_parts.append(f"{pred.column}>{pred.value}")
                elif pred.operator == "<":
                    query_parts.append(f"{pred.column}<{pred.value}")
        
        search_query = " ".join(query_parts)
        url = f"https://{self._host}/api/v2/search.json"
        
        params = {"query": search_query}
        if order_by:
            col, direction = order_by[0]
            params["sort_by"] = col
            params["sort_order"] = "desc" if direction.upper() == "DESC" else "asc"
            
        results = []
        next_page = url
        
        try:
            while next_page:
                response = await self._request_async("GET", next_page, params=params if next_page == url else None)
                data = response.json()
                
                batch = data.get("results", [])
                results.extend(batch)
                
                if limit and len(results) >= limit:
                    results = results[:limit]
                    break
                
                next_page = data.get("next_page")
                
            if not results:
                return pa.table({})
            
            return pa.Table.from_pylist(results)
            
        except Exception as e:
            raise AdapterError(f"Zendesk search failed: {e}")

    def fetch(self, *args, **kwargs) -> pa.Table:
        return anyio.run(lambda: self.fetch_async(*args, **kwargs))

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Synchronous get_schema (runs async)."""
        return anyio.run(lambda: self.get_schema_async(table))

    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Inferred schema from first record."""
        # Use fetch with small limit to get sample
        resource_type = self.TYPE_MAP.get(table.lower(), table.lower())
        url = f"https://{self._host}/api/v2/search.json"
        try:
            response = await self._request_async("GET", url, params={"query": f"type:{resource_type}", "per_page": 1})
            data = response.json()
            results = data.get("results", [])
            if not results: return []
            
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
        resource = table.lower()
        singular = resource[:-1] if resource.endswith("s") else resource
        url = f"https://{self._host}/api/v2/{resource}.json"
        
        payload = {singular: values}
        try:
            await self._request_async("POST", url, json=payload)
            return 1
        except Exception as e:
            raise QueryError(f"Zendesk insert failed: {e}")

    async def _request_async(self, method: str, url: str, **kwargs) -> Any:
        import httpx
        import base64
        
        headers = kwargs.get("headers", {})
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        elif self._email and self._api_token:
            auth_str = f"{self._email}/token:{self._api_token}"
            encoded_auth = base64.b64encode(auth_str.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_auth}"
        kwargs["headers"] = headers
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(method, url, **kwargs)
            if response.status_code >= 400:
                raise AdapterError(f"Zendesk API error ({response.status_code}): {response.text}")
            return response

    async def update_async(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        resource = table.lower()
        singular = resource[:-1] if resource.endswith("s") else resource
        object_id = self._extract_id_from_predicates(predicates, "Zendesk update")
        
        url = f"https://{self._host}/api/v2/{resource}/{object_id}.json"
        
        payload = {singular: values}
        try:
            await self._request_async("PUT", url, json=payload)
            return 1
        except Exception as e:
            raise QueryError(f"Zendesk update failed: {e}")

    async def delete_async(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        resource = table.lower()
        object_id = self._extract_id_from_predicates(predicates, "Zendesk delete")
        
        url = f"https://{self._host}/api/v2/{resource}/{object_id}.json"
        try:
            await self._request_async("DELETE", url)
            return 1
        except Exception as e:
            raise QueryError(f"Zendesk delete failed: {e}")

    def list_tables(self) -> List[str]:
        return list(self.TYPE_MAP.keys())
