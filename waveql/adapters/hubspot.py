"""
HubSpot Adapter - CRM (Contacts, Deals, Companies, Tickets) support.

Features:
- Predicate pushdown to HubSpot CRM Search API (v3)
- Support for Contacts, Companies, Deals, Tickets, and custom objects
- Unified OAuth2 and Private App Token authentication
- Automatic pagination handling
- Batch CRUD operations
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


class HubSpotAdapter(BaseAdapter):
    """
    HubSpot adapter for querying the CRM API.
    
    Translates SQL into HubSpot CRM Search API (v3) requests for optimal filtering.
    """
    
    adapter_name = "hubspot"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    
    # Mapping of common table names to HubSpot object types
    OBJECT_TYPE_MAP = {
        "contacts": "contacts",
        "contact": "contacts",
        "companies": "companies",
        "company": "companies",
        "deals": "deals",
        "deal": "deals",
        "tickets": "tickets",
        "ticket": "tickets",
        "products": "products",
        "line_items": "line_items",
        "quotes": "quotes",
    }
    
    # Operator mapping for HubSpot Search API
    OPERATOR_MAP = {
        "=": "EQ",
        "!=": "NEQ",
        ">": "GT",
        ">=": "GTE",
        "<": "LT",
        "<=": "LTE",
        "LIKE": "CONTAINS_TOKEN", # Nearest equivalent for search
        "IN": "IN",
        "NOT IN": "NOT_IN",
        "IS NULL": "NOT_HAS_PROPERTY",
        "IS NOT NULL": "HAS_PROPERTY",
    }

    def __init__(
        self,
        host: str = "api.hubapi.com",
        auth_manager=None,
        schema_cache=None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        self._access_token = kwargs.get("api_key") or kwargs.get("oauth_token")
        self._config = kwargs
    
    def _get_object_type(self, table: str) -> str:
        """Get the HubSpot object type for a table name."""
        return self.OBJECT_TYPE_MAP.get(table.lower(), table.lower())

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
        """Fetch data from HubSpot CRM (async)."""
        object_type = self._get_object_type(table)
        
        # Build Search API payload
        payload = self._build_search_payload(columns, predicates, limit, offset, order_by)
        
        results = []
        after = None
        remaining_limit = limit if limit else 1000000
        
        # HubSpot Search API uses 'after' for pagination instead of offset
        # But we can simulate offset by skipping records if needed (though Search API doesn't support true offset)
        
        try:
            url = f"https://{self._host}/crm/v3/objects/{object_type}/search"
            
            while True:
                # Update payload with current 'after' token and limit
                current_payload = payload.copy()
                if after:
                    current_payload["after"] = after
                
                # Search API has a max limit of 100 per request
                req_limit = min(100, remaining_limit)
                current_payload["limit"] = req_limit
                
                response = await self._request_async("POST", url, json=current_payload)
                data = response.json()
                
                batch_results = data.get("results", [])
                for item in batch_results:
                    # Flatten properties
                    row = {"id": item["id"]}
                    row.update(item.get("properties", {}))
                    results.append(row)
                
                remaining_limit -= len(batch_results)
                paging = data.get("paging", {}).get("next", {})
                after = paging.get("after")
                
                if not after or remaining_limit <= 0 or (limit and len(results) >= limit):
                    break
            
            if not results:
                return pa.table({})
            
            # Convert to Arrow Table
            table_data = {k: [r.get(k) for r in results] for k in results[0].keys()}
            return pa.table(table_data)
            
        except Exception as e:
            raise AdapterError(f"HubSpot search failed: {e}")

    def fetch(self, *args, **kwargs) -> pa.Table:
        """Synchronous fetch (runs async)."""
        return anyio.run(lambda: self.fetch_async(*args, **kwargs))

    def _build_search_payload(
        self,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
    ) -> Dict[str, Any]:
        """Build HubSpot Search API payload."""
        payload = {
            "filterGroups": [],
            "sorts": [],
            "properties": columns if columns and columns != ["*"] else []
        }
        
        # Convert predicates to filterGroups
        if predicates:
            filters = []
            for pred in predicates:
                op = self.OPERATOR_MAP.get(pred.operator.upper())
                if not op:
                    logger.warning(f"HubSpot does not support operator {pred.operator}, falling back to local filter")
                    continue
                
                filter_obj = {
                    "propertyName": pred.column,
                    "operator": op,
                }
                
                if pred.operator.upper() not in ["IS NULL", "IS NOT NULL"]:
                     filter_obj["value"] = str(pred.value)
                
                filters.append(filter_obj)
            
            if filters:
                payload["filterGroups"] = [{"filters": filters}]
        
        # Sorting
        if order_by:
            for col, direction in order_by:
                payload["sorts"].append({
                    "propertyName": col,
                    "direction": "DESCENDING" if direction.upper() == "DESC" else "ASCENDING"
                })
        
        return payload

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Synchronous get_schema (runs async)."""
        return anyio.run(lambda: self.get_schema_async(table))

    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Get schema from HubSpot property definitions."""
        object_type = self._get_object_type(table)
        url = f"https://{self._host}/crm/v3/properties/{object_type}"
        
        try:
            response = await self._request_async("GET", url)
            data = response.json()
            
            columns = [
                ColumnInfo(
                    name="id",
                    data_type="string",
                    nullable=False
                )
            ]
            
            for prop in data.get("results", []):
                data_type = self._map_type(prop.get("type"), prop.get("fieldType"))
                columns.append(ColumnInfo(
                    name=prop["name"],
                    data_type=data_type,
                    nullable=True
                ))
            
            return columns
        except Exception as e:
            raise AdapterError(f"Failed to get HubSpot schema: {e}")

    def _map_type(self, hubspot_type: str, field_type: str) -> str:
        """Map HubSpot property types to standard SQL types."""
        if hubspot_type == "number":
            return "double" if field_type == "number" else "integer"
        elif hubspot_type == "bool" or field_type == "booleancheckbox":
            return "boolean"
        elif hubspot_type == "datetime" or hubspot_type == "date":
            return "timestamp"
        return "string"

    def list_tables(self) -> List[str]:
        """List standard HubSpot CRM objects."""
        return list(self.OBJECT_TYPE_MAP.keys())

    async def insert_async(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        """Insert a record into HubSpot (async)."""
        object_type = self._get_object_type(table)
        url = f"https://{self._host}/crm/v3/objects/{object_type}"
        
        payload = {"properties": values}
        try:
            await self._request_async("POST", url, json=payload)
            return 1
        except Exception as e:
            raise QueryError(f"HubSpot insert failed: {e}")

    async def update_async(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Update records in HubSpot (requires ID)."""
        object_type = self._get_object_type(table)
        
        # HubSpot update requires an ID. If predicates contain ID, we use it.
        object_id = None
        if predicates:
            for pred in predicates:
                if pred.column.lower() == "id" and pred.operator == "=":
                    object_id = pred.value
                    break
        
        if not object_id:
            raise QueryError("HubSpot update requires 'id' in WHERE clause (e.g., WHERE id = '123')")
            
        url = f"https://{self._host}/crm/v3/objects/{object_type}/{object_id}"
        payload = {"properties": values}
        try:
            await self._request_async("PATCH", url, json=payload)
            return 1
        except Exception as e:
            raise QueryError(f"HubSpot update failed: {e}")

    async def delete_async(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Delete records in HubSpot (requires ID)."""
        object_type = self._get_object_type(table)
        
        object_id = None
        if predicates:
            for pred in predicates:
                if pred.column.lower() == "id" and pred.operator == "=":
                    object_id = pred.value
                    break
        
        if not object_id:
            raise QueryError("HubSpot delete requires 'id' in WHERE clause")
            
        url = f"https://{self._host}/crm/v3/objects/{object_type}/{object_id}"
        try:
            await self._request_async("DELETE", url)
            return 1
        except Exception as e:
            raise QueryError(f"HubSpot delete failed: {e}")

    async def _request_async(self, method: str, url: str, **kwargs) -> Any:
        """Internal helper for async requests with auth."""
        import httpx
        
        headers = kwargs.get("headers", {})
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        kwargs["headers"] = headers
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(method, url, **kwargs)
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    message = error_data.get("message", response.text)
                except:
                    message = response.text
                raise AdapterError(f"HubSpot API error ({response.status_code}): {message}")
            return response
