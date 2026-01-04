"""
Generic REST API Adapter

Allows querying any REST API with configurable endpoints.
"""

from __future__ import annotations
import json
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

import requests
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate


class RESTAdapter(BaseAdapter):
    """
    Generic REST API adapter with configurable endpoints.
    
    Configuration:
        - base_url: Base URL for the API
        - endpoints: Dict mapping table names to endpoint configs
        - data_path: JSON path to data array in response (e.g., "results", "data.items")
    """
    
    adapter_name = "rest"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    
    def __init__(
        self,
        host: str,
        auth_manager=None,
        schema_cache=None,
        endpoints: Dict[str, Dict] = None,
        data_path: str = None,
        timeout: int = 30,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        self._host = host.rstrip("/")
        if not self._host.startswith("http"):
            self._host = f"https://{self._host}"
        
        self._endpoints = endpoints or {}
        self._data_path = data_path
        self._timeout = timeout
        # Note: HTTP sessions are now managed by the connection pool in BaseAdapter
        # Use self._get_session() context manager for requests
    
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
        Fetch data from REST endpoint.
        """
        if bool(group_by or aggregates):
            raise NotImplementedError("RESTAdapter does not support aggregation pushdown")
        endpoint_config = self._get_endpoint_config(table)
        url = f"{self._host}{endpoint_config['path']}"
        
        # Build query params with predicate pushdown
        params = self._build_params(endpoint_config, predicates, limit, offset)
        
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }
        
        with self._get_session() as session:
            def do_request():
                response = session.get(url, params=params, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return response.json()
            
            try:
                # Use rate limiter for automatic retry
                data = self._rate_limiter.execute_with_retry(do_request)
                
                # Extract records from response
                records = self._extract_records(data, endpoint_config)
                
                # Discover schema
                schema_columns = self._get_or_discover_schema(table, records)
                
                # Apply client-side filtering for non-pushdown predicates
                if predicates and not endpoint_config.get("supports_filter", True):
                    records = self._apply_filters(records, predicates)
                
                # Apply limit/offset if not done server-side
                if offset and not endpoint_config.get("supports_offset", True):
                    records = records[offset:]
                if limit and not endpoint_config.get("supports_limit", True):
                    records = records[:limit]
                
                return self._to_arrow(records, schema_columns, columns)
                
            except requests.RequestException as e:
                raise AdapterError(f"REST request failed: {e}")
    
    def _get_endpoint_config(self, table: str) -> Dict:
        """Get endpoint configuration for table."""
        if table in self._endpoints:
            return self._endpoints[table]
        
        # Default: use table name as path
        return {
            "path": f"/{table}",
            "method": "GET",
            "data_path": self._data_path,
        }
    
    def _build_params(
        self,
        config: Dict,
        predicates: List["Predicate"],
        limit: int,
        offset: int,
    ) -> Dict[str, str]:
        """Build query parameters."""
        params = {}
        
        # Add predicates as query params
        if predicates and config.get("supports_filter", True):
            filter_param = config.get("filter_param", "filter")
            filter_format = config.get("filter_format", "query")
            
            if filter_format == "query":
                # Simple key=value params
                for pred in predicates:
                    if pred.operator == "=":
                        params[pred.column] = pred.value
            elif filter_format == "json":
                # JSON filter
                filters = {pred.column: pred.value for pred in predicates if pred.operator == "="}
                params[filter_param] = json.dumps(filters)
        
        # Pagination
        if limit and config.get("supports_limit", True):
            limit_param = config.get("limit_param", "limit")
            params[limit_param] = str(limit)
        
        if offset and config.get("supports_offset", True):
            offset_param = config.get("offset_param", "offset")
            params[offset_param] = str(offset)
        
        return params
    
    def _extract_records(self, data: Any, config: Dict) -> List[Dict]:
        """Extract records from response data."""
        data_path = config.get("data_path", self._data_path)
        
        if data_path:
            # Navigate to data using dot notation
            for key in data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, [])
                elif isinstance(data, list) and key.isdigit():
                    data = data[int(key)]
                else:
                    return []
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []
    
    def _apply_filters(self, records: List[Dict], predicates: List["Predicate"]) -> List[Dict]:
        """Apply client-side filtering."""
        filtered = []
        for record in records:
            match = True
            for pred in predicates:
                value = record.get(pred.column)
                
                if pred.operator == "=":
                    match = value == pred.value
                elif pred.operator == "!=":
                    match = value != pred.value
                elif pred.operator == ">":
                    match = value > pred.value
                elif pred.operator == "<":
                    match = value < pred.value
                elif pred.operator == ">=":
                    match = value >= pred.value
                elif pred.operator == "<=":
                    match = value <= pred.value
                elif pred.operator == "LIKE":
                    import re
                    pattern = pred.value.replace("%", ".*").replace("_", ".")
                    match = bool(re.match(pattern, str(value or ""), re.IGNORECASE))
                
                if not match:
                    break
            
            if match:
                filtered.append(record)
        
        return filtered
    
    def _get_or_discover_schema(self, table: str, records: List[Dict]) -> List[ColumnInfo]:
        """Discover schema from records using multi-sample inference with struct support."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        if not records:
            return []
        
        # Use new schema inference utility for robust multi-sample detection
        from waveql.utils.schema import infer_schema_from_records
        
        arrow_schema = infer_schema_from_records(records, sample_size=5)
        
        # Convert Arrow schema to ColumnInfo for caching
        columns = []
        for field in arrow_schema:
            columns.append(ColumnInfo(
                name=field.name,
                data_type=self._arrow_type_to_string(field.type),
                nullable=True,
                arrow_type=field.type,
            ))
        
        self._cache_schema(table, columns)
        return columns
    
    def _arrow_type_to_string(self, arrow_type) -> str:
        """Convert Arrow type to string representation for legacy compatibility."""
        import pyarrow as pa
        if pa.types.is_boolean(arrow_type):
            return "boolean"
        if pa.types.is_integer(arrow_type):
            return "integer"
        if pa.types.is_floating(arrow_type):
            return "float"
        if pa.types.is_struct(arrow_type):
            return "struct"
        if pa.types.is_list(arrow_type):
            return "list"
        return "string"
    
    def _to_arrow(
        self,
        records: List[Dict],
        schema_columns: List[ColumnInfo],
        selected_columns: List[str] = None,
    ) -> pa.Table:
        """Convert to Arrow table with native struct support."""
        if not records:
            fields = []
            for c in schema_columns:
                arrow_type = getattr(c, 'arrow_type', None) or pa.string()
                fields.append(pa.field(c.name, arrow_type))
            return pa.table({f.name: [] for f in fields})
        
        # Use new schema utility for proper struct conversion
        from waveql.utils.schema import records_to_arrow_table
        
        # Build schema from ColumnInfo (which now includes Arrow types)
        schema_fields = []
        for col in schema_columns:
            if selected_columns and selected_columns != ["*"] and col.name not in selected_columns:
                continue
            arrow_type = getattr(col, 'arrow_type', None) or pa.string()
            schema_fields.append(pa.field(col.name, arrow_type))
        
        schema = pa.schema(schema_fields)
        
        # Filter records to only include selected columns if specified
        if selected_columns and selected_columns != ["*"]:
            filtered_records = [
                {k: v for k, v in rec.items() if k in selected_columns}
                for rec in records
            ]
        else:
            filtered_records = records
        
        # Convert using the new utility with struct support
        return records_to_arrow_table(filtered_records, schema=schema)
    
    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Discover schema."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        # Fetch one record to discover
        records = self.fetch(table, limit=1).to_pylist()
        return self._get_or_discover_schema(table, records)
    
    def insert(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        """Insert via POST."""
        config = self._get_endpoint_config(table)
        url = f"{self._host}{config['path']}"
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.post(url, json=values, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"INSERT failed: {e}")
    
    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update via PUT/PATCH."""
        config = self._get_endpoint_config(table)
        
        # Get ID from predicates
        record_id = None
        id_field = config.get("id_field", "id")
        for pred in (predicates or []):
            if pred.column == id_field and pred.operator == "=":
                record_id = pred.value
                break
        
        if not record_id:
            raise QueryError(f"UPDATE requires {id_field} in WHERE clause")
        
        url = f"{self._host}{config['path']}/{record_id}"
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.patch(url, json=values, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"UPDATE failed: {e}")
    
    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete via DELETE."""
        config = self._get_endpoint_config(table)
        
        # Get ID from predicates
        record_id = None
        id_field = config.get("id_field", "id")
        for pred in (predicates or []):
            if pred.column == id_field and pred.operator == "=":
                record_id = pred.value
                break
        
        if not record_id:
            raise QueryError(f"DELETE requires {id_field} in WHERE clause")
        
        url = f"{self._host}{config['path']}/{record_id}"
        
        headers = {"Accept": "application/json", **self._get_auth_headers()}
        
        try:
            with self._get_session() as session:
                response = session.delete(url, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"DELETE failed: {e}")
