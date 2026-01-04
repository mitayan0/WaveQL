"""
Jira Adapter - Full support for Jira Cloud REST API

Features:
- JQL (Jira Query Language) predicate pushdown
- Dynamic schema discovery from any project
- Pagination handling with startAt/maxResults
- Full CRUD operations for issues
- Support for projects, users, and custom fields
"""

from __future__ import annotations
import json
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
from urllib.parse import quote

import requests
import httpx
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError, RateLimitError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate


class JiraAdapter(BaseAdapter):
    """
    Jira Cloud REST API adapter.
    
    Supports querying issues, projects, users, and other Jira resources.
    Uses JQL for predicate pushdown on issue searches.
    
    Authentication:
        - API Token (recommended): Use email as username and API token as password
        - OAuth 2.0: Supported via AuthManager
    
    Example:
        adapter = JiraAdapter(
            host="your-domain.atlassian.net",
            auth_manager=AuthManager(username="email@example.com", password="api_token")
        )
    """
    
    adapter_name = "jira"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    supports_batch = False
    
    # Jira type to Arrow type mapping
    TYPE_MAP = {
        "string": pa.string(),
        "number": pa.float64(),
        "integer": pa.int64(),
        "boolean": pa.bool_(),
        "datetime": pa.string(),  # ISO format strings
        "date": pa.string(),
        "array": pa.string(),  # JSON array as string
        "object": pa.string(),  # JSON object as string
        "user": pa.string(),  # User display name
    }
    
    # Virtual table configurations
    TABLES = {
        "issue": {
            "endpoint": "/rest/api/3/search",
            "method": "POST",
            "supports_jql": True,
            "id_field": "key",
        },
        "issues": {  # Alias
            "endpoint": "/rest/api/3/search",
            "method": "POST",
            "supports_jql": True,
            "id_field": "key",
        },
        "project": {
            "endpoint": "/rest/api/3/project/search",
            "method": "GET",
            "supports_jql": False,
            "id_field": "key",
        },
        "projects": {  # Alias
            "endpoint": "/rest/api/3/project/search",
            "method": "GET",
            "supports_jql": False,
            "id_field": "key",
        },
        "user": {
            "endpoint": "/rest/api/3/users/search",
            "method": "GET",
            "supports_jql": False,
            "id_field": "accountId",
        },
        "users": {  # Alias
            "endpoint": "/rest/api/3/users/search",
            "method": "GET",
            "supports_jql": False,
            "id_field": "accountId",
        },
        "status": {
            "endpoint": "/rest/api/3/status",
            "method": "GET",
            "supports_jql": False,
            "id_field": "id",
        },
        "priority": {
            "endpoint": "/rest/api/3/priority",
            "method": "GET",
            "supports_jql": False,
            "id_field": "id",
        },
        "issuetype": {
            "endpoint": "/rest/api/3/issuetype",
            "method": "GET",
            "supports_jql": False,
            "id_field": "id",
        },
        "field": {
            "endpoint": "/rest/api/3/field",
            "method": "GET",
            "supports_jql": False,
            "id_field": "id",
        },
    }
    
    def __init__(
        self,
        host: str,
        auth_manager=None,
        schema_cache=None,
        page_size: int = 100,
        timeout: int = 30,
        expand: List[str] = None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        # Normalize host
        self._host = host.rstrip("/")
        if not self._host.startswith("http"):
            self._host = f"https://{self._host}"
        
        self._page_size = min(page_size, 100)  # Jira max is 100
        self._timeout = timeout
        self._expand = expand or ["names", "schema"]
        
        # Note: HTTP sessions managed by connection pool in BaseAdapter
    
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
        """Fetch data from Jira."""
        if bool(group_by or aggregates):
            raise NotImplementedError("JiraAdapter does not support aggregation pushdown")
        
        table_name = self._extract_table_name(table)
        table_config = self._get_table_config(table_name)
        
        if table_config["supports_jql"]:
            return self._fetch_with_jql(table_name, columns, predicates, limit, offset, order_by)
        else:
            return self._fetch_simple(table_name, table_config, columns, limit, offset)
    
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
        """Fetch data from Jira (async)."""
        if bool(group_by or aggregates):
            raise NotImplementedError("JiraAdapter does not support aggregation pushdown")
        
        table_name = self._extract_table_name(table)
        table_config = self._get_table_config(table_name)
        
        if table_config["supports_jql"]:
            return await self._fetch_with_jql_async(table_name, columns, predicates, limit, offset, order_by)
        else:
            return await self._fetch_simple_async(table_name, table_config, columns, limit, offset)
    
    def _extract_table_name(self, table: str) -> str:
        """Extract table name from schema.table format."""
        if "." in table:
            return table.split(".", 1)[1].lower()
        return table.lower()
    
    def _get_table_config(self, table_name: str) -> Dict:
        """Get configuration for a table."""
        if table_name in self.TABLES:
            return self.TABLES[table_name]
        
        # Default: treat as issue search with project filter
        return self.TABLES["issue"]
    
    def _fetch_with_jql(
        self,
        table: str,
        columns: List[str],
        predicates: List["Predicate"],
        limit: int,
        offset: int,
        order_by: List[tuple],
    ) -> pa.Table:
        """Fetch issues using JQL search."""
        url = f"{self._host}/rest/api/3/search"
        
        # Build JQL query
        jql = self._build_jql(predicates, order_by)
        
        # Build request body
        body = {
            "jql": jql,
            "startAt": offset or 0,
            "maxResults": min(limit or self._page_size, self._page_size),
            "expand": self._expand,
        }
        
        # Field selection
        if columns and columns != ["*"]:
            body["fields"] = columns
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        # Fetch with pagination
        all_issues = []
        total_fetched = 0
        
        with self._get_session() as session:
            while True:
                body["startAt"] = (offset or 0) + total_fetched
                
                response = self._request_with_retry(
                    lambda: session.post(url, json=body, headers=headers, timeout=self._timeout)
                )
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                
                response.raise_for_status()
                data = response.json()
                
                issues = data.get("issues", [])
                all_issues.extend(issues)
                total_fetched += len(issues)
                
                # Check if we have more pages
                total = data.get("total", 0)
                if total_fetched >= total:
                    break
                if limit and total_fetched >= limit:
                    break
                if len(issues) < self._page_size:
                    break
        
        # Flatten and convert to Arrow
        records = [self._normalize_issue(issue) for issue in all_issues]
        if limit:
            records = records[:limit]
        
        schema_columns = self._get_or_discover_schema(table, records)
        return self._to_arrow(records, schema_columns, columns)
    
    async def _fetch_with_jql_async(
        self,
        table: str,
        columns: List[str],
        predicates: List["Predicate"],
        limit: int,
        offset: int,
        order_by: List[tuple],
    ) -> pa.Table:
        """Fetch issues using JQL search (async)."""
        url = f"{self._host}/rest/api/3/search"
        
        jql = self._build_jql(predicates, order_by)
        
        body = {
            "jql": jql,
            "startAt": offset or 0,
            "maxResults": min(limit or self._page_size, self._page_size),
            "expand": self._expand,
        }
        
        if columns and columns != ["*"]:
            body["fields"] = columns
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        all_issues = []
        total_fetched = 0
        
        while True:
            body["startAt"] = (offset or 0) + total_fetched
            
            response = await client.post(url, json=body, headers=headers, timeout=self._timeout)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
            
            response.raise_for_status()
            data = response.json()
            
            issues = data.get("issues", [])
            all_issues.extend(issues)
            total_fetched += len(issues)
            
            total = data.get("total", 0)
            if total_fetched >= total:
                break
            if limit and total_fetched >= limit:
                break
            if len(issues) < self._page_size:
                break
        
        records = [self._normalize_issue(issue) for issue in all_issues]
        if limit:
            records = records[:limit]
        
        schema_columns = self._get_or_discover_schema(table, records)
        return self._to_arrow(records, schema_columns, columns)
    
    def _fetch_simple(
        self,
        table: str,
        config: Dict,
        columns: List[str],
        limit: int,
        offset: int,
    ) -> pa.Table:
        """Fetch from simple endpoints (projects, users, etc.)."""
        url = f"{self._host}{config['endpoint']}"
        
        params = {}
        if limit:
            params["maxResults"] = min(limit, self._page_size)
        if offset:
            params["startAt"] = offset
        
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }
        
        with self._get_session() as session:
            response = self._request_with_retry(
                lambda: session.get(url, params=params, headers=headers, timeout=self._timeout)
            )
            response.raise_for_status()
            data = response.json()
        
        # Handle different response formats
        if isinstance(data, list):
            records = data
        elif "values" in data:
            records = data["values"]
        else:
            records = [data]
        
        if limit:
            records = records[:limit]
        
        schema_columns = self._get_or_discover_schema(table, records)
        return self._to_arrow(records, schema_columns, columns)
    
    async def _fetch_simple_async(
        self,
        table: str,
        config: Dict,
        columns: List[str],
        limit: int,
        offset: int,
    ) -> pa.Table:
        """Fetch from simple endpoints (async)."""
        url = f"{self._host}{config['endpoint']}"
        
        params = {}
        if limit:
            params["maxResults"] = min(limit, self._page_size)
        if offset:
            params["startAt"] = offset
        
        headers = {
            "Accept": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        response = await client.get(url, params=params, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            records = data
        elif "values" in data:
            records = data["values"]
        else:
            records = [data]
        
        if limit:
            records = records[:limit]
        
        schema_columns = self._get_or_discover_schema(table, records)
        return self._to_arrow(records, schema_columns, columns)
    
    def _build_jql(self, predicates: List["Predicate"], order_by: List[tuple] = None) -> str:
        """Build JQL query from predicates."""
        jql_parts = []
        
        if predicates:
            for pred in predicates:
                jql_parts.append(self._predicate_to_jql(pred))
        
        jql = " AND ".join(jql_parts) if jql_parts else ""
        
        # Add ORDER BY
        if order_by:
            order_parts = []
            for col, direction in order_by:
                order_parts.append(f"{col} {direction}")
            jql += " ORDER BY " + ", ".join(order_parts)
        
        return jql
    
    def _predicate_to_jql(self, pred: "Predicate") -> str:
        """Convert predicate to JQL syntax."""
        col = pred.column
        op = pred.operator
        val = pred.value
        
        # JQL operator mapping
        op_map = {
            "=": "=",
            "!=": "!=",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
            "LIKE": "~",
            "IN": "IN",
            "IS NULL": "IS EMPTY",
            "IS NOT NULL": "IS NOT EMPTY",
        }
        
        jql_op = op_map.get(op, "=")
        
        if op in ("IS NULL", "IS NOT NULL"):
            return f"{col} {jql_op}"
        elif op == "LIKE":
            # Convert SQL LIKE to JQL contains
            search_val = val.replace("%", "").replace("_", "?")
            return f'{col} ~ "{search_val}"'
        elif op == "IN":
            if isinstance(val, (list, tuple)):
                val_list = ", ".join(f'"{v}"' if isinstance(v, str) else str(v) for v in val)
                return f"{col} IN ({val_list})"
            return f'{col} IN ({val})'
        elif isinstance(val, str):
            return f'{col} {jql_op} "{val}"'
        else:
            return f"{col} {jql_op} {val}"
    
    def _normalize_issue(self, issue: Dict) -> Dict:
        """
        Normalize a Jira issue for Arrow conversion.
        
        Unlike the old _flatten_issue, this preserves nested objects as dicts
        so they become queryable structs in DuckDB:
        
            SELECT fields.reporter.displayName FROM jira.issues
        """
        record = {
            "id": issue.get("id"),
            "key": issue.get("key"),
            "self": issue.get("self"),
        }
        
        # Merge fields directly into record, preserving nested structures
        fields = issue.get("fields", {})
        for field_name, value in fields.items():
            # Keep nested dicts and lists as-is for struct/list type inference
            record[field_name] = value
        
        return record
    
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
    
    def _arrow_type_to_string(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to string representation for legacy compatibility."""
        if pa.types.is_boolean(arrow_type):
            return "boolean"
        if pa.types.is_integer(arrow_type):
            return "integer"
        if pa.types.is_floating(arrow_type):
            return "number"
        if pa.types.is_struct(arrow_type):
            return "struct"
        if pa.types.is_list(arrow_type):
            return "array"
        return "string"
    
    def _to_arrow(
        self,
        records: List[Dict],
        schema_columns: List[ColumnInfo],
        selected_columns: List[str] = None,
    ) -> pa.Table:
        """Convert records to Arrow table with native struct support."""
        if not records:
            fields = []
            for c in schema_columns:
                arrow_type = getattr(c, 'arrow_type', None) or self.TYPE_MAP.get(c.data_type, pa.string())
                fields.append(pa.field(c.name, arrow_type))
            return pa.table({f.name: [] for f in fields})
        
        # Use new schema utility for proper struct conversion
        from waveql.utils.schema import records_to_arrow_table
        
        # Build schema from ColumnInfo (which now includes Arrow types)
        schema_fields = []
        for col in schema_columns:
            if selected_columns and selected_columns != ["*"] and col.name not in selected_columns:
                continue
            arrow_type = getattr(col, 'arrow_type', None) or self.TYPE_MAP.get(col.data_type, pa.string())
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
        """Discover schema by fetching one record."""
        table_name = self._extract_table_name(table)
        
        cached = self._get_cached_schema(table_name)
        if cached:
            return cached
        
        records = self.fetch(table_name, limit=1).to_pylist()
        return self._get_or_discover_schema(table_name, records)
    
    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Discover schema (async)."""
        table_name = self._extract_table_name(table)
        
        cached = self._get_cached_schema(table_name)
        if cached:
            return cached
        
        records = (await self.fetch_async(table_name, limit=1)).to_pylist()
        return self._get_or_discover_schema(table_name, records)
    
    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Create a new issue in Jira."""
        table_name = self._extract_table_name(table)
        
        if table_name not in ("issue", "issues"):
            raise QueryError(f"INSERT not supported for table: {table_name}")
        
        url = f"{self._host}/rest/api/3/issue"
        
        # Build issue payload
        issue_data = {"fields": values}
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.post(url, json=issue_data, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"INSERT failed: {e}")
    
    async def insert_async(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Create a new issue (async)."""
        table_name = self._extract_table_name(table)
        
        if table_name not in ("issue", "issues"):
            raise QueryError(f"INSERT not supported for table: {table_name}")
        
        url = f"{self._host}/rest/api/3/issue"
        issue_data = {"fields": values}
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        response = await client.post(url, json=issue_data, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        return 1
    
    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update an issue in Jira."""
        table_name = self._extract_table_name(table)
        
        if table_name not in ("issue", "issues"):
            raise QueryError(f"UPDATE not supported for table: {table_name}")
        
        # Get issue key from predicates
        issue_key = None
        for pred in (predicates or []):
            if pred.column.lower() in ("key", "id") and pred.operator == "=":
                issue_key = pred.value
                break
        
        if not issue_key:
            raise QueryError("UPDATE requires 'key' or 'id' in WHERE clause")
        
        url = f"{self._host}/rest/api/3/issue/{issue_key}"
        issue_data = {"fields": values}
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.put(url, json=issue_data, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"UPDATE failed: {e}")
    
    async def update_async(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update an issue (async)."""
        table_name = self._extract_table_name(table)
        
        if table_name not in ("issue", "issues"):
            raise QueryError(f"UPDATE not supported for table: {table_name}")
        
        issue_key = None
        for pred in (predicates or []):
            if pred.column.lower() in ("key", "id") and pred.operator == "=":
                issue_key = pred.value
                break
        
        if not issue_key:
            raise QueryError("UPDATE requires 'key' or 'id' in WHERE clause")
        
        url = f"{self._host}/rest/api/3/issue/{issue_key}"
        issue_data = {"fields": values}
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        response = await client.put(url, json=issue_data, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        return 1
    
    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete an issue from Jira."""
        table_name = self._extract_table_name(table)
        
        if table_name not in ("issue", "issues"):
            raise QueryError(f"DELETE not supported for table: {table_name}")
        
        # Get issue key from predicates
        issue_key = None
        for pred in (predicates or []):
            if pred.column.lower() in ("key", "id") and pred.operator == "=":
                issue_key = pred.value
                break
        
        if not issue_key:
            raise QueryError("DELETE requires 'key' or 'id' in WHERE clause")
        
        url = f"{self._host}/rest/api/3/issue/{issue_key}"
        
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.delete(url, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"DELETE failed: {e}")
    
    async def delete_async(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete an issue (async)."""
        table_name = self._extract_table_name(table)
        
        if table_name not in ("issue", "issues"):
            raise QueryError(f"DELETE not supported for table: {table_name}")
        
        issue_key = None
        for pred in (predicates or []):
            if pred.column.lower() in ("key", "id") and pred.operator == "=":
                issue_key = pred.value
                break
        
        if not issue_key:
            raise QueryError("DELETE requires 'key' or 'id' in WHERE clause")
        
        url = f"{self._host}/rest/api/3/issue/{issue_key}"
        
        headers = {
            "Accept": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        response = await client.delete(url, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        return 1
    
    def list_tables(self) -> List[str]:
        """List available Jira tables."""
        return list(self.TABLES.keys())
    
    async def list_tables_async(self) -> List[str]:
        """List available Jira tables (async)."""
        return list(self.TABLES.keys())
