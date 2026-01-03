"""
ServiceNow Adapter - Full CRUD support for ServiceNow Table API

Features:
- Dynamic schema discovery from any table
- Predicate pushdown to sysparm_query
- Pagination handling
- Full CRUD operations
"""

from __future__ import annotations
import json
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

import requests
import httpx
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError, RateLimitError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate


class ServiceNowAdapter(BaseAdapter):
    """
    ServiceNow Table API adapter.
    
    Supports querying any ServiceNow table dynamically.
    """
    
    adapter_name = "servicenow"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    supports_batch = True
    
    # ServiceNow type to Arrow type mapping
    TYPE_MAP = {
        "string": pa.string(),
        "integer": pa.int64(),
        "boolean": pa.bool_(),
        "decimal": pa.float64(),
        "float": pa.float64(),
        "glide_date": pa.string(),  # Keep as string for now
        "glide_date_time": pa.string(),
        "reference": pa.string(),
        "sys_id": pa.string(),
    }
    
    def __init__(
        self,
        host: str,
        auth_manager=None,
        schema_cache=None,
        page_size: int = 1000,
        max_parallel: int = 4,
        timeout: int = 30,
        display_value: str | bool = False,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        # Normalize host
        self._host = host.rstrip("/")
        if not self._host.startswith("http"):
            self._host = f"https://{self._host}"
        
        self._page_size = page_size
        self._max_parallel = max_parallel
        self._timeout = timeout
        self._display_value = display_value
        # Note: HTTP sessions are now managed by the connection pool in BaseAdapter
        # Use self._get_session() context manager or self._get_session_direct() for requests
        
        # Initialize parallel fetcher for high-throughput data retrieval
        from waveql.utils.streaming import ParallelFetcher
        self._parallel_fetcher = ParallelFetcher(
            max_workers=max_parallel,
            batch_size=page_size,
        )
    
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
        """Fetch data from ServiceNow table."""
        # 0. Handle Virtual Tables (Attachments)
        table_name = self._extract_table_name(table)
        if table_name == "sys_attachment_content":
            return self._fetch_attachment_content(predicates)

        if bool(group_by or aggregates):
            return self._fetch_stats(table, predicates, group_by, aggregates, order_by, limit)

        # 1. Resolve columns
        # If columns is None/empty/star, we fetch all returned fields (or explicit sysparm_fields if desired)
        # ServiceNow returns all fields by default if sysparm_fields is not set.
        # However, for schema consistency, we might want to discover schema first if wildcard.
        if not columns or columns == ["*"]:
            query_cols = []  # Empty means all
        else:
            query_cols = columns
        
        # Build URL and params
        table_name = self._extract_table_name(table)
        url = f"{self._host}/api/now/table/{table_name}"
        
        params = self._build_query_params(columns, predicates, limit, offset, order_by)
        
        # Fetch data (with pagination if needed)
        if limit and limit <= self._page_size:
            # Single request
            records = self._fetch_page(url, params)
        else:
            # Paginated fetch
            records = self._fetch_all_pages(url, params, limit)
        
        # Discover/use cached schema
        schema_columns = self._get_or_discover_schema(table_name, records)
        
        # Convert to Arrow
        return self._to_arrow(records, schema_columns, columns)
    
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
        """Fetch data from ServiceNow table (async)."""
        table_name = self._extract_table_name(table)
        if table_name == "sys_attachment_content":
            return await self._fetch_attachment_content_async(predicates)

        if bool(group_by or aggregates):
            return await self._fetch_stats_async(table, predicates, group_by, aggregates, order_by, limit)

        if not columns or columns == ["*"]:
            query_cols = []
        else:
            query_cols = columns
        
        url = f"{self._host}/api/now/table/{table_name}"
        params = self._build_query_params(columns, predicates, limit, offset, order_by)
        
        if limit and limit <= self._page_size:
            records = await self._fetch_page_async(url, params)
        else:
            # Note: ParallelFetcher is not async-native yet, 
            # so for now we just fetch sequentially or implement a simple async loop
            records = await self._fetch_all_pages_async(url, params, limit)
        
        schema_columns = await self._get_or_discover_schema_async(table_name, records)
        return self._to_arrow(records, schema_columns, columns)

    def _extract_table_name(self, table: str) -> str:
        """Extract table name from schema.table format and strip quotes."""
        if not table:
            return table
        if "." in table:
            table = table.rsplit(".", 1)[1]
        return table.strip('"')

    def _clean_column_name(self, col: str) -> str:
        """
        Clean a column name by stripping quotes and table prefixes/aliases.
        """
        if not col or col == "*":
            return col
        if "." in col:
            col = col.rsplit(".", 1)[1]
        return col.strip('"')
    
    def _build_query_params(
        self,
        columns: List[str],
        predicates: List["Predicate"],
        limit: int,
        offset: int,
        order_by: List[tuple],
    ) -> Dict[str, str]:
        """Build ServiceNow query parameters."""
        params = {}
        
        # Readable Labels
        if self._display_value:
            params["sysparm_display_value"] = str(self._display_value).lower()

        # Column selection
        if columns and columns != ["*"]:
            params["sysparm_fields"] = ",".join(self._clean_column_name(c) for c in columns)
        
        # Predicate pushdown
        if predicates:
            query_parts = []
            for pred in predicates:
                sql_pred = self._predicate_to_query(pred)
                if sql_pred:
                    query_parts.append(sql_pred)
            if query_parts:
                params["sysparm_query"] = "^".join(query_parts)
        
        # Pagination
        if limit:
            params["sysparm_limit"] = str(min(limit, self._page_size))
        else:
            params["sysparm_limit"] = str(self._page_size)
        
        if offset:
            params["sysparm_offset"] = str(offset)
        
        # Order by
        if order_by:
            order_parts = []
            for col, direction in order_by:
                prefix = "" if direction == "ASC" else "DESC"
                order_parts.append(f"{prefix}{self._clean_column_name(col)}")
            params["sysparm_query"] = params.get("sysparm_query", "") + \
                                      ("^" if params.get("sysparm_query") else "") + \
                                      f"ORDERBY{','.join(order_parts)}"
        
        return params
    
    def _predicate_to_query(self, pred: "Predicate") -> str:
        """Convert predicate to ServiceNow query syntax."""
        col = self._clean_column_name(pred.column)
        op = pred.operator
        val = pred.value
        
        # ServiceNow query operators
        op_map = {
            "=": "=",
            "!=": "!=",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
            "LIKE": "LIKE",
            "IN": "IN",
            "IS NULL": "ISEMPTY",
            "IS NOT NULL": "ISNOTEMPTY",
        }
        
        sn_op = op_map.get(op, "=")
        
        if op in ("IS NULL", "IS NOT NULL"):
            return f"{col}{sn_op}"
        elif op == "LIKE":
            # Convert SQL LIKE to ServiceNow LIKE (contains)
            # Strip % wildcards as ServiceNow LIKE is a simple contains
            clean_val = str(val).strip("%")
            return f"{col}LIKE{clean_val}"
        elif op == "IN":
            # ServiceNow IN syntax
            if isinstance(val, (list, tuple)):
                return f"{col}IN{','.join(str(v) for v in val)}"
            return f"{col}IN{val}"
        else:
            return f"{col}{sn_op}{val}"
    
    async def _fetch_page_async(self, url: str, params: Dict) -> List[Dict]:
        """Fetch a single page of results (async)."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        
        async def do_request():
            response = await client.get(url, params=params, headers=headers, timeout=self._timeout)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
            
            response.raise_for_status()
            return response.json()
        
        try:
            data = await self._rate_limiter.execute_with_retry_async(do_request)
            return data.get("result", [])
        except httpx.HTTPError as e:
            raise AdapterError(f"ServiceNow request failed (async): {e}")

    def _fetch_page(self, url: str, params: Dict) -> List[Dict]:
        """Fetch a single page of results with automatic retry on rate limits."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        with self._get_session() as session:
            def do_request():
                response = session.get(
                    url, params=params, headers=headers, timeout=self._timeout
                )
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                
                response.raise_for_status()
                return response.json()
            
            try:
                # Use rate limiter for automatic retry
                data = self._rate_limiter.execute_with_retry(do_request)
                return data.get("result", [])
                
            except RateLimitError:
                raise  # Re-raise after all retries exhausted
            except requests.RequestException as e:
                raise AdapterError(f"ServiceNow request failed: {e}")
    
    async def _fetch_all_pages_async(self, url: str, params: Dict, limit: int = None) -> List[Dict]:
        """Fetch all pages asynchronously (simple loop for now)."""
        all_records = []
        page_size = int(params.get("sysparm_limit", self._page_size))
        
        offset = 0
        while True:
            page_params = {**params, "sysparm_offset": str(offset), "sysparm_limit": str(page_size)}
            records = await self._fetch_page_async(url, page_params)
            all_records.extend(records)
            
            if len(records) < page_size:
                break
                
            offset += page_size
            if limit and offset >= limit:
                break
        
        return all_records[:limit] if limit else all_records

    def _fetch_all_pages(self, url: str, params: Dict, limit: int = None) -> List[Dict]:
        """Fetch all pages using ParallelFetcher for high-throughput retrieval."""
        all_records = []
        page_size = int(params.get("sysparm_limit", self._page_size))
        
        # 1. Fetch first page sequentially to check if we even need parallel
        first_params = {**params, "sysparm_offset": "0"}
        first_page = self._fetch_page(url, first_params)
        all_records.extend(first_page)
        
        # If first page is not full, we are done
        if len(first_page) < page_size:
            return all_records
            
        # 2. Need more pages - use ParallelFetcher starting from page 1
        def fetch_page_by_number(page_num: int) -> List[Dict]:
            """Fetch a specific page by number."""
            page_params = {**params, "sysparm_offset": str(page_num * page_size)}
            return self._fetch_page(url, page_params)
        
        result_table = self._parallel_fetcher.fetch_parallel(
            fetch_func=fetch_page_by_number,
            total_pages=None,
            stop_on_empty=True,
            start_page=1, # Start from page 1
        )
        
        # Combine first page with parallel results
        if len(result_table) > 0:
            all_records.extend(result_table.to_pylist())
        
        return all_records[:limit] if limit else all_records
    
    async def _get_or_discover_schema_async(self, table: str, records: List[Dict]) -> List[ColumnInfo]:
        """Get cached schema or discover from response (async)."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        if not records:
            # If no records, we try to discover by fetching one record (async)
            return await self.get_schema_async(table)
        
        columns = []
        sample = records[0]
        for key, value in sample.items():
            col_type = self._infer_type(value)
            columns.append(ColumnInfo(
                name=key,
                data_type=col_type,
                nullable=True,
            ))
        
        self._cache_schema(table, columns)
        return columns

    def _get_or_discover_schema(self, table: str, records: List[Dict]) -> List[ColumnInfo]:
        """Get cached schema or discover from response."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        # Discover from first record
        if not records:
            return []
        
        columns = []
        sample = records[0]
        for key, value in sample.items():
            col_type = self._infer_type(value)
            columns.append(ColumnInfo(
                name=key,
                data_type=col_type,
                nullable=True,
            ))
        
        # Cache the schema
        self._cache_schema(table, columns)
        return columns
    
    def _infer_type(self, value: Any) -> str:
        """Infer data type from value."""
        if value is None:
            return "string"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "float"
        return "string"
    
    def _to_arrow(
        self,
        records: List[Dict],
        schema_columns: List[ColumnInfo],
        selected_columns: List[str] = None,
    ) -> pa.Table:
        """Convert records to Arrow table."""
        if not records:
            # Return empty table with schema
            fields = [
                pa.field(c.name, self.TYPE_MAP.get(c.data_type, pa.string()))
                for c in schema_columns
            ]
            return pa.table({f.name: [] for f in fields})
        
        # Build columns dict
        columns_data = {}
        for col in schema_columns:
            if selected_columns and selected_columns != ["*"] and col.name not in selected_columns:
                continue
            
            values = [record.get(col.name) for record in records]
            arrow_type = self.TYPE_MAP.get(col.data_type, pa.string())
            
            # Convert to Arrow array
            try:
                columns_data[col.name] = pa.array(values, type=arrow_type)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # Fall back to string
                columns_data[col.name] = pa.array([str(v) if v is not None else None for v in values])
        
        return pa.table(columns_data)
    
    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Discover schema by fetching one record (async)."""
        table_name = self._extract_table_name(table)
        cached = self._get_cached_schema(table_name)
        if cached:
            return cached
        
        url = f"{self._host}/api/now/table/{table_name}"
        params = {"sysparm_limit": "1"}
        records = await self._fetch_page_async(url, params)
        
        return await self._get_or_discover_schema_async(table_name, records)

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Discover schema by fetching one record."""
        table_name = self._extract_table_name(table)
        
        # Check cache first
        cached = self._get_cached_schema(table_name)
        if cached:
            return cached
        
        # Fetch one record to discover schema
        url = f"{self._host}/api/now/table/{table_name}"
        params = {"sysparm_limit": "1"}
        records = self._fetch_page(url, params)
        
        return self._get_or_discover_schema(table_name, records)
    
    async def insert_async(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Insert a record into ServiceNow (async)."""
        table_name = self._extract_table_name(table)
        url = f"{self._host}/api/now/table/{table_name}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        client = self._get_async_client()
        response = await client.post(url, json=values, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        return 1

    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Insert a record into ServiceNow."""
        table_name = self._extract_table_name(table)
        url = f"{self._host}/api/now/table/{table_name}"
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.post(
                    url, json=values, headers=headers, timeout=self._timeout
                )
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"INSERT failed: {e}")
    
    async def update_async(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update records in ServiceNow (async)."""
        table_name = self._extract_table_name(table)
        sys_id = None
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id" and pred.operator == "=":
                sys_id = pred.value
                break
        if not sys_id:
            raise QueryError("UPDATE requires sys_id in WHERE clause")
        
        url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        client = self._get_async_client()
        response = await client.patch(url, json=values, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        return 1

    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update records in ServiceNow."""
        table_name = self._extract_table_name(table)
        
        # Get sys_id from predicates
        sys_id = None
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id" and pred.operator == "=":
                sys_id = pred.value
                break
        
        if not sys_id:
            raise QueryError("UPDATE requires sys_id in WHERE clause")
        
        url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        try:
            with self._get_session() as session:
                response = session.patch(
                    url, json=values, headers=headers, timeout=self._timeout
                )
                response.raise_for_status()
                return 1
        except requests.RequestException as e:
            raise QueryError(f"UPDATE failed: {e}")
    
    async def delete_async(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete a record from ServiceNow (async)."""
        table_name = self._extract_table_name(table)
        sys_id = None
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id" and pred.operator == "=":
                sys_id = pred.value
                break
        if not sys_id:
            raise QueryError("DELETE requires sys_id in WHERE clause")
        
        url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
        headers = {
            "Accept": "application/json",
            **await self._get_auth_headers_async(),
        }
        client = self._get_async_client()
        response = await client.delete(url, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        return 1

    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete a record from ServiceNow."""
        table_name = self._extract_table_name(table)
        
        # Get sys_id from predicates
        sys_id = None
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id" and pred.operator == "=":
                sys_id = pred.value
                break
        
        if not sys_id:
            raise QueryError("DELETE requires sys_id in WHERE clause")
        
        url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
        
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
    
    async def list_tables_async(self) -> List[str]:
        """List available ServiceNow tables (async)."""
        try:
            records = await self.fetch_async(
                "sys_db_object",
                columns=["name", "label"],
                limit=1000,
            )
            return [row["name"] for row in records.to_pylist()]
        except Exception:
            return []

    def list_tables(self) -> List[str]:
        """List available ServiceNow tables (from sys_db_object)."""
        try:
            records = self.fetch(
                "sys_db_object",
                columns=["name", "label"],
                limit=1000,
            )
            return [row["name"] for row in records.to_pylist()]
        except Exception:
            return []
    
    async def _fetch_stats_async(self, table, predicates, group_by, aggregates, order_by, limit) -> pa.Table:
        """Fetch aggregation statistics (async)."""
        url = f"{self._host}/api/now/stats/{self._extract_table_name(table)}"
        params = self._build_stats_params(predicates, group_by, aggregates, order_by)
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        response = await client.get(url, params=params, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        data = response.json()
            
        result = data.get("result", [])
        return self._process_stats_result(result, limit, aggregates)

    def _build_stats_params(self, predicates, group_by, aggregates, order_by) -> Dict:
        """Helper to build stats params (moved out of _fetch_stats)."""
        params = {}
        if predicates:
            query_parts = [self._predicate_to_query(p) for p in predicates]
            params["sysparm_query"] = "^".join(filter(None, query_parts))
        if group_by:
            params["sysparm_group_by"] = ",".join(group_by)
        if aggregates:
             for agg in aggregates:
                 func = agg.func.upper()
                 col = agg.column
                 if func == "COUNT": params["sysparm_count"] = "true"
                 elif func == "SUM": params["sysparm_sum_fields"] = params.get("sysparm_sum_fields", "") + ("," if "sysparm_sum_fields" in params else "") + col
                 elif func == "AVG": params["sysparm_avg_fields"] = params.get("sysparm_avg_fields", "") + ("," if "sysparm_avg_fields" in params else "") + col
                 elif func == "MIN": params["sysparm_min_fields"] = params.get("sysparm_min_fields", "") + ("," if "sysparm_min_fields" in params else "") + col
                 elif func == "MAX": params["sysparm_max_fields"] = params.get("sysparm_max_fields", "") + ("," if "sysparm_max_fields" in params else "") + col
        if order_by:
            cols = [self._clean_column_name(col) for col, _ in order_by]
            params["sysparm_order_by"] = ",".join(cols)
        return params

    def _process_stats_result(self, result: Any, limit: int = None, aggregates: List[Any] = None) -> pa.Table:
        """Helper to process stats result JSON (moved out of _fetch_stats)."""
        if isinstance(result, dict):
            result = [result]
        rows = []
        for item in result:
            stats = item.get("stats", {})
            row = {}
            for grp in item.get("groupby_fields", []):
                row[grp["field"]] = grp["value"]
            if "count" in stats:
                # Find alias for COUNT if exists
                alias = "count"
                if aggregates:
                    for agg in aggregates:
                        if agg.func.upper() == "COUNT":
                            alias = agg.alias or f"COUNT({agg.column})"
                            break
                row[alias] = int(stats["count"])
            for agg_type in ["sum", "avg", "min", "max"]:
                if agg_type in stats:
                    for field, val in stats[agg_type].items():
                        # Find alias
                        alias = f"{agg_type.upper()}({field})"
                        if aggregates:
                            for agg in aggregates:
                                if agg.func.upper() == agg_type.upper() and self._clean_column_name(agg.column) == self._clean_column_name(field):
                                    if agg.alias:
                                        alias = agg.alias
                                    break
                        
                        try:
                            row[alias] = float(val) if val else None
                        except ValueError:
                             row[alias] = val
            rows.append(row)
        if limit and rows:
            rows = rows[:limit]
        if not rows:
            return pa.Table.from_pylist([])
        return pa.Table.from_pylist(rows)

    def _fetch_stats(self, table, predicates, group_by, aggregates, order_by, limit) -> pa.Table:
        """Fetch aggregation statistics (sync)."""
        url = f"{self._host}/api/now/stats/{self._extract_table_name(table)}"
        params = self._build_stats_params(predicates, group_by, aggregates, order_by)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        with self._get_session() as session:
            response = session.get(url, params=params, headers=headers)
            response.raise_for_status()
            return self._process_stats_result(response.json().get("result", []), limit, aggregates)

    async def _fetch_attachment_content_async(self, predicates: List["Predicate"]) -> pa.Table:
        """Fetch binary content from the Attachment API (async)."""
        sys_id = None
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id" and pred.operator == "=":
                sys_id = pred.value
                break
        if not sys_id:
            raise QueryError("Fetching attachment content requires 'sys_id' in WHERE clause")

        url = f"{self._host}/api/now/attachment/{sys_id}/file"
        headers = {**await self._get_auth_headers_async()}
        client = self._get_async_client()
        response = await client.get(url, headers=headers, timeout=self._timeout)
        response.raise_for_status()
        content = response.content
        return pa.Table.from_pylist([{"sys_id": sys_id, "content": content}])

    def _fetch_attachment_content(self, predicates: List["Predicate"]) -> pa.Table:
        """Fetch binary content from the Attachment API."""
        # Get sys_id from predicates
        sys_id = None
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id" and pred.operator == "=":
                sys_id = pred.value
                break
        
        if not sys_id:
            raise QueryError("Fetching attachment content requires 'sys_id' in WHERE clause")

        url = f"{self._host}/api/now/attachment/{sys_id}/file"
        headers = {**self._get_auth_headers()}
        
        with self._get_session() as session:
            response = session.get(url, headers=headers, timeout=self._timeout)
            response.raise_for_status()
            
            # Return as an Arrow table with a binary column
            content = response.content
            return pa.Table.from_pylist([{"sys_id": sys_id, "content": content}])
