"""
ServiceNow Adapter - Full CRUD support for ServiceNow Table API

Features:
- Dynamic schema discovery from any table
- Predicate pushdown to sysparm_query
- Pagination handling
- Full CRUD operations
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

import httpx
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError, RateLimitError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate

logger = logging.getLogger(__name__)


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
    
    # Configuration defaults (can be overridden in __init__)
    DEFAULT_PAGE_SIZE = 1000
    DEFAULT_MAX_PARALLEL = 4
    DEFAULT_TIMEOUT = 30
    DEFAULT_SCHEMA_TTL = 3600  # 1 hour
    DEFAULT_LIST_TABLES_LIMIT = 1000
    
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
        page_size: int = None,
        max_parallel: int = None,
        timeout: int = None,
        display_value: str | bool = False,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        # Normalize host
        self._host = host.rstrip("/")
        if not self._host.startswith("http"):
            self._host = f"https://{self._host}"
        
        # Use class defaults if not provided
        self._page_size = page_size if page_size is not None else self.DEFAULT_PAGE_SIZE
        self._max_parallel = max_parallel if max_parallel is not None else self.DEFAULT_MAX_PARALLEL
        self._timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT
        self._display_value = display_value
        # Note: HTTP sessions are now managed by the connection pool in BaseAdapter
        # Use self._get_session() context manager or self._get_session_direct() for requests
        
        # Initialize parallel fetcher for high-throughput data retrieval
        from waveql.utils.streaming import ParallelFetcher
        self._parallel_fetcher = ParallelFetcher(
            max_workers=self._max_parallel,
            batch_size=self._page_size,
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

        # Note: If columns is None/empty/star, we fetch all returned fields.
        # ServiceNow returns all fields by default if sysparm_fields is not set.
        
        # Build URL and params
        table_name = self._extract_table_name(table)
        url = f"{self._host}/api/now/table/{table_name}"
        
        params = self._build_query_params(columns, predicates, limit, offset, order_by, table_name)
        
        # Log the API query for observability
        logger.debug(
            "ServiceNow query: table=%s, sysparm_query=%s, sysparm_fields=%s",
            table_name,
            params.get("sysparm_query", ""),
            params.get("sysparm_fields", "*"),
        )
        
        # Fetch data (with pagination if needed)
        # HTTPX client is now standard
        records = self._fetch_all_pages(url, params, limit)
        
        # Discover/use cached schema
        schema_columns = self._get_or_discover_schema(table_name, records)
        if not schema_columns and not records:
             # If no records and no schema found (and no fallback), we might have issues
             # But let's try to get schema even if no records found
             schema_columns = self._get_or_discover_schema(table_name, [])

        # Convert to Arrow
        table = self._to_arrow(records, schema_columns, columns)
        
        # Attach execution metadata for observability
        if "sysparm_query" in params:
            table = table.replace_schema_metadata({
                b"waveql_source_query": params["sysparm_query"].encode("utf-8")
            })
            
        return table

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

        url = f"{self._host}/api/now/table/{table_name}"
        params = self._build_query_params(columns, predicates, limit, offset, order_by, table_name)
        
        if limit and limit <= self._page_size:
            records = await self._fetch_page_async(url, params)
        else:
            records = await self._fetch_all_pages_async(url, params, limit)
        
        schema_columns = await self._get_or_discover_schema_async(table_name, records)
        if not schema_columns and not records:
             schema_columns = await self._get_or_discover_schema_async(table_name, [])

        table = self._to_arrow(records, schema_columns, columns)
        
        if "sysparm_query" in params:
            table = table.replace_schema_metadata({
                b"waveql_source_query": params["sysparm_query"].encode("utf-8")
            })
            
        return table
    
    def _extract_table_name(self, table: str) -> str:
        """Extract table name from schema.table format and strip quotes."""
        if not table:
            return table
        if "." in table:
            table = table.rsplit(".", 1)[1]
        return table.strip('"')

    def _clean_column_name(self, col: str, table_name: str = None) -> str:
        """
        Clean a column name by stripping quotes and table prefixes/aliases.
        Preserves dot-walking (e.g., 'assigned_to.email') unless it matches
        the query table name.
        """
        if not col or col == "*":
            return col
            
        # Standardize quotes
        col = col.strip('"')
        
        if "." in col:
            parts = col.split(".")
            # If the first part matches the current table, it's an alias -> remove it
            if table_name and parts[0] == table_name:
                return ".".join(parts[1:])
            
            # Otherwise, assume it's a valid dot-walk reference field -> keep it
            # e.g. "super_class.name" or "caller_id.email"
            return col
            
        return col

    def _build_query_params(
        self,
        columns: List[str],
        predicates: List["Predicate"],
        limit: int,
        offset: int,
        order_by: List[tuple],
        table_name: str = None, 
    ) -> Dict[str, str]:
        """Build ServiceNow query parameters."""
        params = {}
        
        # Readable Labels
        if self._display_value:
            params["sysparm_display_value"] = str(self._display_value).lower()

        # Column selection
        if columns and columns != ["*"]:
            params["sysparm_fields"] = ",".join(
                self._clean_column_name(c, table_name) for c in columns
            )
        
        # Predicate pushdown
        if predicates:
            query_parts = []
            for pred in predicates:
                sql_pred = self._predicate_to_query(pred, table_name)
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
                order_parts.append(f"{prefix}{self._clean_column_name(col, table_name)}")
            params["sysparm_query"] = params.get("sysparm_query", "") + \
                                      ("^" if params.get("sysparm_query") else "") + \
                                      f"ORDERBY{','.join(order_parts)}"
        
        return params

    def _predicate_to_query(self, pred: "Predicate", table_name: str = None) -> str:
        """Convert predicate to ServiceNow query syntax."""
        col = self._clean_column_name(pred.column, table_name)
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
    
    def _arrow_type_to_string(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to string representation for legacy compatibility."""
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
        """Convert records to Arrow table with native struct support."""
        # Process records to extract display values from reference fields
        if self._display_value and records:
            records = self._extract_display_values(records)
        
        if not records:
            # Return empty table with schema
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
    
    def _extract_display_values(self, records: List[Dict]) -> List[Dict]:
        """Extract display_value from reference field dicts when display_value mode is enabled."""
        processed = []
        for record in records:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, dict) and 'display_value' in value:
                    # Extract just the display value from reference fields
                    processed_record[key] = value['display_value']
                else:
                    processed_record[key] = value
            processed.append(processed_record)
        return processed

    async def _fetch_all_pages_async(self, url: str, params: Dict, limit: int = None) -> List[Dict]:
        """Fetch all pages asynchronously with parallel requests."""
        import anyio
        
        page_size = int(params.get("sysparm_limit", self._page_size))
        
        # First, fetch the initial page
        first_page_params = {**params, "sysparm_offset": "0", "sysparm_limit": str(page_size)}
        first_page = await self._fetch_page_async(url, first_page_params)
        
        if not first_page:
            return first_page[:limit] if limit else first_page
        
        all_records = list(first_page)
        
        # Calculate pages
        max_pages = min(self._max_parallel, 10)
        if limit:
            remaining = limit - len(all_records)
            estimated_pages = (remaining + page_size - 1) // page_size
            max_pages = min(max_pages, estimated_pages)
        
        # Fetch remaining pages in parallel batches
        offset = page_size
        while True:
            batch_offsets = []
            for i in range(max_pages):
                if limit and offset >= limit:
                    break
                batch_offsets.append(offset)
                offset += page_size
            
            if not batch_offsets:
                break
            
            async def fetch_offset(off: int) -> List[Dict]:
                page_params = {**params, "sysparm_offset": str(off), "sysparm_limit": str(page_size)}
                return await self._fetch_page_async(url, page_params)
            
            results = []
            try:
                async with anyio.create_task_group() as tg:
                    async def fetch_and_store(off: int, idx: int):
                        result = await fetch_offset(off)
                        results.append((idx, result))
                    
                    for idx, off in enumerate(batch_offsets):
                        tg.start_soon(fetch_and_store, off, idx)
            except Exception as e:
                # Fallback to sequential if task group fails (e.g. nested loop issues)
                logger.warning(f"Parallel fetch failed, falling back to sequential: {e}")
                for idx, off in enumerate(batch_offsets):
                    res = await fetch_offset(off)
                    results.append((idx, res))
            
            results.sort(key=lambda x: x[0])
            
            found_partial = False
            for _, records in results:
                all_records.extend(records)
                # Only break if we get NO records, to handle ACL filtering "short page" issues
                if not records:
                    found_partial = True
                    break
            
            if found_partial:
                break
            
            if limit and len(all_records) >= limit:
                break
        
        return all_records[:limit] if limit else all_records
    
    def _fetch_all_pages(self, url: str, params: Dict, limit: int = None) -> List[Dict]:
        """Fetch all pages sequentially until a partial page is returned."""
        all_records = []
        page_size = int(params.get("sysparm_limit", self._page_size))
        offset = int(params.get("sysparm_offset", 0))
        
        # Use a single client for session persistence
        with httpx.Client(timeout=self._timeout) as client:
            while True:
                page_params = {**params, "sysparm_offset": str(offset)}
                records = self._fetch_page(url, page_params, client)
                all_records.extend(records)
                
                # Stop if NO records returned (fixes ACL short-page issue)
                if not records:
                    break
                    
                offset += page_size
                if limit and len(all_records) >= limit:
                    break
        
        return all_records[:limit] if limit else all_records

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

    
    def _fetch_page(self, url: str, params: Dict, client: httpx.Client) -> List[Dict]:
        """Fetch a single page of results with automatic retry on rate limits."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        def do_request():
            response = client.get(
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
        except httpx.HTTPError as e:
            raise AdapterError(f"ServiceNow request failed: {e}")

            
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


        self._cache_schema(table, columns)
        return columns

    async def _get_or_discover_schema_async(self, table: str, records: List[Dict]) -> List[ColumnInfo]:
        """Get cached schema or discover from response (async)."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
            
        # Try metadata first
        columns = await self._fetch_schema_from_metadata_async(table)
        if columns:
            self._cache_schema(table, columns)
            return columns
        
        if not records:
            # Avoid infinite recursion if get_schema_async calls this
            # return await self.get_schema_async(table)
            return []
        
        from waveql.utils.schema import infer_schema_from_records
        arrow_schema = infer_schema_from_records(records, sample_size=5)
        
        columns = []
        for field in arrow_schema:
            columns.append(ColumnInfo(
                name=field.name,
                data_type=self._arrow_type_to_string(field.type),
                nullable=True,
                primary_key=field.name == "sys_id",
                arrow_type=field.type,
            ))
        
        self._cache_schema(table, columns)
        return columns

        return columns

    def _get_or_discover_schema(self, table: str, records: List[Dict]) -> List[ColumnInfo]:
        """Get cached schema or discover from response."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
            
        # Try metadata first
        columns = self._fetch_schema_from_metadata(table)
        if columns:
            self._cache_schema(table, columns)
            return columns
        
        # Discover from records using multi-sample inference
        if not records:
            return []
        
        # Use new schema inference utility for robust multi-sample detection
        from waveql.utils.schema import infer_schema_from_records
        
        arrow_schema = infer_schema_from_records(records, sample_size=5)
        
        # Convert Arrow schema to ColumnInfo for caching
        columns = []
        for field in arrow_schema:
            # Store the Arrow type directly in data_type for struct support
            columns.append(ColumnInfo(
                name=field.name,
                data_type=self._arrow_type_to_string(field.type),
                nullable=True,
                primary_key=field.name == "sys_id", # Best guess for inference
                # Store the actual Arrow type for _to_arrow
                arrow_type=field.type,
            ))
        
        # Cache the schema
        self._cache_schema(table, columns)
        return columns

    
    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Discover schema by fetching metadata or one record (async)."""
        table_name = self._extract_table_name(table)
        cached = self._get_cached_schema(table_name)
        if cached:
            return cached
        
        # Try metadata first
        columns = await self._fetch_schema_from_metadata_async(table_name)
        if columns:
            self._cache_schema(table_name, columns)
            return columns

        url = f"{self._host}/api/now/table/{table_name}"
        params = {"sysparm_limit": "1"}
        records = await self._fetch_page_async(url, params)
        
        return await self._get_or_discover_schema_async(table_name, records)

    def _get_table_hierarchy(self, table_name: str) -> List[str]:
        """Resolve table hierarchy (e.g. sc_req_item -> task)."""
        hierarchy = [table_name]
        current_table = table_name
        
        # Limit depth to avoid infinite loops
        for _ in range(5):
            try:
                url = f"{self._host}/api/now/table/sys_db_object"
                params = {
                    "sysparm_query": f"name={current_table}",
                    "sysparm_fields": "super_class.name",
                    "sysparm_limit": "1"
                }
                
                with httpx.Client(timeout=self._timeout) as client:
                    data = self._fetch_page(url, params, client)
                    
                if not data:
                    break
                    
                parent = data[0].get("super_class.name")
                if not parent:
                    break
                    
                hierarchy.append(parent)
                current_table = parent
            except Exception:
                break
                
        return hierarchy

    def _fetch_schema_from_metadata(self, table_name: str) -> Optional[List[ColumnInfo]]:
        """Fetch schema from sys_dictionary utilizing table hierarchy."""
        try:
            # 1. Resolve hierarchy to include inherited fields
            tables = self._get_table_hierarchy(table_name)
            tables_str = ",".join(tables)
            
            # 2. Query sys_dictionary for all tables in hierarchy
            url = f"{self._host}/api/now/table/sys_dictionary"
            params = {
                "sysparm_query": f"nameIN{tables_str}",
                "sysparm_fields": "element,internal_type,mandatory,primary,attributes,default_value,read_only",
                "sysparm_limit": "2000" # Increase limit for combined fields
            }
            
            with httpx.Client(timeout=self._timeout) as client:
                data = self._fetch_page(url, params, client)
            
            if not data:
                return None
            
            # 3. Process columns, deduplicating by name (child overrides parent usually, but here just unique)
            columns_map = {}
            for row in data:
                name = row.get("element")
                if not name: continue 
                
                # If we already have this column (e.g. from child), skip or merge?
                # Usually we want the definition from the most specific table, but sys_dictionary 
                # often defines it once. We'll simply take the first one seen or overwrite.
                # Given logic order is usually arbitrary unless sorted, we'll store all and dedupe.
                if isinstance(name, dict):
                    name = name.get("value", str(name))
                
                if name in columns_map:
                    continue

                internal_type = row.get("internal_type", "string")
                if isinstance(internal_type, dict):
                    internal_type = internal_type.get("value", str(internal_type))
                
                is_mandatory = str(row.get("mandatory")).lower() == "true"
                is_primary = str(row.get("primary")).lower() == "true" or name == "sys_id"
                is_read_only = str(row.get("read_only")).lower() == "true"
                
                default_val = row.get("default_value", "")
                if isinstance(default_val, dict):
                    default_val = default_val.get("value", str(default_val))
                    
                is_auto_inc = "javascript:getNextObjNumber" in str(default_val) or "Next Obj Number" in str(default_val)
                
                arrow_type = self.TYPE_MAP.get(internal_type, pa.string())
                
                columns_map[name] = ColumnInfo(
                    name=name,
                    data_type=internal_type,
                    nullable=not is_mandatory,
                    primary_key=is_primary,
                    auto_increment=is_auto_inc,
                    read_only=is_read_only,
                    arrow_type=arrow_type
                )
            
            # Ensure sys_id is present if not found (phantom parent issue)
            if "sys_id" not in columns_map:
                 columns_map["sys_id"] = ColumnInfo(
                    name="sys_id",
                    data_type="guid",
                    nullable=False,
                    primary_key=True,
                    read_only=True,
                    arrow_type=pa.string()
                )

            return list(columns_map.values())

        except Exception as e:
            logger.warning("Failed to fetch metadata from sys_dictionary for %s: %s", table_name, e)
            return None
            
        except Exception as e:
            logger.warning("Failed to fetch metadata from sys_dictionary for %s: %s", table_name, e)
            return None

    async def _get_table_hierarchy_async(self, table_name: str) -> List[str]:
        """Resolve table hierarchy async."""
        hierarchy = [table_name]
        current_table = table_name
        
        client = self._get_async_client()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }

        for _ in range(5):
            try:
                url = f"{self._host}/api/now/table/sys_db_object"
                params = {
                    "sysparm_query": f"name={current_table}",
                    "sysparm_fields": "super_class.name",
                    "sysparm_limit": "1"
                }
                response = await client.get(url, params=params, headers=headers, timeout=self._timeout)
                if response.status_code != 200:
                    break
                    
                data = response.json().get("result", [])
                if not data:
                    break
                    
                parent = data[0].get("super_class.name")
                if not parent:
                    break
                    
                hierarchy.append(parent)
                current_table = parent
            except Exception:
                break
        return hierarchy

    async def _fetch_schema_from_metadata_async(self, table_name: str) -> Optional[List[ColumnInfo]]:
        """Fetch schema from sys_dictionary utilizing table hierarchy (async)."""
        try:
            # 1. Resolve hierarchy
            tables = await self._get_table_hierarchy_async(table_name)
            tables_str = ",".join(tables)
            
            # 2. Query sys_dictionary
            url = f"{self._host}/api/now/table/sys_dictionary"
            params = {
                "sysparm_query": f"nameIN{tables_str}",
                "sysparm_fields": "element,internal_type,mandatory,primary,attributes,default_value,read_only",
                "sysparm_limit": "2000"
            }
            
            client = self._get_async_client()
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                **await self._get_auth_headers_async(),
            }
            
            response = await client.get(url, params=params, headers=headers, timeout=self._timeout)
            if response.status_code != 200:
                logger.warning("Schema API failed (async) %s: %s", response.status_code, response.text)
                return None
            
            data = response.json().get("result", [])
            if not data:
                return None
            
            # 3. Process columns
            columns_map = {}
            for row in data:
                name = row.get("element")
                if not name: continue 
                
                if isinstance(name, dict):
                    name = name.get("value", str(name))
                
                if name in columns_map:
                    continue

                internal_type = row.get("internal_type", "string")
                if isinstance(internal_type, dict):
                    internal_type = internal_type.get("value", str(internal_type))
                
                is_mandatory = str(row.get("mandatory")).lower() == "true"
                is_primary = str(row.get("primary")).lower() == "true" or name == "sys_id"
                is_read_only = str(row.get("read_only")).lower() == "true"
                
                default_val = row.get("default_value", "")
                if isinstance(default_val, dict):
                    default_val = default_val.get("value", str(default_val))
                    
                is_auto_inc = "javascript:getNextObjNumber" in str(default_val) or "Next Obj Number" in str(default_val)
                
                arrow_type = self.TYPE_MAP.get(internal_type, pa.string())
                
                columns_map[name] = ColumnInfo(
                    name=name,
                    data_type=internal_type,
                    nullable=not is_mandatory,
                    primary_key=is_primary,
                    auto_increment=is_auto_inc,
                    read_only=is_read_only,
                    arrow_type=arrow_type
                )
            
            if "sys_id" not in columns_map:
                 columns_map["sys_id"] = ColumnInfo(
                    name="sys_id",
                    data_type="guid",
                    nullable=False,
                    primary_key=True,
                    read_only=True,
                    arrow_type=pa.string()
                )
            
            return list(columns_map.values())
        except Exception as e:
            logger.warning("Failed to fetch metadata from sys_dictionary (async) for %s: %s", table_name, e)
            return None

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
        async def do_insert():
            response = await client.post(url, json=values, headers=headers, timeout=self._timeout)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
            response.raise_for_status()
            return response
        
        try:
            await self._rate_limiter.execute_with_retry_async(do_insert)
            return 1
        except RateLimitError:
            raise
        except httpx.HTTPError as e:
            raise QueryError(f"INSERT failed (async): {e}")

    async def update_async(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update records in ServiceNow (async)."""
        table_name = self._extract_table_name(table)
        sys_ids = []
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id":
                if pred.operator == "=":
                    sys_ids = [pred.value]
                    break
                elif pred.operator == "IN" and isinstance(pred.value, (list, tuple)):
                    sys_ids = list(pred.value)
                    break
        
        if not sys_ids:
            raise QueryError("UPDATE requires sys_id in WHERE clause")
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        updated_count = 0
        
        for sys_id in sys_ids:
            url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
            async def do_update(url=url):
                response = await client.patch(url, json=values, headers=headers, timeout=self._timeout)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                response.raise_for_status()
                return response
            
            try:
                await self._rate_limiter.execute_with_retry_async(do_update)
                updated_count += 1
            except RateLimitError:
                raise
            except httpx.HTTPError as e:
                raise QueryError(f"UPDATE failed for sys_id={sys_id} (async): {e}")
        return updated_count

    async def delete_async(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete records from ServiceNow (async)."""
        table_name = self._extract_table_name(table)
        sys_ids = []
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id":
                if pred.operator == "=":
                    sys_ids = [pred.value]
                    break
                elif pred.operator == "IN" and isinstance(pred.value, (list, tuple)):
                    sys_ids = list(pred.value)
                    break
        
        if not sys_ids:
            raise QueryError("DELETE requires sys_id in WHERE clause")
        
        headers = {
            "Accept": "application/json",
            **await self._get_auth_headers_async(),
        }
        
        client = self._get_async_client()
        deleted_count = 0
        for sys_id in sys_ids:
            url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
            async def do_delete(url=url):
                response = await client.delete(url, headers=headers, timeout=self._timeout)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                response.raise_for_status()
                return response
            
            try:
                await self._rate_limiter.execute_with_retry_async(do_delete)
                deleted_count += 1
            except RateLimitError:
                raise
            except httpx.HTTPError as e:
                raise QueryError(f"DELETE failed for sys_id={sys_id} (async): {e}")
        return deleted_count
    
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
        table = self._process_stats_result(result, limit, aggregates)
        if "sysparm_query" in params:
            table = table.replace_schema_metadata({
                b"waveql_source_query": params["sysparm_query"].encode("utf-8")
            })
        return table

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
        
        with httpx.Client(timeout=self._timeout) as client:
            records = self._fetch_page(url, params, client)
        
        return self._get_or_discover_schema(table_name, records)

    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Insert a record into ServiceNow with rate limiting."""
        table_name = self._extract_table_name(table)
        url = f"{self._host}/api/now/table/{table_name}"
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        with httpx.Client(timeout=self._timeout) as client:
            def do_insert():
                response = client.post(
                    url, json=values, headers=headers, timeout=self._timeout
                )
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                response.raise_for_status()
                return response
            
            try:
                self._rate_limiter.execute_with_retry(do_insert)
                return 1
            except RateLimitError:
                raise
            except httpx.HTTPError as e:
                raise QueryError(f"INSERT failed: {e}")

    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update records in ServiceNow with rate limiting and bulk support."""
        table_name = self._extract_table_name(table)
        
        # Get sys_id(s) from predicates
        sys_ids = []
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id":
                if pred.operator == "=":
                    sys_ids = [pred.value]
                    break
                elif pred.operator == "IN" and isinstance(pred.value, (list, tuple)):
                    sys_ids = list(pred.value)
                    break
        
        if not sys_ids:
            raise QueryError("UPDATE requires sys_id in WHERE clause (use = or IN operator)")
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        
        updated_count = 0
        with httpx.Client(timeout=self._timeout) as client:
            for sys_id in sys_ids:
                url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
                
                def do_update(url=url):
                    response = client.patch(
                        url, json=values, headers=headers
                    )
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                    response.raise_for_status()
                    return response
                
                try:
                    self._rate_limiter.execute_with_retry(do_update)
                    updated_count += 1
                except RateLimitError:
                    raise
                except httpx.HTTPError as e:
                    raise QueryError(f"UPDATE failed for sys_id={sys_id}: {e}")
        
        return updated_count

    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete records from ServiceNow with rate limiting and bulk support."""
        table_name = self._extract_table_name(table)
        
        # Get sys_id(s) from predicates
        sys_ids = []
        for pred in (predicates or []):
            if pred.column.lower() == "sys_id":
                if pred.operator == "=":
                    sys_ids = [pred.value]
                    break
                elif pred.operator == "IN" and isinstance(pred.value, (list, tuple)):
                    sys_ids = list(pred.value)
                    break
        
        if not sys_ids:
            raise QueryError("DELETE requires sys_id in WHERE clause (use = or IN operator)")
        
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }
        
        deleted_count = 0
        with httpx.Client(timeout=self._timeout) as client:
            for sys_id in sys_ids:
                url = f"{self._host}/api/now/table/{table_name}/{sys_id}"
                
                def do_delete(url=url):
                    response = client.delete(url, headers=headers)
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        raise RateLimitError("Rate limit exceeded", retry_after=retry_after)
                    response.raise_for_status()
                    return response
                
                try:
                    self._rate_limiter.execute_with_retry(do_delete)
                    deleted_count += 1
                except RateLimitError:
                    raise
                except httpx.HTTPError as e:
                    raise QueryError(f"DELETE failed for sys_id={sys_id}: {e}")
        
        return deleted_count

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

    def _fetch_stats(self, table, predicates, group_by, aggregates, order_by, limit) -> pa.Table:
        """Fetch aggregation statistics (sync)."""
        url = f"{self._host}/api/now/stats/{self._extract_table_name(table)}"
        params = self._build_stats_params(predicates, group_by, aggregates, order_by)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self._get_auth_headers(),
        }
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            table = self._process_stats_result(response.json().get("result", []), limit, aggregates)
            
            # Attach execution metadata
            if "sysparm_query" in params:
                table = table.replace_schema_metadata({
                    b"waveql_source_query": params["sysparm_query"].encode("utf-8")
                })
                
            return table

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
        
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            
            # Return as an Arrow table with a binary column
            content = response.content
            return pa.Table.from_pylist([{"sys_id": sys_id, "content": content}])
