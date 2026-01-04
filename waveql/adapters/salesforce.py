"""
Salesforce Adapter - Query Salesforce using SOQL via REST API
"""

from __future__ import annotations
import urllib.parse
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
import requests
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError, RateLimitError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate


class SalesforceAdapter(BaseAdapter):
    """
    Salesforce Adapter using the REST API and SOQL.
    
    Features:
    - Translates WaveQL predicates to SOQL
    - Handles pagination via 'nextRecordsUrl'
    - Dynamic schema discovery via SObject Describe
    """
    
    adapter_name = "salesforce"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    
    DEFAULT_API_VERSION = "v57.0"
    
    def __init__(
        self,
        host: str,
        auth_manager=None,
        schema_cache=None,
        api_version: str = None,
        timeout: int = 30,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        self._host = host.rstrip("/")
        if not self._host.startswith("http"):
            self._host = f"https://{self._host}"
            
        self._api_version = api_version or self.DEFAULT_API_VERSION
        self._timeout = timeout
        self._session = requests.Session()
        
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
        """Fetch data using SOQL."""
        is_aggregation = bool(aggregates or group_by)
        
        # 1. Resolve Select Fields
        if is_aggregation:
            select_fields = []
            if group_by:
                select_fields.extend(group_by)
            if aggregates:
                for agg in aggregates:
                    expr = f"{agg.func}({agg.column})"
                    if agg.alias:
                        expr += f" {agg.alias}"
                    select_fields.append(expr)
            if not select_fields:
                # Fallback purely to count() if nothing selected? QueryPlanner usually handles this.
                select_fields = ["Id"] 
        else:
            if not columns or columns == ["*"]:
                schema = self.get_schema(table)
                select_fields = [col.name for col in schema]
            else:
                select_fields = columns
            
        # 2. Build SOQL Query
        soql = f"SELECT {', '.join(select_fields)} FROM {table}"
        
        # Add WHERE clause
        if predicates:
            where_clauses = []
            for pred in predicates:
                clause = self._predicate_to_soql(pred)
                if clause:
                    where_clauses.append(clause)
            
            if where_clauses:
                soql += " WHERE " + " AND ".join(where_clauses)
        
        # Add GROUP BY
        if group_by:
            soql += " GROUP BY " + ", ".join(group_by)
        
        # Add ORDER BY
        if order_by:
            sort_expressions = []
            for col, direction in order_by:
                sort_expressions.append(f"{col} {direction}")
            soql += " ORDER BY " + ", ".join(sort_expressions)
            
        # Add LIMIT/OFFSET
        if limit:
            soql += f" LIMIT {limit}"
        if offset:
            soql += f" OFFSET {offset}"
            
        # 3. Execute Query
        records = self._execute_soql(soql)
        
        # 4. Convert to Arrow
        if is_aggregation:
            schema = self._build_aggregate_schema(table, group_by, aggregates)
            # For aggregations, we don't filter columns by select_fields (which contains SQL expressions)
            # We assume results match schema
            return self._to_arrow(records, schema)
        
        if not records:
             # Need schema to return empty table with correct columns
            schema = self.get_schema(table)
            return self._to_arrow([], schema, select_fields)

        # Infer/Use schema from results or cache
        schema = self.get_schema(table)
        return self._to_arrow(records, schema, select_fields)

    def _build_aggregate_schema(self, table: str, group_by: List[str], aggregates: List[Any]) -> List[ColumnInfo]:
        """Build schema for aggregation result."""
        base_schema = {c.name: c for c in self.get_schema(table)}
        result_schema = []
        
        # 1. Group By Columns
        if group_by:
            for col_name in group_by:
                col_info = base_schema.get(col_name)
                if not col_info:
                     # Could be formula or bad name, default to string
                    result_schema.append(ColumnInfo(col_name, "string"))
                else:
                    result_schema.append(col_info)
                    
        # 2. Aggregates
        if aggregates:
            expr_index = 0
            for agg in aggregates:
                if agg.alias:
                    name = agg.alias
                else:
                    # Salesforce uses expr0, expr1 for unaliased aggregates
                    name = f"expr{expr_index}"
                    expr_index += 1
                
                dtype = "string"
                func = agg.func.upper()
                if func == "COUNT":
                    dtype = "integer"
                elif func in ("SUM", "AVG"):
                    dtype = "float"
                else: # MIN/MAX
                    orig = base_schema.get(agg.column)
                    dtype = orig.data_type if orig else "string"
                    
                result_schema.append(ColumnInfo(name, dtype))
                
        return result_schema

    def _execute_soql(self, soql: str) -> List[Dict]:
        """Execute SOQL query and handle pagination."""
        url = f"{self._host}/services/data/{self._api_version}/query"
        params = {"q": soql}
        all_records = []
        
        while True:
            data = self._request("GET", url, params=params)
            all_records.extend(data.get("records", []))
            
            # Check for next page
            if not data.get("done") and data.get("nextRecordsUrl"):
                # nextRecordsUrl is relative, e.g. "/services/data/v57.0/query/01g..."
                next_url = data["nextRecordsUrl"]
                url = f"{self._host}{next_url}"
                params = None # Params are encoded in the nextRecordsUrl
            else:
                break
                
        return all_records

    def _predicate_to_soql(self, pred: "Predicate") -> Optional[str]:
        """Convert a WaveQL predicate to SOQL syntax."""
        col = pred.column
        op = pred.operator.upper()
        val = pred.value
        
        # Handle string quoting
        def format_val(v):
            if isinstance(v, str):
                # Escape single quotes
                escaped = v.replace("'", "\\'")
                return f"'{escaped}'"
            if v is None:
                return "null"
            if isinstance(v, bool):
                return str(v).lower() # true/false
            return str(v)

        if op == "=":
            return f"{col} = {format_val(val)}"
        elif op == "!=":
            return f"{col} != {format_val(val)}"
        elif op == ">":
            return f"{col} > {format_val(val)}"
        elif op == "<":
            return f"{col} < {format_val(val)}"
        elif op == ">=":
            return f"{col} >= {format_val(val)}"
        elif op == "<=":
            return f"{col} <= {format_val(val)}"
        elif op == "LIKE":
            return f"{col} LIKE {format_val(val)}"
        elif op == "IN":
            if isinstance(val, (list, tuple)):
                values = ", ".join(format_val(v) for v in val)
                return f"{col} IN ({values})"
            return f"{col} IN ({format_val(val)})"
        elif op == "IS NULL":
            return f"{col} = null"
        elif op == "IS NOT NULL":
            return f"{col} != null"
            
        return None

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Discover schema via SObject Describe."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
            
        url = f"{self._host}/services/data/{self._api_version}/sobjects/{table}/describe"
        data = self._request("GET", url)
        
        columns = []
        for field in data.get("fields", []):
            col_name = field["name"]
            sf_type = field["type"]
            
            # Map Salesforce types to generic types
            if sf_type in ("boolean",):
                dtype = "boolean"
            elif sf_type in ("int", "integer"):
                dtype = "integer"
            elif sf_type in ("double", "percent", "currency"):
                dtype = "float"
            elif sf_type in ("date", "datetime"):
                dtype = "timestamp" # or string
            else:
                dtype = "string"
                
            columns.append(ColumnInfo(name=col_name, data_type=dtype, nullable=field["nillable"]))
            
        self._cache_schema(table, columns)
        return columns

    def insert(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        """Insert object."""
        url = f"{self._host}/services/data/{self._api_version}/sobjects/{table}"
        self._request("POST", url, json=values)
        return 1

    def update(self, table: str, values: Dict[str, Any], predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Update object (requires ID)."""
        record_id = self._extract_id(predicates)
        if not record_id:
             raise QueryError("UPDATE requires 'Id' in WHERE clause")
             
        url = f"{self._host}/services/data/{self._api_version}/sobjects/{table}/{record_id}"
        self._request("PATCH", url, json=values)
        return 1

    def delete(self, table: str, predicates: List["Predicate"] = None, parameters: Sequence = None) -> int:
        """Delete object (requires ID)."""
        record_id = self._extract_id(predicates)
        if not record_id:
             raise QueryError("DELETE requires 'Id' in WHERE clause")
             
        url = f"{self._host}/services/data/{self._api_version}/sobjects/{table}/{record_id}"
        self._request("DELETE", url)
        return 1
        
    def _extract_id(self, predicates: List["Predicate"]) -> Optional[str]:
        """Extract 'Id' from predicates."""
        if not predicates:
            return None
        for pred in predicates:
            if pred.column.lower() == "id" and pred.operator == "=":
                return pred.value
        return None

    def _request(self, method: str, url: str, **kwargs) -> Any:
        """Perform authenticated request with rate limit handling."""
        headers = kwargs.pop("headers", {})
        request_headers = {
             "Accept": "application/json",
             "Content-Type": "application/json",
             **self._get_auth_headers()
        }
        request_headers.update(headers) # Allow overriding defaults
        headers = request_headers
        
        def do_req():
            resp = self._session.request(method, url, headers=headers, timeout=self._timeout, **kwargs)
            if resp.status_code == 429:
                 # Salesforce doesn't standardly send Retry-After in a header, but we check anyway
                 # Or check body
                 raise RateLimitError("Salesforce rate limit exceeded")
            
            # Handle list of errors in body for 400s
            if not resp.ok:
                try:
                    err_body = resp.json()
                    if isinstance(err_body, list) and len(err_body) > 0:
                        msg = err_body[0].get("message", resp.text)
                        raise AdapterError(f"Salesforce error: {msg}")
                except ValueError:
                    pass
                resp.raise_for_status()
                
            if resp.status_code in (201, 204): # No content or Created (often empty)
                return None
            return resp.json()

        return self._request_with_retry(do_req)

    def insert_bulk(self, table: str, records: List[Dict[str, Any]], wait_timeout: int = 300) -> Dict[str, Any]:
        """
        Insert records using Salesforce Bulk API v2.
        Returns job info and status.
        """
        import io
        import csv
        import time
        
        # 1. Create Job
        url = f"{self._host}/services/data/{self._api_version}/jobs/ingest/"
        job_data = {
            "object": table,
            "contentType": "CSV",
            "operation": "insert"
        }
        job = self._request("POST", url, json=job_data)
        job_id = job["id"]
        
        try:
            # 2. Upload Data (CSV)
            if not records:
                return {"status": "Aborted", "numberRecordsProcessed": 0, "id": job_id}
                
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
            csv_content = csv_buffer.getvalue()
            
            upload_url = f"{url}{job_id}/batches"
            # Text/CSV content type - _request handles JSON by default, need override
            headers = {"Content-Type": "text/csv"}
            
            # We use _session directly to avoid _request forcing JSON headers (though _request allows overriding headers)
            # But _request helper assumes JSON response usually. Salesforce Bulk PUT returns 201 Created (empty body?).
            # Let's modify _request to handle text/csv or just call session directly here.
            # actually _request merges headers, so we can override Content-Type.
            self._request("PUT", upload_url, data=csv_content, headers=headers)
            
            # 3. Close Job (Start processing)
            close_url = f"{url}{job_id}/"
            self._request("PATCH", close_url, json={"state": "UploadComplete"})
            
            # 4. Wait for completion
            start_time = time.time()
            while (time.time() - start_time) < wait_timeout:
                status = self._request("GET", close_url)
                state = status["state"]
                if state in ("JobComplete", "Failed", "Aborted"):
                    return status
                time.sleep(2)
                
            raise AdapterError(f"Bulk insert timed out after {wait_timeout}s. Job ID: {job_id}")
            
        except Exception as e:
            # Try to abort job if something failed
            try:
                self._request("PATCH", f"{url}{job_id}/", json={"state": "Aborted"})
            except:
                pass
            raise e

    def _to_arrow(self, records: List[Dict], schema: List[ColumnInfo], selected_columns: List[str] = None) -> pa.Table:
        """Convert records to Arrow table with native struct support."""
        if not records:
            return pa.table({c.name: [] for c in schema})

        # Use new schema utility for proper struct conversion
        from waveql.utils.schema import records_to_arrow_table, infer_schema_from_records
        
        # Build Arrow schema from ColumnInfo
        schema_fields = []
        for col in schema:
            if selected_columns and selected_columns != ["*"] and col.name not in selected_columns:
                continue
            arrow_type = getattr(col, 'arrow_type', None) or pa.string()
            schema_fields.append(pa.field(col.name, arrow_type))
        
        # If we have arrow_types, use predefined schema; otherwise infer from records
        if schema_fields and any(getattr(c, 'arrow_type', None) for c in schema):
            arrow_schema = pa.schema(schema_fields)
        else:
            # Infer schema from records for proper struct support
            arrow_schema = infer_schema_from_records(records, sample_size=5)
        
        # Filter records to only include selected columns if specified
        if selected_columns and selected_columns != ["*"]:
            filtered_records = [
                {k: v for k, v in rec.items() if k in selected_columns}
                for rec in records
            ]
        else:
            filtered_records = records
        
        # Convert using the new utility with struct support
        return records_to_arrow_table(filtered_records, schema=arrow_schema)

