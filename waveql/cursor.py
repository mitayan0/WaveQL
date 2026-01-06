"""
WaveQL Cursor - DB-API 2.0 compliant cursor with predicate pushdown
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING
import re
import uuid
import logging
import pyarrow as pa

from waveql.exceptions import QueryError
from waveql.query_planner import QueryPlanner
from waveql.observability import QueryPlan

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


class Row:
    """
    A row object that supports both tuple-like indexing and dict-like key access.
    """
    def __init__(self, data: Dict[str, Any], schema: List[Tuple]):
        self._data = data
        self._schema = schema
        self._keys = [col[0] for col in schema]
        self._values = tuple(data[k] for k in self._keys)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._data[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        return f"Row({self._data})"

    def keys(self):
        return self._keys

    def values(self):
        return self._values

    def items(self):
        return self._data.items()

    def as_dict(self):
        return self._data

    def __getattr__(self, name):
        """Allow attribute-style access: row.column_name"""
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"'Row' object has no attribute '{name}'")


class WaveQLCursor:
    """
    DB-API 2.0 compliant cursor with intelligent query routing.
    
    Features:
    - Predicate pushdown to adapters
    - Automatic schema discovery
    - Arrow-native data handling
    - Virtual table registration in DuckDB
    """
    
    def __init__(self, connection: "WaveQLConnection"):
        self._connection = connection
        self._description: Optional[List[Tuple]] = None
        self._rowcount = -1
        self._arraysize = 100
        self._closed = False
        
        # Current result set
        self._result: Optional[pa.Table] = None
        self._result_index = 0
        
        
        # Query planner for predicate extraction
        self._planner = QueryPlanner()
        
        # Last execution plan
        self.last_plan: Optional[QueryPlan] = None
    
    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        DB-API 2.0 description attribute.
        Returns sequence of 7-item tuples describing columns.
        """
        return self._description
    
    @property
    def rowcount(self) -> int:
        """Number of rows affected by last operation."""
        return self._rowcount
    
    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize
    
    @arraysize.setter
    def arraysize(self, value: int):
        self._arraysize = value
    
    def execute(self, operation: str, parameters: Sequence = None) -> "WaveQLCursor":
        """
        Execute a SQL query.
        
        Args:
            operation: SQL query string
            parameters: Query parameters (for parameterized queries)
            
        Returns:
            Self for method chaining
        """
        if self._closed:
            raise QueryError("Cursor is closed")
        
        # Parse query to extract table, predicates, etc.
        query_info = self._planner.parse(operation)
        
        # Initialize execution plan
        self.last_plan = QueryPlan(sql=operation, is_explain=query_info.is_explain)
        
        # Determine which adapter to use
        adapter = self._resolve_adapter(query_info)
        
        try:
            if query_info.joins:
                # Handle virtual join across adapters
                self._result = self._execute_virtual_join(query_info, operation, parameters)
            elif adapter:
                # Route to adapter with predicate pushdown
                self._result = self._execute_via_adapter(query_info, adapter, parameters)
            else:
                # Fall back to direct DuckDB execution
                self._result = self._execute_direct(operation, parameters)
        finally:
            self.last_plan.finish()
        
        if query_info.is_explain:
            # For EXPLAIN, return the plan as a single-column table
            plan_text = self.last_plan.format_text()
            self._result = pa.Table.from_pydict({"Execution Plan": [plan_text]})
            self._rowcount = 1
        
        # Update description from result schema
        self._update_description()
        self._result_index = 0
        
        return self
    
    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence]) -> "WaveQLCursor":
        """Execute operation for each parameter set (for batch INSERT/UPDATE)."""
        if self._closed:
            raise QueryError("Cursor is closed")
        
        query_info = self._planner.parse(operation)
        adapter = self._resolve_adapter(query_info)
        
        if adapter and query_info.operation in ("INSERT", "UPDATE", "DELETE"):
            # Batch operation via adapter
            self._rowcount = adapter.execute_batch(query_info, seq_of_parameters)
        else:
            # Execute one by one
            total = 0
            for params in seq_of_parameters:
                self.execute(operation, params)
                if self._rowcount > 0:
                    total += self._rowcount
            self._rowcount = total
        
        return self
    
    def _clean_table_name(self, table_name: str) -> str:
        """
        Clean a table name by stripping quotes and extracting just the table portion.
        
        Examples:
            '"servicenow"."incident"' -> 'incident'
            'servicenow.incident'      -> 'incident'
            '"incident"'               -> 'incident'
            'incident'                 -> 'incident'
        """
        if not table_name:
            return table_name
        
        # If there's a schema prefix, extract just the table name
        if "." in table_name:
            _, table_part = table_name.rsplit(".", 1)
        else:
            table_part = table_name
        
        # Strip surrounding quotes
        return table_part.strip('"')
    
    def _resolve_adapter(self, query_info):
        """Determine which adapter handles this query based on table name."""
        table_name = query_info.table
        if not table_name:
            return None
        
        # Check for schema prefix (e.g., "sales.Account" or "servicenow"."incident")
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            # Strip quotes from schema name for lookup
            schema = schema.strip('"')
            adapter = self._connection.get_adapter(schema)
            if adapter:
                return adapter
        
        # Use default adapter
        return self._connection.get_adapter("default")
    
    def _execute_via_adapter(self, query_info, adapter, parameters) -> pa.Table:
        """Execute query via adapter with predicate pushdown and caching."""
        # Clean the table name to remove schema prefix and quotes for the adapter
        clean_table = self._clean_table_name(query_info.table)
        
        # Let adapter fetch data with pushed-down predicates
        if query_info.operation == "SELECT":
            # Check cache first
            cache = self._connection._cache
            cache_key = None
            
            if cache.config.enabled and cache.config.should_cache_table(query_info.table):
                # Generate cache key from query components
                cache_key = cache.generate_key(
                    adapter_name=adapter.adapter_name,
                    table=clean_table,
                    columns=tuple(query_info.columns) if query_info.columns else ("*",),
                    predicates=tuple(
                        (p.column, p.operator, p.value) for p in query_info.predicates
                    ) if query_info.predicates else (),
                    limit=query_info.limit,
                    offset=query_info.offset,
                    order_by=tuple(query_info.order_by) if query_info.order_by else None,
                    group_by=tuple(query_info.group_by) if query_info.group_by else None,
                )
                
                # Try to get from cache
                cached_result = cache.get(cache_key)
                if cached_result is not None:
                    # Cache hit - add to execution plan and return
                    step = self.last_plan.add_step(
                        name=f"Cache hit for {clean_table}",
                        type="cache",
                        details={
                            "table": clean_table,
                            "adapter": adapter.adapter_name,
                            "cache_key": cache_key,
                            "rows": len(cached_result),
                        }
                    )
                    step.finish()
                    self._rowcount = len(cached_result)
                    logger.debug(
                        "Cache hit: adapter=%s, table=%s, rows=%d",
                        adapter.adapter_name, clean_table, len(cached_result)
                    )
                    return cached_result
            
            # Cache miss or caching disabled - fetch from adapter
            step = self.last_plan.add_step(
                name=f"Fetch from {adapter.adapter_name}",
                type="fetch",
                details={
                    "table": clean_table,
                    "adapter": adapter.adapter_name,
                    "pushdown_predicates": [str(p) for p in query_info.predicates],
                    "cache_miss": cache_key is not None,
                }
            )
            try:
                data = adapter.fetch(
                    table=clean_table,
                    columns=query_info.columns,
                    predicates=query_info.predicates,
                    limit=query_info.limit,
                    offset=query_info.offset,
                    order_by=query_info.order_by,
                    group_by=query_info.group_by,
                    aggregates=query_info.aggregates,
                )
                
                # Check for source query in metadata
                if data is not None and data.schema.metadata:
                    source_query = data.schema.metadata.get(b"waveql_source_query")
                    if source_query:
                        step.details["source_query"] = source_query.decode("utf-8")
                
                step.finish()
                self._rowcount = len(data) if data else 0
                
                # Store in cache if enabled
                if cache_key is not None and data is not None:
                    cache.put(
                        key=cache_key,
                        data=data,
                        adapter_name=adapter.adapter_name,
                        table_name=clean_table,
                    )
                    logger.debug(
                        "Cache store: adapter=%s, table=%s, rows=%d, size=%.2fMB",
                        adapter.adapter_name, clean_table, len(data),
                        data.nbytes / (1024 * 1024)
                    )
                
                return data
            except NotImplementedError:
                step.finish()
                # Adapter does not support aggregation pushdown.
                # Fallback: Fetch raw data (filtered) and execute SQL locally in DuckDB.
                
                step_raw = self.last_plan.add_step(
                    name=f"Fetch raw data from {adapter.adapter_name} (Fallback)",
                    type="fetch",
                    details={"table": clean_table, "adapter": adapter.adapter_name}
                )
                # Fetch raw data with predicates pushed down
                raw_data = adapter.fetch(
                    table=clean_table,
                    columns=None, 
                    predicates=query_info.predicates
                )
                step_raw.finish()
                
                if not raw_data or len(raw_data) == 0:
                     self._rowcount = 0
                     return raw_data
 
                # Register temp table
                temp_name = f"t_{uuid.uuid4().hex}"
                self._connection._duckdb.register(temp_name, raw_data)
                
                try:
                    step_local = self.last_plan.add_step(
                        name="Local DuckDB execution (Fallback)",
                        type="duckdb",
                        details={"engine": "duckdb"}
                    )
                    # Rewrite SQL: Replace table name with temp table name
                    # We target the FROM clause to be safe
                    # Pattern matches: FROM <whitespace> tableName <word-boundary>
                    pattern = re.compile(f"FROM\\s+{re.escape(query_info.table)}\\b", re.IGNORECASE)
                    rewritten_sql = pattern.sub(f"FROM {temp_name}", query_info.raw_sql, count=1)
                    
                    # Execute
                    result = self._connection._duckdb.execute(rewritten_sql).fetch_arrow_table()
                    step_local.finish()
                    self._rowcount = len(result)
                    return result
                finally:
                    self._connection._duckdb.unregister(temp_name)
        
        elif query_info.operation == "INSERT":
            self._rowcount = adapter.insert(
                table=clean_table,
                values=query_info.values,
                parameters=parameters,
            )
            # Invalidate cache for this table after write
            self._connection._cache.invalidate(table=clean_table)
            return None
        
        elif query_info.operation == "UPDATE":
            self._rowcount = adapter.update(
                table=clean_table,
                values=query_info.values,
                predicates=query_info.predicates,
                parameters=parameters,
            )
            # Invalidate cache for this table after write
            self._connection._cache.invalidate(table=clean_table)
            return None
        
        elif query_info.operation == "DELETE":
            self._rowcount = adapter.delete(
                table=clean_table,
                predicates=query_info.predicates,
                parameters=parameters,
            )
            # Invalidate cache for this table after write
            self._connection._cache.invalidate(table=clean_table)
            return None
        
        else:
            raise QueryError(f"Unsupported operation: {query_info.operation}")
 
    def _execute_virtual_join(self, query_info, sql: str, parameters: Sequence = None) -> pa.Table:
        """
        Execute a virtual join with semi-join pushdown optimization.
        """
        from collections import defaultdict
        from waveql.query_planner import Predicate
        
        registered_tables = []
        created_views = []
        dataset_cache = {} # clean_table_name -> Arrow Table
 
        try:
            # 1. Map Tables & Aliases
            # aliases: alias -> table_name
            # Reverse map: table_name -> list of aliases (usually one)
            table_aliases = defaultdict(list)
            for alias, t_name in query_info.aliases.items():
                table_aliases[t_name].append(alias)
            
            all_tables = {query_info.table}
            for join in query_info.joins:
                all_tables.add(join["table"])
            
            # 2. Group initial predicates by table
            table_predicates = defaultdict(list)
            for pred in query_info.predicates:
                # Find which table this predicate belongs to via alias or direct name
                # Simple logic: if column has dot, split it.
                if "." in pred.column:
                    alias_part, col_part = pred.column.split(".", 1)
                    # Resolve alias
                    table_name = query_info.aliases.get(alias_part, alias_part) # if no alias, assume it is table name
                    # Store predicate with clean column name (optional? Adapter needs mapping?)
                    # Current adapter fetch expects column names matching schema. 
                    # If we strip alias from column, we must ensure adapter handles it.
                    # BaseAdapter usually treats column names as is.
                    # But if we push down 'u.active', the adapter for 'users' expects 'active'.
                    # Let's strip the alias from the predicate column for the pushdown.
                    p_copy = Predicate(column=col_part, operator=pred.operator, value=pred.value)
                    table_predicates[table_name].append(p_copy)
                else:
                    # Ambiguous or Main Table? Assume main table if not aliased? 
                    # For safety, add to main table
                    table_predicates[query_info.table].append(pred)
 
            # 3. Execution Plan (Simple Heuristic: Tables with predicates first)
            # We want to fetch tables that reduce the dataset first.
            sorted_tables = sorted(all_tables, key=lambda t: len(table_predicates[t]), reverse=True)
            
            # 4. Fetch Loop with Pushdown
            pushed_filters = defaultdict(list) # table -> list[Predicate]
            
            fetched_tables = set()
            
            for table_name in sorted_tables:
                # Resolve Adapter
                temp_info = type(query_info)(operation="SELECT", table=table_name)
                adapter = self._resolve_adapter(temp_info)
                
                if adapter:
                    clean_table = self._clean_table_name(table_name)
                    
                    # Combine Base Predicates + Pushed Filters
                    current_preds = table_predicates[table_name] + pushed_filters[table_name]
                    
                    # Fetch
                    data = adapter.fetch(
                        table=clean_table, 
                        columns=["*"], 
                        predicates=current_preds
                    )
                    dataset_cache[table_name] = data
                    fetched_tables.add(table_name)
                    
                    # 5. Analyze Joins for Pushdown Opportunities
                    # Check if this table is joined with any table NOT yet fetched
                    # And if we can generate a filter.
                    
                    # We need to look at all joins
                    # We look for: ON T1.c1 = T2.c2
                    # If T1 is current table, and T2 is not fetched, push condition to T2.
                    if data and len(data) > 0 and len(data) < 10000: # Limit pushdown for massive results
                        for join in query_info.joins:
                            if not join.get("on"): continue
                            
                            # Join involves which tables?
                            # We need to parse the predicates in 'on'
                            for on_pred in join["on"]:
                                if on_pred.operator == "=":
                                    # Check left and right operands
                                    # We expect format like "alias1.col"
                                    # Simple parsing:
                                    left, right = on_pred.column, on_pred.value
                                    if not isinstance(right, str): continue # Value must be a column reference string
                                    
                                    # Resolve tables for left and right
                                    t1_alias, t1_col = left.split(".", 1) if "." in left else (None, left)
                                    t2_alias, t2_col = right.split(".", 1) if "." in right else (None, right)
                                    
                                    t1_name = query_info.aliases.get(t1_alias) if t1_alias else None # Ambiguous handling omitted
                                    t2_name = query_info.aliases.get(t2_alias) if t2_alias else None
                                    
                                    target = None
                                    source_col = None
                                    target_col = None
                                    
                                    # If current table is T1, target is T2
                                    if t1_name == table_name and t2_name and t2_name not in fetched_tables:
                                        target = t2_name
                                        source_col = t1_col
                                        target_col = t2_col
                                    elif t2_name == table_name and t1_name and t1_name not in fetched_tables:
                                        target = t1_name
                                        source_col = t2_col
                                        target_col = t1_col
                                    
                                    if target:
                                        # Extract unique values from current data
                                        try:
                                            # DuckDB/Arrow extraction
                                            # source column in data might be 'c1' not 'alias.c1'
                                            # Adapter fetch creates columns based on schema.
                                            # Usually standard names.
                                            unique_vals = data.column(source_col).unique().to_pylist()
                                            # Remove None
                                            unique_vals = [v for v in unique_vals if v is not None]
                                            
                                            if unique_vals and len(unique_vals) < 2000: # IN clause limit
                                                # Create IN predicate
                                                pushed_filters[target].append(
                                                    Predicate(column=target_col, operator="IN", value=unique_vals)
                                                )
                                        except KeyError:
                                            # Column not found in result, maybe aliasing mismatch
                                            pass
            
            # 6. Register all fetched data
            for table_name, data in dataset_cache.items():
                if data is not None:
                     if "." in table_name:
                         schema, name = table_name.split(".", 1)
                         schema = schema.strip('"')
                         name = name.strip('"')
                         
                         self._connection.duckdb.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                         temp_name = f"t_{uuid.uuid4().hex}"
                         self._connection.duckdb.register(temp_name, data)
                         registered_tables.append(temp_name)
                         
                         view_name = f'"{schema}"."{name}"'
                         self._connection.duckdb.execute(
                            f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM "{temp_name}"'
                         )
                         created_views.append(view_name)
                     else:
                         self._connection.duckdb.register(table_name, data)
                         registered_tables.append(table_name)
 
            # 7. Execute JOIN
            step_join = self.last_plan.add_step(name="Virtual Join (DuckDB)", type="join")
            if parameters:
                result = self._connection.duckdb.execute(sql, parameters)
            else:
                result = self._connection.duckdb.execute(sql)
            
            table = result.fetch_arrow_table()
            step_join.finish()
            
            self._rowcount = -1
            return table
 
        except Exception as e:
            raise QueryError(f"Virtual join failed: {e}") from e
        finally:
            for view_name in created_views:
                try:
                    self._connection.duckdb.execute(f'DROP VIEW IF EXISTS {view_name}')
                except Exception:
                    pass
            for temp_name in registered_tables:
                try:
                    self._connection.duckdb.unregister(temp_name)
                except Exception:
                    pass
    
    def _execute_direct(self, sql: str, parameters: Sequence = None) -> pa.Table:
        """Execute directly on DuckDB."""
        step = self.last_plan.add_step(name="Direct DuckDB execution", type="duckdb")
        try:
            if parameters:
                result = self._connection.duckdb.execute(sql, parameters)
            else:
                result = self._connection.duckdb.execute(sql)
            
            table = result.fetch_arrow_table()
            step.finish()
            return table
        except Exception as e:
            step.finish()
            raise QueryError(f"Query execution failed: {e}")
    
    def _update_description(self):
        """Update cursor description from Arrow schema."""
        if self._result is None:
            self._description = None
            return
        
        schema = self._result.schema
        self._description = [
            (
                field.name,           # name
                field.type,           # type_code
                None,                 # display_size
                None,                 # internal_size
                None,                 # precision
                None,                 # scale
                field.nullable,       # null_ok
            )
            for field in schema
        ]
    
    def fetchone(self) -> Optional[Row]:
        """Fetch next row of result set."""
        if self._result is None or self._result_index >= len(self._result):
            return None
        
        row_dict = self._result.slice(self._result_index, 1).to_pylist()[0]
        self._result_index += 1
        
        return Row(row_dict, self._description)
    
    def fetchmany(self, size: int = None) -> List[Row]:
        """Fetch next set of rows."""
        if size is None:
            size = self._arraysize
        
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        
        return rows
    
    def fetchall(self) -> List[Row]:
        """Fetch all remaining rows."""
        if self._result is None:
            return []
        
        results = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        
        return results
    
    def to_arrow(self) -> Optional[pa.Table]:
        """Return result as Arrow Table (extension method)."""
        return self._result
    
    def to_df(self):
        """Return result as Pandas DataFrame (extension method)."""
        if self._result is None:
            return None
        return self._result.to_pandas()
    
    def stream_batches(
        self,
        operation: str,
        batch_size: int = 1000,
        max_records: int = None,
        progress_callback = None,
    ):
        """
        Stream query results as RecordBatches for memory-efficient processing.
        
        This method yields Arrow RecordBatches one at a time, enabling:
        - Processing of million-row exports without loading into memory
        - Progress tracking for long-running queries
        - Early termination (just stop iterating)
        
        Args:
            operation: SQL SELECT query
            batch_size: Number of records per batch (default 1000)
            max_records: Maximum total records to fetch (None = unlimited)
            progress_callback: Function(records_fetched, total_estimate) for progress
            
        Yields:
            pa.RecordBatch objects
            
        Example:
            for batch in cursor.stream_batches("SELECT * FROM large_table"):
                for row in batch.to_pylist():
                    process(row)
        """
        from waveql.streaming import RecordBatchStream, StreamConfig
        
        if self._closed:
            raise QueryError("Cursor is closed")
        
        # Parse query to get table and predicates
        query_info = self._planner.parse(operation)
        
        if query_info.operation != "SELECT":
            raise QueryError("stream_batches() only supports SELECT queries")
        
        # Resolve adapter
        adapter = self._resolve_adapter(query_info)
        if not adapter:
            raise QueryError("stream_batches() requires an adapter-backed table")
        
        clean_table = self._clean_table_name(query_info.table)
        
        config = StreamConfig(
            batch_size=batch_size,
            max_records=max_records,
            progress_callback=progress_callback,
        )
        
        stream = RecordBatchStream(
            adapter=adapter,
            table=clean_table,
            columns=query_info.columns if query_info.columns != ["*"] else None,
            predicates=query_info.predicates,
            order_by=query_info.order_by,
            config=config,
        )
        
        return stream
    
    def stream_to_file(
        self,
        operation: str,
        output_path: str,
        batch_size: int = 1000,
        compression: str = "snappy",
        progress_callback = None,
    ):
        """
        Stream query results directly to a Parquet file without loading into memory.
        
        This is the most memory-efficient way to export large datasets.
        
        Args:
            operation: SQL SELECT query
            output_path: Path to output Parquet file
            batch_size: Number of records per batch (default 1000)
            compression: Parquet compression ('snappy', 'gzip', 'zstd', 'none')
            progress_callback: Function(records_fetched, total_estimate) for progress
            
        Returns:
            StreamStats with operation statistics
            
        Example:
            stats = cursor.stream_to_file(
                "SELECT * FROM large_table",
                "export.parquet",
                progress_callback=lambda n, t: print(f"Exported {n:,} records")
            )
            print(f"Total: {stats.records_fetched:,} records")
        """
        from waveql.streaming import StreamConfig
        
        config = StreamConfig(
            batch_size=batch_size,
            compression=compression,
            progress_callback=progress_callback,
        )
        
        stream = self.stream_batches(
            operation,
            batch_size=batch_size,
            progress_callback=progress_callback,
        )
        stream._config.compression = compression
        
        return stream.to_parquet(output_path)
    
    def close(self):
        """Close the cursor."""
        self._closed = True
        self._result = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "closed" if self._closed else "open"
        result_len = len(self._result) if self._result is not None else 0
        return f"<WaveQLCursor status={status} rows={result_len} position={self._result_index}>"
