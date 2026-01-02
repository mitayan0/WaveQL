"""
WaveQL Cursor - DB-API 2.0 compliant cursor with predicate pushdown
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING
import re
import uuid
import pyarrow as pa

from waveql.exceptions import QueryError
from waveql.query_planner import QueryPlanner

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection


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
        
        # Determine which adapter to use
        adapter = self._resolve_adapter(query_info)
        
        if query_info.joins:
            # Handle virtual join across adapters
            self._result = self._execute_virtual_join(query_info, operation, parameters)
        elif adapter:
            # Route to adapter with predicate pushdown
            self._result = self._execute_via_adapter(query_info, adapter, parameters)
        else:
            # Fall back to direct DuckDB execution
            self._result = self._execute_direct(operation, parameters)
        
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
    
    def _resolve_adapter(self, query_info):
        """Determine which adapter handles this query based on table name."""
        table_name = query_info.table
        if not table_name:
            return None
        
        # Check for schema prefix (e.g., "sales.Account")
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            adapter = self._connection.get_adapter(schema)
            if adapter:
                return adapter
        
        # Use default adapter
        return self._connection.get_adapter("default")
    
    def _execute_via_adapter(self, query_info, adapter, parameters) -> pa.Table:
        """Execute query via adapter with predicate pushdown."""
        # Let adapter fetch data with pushed-down predicates
        if query_info.operation == "SELECT":
            try:
                data = adapter.fetch(
                    table=query_info.table,
                    columns=query_info.columns,
                    predicates=query_info.predicates,
                    limit=query_info.limit,
                    offset=query_info.offset,
                    order_by=query_info.order_by,
                    group_by=query_info.group_by,
                    aggregates=query_info.aggregates,
                )
                self._rowcount = len(data) if data else 0
                return data
            except NotImplementedError:
                # Adapter does not support aggregation pushdown.
                # Fallback: Fetch raw data (filtered) and execute SQL locally in DuckDB.
                
                # Fetch raw data with predicates pushed down
                raw_data = adapter.fetch(
                    table=query_info.table,
                    columns=None, 
                    predicates=query_info.predicates
                    # Limit/Offset/Order apply to result, so we apply them in local SQL
                )
                
                if not raw_data or len(raw_data) == 0:
                     self._rowcount = 0
                     return raw_data

                # Register temp table
                temp_name = f"t_{uuid.uuid4().hex}"
                self._connection._duckdb.register(temp_name, raw_data)
                
                try:
                    # Rewrite SQL: Replace table name with temp table name
                    # We target the FROM clause to be safe
                    # Pattern matches: FROM <whitespace> tableName <word-boundary>
                    pattern = re.compile(f"FROM\\s+{re.escape(query_info.table)}\\b", re.IGNORECASE)
                    rewritten_sql = pattern.sub(f"FROM {temp_name}", query_info.raw_sql, count=1)
                    
                    # Execute
                    result = self._connection._duckdb.execute(rewritten_sql).fetch_arrow_table()
                    self._rowcount = len(result)
                    return result
                finally:
                    self._connection._duckdb.unregister(temp_name)
        
        elif query_info.operation == "INSERT":
            self._rowcount = adapter.insert(
                table=query_info.table,
                values=query_info.values,
                parameters=parameters,
            )
            return None
        
        elif query_info.operation == "UPDATE":
            self._rowcount = adapter.update(
                table=query_info.table,
                values=query_info.values,
                predicates=query_info.predicates,
                parameters=parameters,
            )
            return None
        
        elif query_info.operation == "DELETE":
            self._rowcount = adapter.delete(
                table=query_info.table,
                predicates=query_info.predicates,
                parameters=parameters,
            )
            return None
        
        else:
            raise QueryError(f"Unsupported operation: {query_info.operation}")

    def _execute_virtual_join(self, query_info, sql: str, parameters: Sequence = None) -> pa.Table:
        """
        Execute a virtual join across different adapters.
        
        Strategy:
        1. Identify all tables involved (main table + joins).
        2. Fetch data from each adapter (pushing down predicates if possible).
        3. Register Arrow tables in DuckDB with proper schema handling.
        4. Execute the original SQL against these registered tables.
        """
        registered_tables = []
        
        try:
            # 1. Identify tables and their adapters
            tables = {query_info.table}
            for join in query_info.joins:
                tables.add(join["table"])
            
            # 2. Fetch and register data for each table
            for table_name in tables:
                # Use a dummy QueryInfo to resolve adapter
                temp_info = type(query_info)(operation="SELECT", table=table_name)
                adapter = self._resolve_adapter(temp_info)
                
                if adapter:
                    # Fetch data (select * for now to support join filtering)
                    data = adapter.fetch(table=table_name, columns=["*"])
                    
                    if data is not None and len(data) >= 0:
                        # Handle schema-qualified table names (e.g., "sales.Account")
                        if "." in table_name:
                            schema, name = table_name.split(".", 1)
                            
                            # Create schema in DuckDB if needed
                            self._connection.duckdb.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                            
                            # Register with a unique temporary name
                            import uuid
                            temp_name = f"t_{uuid.uuid4().hex}"
                            self._connection.duckdb.register(temp_name, data)
                            registered_tables.append(temp_name)
                            
                            # Create a view with the schema-qualified name
                            self._connection.duckdb.execute(
                                f'CREATE OR REPLACE VIEW "{schema}"."{name}" AS SELECT * FROM "{temp_name}"'
                            )
                        else:
                            # Simple table name - register directly
                            self._connection.duckdb.register(table_name, data)
                            registered_tables.append(table_name)
            
            # 3. Execute the JOIN query
            if parameters:
                result = self._connection.duckdb.execute(sql, parameters)
            else:
                result = self._connection.duckdb.execute(sql)
            
            self._rowcount = -1  # Unknown for virtual join
            return result.fetch_arrow_table()
            
        except Exception as e:
            raise QueryError(f"Virtual join failed: {e}")
        finally:
            # Cleanup: unregister temporary tables to avoid memory leaks
            for temp_name in registered_tables:
                try:
                    self._connection.duckdb.unregister(temp_name)
                except Exception:
                    pass  # Ignore cleanup errors
    
    def _execute_direct(self, sql: str, parameters: Sequence = None) -> pa.Table:
        """Execute directly on DuckDB."""
        try:
            if parameters:
                result = self._connection.duckdb.execute(sql, parameters)
            else:
                result = self._connection.duckdb.execute(sql)
            
            return result.fetch_arrow_table()
        except Exception as e:
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
    
    def fetchone(self) -> Optional[Tuple]:
        """Fetch next row of result set."""
        if self._result is None or self._result_index >= len(self._result):
            return None
        
        row = self._result.slice(self._result_index, 1).to_pydict()
        self._result_index += 1
        
        return tuple(v[0] for v in row.values())
    
    def fetchmany(self, size: int = None) -> List[Tuple]:
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
    
    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows."""
        if self._result is None:
            return []
        
        remaining = self._result.slice(self._result_index)
        self._result_index = len(self._result)
        
        return [tuple(row.values()) for row in remaining.to_pylist()]
    
    def to_arrow(self) -> Optional[pa.Table]:
        """Return result as Arrow Table (extension method)."""
        return self._result
    
    def to_df(self):
        """Return result as Pandas DataFrame (extension method)."""
        if self._result is None:
            return None
        return self._result.to_pandas()
    
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
