
from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING
import re
import uuid
import anyio
import pyarrow as pa

from waveql.exceptions import QueryError
from waveql.query_planner import QueryPlanner

if TYPE_CHECKING:
    from waveql.async_connection import AsyncWaveQLConnection


class AsyncWaveQLCursor:
    """Async version of WaveQLCursor."""
    
    def __init__(self, connection: "AsyncWaveQLConnection"):
        self._connection = connection
        self._description: Optional[List[Tuple]] = None
        self._rowcount = -1
        self._arraysize = 100
        self._closed = False
        self._result: Optional[pa.Table] = None
        self._result_index = 0
        self._planner = QueryPlanner()

    @property
    def description(self) -> Optional[List[Tuple]]:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    async def execute(self, operation: str, parameters: Sequence = None) -> "AsyncWaveQLCursor":
        if self._closed:
            raise QueryError("Cursor is closed")
        
        query_info = self._planner.parse(operation)
        adapter = self._resolve_adapter(query_info)
        
        if query_info.joins:
            # Join logic is complex, for now we run it synchronously in a thread
            # or we could make it async too. Making it async is better.
            self._result = await self._execute_virtual_join_async(query_info, operation, parameters)
        elif adapter:
            self._result = await self._execute_via_adapter_async(query_info, adapter, parameters)
        else:
            # DuckDB part is sync, so we run in thread
            self._result = await anyio.to_thread.run_sync(self._execute_direct, operation, parameters)
        
        self._update_description()
        self._result_index = 0
        return self

    def _resolve_adapter(self, query_info):
        table_name = query_info.table
        if not table_name: return None
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            adapter = self._connection.get_adapter(schema)
            if adapter: return adapter
        return self._connection.get_adapter("default")

    async def _execute_via_adapter_async(self, query_info, adapter, parameters) -> pa.Table:
        if query_info.operation == "SELECT":
            try:
                data = await adapter.fetch_async(
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
                # Fallback to local SQL
                raw_data = await adapter.fetch_async(table=query_info.table, columns=None, predicates=query_info.predicates)
                if not raw_data or len(raw_data) == 0:
                    self._rowcount = 0
                    return raw_data
                
                return await anyio.to_thread.run_sync(self._execute_fallback_local, query_info, raw_data)
        
        elif query_info.operation == "INSERT":
            self._rowcount = await adapter.insert_async(table=query_info.table, values=query_info.values, parameters=parameters)
            return None
        elif query_info.operation == "UPDATE":
            self._rowcount = await adapter.update_async(table=query_info.table, values=query_info.values, predicates=query_info.predicates, parameters=parameters)
            return None
        elif query_info.operation == "DELETE":
            self._rowcount = await adapter.delete_async(table=query_info.table, predicates=query_info.predicates, parameters=parameters)
            return None
        else:
            raise QueryError(f"Unsupported operation: {query_info.operation}")

    def _execute_fallback_local(self, query_info, raw_data) -> pa.Table:
        temp_name = f"t_{uuid.uuid4().hex}"
        self._connection._duckdb.register(temp_name, raw_data)
        try:
            pattern = re.compile(f"FROM\\s+{re.escape(query_info.table)}\\b", re.IGNORECASE)
            rewritten_sql = pattern.sub(f"FROM {temp_name}", query_info.raw_sql, count=1)
            result = self._connection._duckdb.execute(rewritten_sql).fetch_arrow_table()
            self._rowcount = len(result)
            return result
        finally:
            self._connection._duckdb.unregister(temp_name)

    async def _execute_virtual_join_async(self, query_info, sql: str, parameters: Sequence = None) -> pa.Table:
        # Fetching data for joins is highly parallelizable with async
        registered_tables = []
        try:
            tables = {query_info.table}
            for join in query_info.joins:
                tables.add(join["table"])
            
            async def fetch_and_register(table_name):
                temp_info = type(query_info)(operation="SELECT", table=table_name)
                adapter = self._resolve_adapter(temp_info)
                if adapter:
                    data = await adapter.fetch_async(table=table_name, columns=["*"])
                    if data is not None:
                        return table_name, data
                return None, None

            # Fetch all tables concurrently
            results = []
            async with anyio.create_task_group() as tg:
                for t in tables:
                    # We can't easily return values from tg.start_soon, 
                    # so we use a list or another pattern.
                    pass # Placeholder for concurrent fetch logic
            
            # Simplified sequential for now but could be parallel
            for t in tables:
                name, data = await fetch_and_register(t)
                if name:
                    # Registration in DuckDB is sync
                    await anyio.to_thread.run_sync(self._register_in_duckdb, name, data, registered_tables)
            
            # Execute JOIN in thread
            return await anyio.to_thread.run_sync(self._execute_direct, sql, parameters)
        except Exception as e:
            raise QueryError(f"Virtual join failed (async): {e}")
        finally:
            for t in registered_tables:
                try: self._connection._duckdb.unregister(t)
                except: pass

    def _register_in_duckdb(self, table_name, data, registered_list):
        if "." in table_name:
            schema, name = table_name.split(".", 1)
            self._connection.duckdb.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            temp_name = f"t_{uuid.uuid4().hex}"
            self._connection.duckdb.register(temp_name, data)
            registered_list.append(temp_name)
            self._connection.duckdb.execute(f'CREATE OR REPLACE VIEW "{schema}"."{name}" AS SELECT * FROM "{temp_name}"')
        else:
            self._connection.duckdb.register(table_name, data)
            registered_list.append(table_name)

    def _execute_direct(self, sql: str, parameters: Sequence = None) -> pa.Table:
        res = self._connection.duckdb.execute(sql, parameters) if parameters else self._connection.duckdb.execute(sql)
        return res.fetch_arrow_table()

    def _update_description(self):
        if self._result is None:
            self._description = None
            return
        self._description = [(f.name, f.type, None, None, None, None, f.nullable) for f in self._result.schema]

    def fetchone(self) -> Optional[Tuple]:
        if self._result is None or self._result_index >= len(self._result): return None
        row = self._result.slice(self._result_index, 1).to_pydict()
        self._result_index += 1
        return tuple(v[0] for v in row.values())

    def fetchall(self) -> List[Tuple]:
        if self._result is None: return []
        remaining = self._result.slice(self._result_index)
        self._result_index = len(self._result)
        return [tuple(row.values()) for row in remaining.to_pylist()]

    async def close(self):
        self._closed = True
        self._result = None

    def to_arrow(self) -> Optional[pa.Table]:
        """Return result as Arrow Table."""
        return self._result

    def to_df(self):
        """Return result as Pandas DataFrame."""
        if self._result is None:
            return None
        return self._result.to_pandas()
