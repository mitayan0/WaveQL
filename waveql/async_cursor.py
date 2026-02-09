"""
WaveQL Async Cursor - Asynchronous DB-API 2.0 style cursor with predicate pushdown

Provides async/await support for:
- Query execution with automatic adapter routing
- Predicate pushdown to data sources
- Arrow-native data handling
"""

from __future__ import annotations
from typing import List, Optional, Sequence, Tuple, TYPE_CHECKING
import re
import uuid
import logging
import anyio
import pyarrow as pa

from waveql.exceptions import QueryError
from waveql.query_planner import QueryPlanner

if TYPE_CHECKING:
    from waveql.async_connection import AsyncWaveQLConnection

logger = logging.getLogger(__name__)


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
            # Strip quotes from schema name for lookup
            schema = schema.strip('"')
            adapter = self._connection.get_adapter(schema)
            if adapter: return adapter
        return self._connection.get_adapter("default")

    async def _execute_via_adapter_async(self, query_info, adapter, parameters) -> pa.Table:
        """Execute query via adapter with predicate pushdown and caching."""
        # Clean table name
        clean_table = query_info.table.split(".")[-1].strip('"') if query_info.table else query_info.table
        
        if query_info.operation == "SELECT":
            # Check cache first
            cache = self._connection._cache
            cache_key = None
            
            if cache.config.enabled and cache.config.should_cache_table(query_info.table):
                # Generate cache key
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
                    self._rowcount = len(cached_result)
                    logger.debug(
                        "Cache hit (async): adapter=%s, table=%s, rows=%d",
                        adapter.adapter_name, clean_table, len(cached_result)
                    )
                    return cached_result
            
            # Cache miss - fetch from adapter
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
                
                # Store in cache if enabled
                if cache_key is not None and data is not None:
                    cache.put(
                        key=cache_key,
                        data=data,
                        adapter_name=adapter.adapter_name,
                        table_name=clean_table,
                    )
                    logger.debug(
                        "Cache store (async): adapter=%s, table=%s, rows=%d",
                        adapter.adapter_name, clean_table, len(data)
                    )
                
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
            # Invalidate cache for this table
            self._connection._cache.invalidate(table=clean_table)
            return None
        elif query_info.operation == "UPDATE":
            self._rowcount = await adapter.update_async(table=query_info.table, values=query_info.values, predicates=query_info.predicates, parameters=parameters)
            # Invalidate cache for this table
            self._connection._cache.invalidate(table=clean_table)
            return None
        elif query_info.operation == "DELETE":
            self._rowcount = await adapter.delete_async(table=query_info.table, predicates=query_info.predicates, parameters=parameters)
            # Invalidate cache for this table
            self._connection._cache.invalidate(table=clean_table)
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
            
            async def fetch_and_register(table_name, results_dict):
                """Fetch table data and store in shared dictionary."""
                temp_info = type(query_info)(operation="SELECT", table=table_name)
                adapter = self._resolve_adapter(temp_info)
                if adapter:
                    data = await adapter.fetch_async(table=table_name, columns=["*"])
                    if data is not None:
                        results_dict[table_name] = data

            # Fetch all tables concurrently using task group
            fetched_data = {}
            async with anyio.create_task_group() as tg:
                for t in tables:
                    tg.start_soon(fetch_and_register, t, fetched_data)
            
            # Register all fetched tables in DuckDB (sync operations)
            for table_name, data in fetched_data.items():
                await anyio.to_thread.run_sync(self._register_in_duckdb, table_name, data, registered_tables)
            
            # Execute JOIN in thread
            return await anyio.to_thread.run_sync(self._execute_direct, sql, parameters)
        except Exception as e:
            raise QueryError(f"Virtual join failed (async): {e}") from e
        finally:
            for t in registered_tables:
                try: self._connection._duckdb.unregister(t)
                except Exception: pass

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
        """Close the cursor."""
        self._closed = True
        self._result = None

    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize
    
    @arraysize.setter
    def arraysize(self, value: int):
        self._arraysize = value

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

    def to_arrow(self) -> Optional[pa.Table]:
        """Return result as Arrow Table."""
        return self._result

    def to_df(self):
        """Return result as Pandas DataFrame."""
        if self._result is None:
            return None
        return self._result.to_pandas()

    def stream_batches_async(
        self,
        operation: str,
        batch_size: int = 1000,
        max_records: int = None,
        progress_callback = None,
        use_buffer: bool = False,
    ):
        """
        Stream query results as RecordBatches for memory-efficient async processing.
        
        This method returns an async iterator that yields Arrow RecordBatches
        one at a time with proper backpressure support, enabling:
        - Processing of million-row exports without loading into memory
        - Concurrent prefetching for maximum throughput (use_buffer=True)
        - Progress tracking for long-running queries
        - Early termination (just stop iterating)
        
        Args:
            operation: SQL SELECT query
            batch_size: Number of records per batch (default 1000)
            max_records: Maximum total records to fetch (None = unlimited)
            progress_callback: Function(records_fetched, total_estimate) for progress
            use_buffer: If True, use buffered prefetching for better throughput
            
        Returns:
            AsyncIterator[pa.RecordBatch]
            
        Example:
            async for batch in cursor.stream_batches_async("SELECT * FROM large_table"):
                for row in batch.to_pylist():
                    await process_async(row)
        """
        from waveql.streaming import AsyncRecordBatchStream, BufferedAsyncStream, StreamConfig
        
        if self._closed:
            raise QueryError("Cursor is closed")
        
        # Parse query to get table and predicates
        query_info = self._planner.parse(operation)
        
        if query_info.operation != "SELECT":
            raise QueryError("stream_batches_async() only supports SELECT queries")
        
        # Resolve adapter
        adapter = self._resolve_adapter(query_info)
        if not adapter:
            raise QueryError("stream_batches_async() requires an adapter-backed table")
        
        clean_table = query_info.table.split(".")[-1].strip('"') if query_info.table else query_info.table
        
        config = StreamConfig(
            batch_size=batch_size,
            max_records=max_records,
            progress_callback=progress_callback,
        )
        
        if use_buffer:
            return BufferedAsyncStream(
                adapter=adapter,
                table=clean_table,
                columns=query_info.columns if query_info.columns != ["*"] else None,
                predicates=query_info.predicates,
                order_by=query_info.order_by,
                config=config,
            )
        else:
            return AsyncRecordBatchStream(
                adapter=adapter,
                table=clean_table,
                columns=query_info.columns if query_info.columns != ["*"] else None,
                predicates=query_info.predicates,
                order_by=query_info.order_by,
                config=config,
            )

    async def stream_to_file_async(
        self,
        operation: str,
        output_path: str,
        batch_size: int = 1000,
        compression: str = "snappy",
        progress_callback = None,
    ):
        """
        Stream query results directly to a Parquet file without loading into memory.
        
        This is the most memory-efficient way to export large datasets (async version).
        
        Args:
            operation: SQL SELECT query
            output_path: Path to output Parquet file
            batch_size: Number of records per batch (default 1000)
            compression: Parquet compression ('snappy', 'gzip', 'zstd', 'none')
            progress_callback: Function(records_fetched, total_estimate) for progress
            
        Returns:
            StreamStats with operation statistics
            
        Example:
            stats = await cursor.stream_to_file_async(
                "SELECT * FROM large_table",
                "export.parquet",
                progress_callback=lambda n, t: print(f"Exported {n:,} records")
            )
            print(f"Total: {stats.records_fetched:,} records")
        """
        stream = self.stream_batches_async(
            operation,
            batch_size=batch_size,
            progress_callback=progress_callback,
        )
        stream._config.compression = compression
        
        return await stream.to_parquet(output_path)

    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "closed" if self._closed else "open"
        result_len = len(self._result) if self._result is not None else 0
        return f"<AsyncWaveQLCursor status={status} rows={result_len} position={self._result_index}>"

