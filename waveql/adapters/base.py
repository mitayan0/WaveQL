"""
Base Adapter - Abstract base class for all data source adapters

Adapters are responsible for:
1. Fetching data from the source (with predicate pushdown)
2. Inserting/Updating/Deleting records
3. Schema discovery
4. Converting data to Arrow format
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
from urllib.parse import urlparse

from waveql.utils.wasm import is_wasm
import pyarrow as pa

if TYPE_CHECKING:
    from waveql.auth.manager import AuthManager
    from waveql.schema_cache import SchemaCache, ColumnInfo
    from waveql.query_planner import Predicate
    import requests
    import httpx


class BaseAdapter(ABC):
    """
    Abstract base class for WaveQL adapters.
    
    Subclasses must implement:
    - fetch(): Retrieve data with optional filtering
    - get_schema(): Discover table schema
    
    Optional overrides for CRUD:
    - insert()
    - update()
    - delete()
    - execute_batch()
    
    Connection Pooling:
    - Set use_connection_pool=True to use the global connection pool
    - This enables connection reuse across multiple requests
    """
    
    # Adapter metadata
    adapter_name: str = "base"
    supports_predicate_pushdown: bool = True
    supports_aggregation: bool = False  # Server-side aggregation support
    supports_insert: bool = False
    supports_update: bool = False
    supports_delete: bool = False
    supports_batch: bool = False
    supports_parallel_scan: bool = False # For large scale data fetching
    
    # Auto-chunking configuration (subclasses can override)
    # These limits prevent HTTP 414 URI Too Long errors
    max_in_clause_values: int = 100       # Max values in a single IN (...) clause
    chunk_threshold: int = 50              # Start chunking when IN has more than this
    max_parallel_chunks: int = 4           # Max parallel chunk workers
    
    def __init__(
        self,
        host: str = None,
        auth_manager: "AuthManager" = None,
        schema_cache: "SchemaCache" = None,
        max_retries: int = 5,
        retry_base_delay: float = 1.0,
        use_connection_pool: bool = True,
        **kwargs
    ):
        self._host = host
        self._auth_manager = auth_manager
        self._schema_cache = schema_cache
        self._config = kwargs
        self._use_connection_pool = use_connection_pool
        
        # Extract host for connection pool key
        self._pool_host = self._extract_host(host) if host else "default"
        
        # Initialize rate limiter for automatic retry on rate limits
        from waveql.utils.rate_limiter import RateLimiter
        self._rate_limiter = RateLimiter(
            max_retries=max_retries,
            base_delay=retry_base_delay,
        )
        
        # Lazy-loaded local session (when not using pool)
        self._local_session: Optional["requests.Session"] = None
        
        # Performance tracking (for Cost-Based Optimizer)
        self.avg_latency_per_row: float = 0.001  # Default: 1ms per row
        self._execution_history: List[Dict[str, Any]] = []
    
    def _extract_host(self, url: str) -> str:
        """Extract hostname from URL for pool keying."""
        if not url:
            return "default"
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        parsed = urlparse(url)
        return parsed.netloc or parsed.path.split("/")[0]
    
    @contextmanager
    def _get_session(self) -> "requests.Session":
        """
        Get an HTTP session from the connection pool or create a local one.
        
        Usage:
            with self._get_session() as session:
                response = session.get(url)
        
        Returns:
            Context manager yielding a requests.Session
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_sync_pool
            pool = get_sync_pool()
            with pool.get_session(self._pool_host) as session:
                yield session
        else:
            # Use local session (backward compatible)
            if self._local_session is None:
                import requests
                self._local_session = requests.Session()
            yield self._local_session
    
    def _get_session_direct(self) -> "requests.Session":
        """
        Get an HTTP session directly (without context manager).
        
        Use this when you need to keep the session across multiple operations.
        Remember to call _return_session() when done if using the pool.
        
        Returns:
            requests.Session instance
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_sync_pool
            pool = get_sync_pool()
            return pool.get_session_direct(self._pool_host)
        else:
            if self._local_session is None:
                import requests
                self._local_session = requests.Session()
            return self._local_session
    
    def _return_session(self, session: "requests.Session"):
        """
        Return a session to the pool (for use with _get_session_direct).
        
        Args:
            session: The session to return
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_sync_pool
            pool = get_sync_pool()
            pool.return_session(self._pool_host, session)
    
    def _get_async_client(self) -> "httpx.AsyncClient":
        """
        Get an async HTTP client from the connection pool.
        
        The async pool shares clients per host, so this returns
        a shared client that should NOT be closed by the caller.
        
        Returns:
            httpx.AsyncClient instance
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_async_pool
            pool = get_async_pool()
            return pool.get_client(self._pool_host)
        else:
            # Create a new client (caller manages lifecycle)
            import httpx
            return httpx.AsyncClient()
    
    def set_auth_manager(self, auth_manager: "AuthManager"):
        """Set the authentication manager."""
        self._auth_manager = auth_manager
    
    def set_schema_cache(self, schema_cache: "SchemaCache"):
        """Set the schema cache."""
        self._schema_cache = schema_cache
    
    @abstractmethod
    def fetch(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List["Aggregate"] = None,
    ) -> pa.Table:
        """
        Fetch data from the source.
        
        Args:
            table: Table/resource name
            columns: Columns to retrieve (None = all)
            predicates: WHERE clause predicates for pushdown
            limit: Max rows to return
            offset: Row offset
            order_by: List of (column, direction) tuples
            
        Returns:
            PyArrow Table with results
        """
        if is_wasm():
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"[{self.adapter_name}] Synchronous fetch used in Wasm/Pyodide environment. "
                           "This may block the browser UI. Use fetch_async() instead.")
        pass
    
    @abstractmethod
    def get_schema(self, table: str) -> List["ColumnInfo"]:
        """Discover schema for a table."""
        pass
    
    async def fetch_async(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List["Aggregate"] = None,
    ) -> pa.Table:
        """Fetch data from the source (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support fetch_async")

    async def get_schema_async(self, table: str) -> List["ColumnInfo"]:
        """Discover schema for a table (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support get_schema_async")
    
    def get_parallel_plan(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        n_partitions: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Generate a plan for parallel execution.
        
        Returns a list of partition definitions that can be passed to fetch()
        by separate workers.
        """
        if not self.supports_parallel_scan:
             return [{"partition_index": 0, "total_partitions": 1}]
             
        # Default behavior: Base adapter doesn't know how to split, so returns 1 partition
        # regardless of requested n_partitions. Subclasses must override to support splitting.
        return [{"partition_index": 0, "total_partitions": 1}]
    
    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """
        Insert a record.
        
        Args:
            table: Table name
            values: Column-value pairs
            parameters: Additional parameters
            
        Returns:
            Number of rows inserted
        """
        raise NotImplementedError(f"{self.adapter_name} does not support INSERT")
    
    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """
        Update records.
        
        Args:
            table: Table name
            values: Column-value pairs to update
            predicates: WHERE conditions
            parameters: Additional parameters
            
        Returns:
            Number of rows updated
        """
        raise NotImplementedError(f"{self.adapter_name} does not support UPDATE")
    
    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """
        Delete records.
        
        Args:
            table: Table name
            predicates: WHERE conditions
            parameters: Additional parameters
            
        Returns:
            Number of rows deleted
        """
        raise NotImplementedError(f"{self.adapter_name} does not support DELETE")

    async def insert_async(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Insert a record (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support insert_async")

    async def update_async(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update records (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support update_async")

    async def delete_async(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete records (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support delete_async")
    
    def execute_batch(
        self,
        query_info,
        seq_of_parameters: Sequence[Sequence],
    ) -> int:
        """
        Execute batch operation.
        
        Args:
            query_info: Parsed query info
            seq_of_parameters: Sequence of parameter sets
            
        Returns:
            Total rows affected
        """
        raise NotImplementedError(f"{self.adapter_name} does not support batch operations")
    
    def list_tables(self) -> List[str]:
        """
        List available tables/resources.
        
        Returns:
            List of table names
        """
        return []
    
    def _get_cached_schema(self, table: str) -> Optional[List["ColumnInfo"]]:
        """Get schema from cache if available."""
        if self._schema_cache:
            cached = self._schema_cache.get(self.adapter_name, table)
            if cached:
                return cached.columns
        return None
    
    def _cache_schema(self, table: str, columns: List["ColumnInfo"], ttl: int = 3600) -> None:
        """Cache discovered schema."""
        if self._schema_cache:
            self._schema_cache.set(self.adapter_name, table, columns, ttl)
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers from auth manager."""
        if self._auth_manager:
            return self._auth_manager.get_headers()
        return {}

    async def _get_auth_headers_async(self) -> Dict[str, str]:
        """Get authentication headers from auth manager (async)."""
        if self._auth_manager:
            return await self._auth_manager.get_headers_async()
        return {}
    
    def _extract_id_from_predicates(self, predicates: List["Predicate"], operation: str) -> str:
        """
        Helper to extract 'id' from predicates.
        
        Args:
            predicates: List of predicates
            operation: Name of operation for error message
            
        Returns:
            The extracted ID string
            
        Raises:
            QueryError: If ID is not found or invalid
        """
        from waveql.exceptions import QueryError
        
        if not predicates:
            raise QueryError(f"{operation} requires 'id' in WHERE clause (e.g., WHERE id = '123')")
            
        object_id = None
        for pred in predicates:
            if pred.column.lower() == "id" and pred.operator == "=":
                object_id = pred.value
                break
                
        if not object_id:
            raise QueryError(f"{operation} requires 'id' in WHERE clause")
            
        return str(object_id)

    def _request_with_retry(self, request_func, *args, **kwargs) -> Any:
        """
        Execute an HTTP request with automatic retry on rate limits.
        
        Args:
            request_func: Function to execute (e.g., session.get, session.post)
            *args, **kwargs: Arguments for the request function
            
        Returns:
            Response from the request
            
        Raises:
            Original exception if all retries fail
        """
        return self._rate_limiter.execute_with_retry(request_func, *args, **kwargs)

    def _update_performance_metrics(self, row_count: int, duration: float):
        """Update latency metrics for the CBO."""
        if row_count > 0:
            latency = duration / row_count
            # Simple moving average (alpha=0.2)
            self.avg_latency_per_row = (0.8 * self.avg_latency_per_row) + (0.2 * latency)
            
            # Keep history for more complex analysis
            self._execution_history.append({
                "rows": row_count,
                "duration": duration,
                "latency_per_row": latency
            })
            if len(self._execution_history) > 100:
                self._execution_history.pop(0)

    def fetch_with_auto_chunking(
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
        Fetch data with automatic chunking for large IN predicates.
        
        This method transparently handles large IN (...) clauses by:
        1. Detecting predicates that exceed max_in_clause_values
        2. Splitting them into smaller chunks
        3. Executing chunks in parallel
        4. Merging results
        
        Users don't need to configure anything - it just works.
        
        Args:
            Same as fetch()
            
        Returns:
            PyArrow Table with results
        """
        predicates = predicates or []
        
        # Check if any IN predicate needs chunking
        needs_chunking = False
        for pred in predicates:
            if (pred.operator.upper() == "IN" 
                and isinstance(pred.value, (list, tuple))
                and len(pred.value) > self.chunk_threshold):
                needs_chunking = True
                break
        
        if not needs_chunking:
            # Fast path: no chunking needed
            return self.fetch(
                table=table,
                columns=columns,
                predicates=predicates,
                limit=limit,
                offset=offset,
                order_by=order_by,
                group_by=group_by,
                aggregates=aggregates,
            )
        
        # Use chunked executor (internal module)
        from waveql.chunked_executor import ChunkedExecutor, ChunkConfig
        
        config = ChunkConfig(
            max_chunk_size=self.max_in_clause_values,
            chunk_threshold=self.chunk_threshold,
            max_workers=self.max_parallel_chunks,
        )
        executor = ChunkedExecutor(self, config)
        
        return executor.execute_chunked(
            table=table,
            columns=columns,
            predicates=predicates,
            limit=limit,
            offset=offset,
            order_by=order_by,
            group_by=group_by,
            aggregates=aggregates,
        )

    # Performance threshold for client-side aggregation warning
    CLIENT_SIDE_AGGREGATION_WARNING_THRESHOLD = 5000
    
    # Maximum rows to process for approximate aggregation (when enabled)
    APPROXIMATE_AGGREGATION_SAMPLE_SIZE = 10000
    
    def _compute_client_side_aggregates(
        self,
        table: pa.Table,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        """
        Compute aggregations client-side with memory-efficient streaming approach.
        
        Uses incremental computation to minimize memory usage:
        - COUNT: Simple counter
        - SUM: Running total
        - AVG: Running sum + count
        - MIN/MAX: Track boundary values
        
        For GROUP BY queries, uses PyArrow native grouping (zero-copy).
        
        Performance Note:
            Client-side aggregation requires fetching all matching rows first.
            For large datasets, this can be slow and memory-intensive.
            Consider using LIMIT or filtering to reduce data volume.
        
        Args:
            table: Input Arrow table
            group_by: Columns to group by
            aggregates: List of Aggregate objects with func, column, alias
            
        Returns:
            Aggregated Arrow table
        """
        import pyarrow.compute as pc
        import logging
        
        logger = logging.getLogger(__name__)
        
        if not aggregates:
            return table
        
        row_count = len(table)
        
        # Performance warning for large datasets
        if row_count > self.CLIENT_SIDE_AGGREGATION_WARNING_THRESHOLD:
            logger.warning(
                f"[{self.adapter_name}] Client-side aggregation on {row_count:,} rows. "
                f"This may be slow. Consider using LIMIT, WHERE filters, or a more specific query."
            )
        
        # If empty and GROUP BY is present, return empty result
        # If empty and NO GROUP BY (scalar agg), we must return 1 row (e.g. COUNT(*)=0)
        if row_count == 0 and group_by:
            # Return empty result with correct columns
            result_cols = {}
            if group_by:
                for col in group_by:
                    result_cols[col] = pa.array([], type=pa.string())
            for agg in aggregates:
                alias = agg.alias or f"{agg.func.upper()}({agg.column})"
                # Aggregates on empty groups don't exist
                result_cols[alias] = pa.array([], type=pa.float64())
            return pa.table(result_cols)
        
        # If row_count == 0 and NO group_by, fall through to _streaming_aggregate
        # which handles scalar aggregation on empty input (e.g. returns [0] for count)
        
        # For GROUP BY queries, use PyArrow native grouping
        if group_by:
            return self._aggregate_with_groupby(table, group_by, aggregates)
        
        # For non-GROUP BY, use streaming aggregation (memory efficient)
        return self._streaming_aggregate(table, aggregates)
    
    def _streaming_aggregate(
        self,
        table: pa.Table,
        aggregates: List[Any],
    ) -> pa.Table:
        """
        Memory-efficient streaming aggregation without GROUP BY.
        
        Uses PyArrow compute functions directly instead of loading into Pandas,
        reducing memory footprint for large tables.
        """
        import pyarrow.compute as pc
        
        result_data = {}
        
        for agg in aggregates:
            func = agg.func.upper()
            col = agg.column
            alias = agg.alias or f"{func}({col})"
            
            if func == "COUNT":
                if col == "*" or col is None:
                    result_data[alias] = [len(table)]
                else:
                    # Count non-null values in column
                    if col in table.column_names:
                        column = table.column(col)
                        # Count non-nulls. pc.sum returns None on empty arrays
                        nulls = pc.is_null(column)
                        null_count_scalar = pc.sum(nulls).as_py()
                        null_count = null_count_scalar if null_count_scalar is not None else 0
                        result_data[alias] = [len(column) - null_count]
                    else:
                        result_data[alias] = [0]
            
            elif func in ("SUM", "AVG", "MIN", "MAX"):
                if col not in table.column_names:
                    result_data[alias] = [None]
                    continue
                    
                column = table.column(col)
                
                # Use PyArrow compute for efficiency
                if func == "SUM":
                    result_data[alias] = [pc.sum(column).as_py()]
                elif func == "AVG":
                    result_data[alias] = [pc.mean(column).as_py()]
                elif func == "MIN":
                    result_data[alias] = [pc.min(column).as_py()]
                elif func == "MAX":
                    result_data[alias] = [pc.max(column).as_py()]
        
        return pa.table(result_data)
    
    def _aggregate_with_groupby(
        self,
        table: pa.Table,
        group_by: List[str],
        aggregates: List[Any],
    ) -> pa.Table:
        """
        Aggregation with GROUP BY using PyArrow native grouping.
        """
        arrow_aggs = []
        aliases = []
        
        for agg in aggregates:
            func = agg.func.lower()
            if func == "avg": func = "mean"
            
            col = agg.column
            if col is None or col == "*":
                # For count(*), we can pick any column or let pyarrow handle "count"
                # PyArrow "count" works on a column.
                if func == "count":
                    col = table.column_names[0]
            
            alias = agg.alias or f"{agg.func.upper()}({agg.column})"
            
            # (column, function)
            arrow_aggs.append((col, func))
            aliases.append(alias)
            
        # Perform aggregation
        grouped = table.group_by(group_by)
        result = grouped.aggregate(arrow_aggs)
        
        # PyArrow result columns: group_by cols + agg cols
        # Agg cols usually named like "{col}_{func}".
        # We need to rename the aggregation columns to match our aliases.
        # The first N columns are the group_by columns.
        
        current_names = result.column_names
        # We expect len(current_names) == len(group_by) + len(arrow_aggs)
        
        # Build new column mappings
        # Keep group_by info as is
        new_names = current_names[:len(group_by)]
        new_names.extend(aliases)
        
        if len(new_names) == len(current_names):
             result = result.rename_columns(new_names)
             
        return result
    
    def _compute_approximate_aggregates(
        self,
        table: pa.Table,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
        sample_size: int = None,
    ) -> pa.Table:
        """
        Compute approximate aggregates using sampling.
        
        For very large datasets where exact counts aren't critical,
        this provides a fast approximation by sampling.
        
        Args:
            table: Input Arrow table
            group_by: Columns to group by
            aggregates: List of Aggregate objects
            sample_size: Number of rows to sample (default: APPROXIMATE_AGGREGATION_SAMPLE_SIZE)
            
        Returns:
            Aggregated Arrow table with approximate values
        """
        import pyarrow.compute as pc
        import logging
        
        logger = logging.getLogger(__name__)
        
        if not aggregates:
            return table
        
        sample_size = sample_size or self.APPROXIMATE_AGGREGATION_SAMPLE_SIZE
        total_rows = len(table)
        
        if total_rows <= sample_size:
            # No need to sample, compute exact
            return self._compute_client_side_aggregates(table, group_by, aggregates)
        
        # Sample the data
        import random
        indices = sorted(random.sample(range(total_rows), sample_size))
        sampled_table = table.take(indices)
        
        # Compute aggregates on sample
        result = self._compute_client_side_aggregates(sampled_table, group_by, aggregates)
        
        # Adjust COUNT and SUM based on sampling ratio
        sampling_ratio = total_rows / sample_size
        
        alias_to_func = {}
        for agg in aggregates:
            alias = agg.alias or f"{agg.func.upper()}({agg.column})"
            alias_to_func[alias] = agg.func.upper()
            
        output_arrays = []
        output_names = result.column_names
        
        for col_name in output_names:
            column = result.column(col_name)
            # Check if this column corresponds to an aggregate function
            func = alias_to_func.get(col_name)
            
            if func == "COUNT":
                # Scale and cast to int64
                scaled = pc.multiply(column, sampling_ratio)
                output_arrays.append(pc.cast(scaled, pa.int64()))
            elif func == "SUM":
                # Scale
                # Check type, if int, maybe stay int or float? Usually SUM can be float.
                output_arrays.append(pc.multiply(column, sampling_ratio))
            else:
                # AVG, MIN, MAX or Group Key - keep as is
                output_arrays.append(column)
        
        result_scaled = pa.Table.from_arrays(output_arrays, names=output_names)
        
        logger.info(
            f"[{self.adapter_name}] Approximate aggregation: sampled {sample_size:,} of {total_rows:,} rows "
            f"(ratio: {sampling_ratio:.2f}x)"
        )
        
        return result_scaled

    def discover_relationships(self) -> List[Any]:
        """
        Discover potential relationships (Foreign Keys) within this adapter.
        
        Returns:
            List of RelationshipContract objects
        """
        return []

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"<{self.__class__.__name__} host={self._host} pool={self._use_connection_pool}>"
