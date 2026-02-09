"""
Chunked Executor - Smart Chunking for Bind Joins

Automatically splits large IN (...) predicates into micro-batches to avoid:
- HTTP 414 URI Too Long errors
- API payload size limits
- Rate limit exhaustion from massive single requests

Features:
- Configurable chunk sizes based on adapter capabilities
- Parallel execution of chunks with ThreadPoolExecutor
- Automatic result merging with deduplication
- Progress tracking for long-running chunked queries
"""

from __future__ import annotations
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from waveql.query_planner import Predicate
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


# Default chunk sizes by adapter type (based on typical API limits)
# These are conservative defaults - adapters can override
DEFAULT_CHUNK_SIZES = {
    "servicenow": 100,      # ServiceNow sysparm_query has character limits
    "salesforce": 200,      # SOQL IN clause limit
    "zendesk": 100,         # Zendesk API query limits
    "jira": 100,            # JQL has query length limits
    "hubspot": 100,         # HubSpot batch API limits
    "github": 50,           # GitHub GraphQL complexity limits
    "rest": 100,            # Generic REST default
    "default": 100,         # Safe default
}

# Maximum URL length before risking 414 errors (most servers: 8KB, IIS: 16KB)
MAX_ESTIMATED_URL_LENGTH = 6000  # Leave buffer for other query params


@dataclass
class ChunkConfig:
    """Configuration for chunked execution."""
    
    # Maximum values per IN clause chunk
    max_chunk_size: int = 100
    
    # Maximum parallel workers
    max_workers: int = 4
    
    # Threshold: Only chunk if IN list exceeds this size
    chunk_threshold: int = 50
    
    # Estimated bytes per value in URL encoding (for URL length estimation)
    estimated_bytes_per_value: int = 40
    
    # Whether to deduplicate results (useful for overlapping chunks)
    deduplicate: bool = False
    
    # Primary key columns for deduplication (if deduplicate=True)
    primary_keys: List[str] = field(default_factory=list)
    
    # Progress callback: fn(completed_chunks, total_chunks, rows_so_far)
    progress_callback: Optional[Callable[[int, int, int], None]] = None


@dataclass
class ChunkResult:
    """Result from a single chunk execution."""
    chunk_index: int
    data: Optional[pa.Table]
    row_count: int
    duration: float
    error: Optional[str] = None
    rate_limited: bool = False


class ChunkedExecutor:
    """
    Executes queries with large IN predicates by splitting into parallel chunks.
    
    Usage:
        executor = ChunkedExecutor(adapter, config)
        result = executor.execute_chunked(
            table="incident",
            columns=["sys_id", "number", "short_description"],
            predicates=[Predicate("sys_id", "IN", large_list_of_ids)],
            other_predicates=[Predicate("active", "=", True)],
        )
    """
    
    def __init__(
        self,
        adapter: "BaseAdapter",
        config: ChunkConfig = None,
    ):
        self.adapter = adapter
        self.config = config or ChunkConfig()
        
        # Auto-detect chunk size from adapter type if not specified
        if config is None:
            adapter_name = getattr(adapter, "adapter_name", "default").lower()
            self.config.max_chunk_size = DEFAULT_CHUNK_SIZES.get(
                adapter_name, 
                DEFAULT_CHUNK_SIZES["default"]
            )
    
    def should_chunk(self, predicates: List["Predicate"]) -> bool:
        """
        Determine if chunking is needed for the given predicates.
        
        Returns True if any IN predicate has more values than the threshold.
        """
        for pred in predicates:
            if pred.operator.upper() == "IN" and isinstance(pred.value, (list, tuple)):
                if len(pred.value) > self.config.chunk_threshold:
                    return True
                    
                # Also check estimated URL length
                estimated_length = self._estimate_url_length(pred.value)
                if estimated_length > MAX_ESTIMATED_URL_LENGTH:
                    return True
        
        return False
    
    def _estimate_url_length(self, values: List[Any]) -> int:
        """Estimate the URL-encoded length of an IN clause."""
        total = 0
        for v in values:
            # Account for URL encoding: quotes, commas, special chars
            if isinstance(v, str):
                total += len(v) * 3  # URL encoding can triple string length
            else:
                total += len(str(v)) + 2  # numeric + padding
        return total
    
    def _split_in_predicate(
        self, 
        predicate: "Predicate",
        chunk_size: int = None,
    ) -> List["Predicate"]:
        """
        Split a single IN predicate into multiple smaller IN predicates.
        
        Args:
            predicate: Original IN predicate with large value list
            chunk_size: Override chunk size (uses config default if None)
            
        Returns:
            List of IN predicates, each with at most chunk_size values
        """
        from waveql.query_planner import Predicate
        
        if predicate.operator.upper() != "IN":
            return [predicate]
        
        values = predicate.value
        if not isinstance(values, (list, tuple)):
            return [predicate]
        
        chunk_size = chunk_size or self.config.max_chunk_size
        
        # Adjust chunk size based on estimated URL length
        if self._estimate_url_length(values[:chunk_size]) > MAX_ESTIMATED_URL_LENGTH:
            # Reduce chunk size to fit URL limits
            chunk_size = max(10, chunk_size // 2)
            logger.debug(
                "Reduced chunk size to %d due to URL length limits",
                chunk_size
            )
        
        chunks = []
        for i in range(0, len(values), chunk_size):
            chunk_values = values[i:i + chunk_size]
            chunks.append(Predicate(
                column=predicate.column,
                operator="IN",
                value=list(chunk_values),
            ))
        
        logger.info(
            "Split IN predicate on column '%s' into %d chunks (%d values -> %d per chunk)",
            predicate.column,
            len(chunks),
            len(values),
            chunk_size,
        )
        
        return chunks
    
    def execute_chunked(
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
        Execute a query with automatic chunking for large IN predicates.
        
        Identifies IN predicates that exceed thresholds, splits them into
        chunks, executes in parallel, and merges results.
        
        Args:
            table: Table name
            columns: Columns to fetch
            predicates: List of predicates (may contain large IN clauses)
            limit: Result limit (applied after merge)
            offset: Result offset (applied after merge)
            order_by: Order specification (applied after merge)
            group_by: Group by columns
            aggregates: Aggregation functions
            
        Returns:
            Merged PyArrow Table with all results
        """
        predicates = predicates or []
        
        # Find IN predicates that need chunking
        in_predicate = None
        other_predicates = []
        
        for i, pred in enumerate(predicates):
            if (pred.operator.upper() == "IN" 
                and isinstance(pred.value, (list, tuple))
                and len(pred.value) > self.config.chunk_threshold):
                # Take the first large IN predicate for chunking
                if in_predicate is None:
                    in_predicate = pred
                else:
                    # Multiple large IN predicates - keep the larger one
                    if len(pred.value) > len(in_predicate.value):
                        other_predicates.append(in_predicate)
                        in_predicate = pred
                    else:
                        other_predicates.append(pred)
            else:
                other_predicates.append(pred)
        
        if in_predicate is None:
            # No chunking needed - execute normally
            return self.adapter.fetch(
                table=table,
                columns=columns,
                predicates=predicates,
                limit=limit,
                offset=offset,
                order_by=order_by,
                group_by=group_by,
                aggregates=aggregates,
            )
        
        # Split the large IN predicate into chunks
        predicate_chunks = self._split_in_predicate(in_predicate)
        total_chunks = len(predicate_chunks)
        
        logger.info(
            "Executing chunked query: table=%s, chunks=%d, max_workers=%d",
            table,
            total_chunks,
            self.config.max_workers,
        )
        
        # Execute chunks in parallel
        results: List[ChunkResult] = []
        rows_fetched = 0
        
        def fetch_chunk(chunk_idx: int, chunk_pred: "Predicate") -> ChunkResult:
            """Execute a single chunk."""
            start_time = time.perf_counter()
            try:
                # Combine chunk predicate with other predicates
                chunk_predicates = [chunk_pred] + other_predicates
                
                data = self.adapter.fetch(
                    table=table,
                    columns=columns,
                    predicates=chunk_predicates,
                    # Don't apply limit/offset to individual chunks
                    limit=None,
                    offset=None,
                    order_by=None,  # Apply order after merge
                    group_by=group_by,
                    aggregates=aggregates,
                )
                
                duration = time.perf_counter() - start_time
                row_count = len(data) if data is not None else 0
                
                return ChunkResult(
                    chunk_index=chunk_idx,
                    data=data,
                    row_count=row_count,
                    duration=duration,
                )
                
            except Exception as e:
                duration = time.perf_counter() - start_time
                error_msg = str(e)
                rate_limited = "429" in error_msg or "rate" in error_msg.lower()
                
                logger.warning(
                    "Chunk %d failed: %s (rate_limited=%s)",
                    chunk_idx, error_msg, rate_limited
                )
                
                return ChunkResult(
                    chunk_index=chunk_idx,
                    data=None,
                    row_count=0,
                    duration=duration,
                    error=error_msg,
                    rate_limited=rate_limited,
                )
        
        # Use ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(fetch_chunk, i, pred): i
                for i, pred in enumerate(predicate_chunks)
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_chunk):
                result = future.result()
                results.append(result)
                completed += 1
                
                if result.data is not None:
                    rows_fetched += result.row_count
                
                # Progress callback
                if self.config.progress_callback:
                    self.config.progress_callback(
                        completed, total_chunks, rows_fetched
                    )
                
                logger.debug(
                    "Chunk %d/%d complete: %d rows in %.3fs%s",
                    completed,
                    total_chunks,
                    result.row_count,
                    result.duration,
                    " (FAILED)" if result.error else "",
                )
        
        # Sort results by chunk index to maintain order
        results.sort(key=lambda r: r.chunk_index)
        
        # Merge results
        merged = self._merge_results(results)
        
        if merged is None or len(merged) == 0:
            return pa.table({})
        
        # Apply post-merge operations
        merged = self._apply_post_merge(
            merged,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
        
        logger.info(
            "Chunked execution complete: %d chunks, %d total rows, %d after limit/offset",
            total_chunks,
            rows_fetched,
            len(merged),
        )
        
        return merged
    
    def _merge_results(self, results: List[ChunkResult]) -> Optional[pa.Table]:
        """
        Merge chunk results into a single Arrow table.
        
        Handles:
        - Concatenation of successful chunk results
        - Optional deduplication based on primary keys
        - Schema alignment for heterogeneous results
        """
        # Filter successful results
        tables = [r.data for r in results if r.data is not None and len(r.data) > 0]
        
        if not tables:
            return None
        
        if len(tables) == 1:
            merged = tables[0]
        else:
            # Concatenate all tables
            try:
                merged = pa.concat_tables(tables, promote_options="default")
            except pa.ArrowInvalid as e:
                # Schema mismatch - try to align schemas
                logger.warning("Schema mismatch during merge, attempting alignment: %s", e)
                merged = self._align_and_concat(tables)
        
        # Deduplicate if configured
        if self.config.deduplicate and self.config.primary_keys:
            merged = self._deduplicate(merged, self.config.primary_keys)
        
        return merged
    
    def _align_and_concat(self, tables: List[pa.Table]) -> pa.Table:
        """
        Align schemas and concatenate tables with different column sets.
        
        Adds null columns where needed to create a unified schema.
        """
        # Find union of all columns
        all_columns = {}
        for table in tables:
            for field in table.schema:
                if field.name not in all_columns:
                    all_columns[field.name] = field.type
        
        # Align each table to the unified schema
        aligned_tables = []
        for table in tables:
            aligned_arrays = []
            aligned_names = []
            
            for col_name, col_type in all_columns.items():
                if col_name in table.column_names:
                    aligned_arrays.append(table.column(col_name))
                else:
                    # Add null column
                    null_array = pa.nulls(len(table), type=col_type)
                    aligned_arrays.append(null_array)
                aligned_names.append(col_name)
            
            aligned_tables.append(pa.table(dict(zip(aligned_names, aligned_arrays))))
        
        return pa.concat_tables(aligned_tables)
    
    def _deduplicate(self, table: pa.Table, primary_keys: List[str]) -> pa.Table:
        """
        Deduplicate table based on primary key columns.
        
        Uses DuckDB for efficient deduplication if available.
        """
        try:
            import duckdb
            
            # Use DuckDB for efficient deduplication
            conn = duckdb.connect(":memory:")
            conn.register("data", table)
            
            pk_cols = ", ".join([f'"{pk}"' for pk in primary_keys])
            all_cols = ", ".join([f'"{c}"' for c in table.column_names])
            
            result = conn.execute(f"""
                SELECT {all_cols}
                FROM data
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk_cols} ORDER BY 1) = 1
            """).fetch_arrow_table()
            
            conn.close()
            return result
            
        except Exception as e:
            logger.warning("DuckDB deduplication failed, using fallback: %s", e)
            # Fallback: Keep first occurrence using pandas
            df = table.to_pandas()
            df = df.drop_duplicates(subset=primary_keys, keep="first")
            return pa.Table.from_pandas(df, preserve_index=False)
    
    def _apply_post_merge(
        self,
        table: pa.Table,
        order_by: List[tuple] = None,
        limit: int = None,
        offset: int = None,
    ) -> pa.Table:
        """
        Apply ordering, limit, and offset after merging chunks.
        
        Uses DuckDB for efficient post-processing.
        """
        if not order_by and limit is None and offset is None:
            return table
        
        try:
            import duckdb
            
            conn = duckdb.connect(":memory:")
            conn.register("data", table)
            
            sql = "SELECT * FROM data"
            
            if order_by:
                order_clauses = []
                for col, direction in order_by:
                    direction = direction.upper() if direction else "ASC"
                    order_clauses.append(f'"{col}" {direction}')
                sql += " ORDER BY " + ", ".join(order_clauses)
            
            if limit is not None:
                sql += f" LIMIT {limit}"
            
            if offset is not None:
                sql += f" OFFSET {offset}"
            
            result = conn.execute(sql).fetch_arrow_table()
            conn.close()
            return result
            
        except Exception as e:
            logger.warning("DuckDB post-processing failed, using fallback: %s", e)
            # Fallback: Use pandas
            df = table.to_pandas()
            
            if order_by:
                cols = [col for col, _ in order_by]
                ascending = [d.upper() != "DESC" if d else True for _, d in order_by]
                df = df.sort_values(by=cols, ascending=ascending)
            
            if offset:
                df = df.iloc[offset:]
            
            if limit:
                df = df.head(limit)
            
            return pa.Table.from_pandas(df, preserve_index=False)


def get_optimal_chunk_size(
    adapter_name: str,
    estimated_value_length: int = 20,
) -> int:
    """
    Calculate optimal chunk size based on adapter and value characteristics.
    
    Args:
        adapter_name: Name of the adapter
        estimated_value_length: Average length of values in the IN clause
        
    Returns:
        Recommended chunk size
    """
    base_size = DEFAULT_CHUNK_SIZES.get(adapter_name.lower(), DEFAULT_CHUNK_SIZES["default"])
    
    # Adjust based on value length
    # For very long values (like UUIDs), reduce chunk size
    if estimated_value_length > 36:  # UUID length
        base_size = max(10, base_size // 2)
    elif estimated_value_length > 50:
        base_size = max(5, base_size // 3)
    
    # Calculate based on URL length limit
    max_by_url = MAX_ESTIMATED_URL_LENGTH // (estimated_value_length * 3)  # URL encoding
    
    return min(base_size, max_by_url)
