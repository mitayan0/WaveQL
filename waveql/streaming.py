"""
WaveQL Streaming - Memory-efficient streaming for large result sets.

Features:
- Generator-based RecordBatch yielding for sync operations
- AsyncIterator-based streaming with backpressure support
- Memory-efficient fetching for million-row exports
- Configurable batch sizes for optimal performance

Usage (Sync):
    for batch in cursor.stream_batches("SELECT * FROM large_table"):
        for row in batch.to_pylist():
            process(row)

Usage (Async):
    async for batch in cursor.stream_batches_async("SELECT * FROM large_table"):
        for row in batch.to_pylist():
            await process_async(row)

Usage (Export):
    cursor.stream_to_file("SELECT * FROM large_table", "output.parquet")
"""

from __future__ import annotations
import logging
from typing import (
    Any, AsyncIterator, Callable, Dict, Generator, Iterator, 
    List, Optional, TYPE_CHECKING, Union
)
from dataclasses import dataclass, field
import asyncio

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter
    from waveql.query_planner import QueryInfo

logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """Configuration for streaming operations."""
    
    # Number of records per batch
    batch_size: int = 1000
    
    # Maximum records to fetch total (None = unlimited)
    max_records: Optional[int] = None
    
    # Callback for progress reporting: fn(records_fetched, total_estimated)
    progress_callback: Optional[Callable[[int, Optional[int]], None]] = None
    
    # For async: maximum concurrent pages to buffer (backpressure control)
    max_buffer_size: int = 3
    
    # For exports: compression type for parquet files
    compression: str = "snappy"
    
    # Write row group size for parquet
    row_group_size: int = 100_000


@dataclass
class StreamStats:
    """Statistics about a streaming operation."""
    
    records_fetched: int = 0
    batches_yielded: int = 0
    bytes_transferred: int = 0
    pages_fetched: int = 0
    
    def __repr__(self) -> str:
        return (
            f"StreamStats(records={self.records_fetched:,}, "
            f"batches={self.batches_yielded}, pages={self.pages_fetched})"
        )


class RecordBatchStream:
    """
    Sync iterator that yields RecordBatches from a paginated API source.
    
    This is a generator-based stream that fetches data page by page,
    converts to Arrow RecordBatches, and yields them without loading
    the entire result set into memory.
    """
    
    def __init__(
        self,
        adapter: "BaseAdapter",
        table: str,
        columns: List[str] = None,
        predicates: List = None,
        order_by: List[tuple] = None,
        config: StreamConfig = None,
    ):
        self._adapter = adapter
        self._table = table
        self._columns = columns
        self._predicates = predicates
        self._order_by = order_by
        self._config = config or StreamConfig()
        self._stats = StreamStats()
        self._schema: Optional[pa.Schema] = None
        self._exhausted = False
        self._offset = 0
    
    @property
    def stats(self) -> StreamStats:
        """Get current streaming statistics."""
        return self._stats
    
    @property
    def schema(self) -> Optional[pa.Schema]:
        """Get the Arrow schema (available after first batch)."""
        return self._schema
    
    def __iter__(self) -> Iterator[pa.RecordBatch]:
        """Iterate over RecordBatches."""
        return self._generate_batches()
    
    def _generate_batches(self) -> Generator[pa.RecordBatch, None, None]:
        """Generator that yields RecordBatches."""
        offset = 0
        total_fetched = 0
        max_records = self._config.max_records
        batch_size = self._config.batch_size
        
        while True:
            # Check if we've hit the max records limit
            if max_records and total_fetched >= max_records:
                break
            
            # Adjust batch size for final batch if needed
            current_limit = batch_size
            if max_records:
                remaining = max_records - total_fetched
                current_limit = min(batch_size, remaining)
            
            # Fetch a page of data
            try:
                table = self._adapter.fetch(
                    self._table,
                    columns=self._columns,
                    predicates=self._predicates,
                    limit=current_limit,
                    offset=offset,
                    order_by=self._order_by,
                )
                self._stats.pages_fetched += 1
            except Exception as e:
                logger.error(f"Error fetching page at offset {offset}: {e}")
                raise
            
            # Check if we got any data
            if len(table) == 0:
                self._exhausted = True
                break
            
            # Store schema from first batch
            if self._schema is None:
                self._schema = table.schema
            
            # Convert table to record batches
            for batch in table.to_batches():
                self._stats.records_fetched += batch.num_rows
                self._stats.batches_yielded += 1
                self._stats.bytes_transferred += batch.nbytes
                
                # Report progress if callback provided
                if self._config.progress_callback:
                    self._config.progress_callback(
                        self._stats.records_fetched, 
                        max_records
                    )
                
                yield batch
            
            total_fetched += len(table)
            offset += len(table)
            
            # Check if this was the last page (got fewer records than requested)
            if len(table) < current_limit:
                self._exhausted = True
                break
        
        logger.info(f"Stream completed: {self._stats}")
    
    def to_arrow_table(self) -> pa.Table:
        """
        Collect all batches into a single Arrow Table.
        
        WARNING: This loads all data into memory. For large datasets,
        use the iterator interface instead.
        """
        batches = list(self)
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)
    
    def to_parquet(self, path: str) -> StreamStats:
        """
        Stream data directly to a Parquet file without loading into memory.
        
        Args:
            path: Output file path
            
        Returns:
            StreamStats with operation statistics
        """
        writer = None
        try:
            for batch in self:
                if writer is None:
                    writer = pq.ParquetWriter(
                        path,
                        batch.schema,
                        compression=self._config.compression,
                    )
                writer.write_batch(batch)
        finally:
            if writer:
                writer.close()
        
        logger.info(f"Wrote {self._stats.records_fetched:,} records to {path}")
        return self._stats


class AsyncRecordBatchStream:
    """
    Async iterator that yields RecordBatches with backpressure support.
    
    This uses an async buffer to prefetch pages while the consumer
    processes the current batch, providing efficient pipelining.
    """
    
    def __init__(
        self,
        adapter: "BaseAdapter",
        table: str,
        columns: List[str] = None,
        predicates: List = None,
        order_by: List[tuple] = None,
        config: StreamConfig = None,
    ):
        self._adapter = adapter
        self._table = table
        self._columns = columns
        self._predicates = predicates
        self._order_by = order_by
        self._config = config or StreamConfig()
        self._stats = StreamStats()
        self._schema: Optional[pa.Schema] = None
        self._buffer: asyncio.Queue = None
        self._producer_task: Optional[asyncio.Task] = None
        self._exhausted = False
        self._error: Optional[Exception] = None
    
    @property
    def stats(self) -> StreamStats:
        """Get current streaming statistics."""
        return self._stats
    
    @property
    def schema(self) -> Optional[pa.Schema]:
        """Get the Arrow schema (available after first batch)."""
        return self._schema
    
    def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        """Start async iteration."""
        return self._generate_batches_async()
    
    async def _generate_batches_async(self) -> AsyncIterator[pa.RecordBatch]:
        """Async generator that yields RecordBatches with backpressure."""
        offset = 0
        total_fetched = 0
        max_records = self._config.max_records
        batch_size = self._config.batch_size
        
        while True:
            # Check if we've hit the max records limit
            if max_records and total_fetched >= max_records:
                break
            
            # Adjust batch size for final batch if needed
            current_limit = batch_size
            if max_records:
                remaining = max_records - total_fetched
                current_limit = min(batch_size, remaining)
            
            # Fetch a page of data (async)
            try:
                table = await self._adapter.fetch_async(
                    self._table,
                    columns=self._columns,
                    predicates=self._predicates,
                    limit=current_limit,
                    offset=offset,
                    order_by=self._order_by,
                )
                self._stats.pages_fetched += 1
            except Exception as e:
                logger.error(f"Error fetching page at offset {offset}: {e}")
                raise
            
            # Check if we got any data
            if len(table) == 0:
                self._exhausted = True
                break
            
            # Store schema from first batch
            if self._schema is None:
                self._schema = table.schema
            
            # Convert table to record batches and yield
            for batch in table.to_batches():
                self._stats.records_fetched += batch.num_rows
                self._stats.batches_yielded += 1
                self._stats.bytes_transferred += batch.nbytes
                
                # Report progress if callback provided
                if self._config.progress_callback:
                    self._config.progress_callback(
                        self._stats.records_fetched, 
                        max_records
                    )
                
                # Yield and allow consumer to process (backpressure point)
                yield batch
                await asyncio.sleep(0)  # Yield control to event loop
            
            total_fetched += len(table)
            offset += len(table)
            
            # Check if this was the last page
            if len(table) < current_limit:
                self._exhausted = True
                break
        
        logger.info(f"Async stream completed: {self._stats}")
    
    async def to_arrow_table(self) -> pa.Table:
        """
        Collect all batches into a single Arrow Table.
        
        WARNING: This loads all data into memory.
        """
        batches = []
        async for batch in self:
            batches.append(batch)
        
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)
    
    async def to_parquet(self, path: str) -> StreamStats:
        """
        Stream data directly to a Parquet file without loading into memory.
        
        Args:
            path: Output file path
            
        Returns:
            StreamStats with operation statistics
        """
        writer = None
        try:
            async for batch in self:
                if writer is None:
                    writer = pq.ParquetWriter(
                        path,
                        batch.schema,
                        compression=self._config.compression,
                    )
                writer.write_batch(batch)
        finally:
            if writer:
                writer.close()
        
        logger.info(f"Async: Wrote {self._stats.records_fetched:,} records to {path}")
        return self._stats


class BufferedAsyncStream:
    """
    Async stream with prefetching buffer for maximum throughput.
    
    This implementation uses a producer-consumer pattern with an async queue
    to prefetch pages while the consumer processes the current batch.
    """
    
    def __init__(
        self,
        adapter: "BaseAdapter",
        table: str,
        columns: List[str] = None,
        predicates: List = None,
        order_by: List[tuple] = None,
        config: StreamConfig = None,
    ):
        self._adapter = adapter
        self._table = table
        self._columns = columns
        self._predicates = predicates
        self._order_by = order_by
        self._config = config or StreamConfig()
        self._stats = StreamStats()
        self._schema: Optional[pa.Schema] = None
    
    @property
    def stats(self) -> StreamStats:
        return self._stats
    
    async def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate with prefetching."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._config.max_buffer_size)
        producer_done = asyncio.Event()
        error_holder: Dict[str, Exception] = {}
        
        async def producer():
            """Fetch pages and put them in the queue."""
            offset = 0
            total_fetched = 0
            max_records = self._config.max_records
            batch_size = self._config.batch_size
            
            try:
                while True:
                    if max_records and total_fetched >= max_records:
                        break
                    
                    current_limit = batch_size
                    if max_records:
                        remaining = max_records - total_fetched
                        current_limit = min(batch_size, remaining)
                    
                    table = await self._adapter.fetch_async(
                        self._table,
                        columns=self._columns,
                        predicates=self._predicates,
                        limit=current_limit,
                        offset=offset,
                        order_by=self._order_by,
                    )
                    self._stats.pages_fetched += 1
                    
                    if len(table) == 0:
                        break
                    
                    if self._schema is None:
                        self._schema = table.schema
                    
                    # Put batches in queue (will block if queue is full - backpressure!)
                    for batch in table.to_batches():
                        await queue.put(batch)
                    
                    total_fetched += len(table)
                    offset += len(table)
                    
                    if len(table) < current_limit:
                        break
                        
            except Exception as e:
                error_holder["error"] = e
            finally:
                producer_done.set()
        
        # Start producer task
        producer_task = asyncio.create_task(producer())
        
        # Consume from queue
        try:
            while True:
                # Check for errors
                if "error" in error_holder:
                    raise error_holder["error"]
                
                # Try to get from queue with timeout
                try:
                    batch = await asyncio.wait_for(
                        queue.get(),
                        timeout=0.1
                    )
                    self._stats.records_fetched += batch.num_rows
                    self._stats.batches_yielded += 1
                    self._stats.bytes_transferred += batch.nbytes
                    
                    if self._config.progress_callback:
                        self._config.progress_callback(
                            self._stats.records_fetched,
                            self._config.max_records
                        )
                    
                    yield batch
                    
                except asyncio.TimeoutError:
                    # Check if producer is done
                    if producer_done.is_set() and queue.empty():
                        break
        finally:
            producer_task.cancel()
            try:
                await producer_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Buffered stream completed: {self._stats}")


def create_stream(
    adapter: "BaseAdapter",
    table: str,
    columns: List[str] = None,
    predicates: List = None,
    order_by: List[tuple] = None,
    config: StreamConfig = None,
    use_buffer: bool = False,
) -> Union[RecordBatchStream, AsyncRecordBatchStream, BufferedAsyncStream]:
    """
    Factory function to create appropriate stream type.
    
    Args:
        adapter: The adapter to fetch data from
        table: Table name
        columns: Columns to select
        predicates: WHERE predicates
        order_by: ORDER BY clauses
        config: Stream configuration
        use_buffer: If True, use BufferedAsyncStream for async (prefetching)
        
    Returns:
        RecordBatchStream for sync, AsyncRecordBatchStream/BufferedAsyncStream for async
    """
    # Check if adapter supports async
    has_async = hasattr(adapter, 'fetch_async') and callable(getattr(adapter, 'fetch_async'))
    
    if use_buffer and has_async:
        return BufferedAsyncStream(
            adapter, table, columns, predicates, order_by, config
        )
    elif has_async:
        return AsyncRecordBatchStream(
            adapter, table, columns, predicates, order_by, config
        )
    else:
        return RecordBatchStream(
            adapter, table, columns, predicates, order_by, config
        )
