"""
Tests for WaveQL Streaming Module.
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, AsyncMock

from waveql.streaming import (
    StreamConfig,
    StreamStats,
    RecordBatchStream,
    AsyncRecordBatchStream,
    BufferedAsyncStream,
    create_stream,
)


@pytest.fixture
def mock_adapter():
    """Create a mock adapter for testing."""
    adapter = MagicMock()
    adapter.adapter_name = "test"
    
    # Create mock data
    def fetch_page(table, columns=None, predicates=None, limit=100, offset=0, order_by=None):
        if offset >= 300:  # Simulate 300 total records
            return pa.table({"id": [], "name": []})
        
        remaining = min(limit, 300 - offset)
        return pa.table({
            "id": list(range(offset, offset + remaining)),
            "name": [f"Item {i}" for i in range(offset, offset + remaining)],
        })
    
    adapter.fetch = MagicMock(side_effect=fetch_page)
    
    # Async version
    async def fetch_page_async(table, columns=None, predicates=None, limit=100, offset=0, order_by=None):
        return fetch_page(table, columns, predicates, limit, offset, order_by)
    
    adapter.fetch_async = AsyncMock(side_effect=fetch_page_async)
    
    return adapter


class TestStreamConfig:
    """Tests for StreamConfig."""
    
    def test_default_values(self):
        config = StreamConfig()
        assert config.batch_size == 1000
        assert config.max_records is None
        assert config.max_buffer_size == 3
        assert config.compression == "snappy"
    
    def test_custom_values(self):
        def callback(n, t):
            return print(n)
        config = StreamConfig(
            batch_size=500,
            max_records=10000,
            progress_callback=callback,
            max_buffer_size=5,
            compression="gzip",
        )
        assert config.batch_size == 500
        assert config.max_records == 10000
        assert config.progress_callback == callback
        assert config.max_buffer_size == 5
        assert config.compression == "gzip"


class TestStreamStats:
    """Tests for StreamStats."""
    
    def test_default_values(self):
        stats = StreamStats()
        assert stats.records_fetched == 0
        assert stats.batches_yielded == 0
        assert stats.bytes_transferred == 0
        assert stats.pages_fetched == 0
    
    def test_repr(self):
        stats = StreamStats(records_fetched=1000, batches_yielded=10, pages_fetched=10)
        repr_str = repr(stats)
        assert "1,000" in repr_str
        assert "batches=10" in repr_str


class TestRecordBatchStream:
    """Tests for sync RecordBatchStream."""
    
    def test_basic_streaming(self, mock_adapter):
        """Test basic streaming of RecordBatches."""
        config = StreamConfig(batch_size=100)
        stream = RecordBatchStream(
            adapter=mock_adapter,
            table="test_table",
            config=config,
        )
        
        batches = list(stream)
        
        assert len(batches) > 0
        assert stream.stats.records_fetched == 300
        assert stream.stats.pages_fetched >= 3
    
    def test_max_records_limit(self, mock_adapter):
        """Test that max_records limits the total fetched."""
        config = StreamConfig(batch_size=50, max_records=125)
        stream = RecordBatchStream(
            adapter=mock_adapter,
            table="test_table",
            config=config,
        )
        
        list(stream)
        
        # Should stop at 125 records
        assert stream.stats.records_fetched <= 150  # Due to batch boundaries
    
    def test_progress_callback(self, mock_adapter):
        """Test that progress callback is called."""
        progress_calls = []
        
        def callback(fetched, total):
            progress_calls.append((fetched, total))
        
        config = StreamConfig(batch_size=100, progress_callback=callback)
        stream = RecordBatchStream(
            adapter=mock_adapter,
            table="test_table",
            config=config,
        )
        
        list(stream)  # Consume the stream
        
        assert len(progress_calls) > 0
        # Progress should increase
        fetched_values = [c[0] for c in progress_calls]
        assert fetched_values == sorted(fetched_values)
    
    def test_schema_available(self, mock_adapter):
        """Test that schema is available after first batch."""
        stream = RecordBatchStream(
            adapter=mock_adapter,
            table="test_table",
            config=StreamConfig(batch_size=100),
        )
        
        # Schema should be None before iteration
        assert stream.schema is None
        
        # Get first batch
        next(iter(stream))
        
        # Schema should now be available
        assert stream.schema is not None
        assert "id" in [f.name for f in stream.schema]
        assert "name" in [f.name for f in stream.schema]


class TestAsyncRecordBatchStream:
    """Tests for async RecordBatchStream."""
    
    @pytest.mark.asyncio
    async def test_basic_async_streaming(self, mock_adapter):
        """Test basic async streaming of RecordBatches."""
        config = StreamConfig(batch_size=100)
        stream = AsyncRecordBatchStream(
            adapter=mock_adapter,
            table="test_table",
            config=config,
        )
        
        batches = []
        async for batch in stream:
            batches.append(batch)
        
        assert len(batches) > 0
        assert stream.stats.records_fetched == 300
    
    @pytest.mark.asyncio
    async def test_async_max_records(self, mock_adapter):
        """Test async streaming with max_records limit."""
        config = StreamConfig(batch_size=50, max_records=100)
        stream = AsyncRecordBatchStream(
            adapter=mock_adapter,
            table="test_table",
            config=config,
        )
        
        batches = []
        async for batch in stream:
            batches.append(batch)
        
        assert stream.stats.records_fetched == 100


class TestBufferedAsyncStream:
    """Tests for buffered async streaming."""
    
    @pytest.mark.asyncio
    async def test_buffered_streaming(self, mock_adapter):
        """Test buffered async streaming."""
        config = StreamConfig(batch_size=100, max_buffer_size=2)
        stream = BufferedAsyncStream(
            adapter=mock_adapter,
            table="test_table",
            config=config,
        )
        
        batches = []
        async for batch in stream:
            batches.append(batch)
        
        assert len(batches) > 0
        assert stream.stats.records_fetched == 300


class TestCreateStream:
    """Tests for stream factory function."""
    
    def test_create_sync_stream(self, mock_adapter):
        """Test creating a sync stream."""
        # Remove async capability to force sync
        del mock_adapter.fetch_async
        
        stream = create_stream(mock_adapter, "test_table")
        
        assert isinstance(stream, RecordBatchStream)
    
    def test_create_async_stream(self, mock_adapter):
        """Test creating an async stream."""
        stream = create_stream(mock_adapter, "test_table")
        
        # Should return async stream when adapter supports async
        assert isinstance(stream, AsyncRecordBatchStream)
    
    def test_create_buffered_async_stream(self, mock_adapter):
        """Test creating a buffered async stream."""
        stream = create_stream(mock_adapter, "test_table", use_buffer=True)
        
        assert isinstance(stream, BufferedAsyncStream)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
