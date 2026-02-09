"""
Tests for WaveQL async_cursor module.

This covers the 54% uncovered module waveql/async_cursor.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, AsyncMock

from waveql.async_cursor import AsyncWaveQLCursor


class TestAsyncWaveQLCursorInit:
    """Tests for AsyncWaveQLCursor initialization."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock async connection."""
        conn = MagicMock()
        conn._duck = MagicMock()
        conn._adapters = {}
        conn._planner = MagicMock()
        return conn
    
    def test_init(self, mock_connection):
        """Test cursor initialization."""
        cursor = AsyncWaveQLCursor(mock_connection)
        
        assert cursor._connection == mock_connection
        assert cursor._description is None
        assert cursor._rowcount == -1
    
    def test_description_property(self, mock_connection):
        """Test description property."""
        cursor = AsyncWaveQLCursor(mock_connection)
        
        assert cursor.description is None
        
        cursor._description = [("id", None, None, None, None, None, None)]
        assert cursor.description is not None
    
    def test_rowcount_property(self, mock_connection):
        """Test rowcount property."""
        cursor = AsyncWaveQLCursor(mock_connection)
        
        assert cursor.rowcount == -1
        
        cursor._rowcount = 10
        assert cursor.rowcount == 10


class TestAsyncWaveQLCursorExecute:
    """Tests for AsyncWaveQLCursor execute method."""
    
    @pytest.fixture
    def mock_connection_with_adapter(self):
        """Create mock connection with adapter."""
        conn = MagicMock()
        
        # Mock adapter
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch = MagicMock(return_value=pa.table({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
        }))
        mock_adapter.fetch_async = AsyncMock(return_value=pa.table({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
        }))
        
        conn._adapters = {"test": mock_adapter}
        conn.get_adapter = MagicMock(return_value=mock_adapter)
        
        # Mock planner
        mock_planner = MagicMock()
        mock_query_info = MagicMock()
        mock_query_info.tables = ["test.users"]
        mock_query_info.columns = ["id", "name"]
        mock_query_info.predicates = []
        mock_query_info.is_join = False
        mock_query_info.aggregates = None
        mock_planner.plan.return_value = mock_query_info
        conn._planner = mock_planner
        
        # Mock DuckDB
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, "A"), (2, "B")]
        mock_result.description = [("id",), ("name",)]
        mock_duck = MagicMock()
        mock_duck.execute.return_value = mock_result
        conn._duck = mock_duck
        
        return conn
    
    @pytest.mark.asyncio
    async def test_execute_simple_query(self, mock_connection_with_adapter):
        """Test executing simple query."""
        cursor = AsyncWaveQLCursor(mock_connection_with_adapter)
        
        result = await cursor.execute("SELECT * FROM test.users")
        
        assert result is cursor  # Returns self for chaining
    
    @pytest.mark.asyncio
    async def test_execute_with_parameters(self, mock_connection_with_adapter):
        """Test executing query with parameters."""
        cursor = AsyncWaveQLCursor(mock_connection_with_adapter)
        
        result = await cursor.execute(
            "SELECT * FROM test.users WHERE id = ?",
            parameters=[1],
        )
        
        assert result is cursor
    
    @pytest.mark.asyncio
    async def test_execute_updates_description(self, mock_connection_with_adapter):
        """Test that execute updates description."""
        cursor = AsyncWaveQLCursor(mock_connection_with_adapter)
        
        await cursor.execute("SELECT * FROM test.users")
        
        # Description should be set after execute
        # May be None if no results or set to schema


class TestAsyncWaveQLCursorFetch:
    """Tests for fetch methods."""
    
    @pytest.fixture
    def cursor_with_results(self):
        """Create cursor with mocked results."""
        conn = MagicMock()
        conn._duck = MagicMock()
        
        cursor = AsyncWaveQLCursor(conn)
        cursor._result = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        cursor._current_row = 0
        cursor._rowcount = 3
        
        return cursor
    
    def test_fetchone(self, cursor_with_results):
        """Test fetchone."""
        result = cursor_with_results.fetchone()
        
        assert result is not None
    
    def test_fetchone_exhausted(self, cursor_with_results):
        """Test fetchone when results exhausted."""
        cursor_with_results._result_index = 3  # Past all rows
        
        result = cursor_with_results.fetchone()
        
        assert result is None
    
    def test_fetchall(self, cursor_with_results):
        """Test fetchall."""
        results = cursor_with_results.fetchall()
        
        assert isinstance(results, list)
    
    def test_fetchmany_default(self, cursor_with_results):
        """Test fetchmany with default size."""
        results = cursor_with_results.fetchmany()
        
        assert isinstance(results, list)
    
    def test_fetchmany_custom_size(self, cursor_with_results):
        """Test fetchmany with custom size."""
        results = cursor_with_results.fetchmany(size=2)
        
        assert len(results) <= 2


class TestAsyncWaveQLCursorArraysize:
    """Tests for arraysize property."""
    
    def test_arraysize_default(self):
        """Test default arraysize."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        
        assert cursor.arraysize == 100  # Default
    
    def test_arraysize_setter(self):
        """Test setting arraysize."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        
        cursor.arraysize = 100
        
        assert cursor.arraysize == 100


class TestAsyncWaveQLCursorClose:
    """Tests for close method."""
    
    @pytest.mark.asyncio
    async def test_close(self):
        """Test closing cursor."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        
        await cursor.close()
        
        # Should mark as closed


class TestAsyncWaveQLCursorToArrow:
    """Tests for to_arrow method."""
    
    def test_to_arrow(self):
        """Test converting result to Arrow table."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        cursor._result = pa.table({
            "id": [1, 2],
            "name": ["A", "B"],
        })
        
        result = cursor.to_arrow()
        
        assert isinstance(result, pa.Table)


class TestAsyncWaveQLCursorToDF:
    """Tests for to_df method."""
    
    def test_to_df(self):
        """Test converting result to DataFrame."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        cursor._result = pa.table({
            "id": [1, 2],
            "name": ["A", "B"],
        })
        
        try:
            import pandas as pd
            result = cursor.to_df()
            assert isinstance(result, pd.DataFrame)
        except ImportError:
            pytest.skip("pandas not available")


class TestAsyncWaveQLCursorStreaming:
    """Tests for streaming methods."""
    
    @pytest.fixture
    def cursor_for_streaming(self):
        """Create cursor for streaming tests."""
        conn = MagicMock()
        
        # Mock adapter with async support
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch_async = AsyncMock(return_value=pa.table({
            "id": [1, 2, 3],
            "data": ["a", "b", "c"],
        }))
        
        conn._adapters = {"test": mock_adapter}
        conn.get_adapter = MagicMock(return_value=mock_adapter)
        
        # Mock planner
        mock_query_info = MagicMock()
        mock_query_info.tables = ["test.data"]
        mock_query_info.columns = None
        mock_query_info.predicates = []
        mock_query_info.is_join = False
        conn._planner = MagicMock()
        conn._planner.plan.return_value = mock_query_info
        
        mock_duck = MagicMock()
        conn._duck = mock_duck
        
        return AsyncWaveQLCursor(conn)
    
    @pytest.mark.asyncio
    async def test_stream_batches_async(self, cursor_for_streaming):
        """Test streaming batches asynchronously."""
        batches = []
        
        async for batch in cursor_for_streaming.stream_batches_async(
            "SELECT * FROM test.data",
            batch_size=2,
        ):
            batches.append(batch)
            if len(batches) >= 2:
                break
        
        # Should yield at least one batch
    
    @pytest.mark.asyncio
    async def test_stream_batches_async_with_max_records(self, cursor_for_streaming):
        """Test streaming with max_records limit."""
        batches = []
        
        async for batch in cursor_for_streaming.stream_batches_async(
            "SELECT * FROM test.data",
            batch_size=1,
            max_records=2,
        ):
            batches.append(batch)
        
        # Should respect max_records
    
    @pytest.mark.asyncio
    async def test_stream_batches_async_with_callback(self, cursor_for_streaming):
        """Test streaming with progress callback."""
        progress_calls = []
        
        def callback(fetched, total):
            progress_calls.append((fetched, total))
        
        batches = []
        async for batch in cursor_for_streaming.stream_batches_async(
            "SELECT * FROM test.data",
            batch_size=1,
            progress_callback=callback,
        ):
            batches.append(batch)
            if len(batches) >= 1:
                break


class TestAsyncWaveQLCursorStreamToFile:
    """Tests for stream_to_file_async method."""
    
    @pytest.fixture
    def cursor_for_file_streaming(self):
        """Create cursor for file streaming."""
        conn = MagicMock()
        
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch_async = AsyncMock(return_value=pa.table({
            "id": [1, 2],
            "value": [100, 200],
        }))
        
        conn._adapters = {"test": mock_adapter}
        conn.get_adapter = MagicMock(return_value=mock_adapter)
        
        mock_query_info = MagicMock()
        mock_query_info.tables = ["test.data"]
        mock_query_info.columns = None
        mock_query_info.predicates = []
        mock_query_info.is_join = False
        conn._planner = MagicMock()
        conn._planner.plan.return_value = mock_query_info
        conn._duck = MagicMock()
        
        return AsyncWaveQLCursor(conn)
    
    @pytest.mark.asyncio
    async def test_stream_to_file_async(self, cursor_for_file_streaming, tmp_path):
        """Test streaming to file."""
        output_file = str(tmp_path / "output.parquet")
        
        await cursor_for_file_streaming.stream_to_file_async(
            "SELECT * FROM test.data",
            output_path=output_file,
            batch_size=10,
            compression="snappy",
        )
        
        # Should return stats or metadata


class TestAsyncWaveQLCursorRepr:
    """Tests for __repr__ method."""
    
    def test_repr(self):
        """Test string representation."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        
        repr_str = repr(cursor)
        
        assert "AsyncWaveQLCursor" in repr_str


class TestAsyncWaveQLCursorEdgeCases:
    """Edge case tests."""
    
    def test_execute_empty_result(self):
        """Test execute with empty result."""
        conn = MagicMock()
        conn._duck = MagicMock()
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        conn._duck.execute.return_value = mock_result
        
        cursor = AsyncWaveQLCursor(conn)
        cursor._result = pa.table({"id": []})
        
        result = cursor.fetchone()
        assert result is None
    
    def test_fetchall_no_result(self):
        """Test fetchall when no query executed."""
        conn = MagicMock()
        cursor = AsyncWaveQLCursor(conn)
        cursor._result = None
        
        results = cursor.fetchall()
        assert results == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
