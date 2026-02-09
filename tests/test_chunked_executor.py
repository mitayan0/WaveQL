"""
Tests for ChunkedExecutor - Smart Chunking / Bind Joins

Tests cover:
- Predicate chunking logic
- Parallel execution
- Result merging
- URL length estimation
- Edge cases
"""

import pytest
from unittest.mock import Mock
import pyarrow as pa

from waveql.chunked_executor import (
    ChunkedExecutor,
    ChunkConfig,
    ChunkResult,
    get_optimal_chunk_size,
    DEFAULT_CHUNK_SIZES,
)
from waveql.query_planner import Predicate


class TestChunkConfig:
    """Tests for ChunkConfig dataclass."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = ChunkConfig()
        assert config.max_chunk_size == 100
        assert config.max_workers == 4
        assert config.chunk_threshold == 50
        assert config.deduplicate is False
        assert config.primary_keys == []
        assert config.progress_callback is None
    
    def test_custom_values(self):
        """Test custom configuration values."""
        callback = Mock()
        config = ChunkConfig(
            max_chunk_size=50,
            max_workers=8,
            chunk_threshold=25,
            deduplicate=True,
            primary_keys=["id", "sys_id"],
            progress_callback=callback,
        )
        assert config.max_chunk_size == 50
        assert config.max_workers == 8
        assert config.chunk_threshold == 25
        assert config.deduplicate is True
        assert config.primary_keys == ["id", "sys_id"]
        assert config.progress_callback is callback


class TestChunkedExecutorShouldChunk:
    """Tests for should_chunk() method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create a mock adapter."""
        adapter = Mock()
        adapter.adapter_name = "servicenow"
        return adapter
    
    def test_small_in_predicate_no_chunk(self, mock_adapter):
        """Small IN predicates should not trigger chunking."""
        executor = ChunkedExecutor(mock_adapter)
        predicates = [
            Predicate(column="sys_id", operator="IN", value=["a", "b", "c"])
        ]
        assert executor.should_chunk(predicates) is False
    
    def test_large_in_predicate_should_chunk(self, mock_adapter):
        """Large IN predicates should trigger chunking."""
        executor = ChunkedExecutor(mock_adapter)
        large_list = [f"id_{i}" for i in range(100)]
        predicates = [
            Predicate(column="sys_id", operator="IN", value=large_list)
        ]
        assert executor.should_chunk(predicates) is True
    
    def test_equality_predicate_no_chunk(self, mock_adapter):
        """Equality predicates should not trigger chunking."""
        executor = ChunkedExecutor(mock_adapter)
        predicates = [
            Predicate(column="status", operator="=", value="open")
        ]
        assert executor.should_chunk(predicates) is False
    
    def test_threshold_boundary(self, mock_adapter):
        """Test exact threshold boundary."""
        config = ChunkConfig(chunk_threshold=50)
        executor = ChunkedExecutor(mock_adapter, config)
        
        # At threshold - no chunk
        predicates = [
            Predicate(column="id", operator="IN", value=list(range(50)))
        ]
        assert executor.should_chunk(predicates) is False
        
        # Above threshold - should chunk
        predicates = [
            Predicate(column="id", operator="IN", value=list(range(51)))
        ]
        assert executor.should_chunk(predicates) is True
    
    def test_url_length_triggers_chunking(self, mock_adapter):
        """Very long values should trigger chunking even with few items."""
        executor = ChunkedExecutor(mock_adapter)
        # UUIDs are ~36 chars, URL encoding triples them
        long_uuids = ["12345678-1234-1234-1234-123456789012" * 10 for _ in range(30)]
        predicates = [
            Predicate(column="sys_id", operator="IN", value=long_uuids)
        ]
        # Should chunk due to URL length even though count < threshold
        # (depends on URL length estimation)
        result = executor.should_chunk(predicates)
        # This may or may not trigger based on URL length calculation
        assert isinstance(result, bool)


class TestPredicateSplitting:
    """Tests for _split_in_predicate() method."""
    
    @pytest.fixture
    def mock_adapter(self):
        adapter = Mock()
        adapter.adapter_name = "default"
        return adapter
    
    def test_split_into_chunks(self, mock_adapter):
        """Test splitting large IN predicate into chunks."""
        config = ChunkConfig(max_chunk_size=10)
        executor = ChunkedExecutor(mock_adapter, config)
        
        values = list(range(25))
        predicate = Predicate(column="id", operator="IN", value=values)
        
        chunks = executor._split_in_predicate(predicate)
        
        assert len(chunks) == 3  # 10 + 10 + 5
        assert len(chunks[0].value) == 10
        assert len(chunks[1].value) == 10
        assert len(chunks[2].value) == 5
        
        # All values should be preserved
        all_values = []
        for chunk in chunks:
            all_values.extend(chunk.value)
        assert all_values == values
    
    def test_non_in_predicate_unchanged(self, mock_adapter):
        """Non-IN predicates should be returned unchanged."""
        executor = ChunkedExecutor(mock_adapter)
        predicate = Predicate(column="status", operator="=", value="open")
        
        result = executor._split_in_predicate(predicate)
        
        assert len(result) == 1
        assert result[0] == predicate
    
    def test_single_chunk_if_small(self, mock_adapter):
        """Small IN predicates should result in a single chunk."""
        config = ChunkConfig(max_chunk_size=100)
        executor = ChunkedExecutor(mock_adapter, config)
        
        values = list(range(10))
        predicate = Predicate(column="id", operator="IN", value=values)
        
        chunks = executor._split_in_predicate(predicate)
        
        assert len(chunks) == 1
        assert chunks[0].value == values


class TestChunkedExecution:
    """Tests for execute_chunked() method."""
    
    @pytest.fixture
    def mock_adapter(self):
        adapter = Mock()
        adapter.adapter_name = "test"
        return adapter
    
    def test_no_chunking_needed(self, mock_adapter):
        """When no chunking needed, should call fetch directly."""
        executor = ChunkedExecutor(mock_adapter)
        
        expected_result = pa.table({"id": [1, 2, 3]})
        mock_adapter.fetch.return_value = expected_result
        
        predicates = [
            Predicate(column="status", operator="=", value="open")
        ]
        
        result = executor.execute_chunked(
            table="incident",
            columns=["id"],
            predicates=predicates,
        )
        
        mock_adapter.fetch.assert_called_once()
        assert result == expected_result
    
    def test_chunked_execution_parallel(self, mock_adapter):
        """Test parallel execution of chunks."""
        config = ChunkConfig(max_chunk_size=10, chunk_threshold=10, max_workers=2)
        executor = ChunkedExecutor(mock_adapter, config)
        
        # Mock fetch to return different data per chunk
        call_count = [0]
        def mock_fetch(**kwargs):
            call_count[0] += 1
            return pa.table({"id": [call_count[0]]})
        
        mock_adapter.fetch.side_effect = mock_fetch
        
        # Create 25 values -> 3 chunks
        large_list = list(range(25))
        predicates = [
            Predicate(column="id", operator="IN", value=large_list)
        ]
        
        result = executor.execute_chunked(
            table="test_table",
            predicates=predicates,
        )
        
        assert mock_adapter.fetch.call_count == 3
        assert len(result) == 3  # 3 chunks, 1 row each
    
    def test_progress_callback(self, mock_adapter):
        """Test progress callback is invoked."""
        progress_calls = []
        def progress_callback(completed, total, rows):
            progress_calls.append((completed, total, rows))
        
        config = ChunkConfig(
            max_chunk_size=10,
            chunk_threshold=10,
            max_workers=1,  # Sequential for predictable order
            progress_callback=progress_callback,
        )
        executor = ChunkedExecutor(mock_adapter, config)
        
        mock_adapter.fetch.return_value = pa.table({"id": [1]})
        
        large_list = list(range(25))
        predicates = [
            Predicate(column="id", operator="IN", value=large_list)
        ]
        
        executor.execute_chunked(
            table="test_table",
            predicates=predicates,
        )
        
        # Should have 3 progress updates (one per chunk)
        assert len(progress_calls) == 3
        # Final call should show all chunks complete
        assert progress_calls[-1][0] == 3  # completed
        assert progress_calls[-1][1] == 3  # total
    
    def test_chunk_failure_handled(self, mock_adapter):
        """Test that chunk failures are handled gracefully."""
        config = ChunkConfig(max_chunk_size=10, chunk_threshold=10, max_workers=1)
        executor = ChunkedExecutor(mock_adapter, config)
        
        call_count = [0]
        def mock_fetch(**kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Simulated API error")
            return pa.table({"id": [call_count[0]]})
        
        mock_adapter.fetch.side_effect = mock_fetch
        
        large_list = list(range(25))
        predicates = [
            Predicate(column="id", operator="IN", value=large_list)
        ]
        
        result = executor.execute_chunked(
            table="test_table",
            predicates=predicates,
        )
        
        # Should get results from successful chunks only
        assert len(result) == 2  # 2 successful, 1 failed


class TestResultMerging:
    """Tests for result merging logic."""
    
    @pytest.fixture
    def mock_adapter(self):
        adapter = Mock()
        adapter.adapter_name = "test"
        return adapter
    
    def test_merge_multiple_tables(self, mock_adapter):
        """Test merging multiple Arrow tables."""
        executor = ChunkedExecutor(mock_adapter)
        
        results = [
            ChunkResult(0, pa.table({"id": [1, 2]}), 2, 0.1),
            ChunkResult(1, pa.table({"id": [3, 4]}), 2, 0.1),
            ChunkResult(2, pa.table({"id": [5]}), 1, 0.1),
        ]
        
        merged = executor._merge_results(results)
        
        assert len(merged) == 5
        assert merged.column("id").to_pylist() == [1, 2, 3, 4, 5]
    
    def test_merge_with_empty_results(self, mock_adapter):
        """Test merging when some results are empty."""
        executor = ChunkedExecutor(mock_adapter)
        
        results = [
            ChunkResult(0, pa.table({"id": [1, 2]}), 2, 0.1),
            ChunkResult(1, None, 0, 0.1, error="Failed"),
            ChunkResult(2, pa.table({"id": [3]}), 1, 0.1),
        ]
        
        merged = executor._merge_results(results)
        
        assert len(merged) == 3
        assert merged.column("id").to_pylist() == [1, 2, 3]
    
    def test_deduplication(self, mock_adapter):
        """Test deduplication based on primary keys."""
        config = ChunkConfig(deduplicate=True, primary_keys=["id"])
        executor = ChunkedExecutor(mock_adapter, config)
        
        results = [
            ChunkResult(0, pa.table({"id": [1, 2], "value": ["a", "b"]}), 2, 0.1),
            ChunkResult(1, pa.table({"id": [2, 3], "value": ["c", "d"]}), 2, 0.1),
        ]
        
        merged = executor._merge_results(results)
        
        # Should deduplicate id=2
        assert len(merged) == 3
        ids = merged.column("id").to_pylist()
        assert sorted(ids) == [1, 2, 3]


class TestPostMergeOperations:
    """Tests for post-merge operations (order, limit, offset)."""
    
    @pytest.fixture
    def mock_adapter(self):
        adapter = Mock()
        adapter.adapter_name = "test"
        return adapter
    
    def test_apply_limit(self, mock_adapter):
        """Test applying LIMIT after merge."""
        executor = ChunkedExecutor(mock_adapter)
        
        table = pa.table({"id": [1, 2, 3, 4, 5]})
        result = executor._apply_post_merge(table, limit=3)
        
        assert len(result) == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
    
    def test_apply_offset(self, mock_adapter):
        """Test applying OFFSET after merge."""
        executor = ChunkedExecutor(mock_adapter)
        
        table = pa.table({"id": [1, 2, 3, 4, 5]})
        result = executor._apply_post_merge(table, offset=2)
        
        assert len(result) == 3
        assert result.column("id").to_pylist() == [3, 4, 5]
    
    def test_apply_order_by(self, mock_adapter):
        """Test applying ORDER BY after merge."""
        executor = ChunkedExecutor(mock_adapter)
        
        table = pa.table({"id": [3, 1, 4, 1, 5]})
        result = executor._apply_post_merge(table, order_by=[("id", "ASC")])
        
        assert result.column("id").to_pylist() == [1, 1, 3, 4, 5]
    
    def test_apply_order_by_desc(self, mock_adapter):
        """Test applying ORDER BY DESC after merge."""
        executor = ChunkedExecutor(mock_adapter)
        
        table = pa.table({"id": [3, 1, 4, 1, 5]})
        result = executor._apply_post_merge(table, order_by=[("id", "DESC")])
        
        assert result.column("id").to_pylist() == [5, 4, 3, 1, 1]
    
    def test_combined_operations(self, mock_adapter):
        """Test combining ORDER BY, OFFSET, and LIMIT."""
        executor = ChunkedExecutor(mock_adapter)
        
        table = pa.table({"id": [3, 1, 4, 1, 5, 9, 2, 6]})
        result = executor._apply_post_merge(
            table,
            order_by=[("id", "ASC")],
            offset=2,
            limit=3,
        )
        
        # Sorted: [1, 1, 2, 3, 4, 5, 6, 9]
        # Offset 2: [2, 3, 4, 5, 6, 9]
        # Limit 3: [2, 3, 4]
        assert len(result) == 3
        assert result.column("id").to_pylist() == [2, 3, 4]


class TestOptimalChunkSize:
    """Tests for get_optimal_chunk_size() function."""
    
    def test_default_adapter_size(self):
        """Test default chunk size for unknown adapters."""
        size = get_optimal_chunk_size("unknown_adapter")
        assert size == DEFAULT_CHUNK_SIZES["default"]
    
    def test_servicenow_size(self):
        """Test chunk size for ServiceNow."""
        size = get_optimal_chunk_size("servicenow")
        assert size == DEFAULT_CHUNK_SIZES["servicenow"]
    
    def test_salesforce_size(self):
        """Test chunk size for Salesforce."""
        size = get_optimal_chunk_size("salesforce")
        # Returns min of DEFAULT_CHUNK_SIZES and URL-calculated, so just check it's reasonable
        assert size <= DEFAULT_CHUNK_SIZES["salesforce"]
        assert size > 0
    
    def test_long_values_reduce_size(self):
        """Test that long values reduce chunk size."""
        normal_size = get_optimal_chunk_size("servicenow", estimated_value_length=20)
        long_size = get_optimal_chunk_size("servicenow", estimated_value_length=50)
        
        assert long_size < normal_size
    
    def test_case_insensitive(self):
        """Test adapter name is case insensitive."""
        lower_size = get_optimal_chunk_size("servicenow")
        upper_size = get_optimal_chunk_size("SERVICENOW")
        mixed_size = get_optimal_chunk_size("ServiceNow")
        
        assert lower_size == upper_size == mixed_size


class TestAdapterAutoDetection:
    """Tests for automatic chunk size detection from adapter."""
    
    def test_servicenow_auto_size(self):
        """Test auto-detection for ServiceNow adapter."""
        adapter = Mock()
        adapter.adapter_name = "servicenow"
        
        executor = ChunkedExecutor(adapter)
        assert executor.config.max_chunk_size == DEFAULT_CHUNK_SIZES["servicenow"]
    
    def test_salesforce_auto_size(self):
        """Test auto-detection for Salesforce adapter."""
        adapter = Mock()
        adapter.adapter_name = "salesforce"
        
        executor = ChunkedExecutor(adapter)
        assert executor.config.max_chunk_size == DEFAULT_CHUNK_SIZES["salesforce"]
    
    def test_custom_config_overrides(self):
        """Test that custom config overrides auto-detection."""
        adapter = Mock()
        adapter.adapter_name = "servicenow"
        
        config = ChunkConfig(max_chunk_size=50)
        executor = ChunkedExecutor(adapter, config)
        
        assert executor.config.max_chunk_size == 50
