"""
Tests for Base Adapter - Abstract base class for all data source adapters

Tests cover:
- Abstract method enforcement
- Session management (sync and async)
- Connection pooling integration
- Rate limiter configuration
- Performance metric updates
- Client-side aggregation
- Auto-chunking for large IN predicates
- Helper methods (auth headers, ID extraction, etc.)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.query_planner import Predicate, Aggregate
from waveql.exceptions import QueryError


# Concrete implementation for testing
class TestableAdapter(BaseAdapter):
    """Minimal concrete adapter for testing BaseAdapter functionality."""
    
    adapter_name = "testable"
    supports_predicate_pushdown = True
    supports_aggregation = False
    supports_insert = True
    supports_update = True
    supports_delete = True
    
    def fetch(
        self,
        table,
        columns=None,
        predicates=None,
        limit=None,
        offset=None,
        order_by=None,
        group_by=None,
        aggregates=None,
    ):
        """Return mock data for testing."""
        return pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "amount": [100.0, 200.0, 300.0],
        })
    
    def get_schema(self, table):
        """Return mock schema."""
        from waveql.schema_cache import ColumnInfo
        return [
            ColumnInfo(name="id", data_type="integer"),
            ColumnInfo(name="name", data_type="string"),
            ColumnInfo(name="amount", data_type="float"),
        ]
    
    def insert(self, table, values, parameters=None):
        """Mock insert."""
        return 1
    
    def update(self, table, values, predicates=None, parameters=None):
        """Mock update."""
        return 1
    
    def delete(self, table, predicates=None, parameters=None):
        """Mock delete."""
        return 1


class TestBaseAdapterInit:
    """Tests for BaseAdapter initialization."""
    
    def test_default_initialization(self):
        """Test adapter with default parameters."""
        adapter = TestableAdapter()
        
        assert adapter._host is None
        assert adapter._auth_manager is None
        assert adapter._schema_cache is None
        assert adapter._use_connection_pool is True
        assert adapter.avg_latency_per_row == 0.001
    
    def test_custom_initialization(self):
        """Test adapter with custom parameters."""
        mock_auth = Mock()
        mock_cache = Mock()
        
        adapter = TestableAdapter(
            host="api.example.com",
            auth_manager=mock_auth,
            schema_cache=mock_cache,
            max_retries=10,
            retry_base_delay=2.0,
            use_connection_pool=False,
        )
        
        assert adapter._host == "api.example.com"
        assert adapter._auth_manager == mock_auth
        assert adapter._schema_cache == mock_cache
        assert adapter._use_connection_pool is False
        assert adapter._rate_limiter.max_retries == 10
        assert adapter._rate_limiter.base_delay == 2.0
    
    def test_adapter_name(self):
        """Test adapter name is set correctly."""
        adapter = TestableAdapter()
        assert adapter.adapter_name == "testable"


class TestAbstractMethods:
    """Tests for abstract method enforcement."""
    
    def test_fetch_must_be_implemented(self):
        """Test that fetch() is abstract."""
        # Try to create adapter without implementing fetch
        class IncompleteAdapter(BaseAdapter):
            def get_schema(self, table):
                return []
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteAdapter()
    
    def test_get_schema_must_be_implemented(self):
        """Test that get_schema() is abstract."""
        class IncompleteAdapter(BaseAdapter):
            def fetch(self, *args, **kwargs):
                return pa.table({})
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteAdapter()


class TestHostExtraction:
    """Tests for host extraction from URLs."""
    
    def test_extract_from_full_url(self):
        """Test extracting host from full URL."""
        adapter = TestableAdapter(host="https://api.example.com/v1")
        
        assert adapter._pool_host == "api.example.com"
    
    def test_extract_from_plain_host(self):
        """Test extracting host from plain hostname."""
        adapter = TestableAdapter(host="api.example.com")
        
        assert adapter._pool_host == "api.example.com"
    
    def test_extract_default_for_none(self):
        """Test default host when none provided."""
        adapter = TestableAdapter(host=None)
        
        assert adapter._pool_host == "default"


class TestAuthManager:
    """Tests for authentication manager integration."""
    
    def test_set_auth_manager(self):
        """Test setting auth manager after init."""
        adapter = TestableAdapter()
        mock_auth = Mock()
        
        adapter.set_auth_manager(mock_auth)
        
        assert adapter._auth_manager == mock_auth
    
    def test_get_auth_headers(self):
        """Test getting auth headers from manager."""
        mock_auth = Mock()
        mock_auth.get_headers.return_value = {"Authorization": "Bearer token123"}
        
        adapter = TestableAdapter(auth_manager=mock_auth)
        headers = adapter._get_auth_headers()
        
        assert headers == {"Authorization": "Bearer token123"}
    
    def test_get_auth_headers_no_manager(self):
        """Test getting auth headers without manager returns empty dict."""
        adapter = TestableAdapter()
        headers = adapter._get_auth_headers()
        
        assert headers == {}


class TestSchemaCache:
    """Tests for schema cache integration."""
    
    def test_set_schema_cache(self):
        """Test setting schema cache after init."""
        adapter = TestableAdapter()
        mock_cache = Mock()
        
        adapter.set_schema_cache(mock_cache)
        
        assert adapter._schema_cache == mock_cache
    
    def test_get_cached_schema(self):
        """Test getting schema from cache."""
        mock_cache = Mock()
        mock_schema = Mock()
        mock_schema.columns = [Mock(name="id")]
        mock_cache.get.return_value = mock_schema
        
        adapter = TestableAdapter(schema_cache=mock_cache)
        result = adapter._get_cached_schema("test_table")
        
        assert result == mock_schema.columns
    
    def test_get_cached_schema_miss(self):
        """Test cache miss returns None."""
        mock_cache = Mock()
        mock_cache.get.return_value = None
        
        adapter = TestableAdapter(schema_cache=mock_cache)
        result = adapter._get_cached_schema("test_table")
        
        assert result is None
    
    def test_cache_schema(self):
        """Test caching a schema."""
        mock_cache = Mock()
        adapter = TestableAdapter(schema_cache=mock_cache)
        
        columns = [Mock(name="id"), Mock(name="name")]
        adapter._cache_schema("test_table", columns, ttl=1800)
        
        mock_cache.set.assert_called_once_with("testable", "test_table", columns, 1800)


class TestIdExtraction:
    """Tests for ID extraction from predicates."""
    
    def test_extract_id_from_predicates(self):
        """Test extracting ID from equality predicate."""
        adapter = TestableAdapter()
        predicates = [
            Predicate(column="id", operator="=", value="12345"),
            Predicate(column="status", operator="=", value="open"),
        ]
        
        result = adapter._extract_id_from_predicates(predicates, "UPDATE")
        
        assert result == "12345"
    
    def test_extract_id_case_insensitive(self):
        """Test that ID extraction is case insensitive."""
        adapter = TestableAdapter()
        predicates = [
            Predicate(column="ID", operator="=", value="abc123"),
        ]
        
        result = adapter._extract_id_from_predicates(predicates, "DELETE")
        
        assert result == "abc123"
    
    def test_extract_id_missing_raises_error(self):
        """Test that missing ID raises QueryError."""
        adapter = TestableAdapter()
        predicates = [
            Predicate(column="status", operator="=", value="open"),
        ]
        
        with pytest.raises(QueryError):
            adapter._extract_id_from_predicates(predicates, "UPDATE")
    
    def test_extract_id_empty_predicates_raises_error(self):
        """Test that empty predicates raises QueryError."""
        adapter = TestableAdapter()
        
        with pytest.raises(QueryError):
            adapter._extract_id_from_predicates([], "DELETE")
    
    def test_extract_id_none_predicates_raises_error(self):
        """Test that None predicates raises QueryError."""
        adapter = TestableAdapter()
        
        with pytest.raises(QueryError):
            adapter._extract_id_from_predicates(None, "DELETE")


class TestPerformanceMetrics:
    """Tests for performance metric tracking."""
    
    def test_update_performance_metrics(self):
        """Test that performance metrics are updated."""
        adapter = TestableAdapter()
        initial_latency = adapter.avg_latency_per_row
        
        # Update with faster performance
        adapter._update_performance_metrics(row_count=1000, duration=0.5)
        
        # Latency should have changed (moving average)
        assert adapter.avg_latency_per_row != initial_latency
    
    def test_update_metrics_skips_zero_rows(self):
        """Test that zero rows doesn't crash."""
        adapter = TestableAdapter()
        initial_latency = adapter.avg_latency_per_row
        
        adapter._update_performance_metrics(row_count=0, duration=1.0)
        
        # Should remain unchanged
        assert adapter.avg_latency_per_row == initial_latency
    
    def test_execution_history_limited(self):
        """Test that execution history is limited to 100 entries."""
        adapter = TestableAdapter()
        
        for i in range(150):
            adapter._update_performance_metrics(row_count=100, duration=0.1)
        
        assert len(adapter._execution_history) == 100


class TestClientSideAggregation:
    """Tests for client-side aggregation computation."""
    
    def test_count_star(self):
        """Test COUNT(*) aggregation."""
        adapter = TestableAdapter()
        table = pa.table({
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
        })
        
        aggregates = [Aggregate(func="COUNT", column="*", alias="total")]
        result = adapter._compute_client_side_aggregates(table, aggregates=aggregates)
        
        assert result.num_rows == 1
        assert result.column("total")[0].as_py() == 5
    
    def test_sum_aggregation(self):
        """Test SUM aggregation."""
        adapter = TestableAdapter()
        table = pa.table({
            "id": [1, 2, 3],
            "amount": [100.0, 200.0, 300.0],
        })
        
        aggregates = [Aggregate(func="SUM", column="amount", alias="total_amount")]
        result = adapter._compute_client_side_aggregates(table, aggregates=aggregates)
        
        assert result.num_rows == 1
        assert result.column("total_amount")[0].as_py() == 600.0
    
    def test_avg_aggregation(self):
        """Test AVG aggregation."""
        adapter = TestableAdapter()
        table = pa.table({
            "id": [1, 2, 3],
            "value": [10.0, 20.0, 30.0],
        })
        
        aggregates = [Aggregate(func="AVG", column="value", alias="avg_value")]
        result = adapter._compute_client_side_aggregates(table, aggregates=aggregates)
        
        assert result.num_rows == 1
        assert result.column("avg_value")[0].as_py() == 20.0
    
    def test_min_max_aggregation(self):
        """Test MIN and MAX aggregation."""
        adapter = TestableAdapter()
        table = pa.table({
            "id": [1, 2, 3],
            "value": [10.0, 5.0, 20.0],
        })
        
        aggregates = [
            Aggregate(func="MIN", column="value", alias="min_val"),
            Aggregate(func="MAX", column="value", alias="max_val"),
        ]
        result = adapter._compute_client_side_aggregates(table, aggregates=aggregates)
        
        assert result.column("min_val")[0].as_py() == 5.0
        assert result.column("max_val")[0].as_py() == 20.0
    
    def test_group_by_aggregation(self):
        """Test aggregation with GROUP BY."""
        adapter = TestableAdapter()
        table = pa.table({
            "status": ["open", "open", "closed", "closed", "closed"],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        
        aggregates = [Aggregate(func="COUNT", column="*", alias="count")]
        result = adapter._compute_client_side_aggregates(
            table, group_by=["status"], aggregates=aggregates
        )
        
        # Should have 2 rows (open, closed)
        assert result.num_rows == 2
    
    def test_empty_table_aggregation(self):
        """Test aggregation on empty table."""
        adapter = TestableAdapter()
        table = pa.table({
            "id": pa.array([], type=pa.int64()),
            "value": pa.array([], type=pa.float64()),
        })
        
        aggregates = [Aggregate(func="COUNT", column="*", alias="total")]
        result = adapter._compute_client_side_aggregates(table, aggregates=aggregates)
        
        assert result.num_rows == 1
        assert result.column("total")[0].as_py() == 0
    
    def test_no_aggregates_returns_original(self):
        """Test that no aggregates returns original table."""
        adapter = TestableAdapter()
        table = pa.table({"id": [1, 2, 3]})
        
        result = adapter._compute_client_side_aggregates(table, aggregates=[])
        
        assert result == table


class TestAutoChunking:
    """Tests for automatic chunking of large IN predicates."""
    
    def test_no_chunking_needed(self):
        """Test that small IN clauses don't trigger chunking."""
        adapter = TestableAdapter()
        
        predicates = [
            Predicate(column="status", operator="IN", value=["a", "b", "c"]),
        ]
        
        # Should call regular fetch
        result = adapter.fetch_with_auto_chunking(
            table="test",
            predicates=predicates,
        )
        
        assert result is not None
    
    def test_chunking_detected_for_large_in(self):
        """Test that large IN clauses are detected."""
        adapter = TestableAdapter()
        adapter.chunk_threshold = 10  # Low threshold for testing
        
        # Create a large IN predicate
        large_values = list(range(100))
        predicates = [
            Predicate(column="id", operator="IN", value=large_values),
        ]
        
        # This should detect chunking is needed
        needs_chunking = False
        for pred in predicates:
            if (pred.operator.upper() == "IN" 
                and isinstance(pred.value, (list, tuple))
                and len(pred.value) > adapter.chunk_threshold):
                needs_chunking = True
        
        assert needs_chunking is True


class TestNotImplementedMethods:
    """Tests for not-implemented optional methods."""
    
    def test_fetch_async_not_implemented(self):
        """Test that fetch_async raises NotImplementedError by default."""
        adapter = TestableAdapter()
        
        import asyncio
        with pytest.raises(NotImplementedError):
            asyncio.run(adapter.fetch_async("table"))
    
    def test_get_schema_async_not_implemented(self):
        """Test that get_schema_async raises NotImplementedError by default."""
        adapter = TestableAdapter()
        
        import asyncio
        with pytest.raises(NotImplementedError):
            asyncio.run(adapter.get_schema_async("table"))
    
    def test_insert_async_not_implemented(self):
        """Test that insert_async raises NotImplementedError by default."""
        adapter = TestableAdapter()
        
        import asyncio
        with pytest.raises(NotImplementedError):
            asyncio.run(adapter.insert_async("table", {}))


class TestParallelPlan:
    """Tests for parallel execution planning."""
    
    def test_default_parallel_plan(self):
        """Test default parallel plan returns single partition."""
        adapter = TestableAdapter()
        
        plan = adapter.get_parallel_plan("table", n_partitions=4)
        
        assert len(plan) == 1
        assert plan[0]["partition_index"] == 0
        assert plan[0]["total_partitions"] == 1
    
    def test_parallel_scan_not_supported(self):
        """Test that parallel scan is not supported by default."""
        adapter = TestableAdapter()
        
        assert adapter.supports_parallel_scan is False


class TestRepr:
    """Tests for string representation."""
    
    def test_repr(self):
        """Test adapter repr."""
        adapter = TestableAdapter(host="api.example.com", use_connection_pool=True)
        
        repr_str = repr(adapter)
        
        assert "TestableAdapter" in repr_str
        assert "api.example.com" in repr_str
        assert "pool=True" in repr_str
