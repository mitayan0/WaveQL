"""
Tests for JoinOptimizer - Cost-Based Join Re-ordering with Real-Time Latency Stats

These tests verify:
1. Table statistics tracking and updates
2. Selectivity estimation
3. Join reordering based on cost model
4. Rate limit awareness
5. Integration with the optimizer and cursor
"""

import pytest
import time
from unittest.mock import MagicMock, patch
import pyarrow as pa

from waveql.join_optimizer import (
    JoinOptimizer, 
    TableStats, 
    JoinEdge, 
    JoinPlan,
    get_join_optimizer
)
from waveql.query_planner import Predicate
from waveql.adapters.base import BaseAdapter


class MockAdapter(BaseAdapter):
    """Mock adapter for testing."""
    
    def __init__(self, name, latency=0.001, avg_rows=100):
        super().__init__(host="mock")
        self.adapter_name = name
        self.avg_latency_per_row = latency
        self._execution_history = []
        self._avg_rows = avg_rows
    
    def fetch(self, table, columns=None, predicates=None, **kwargs):
        return pa.Table.from_pylist([{"id": i} for i in range(self._avg_rows)])
    
    def get_schema(self, table):
        return []


@pytest.fixture(autouse=True)
def clear_optimizer_stats():
    """Clear optimizer stats before each test."""
    optimizer = get_join_optimizer()
    optimizer.clear_stats()
    yield
    optimizer.clear_stats()


class TestTableStats:
    """Tests for TableStats tracking."""
    
    def test_initial_values(self):
        """Test default values."""
        stats = TableStats()
        assert stats.avg_latency_per_row == 0.001
        assert stats.avg_row_count == 1000.0
        assert stats.execution_count == 0
    
    def test_update_stats(self):
        """Test updating stats with execution data."""
        stats = TableStats()
        
        # First execution: 100 rows in 0.1 seconds
        stats.update(row_count=100, duration=0.1)
        
        assert stats.execution_count == 1
        assert stats.total_rows_fetched == 100
        assert stats.total_duration == 0.1
        assert stats.avg_row_count == 100.0
        
        # Check latency update (EMA with alpha=0.3)
        # new_latency = 0.1/100 = 0.001
        # avg = 0.7 * 0.001 + 0.3 * 0.001 = 0.001
        assert abs(stats.avg_latency_per_row - 0.001) < 0.0001
    
    def test_latency_ema_adaptation(self):
        """Test that latency adapts to changing conditions."""
        stats = TableStats()
        
        # Simulate fast responses
        for _ in range(5):
            stats.update(row_count=100, duration=0.01)  # 0.0001 per row
        
        fast_latency = stats.avg_latency_per_row
        
        # Now simulate slow responses
        for _ in range(10):
            stats.update(row_count=100, duration=1.0)  # 0.01 per row
        
        slow_latency = stats.avg_latency_per_row
        
        # Latency should have increased significantly
        assert slow_latency > fast_latency * 2
    
    def test_rate_limit_penalty(self):
        """Test that rate limits increase effective latency."""
        stats = TableStats()
        stats.update(row_count=100, duration=0.1)
        
        base_latency = stats.get_effective_latency()
        
        # Simulate rate limit
        stats.update(row_count=100, duration=0.1, rate_limited=True)
        
        penalized_latency = stats.get_effective_latency()
        
        # Effective latency should be higher due to penalty
        assert penalized_latency > base_latency
    
    def test_stale_detection(self):
        """Test that stale stats are detected."""
        stats = TableStats()
        assert stats.is_stale()  # No updates yet
        
        stats.update(row_count=100, duration=0.1)
        assert not stats.is_stale()  # Just updated
        
        # Simulate old update
        stats.last_update = time.time() - 600  # 10 minutes ago
        assert stats.is_stale(max_age_seconds=300)


class TestSelectivityEstimation:
    """Tests for predicate selectivity estimation."""
    
    def test_no_predicates(self):
        """Test that no predicates = full selectivity."""
        optimizer = get_join_optimizer()
        selectivity = optimizer.estimate_selectivity([])
        assert selectivity == 1.0
    
    def test_equality_predicate(self):
        """Test that equality is highly selective."""
        optimizer = get_join_optimizer()
        preds = [Predicate(column="status", operator="=", value="active")]
        selectivity = optimizer.estimate_selectivity(preds)
        assert selectivity == 0.1  # 10% remain
    
    def test_multiple_predicates(self):
        """Test that multiple predicates multiply selectivity."""
        optimizer = get_join_optimizer()
        preds = [
            Predicate(column="status", operator="=", value="active"),
            Predicate(column="priority", operator="=", value="high"),
        ]
        selectivity = optimizer.estimate_selectivity(preds)
        assert selectivity == pytest.approx(0.01)  # 0.1 * 0.1
    
    def test_in_predicate(self):
        """Test IN predicate selectivity based on value count."""
        optimizer = get_join_optimizer()
        
        # Few values = more selective
        preds_few = [Predicate(column="status", operator="IN", value=["a", "b"])]
        selectivity_few = optimizer.estimate_selectivity(preds_few)
        
        # Many values = less selective
        preds_many = [Predicate(column="status", operator="IN", value=list(range(20)))]
        selectivity_many = optimizer.estimate_selectivity(preds_many)
        
        assert selectivity_few < selectivity_many
    
    def test_range_predicates(self):
        """Test range predicate selectivity."""
        optimizer = get_join_optimizer()
        
        preds = [Predicate(column="created_at", operator=">", value="2024-01-01")]
        selectivity = optimizer.estimate_selectivity(preds)
        
        assert selectivity == 0.3  # 30% remain for range


class TestJoinReordering:
    """Tests for join reordering logic."""
    
    def test_single_table(self):
        """Test that single table returns immediately."""
        optimizer = get_join_optimizer()
        
        conn = MagicMock()
        conn.get_adapter.return_value = None
        
        plan = optimizer.reorder_joins(
            tables=["servicenow.incident"],
            table_predicates={},
            join_edges=[],
            connection=conn
        )
        
        assert plan.table_order == ["servicenow.incident"]
        assert plan.join_strategy == "single_table"
    
    def test_empty_tables(self):
        """Test empty table list."""
        optimizer = get_join_optimizer()
        
        plan = optimizer.reorder_joins(
            tables=[],
            table_predicates={},
            join_edges=[],
            connection=MagicMock()
        )
        
        assert plan.table_order == []
        assert plan.join_strategy == "none"
    
    def test_prefer_filtered_table_first(self):
        """Test that table with more selective predicates comes first."""
        optimizer = get_join_optimizer()
        
        # Set up mock connection with adapters
        conn = MagicMock()
        
        slow_adapter = MockAdapter("db1", latency=0.001, avg_rows=1000)
        fast_adapter = MockAdapter("db2", latency=0.001, avg_rows=1000)
        
        def get_adapter(name):
            if name == "db1":
                return slow_adapter
            elif name == "db2":
                return fast_adapter
            return None
        
        conn.get_adapter.side_effect = get_adapter
        
        # Table with highly selective predicate
        preds = {
            "db1.big_table": [],  # No predicates
            "db2.small_table": [
                Predicate(column="status", operator="=", value="active"),
                Predicate(column="type", operator="=", value="incident"),
            ],
        }
        
        plan = optimizer.reorder_joins(
            tables=["db1.big_table", "db2.small_table"],
            table_predicates=preds,
            join_edges=[],
            connection=conn
        )
        
        # Table with predicates should come first (lower cost)
        assert plan.table_order[0] == "db2.small_table"
    
    def test_prefer_faster_adapter(self):
        """Test that table from faster adapter comes first."""
        optimizer = get_join_optimizer()
        
        conn = MagicMock()
        
        slow_adapter = MockAdapter("slow_db", latency=0.01, avg_rows=100)
        fast_adapter = MockAdapter("fast_db", latency=0.0001, avg_rows=100)
        
        def get_adapter(name):
            if name == "slow_db":
                return slow_adapter
            elif name == "fast_db":
                return fast_adapter
            return None
        
        conn.get_adapter.side_effect = get_adapter
        
        plan = optimizer.reorder_joins(
            tables=["slow_db.table1", "fast_db.table2"],
            table_predicates={},
            join_edges=[],
            connection=conn
        )
        
        # Faster adapter should come first
        assert plan.table_order[0] == "fast_db.table2"
    
    def test_uses_realtime_stats(self):
        """Test that real-time stats are used when available."""
        optimizer = get_join_optimizer()
        
        # Pre-populate stats for one table
        optimizer.update_table_stats(
            adapter_name="db1",
            table_name="fast_table",
            row_count=10,
            duration=0.001  # Very fast
        )
        
        optimizer.update_table_stats(
            adapter_name="db2",
            table_name="slow_table",
            row_count=1000,
            duration=10.0  # Very slow
        )
        
        conn = MagicMock()
        conn.get_adapter.return_value = MockAdapter("mock", latency=0.001)
        
        plan = optimizer.reorder_joins(
            tables=["db1.fast_table", "db2.slow_table"],
            table_predicates={},
            join_edges=[],
            connection=conn
        )
        
        # Table with better real-time stats should come first
        assert plan.table_order[0] == "db1.fast_table"
    
    def test_semi_join_strategy_detection(self):
        """Test that semi-join pushdown strategy is detected."""
        optimizer = get_join_optimizer()
        
        # Pre-populate stats: first table much smaller
        optimizer.update_table_stats(
            adapter_name="db1",
            table_name="small_table",
            row_count=50,
            duration=0.05
        )
        
        optimizer.update_table_stats(
            adapter_name="db2",
            table_name="large_table",
            row_count=10000,
            duration=10.0
        )
        
        conn = MagicMock()
        adapter = MockAdapter("mock", latency=0.001)
        conn.get_adapter.return_value = adapter
        
        plan = optimizer.reorder_joins(
            tables=["db1.small_table", "db2.large_table"],
            table_predicates={},
            join_edges=[],
            connection=conn
        )
        
        # Semi-join should be detected when first table is much smaller
        assert plan.join_strategy == "semi_join_pushdown"


class TestSimpleInterface:
    """Test the simplified interface for backward compatibility."""
    
    def test_reorder_joins_simple(self):
        """Test the simple reorder_joins interface."""
        optimizer = get_join_optimizer()
        
        conn = MagicMock()
        conn.get_adapter.return_value = MockAdapter("mock")
        
        result = optimizer.reorder_joins_simple(
            tables=["db1.table1", "db2.table2"],
            predicates={"db1.table1": [Predicate("status", "=", "active")]},
            connection=conn
        )
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert "db1.table1" in result
        assert "db2.table2" in result


class TestGetAllStats:
    """Test statistics retrieval."""
    
    def test_get_all_stats(self):
        """Test retrieving all statistics."""
        optimizer = get_join_optimizer()
        
        optimizer.update_table_stats("db1", "table1", 100, 0.1)
        optimizer.update_table_stats("db2", "table2", 200, 0.2)
        
        stats = optimizer.get_all_stats()
        
        assert "db1.table1" in stats
        assert "db2.table2" in stats
        assert stats["db1.table1"]["execution_count"] == 1
        assert stats["db2.table2"]["execution_count"] == 1


class TestIntegrationWithOptimizer:
    """Test integration with the QueryOptimizer."""
    
    def test_optimizer_uses_join_optimizer(self):
        """Test that QueryOptimizer delegates to JoinOptimizer."""
        from waveql.optimizer import QueryOptimizer
        
        optimizer = QueryOptimizer()
        join_optimizer = get_join_optimizer()
        
        # Pre-populate stats
        join_optimizer.update_table_stats("db1", "table1", 50, 0.05)
        
        conn = MagicMock()
        conn.get_adapter.return_value = MockAdapter("mock")
        
        result = optimizer.reorder_joins(
            tables=["db1.table1", "db2.table2"],
            predicates={"db1.table1": [Predicate("status", "=", "active")]},
            connection=conn
        )
        
        assert isinstance(result, list)
        assert len(result) == 2
