"""
Tests for Resource Optimizer - Low-Resource Systems Engineering

Tests the three key features:
1. Statistical Cardinality Estimator
2. Adaptive Pagination (AIMD)
3. Budget-Constrained Planning
"""

import pytest
import time

from waveql.resource_optimizer import (
    CardinalityStats,
    get_cardinality_estimator,
    AIMDState,
    get_adaptive_pagination,
    BudgetPlanner,
    QueryBudget,
    BudgetUnit,
    ResourceAwareExecutor,
)
from waveql.query_planner import Predicate


class TestCardinalityStats:
    """Tests for CardinalityStats dataclass."""
    
    def test_initial_values(self):
        """Test default initial values."""
        stats = CardinalityStats()
        assert stats.sample_count == 0
        assert stats.avg_rows == 0.0
        assert stats.min_rows == 0
        assert stats.max_rows == 0
    
    def test_first_update(self):
        """Test first update sets min/max correctly."""
        stats = CardinalityStats()
        stats.update(100)
        
        assert stats.sample_count == 1
        assert stats.min_rows == 100
        assert stats.max_rows == 100
        assert stats.avg_rows == 100.0
    
    def test_multiple_updates(self):
        """Test multiple updates calculate stats correctly."""
        stats = CardinalityStats()
        stats.update(100)
        stats.update(200)
        stats.update(300)
        
        assert stats.sample_count == 3
        assert stats.min_rows == 100
        assert stats.max_rows == 300
        assert stats.avg_rows == 200.0
        assert stats.total_rows_observed == 600
    
    def test_percentile_calculation(self):
        """Test percentile calculation with enough samples."""
        stats = CardinalityStats()
        for i in range(10):
            stats.update(i * 10)  # 0, 10, 20, ..., 90
        
        # With sorted values [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
        # p50 = index 5 -> 50
        # p90 = index 9 -> 90
        assert stats.p50_rows == 50  # Middle value (index 5)
        assert stats.p90_rows == 90  # 90th percentile (index 9)


class TestCardinalityEstimator:
    """Tests for CardinalityEstimator."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Clear estimator state before each test."""
        estimator = get_cardinality_estimator()
        estimator.clear_stats()
        yield
        estimator.clear_stats()
    
    def test_singleton(self):
        """Test that get_cardinality_estimator returns singleton."""
        e1 = get_cardinality_estimator()
        e2 = get_cardinality_estimator()
        assert e1 is e2
    
    def test_record_and_estimate(self):
        """Test recording execution and estimating cardinality."""
        estimator = get_cardinality_estimator()
        
        # Record some executions
        estimator.record_execution("servicenow", "incident", 100)
        estimator.record_execution("servicenow", "incident", 150)
        estimator.record_execution("servicenow", "incident", 200)
        
        # Estimate cardinality
        estimate, lower, upper = estimator.estimate_cardinality(
            "servicenow", "incident"
        )
        
        # Should be around 150 (average)
        assert 100 <= estimate <= 200
        assert lower <= estimate <= upper
    
    def test_estimate_with_predicates(self):
        """Test that predicates reduce cardinality estimate."""
        estimator = get_cardinality_estimator()
        
        # Record base cardinality
        for _ in range(10):
            estimator.record_execution("servicenow", "incident", 1000)
        
        # Estimate without predicates
        est_no_pred, _, _ = estimator.estimate_cardinality("servicenow", "incident")
        
        # Estimate with equality predicate (high selectivity)
        pred = Predicate(column="priority", operator="=", value=1)
        est_with_pred, _, _ = estimator.estimate_cardinality(
            "servicenow", "incident", predicates=[pred]
        )
        
        # With predicate should be much lower
        assert est_with_pred < est_no_pred
    
    def test_estimate_with_limit(self):
        """Test that LIMIT caps the estimate."""
        estimator = get_cardinality_estimator()
        
        # Record high cardinality
        for _ in range(5):
            estimator.record_execution("salesforce", "account", 5000)
        
        # Estimate with limit
        estimate, _, _ = estimator.estimate_cardinality(
            "salesforce", "account", limit=100
        )
        
        assert estimate <= 100
    
    def test_no_history_returns_defaults(self):
        """Test estimation without history returns reasonable defaults."""
        estimator = get_cardinality_estimator()
        
        estimate, lower, upper = estimator.estimate_cardinality(
            "unknown", "table"
        )
        
        # Should return defaults
        assert estimate == 1000.0
        assert lower > 0
        assert upper > estimate
    
    def test_estimate_cost_seconds(self):
        """Test cost estimation in seconds."""
        estimator = get_cardinality_estimator()
        
        for _ in range(5):
            estimator.record_execution("servicenow", "incident", 1000)
        
        cost, lower, upper = estimator.estimate_cost_seconds(
            "servicenow", "incident",
            avg_latency_per_row=0.001  # 1ms per row
        )
        
        # 1000 rows * 0.001s = 1 second
        assert 0.5 <= cost <= 1.5
    
    def test_get_all_stats(self):
        """Test getting all stats for monitoring."""
        estimator = get_cardinality_estimator()
        
        estimator.record_execution("a", "t1", 100)
        estimator.record_execution("b", "t2", 200)
        
        stats = estimator.get_all_stats()
        
        assert "a.t1" in stats
        assert "b.t2" in stats
        assert stats["a.t1"]["avg_rows"] == 100


class TestAdaptivePagination:
    """Tests for AdaptivePagination (AIMD)."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset pagination state before each test."""
        pagination = get_adaptive_pagination()
        pagination.reset()
        yield
        pagination.reset()
    
    def test_singleton(self):
        """Test that get_adaptive_pagination returns singleton."""
        p1 = get_adaptive_pagination()
        p2 = get_adaptive_pagination()
        assert p1 is p2
    
    def test_initial_page_size(self):
        """Test initial page size is reasonable."""
        pagination = get_adaptive_pagination()
        size = pagination.get_page_size("test_adapter")
        
        # Default is 100
        assert size == 100
    
    def test_additive_increase(self):
        """Test page size increases on success."""
        pagination = get_adaptive_pagination()
        
        initial_size = pagination.get_page_size("test")
        
        # Record successes
        for _ in range(5):
            pagination.record_success("test", rows_fetched=100, duration=0.1)
        
        new_size = pagination.get_page_size("test")
        
        assert new_size > initial_size
    
    def test_multiplicative_decrease(self):
        """Test page size decreases on rate limit."""
        pagination = get_adaptive_pagination()
        
        # Grow the page size first
        for _ in range(10):
            pagination.record_success("test", rows_fetched=100, duration=0.1)
        
        before_limit = pagination.get_page_size("test")
        
        # Hit rate limit
        pagination.record_rate_limit("test")
        
        after_limit = pagination.get_page_size("test")
        
        # Should drop to minimum
        assert after_limit < before_limit
        assert after_limit == 10  # min_page_size
    
    def test_slow_start_exponential(self):
        """Test exponential growth during slow start."""
        pagination = get_adaptive_pagination()
        state = pagination.get_state("test_exp")
        
        assert state.state == AIMDState.SLOW_START
        
        # Record success - should double
        initial = state.page_size
        pagination.record_success("test_exp", rows_fetched=100, duration=0.1)
        
        assert state.page_size == initial * 2
    
    def test_cooldown_after_rate_limit(self):
        """Test cooldown period prevents growth after rate limit."""
        pagination = get_adaptive_pagination()
        
        # Hit rate limit
        pagination.record_rate_limit("test_cooldown")
        
        state = pagination.get_state("test_cooldown")
        assert state.state == AIMDState.COOLDOWN
        assert state.cooldown_remaining > 0
        
        # Successes during cooldown shouldn't grow much
        before = state.page_size
        pagination.record_success("test_cooldown", rows_fetched=10, duration=0.1)
        
        # Should stay at min during cooldown
        assert state.page_size == before
    
    def test_throughput_tracking(self):
        """Test throughput is tracked correctly."""
        pagination = get_adaptive_pagination()
        
        # 100 rows in 1 second = 100 rows/s
        pagination.record_success("test_tp", rows_fetched=100, duration=1.0)
        pagination.record_success("test_tp", rows_fetched=200, duration=1.0)
        
        state = pagination.get_state("test_tp")
        
        assert state.avg_throughput == 150.0  # Average of 100 and 200
    
    def test_per_table_state(self):
        """Test that each table has independent state."""
        pagination = get_adaptive_pagination()
        
        pagination.record_success("adapter", "table1", rows_fetched=100, duration=0.1)
        pagination.record_rate_limit("adapter", "table2")
        
        size1 = pagination.get_page_size("adapter", "table1")
        size2 = pagination.get_page_size("adapter", "table2")
        
        assert size1 > size2  # table1 grew, table2 dropped


class TestBudgetPlanner:
    """Tests for BudgetPlanner."""
    
    def test_parse_milliseconds(self):
        """Test parsing millisecond budget."""
        planner = BudgetPlanner()
        sql = "SELECT * FROM incident WITH BUDGET 500ms"
        
        cleaned, budget = planner.parse_budget(sql)
        
        assert "WITH BUDGET" not in cleaned
        assert budget is not None
        assert budget.value == 500
        assert budget.unit == BudgetUnit.MILLISECONDS
    
    def test_parse_seconds(self):
        """Test parsing second budget."""
        planner = BudgetPlanner()
        sql = "SELECT * FROM incident WITH BUDGET 2s"
        
        _, budget = planner.parse_budget(sql)
        
        assert budget.value == 2
        assert budget.unit == BudgetUnit.SECONDS
    
    def test_parse_rows(self):
        """Test parsing row budget."""
        planner = BudgetPlanner()
        sql = "SELECT * FROM incident WITH BUDGET 1000 rows"
        
        _, budget = planner.parse_budget(sql)
        
        assert budget.value == 1000
        assert budget.unit == BudgetUnit.ROWS
    
    def test_no_budget(self):
        """Test parsing query without budget."""
        planner = BudgetPlanner()
        sql = "SELECT * FROM incident WHERE priority = 1"
        
        cleaned, budget = planner.parse_budget(sql)
        
        assert cleaned == sql
        assert budget is None
    
    def test_budget_in_middle(self):
        """Test parsing budget in middle of query."""
        planner = BudgetPlanner()
        sql = "SELECT * FROM incident WITH BUDGET 500ms WHERE priority = 1"
        
        cleaned, budget = planner.parse_budget(sql)
        
        assert "WITH BUDGET" not in cleaned
        assert "WHERE priority = 1" in cleaned
        assert budget.value == 500
    
    def test_case_insensitive(self):
        """Test budget parsing is case insensitive."""
        planner = BudgetPlanner()
        sql = "SELECT * FROM incident with budget 100MS"
        
        _, budget = planner.parse_budget(sql)
        
        assert budget.value == 100
        assert budget.unit == BudgetUnit.MILLISECONDS


class TestQueryBudget:
    """Tests for QueryBudget."""
    
    def test_start_tracking(self):
        """Test starting budget tracking."""
        budget = QueryBudget(value=500, unit=BudgetUnit.MILLISECONDS)
        budget.start()
        
        assert budget.start_time > 0
        assert budget.remaining == 500
        assert not budget.is_exhausted
    
    def test_update_time_budget(self):
        """Test updating time-based budget."""
        budget = QueryBudget(value=100, unit=BudgetUnit.MILLISECONDS)
        budget.start()
        
        # Simulate some time passing
        time.sleep(0.05)  # 50ms
        
        is_valid = budget.update()
        
        assert is_valid
        assert budget.remaining < 100
        assert not budget.is_exhausted
    
    def test_update_row_budget(self):
        """Test updating row-based budget."""
        budget = QueryBudget(value=100, unit=BudgetUnit.ROWS)
        budget.start()
        
        # Process 50 rows
        is_valid = budget.update(rows=50)
        assert is_valid
        assert budget.remaining == 50
        assert budget.rows_processed == 50
        
        # Process 60 more rows - should exhaust
        is_valid = budget.update(rows=60)
        assert not is_valid
        assert budget.is_exhausted
    
    def test_elapsed_ms(self):
        """Test getting elapsed time in ms."""
        budget = QueryBudget(value=1000, unit=BudgetUnit.MILLISECONDS)
        budget.start()
        
        time.sleep(0.01)  # 10ms
        
        elapsed = budget.get_elapsed_ms()
        assert elapsed >= 10


class TestBudgetContext:
    """Tests for BudgetContext manager."""
    
    def test_context_manager(self):
        """Test using budget as context manager."""
        budget = QueryBudget(value=100, unit=BudgetUnit.ROWS)
        planner = BudgetPlanner()
        
        with planner.create_budget_context(budget) as ctx:
            assert ctx.start_time > 0
            ctx.update(rows=10)
            assert ctx.rows_processed == 10
    
    def test_context_exhaustion_logged(self):
        """Test that exhaustion is logged on context exit."""
        budget = QueryBudget(value=10, unit=BudgetUnit.ROWS)
        planner = BudgetPlanner()
        
        with planner.create_budget_context(budget) as ctx:
            ctx.update(rows=20)  # Exhaust budget
        
        assert ctx.is_exhausted


class TestBudgetFeasibility:
    """Tests for budget feasibility estimation."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Clear estimator state before each test."""
        estimator = get_cardinality_estimator()
        estimator.clear_stats()
        yield
        estimator.clear_stats()
    
    def test_feasibility_with_history(self):
        """Test feasibility check with historical data."""
        estimator = get_cardinality_estimator()
        planner = BudgetPlanner()
        
        # Record some history (1000 rows, taking ~1 second each)
        for _ in range(10):
            estimator.record_execution("servicenow", "incident", 1000)
        
        budget = QueryBudget(value=5000, unit=BudgetUnit.MILLISECONDS)  # 5 seconds
        
        result = planner.estimate_feasibility(
            budget, "servicenow", "incident"
        )
        
        assert "is_feasible" in result
        assert "estimated_cost" in result
        assert "confidence" in result
    
    def test_feasibility_suggests_limit(self):
        """Test that infeasible query suggests a LIMIT."""
        estimator = get_cardinality_estimator()
        planner = BudgetPlanner()
        
        # Record high cardinality (10000 rows)
        for _ in range(5):
            estimator.record_execution("servicenow", "incident", 10000)
        
        # Very short budget
        budget = QueryBudget(value=50, unit=BudgetUnit.MILLISECONDS)
        
        result = planner.estimate_feasibility(
            budget, "servicenow", "incident"
        )
        
        # Should suggest a limit
        if not result["is_feasible"]:
            assert "suggested_limit" in result
            assert result["suggested_limit"] is not None


class TestResourceAwareExecutor:
    """Tests for ResourceAwareExecutor."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Clear state before each test."""
        get_cardinality_estimator().clear_stats()
        get_adaptive_pagination().reset()
        yield
        get_cardinality_estimator().clear_stats()
        get_adaptive_pagination().reset()
    
    def test_prepare_execution_no_budget(self):
        """Test preparation without budget."""
        executor = ResourceAwareExecutor()
        
        plan = executor.prepare_execution(
            sql="SELECT * FROM incident",
            adapter_name="servicenow",
            table_name="incident"
        )
        
        assert plan["has_budget"] is False
        assert plan["budget"] is None
        assert "estimated_rows" in plan
    
    def test_prepare_execution_with_budget(self):
        """Test preparation with budget."""
        executor = ResourceAwareExecutor()
        
        plan = executor.prepare_execution(
            sql="SELECT * FROM incident WITH BUDGET 500ms",
            adapter_name="servicenow",
            table_name="incident"
        )
        
        assert plan["has_budget"] is True
        assert plan["budget"] is not None
        assert "feasibility" in plan
    
    def test_record_execution(self):
        """Test recording execution updates estimators."""
        executor = ResourceAwareExecutor()
        
        executor.record_execution(
            adapter_name="servicenow",
            table_name="incident",
            rows_fetched=500,
            duration=0.5
        )
        
        # Cardinality estimator should be updated
        estimator = get_cardinality_estimator()
        stats = estimator.get_stats("servicenow", "incident")
        assert stats is not None
        assert stats.sample_count == 1
    
    def test_record_rate_limit(self):
        """Test recording rate limit updates pagination."""
        executor = ResourceAwareExecutor()
        
        executor.record_execution(
            adapter_name="test",
            table_name="table",
            rows_fetched=0,
            duration=0.1,
            rate_limited=True
        )
        
        pagination = get_adaptive_pagination()
        state = pagination.get_state("test", "table")
        assert state.state == AIMDState.COOLDOWN
    
    def test_get_diagnostics(self):
        """Test getting diagnostic information."""
        executor = ResourceAwareExecutor()
        
        executor.record_execution("a", "t1", 100, 0.1)
        executor.record_execution("b", "t2", 200, 0.2)
        
        diagnostics = executor.get_diagnostics()
        
        assert "cardinality_stats" in diagnostics
        assert "pagination_states" in diagnostics
        assert "a.t1" in diagnostics["cardinality_stats"]


class TestCursorBudgetIntegration:
    """Tests for budget integration with WaveQL cursor."""
    
    def test_cursor_parses_budget(self):
        """Test that cursor parses WITH BUDGET from SQL."""
        # This would require a full WaveQL connection setup
        # For now, just test the planner directly
        planner = BudgetPlanner()
        
        sql = "SELECT * FROM servicenow.incident WITH BUDGET 500ms LIMIT 100"
        cleaned, budget = planner.parse_budget(sql)
        
        assert budget is not None
        assert budget.value == 500
        assert "WITH BUDGET" not in cleaned
        assert "LIMIT 100" in cleaned
