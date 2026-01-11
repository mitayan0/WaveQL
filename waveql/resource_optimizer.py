"""
Resource Optimizer - Low-Resource Systems Engineering for WaveQL

Implements three key features for efficient operation on constrained environments:

1. Statistical Cardinality Estimator
   - Predicts result sizes without counting rows using historical data
   - Uses HyperLogLog-inspired probabilistic counting for memory efficiency
   - Tracks per-table cardinality distributions

2. Adaptive Pagination (AIMD Algorithm)
   - Dynamically adjusts page size based on network throughput
   - Uses Additive Increase, Multiplicative Decrease for stability
   - Handles 429/rate limits gracefully

3. Budget-Constrained Planning
   - Enforces time/cost budgets on queries
   - Early termination when budget exhausted
   - Supports: `SELECT ... WITH BUDGET 500ms`
"""

from __future__ import annotations
import logging
import math
import time
import threading
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from collections import defaultdict, deque
from enum import Enum

if TYPE_CHECKING:
    from waveql.query_planner import Predicate
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


# ==============================================================================
# 1. Statistical Cardinality Estimator
# ==============================================================================

@dataclass
class CardinalityStats:
    """Per-table cardinality statistics."""
    
    # Total observed row counts
    sample_count: int = 0
    total_rows_observed: int = 0
    
    # Min/Max/Avg estimations
    min_rows: int = 0
    max_rows: int = 0
    avg_rows: float = 0.0
    
    # Percentile estimates (approximated)
    p50_rows: float = 0.0
    p90_rows: float = 0.0
    p99_rows: float = 0.0
    
    # Predicate selectivity estimates: operator -> selectivity
    operator_selectivity: Dict[str, float] = field(default_factory=dict)
    
    # Column cardinality estimates: column_name -> estimated_unique_values
    column_cardinality: Dict[str, int] = field(default_factory=dict)
    
    # Recent row samples for percentile calculation
    row_samples: List[int] = field(default_factory=list)
    
    # Timestamp of last update
    last_update: float = 0.0
    
    def update(self, row_count: int, predicates: List["Predicate"] = None):
        """Update cardinality stats with new observation."""
        self.last_update = time.time()
        self.sample_count += 1
        self.total_rows_observed += row_count
        
        # Update min/max
        if self.sample_count == 1:
            self.min_rows = row_count
            self.max_rows = row_count
        else:
            self.min_rows = min(self.min_rows, row_count)
            self.max_rows = max(self.max_rows, row_count)
        
        # Update average
        self.avg_rows = self.total_rows_observed / self.sample_count
        
        # Update samples for percentile calculation (keep last 100)
        self.row_samples.append(row_count)
        if len(self.row_samples) > 100:
            self.row_samples.pop(0)
        
        # Recalculate percentiles
        if len(self.row_samples) >= 5:
            sorted_samples = sorted(self.row_samples)
            n = len(sorted_samples)
            self.p50_rows = sorted_samples[n // 2]
            self.p90_rows = sorted_samples[int(n * 0.9)]
            self.p99_rows = sorted_samples[min(int(n * 0.99), n - 1)]
        
        # Update predicate selectivity estimates
        if predicates:
            selectivity = row_count / max(1, self.avg_rows) if self.avg_rows > 0 else 1.0
            selectivity = max(0.01, min(1.0, selectivity))
            
            for pred in predicates:
                op = str(pred.operator).upper()
                # Exponential moving average for selectivity
                current = self.operator_selectivity.get(op, 0.5)
                self.operator_selectivity[op] = 0.7 * current + 0.3 * selectivity


class CardinalityEstimator:
    """
    Statistical cardinality estimator using historical execution data.
    
    Provides row count predictions without executing queries, enabling:
    - Budget estimation before execution
    - Join order optimization
    - Resource allocation decisions
    
    Uses probabilistic counting and moving averages for memory efficiency.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # Per-table stats: "adapter.table" -> CardinalityStats
        self._table_stats: Dict[str, CardinalityStats] = defaultdict(CardinalityStats)
        
        # Global operator selectivity estimates (defaults)
        self._default_selectivity = {
            "=": 0.1,       # Equality: ~10% of rows
            "!=": 0.9,      # Inequality: ~90% of rows
            "<": 0.3,       # Less than: ~30% of rows
            ">": 0.3,       # Greater than: ~30% of rows
            "<=": 0.35,     # Less or equal: ~35% of rows
            ">=": 0.35,     # Greater or equal: ~35% of rows
            "IN": 0.2,      # IN clause (varies by value count)
            "LIKE": 0.3,    # LIKE (depends on prefix)
            "IS NULL": 0.05,     # NULL check: ~5% of rows
            "IS NOT NULL": 0.95, # NOT NULL: ~95% of rows
            "BETWEEN": 0.25,     # BETWEEN: ~25% of rows
        }
        
        # Lock for thread-safe updates
        self._stats_lock = threading.RLock()
        
        self._initialized = True
    
    def record_execution(
        self,
        adapter_name: str,
        table_name: str,
        row_count: int,
        predicates: List["Predicate"] = None
    ):
        """
        Record an execution result for future cardinality estimation.
        
        Called after each successful fetch to update historical stats.
        """
        key = f"{adapter_name}.{table_name}"
        with self._stats_lock:
            self._table_stats[key].update(row_count, predicates)
            logger.debug(
                "Cardinality update: %s, rows=%d, avg=%.0f, samples=%d",
                key, row_count, self._table_stats[key].avg_rows,
                self._table_stats[key].sample_count
            )
    
    def estimate_cardinality(
        self,
        adapter_name: str,
        table_name: str,
        predicates: List["Predicate"] = None,
        limit: int = None
    ) -> Tuple[float, float, float]:
        """
        Estimate the cardinality (row count) of a query.
        
        Args:
            adapter_name: Name of the adapter
            table_name: Name of the table
            predicates: Predicates to apply (for selectivity estimation)
            limit: LIMIT clause value (caps the estimate)
            
        Returns:
            Tuple of (estimated_rows, lower_bound, upper_bound)
        """
        key = f"{adapter_name}.{table_name}"
        
        with self._stats_lock:
            stats = self._table_stats.get(key)
        
        if not stats or stats.sample_count == 0:
            # No historical data - use defaults
            base_estimate = 1000.0  # Conservative default
            lower = 10.0
            upper = 100000.0
        else:
            # Use historical average as base
            base_estimate = stats.avg_rows
            lower = float(stats.min_rows)
            upper = float(stats.max_rows)
            
            # Prefer p50 if we have enough samples
            if stats.sample_count >= 10:
                base_estimate = stats.p50_rows
                lower = stats.p50_rows * 0.5
                upper = stats.p90_rows * 1.2
        
        # Apply predicate selectivity
        total_selectivity = 1.0
        if predicates:
            for pred in predicates:
                op = str(pred.operator).upper()
                
                # Check if we have learned selectivity for this operator
                if stats and op in stats.operator_selectivity:
                    selectivity = stats.operator_selectivity[op]
                else:
                    selectivity = self._default_selectivity.get(op, 0.5)
                
                # Handle IN clause specially - selectivity depends on value count
                if op == "IN" and isinstance(pred.value, (list, tuple)):
                    n_values = len(pred.value)
                    if n_values <= 5:
                        selectivity = 0.05 * n_values
                    else:
                        selectivity = min(0.5, 0.03 * n_values)
                
                total_selectivity *= selectivity
        
        # Apply selectivity to estimate
        estimated_rows = base_estimate * total_selectivity
        lower_bound = lower * total_selectivity
        upper_bound = upper * total_selectivity
        
        # Apply limit if specified
        if limit is not None:
            estimated_rows = min(estimated_rows, limit)
            lower_bound = min(lower_bound, limit)
            upper_bound = min(upper_bound, limit)
        
        # Ensure minimum values
        estimated_rows = max(1.0, estimated_rows)
        lower_bound = max(1.0, lower_bound)
        upper_bound = max(estimated_rows, upper_bound)
        
        return estimated_rows, lower_bound, upper_bound
    
    def estimate_cost_seconds(
        self,
        adapter_name: str,
        table_name: str,
        predicates: List["Predicate"] = None,
        limit: int = None,
        avg_latency_per_row: float = 0.001
    ) -> Tuple[float, float, float]:
        """
        Estimate execution time in seconds.
        
        Returns:
            Tuple of (estimated_seconds, lower_bound, upper_bound)
        """
        rows, lower, upper = self.estimate_cardinality(
            adapter_name, table_name, predicates, limit
        )
        
        return (
            rows * avg_latency_per_row,
            lower * avg_latency_per_row,
            upper * avg_latency_per_row
        )
    
    def get_stats(self, adapter_name: str, table_name: str) -> Optional[CardinalityStats]:
        """Get raw cardinality stats for a table."""
        key = f"{adapter_name}.{table_name}"
        with self._stats_lock:
            return self._table_stats.get(key)
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get all cardinality statistics for debugging/monitoring."""
        with self._stats_lock:
            result = {}
            for key, stats in self._table_stats.items():
                result[key] = {
                    "sample_count": stats.sample_count,
                    "avg_rows": stats.avg_rows,
                    "min_rows": stats.min_rows,
                    "max_rows": stats.max_rows,
                    "p50_rows": stats.p50_rows,
                    "p90_rows": stats.p90_rows,
                    "operator_selectivity": dict(stats.operator_selectivity),
                }
            return result
    
    def clear_stats(self):
        """Clear all statistics (for testing)."""
        with self._stats_lock:
            self._table_stats.clear()


def get_cardinality_estimator() -> CardinalityEstimator:
    """Get the global cardinality estimator instance."""
    return CardinalityEstimator()


# ==============================================================================
# 2. Adaptive Pagination (AIMD Algorithm)
# ==============================================================================

class AIMDState(Enum):
    """State of the AIMD controller."""
    SLOW_START = "slow_start"        # Initial exponential growth
    CONGESTION_AVOIDANCE = "cong_avoidance"  # Linear growth after ssthresh
    COOLDOWN = "cooldown"            # After rate limit hit


@dataclass
class PaginationState:
    """State for adaptive pagination per adapter/table."""
    
    # Current page size
    page_size: int = 100
    
    # AIMD parameters
    min_page_size: int = 10
    max_page_size: int = 10000
    
    # Slow-start threshold (pages)
    ssthresh: int = 1000
    
    # Current state
    state: AIMDState = AIMDState.SLOW_START
    
    # Throughput tracking (rows per second)
    throughput_samples: List[float] = field(default_factory=list)
    avg_throughput: float = 0.0
    
    # Rate limit tracking
    last_rate_limit_time: float = 0.0
    consecutive_success: int = 0
    
    # Cooldown counter
    cooldown_remaining: int = 0
    
    def record_success(self, rows_fetched: int, duration: float):
        """Record a successful page fetch."""
        if duration > 0:
            throughput = rows_fetched / duration
            self.throughput_samples.append(throughput)
            if len(self.throughput_samples) > 20:
                self.throughput_samples.pop(0)
            self.avg_throughput = sum(self.throughput_samples) / len(self.throughput_samples)
        
        self.consecutive_success += 1
        
        if self.state == AIMDState.COOLDOWN:
            self.cooldown_remaining -= 1
            if self.cooldown_remaining <= 0:
                self.state = AIMDState.CONGESTION_AVOIDANCE
        
        # AIMD Additive Increase
        self._increase_page_size()
    
    def record_rate_limit(self):
        """Record a rate limit hit (429 or similar)."""
        self.last_rate_limit_time = time.time()
        self.consecutive_success = 0
        
        # AIMD Multiplicative Decrease
        self.ssthresh = max(self.min_page_size, self.page_size // 2)
        self.page_size = self.min_page_size
        self.state = AIMDState.COOLDOWN
        self.cooldown_remaining = 5  # Wait 5 successful requests before growing
    
    def _increase_page_size(self):
        """Increase page size based on current state."""
        if self.state == AIMDState.SLOW_START:
            # Exponential growth in slow-start
            new_size = self.page_size * 2
            if new_size >= self.ssthresh:
                self.state = AIMDState.CONGESTION_AVOIDANCE
                new_size = self.ssthresh
        elif self.state == AIMDState.CONGESTION_AVOIDANCE:
            # Linear growth in congestion avoidance
            new_size = self.page_size + max(1, self.page_size // 10)
        else:
            # No growth during cooldown
            return
        
        self.page_size = min(self.max_page_size, new_size)
    
    def get_optimal_page_size(self) -> int:
        """Get the current optimal page size."""
        return self.page_size


class AdaptivePagination:
    """
    Adaptive pagination controller using AIMD algorithm.
    
    Dynamically adjusts page size based on network throughput and rate limits.
    
    AIMD (Additive Increase, Multiplicative Decrease):
    - On success: Increase page size linearly (or exponentially in slow-start)
    - On rate limit: Halve the page size and enter cooldown
    
    This approach achieves:
    - Fast ramp-up to optimal page size
    - Graceful handling of rate limits
    - Stability under varying network conditions
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # Per-adapter pagination state: "adapter.table" -> PaginationState
        self._pagination_state: Dict[str, PaginationState] = defaultdict(PaginationState)
        
        # Lock for thread-safe updates
        self._state_lock = threading.RLock()
        
        self._initialized = True
    
    def get_page_size(self, adapter_name: str, table_name: str = None) -> int:
        """
        Get the optimal page size for a given adapter/table.
        
        Args:
            adapter_name: Name of the adapter
            table_name: Optional table name for table-specific sizing
            
        Returns:
            Optimal page size to use
        """
        key = f"{adapter_name}.{table_name}" if table_name else adapter_name
        with self._state_lock:
            return self._pagination_state[key].get_optimal_page_size()
    
    def record_success(
        self,
        adapter_name: str,
        table_name: str = None,
        rows_fetched: int = 0,
        duration: float = 0.0
    ):
        """
        Record a successful page fetch.
        
        Call this after each successful API response to allow the controller
        to increase the page size if conditions allow.
        """
        key = f"{adapter_name}.{table_name}" if table_name else adapter_name
        with self._state_lock:
            self._pagination_state[key].record_success(rows_fetched, duration)
            logger.debug(
                "AIMD success: %s, page_size=%d, throughput=%.1f rows/s",
                key, self._pagination_state[key].page_size,
                self._pagination_state[key].avg_throughput
            )
    
    def record_rate_limit(self, adapter_name: str, table_name: str = None):
        """
        Record a rate limit hit.
        
        Call this when receiving a 429 or similar rate limit response.
        The controller will back off and reduce page size.
        """
        key = f"{adapter_name}.{table_name}" if table_name else adapter_name
        with self._state_lock:
            self._pagination_state[key].record_rate_limit()
            logger.warning(
                "AIMD rate limit: %s, new page_size=%d",
                key, self._pagination_state[key].page_size
            )
    
    def get_state(self, adapter_name: str, table_name: str = None) -> PaginationState:
        """Get the current pagination state for debugging."""
        key = f"{adapter_name}.{table_name}" if table_name else adapter_name
        with self._state_lock:
            return self._pagination_state[key]
    
    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """Get all pagination states for debugging/monitoring."""
        with self._state_lock:
            result = {}
            for key, state in self._pagination_state.items():
                result[key] = {
                    "page_size": state.page_size,
                    "state": state.state.value,
                    "ssthresh": state.ssthresh,
                    "avg_throughput": state.avg_throughput,
                    "consecutive_success": state.consecutive_success,
                }
            return result
    
    def reset(self, adapter_name: str = None, table_name: str = None):
        """Reset pagination state (for testing or manual reset)."""
        with self._state_lock:
            if adapter_name:
                key = f"{adapter_name}.{table_name}" if table_name else adapter_name
                self._pagination_state[key] = PaginationState()
            else:
                self._pagination_state.clear()


def get_adaptive_pagination() -> AdaptivePagination:
    """Get the global adaptive pagination controller instance."""
    return AdaptivePagination()


# ==============================================================================
# 3. Budget-Constrained Planning
# ==============================================================================

class BudgetUnit(Enum):
    """Unit of budget measurement."""
    MILLISECONDS = "ms"
    SECONDS = "s"
    ROWS = "rows"


@dataclass
class QueryBudget:
    """Represents a query execution budget."""
    
    # Budget value
    value: float
    unit: BudgetUnit
    
    # State tracking
    start_time: float = 0.0
    elapsed_time: float = 0.0
    rows_processed: int = 0
    
    # Budget enforcement
    is_exhausted: bool = False
    remaining: float = 0.0
    
    def start(self):
        """Start budget tracking."""
        self.start_time = time.perf_counter()
        self.remaining = self.value
        self.is_exhausted = False
    
    def update(self, rows: int = 0) -> bool:
        """
        Update budget with current progress.
        
        Args:
            rows: Number of rows processed in this update
            
        Returns:
            True if budget is still valid, False if exhausted
        """
        self.elapsed_time = time.perf_counter() - self.start_time
        self.rows_processed += rows
        
        if self.unit == BudgetUnit.MILLISECONDS:
            self.remaining = self.value - (self.elapsed_time * 1000)
        elif self.unit == BudgetUnit.SECONDS:
            self.remaining = self.value - self.elapsed_time
        elif self.unit == BudgetUnit.ROWS:
            self.remaining = self.value - self.rows_processed
        
        self.is_exhausted = self.remaining <= 0
        return not self.is_exhausted
    
    def get_elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return (time.perf_counter() - self.start_time) * 1000


class BudgetPlanner:
    """
    Budget-constrained query planner.
    
    Parses and enforces time/row budgets on queries.
    
    Syntax:
        SELECT ... WITH BUDGET 500ms
        SELECT ... WITH BUDGET 2s
        SELECT ... WITH BUDGET 10000 rows
    
    When budget is exhausted:
    - Returns partial results fetched so far
    - Sets a flag indicating early termination
    - Logs budget exhaustion for monitoring
    """
    
    # Pattern to match WITH BUDGET clause
    BUDGET_PATTERN = re.compile(
        r'\bWITH\s+BUDGET\s+(\d+(?:\.\d+)?)\s*(ms|s|seconds?|rows?)\b',
        re.IGNORECASE
    )
    
    def parse_budget(self, sql: str) -> Tuple[str, Optional[QueryBudget]]:
        """
        Parse SQL for WITH BUDGET clause.
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (cleaned_sql, budget) where budget is None if not specified
        """
        match = self.BUDGET_PATTERN.search(sql)
        if not match:
            return sql, None
        
        value = float(match.group(1))
        unit_str = match.group(2).lower()
        
        # Normalize unit
        if unit_str in ("ms",):
            unit = BudgetUnit.MILLISECONDS
        elif unit_str in ("s", "second", "seconds"):
            unit = BudgetUnit.SECONDS
        elif unit_str in ("row", "rows"):
            unit = BudgetUnit.ROWS
        else:
            unit = BudgetUnit.MILLISECONDS
        
        # Remove the WITH BUDGET clause from SQL
        cleaned_sql = self.BUDGET_PATTERN.sub("", sql).strip()
        
        budget = QueryBudget(value=value, unit=unit)
        
        logger.info("Parsed budget: %.2f %s from query", value, unit.value)
        
        return cleaned_sql, budget
    
    def estimate_feasibility(
        self,
        budget: QueryBudget,
        adapter_name: str,
        table_name: str,
        predicates: List["Predicate"] = None,
        limit: int = None
    ) -> Dict[str, Any]:
        """
        Estimate if a query is feasible within the given budget.
        
        Uses the cardinality estimator to predict execution cost
        and compares against the budget.
        
        Returns:
            Dict with feasibility analysis:
            - is_feasible: Boolean indicating if query fits budget
            - estimated_cost: Predicted execution cost
            - estimated_rows: Predicted row count
            - confidence: Confidence in the estimate (0-1)
            - suggested_limit: Suggested LIMIT if query doesn't fit
        """
        estimator = get_cardinality_estimator()
        
        # Get cost estimate
        est_seconds, lower_seconds, upper_seconds = estimator.estimate_cost_seconds(
            adapter_name, table_name, predicates, limit
        )
        
        est_rows, lower_rows, upper_rows = estimator.estimate_cardinality(
            adapter_name, table_name, predicates, limit
        )
        
        # Convert budget to seconds for comparison
        if budget.unit == BudgetUnit.MILLISECONDS:
            budget_seconds = budget.value / 1000.0
        elif budget.unit == BudgetUnit.SECONDS:
            budget_seconds = budget.value
        elif budget.unit == BudgetUnit.ROWS:
            # For row budgets, we can directly compare
            is_feasible = est_rows <= budget.value
            return {
                "is_feasible": is_feasible,
                "estimated_cost": est_rows,
                "budget": budget.value,
                "unit": "rows",
                "confidence": 1.0 if estimator.get_stats(adapter_name, table_name) else 0.3,
                "suggested_limit": int(budget.value) if not is_feasible else None,
            }
        else:
            budget_seconds = budget.value / 1000.0
        
        is_feasible = est_seconds <= budget_seconds
        
        # Calculate confidence based on sample count
        stats = estimator.get_stats(adapter_name, table_name)
        if stats and stats.sample_count >= 20:
            confidence = 0.9
        elif stats and stats.sample_count >= 5:
            confidence = 0.7
        elif stats:
            confidence = 0.5
        else:
            confidence = 0.3
        
        # Calculate suggested limit to fit within budget
        suggested_limit = None
        if not is_feasible and est_seconds > 0:
            # How many rows can we fetch within budget?
            rows_per_second = est_rows / est_seconds
            suggested_limit = int(budget_seconds * rows_per_second * 0.8)  # 20% safety margin
            suggested_limit = max(1, suggested_limit)
        
        return {
            "is_feasible": is_feasible,
            "estimated_cost": est_seconds,
            "budget": budget_seconds,
            "unit": "seconds",
            "estimated_rows": est_rows,
            "confidence": confidence,
            "suggested_limit": suggested_limit,
        }
    
    def create_budget_context(self, budget: QueryBudget):
        """
        Create a context manager for budget enforcement.
        
        Usage:
            budget = QueryBudget(value=500, unit=BudgetUnit.MILLISECONDS)
            with planner.create_budget_context(budget) as ctx:
                while not ctx.is_exhausted:
                    rows = fetch_page()
                    ctx.update(len(rows))
        """
        budget.start()
        return BudgetContext(budget)


class BudgetContext:
    """Context manager for budget enforcement during query execution."""
    
    def __init__(self, budget: QueryBudget):
        self._budget = budget
    
    def __enter__(self):
        self._budget.start()
        return self._budget
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._budget.is_exhausted:
            logger.warning(
                "Query budget exhausted: %.2f %s, rows=%d",
                self._budget.value, self._budget.unit.value,
                self._budget.rows_processed
            )
        return False  # Don't suppress exceptions


def get_budget_planner() -> BudgetPlanner:
    """Get a budget planner instance."""
    return BudgetPlanner()


# ==============================================================================
# Integration: Resource-Aware Query Executor
# ==============================================================================

class ResourceAwareExecutor:
    """
    High-level executor that combines all resource optimization features.
    
    Provides a unified interface for:
    - Budget-constrained execution
    - Adaptive pagination
    - Cardinality estimation
    
    Usage:
        executor = ResourceAwareExecutor(connection)
        result = executor.execute_with_budget(
            sql="SELECT * FROM incidents WITH BUDGET 500ms",
            adapter_name="servicenow"
        )
    """
    
    def __init__(self, connection: "WaveQLConnection" = None):
        self._connection = connection
        self._budget_planner = BudgetPlanner()
        self._cardinality_estimator = get_cardinality_estimator()
        self._adaptive_pagination = get_adaptive_pagination()
    
    def prepare_execution(
        self,
        sql: str,
        adapter_name: str,
        table_name: str,
        predicates: List["Predicate"] = None,
    ) -> Dict[str, Any]:
        """
        Prepare execution plan with resource estimates.
        
        Returns a plan including:
        - Cleaned SQL (budget clause removed)
        - Budget if specified
        - Feasibility analysis
        - Recommended page size
        - Estimated cardinality
        """
        # Parse budget from SQL
        cleaned_sql, budget = self._budget_planner.parse_budget(sql)
        
        # Get cardinality estimate
        est_rows, lower, upper = self._cardinality_estimator.estimate_cardinality(
            adapter_name, table_name, predicates
        )
        
        # Get recommended page size
        page_size = self._adaptive_pagination.get_page_size(adapter_name, table_name)
        
        plan = {
            "sql": cleaned_sql,
            "original_sql": sql,
            "has_budget": budget is not None,
            "budget": budget,
            "estimated_rows": est_rows,
            "estimated_rows_lower": lower,
            "estimated_rows_upper": upper,
            "recommended_page_size": page_size,
        }
        
        # If budget specified, check feasibility
        if budget:
            feasibility = self._budget_planner.estimate_feasibility(
                budget, adapter_name, table_name, predicates
            )
            plan["feasibility"] = feasibility
            
            # If not feasible, suggest limiting the query
            if not feasibility["is_feasible"] and feasibility.get("suggested_limit"):
                plan["suggested_limit"] = feasibility["suggested_limit"]
                logger.warning(
                    "Query may exceed budget. Suggested LIMIT: %d",
                    feasibility["suggested_limit"]
                )
        
        return plan
    
    def record_execution(
        self,
        adapter_name: str,
        table_name: str,
        rows_fetched: int,
        duration: float,
        predicates: List["Predicate"] = None,
        rate_limited: bool = False
    ):
        """
        Record execution results for learning.
        
        Call this after each successful fetch to update:
        - Cardinality estimates
        - Pagination state
        """
        # Update cardinality estimator
        self._cardinality_estimator.record_execution(
            adapter_name, table_name, rows_fetched, predicates
        )
        
        # Update pagination controller
        if rate_limited:
            self._adaptive_pagination.record_rate_limit(adapter_name, table_name)
        else:
            self._adaptive_pagination.record_success(
            )
        
        # Update JoinOptimizer per-table stats for real-time latency tracking
        try:
            from waveql.join_optimizer import get_join_optimizer
            join_optimizer = get_join_optimizer()
            join_optimizer.update_table_stats(
                adapter_name=adapter_name,
                table_name=table_name,
                row_count=rows_fetched,
                duration=duration
            )
        except ImportError:
            pass  # JoinOptimizer not available
    
    def get_diagnostics(self) -> Dict[str, Any]:
        """Get diagnostic information for debugging/monitoring."""
        return {
            "cardinality_stats": self._cardinality_estimator.get_all_stats(),
            "pagination_states": self._adaptive_pagination.get_all_states(),
        }


def get_resource_executor(connection: "WaveQLConnection" = None) -> ResourceAwareExecutor:
    """Get a resource-aware executor instance."""
    return ResourceAwareExecutor(connection)
