"""
Join Optimizer - Cost-Based Join Re-ordering with Real-Time Latency Stats

This module implements intelligent join re-ordering based on:
1. Real-time latency tracking per table
2. Cardinality estimation using historical data
3. Selectivity estimation based on predicates
4. Network conditions and API rate limits

The optimizer uses a dynamic cost model that adapts to:
- API response times
- Data volume patterns
- Network fluctuations
- Rate limiting behavior
"""

from __future__ import annotations
import logging
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from collections import defaultdict
import math

if TYPE_CHECKING:
    from waveql.query_planner import Predicate
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


@dataclass
class TableStats:
    """Real-time statistics for a table."""
    
    # Latency metrics (in seconds)
    avg_latency_per_row: float = 0.001  # Default: 1ms per row
    p95_latency_per_row: float = 0.002  # 95th percentile
    last_latency: float = 0.001         # Most recent measurement
    
    # Cardinality metrics
    avg_row_count: float = 1000.0       # Average rows returned
    total_rows_estimate: float = 10000.0  # Estimated total table size
    
    # Rate limiting metrics
    rate_limit_hits: int = 0            # Number of rate limit encounters
    last_rate_limit_time: float = 0.0   # Last time rate limited
    
    # Execution history
    execution_count: int = 0
    total_rows_fetched: int = 0
    total_duration: float = 0.0
    
    # Recent latency samples for percentile calculation
    latency_samples: List[float] = field(default_factory=list)
    
    # Timestamp tracking
    last_update: float = 0.0
    
    def update(self, row_count: int, duration: float, rate_limited: bool = False):
        """Update stats with new execution data."""
        self.last_update = time.time()
        self.execution_count += 1
        self.total_rows_fetched += row_count
        self.total_duration += duration
        
        if row_count > 0:
            latency = duration / row_count
            self.last_latency = latency
            
            # Exponential moving average (alpha=0.3 for responsiveness)
            self.avg_latency_per_row = (0.7 * self.avg_latency_per_row) + (0.3 * latency)
            
            # Update latency samples for percentile
            self.latency_samples.append(latency)
            if len(self.latency_samples) > 50:
                self.latency_samples.pop(0)
            
            # Calculate p95
            if len(self.latency_samples) >= 5:
                sorted_samples = sorted(self.latency_samples)
                p95_idx = int(len(sorted_samples) * 0.95)
                self.p95_latency_per_row = sorted_samples[min(p95_idx, len(sorted_samples) - 1)]
        
        # Update cardinality estimate
        if self.execution_count > 0:
            self.avg_row_count = self.total_rows_fetched / self.execution_count
        
        if rate_limited:
            self.rate_limit_hits += 1
            self.last_rate_limit_time = time.time()
    
    def get_effective_latency(self) -> float:
        """
        Get effective latency considering recent rate limits.
        
        If recently rate limited, we penalize the latency to prefer
        other tables that aren't rate limited.
        """
        base_latency = self.avg_latency_per_row
        
        # If rate limited in last 60 seconds, add penalty
        if self.last_rate_limit_time and (time.time() - self.last_rate_limit_time) < 60:
            # Exponential decay penalty
            age = time.time() - self.last_rate_limit_time
            penalty = math.exp(-age / 30) * 2.0  # 2x penalty that decays over 30s
            base_latency *= (1 + penalty)
        
        return base_latency
    
    def is_stale(self, max_age_seconds: float = 300) -> bool:
        """Check if stats are stale (older than max_age_seconds)."""
        if self.last_update == 0:
            return True
        return (time.time() - self.last_update) > max_age_seconds


@dataclass
class JoinEdge:
    """Represents a join relationship between two tables."""
    left_table: str
    right_table: str
    left_column: str
    right_column: str
    join_type: str = "INNER"  # INNER, LEFT, RIGHT, FULL
    
    # Estimated selectivity (0-1, lower = more selective)
    selectivity: float = 0.5


@dataclass
class JoinPlan:
    """A complete join execution plan."""
    table_order: List[str]
    estimated_cost: float
    estimated_rows: float
    join_strategy: str  # "semi_join_pushdown", "hash_join", "nested_loop"
    details: Dict[str, Any] = field(default_factory=dict)


class JoinOptimizer:
    """
    Cost-based join optimizer with real-time latency adaptation.
    
    The optimizer maintains per-table statistics and uses them to
    dynamically reorder joins for optimal performance.
    
    Cost Model:
        Cost(scan) = estimated_rows * effective_latency * selectivity_factor
        Cost(join) = Cost(outer) + Cost(inner) * join_selectivity
        
    Key Features:
        - Real-time latency tracking per table
        - Automatic adaptation to network conditions
        - Support for semi-join pushdown optimization
        - Rate limit awareness
        - Cardinality estimation using historical data
    """
    
    # Singleton instance for global stats
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
        
        # Per-table statistics: adapter.table -> TableStats
        self._table_stats: Dict[str, TableStats] = defaultdict(TableStats)
        
        # Selectivity estimates for common operators
        self._operator_selectivity = {
            "=": 0.1,       # Equality is highly selective
            "!=": 0.9,      # Inequality returns most rows
            "<": 0.3,       # Range scans return moderate rows
            ">": 0.3,
            "<=": 0.35,
            ">=": 0.35,
            "IN": 0.2,      # IN with few values is selective
            "LIKE": 0.3,    # LIKE with prefix is moderately selective
            "IS NULL": 0.05,  # Null checks are usually selective
            "IS NOT NULL": 0.95,
            "BETWEEN": 0.25,
        }
        
        # Lock for thread-safe stats updates
        self._stats_lock = threading.RLock()
        
        self._initialized = True
    
    def get_table_key(self, adapter_name: str, table_name: str) -> str:
        """Generate a unique key for table stats."""
        return f"{adapter_name}.{table_name}"
    
    def update_table_stats(
        self,
        adapter_name: str,
        table_name: str,
        row_count: int,
        duration: float,
        rate_limited: bool = False
    ):
        """
        Update real-time statistics for a table.
        
        Called after each fetch operation to track performance.
        """
        key = self.get_table_key(adapter_name, table_name)
        with self._stats_lock:
            self._table_stats[key].update(row_count, duration, rate_limited)
            
            logger.debug(
                "Updated stats for %s: latency=%.4fs/row, avg_rows=%.0f, executions=%d",
                key,
                self._table_stats[key].avg_latency_per_row,
                self._table_stats[key].avg_row_count,
                self._table_stats[key].execution_count
            )
    
    def get_table_stats(self, adapter_name: str, table_name: str) -> TableStats:
        """Get current statistics for a table."""
        key = self.get_table_key(adapter_name, table_name)
        with self._stats_lock:
            return self._table_stats[key]
    
    def estimate_selectivity(self, predicates: List["Predicate"]) -> float:
        """
        Estimate the selectivity of a set of predicates.
        
        Selectivity is a value between 0 and 1 representing the
        fraction of rows that will pass the filter.
        
        For AND predicates, we multiply selectivities (independent assumption).
        """
        if not predicates:
            return 1.0  # No filter = all rows
        
        selectivity = 1.0
        for pred in predicates:
            op = str(pred.operator).upper()
            op_selectivity = self._operator_selectivity.get(op, 0.5)
            
            # Adjust for IN clause based on number of values
            if op == "IN" and isinstance(pred.value, (list, tuple)):
                # More values = less selective
                n_values = len(pred.value)
                if n_values <= 5:
                    op_selectivity = 0.05 * n_values
                else:
                    op_selectivity = min(0.5, 0.03 * n_values)
            
            selectivity *= op_selectivity
        
        return max(0.01, min(1.0, selectivity))  # Clamp to [0.01, 1.0]
    
    def estimate_join_selectivity(self, join_edge: JoinEdge) -> float:
        """
        Estimate the selectivity of a join.
        
        This is a simplistic model that assumes:
        - Primary key joins are 1:1 (selectivity = 1.0)
        - Foreign key joins reduce by factor of 0.5
        - Many-to-many joins expand (selectivity > 1.0)
        """
        # TODO: Use column statistics if available
        # For now, assume foreign key relationship with moderate reduction
        return 0.7
    
    def estimate_cost(
        self,
        table_name: str,
        predicates: List["Predicate"],
        connection: "WaveQLConnection"
    ) -> Tuple[float, float]:
        """
        Estimate the cost to scan a table with given predicates.
        
        Returns:
            Tuple of (estimated_cost, estimated_rows)
        """
        # Parse table name for adapter
        adapter_name = "default"
        clean_table = table_name
        if "." in table_name:
            parts = table_name.split(".", 1)
            adapter_name = parts[0].strip('"')
            clean_table = parts[1].strip('"') if len(parts) > 1 else table_name
        
        # Get adapter and its stats
        adapter = connection.get_adapter(adapter_name) if connection else None
        adapter_latency = getattr(adapter, "avg_latency_per_row", 0.001) if adapter else 0.001
        
        # Get table-specific stats if available
        key = self.get_table_key(adapter_name, clean_table)
        with self._stats_lock:
            stats = self._table_stats.get(key)
        
        if stats and not stats.is_stale():
            # Use real-time stats
            latency = stats.get_effective_latency()
            base_rows = stats.avg_row_count if stats.avg_row_count > 0 else 1000.0
        else:
            # Fall back to adapter-level stats
            latency = adapter_latency
            base_rows = 1000.0  # Default assumption
            
            # Try to get history from adapter
            if adapter and hasattr(adapter, "_execution_history"):
                history = adapter._execution_history
                if history:
                    total_rows = sum(h.get("rows", 0) for h in history)
                    base_rows = total_rows / len(history) if history else 1000.0
        
        # Apply selectivity
        selectivity = self.estimate_selectivity(predicates)
        estimated_rows = max(1, base_rows * selectivity)
        
        # Calculate cost
        # Cost = rows * latency * (1 + log(rows)/10)  # Slight superlinear scaling for large results
        cost = estimated_rows * latency * (1 + math.log1p(estimated_rows) / 10)
        
        return cost, estimated_rows
    
    def reorder_joins(
        self,
        tables: List[str],
        table_predicates: Dict[str, List["Predicate"]],
        join_edges: List[JoinEdge],
        connection: "WaveQLConnection"
    ) -> JoinPlan:
        """
        Reorder tables for optimal join execution based on real-time latency stats.
        
        Uses a greedy algorithm with look-ahead:
        1. Estimate cost for each table as the starting point
        2. Pick the cheapest table (considering predicates)
        3. For remaining tables, prefer those with join conditions to already-picked tables
        4. Apply semi-join pushdown when beneficial
        
        Args:
            tables: List of normalized table names (e.g., "servicenow.incident")
            table_predicates: Dict mapping table name to its predicates
            join_edges: List of join relationships between tables
            connection: Connection object for adapter access
            
        Returns:
            JoinPlan with optimized table order and cost estimate
        """
        if not tables:
            return JoinPlan(
                table_order=[],
                estimated_cost=0,
                estimated_rows=0,
                join_strategy="none"
            )
        
        if len(tables) == 1:
            cost, rows = self.estimate_cost(tables[0], table_predicates.get(tables[0], []), connection)
            return JoinPlan(
                table_order=tables,
                estimated_cost=cost,
                estimated_rows=rows,
                join_strategy="single_table"
            )
        
        # Build join graph for connectivity analysis
        join_graph: Dict[str, List[Tuple[str, JoinEdge]]] = defaultdict(list)
        for edge in join_edges:
            join_graph[edge.left_table].append((edge.right_table, edge))
            join_graph[edge.right_table].append((edge.left_table, edge))
        
        # Calculate initial costs for all tables
        table_costs: Dict[str, Tuple[float, float]] = {}
        for table in tables:
            preds = table_predicates.get(table, [])
            cost, rows = self.estimate_cost(table, preds, connection)
            table_costs[table] = (cost, rows)
        
        # Greedy selection with join-awareness
        ordered_tables = []
        remaining = set(tables)
        total_cost = 0.0
        cumulative_rows = 0.0
        
        while remaining:
            best_table = None
            best_score = float("inf")
            
            for table in remaining:
                base_cost, base_rows = table_costs[table]
                
                # Adjust cost based on connectivity to already-selected tables
                connectivity_bonus = 0.0
                if ordered_tables:
                    for selected_table in ordered_tables:
                        # Check if there's a direct join edge
                        for neighbor, edge in join_graph.get(selected_table, []):
                            if neighbor == table:
                                # Bonus for being connected (enables semi-join pushdown)
                                connectivity_bonus = 0.3  # 30% cost reduction
                                break
                
                # Calculate effective score
                score = base_cost * (1 - connectivity_bonus)
                
                # If this is not the first table, account for join cost
                if ordered_tables:
                    # Join cost depends on cumulative rows from previous tables
                    join_factor = self.estimate_join_selectivity(
                        JoinEdge(ordered_tables[-1], table, "", "")
                    )
                    score += cumulative_rows * base_cost * 0.1 * join_factor
                
                if score < best_score:
                    best_score = score
                    best_table = table
            
            if best_table:
                ordered_tables.append(best_table)
                remaining.remove(best_table)
                
                cost, rows = table_costs[best_table]
                total_cost += cost
                
                # Update cumulative rows for next iteration
                if cumulative_rows == 0:
                    cumulative_rows = rows
                else:
                    # Apply join selectivity
                    cumulative_rows = cumulative_rows * rows * 0.5  # Simplified
        
        # Determine join strategy
        strategy = "nested_loop"  # Default
        if len(ordered_tables) >= 2:
            # Check if semi-join pushdown is beneficial
            # Condition: First table is much smaller and connected to second
            first_cost, first_rows = table_costs[ordered_tables[0]]
            second_cost, second_rows = table_costs[ordered_tables[1]]
            
            if first_rows < second_rows * 0.5 and first_rows < 10000:
                # Semi-join pushdown is beneficial
                strategy = "semi_join_pushdown"
        
        plan = JoinPlan(
            table_order=ordered_tables,
            estimated_cost=total_cost,
            estimated_rows=cumulative_rows,
            join_strategy=strategy,
            details={
                "table_costs": {t: table_costs[t] for t in ordered_tables},
                "join_edges": len(join_edges),
            }
        )
        
        logger.info(
            "Join plan: %s (cost=%.4f, rows=%.0f, strategy=%s)",
            " -> ".join(ordered_tables),
            total_cost,
            cumulative_rows,
            strategy
        )
        
        return plan
    
    def reorder_joins_simple(
        self,
        tables: List[str],
        predicates: Dict[str, List["Predicate"]],
        connection: "WaveQLConnection"
    ) -> List[str]:
        """
        Simplified join reordering (backward compatible interface).
        
        This method is used by the existing optimizer.reorder_joins() call
        in cursor.py.
        """
        plan = self.reorder_joins(
            tables,
            predicates,
            [],  # No explicit join edges (inferred from query)
            connection
        )
        return plan.table_order
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get all table statistics for debugging/monitoring."""
        with self._stats_lock:
            result = {}
            for key, stats in self._table_stats.items():
                result[key] = {
                    "avg_latency_per_row": stats.avg_latency_per_row,
                    "p95_latency_per_row": stats.p95_latency_per_row,
                    "avg_row_count": stats.avg_row_count,
                    "execution_count": stats.execution_count,
                    "rate_limit_hits": stats.rate_limit_hits,
                    "is_stale": stats.is_stale(),
                }
            return result
    
    def clear_stats(self):
        """Clear all statistics (for testing)."""
        with self._stats_lock:
            self._table_stats.clear()


# Global optimizer instance
def get_join_optimizer() -> JoinOptimizer:
    """Get the global join optimizer instance."""
    return JoinOptimizer()
