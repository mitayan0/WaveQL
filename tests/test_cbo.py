from waveql.connection import WaveQLConnection
from waveql.adapters.base import BaseAdapter
from waveql.optimizer import QueryOptimizer
from waveql.schema_cache import ColumnInfo

class MockLatencyAdapter(BaseAdapter):
    def __init__(self, latency, avg_rows, **kwargs):
        super().__init__(**kwargs)
        self.avg_latency_per_row = latency
        # Pre-populate history to simulate knowledge
        self._execution_history = [{"rows": avg_rows, "duration": latency * avg_rows, "latency_per_row": latency}] * 10

    def list_tables(self):
        return ["table"]

    def get_schema(self, table):
        return [ColumnInfo("id", "string"), ColumnInfo("fkey", "string")]

    def fetch(self, table, columns=None, predicates=None, **kwargs):
        return None  # We only care about planning order

def test_join_reordering_by_latency():
    """
    Test that the planner reorders tables to prioritize:
    1. Tables with predicates (selectivity)
    2. Faster adapters (lower latency)
    """
    conn = WaveQLConnection()
    
    # Adapter A: Fast, 1000 rows
    adapter_fast = MockLatencyAdapter(latency=0.001, avg_rows=1000)
    conn.register_adapter("fast", adapter_fast)
    
    # Adapter B: Slow, 1000 rows
    adapter_slow = MockLatencyAdapter(latency=1.0, avg_rows=1000)
    conn.register_adapter("slow", adapter_slow)
    
    conn.cursor()
    
    # Query involving both tables with NO predicates
    # Should prefer 'fast' first to build filter for 'slow'
    
    # We need to access the planner logic directly or inspect the private method
    # Since we can't easily spy on the internal sorting of execute(), we will expose the sort function
    # or rely on a new optimizer method we invoke.
    
    
    # Create a mock optimizer we will implement
    optimizer = QueryOptimizer()
    
    tables = ["fast.table", "slow.table"]
    predicates = {"fast.table": [], "slow.table": []}
    
    # We expect this new method to exist
    ordered = optimizer.reorder_joins(tables, predicates, conn)
    
    assert ordered[0] == "fast.table"
    assert ordered[1] == "slow.table"

def test_join_reordering_by_cardinality_overrides_latency():
    """
    Test that cardinality (predicates/history) overrides latency.
    We prefer a Slow Small table over a Fast Huge table for semi-join start.
    """
    conn = WaveQLConnection()
    optimizer = QueryOptimizer()
    
    # Adapter A: Fast, 1,000,000 rows
    adapter_fast_huge = MockLatencyAdapter(latency=0.001, avg_rows=1_000_000)
    conn.register_adapter("fast_huge", adapter_fast_huge)
    
    # Adapter B: Slow, 100 rows
    adapter_slow_small = MockLatencyAdapter(latency=1.0, avg_rows=100)
    conn.register_adapter("slow_small", adapter_slow_small)
    
    tables = ["fast_huge.table", "slow_small.table"]
    # Even though slow_small is slow, it returns few rows, so it's a better filter source.
    # Estimated Cost to fetch first:
    # fast_huge: 1000000 * 0.001 = 1000s
    # slow_small: 100 * 1.0 = 100s
    # So slow_small is cheaper to use as driver.
    
    predicates = {"fast_huge.table": [], "slow_small.table": []}
    
    ordered = optimizer.reorder_joins(tables, predicates, conn)
    
    assert ordered[0] == "slow_small.table"
    assert ordered[1] == "fast_huge.table"
