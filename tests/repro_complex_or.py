
import unittest
from unittest.mock import MagicMock
import pyarrow as pa
from waveql.cursor import WaveQLCursor
from waveql.connection import WaveQLConnection
from waveql.adapters.base import BaseAdapter

class MockAdapter(BaseAdapter):
    def __init__(self, name="mock"):
        super().__init__(host="mock")
        self.adapter_name = name
        self.fetch_calls = []

    def fetch(self, table, columns=None, predicates=None, **kwargs):
        self.fetch_calls.append({
            "predicates": predicates
        })
        # Return dummy data: 2 rows.
        # Row 1: matches 'A'
        # Row 2: matches 'B'
        # Row 3: matches 'C' (should be filtered out by A OR B)
        data = [
            {"col": "A"},
            {"col": "B"},
            {"col": "C"}
        ]
        return pa.Table.from_pylist(data)

class TestComplexOR(unittest.TestCase):
    def test_complex_or_pushdown_failure(self):
        # Setup
        conn = MagicMock(spec=WaveQLConnection)
        conn._virtual_views = {}
        conn._policy_manager = None
        conn._cache = MagicMock()
        conn._cache.config.enabled = False
        
        adapter = MockAdapter()
        conn.get_adapter.return_value = adapter
        
        # We need a real DuckDB for the cursor to work if it decides to go local
        import duckdb
        conn._duckdb = duckdb.connect()
        conn.duckdb = conn._duckdb
        
        cursor = WaveQLCursor(conn)
        
        # Query with Complex OR (different columns or just generic OR that QueryPlanner might fail on)
        # QueryPlanner optimizes "col = X OR col = Y" to "col IN (X, Y)".
        # We need "col1 = X OR col2 = Y" to break it.
        sql = "SELECT * FROM mock.table WHERE colA = 'valA' OR colB = 'valB'"
        
        print(f"Executing: {sql}")
        cursor.execute(sql)
        
        # Check what the adapter received
        last_call = adapter.fetch_calls[0]
        preds = last_call["predicates"]
        
        print("Predicates sent to adapter:", preds)
        
        # Expectation: Predicates list is likely EMPTY because QueryPlanner dropped the OR
        if not preds:
            print("FAILURE CONFIRMED: QueryPlanner dropped the complex OR predicate.")
        else:
            print("Surprise! Predicates were preserved:", preds)
            
        # Check result
        result = cursor._result
        print("Result rows:", result.num_rows)
        # If filtering failed, we get all 3 rows from mock
        if result.num_rows == 3:
             print("FAILURE CONFIRMED: Result contains unfiltered rows.")
        
if __name__ == "__main__":
    unittest.main()
