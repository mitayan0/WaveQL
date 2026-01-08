"""
Tests for WaveQL v0.1.7 "Intelligence Layer" features.
Includes:
- CBO Latency Tracking
- Semantic Relationship Discovery
- Parallel Fetching Foundations
- Hybrid Querying Routing
"""

import pytest
import waveql
import pyarrow as pa
import time
from unittest.mock import MagicMock, patch
from waveql.adapters.base import BaseAdapter
from waveql.contracts.models import RelationshipContract, DataContract, ColumnContract

class MockAdapter(BaseAdapter):
    adapter_name = "mock"
    supports_parallel_scan = True
    
    def fetch(self, table, columns=None, predicates=None, **kwargs):
        # Simulate local latency
        time.sleep(0.01)
        return pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    
    def get_schema(self, table):
        return [
            ColumnInfo(name="id", data_type="string", primary_key=True),
            ColumnInfo(name="name", data_type="string")
        ]
        
    def list_tables(self):
        return ["users", "orders"]

from waveql.schema_cache import ColumnInfo

def test_cbo_latency_tracking():
    """Test that latency metrics are updated after execution."""
    conn = waveql.connect()
    adapter = MockAdapter(host="mock")
    conn.register_adapter("mock", adapter)
    
    cursor = conn.cursor()
    
    # Initial latency should be default
    initial_latency = adapter.avg_latency_per_row
    
    # Execute a query
    cursor.execute("SELECT * FROM mock.users")
    cursor.fetchall()
    
    # Latency should have been updated
    assert adapter.avg_latency_per_row != initial_latency
    assert len(adapter._execution_history) == 1
    assert adapter._execution_history[0]["rows"] == 3

def test_relationship_discovery_exact_match():
    """Test discovery of relationships based on naming and types."""
    conn = waveql.connect()
    
    # Mock two adapters with common columns
    adapter1 = MagicMock(spec=BaseAdapter)
    adapter1.list_tables.return_value = ["jira_users"]
    adapter1.get_schema.return_value = [
        ColumnInfo(name="email", data_type="string"),
        ColumnInfo(name="user_id", data_type="string")
    ]
    
    adapter2 = MagicMock(spec=BaseAdapter)
    adapter2.list_tables.return_value = ["sn_users"]
    adapter2.get_schema.return_value = [
        ColumnInfo(name="email", data_type="string"),
        ColumnInfo(name="sys_id", data_type="string")
    ]
    
    conn.register_adapter("jira", adapter1)
    conn.register_adapter("sn", adapter2)
    
    # Discover
    rels = conn.discover_relationships()
    
    # Should find the email -> email link
    assert len(rels) > 0
    email_rel = next((r for r in rels if "email" in r.name), None)
    assert email_rel is not None
    assert "email" in email_rel.source_column
    assert "email" in email_rel.target_column

def test_parallel_fetching_foundations():
    """Test the parallel plan primitive in BaseAdapter."""
    adapter = MockAdapter()
    
    # Default behavior for non-parallel adapter
    adapter.supports_parallel_scan = False
    plan = adapter.get_parallel_plan("table")
    assert len(plan) == 1
    
    # Default behavior for parallel-enabled adapter (single partition unless overridden)
    adapter.supports_parallel_scan = True
    plan = adapter.get_parallel_plan("table", n_partitions=4)
    assert len(plan) == 1

def test_hybrid_query_routing():
    """Test that /*+ HYBRID */ hint triggers hybrid execution logic."""
    conn = waveql.connect()
    cursor = conn.cursor()
    
    # We mock _execute_hybrid to see if it's called
    # We mock _execute_hybrid to see if it's called
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    cursor._execute_hybrid = MagicMock(return_value=mock_table)
    
    cursor.execute("/*+ HYBRID */ SELECT * FROM my_table")
    
    cursor._execute_hybrid.assert_called_once()
    # Verify SQL was passed through
    # call_args[0] is the properties tuple (args, kwargs) if we access .call_args
    # But here we accessed .call_args[0] which is the positional args tuple
    args = cursor._execute_hybrid.call_args[0]
    assert "SELECT * FROM my_table" in args[1]

def test_llm_context_generation():
    """Test semantic metadata and LLM context generation."""
    contract = DataContract(
        table="incidents",
        adapter="servicenow",
        description="Ticketing data",
        columns=[
            ColumnContract(name="sys_id", type="string", nullable=False, primary_key=True, description="Unique ID"),
            ColumnContract(name="short_description", type="string", nullable=False, description="Summary")
        ]
    )
    
    context = contract.to_llm_context()
    assert "### Table: incidents" in context
    assert "Source: servicenow" in context
    assert "sys_id (string, required) [Primary Key]: Unique ID" in context
    assert "short_description (string, required): Summary" in context
