"""
Tests for WaveQL Observability and EXPLAIN support.
"""

import pytest
import waveql
import pyarrow as pa
from waveql.auth.manager import AuthManager
from waveql.adapters.servicenow import ServiceNowAdapter

def test_explain_direct():
    """Test EXPLAIN with direct DuckDB execution."""
    conn = waveql.connect()
    cursor = conn.cursor()
    
    cursor.execute("EXPLAIN SELECT 1 as test")
    result = cursor.fetchall()
    
    assert len(result) == 1
    assert "Execution Plan" in cursor.description[0][0]
    assert "Direct DuckDB execution" in result[0][0]
    
    # Verify last_plan
    assert cursor.last_plan is not None
    assert cursor.last_plan.is_explain is True
    assert any(step.type == "duckdb" for step in cursor.last_plan.steps)

def test_metrics_capture():
    """Test that metrics are captured for standard queries."""
    conn = waveql.connect()
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1 as val")
    cursor.fetchall()
    
    plan = cursor.last_plan
    assert plan is not None
    assert plan.is_explain is False
    assert plan.total_duration_ms >= 0
    assert len(plan.steps) > 0
    assert plan.steps[0].name == "Direct DuckDB execution"

def test_explain_servicenow_parsing():
    """Test that EXPLAIN correctly identifies adapter routing without executing network calls."""
    # Setup mock adapter
    auth = AuthManager()
    conn = waveql.connect()
    adapter = ServiceNowAdapter(host="sandbox.service-now.com", auth_manager=auth)
    conn.register_adapter("servicenow", adapter)
    
    cursor = conn.cursor()
    
    # We use a table that doesn't exist or a host that's unreachable, 
    # but we want to see if the planner correctly identifies the step.
    # We catch the exception but verify the plan was initialized.
    try:
        cursor.execute("EXPLAIN SELECT * FROM servicenow.incident WHERE active=true")
    except Exception:
        pass
    
    plan = cursor.last_plan
    assert plan is not None
    assert plan.is_explain is True
    # The first step should have been the fetch attempt
    assert plan.steps[0].type == "fetch"
    assert "servicenow" in plan.steps[0].name.lower()

def test_query_metadata_attachment():
    """Test that adapter-specific query metadata is attached to the plan details."""
    conn = waveql.connect()
    cursor = conn.cursor()
    
    # Mock some data with metadata
    data = pa.Table.from_pydict({"a": [1]}, metadata={b"waveql_source_query": b"sysparm_query=active=true"})
    
    # We'll manually add a step to a plan to verify the logic in observability.py
    from waveql.observability import QueryPlan
    plan = QueryPlan(sql="SELECT * FROM mock")
    step = plan.add_step("Mock Fetch", "fetch")
    
    source_query = data.schema.metadata.get(b"waveql_source_query").decode("utf-8")
    step.details["source_query"] = source_query
    step.finish()
    
    assert "active=true" in plan.format_text()
    assert "source_query" in step.details
