"""
Tests for Query Provenance - Novel Research Area

These tests verify the provenance tracking infrastructure for
SQL-over-API federation systems.
"""

import pytest
import time
import threading

from waveql.provenance import (
    enable_provenance,
    disable_provenance,
    get_provenance_tracker,
    APICallTrace,
    PredicateMatch,
    RowProvenance,
    QueryProvenance,
)
from waveql.provenance.tracker import attach_provenance, extract_provenance


class TestProvenanceModels:
    """Test provenance data models."""
    
    def test_api_call_trace_defaults(self):
        """APICallTrace should have sensible defaults."""
        trace = APICallTrace()
        assert trace.trace_id is not None
        assert trace.adapter_name == ""
        assert trace.http_method == "GET"
        assert trace.response_status == 200
    
    def test_api_call_trace_repr(self):
        """APICallTrace should have readable repr."""
        trace = APICallTrace(
            adapter_name="servicenow",
            table_name="incident",
            rows_returned=42,
            response_time_ms=150.5,
        )
        assert "servicenow.incident" in repr(trace)
        assert "42 rows" in repr(trace)
    
    def test_predicate_match(self):
        """PredicateMatch should capture predicate details."""
        match = PredicateMatch(
            column="priority",
            operator="<",
            value=3,
            source="user",
        )
        assert match.column == "priority"
        assert "priority < 3" in repr(match)
    
    def test_row_provenance(self):
        """RowProvenance should track single row origin."""
        prov = RowProvenance(
            row_index=0,
            source_adapter="servicenow",
            source_table="incident",
            source_primary_key="abc123",
        )
        assert prov.source_adapter == "servicenow"
        assert "pk=abc123" in repr(prov)
    
    def test_query_provenance_to_dict(self):
        """QueryProvenance should serialize to dict."""
        query_prov = QueryProvenance(
            original_sql="SELECT * FROM incident",
        )
        query_prov.api_calls.append(APICallTrace(
            adapter_name="servicenow",
            table_name="incident",
            rows_returned=10,
        ))
        
        d = query_prov.to_dict()
        assert d["original_sql"] == "SELECT * FROM incident"
        assert len(d["api_calls"]) == 1
        assert d["api_calls"][0]["adapter"] == "servicenow"


class TestProvenanceTracker:
    """Test the ProvenanceTracker singleton."""
    
    def setup_method(self):
        """Reset tracker state before each test."""
        tracker = get_provenance_tracker()
        tracker.disable()
        tracker.clear_history()
    
    def test_singleton_pattern(self):
        """Tracker should be a singleton."""
        t1 = get_provenance_tracker()
        t2 = get_provenance_tracker()
        assert t1 is t2
    
    def test_enable_disable(self):
        """Tracker should toggle enabled state."""
        tracker = get_provenance_tracker()
        
        assert not tracker.enabled
        
        enable_provenance(mode="summary")
        assert tracker.enabled
        assert tracker.mode == "summary"
        
        disable_provenance()
        assert not tracker.enabled
    
    def test_invalid_mode_raises(self):
        """Invalid mode should raise ValueError."""
        tracker = get_provenance_tracker()
        with pytest.raises(ValueError):
            tracker.enable(mode="invalid")
    
    def test_trace_query_context_manager(self):
        """trace_query should capture SQL and timing."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            assert prov.original_sql == "SELECT * FROM incident"
            assert prov.execution_start is not None
            time.sleep(0.01)  # Small delay to ensure timing
        
        assert prov.execution_end is not None
        assert prov.total_latency_ms > 0
        assert prov in tracker.get_history()
    
    def test_trace_query_disabled(self):
        """trace_query should yield None when disabled."""
        tracker = get_provenance_tracker()
        tracker.disable()
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            assert prov is None
    
    def test_record_api_call(self):
        """API calls should be recorded in active trace."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            trace_id = tracker.record_api_call(
                adapter_name="servicenow",
                table_name="incident",
                endpoint_url="https://dev.service-now.com/api/now/table/incident",
                response_time_ms=150.0,
                rows_returned=42,
            )
            assert trace_id is not None
        
        assert len(prov.api_calls) == 1
        assert prov.api_calls[0].rows_returned == 42
        assert prov.api_calls[0].adapter_name == "servicenow"
        assert "servicenow" in prov.adapters_used
        assert "servicenow.incident" in prov.tables_accessed
    
    def test_record_api_call_disabled(self):
        """No data should be recorded when disabled."""
        tracker = get_provenance_tracker()
        tracker.disable()
        
        result = tracker.record_api_call(
            adapter_name="test",
            table_name="test",
            endpoint_url="http://test",
        )
        
        assert result is None
    
    def test_mode_summary_skips_row_provenance(self):
        """Summary mode should not record per-row provenance."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            tracker.record_row_provenance(
                row_index=0,
                source_adapter="servicenow",
                source_table="incident",
            )
        
        assert len(prov.row_provenance) == 0
    
    def test_mode_full_records_row_provenance(self):
        """Full mode should record per-row provenance."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="full")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            tracker.record_row_provenance(
                row_index=0,
                source_adapter="servicenow",
                source_table="incident",
                source_primary_key="abc123",
            )
        
        assert len(prov.row_provenance) == 1
        assert prov.row_provenance[0].source_primary_key == "abc123"
    
    def test_row_provenance_cap(self):
        """Row provenance should be capped at max_row_provenance."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="full")
        tracker.max_row_provenance = 5
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            for i in range(10):
                tracker.record_row_provenance(
                    row_index=i,
                    source_adapter="servicenow",
                    source_table="incident",
                )
        
        assert len(prov.row_provenance) == 5
    
    def test_history_cap(self):
        """History should be capped at max_history."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        tracker._max_history = 3
        
        for i in range(5):
            with tracker.trace_query(f"SELECT {i} FROM test"):
                pass
        
        history = tracker.get_history()
        assert len(history) == 3
        # Should have the most recent queries
        assert "SELECT 4" in history[-1].original_sql
    
    def test_thread_safety(self):
        """Tracker should be thread-safe."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        results = []
        
        def run_query(query_id):
            with tracker.trace_query(f"SELECT * FROM table_{query_id}") as prov:
                tracker.record_api_call(
                    adapter_name=f"adapter_{query_id}",
                    table_name=f"table_{query_id}",
                    endpoint_url=f"http://test/{query_id}",
                    rows_returned=query_id,
                )
                time.sleep(0.01)  # Simulate work
                results.append(prov)
        
        threads = [threading.Thread(target=run_query, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Each thread should have its own provenance
        assert len(results) == 5
        for prov in results:
            assert len(prov.api_calls) == 1


class TestPyArrowIntegration:
    """Test PyArrow metadata integration."""
    
    def test_attach_and_extract_provenance(self):
        """Provenance should round-trip through PyArrow metadata."""
        import pyarrow as pa
        
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        
        prov = QueryProvenance(
            original_sql="SELECT * FROM test",
        )
        prov.api_calls.append(APICallTrace(
            adapter_name="test",
            table_name="test",
            rows_returned=3,
        ))
        prov.adapters_used = ["test"]
        
        # Attach
        table_with_prov = attach_provenance(table, prov)
        
        # Extract
        extracted = extract_provenance(table_with_prov)
        
        assert extracted is not None
        assert extracted["original_sql"] == "SELECT * FROM test"
        assert len(extracted["api_calls"]) == 1
        assert extracted["api_calls"][0]["adapter"] == "test"
    
    def test_extract_from_table_without_provenance(self):
        """Extracting from table without provenance should return None."""
        import pyarrow as pa
        
        table = pa.table({"id": [1, 2]})
        assert extract_provenance(table) is None


class TestPublicAPI:
    """Test the public API convenience functions."""
    
    def setup_method(self):
        disable_provenance()
        get_provenance_tracker().clear_history()
    
    def test_enable_disable_functions(self):
        """enable_provenance and disable_provenance should work."""
        tracker = get_provenance_tracker()
        
        enable_provenance(mode="full")
        assert tracker.enabled
        assert tracker.mode == "full"
        
        disable_provenance()
        assert not tracker.enabled
