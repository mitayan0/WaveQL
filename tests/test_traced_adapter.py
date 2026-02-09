"""
Tests for WaveQL provenance/traced_adapter module.

This covers the 29% uncovered module waveql/provenance/traced_adapter.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch

from waveql.provenance.traced_adapter import (
    find_primary_key_column,
    traced_fetch,
    _record_row_provenance,
    _safe_str,
    ProvenanceAdapterProxy,
    ADAPTER_PRIMARY_KEYS,
)
from waveql.query_planner import Predicate


class TestFindPrimaryKeyColumn:
    """Tests for find_primary_key_column function."""
    
    def test_find_key_from_defaults(self):
        """Test finding primary key from adapter defaults."""
        table = pa.table({"Id": [1, 2], "Name": ["A", "B"]})
        
        result = find_primary_key_column(table, "salesforce")
        assert result == "Id"
    
    def test_find_key_jira(self):
        """Test finding primary key for Jira adapter."""
        table = pa.table({"key": ["PROJ-1", "PROJ-2"], "summary": ["A", "B"]})
        
        result = find_primary_key_column(table, "jira")
        assert result == "key"
    
    def test_find_key_hubspot(self):
        """Test finding primary key for HubSpot adapter."""
        table = pa.table({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        
        result = find_primary_key_column(table, "hubspot")
        assert result == "id"
    
    def test_find_key_not_in_table(self):
        """Test when default key column doesn't exist in table."""
        table = pa.table({"other_col": [1, 2], "name": ["A", "B"]})
        
        result = find_primary_key_column(table, "salesforce")
        # Should return None if Id column not in table
        assert result is None or result in table.column_names
    
    def test_find_key_unknown_adapter(self):
        """Test finding key for unknown adapter - falls back to heuristic."""
        table = pa.table({"id": [1, 2], "data": ["A", "B"]})
        
        result = find_primary_key_column(table, "unknown_adapter")
        # Falls back to finding common ID column names like 'id'
        assert result == "id"
    
    def test_find_key_csv_adapter(self):
        """Test finding key for CSV adapter falls back to heuristic."""
        table = pa.table({"id": [1, 2], "data": ["A", "B"]})
        
        result = find_primary_key_column(table, "csv")
        # CSV has no default PK but falls back to finding 'id' column
        assert result == "id"
    
    def test_find_key_no_common_columns(self):
        """Test finding key when no common columns exist."""
        table = pa.table({"foo": [1, 2], "bar": ["A", "B"]})
        
        result = find_primary_key_column(table, "csv")
        # No default and no fallback columns found
        assert result is None


class TestSafeStr:
    """Tests for _safe_str helper function."""
    
    def test_safe_str_string(self):
        """Test with string input."""
        assert _safe_str("hello") == "hello"
    
    def test_safe_str_int(self):
        """Test with integer input."""
        assert _safe_str(42) == "42"
    
    def test_safe_str_none(self):
        """Test with None input."""
        assert _safe_str(None) == "NULL"
    
    def test_safe_str_list(self):
        """Test with list input."""
        assert _safe_str([1, 2, 3]) == "[1, 2, 3]"
    
    def test_safe_str_dict(self):
        """Test with dict input."""
        result = _safe_str({"key": "value"})
        assert "key" in result
        assert "value" in result


class TestRecordRowProvenance:
    """Tests for _record_row_provenance helper function."""
    
    def test_record_row_provenance_basic(self):
        """Test recording row provenance."""
        tracker = MagicMock()
        result = pa.table({
            "Id": [1, 2, 3],
            "Name": ["A", "B", "C"],
        })
        
        _record_row_provenance(
            tracker=tracker,
            result=result,
            adapter_name="salesforce",
            table_name="Account",
            trace_id="test-trace-123",
            predicates=[],
        )
        
        # Tracker should have been called for each row
        assert tracker.record_row_provenance.call_count == 3
    
    def test_record_row_provenance_with_predicates(self):
        """Test recording row provenance with predicates."""
        tracker = MagicMock()
        result = pa.table({"id": [1], "name": ["test"]})
        predicates = [
            Predicate(column="status", operator="=", value="active"),
        ]
        
        _record_row_provenance(
            tracker=tracker,
            result=result,
            adapter_name="rest",
            table_name="items",
            trace_id="trace-456",
            predicates=predicates,
        )
        
        assert tracker.record_row_provenance.call_count == 1
    
    def test_record_row_provenance_empty_result(self):
        """Test recording row provenance with empty result."""
        tracker = MagicMock()
        result = pa.table({"id": [], "name": []})
        
        _record_row_provenance(
            tracker=tracker,
            result=result,
            adapter_name="salesforce",
            table_name="Account",
            trace_id="trace-789",
            predicates=[],
        )
        
        # No rows to record
        assert tracker.record_row_provenance.call_count == 0


class TestTracedFetch:
    """Tests for traced_fetch function."""
    
    def test_traced_fetch_basic(self):
        """Test traced fetch with basic parameters."""
        # Create mock adapter
        adapter = MagicMock()
        adapter.adapter_name = "salesforce"
        adapter.fetch.return_value = pa.table({
            "Id": [1, 2],
            "Name": ["A", "B"],
        })
        
        # Create mock tracker
        tracker = MagicMock()
        
        with patch("waveql.provenance.traced_adapter.get_provenance_tracker", return_value=tracker):
            result = traced_fetch(
                adapter=adapter,
                table="Account",
                columns=["Id", "Name"],
            )
        
        assert isinstance(result, pa.Table)
        assert len(result) == 2
        adapter.fetch.assert_called_once()
    
    def test_traced_fetch_with_predicates(self):
        """Test traced fetch with predicates."""
        adapter = MagicMock()
        adapter.adapter_name = "jira"
        adapter.fetch.return_value = pa.table({"key": ["PROJ-1"], "summary": ["Test"]})
        
        predicates = [Predicate(column="project", operator="=", value="PROJ")]
        
        tracker = MagicMock()
        
        with patch("waveql.provenance.traced_adapter.get_provenance_tracker", return_value=tracker):
            result = traced_fetch(
                adapter=adapter,
                table="issues",
                predicates=predicates,
                limit=100,
            )
        
        assert len(result) == 1
        adapter.fetch.assert_called_once()
    
    def test_traced_fetch_disabled_tracker(self):
        """Test traced fetch when tracker is disabled."""
        adapter = MagicMock()
        adapter.adapter_name = "rest"
        adapter.fetch.return_value = pa.table({"id": [1]})
        
        # Mock a disabled tracker instead of None
        mock_tracker = MagicMock()
        mock_tracker.enabled = False
        mock_tracker.current_query = None
        
        with patch("waveql.provenance.traced_adapter.get_provenance_tracker", return_value=mock_tracker):
            result = traced_fetch(
                adapter=adapter,
                table="items",
            )
        
        assert len(result) == 1
        adapter.fetch.assert_called_once()
    
    def test_traced_fetch_with_aggregates(self):
        """Test traced fetch with aggregates."""
        adapter = MagicMock()
        adapter.adapter_name = "salesforce"
        adapter.fetch.return_value = pa.table({"count": [100]})
        
        tracker = MagicMock()
        
        with patch("waveql.provenance.traced_adapter.get_provenance_tracker", return_value=tracker):
            result = traced_fetch(
                adapter=adapter,
                table="Account",
                aggregates=[("COUNT", "*")],
                group_by=["Type"],
            )
        
        assert len(result) == 1


class TestProvenanceAdapterProxy:
    """Tests for ProvenanceAdapterProxy class."""
    
    def test_proxy_init(self):
        """Test proxy initialization."""
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        
        proxy = ProvenanceAdapterProxy(mock_adapter)
        
        assert proxy._adapter == mock_adapter
    
    def test_proxy_getattr_delegation(self):
        """Test that proxy delegates attribute access."""
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "salesforce"
        mock_adapter.some_method.return_value = "result"
        
        proxy = ProvenanceAdapterProxy(mock_adapter)
        
        # Access adapter attribute
        assert proxy.adapter_name == "salesforce"
        
        # Call adapter method
        result = proxy.some_method()
        assert result == "result"
    
    def test_proxy_fetch(self):
        """Test proxy fetch method."""
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "jira"
        mock_adapter.fetch.return_value = pa.table({
            "key": ["PROJ-1", "PROJ-2"],
            "summary": ["Issue 1", "Issue 2"],
        })
        
        proxy = ProvenanceAdapterProxy(mock_adapter)
        
        tracker = MagicMock()
        
        with patch("waveql.provenance.traced_adapter.get_provenance_tracker", return_value=tracker):
            result = proxy.fetch(
                table="issues",
                columns=["key", "summary"],
            )
        
        assert isinstance(result, pa.Table)
        assert len(result) == 2
    
    def test_proxy_fetch_with_all_params(self):
        """Test proxy fetch with all parameters."""
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "rest"
        mock_adapter.fetch.return_value = pa.table({"id": [1]})
        
        proxy = ProvenanceAdapterProxy(mock_adapter)
        
        # Mock a disabled tracker instead of None
        mock_tracker = MagicMock()
        mock_tracker.enabled = False
        mock_tracker.current_query = None
        
        with patch("waveql.provenance.traced_adapter.get_provenance_tracker", return_value=mock_tracker):
            result = proxy.fetch(
                table="items",
                columns=["id", "name"],
                predicates=[Predicate("status", "=", "active")],
                limit=10,
                offset=0,
                order_by=[("created", "DESC")],
                group_by=None,
                aggregates=None,
            )
        
        assert len(result) == 1


class TestAdapterPrimaryKeys:
    """Tests for ADAPTER_PRIMARY_KEYS mapping."""
    
    def test_mapping_exists(self):
        """Test that mapping exists for common adapters."""
        assert "salesforce" in ADAPTER_PRIMARY_KEYS
        assert "jira" in ADAPTER_PRIMARY_KEYS
        assert "hubspot" in ADAPTER_PRIMARY_KEYS
        assert "zendesk" in ADAPTER_PRIMARY_KEYS
        assert "shopify" in ADAPTER_PRIMARY_KEYS
        assert "stripe" in ADAPTER_PRIMARY_KEYS
    
    def test_mapping_values(self):
        """Test correct values in mapping."""
        assert ADAPTER_PRIMARY_KEYS["salesforce"] == "Id"
        assert ADAPTER_PRIMARY_KEYS["jira"] == "key"
        assert ADAPTER_PRIMARY_KEYS["rest"] == "id"
    
    def test_mapping_file_adapters_none(self):
        """Test file adapters have None as primary key."""
        assert ADAPTER_PRIMARY_KEYS["csv"] is None
        assert ADAPTER_PRIMARY_KEYS["excel"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
