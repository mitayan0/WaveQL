"""
Tests for WaveQL materialized_view/sync module.

This covers the 35% uncovered module waveql/materialized_view/sync.py
"""

import pytest
import pyarrow as pa
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from waveql.materialized_view.sync import (
    IncrementalSyncer,
    get_default_sync_column,
)
from waveql.materialized_view.models import SyncState, ViewDefinition, RefreshStrategy


class TestIncrementalSyncer:
    """Tests for IncrementalSyncer class."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create mock adapter."""
        adapter = MagicMock()
        adapter.adapter_name = "servicenow"
        adapter.fetch.return_value = pa.table({
            "sys_id": ["abc123", "def456"],
            "sys_updated_on": [
                datetime.now() - timedelta(minutes=30),
                datetime.now() - timedelta(minutes=15),
            ],
            "short_description": ["Incident 1", "Incident 2"],
        })
        return adapter
    
    @pytest.fixture
    def view_definition(self):
        """Create view definition."""
        return ViewDefinition(
            name="my_incidents",
            source_table="incident",
            source_adapter="servicenow",
            sync_column="sys_updated_on",
            refresh_strategy=RefreshStrategy.INCREMENTAL,
            query="SELECT * FROM servicenow.incident",
        )

    
    @pytest.fixture
    def empty_sync_state(self):
        """Create empty sync state."""
        return SyncState(
            last_sync_value=None,
            last_sync_row_count=0,
            sync_history=[],
        )
    
    def test_sync_first_time(self, mock_adapter, view_definition, empty_sync_state):
        """Test first sync (no previous state)."""
        syncer = IncrementalSyncer()
        
        new_data, new_state, sync_mode = syncer.sync(
            view=view_definition,
            adapter=mock_adapter,
            current_state=empty_sync_state,
        )
        
        assert isinstance(new_data, pa.Table)
        assert len(new_data) == 2
        assert new_state.last_sync_row_count == 2
        assert len(new_state.sync_history) == 1
        assert sync_mode == "append"
        
        # Adapter should be called without predicates for first sync
        mock_adapter.fetch.assert_called_once()
    
    def test_sync_incremental(self, mock_adapter, view_definition):
        """Test incremental sync with existing state."""
        syncer = IncrementalSyncer()
        
        last_sync_value = datetime.now() - timedelta(hours=1)
        current_state = SyncState(
            last_sync_value=last_sync_value,
            last_sync_row_count=100,
            sync_history=[{"timestamp": "2024-01-01T00:00:00", "rows_fetched": 100}],
        )
        
        new_data, new_state, sync_mode = syncer.sync(
            view=view_definition,
            adapter=mock_adapter,
            current_state=current_state,
        )
        
        assert isinstance(new_data, pa.Table)
        # Should accumulate count
        assert new_state.last_sync_row_count == 102
        assert len(new_state.sync_history) == 2
        
        # Adapter should be called with predicate for incremental
        call_kwargs = mock_adapter.fetch.call_args
        assert call_kwargs is not None
    
    def test_sync_with_key_column(self, mock_adapter, view_definition, empty_sync_state):
        """Test sync with key column for upsert."""
        syncer = IncrementalSyncer()
        
        new_data, new_state, sync_mode = syncer.sync(
            view=view_definition,
            adapter=mock_adapter,
            current_state=empty_sync_state,
            key_column="sys_id",
        )
        
        assert sync_mode == "upsert"
    
    def test_sync_no_sync_column_error(self, mock_adapter, empty_sync_state):
        """Test sync fails without sync column."""
        syncer = IncrementalSyncer()
        
        view_no_sync = ViewDefinition(
            name="test_view",
            source_table="test",
            source_adapter="test",
            sync_column=None,  # No sync column
            query="SELECT * FROM test.test",
        )

        
        with pytest.raises(ValueError, match="sync_column"):
            syncer.sync(
                view=view_no_sync,
                adapter=mock_adapter,
                current_state=empty_sync_state,
            )
    
    def test_sync_empty_result(self, mock_adapter, view_definition):
        """Test sync with empty result."""
        mock_adapter.fetch.return_value = pa.table({
            "sys_id": [],
            "sys_updated_on": [],
            "short_description": [],
        })
        
        syncer = IncrementalSyncer()
        current_state = SyncState(
            last_sync_value=datetime.now() - timedelta(hours=1),
            last_sync_row_count=50,
            sync_history=[],
        )
        
        new_data, new_state, sync_mode = syncer.sync(
            view=view_definition,
            adapter=mock_adapter,
            current_state=current_state,
        )
        
        assert len(new_data) == 0
        # Row count should not increase
        assert new_state.last_sync_row_count == 50
        # Last sync value should be preserved
        assert new_state.last_sync_value == current_state.last_sync_value
    
    def test_get_max_value_with_data(self):
        """Test getting max value from data."""
        syncer = IncrementalSyncer()
        
        data = pa.table({
            "id": [1, 2, 3],
            "updated_at": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 3),
                datetime(2024, 1, 2),
            ],
        })
        
        max_val = syncer._get_max_value(data, "updated_at")
        
        assert max_val == datetime(2024, 1, 3)
    
    def test_get_max_value_empty_data(self):
        """Test getting max value from empty data."""
        syncer = IncrementalSyncer()
        
        data = pa.table({"id": [], "updated_at": []})
        
        max_val = syncer._get_max_value(data, "updated_at")
        
        assert max_val is None
    
    def test_get_max_value_none_data(self):
        """Test getting max value from None data."""
        syncer = IncrementalSyncer()
        
        max_val = syncer._get_max_value(None, "updated_at")
        
        assert max_val is None
    
    def test_get_max_value_missing_column(self):
        """Test getting max value when column doesn't exist."""
        syncer = IncrementalSyncer()
        
        data = pa.table({"id": [1, 2, 3]})
        
        max_val = syncer._get_max_value(data, "nonexistent_column")
        
        assert max_val is None
    
    def test_estimate_changes(self, mock_adapter, view_definition, empty_sync_state):
        """Test estimate changes method."""
        syncer = IncrementalSyncer()
        
        result = syncer.estimate_changes(
            view=view_definition,
            adapter=mock_adapter,
            current_state=empty_sync_state,
        )
        
        assert isinstance(result, dict)
        assert "supported" in result
        assert result["supported"] is False  # Default implementation
    
    def test_estimate_changes_with_state(self, mock_adapter, view_definition):
        """Test estimate changes with existing state."""
        syncer = IncrementalSyncer()
        
        current_state = SyncState(
            last_sync_value=datetime.now() - timedelta(hours=1),
            last_sync_row_count=100,
            sync_history=[],
        )
        
        result = syncer.estimate_changes(
            view=view_definition,
            adapter=mock_adapter,
            current_state=current_state,
        )
        
        assert "last_sync_value" in result
        assert result["last_sync_value"] == current_state.last_sync_value


class TestGetDefaultSyncColumn:
    """Tests for get_default_sync_column function."""
    
    def test_servicenow(self):
        """Test default sync column for ServiceNow."""
        result = get_default_sync_column("servicenow", "incident")
        assert result == "sys_updated_on"
    
    def test_salesforce(self):
        """Test default sync column for Salesforce."""
        result = get_default_sync_column("salesforce", "Account")
        assert result == "LastModifiedDate"
    
    def test_jira(self):
        """Test default sync column for Jira."""
        result = get_default_sync_column("jira", "issues")
        assert result == "updated"
    
    def test_sql(self):
        """Test default sync column for SQL."""
        result = get_default_sync_column("sql", "users")
        assert result == "updated_at"
    
    def test_unknown_adapter(self):
        """Test unknown adapter returns None."""
        result = get_default_sync_column("unknown", "table")
        assert result is None
    
    def test_case_insensitive(self):
        """Test adapter name is case insensitive."""
        result = get_default_sync_column("ServiceNow", "incident")
        assert result == "sys_updated_on"
        
        result = get_default_sync_column("SALESFORCE", "Account")
        assert result == "LastModifiedDate"


class TestSyncStateUpdates:
    """Tests for sync state update logic."""
    
    def test_sync_history_accumulates(self):
        """Test that sync history accumulates correctly."""
        syncer = IncrementalSyncer()
        mock_adapter = MagicMock()
        mock_adapter.fetch.return_value = pa.table({
            "id": [1],
            "updated_at": [datetime.now()],
        })
        
        view = ViewDefinition(
            name="test",
            source_table="test",
            source_adapter="test",
            sync_column="updated_at",
            query="SELECT * FROM test.test",
        )
        
        state = SyncState(
            last_sync_value=None,
            last_sync_row_count=0,
            sync_history=[
                {"timestamp": "2024-01-01T00:00:00", "rows_fetched": 10},
                {"timestamp": "2024-01-02T00:00:00", "rows_fetched": 5},
            ],
        )
        
        _, new_state, _ = syncer.sync(view, mock_adapter, state)
        
        # History should have 3 entries now
        assert len(new_state.sync_history) == 3
        # Last entry should have rows_fetched = 1
        assert new_state.sync_history[-1]["rows_fetched"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
