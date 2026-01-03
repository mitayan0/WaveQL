"""
Tests for Change Data Capture (CDC) functionality
"""

import pytest
from datetime import datetime, timedelta

from waveql.cdc.models import Change, ChangeType, ChangeStream, CDCConfig


class TestCDCModels:
    """Test CDC data models."""
    
    def test_change_type_enum(self):
        assert ChangeType.INSERT.value == "insert"
        assert ChangeType.UPDATE.value == "update"
        assert ChangeType.DELETE.value == "delete"
    
    def test_change_creation(self):
        change = Change(
            table="incident",
            operation=ChangeType.INSERT,
            key="abc123",
            data={"number": "INC001", "description": "Test"},
            timestamp=datetime.now(),
            source_adapter="servicenow",
        )
        
        assert change.table == "incident"
        assert change.operation == ChangeType.INSERT
        assert change.key == "abc123"
        assert change.is_insert is True
        assert change.is_update is False
        assert change.is_delete is False
    
    def test_change_serialization(self):
        change = Change(
            table="incident",
            operation=ChangeType.UPDATE,
            key="xyz789",
            data={"priority": 1},
        )
        
        data = change.to_dict()
        assert data["table"] == "incident"
        assert data["operation"] == "update"
        assert data["key"] == "xyz789"
        
        # Restore
        restored = Change.from_dict(data)
        assert restored.table == change.table
        assert restored.operation == change.operation
        assert restored.key == change.key
    
    def test_change_repr(self):
        change = Change(
            table="incident",
            operation=ChangeType.INSERT,
            key="abc",
        )
        
        repr_str = repr(change)
        assert "insert" in repr_str
        assert "incident" in repr_str
        assert "abc" in repr_str
    
    def test_change_stream_state(self):
        stream = ChangeStream(table="incident", adapter="servicenow")
        
        assert stream.changes_processed == 0
        
        # Process a change
        change = Change(
            table="incident",
            operation=ChangeType.INSERT,
            key="abc",
            timestamp=datetime.now(),
        )
        stream.update(change)
        
        assert stream.changes_processed == 1
        assert stream.last_key == "abc"
        assert stream.last_sync is not None
    
    def test_cdc_config_defaults(self):
        config = CDCConfig()
        
        assert config.poll_interval == 5.0
        assert config.batch_size == 100
        assert config.include_data is True
        assert config.key_column == "sys_id"
        assert config.sync_column == "sys_updated_on"
    
    def test_cdc_config_custom(self):
        config = CDCConfig(
            poll_interval=10.0,
            batch_size=50,
            key_column="Id",
            sync_column="LastModifiedDate",
            since=datetime(2024, 1, 1),
        )
        
        assert config.poll_interval == 10.0
        assert config.batch_size == 50
        assert config.since == datetime(2024, 1, 1)
    
    def test_cdc_config_validation_poll_interval(self):
        # poll_interval must be positive
        with pytest.raises(ValueError, match="poll_interval must be positive"):
            CDCConfig(poll_interval=0)
        
        with pytest.raises(ValueError, match="poll_interval must be positive"):
            CDCConfig(poll_interval=-1)
    
    def test_cdc_config_validation_batch_size(self):
        # batch_size must be positive
        with pytest.raises(ValueError, match="batch_size must be positive"):
            CDCConfig(batch_size=0)
        
        # batch_size cannot exceed 10000
        with pytest.raises(ValueError, match="batch_size cannot exceed 10000"):
            CDCConfig(batch_size=10001)
    
    def test_cdc_config_serialization(self):
        config = CDCConfig(
            poll_interval=2.5,
            batch_size=50,
            since=datetime(2024, 6, 15, 12, 0, 0),
        )
        
        data = config.to_dict()
        restored = CDCConfig.from_dict(data)
        
        assert restored.poll_interval == config.poll_interval
        assert restored.batch_size == config.batch_size
        assert restored.since == config.since


class TestCDCProviders:
    """Test CDC provider functionality."""
    
    def test_provider_registry(self):
        from waveql.cdc.providers import CDC_PROVIDERS
        
        assert "servicenow" in CDC_PROVIDERS
        assert "salesforce" in CDC_PROVIDERS
        assert "jira" in CDC_PROVIDERS
    
    def test_get_cdc_provider_servicenow(self):
        from waveql.cdc.providers import get_cdc_provider, ServiceNowCDCProvider
        
        # Create a mock adapter
        class MockAdapter:
            adapter_name = "servicenow"
        
        provider = get_cdc_provider("servicenow", MockAdapter())
        
        assert provider is not None
        assert isinstance(provider, ServiceNowCDCProvider)
        assert provider.provider_name == "servicenow"
    
    def test_get_cdc_provider_unknown(self):
        from waveql.cdc.providers import get_cdc_provider
        
        class MockAdapter:
            pass
        
        provider = get_cdc_provider("unknown_adapter", MockAdapter())
        assert provider is None
    
    def test_operation_detection(self):
        from waveql.cdc.providers import BaseCDCProvider
        
        class TestProvider(BaseCDCProvider):
            async def get_changes(self, *args, **kwargs):
                return []
            async def stream_changes(self, *args, **kwargs):
                yield None
        
        class MockAdapter:
            pass
        
        provider = TestProvider(MockAdapter())
        
        # Same created/updated = INSERT
        record = {"created_at": "2024-01-01", "updated_at": "2024-01-01"}
        op = provider._detect_operation(record, "created_at", "updated_at")
        assert op == ChangeType.INSERT
        
        # Different created/updated = UPDATE
        record = {"created_at": "2024-01-01", "updated_at": "2024-01-02"}
        op = provider._detect_operation(record, "created_at", "updated_at")
        assert op == ChangeType.UPDATE


class TestCDCStream:
    """Test the main CDC stream."""
    
    @pytest.fixture
    def mock_conn(self):
        """Create a mock connection."""
        import waveql
        conn = waveql.connect()
        return conn
    
    def test_stream_creation(self, mock_conn):
        from waveql.cdc.stream import CDCStream
        
        stream = CDCStream(mock_conn, "incident")
        
        assert stream.table == "incident"
        assert stream.is_running is False
    
    def test_stream_with_schema_qualified_table(self, mock_conn):
        from waveql.cdc.stream import CDCStream
        
        stream = CDCStream(mock_conn, "servicenow.incident")
        
        assert stream._adapter_name == "servicenow"
        assert stream._table_name == "incident"
    
    def test_connection_stream_changes_method(self, mock_conn):
        stream = mock_conn.stream_changes("incident", poll_interval=10.0)
        
        assert stream is not None
        assert stream.config.poll_interval == 10.0


class TestCDCIntegration:
    """Integration tests for CDC (would require live connection)."""
    
    @pytest.mark.skip(reason="Requires live ServiceNow connection")
    async def test_servicenow_stream(self):
        import waveql
        from datetime import datetime, timedelta
        
        conn = waveql.connect(
            "servicenow://instance.service-now.com",
            username="admin",
            password="password"
        )
        
        # Get changes from last hour
        since = datetime.now() - timedelta(hours=1)
        
        stream = conn.stream_changes(
            "incident",
            since=since,
            poll_interval=5.0
        )
        
        # Get first batch of changes
        changes = await stream.get_changes(since)
        
        for change in changes:
            print(f"{change.operation}: {change.key}")
            assert change.table == "incident"
            assert change.key is not None


# Run with: python -m pytest tests/test_cdc.py -v
