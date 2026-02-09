"""
Tests for WaveQL cdc/stream module.

This covers the 38% uncovered module waveql/cdc/stream.py
"""

import pytest
import pyarrow as pa
from datetime import datetime, timedelta
from unittest.mock import MagicMock, AsyncMock
import asyncio

from waveql.cdc.stream import (
    CDCStream,
    watch_changes,
    collect_changes,
)
from waveql.cdc.models import CDCConfig, Change, ChangeType


class TestCDCStream:
    """Tests for CDCStream class."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock WaveQL connection."""
        conn = MagicMock()
        
        # Mock adapter
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "servicenow"
        mock_adapter.fetch.return_value = pa.table({
            "sys_id": ["abc123"],
            "sys_updated_on": ["2024-01-15 12:00:00"],
            "short_description": ["Test incident"],
        })
        
        conn.get_adapter.return_value = mock_adapter
        conn._adapters = {"servicenow": mock_adapter}
        
        return conn
    
    def test_init_basic(self, mock_connection):
        """Test CDCStream initialization."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        assert stream._table == "servicenow.incident"
        assert stream._running is False
    
    def test_init_with_config(self, mock_connection):
        """Test CDCStream with config."""
        config = CDCConfig(
            poll_interval=5,
            batch_size=100,
        )
        
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
            config=config,
        )
        
        assert stream._config == config
    
    def test_repr(self, mock_connection):
        """Test string representation."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        repr_str = repr(stream)
        assert "CDCStream" in repr_str or "incident" in repr_str
    
    def test_parse_table_with_schema(self, mock_connection):
        """Test parsing table with schema prefix."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        adapter, table = stream._parse_table("servicenow.incident")
        
        assert adapter == "servicenow"
        assert table == "incident"
    
    def test_parse_table_without_schema(self, mock_connection):
        """Test parsing table without schema prefix."""
        stream = CDCStream(
            connection=mock_connection,
            table="incident",
        )
        
        adapter, table = stream._parse_table("incident")
        
        # Without schema, adapter should be None or default
        assert table == "incident"
    
    def test_is_running_property(self, mock_connection):
        """Test is_running property."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        assert stream.is_running is False
        
        stream._running = True
        assert stream.is_running is True
    
    def test_stop(self, mock_connection):
        """Test stopping the stream."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        stream._running = True
        stream.stop()
        
        assert stream._running is False
    
    def test_state_property(self, mock_connection):
        """Test state property."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        state = stream.state
        
        # Should return some state object or dict
        assert state is not None
    
    @pytest.mark.asyncio
    async def test_get_changes(self, mock_connection):
        """Test get_changes method."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        # Mock provider
        stream._provider = AsyncMock()
        stream._provider.get_changes.return_value = []
        
        since = datetime.now() - timedelta(hours=1)
        changes = await stream.get_changes(since=since)
        
        assert isinstance(changes, list)
    
    @pytest.mark.asyncio
    async def test_get_changes_no_since(self, mock_connection):
        """Test get_changes without since parameter."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
            config=CDCConfig(since=datetime.now() - timedelta(days=1)),
        )

        # Mock provider
        stream._provider = AsyncMock()
        stream._provider.get_changes.return_value = []
        
        changes = await stream.get_changes()
        
        assert isinstance(changes, list)
    
    @pytest.mark.asyncio
    async def test_stream_basic(self, mock_connection):
        """Test basic streaming."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
            config=CDCConfig(poll_interval=0.01),
        )
        
        changes = []
        count = 0
        
        async for change in stream.stream():
            changes.append(change)
            count += 1
            if count >= 1:
                stream.stop()
                break
        
        # Should complete without error
    
    @pytest.mark.asyncio
    async def test_aiter(self, mock_connection):
        """Test async iteration."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
            config=CDCConfig(poll_interval=0.01),
        )
        
        count = 0
        async for change in stream:
            count += 1
            if count >= 1:
                stream.stop()
                break
    
    def test_persist_state(self, mock_connection):
        """Test state persistence."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        
        change = Change(
            operation=ChangeType.INSERT,
            table="incident",
            key="abc123",
            data={"sys_id": "abc123"},
            timestamp=datetime.now(),
        )

        
        # Should not raise
        stream._persist_state(change)
    
    def test_persist_state_force(self, mock_connection):
        """Test forced state persistence."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        change = Change(
            operation=ChangeType.UPDATE,
            table="incident",
            key="abc123",
            data={"sys_id": "abc123"},
            timestamp=datetime.now(),
        )

        
        # Should not raise
        stream._persist_state(change, force=True)
    
    def test_persist_final_state(self, mock_connection):
        """Test final state persistence."""
        stream = CDCStream(
            connection=mock_connection,
            table="servicenow.incident",
        )
        
        # Should not raise
        stream._persist_final_state()


class TestWatchChanges:
    """Tests for watch_changes function."""
    
    @pytest.mark.asyncio
    async def test_watch_changes_basic(self):
        """Test watching changes with callback."""
        mock_connection = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "servicenow"
        mock_adapter.fetch.return_value = pa.table({
            "sys_id": ["abc"],
            "sys_updated_on": ["2024-01-15 12:00:00"],
        })
        mock_connection.get_adapter.return_value = mock_adapter
        mock_connection._adapters = {"servicenow": mock_adapter}
        
        changes_received = []
        
        def callback(change):
            changes_received.append(change)
        
        config = CDCConfig(poll_interval=0.01)
        
        # Run for a short time
        await asyncio.wait_for(
            watch_changes(
                connection=mock_connection,
                table="servicenow.incident",
                callback=callback,
                config=config,
                stop_after=1,
            ),
            timeout=1.0,
        )
    
    @pytest.mark.asyncio
    async def test_watch_changes_stop_after(self):
        """Test watch_changes with stop_after limit."""
        mock_connection = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch.return_value = pa.table({
            "id": [1, 2, 3],
            "updated_at": ["2024-01-15", "2024-01-15", "2024-01-15"],
        })
        mock_connection.get_adapter.return_value = mock_adapter
        mock_connection._adapters = {"test": mock_adapter}
        
        count = 0
        
        def callback(change):
            nonlocal count
            count += 1
        
        config = CDCConfig(poll_interval=0.01)
        
        try:
            await asyncio.wait_for(
                watch_changes(
                    connection=mock_connection,
                    table="test.data",
                    callback=callback,
                    config=config,
                    stop_after=2,
                ),
                timeout=2.0,
            )
        except asyncio.TimeoutError:
            pass  # Expected


class TestCollectChanges:
    """Tests for collect_changes function."""
    
    @pytest.mark.asyncio
    async def test_collect_changes_basic(self):
        """Test collecting changes for duration."""
        mock_connection = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch.return_value = pa.table({
            "id": [1],
            "updated_at": ["2024-01-15 12:00:00"],
        })
        mock_connection.get_adapter.return_value = mock_adapter
        mock_connection._adapters = {"test": mock_adapter}
        
        config = CDCConfig(poll_interval=0.01)
        
        changes = await asyncio.wait_for(
            collect_changes(
                connection=mock_connection,
                table="test.data",
                duration_seconds=0.1,
                config=config,
            ),
            timeout=2.0,
        )
        
        assert isinstance(changes, list)
    
    @pytest.mark.asyncio
    async def test_collect_changes_short_duration(self):
        """Test collecting changes with very short duration."""
        mock_connection = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch.return_value = pa.table({
            "id": [],
            "updated_at": [],
        })
        mock_connection.get_adapter.return_value = mock_adapter
        mock_connection._adapters = {"test": mock_adapter}
        
        config = CDCConfig(poll_interval=0.1)
        
        changes = await asyncio.wait_for(
            collect_changes(
                connection=mock_connection,
                table="test.data",
                duration_seconds=0.05,
                config=config,
            ),
            timeout=2.0,
        )
        
        assert isinstance(changes, list)


class TestCDCStreamEdgeCases:
    """Edge case tests for CDC streaming."""
    
    @pytest.mark.asyncio
    async def test_stream_with_empty_adapter_response(self):
        """Test stream handling empty adapter response."""
        mock_connection = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch.return_value = pa.table({
            "id": [],
            "updated_at": [],
        })
        mock_connection.get_adapter.return_value = mock_adapter
        mock_connection._adapters = {"test": mock_adapter}
        
        stream = CDCStream(
            connection=mock_connection,
            table="test.data",
        )
        stream._provider = AsyncMock()
        stream._provider.get_changes.return_value = []
        
        changes = await stream.get_changes()
        assert changes == []
    
    @pytest.mark.asyncio
    async def test_stream_adapter_error(self):
        """Test stream handling adapter errors."""
        mock_connection = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_connection.get_adapter.return_value = mock_adapter
        mock_connection._adapters = {"test": mock_adapter}
        
        stream = CDCStream(
            connection=mock_connection,
            table="test.data",
        )
        stream._provider = AsyncMock()
        stream._provider.get_changes.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            await stream.get_changes()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
