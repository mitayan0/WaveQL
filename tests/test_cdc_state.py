"""
Tests for CDC State Backend (Persistence Layer)
"""

import os
import tempfile
from datetime import datetime

import pytest

from waveql.cdc.state import (
    StreamPosition,
    MemoryStateBackend,
    SQLiteStateBackend,
    create_state_backend,
)


class TestStreamPosition:
    """Tests for StreamPosition dataclass."""
    
    def test_create_position(self):
        """Test creating a stream position."""
        pos = StreamPosition(
            table="incident",
            adapter="servicenow",
            lsn="00000001/00000010",
            offset=100,
        )
        
        assert pos.table == "incident"
        assert pos.adapter == "servicenow"
        assert pos.lsn == "00000001/00000010"
        assert pos.offset == 100
    
    def test_to_dict(self):
        """Test serialization to dictionary."""
        pos = StreamPosition(
            table="users",
            adapter="postgres",
            lsn="ABC123",
            last_sync=datetime(2024, 1, 15, 10, 30, 0),
        )
        
        data = pos.to_dict()
        
        assert data["table"] == "users"
        assert data["adapter"] == "postgres"
        assert data["lsn"] == "ABC123"
        assert data["last_sync"] == "2024-01-15T10:30:00"
    
    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "table": "orders",
            "adapter": "salesforce",
            "lsn": None,
            "offset": 500,
            "last_key": "ABC123",
            "last_sync": "2024-01-15T10:30:00",
            "metadata": {"extra": "data"},
        }
        
        pos = StreamPosition.from_dict(data)
        
        assert pos.table == "orders"
        assert pos.adapter == "salesforce"
        assert pos.offset == 500
        assert pos.last_key == "ABC123"
        assert pos.metadata == {"extra": "data"}
    
    def test_repr(self):
        """Test string representation."""
        pos = StreamPosition(table="test", adapter="mock", lsn="12345")
        
        assert "mock.test" in repr(pos)
        assert "12345" in repr(pos)


class TestMemoryStateBackend:
    """Tests for MemoryStateBackend."""
    
    @pytest.fixture
    def backend(self):
        """Create a memory backend."""
        return MemoryStateBackend()
    
    def test_save_and_get(self, backend):
        """Test saving and retrieving a position."""
        backend.save_position(
            table="incident",
            adapter="servicenow",
            lsn="ABC123",
            offset=100,
        )
        
        pos = backend.get_position("incident", "servicenow")
        
        assert pos is not None
        assert pos.table == "incident"
        assert pos.adapter == "servicenow"
        assert pos.lsn == "ABC123"
        assert pos.offset == 100
    
    def test_get_nonexistent(self, backend):
        """Test getting a non-existent position."""
        pos = backend.get_position("nonexistent", "mock")
        
        assert pos is None
    
    def test_update_position(self, backend):
        """Test updating an existing position."""
        backend.save_position(table="test", adapter="mock", offset=10)
        backend.save_position(table="test", adapter="mock", offset=20)
        
        pos = backend.get_position("test", "mock")
        
        assert pos.offset == 20
    
    def test_delete_position(self, backend):
        """Test deleting a position."""
        backend.save_position(table="test", adapter="mock", offset=10)
        
        result = backend.delete_position("test", "mock")
        
        assert result is True
        assert backend.get_position("test", "mock") is None
    
    def test_delete_nonexistent(self, backend):
        """Test deleting a non-existent position."""
        result = backend.delete_position("nonexistent", "mock")
        
        assert result is False
    
    def test_list_positions(self, backend):
        """Test listing all positions."""
        backend.save_position(table="t1", adapter="a1", offset=1)
        backend.save_position(table="t2", adapter="a1", offset=2)
        backend.save_position(table="t3", adapter="a2", offset=3)
        
        all_positions = backend.list_positions()
        
        assert len(all_positions) == 3
    
    def test_list_positions_by_adapter(self, backend):
        """Test listing positions filtered by adapter."""
        backend.save_position(table="t1", adapter="a1", offset=1)
        backend.save_position(table="t2", adapter="a1", offset=2)
        backend.save_position(table="t3", adapter="a2", offset=3)
        
        a1_positions = backend.list_positions(adapter="a1")
        
        assert len(a1_positions) == 2
        assert all(p.adapter == "a1" for p in a1_positions)
    
    def test_close(self, backend):
        """Test closing the backend."""
        backend.save_position(table="test", adapter="mock", offset=10)
        backend.close()
        
        # Memory backend clears on close
        pos = backend.get_position("test", "mock")
        assert pos is None


class TestSQLiteStateBackend:
    """Tests for SQLiteStateBackend."""
    
    @pytest.fixture
    def db_path(self):
        """Create a temporary database path."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        yield path
        # Cleanup
        if os.path.exists(path):
            os.unlink(path)
    
    @pytest.fixture
    def backend(self, db_path):
        """Create a SQLite backend."""
        return SQLiteStateBackend(db_path)
    
    def test_init_creates_db(self, db_path):
        """Test that initialization creates the database."""
        backend = SQLiteStateBackend(db_path)
        
        assert os.path.exists(db_path)
        backend.close()
    
    def test_save_and_get(self, backend):
        """Test saving and retrieving a position."""
        backend.save_position(
            table="incident",
            adapter="servicenow",
            lsn="ABC123",
            offset=100,
            metadata={"source": "test"},
        )
        
        pos = backend.get_position("incident", "servicenow")
        
        assert pos is not None
        assert pos.table == "incident"
        assert pos.adapter == "servicenow"
        assert pos.lsn == "ABC123"
        assert pos.offset == 100
        assert pos.metadata == {"source": "test"}
    
    def test_persistence(self, db_path):
        """Test that data persists across backend instances."""
        # Save with first instance
        backend1 = SQLiteStateBackend(db_path)
        backend1.save_position(table="test", adapter="mock", offset=42)
        backend1.close()
        
        # Read with second instance
        backend2 = SQLiteStateBackend(db_path)
        pos = backend2.get_position("test", "mock")
        backend2.close()
        
        assert pos is not None
        assert pos.offset == 42
    
    def test_update_position(self, backend):
        """Test updating an existing position."""
        backend.save_position(table="test", adapter="mock", offset=10)
        backend.save_position(table="test", adapter="mock", offset=20)
        
        pos = backend.get_position("test", "mock")
        
        assert pos.offset == 20
    
    def test_delete_position(self, backend):
        """Test deleting a position."""
        backend.save_position(table="test", adapter="mock", offset=10)
        
        result = backend.delete_position("test", "mock")
        
        assert result is True
        assert backend.get_position("test", "mock") is None
    
    def test_list_positions(self, backend):
        """Test listing all positions."""
        backend.save_position(table="t1", adapter="a1", offset=1)
        backend.save_position(table="t2", adapter="a1", offset=2)
        backend.save_position(table="t3", adapter="a2", offset=3)
        
        all_positions = backend.list_positions()
        
        assert len(all_positions) == 3
    
    def test_list_positions_by_adapter(self, backend):
        """Test listing positions filtered by adapter."""
        backend.save_position(table="t1", adapter="a1", offset=1)
        backend.save_position(table="t2", adapter="a1", offset=2)
        backend.save_position(table="t3", adapter="a2", offset=3)
        
        a1_positions = backend.list_positions(adapter="a1")
        
        assert len(a1_positions) == 2


class TestCreateStateBackend:
    """Tests for create_state_backend factory function."""
    
    def test_create_memory(self):
        """Test creating a memory backend."""
        backend = create_state_backend("memory")
        
        assert isinstance(backend, MemoryStateBackend)
        backend.close()
    
    def test_create_sqlite(self):
        """Test creating a SQLite backend."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        
        try:
            backend = create_state_backend("sqlite", db_path=path)
            
            assert isinstance(backend, SQLiteStateBackend)
            backend.close()
        finally:
            if os.path.exists(path):
                os.unlink(path)
    
    def test_create_unknown(self):
        """Test error for unknown backend type."""
        with pytest.raises(ValueError, match="Unknown backend type"):
            create_state_backend("unknown")
    
    def test_case_insensitive(self):
        """Test backend type is case insensitive."""
        backend = create_state_backend("MEMORY")
        
        assert isinstance(backend, MemoryStateBackend)
        backend.close()


class TestIntegration:
    """Integration tests for state backends."""
    
    def test_resume_from_position(self):
        """Test simulating CDC resume from saved position."""
        backend = MemoryStateBackend()
        
        # Simulate processing changes
        backend.save_position(
            table="orders",
            adapter="shopify",
            offset=0,
            last_key="order_001",
        )
        
        # Simulate more processing
        backend.save_position(
            table="orders",
            adapter="shopify",
            offset=50,
            last_key="order_051",
        )
        
        # Simulate crash and restart
        backend2 = MemoryStateBackend()
        backend2._positions = backend._positions.copy()  # Simulate persistence
        
        pos = backend2.get_position("orders", "shopify")
        
        assert pos.offset == 50
        assert pos.last_key == "order_051"
    
    def test_multiple_streams(self):
        """Test tracking multiple streams simultaneously."""
        backend = MemoryStateBackend()
        
        streams = [
            ("incident", "servicenow", 100),
            ("users", "salesforce", 200),
            ("issues", "jira", 300),
            ("orders", "shopify", 400),
        ]
        
        for table, adapter, offset in streams:
            backend.save_position(table=table, adapter=adapter, offset=offset)
        
        # Verify all were saved
        all_positions = backend.list_positions()
        assert len(all_positions) == 4
        
        # Verify each can be retrieved
        for table, adapter, expected_offset in streams:
            pos = backend.get_position(table, adapter)
            assert pos.offset == expected_offset
