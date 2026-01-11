"""
Tests for Schema Cache - SQLite-based metadata caching

Tests cover:
- Schema storage and retrieval
- TTL-based expiration
- Cache invalidation (single table and adapter-wide)
- Thread safety
- SHOW TABLES / DESCRIBE support
- ColumnInfo and TableSchema dataclasses
"""

import pytest
import time
import threading
from pathlib import Path

from waveql.schema_cache import (
    SchemaCache,
    TableSchema,
    ColumnInfo,
)


class TestColumnInfo:
    """Tests for ColumnInfo dataclass."""
    
    def test_column_info_creation(self):
        """Test creating a ColumnInfo."""
        col = ColumnInfo(
            name="id",
            data_type="integer",
            nullable=False,
            primary_key=True,
            description="Primary key",
        )
        
        assert col.name == "id"
        assert col.data_type == "integer"
        assert col.nullable is False
        assert col.primary_key is True
        assert col.description == "Primary key"
    
    def test_column_info_defaults(self):
        """Test ColumnInfo default values."""
        col = ColumnInfo(name="name", data_type="string")
        
        assert col.nullable is True
        assert col.primary_key is False
        assert col.description == ""
        assert col.arrow_type is None


class TestTableSchema:
    """Tests for TableSchema dataclass."""
    
    def test_table_schema_creation(self):
        """Test creating a TableSchema."""
        columns = [
            ColumnInfo(name="id", data_type="integer", primary_key=True),
            ColumnInfo(name="name", data_type="string"),
        ]
        
        schema = TableSchema(
            name="users",
            columns=columns,
            adapter="servicenow",
            discovered_at=time.time(),
            ttl=3600,
        )
        
        assert schema.name == "users"
        assert len(schema.columns) == 2
        assert schema.adapter == "servicenow"
        assert schema.ttl == 3600
    
    def test_is_expired_false(self):
        """Test that fresh schema is not expired."""
        schema = TableSchema(
            name="test",
            columns=[],
            adapter="test",
            discovered_at=time.time(),
            ttl=3600,
        )
        
        assert schema.is_expired() is False
    
    def test_is_expired_true(self):
        """Test that stale schema is expired."""
        schema = TableSchema(
            name="test",
            columns=[],
            adapter="test",
            discovered_at=time.time() - 7200,  # 2 hours ago
            ttl=3600,  # 1 hour TTL
        )
        
        assert schema.is_expired() is True
    
    def test_to_dict(self):
        """Test serialization to dict."""
        columns = [
            ColumnInfo(name="id", data_type="integer"),
        ]
        schema = TableSchema(
            name="users",
            columns=columns,
            adapter="servicenow",
            discovered_at=1234567890.0,
            ttl=1800,
        )
        
        data = schema.to_dict()
        
        assert data["name"] == "users"
        assert data["adapter"] == "servicenow"
        assert data["discovered_at"] == 1234567890.0
        assert data["ttl"] == 1800
        assert len(data["columns"]) == 1
        assert data["columns"][0]["name"] == "id"
    
    def test_from_dict(self):
        """Test deserialization from dict."""
        data = {
            "name": "users",
            "columns": [
                {"name": "id", "data_type": "integer", "nullable": False, "primary_key": True, "description": ""},
                {"name": "name", "data_type": "string", "nullable": True, "primary_key": False, "description": ""},
            ],
            "adapter": "servicenow",
            "discovered_at": 1234567890.0,
            "ttl": 1800,
        }
        
        schema = TableSchema.from_dict(data)
        
        assert schema.name == "users"
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "id"
        assert schema.columns[0].primary_key is True


class TestSchemaCacheInMemory:
    """Tests for in-memory SchemaCache."""
    
    def test_create_in_memory_cache(self):
        """Test creating in-memory cache."""
        cache = SchemaCache()
        
        assert cache is not None
        # In-memory mode - no _db_path attribute
        assert not hasattr(cache, '_db_path') or cache._db_path is None or str(cache._db_path) == ":memory:"
    
    def test_set_and_get(self):
        """Test basic set and get operations."""
        cache = SchemaCache()
        
        columns = [
            ColumnInfo(name="id", data_type="integer", primary_key=True),
            ColumnInfo(name="name", data_type="string"),
        ]
        
        cache.set("servicenow", "incident", columns, ttl=3600)
        
        schema = cache.get("servicenow", "incident")
        
        assert schema is not None
        assert schema.name == "incident"
        assert len(schema.columns) == 2
    
    def test_get_missing_returns_none(self):
        """Test that getting non-existent schema returns None."""
        cache = SchemaCache()
        
        schema = cache.get("servicenow", "nonexistent")
        
        assert schema is None
    
    def test_get_expired_returns_none(self):
        """Test that expired schemas return None and are invalidated."""
        cache = SchemaCache()
        
        columns = [ColumnInfo(name="id", data_type="integer")]
        
        # Set with very short TTL
        cache.set("servicenow", "incident", columns, ttl=0)
        
        # Wait for expiration
        time.sleep(0.1)
        
        schema = cache.get("servicenow", "incident")
        
        assert schema is None
    
    def test_set_overwrites_existing(self):
        """Test that set overwrites existing schema."""
        cache = SchemaCache()
        
        columns1 = [ColumnInfo(name="id", data_type="integer")]
        columns2 = [ColumnInfo(name="id", data_type="integer"), ColumnInfo(name="name", data_type="string")]
        
        cache.set("servicenow", "incident", columns1)
        cache.set("servicenow", "incident", columns2)
        
        schema = cache.get("servicenow", "incident")
        
        assert len(schema.columns) == 2


class TestCacheInvalidation:
    """Tests for cache invalidation."""
    
    def test_invalidate_single_table(self):
        """Test invalidating a single table."""
        cache = SchemaCache()
        
        columns = [ColumnInfo(name="id", data_type="integer")]
        cache.set("servicenow", "incident", columns)
        cache.set("servicenow", "problem", columns)
        
        cache.invalidate("servicenow", "incident")
        
        assert cache.get("servicenow", "incident") is None
        assert cache.get("servicenow", "problem") is not None
    
    def test_invalidate_all_for_adapter(self):
        """Test invalidating all tables for an adapter."""
        cache = SchemaCache()
        
        columns = [ColumnInfo(name="id", data_type="integer")]
        cache.set("servicenow", "incident", columns)
        cache.set("servicenow", "problem", columns)
        cache.set("salesforce", "account", columns)
        
        cache.invalidate("servicenow")
        
        assert cache.get("servicenow", "incident") is None
        assert cache.get("servicenow", "problem") is None
        assert cache.get("salesforce", "account") is not None


class TestListTables:
    """Tests for listing cached tables."""
    
    def test_list_all_tables(self):
        """Test listing all cached tables."""
        cache = SchemaCache()
        
        columns = [ColumnInfo(name="id", data_type="integer")]
        cache.set("servicenow", "incident", columns)
        cache.set("servicenow", "problem", columns)
        cache.set("salesforce", "account", columns)
        
        tables = cache.list_tables()
        
        assert len(tables) == 3
        assert "incident" in tables
        assert "problem" in tables
        assert "account" in tables
    
    def test_list_tables_by_adapter(self):
        """Test listing tables for a specific adapter."""
        cache = SchemaCache()
        
        columns = [ColumnInfo(name="id", data_type="integer")]
        cache.set("servicenow", "incident", columns)
        cache.set("servicenow", "problem", columns)
        cache.set("salesforce", "account", columns)
        
        tables = cache.list_tables(adapter="servicenow")
        
        assert len(tables) == 2
        assert "incident" in tables
        assert "problem" in tables
        assert "account" not in tables
    
    def test_list_tables_empty(self):
        """Test listing tables when cache is empty."""
        cache = SchemaCache()
        
        tables = cache.list_tables()
        
        assert tables == []


class TestDescribeTable:
    """Tests for DESCRIBE table functionality."""
    
    def test_describe_table(self):
        """Test getting table description."""
        cache = SchemaCache()
        
        columns = [
            ColumnInfo(name="id", data_type="integer", nullable=False, primary_key=True),
            ColumnInfo(name="name", data_type="string", nullable=True, description="User name"),
        ]
        cache.set("servicenow", "users", columns)
        
        description = cache.describe_table("servicenow", "users")
        
        assert len(description) == 2
        assert description[0]["Field"] == "id"
        assert description[0]["Type"] == "integer"
        assert description[0]["Null"] == "NO"
        assert description[0]["Key"] == "PRI"
        assert description[1]["Field"] == "name"
        assert description[1]["Null"] == "YES"
    
    def test_describe_nonexistent_table(self):
        """Test describing non-existent table returns None."""
        cache = SchemaCache()
        
        description = cache.describe_table("servicenow", "nonexistent")
        
        assert description is None


class TestSchemaCachePersistence:
    """Tests for persistent SchemaCache (file-based)."""
    
    def test_create_file_cache(self, tmp_path):
        """Test creating file-based cache."""
        cache_path = tmp_path / "schema_cache.db"
        
        cache = SchemaCache(str(cache_path))
        
        assert cache_path.exists()
        
        cache.close()
    
    def test_data_persists(self, tmp_path):
        """Test that data persists across cache instances."""
        cache_path = tmp_path / "schema_cache.db"
        
        # Create and populate cache
        cache1 = SchemaCache(str(cache_path))
        columns = [ColumnInfo(name="id", data_type="integer")]
        cache1.set("servicenow", "incident", columns, ttl=3600)
        cache1.close()
        
        # Reopen and verify
        cache2 = SchemaCache(str(cache_path))
        schema = cache2.get("servicenow", "incident")
        
        assert schema is not None
        assert schema.name == "incident"
        
        cache2.close()


class TestThreadSafety:
    """Tests for thread-safe operations."""
    
    def test_concurrent_writes(self):
        """Test concurrent writes don't corrupt cache."""
        cache = SchemaCache()
        errors = []
        
        def write_thread(adapter_name: str):
            try:
                for i in range(50):
                    columns = [ColumnInfo(name=f"col{i}", data_type="string")]
                    cache.set(adapter_name, f"table{i}", columns)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=write_thread, args=(f"adapter{i}",))
            for i in range(5)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
    
    def test_concurrent_reads_and_writes(self):
        """Test concurrent reads and writes don't cause issues."""
        cache = SchemaCache()
        errors = []
        
        # Prepopulate
        columns = [ColumnInfo(name="id", data_type="integer")]
        for i in range(10):
            cache.set("servicenow", f"table{i}", columns)
        
        def read_thread():
            try:
                for _ in range(100):
                    for i in range(10):
                        cache.get("servicenow", f"table{i}")
            except Exception as e:
                errors.append(e)
        
        def write_thread():
            try:
                for i in range(100):
                    columns = [ColumnInfo(name=f"col{i}", data_type="string")]
                    cache.set("salesforce", f"account{i}", columns)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=read_thread),
            threading.Thread(target=read_thread),
            threading.Thread(target=write_thread),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0


class TestSchemaCacheRepr:
    """Tests for string representation."""
    
    def test_repr_in_memory(self):
        """Test repr for in-memory cache."""
        cache = SchemaCache()
        
        repr_str = repr(cache)
        
        assert "SchemaCache" in repr_str
    
    def test_repr_file_based(self, tmp_path):
        """Test repr for file-based cache."""
        cache_path = tmp_path / "test.db"
        cache = SchemaCache(str(cache_path))
        
        repr_str = repr(cache)
        
        assert "SchemaCache" in repr_str
        assert "test.db" in repr_str
        
        cache.close()
