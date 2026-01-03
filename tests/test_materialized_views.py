"""
Tests for Materialized Views functionality
"""

import pytest
from pathlib import Path
import tempfile
import shutil

import pyarrow as pa


class TestMaterializedViewModels:
    """Test data models."""
    
    def test_view_definition_serialization(self):
        from waveql.materialized_view.models import ViewDefinition, RefreshStrategy, ColumnInfo
        
        view = ViewDefinition(
            name="test_view",
            query="SELECT * FROM incident",
            source_adapter="servicenow",
            source_table="incident",
            refresh_strategy=RefreshStrategy.INCREMENTAL,
            sync_column="sys_updated_on",
            columns=[ColumnInfo(name="id", data_type="string")],
        )
        
        # Serialize and deserialize
        data = view.to_dict()
        restored = ViewDefinition.from_dict(data)
        
        assert restored.name == view.name
        assert restored.query == view.query
        assert restored.refresh_strategy == RefreshStrategy.INCREMENTAL
        assert restored.sync_column == "sys_updated_on"
        assert len(restored.columns) == 1
    
    def test_view_stats_serialization(self):
        from waveql.materialized_view.models import ViewStats
        from datetime import datetime
        
        stats = ViewStats(
            row_count=1000,
            size_bytes=1024 * 1024,
            last_refresh=datetime.now(),
            refresh_duration_ms=500.5,
        )
        
        data = stats.to_dict()
        restored = ViewStats.from_dict(data)
        
        assert restored.row_count == 1000
        assert restored.size_bytes == 1024 * 1024
        assert restored.refresh_duration_ms == 500.5


class TestViewRegistry:
    """Test the SQLite registry."""
    
    @pytest.fixture
    def temp_registry(self, tmp_path):
        from waveql.materialized_view.registry import ViewRegistry
        registry = ViewRegistry(tmp_path / "registry.db")
        yield registry
        registry.close()
    
    def test_register_and_get(self, temp_registry):
        from waveql.materialized_view.models import ViewDefinition
        
        view = ViewDefinition(
            name="test_view",
            query="SELECT * FROM test_table",
        )
        
        temp_registry.register(view)
        
        # Retrieve
        info = temp_registry.get("test_view")
        assert info is not None
        assert info.definition.name == "test_view"
        assert info.definition.query == "SELECT * FROM test_table"
    
    def test_exists(self, temp_registry):
        from waveql.materialized_view.models import ViewDefinition
        
        assert not temp_registry.exists("nonexistent")
        
        view = ViewDefinition(name="my_view", query="SELECT 1")
        temp_registry.register(view)
        
        assert temp_registry.exists("my_view")
    
    def test_list_all(self, temp_registry):
        from waveql.materialized_view.models import ViewDefinition
        
        # Empty list initially
        assert len(temp_registry.list_all()) == 0
        
        # Add views
        for i in range(3):
            view = ViewDefinition(name=f"view_{i}", query=f"SELECT {i}")
            temp_registry.register(view)
        
        views = temp_registry.list_all()
        assert len(views) == 3
    
    def test_delete(self, temp_registry):
        from waveql.materialized_view.models import ViewDefinition
        
        view = ViewDefinition(name="to_delete", query="SELECT 1")
        temp_registry.register(view)
        
        assert temp_registry.exists("to_delete")
        
        deleted = temp_registry.delete("to_delete")
        assert deleted is True
        assert not temp_registry.exists("to_delete")
        
        # Deleting again returns False
        deleted = temp_registry.delete("to_delete")
        assert deleted is False


class TestViewStorage:
    """Test Parquet storage."""
    
    @pytest.fixture
    def temp_storage(self, tmp_path):
        from waveql.materialized_view.storage import ViewStorage
        storage = ViewStorage(tmp_path / "views")
        yield storage
    
    def test_write_and_read(self, temp_storage):
        # Create test data
        data = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        
        # Write
        stats = temp_storage.write("test_view", data)
        assert stats.row_count == 3
        assert stats.size_bytes > 0
        
        # Read
        result = temp_storage.read("test_view")
        assert result is not None
        assert len(result) == 3
        assert result.column_names == ["id", "name"]
    
    def test_append(self, temp_storage):
        # Initial data
        data1 = pa.table({"id": [1, 2]})
        temp_storage.write("append_test", data1)
        
        # Append
        data2 = pa.table({"id": [3, 4]})
        stats = temp_storage.append("append_test", data2)
        
        assert stats.row_count == 4
        
        result = temp_storage.read("append_test")
        assert len(result) == 4
    
    def test_exists(self, temp_storage):
        assert not temp_storage.exists("nonexistent")
        
        data = pa.table({"x": [1]})
        temp_storage.write("exists_test", data)
        
        assert temp_storage.exists("exists_test")
    
    def test_delete(self, temp_storage):
        data = pa.table({"x": [1]})
        temp_storage.write("delete_test", data)
        
        assert temp_storage.exists("delete_test")
        
        deleted = temp_storage.delete("delete_test")
        assert deleted is True
        assert not temp_storage.exists("delete_test")
    
    def test_get_stats(self, temp_storage):
        data = pa.table({
            "id": list(range(100)),
            "value": [f"value_{i}" for i in range(100)],
        })
        temp_storage.write("stats_test", data)
        
        stats = temp_storage.get_stats("stats_test")
        assert stats.row_count == 100
        assert stats.size_bytes > 0


class TestMaterializedViewManager:
    """Test the main manager."""
    
    @pytest.fixture
    def temp_conn(self, tmp_path):
        """Create a connection with temporary storage."""
        import waveql
        
        conn = waveql.connect()
        
        # Override storage paths
        from waveql.materialized_view.manager import MaterializedViewManager
        from waveql.materialized_view.registry import ViewRegistry
        from waveql.materialized_view.storage import ViewStorage
        
        manager = MaterializedViewManager(
            connection=conn,
            storage_path=tmp_path / "views",
            registry_path=tmp_path / "registry.db",
        )
        conn._view_manager = manager
        
        yield conn
        conn.close()
    
    def test_create_simple_view(self, temp_conn):
        # Create a simple view from DuckDB data
        temp_conn.duckdb.execute("""
            CREATE TABLE test_data AS 
            SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)
        """)
        
        temp_conn.create_materialized_view(
            name="my_view",
            query="SELECT * FROM test_data"
        )
        
        # Verify it exists
        views = temp_conn.list_materialized_views()
        assert len(views) == 1
        assert views[0]["name"] == "my_view"
        assert views[0]["row_count"] == 3
    
    def test_query_materialized_view(self, temp_conn):
        # Setup
        temp_conn.duckdb.execute("""
            CREATE TABLE source AS 
            SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(id, value)
        """)
        
        temp_conn.create_materialized_view(
            name="cached_data",
            query="SELECT * FROM source"
        )
        
        # Query the view
        cursor = temp_conn.cursor()
        cursor.execute("SELECT * FROM cached_data WHERE value > 150")
        results = cursor.fetchall()
        
        # Should return rows with value > 150
        assert len(results) == 2
    
    def test_refresh_view(self, temp_conn):
        # Setup
        temp_conn.duckdb.execute("""
            CREATE TABLE refresh_test AS 
            SELECT * FROM (VALUES (1, 'initial')) AS t(id, data)
        """)
        
        temp_conn.create_materialized_view(
            name="refresh_view",
            query="SELECT * FROM refresh_test"
        )
        
        # Add more data to source
        temp_conn.duckdb.execute("""
            INSERT INTO refresh_test VALUES (2, 'added')
        """)
        
        # Refresh
        stats = temp_conn.refresh_materialized_view("refresh_view")
        assert stats["row_count"] == 2
        
        # Verify new data is accessible
        cursor = temp_conn.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM refresh_view")
        result = cursor.fetchone()
        assert result[0] == 2
    
    def test_drop_view(self, temp_conn):
        # Setup
        temp_conn.duckdb.execute("CREATE TABLE drop_source AS SELECT 1 as x")
        temp_conn.create_materialized_view(
            name="to_drop",
            query="SELECT * FROM drop_source"
        )
        
        assert len(temp_conn.list_materialized_views()) == 1
        
        # Drop
        result = temp_conn.drop_materialized_view("to_drop")
        assert result is True
        assert len(temp_conn.list_materialized_views()) == 0
    
    def test_drop_nonexistent_with_if_exists(self, temp_conn):
        # Should not raise
        result = temp_conn.drop_materialized_view("nonexistent", if_exists=True)
        assert result is False
    
    def test_create_if_not_exists(self, temp_conn):
        temp_conn.duckdb.execute("CREATE TABLE ine_source AS SELECT 1 as x")
        
        # First create
        temp_conn.create_materialized_view(
            name="ine_view",
            query="SELECT * FROM ine_source"
        )
        
        # Second create with if_not_exists should not raise
        temp_conn.create_materialized_view(
            name="ine_view",
            query="SELECT * FROM ine_source",
            if_not_exists=True
        )
        
        assert len(temp_conn.list_materialized_views()) == 1
    
    def test_get_view_info(self, temp_conn):
        temp_conn.duckdb.execute("CREATE TABLE info_source AS SELECT 1 as id, 'test' as name")
        temp_conn.create_materialized_view(
            name="info_view",
            query="SELECT * FROM info_source"
        )
        
        info = temp_conn.get_materialized_view("info_view")
        assert info is not None
        assert info["name"] == "info_view"
        assert info["row_count"] == 1
        assert "size_mb" in info


class TestIncrementalSync:
    """Test incremental sync functionality."""
    
    def test_get_default_sync_column(self):
        from waveql.materialized_view.sync import get_default_sync_column
        
        assert get_default_sync_column("servicenow", "incident") == "sys_updated_on"
        assert get_default_sync_column("salesforce", "Account") == "LastModifiedDate"
        assert get_default_sync_column("jira", "issues") == "updated"
        assert get_default_sync_column("unknown", "table") is None


# Run tests with: python -m pytest tests/test_materialized_views.py -v
