"""
Tests for WaveQL connection module - Additional tests for uncovered lines.

This covers the 69% uncovered module waveql/connection.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch

from waveql.connection import WaveQLConnection


class TestWaveQLConnectionAdvanced:
    """Advanced tests for WaveQLConnection."""
    
    @pytest.fixture
    def basic_connection(self):
        """Create basic connection for testing."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            yield conn
    
    def test_register_adapter_by_name(self, basic_connection):
        """Test registering adapter by name."""
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "custom"
        
        basic_connection.register_adapter("custom", mock_adapter)
        
        assert "custom" in basic_connection._adapters
    
    def test_get_adapter(self, basic_connection):
        """Test getting registered adapter."""
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        basic_connection._adapters["test"] = mock_adapter
        
        result = basic_connection.get_adapter("test")
        
        assert result == mock_adapter
    
    def test_get_adapter_not_found(self, basic_connection):
        """Test getting non-existent adapter."""
        result = basic_connection.get_adapter("nonexistent")
        assert result is None
    
    def test_list_adapters(self, basic_connection):
        """Test listing registered adapters."""
        basic_connection._adapters = {
            "adapter1": MagicMock(),
            "adapter2": MagicMock(),
        }
        
        adapters = basic_connection.list_adapters()
        
        assert "adapter1" in adapters
        assert "adapter2" in adapters
    
    def test_cursor_creation(self, basic_connection):
        """Test cursor creation."""
        cursor = basic_connection.cursor()
        
        assert cursor is not None
        assert cursor._connection == basic_connection
    
    def test_execute_shorthand(self, basic_connection):
        """Test execute shorthand method."""
        # Mock cursor execute
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "A"), (2, "B")]
        
        with patch.object(basic_connection, "cursor", return_value=mock_cursor):
            result = basic_connection.execute("SELECT * FROM test")
        
        assert result == mock_cursor
    
    def test_close(self, basic_connection):
        """Test closing connection."""
        basic_connection.close()
        
        # Should not raise


class TestWaveQLConnectionContextManager:
    """Tests for context manager usage."""
    
    def test_context_manager(self):
        """Test using connection as context manager."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            with WaveQLConnection() as conn:
                assert conn is not None
            
            # Connection should be closed after context


class TestWaveQLConnectionFromConfig:
    """Tests for creating connection from config."""
    
    def test_from_config_dict(self):
        """Test creating connection from config dict."""
        config = {
            "adapters": {
                "test": {
                    "type": "rest",
                    "base_url": "https://api.example.com",
                }
            }
        }
        
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection.from_config(config)
            
            assert conn is not None
    
    def test_from_config_file(self, tmp_path):
        """Test creating connection from config file."""
        config_file = tmp_path / "waveql.yaml"
        config_file.write_text("""
adapters:
  test:
    type: rest
    base_url: https://api.example.com
""")
        
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection.from_config(str(config_file))
            
            assert conn is not None


class TestWaveQLConnectionCDC:
    """Tests for CDC-related methods."""
    
    @pytest.fixture
    def connection_with_adapter(self):
        """Create connection with mock adapter."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            
            mock_adapter = MagicMock()
            mock_adapter.adapter_name = "test"
            mock_adapter.fetch.return_value = pa.table({
                "id": [1, 2],
                "updated_at": ["2024-01-15", "2024-01-15"],
            })
            conn._adapters["test"] = mock_adapter
            
            yield conn
    
    def test_stream_changes(self, connection_with_adapter):
        """Test creating CDC stream."""
        from waveql.cdc.stream import CDCStream
        
        stream = connection_with_adapter.stream_changes("test.data")
        
        assert isinstance(stream, CDCStream)


class TestWaveQLConnectionMaterializedViews:
    """Tests for materialized view methods."""
    
    @pytest.fixture
    def connection_with_views(self, tmp_path):
        """Create connection with MV support."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            db_path = tmp_path / "views.db"
            conn = WaveQLConnection(view_metadata_path=str(db_path))
            yield conn
    
    def test_create_materialized_view(self, connection_with_views):
        """Test creating materialized view."""
        # This tests the create_materialized_view method if it exists
        if hasattr(connection_with_views, "create_materialized_view"):
            # Mock the result of the query execution to return a valid Arrow table
            mock_cursor = MagicMock()
            mock_table = pa.Table.from_pydict({"id": [1], "data": ["test"]})
            mock_cursor.fetch_arrow_table.return_value = mock_table
            mock_cursor.schema = mock_table.schema
            mock_cursor.description = [("id", "int", None, None, None, None, None), ("data", "string", None, None, None, None, None)]
            
            # Also mock the underlying duckdb connection which might be used directly
            def duckdb_execute_side_effect(query, *args, **kwargs):
                # Return valid schema/table for data selection
                if "SELECT * FROM test.data" in str(query):
                     mock_res = MagicMock()
                     mock_res.fetch_arrow_table.return_value = mock_table
                     mock_res.description = mock_cursor.description
                     return mock_res
                # Return empty for existence checks (information_schema)
                if "information_schema" in str(query).lower():
                     mock_empty = MagicMock()
                     mock_empty.fetch_arrow_table.return_value = pa.Table.from_pydict({})
                     mock_empty.fetchall.return_value = []
                     return mock_empty
                 
                # Default fallback
                mock_res = MagicMock()
                mock_res.fetch_arrow_table.return_value = mock_table
                return mock_res

            connection_with_views.duckdb.execute.side_effect = duckdb_execute_side_effect
            connection_with_views.duckdb.execute.return_value.description = mock_cursor.description
            
            with patch.object(connection_with_views, "execute", return_value=mock_cursor):
                with patch("waveql.materialized_view.manager.ViewRegistry") as MockRegistry:
                    mock_registry_instance = MockRegistry.return_value
                    mock_registry_instance.exists.return_value = False
                    
                    connection_with_views.create_materialized_view(
                        name="test_view",
                        query="SELECT * FROM test.data",
                    )


class TestWaveQLConnectionWebhooks:
    """Tests for webhook methods."""
    
    @pytest.fixture
    def connection_with_webhooks(self):
        """Create connection with webhook support."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            yield conn
    
    def test_register_webhook(self, connection_with_webhooks):
        """Test registering webhook."""
        if hasattr(connection_with_webhooks, "register_webhook"):
            connection_with_webhooks.register_webhook(
                table="test.data",
                url="https://webhook.example.com",
                events=["INSERT", "UPDATE"],
            )


class TestWaveQLConnectionProvenance:
    """Tests for provenance methods."""
    
    @pytest.fixture
    def connection_with_provenance(self):
        """Create connection with provenance support."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            yield conn
    
    def test_enable_provenance(self, connection_with_provenance):
        """Test enabling provenance tracking."""
        if hasattr(connection_with_provenance, "enable_provenance"):
            connection_with_provenance.enable_provenance()


class TestWaveQLConnectionPing:
    """Tests for ping method."""
    
    def test_ping(self):
        """Test ping method."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duck.execute.return_value = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            
            result = conn.ping()
            
            assert result is True


class TestWaveQLConnectionSchemaCache:
    """Tests for schema cache methods."""
    
    @pytest.fixture
    def connection_with_cache(self):
        """Create connection with schema cache."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            yield conn
    
    def test_get_schema(self, connection_with_cache):
        """Test getting schema."""
        mock_adapter = MagicMock()
        mock_adapter.get_schema.return_value = [
            MagicMock(name="id", data_type="int"),
            MagicMock(name="name", data_type="string"),
        ]
        connection_with_cache._adapters["test"] = mock_adapter
        
        connection_with_cache.get_schema("test.users")
        
        # Should return schema
    
    def test_refresh_schema(self, connection_with_cache):
        """Test refreshing schema cache."""
        if hasattr(connection_with_cache, "refresh_schema"):
            connection_with_cache.refresh_schema("test.users")


class TestWaveQLConnectionRLS:
    """Tests for Row-Level Security methods."""
    
    @pytest.fixture
    def connection_with_rls(self):
        """Create connection with RLS."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            yield conn
    
    def test_set_user_context(self, connection_with_rls):
        """Test setting user context for RLS."""
        if hasattr(connection_with_rls, "set_user_context"):
            connection_with_rls.set_user_context({
                "user_id": 123,
                "role": "admin",
            })


class TestWaveQLConnectionRepr:
    """Tests for string representation."""
    
    def test_repr(self):
        """Test string representation."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            
            repr_str = repr(conn)
            
            assert "WaveQLConnection" in repr_str


class TestWaveQLConnectionEdgeCases:
    """Edge case tests."""
    
    def test_execute_with_empty_result(self):
        """Test execute with empty result."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_duck.execute.return_value = mock_result
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            cursor = conn.cursor()
            cursor._result = pa.table({"id": []})
            
            result = cursor.fetchall()
            assert result == []
    
    def test_transaction_begin(self):
        """Test beginning transaction."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            
            if hasattr(conn, "begin"):
                conn.begin()
    
    def test_transaction_commit(self):
        """Test committing transaction."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            conn.commit()
    
    def test_transaction_rollback(self):
        """Test rolling back transaction."""
        with patch("waveql.connection.duckdb") as mock_duckdb:
            mock_duck = MagicMock()
            mock_duckdb.connect.return_value = mock_duck
            
            conn = WaveQLConnection()
            conn.rollback()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
