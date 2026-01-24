"""
Tests for WaveQL pg_wire/catalog module.

This covers the 48% uncovered module waveql/pg_wire/catalog.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch, PropertyMock

from waveql.pg_wire.catalog import PGCatalogEmulator


class TestPGCatalogEmulatorInit:
    """Tests for PGCatalogEmulator initialization."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock WaveQL connection."""
        conn = MagicMock()
        
        # Mock adapters
        mock_adapter1 = MagicMock()
        mock_adapter1.adapter_name = "servicenow"
        mock_adapter1.list_tables.return_value = ["incident", "sys_user", "change_request"]
        mock_adapter1.get_schema.return_value = [
            MagicMock(name="sys_id", data_type="string"),
            MagicMock(name="number", data_type="string"),
            MagicMock(name="short_description", data_type="string"),
        ]
        
        mock_adapter2 = MagicMock()
        mock_adapter2.adapter_name = "salesforce"
        mock_adapter2.list_tables.return_value = ["Account", "Contact", "Opportunity"]
        mock_adapter2.get_schema.return_value = [
            MagicMock(name="Id", data_type="string"),
            MagicMock(name="Name", data_type="string"),
        ]
        
        conn._adapters = {
            "servicenow": mock_adapter1,
            "salesforce": mock_adapter2,
        }
        
        return conn
    
    def test_init(self, mock_connection):
        """Test catalog emulator initialization."""
        catalog = PGCatalogEmulator(mock_connection)
        
        assert catalog._connection == mock_connection
    
    def test_get_schema_oid(self, mock_connection):
        """Test schema OID assignment."""
        catalog = PGCatalogEmulator(mock_connection)
        
        oid1 = catalog._get_schema_oid("servicenow")
        oid2 = catalog._get_schema_oid("salesforce")
        oid3 = catalog._get_schema_oid("servicenow")  # Same as oid1
        
        assert isinstance(oid1, int)
        assert isinstance(oid2, int)
        assert oid1 == oid3  # Same schema gets same OID
        assert oid1 != oid2  # Different schemas get different OIDs
    
    def test_get_table_oid(self, mock_connection):
        """Test table OID assignment."""
        catalog = PGCatalogEmulator(mock_connection)
        
        oid1 = catalog._get_table_oid("servicenow", "incident")
        oid2 = catalog._get_table_oid("servicenow", "sys_user")
        oid3 = catalog._get_table_oid("servicenow", "incident")
        
        assert isinstance(oid1, int)
        assert oid1 == oid3  # Same table gets same OID
        assert oid1 != oid2  # Different tables get different OIDs


class TestPGCatalogEmulatorHandleQuery:
    """Tests for handle_catalog_query method."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock connection with adapters."""
        conn = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.list_tables.return_value = ["table1", "table2"]
        mock_adapter.get_schema.return_value = [
            MagicMock(name="id", data_type="int64"),
            MagicMock(name="name", data_type="string"),
        ]
        conn._adapters = {"test": mock_adapter}
        return conn
    
    def test_handle_pg_namespace(self, mock_connection):
        """Test handling pg_namespace query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_namespace")
        
        assert isinstance(result, pa.Table)
        assert "nspname" in result.column_names
    
    def test_handle_pg_class(self, mock_connection):
        """Test handling pg_class query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_class")
        
        assert isinstance(result, pa.Table)
        assert "relname" in result.column_names
    
    def test_handle_pg_attribute(self, mock_connection):
        """Test handling pg_attribute query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_attribute")
        
        assert isinstance(result, pa.Table)
        assert "attname" in result.column_names
    
    def test_handle_pg_type(self, mock_connection):
        """Test handling pg_type query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_type")
        
        assert isinstance(result, pa.Table)
        assert "typname" in result.column_names
    
    def test_handle_pg_database(self, mock_connection):
        """Test handling pg_database query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_database")
        
        assert isinstance(result, pa.Table)
        assert "datname" in result.column_names
    
    def test_handle_pg_tables(self, mock_connection):
        """Test handling pg_tables view."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_tables")
        
        assert isinstance(result, pa.Table)
        assert "tablename" in result.column_names
    
    def test_handle_pg_views(self, mock_connection):
        """Test handling pg_views query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_views")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_indexes(self, mock_connection):
        """Test handling pg_indexes query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_indexes")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_constraint(self, mock_connection):
        """Test handling pg_constraint query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_constraint")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_description(self, mock_connection):
        """Test handling pg_description query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_description")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_settings(self, mock_connection):
        """Test handling pg_settings query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_settings")
        
        assert isinstance(result, pa.Table)
        assert "name" in result.column_names
    
    def test_handle_pg_stat_user_tables(self, mock_connection):
        """Test handling pg_stat_user_tables query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_stat_user_tables")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_proc(self, mock_connection):
        """Test handling pg_proc query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_proc")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_am(self, mock_connection):
        """Test handling pg_am query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_am")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_pg_extension(self, mock_connection):
        """Test handling pg_extension query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_extension")
        
        assert isinstance(result, pa.Table)
    
    def test_handle_unknown_table(self, mock_connection):
        """Test handling unknown catalog table."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query("pg_unknown")
        
        # Should return None for unknown tables
        assert result is None
    
    def test_handle_with_columns(self, mock_connection):
        """Test handling query with specific columns."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.handle_catalog_query(
            "pg_namespace",
            columns=["nspname", "oid"],
        )
        
        assert isinstance(result, pa.Table)


class TestPGCatalogEmulatorHelpers:
    """Tests for helper methods."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock connection."""
        conn = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.list_tables.return_value = ["users", "orders"]
        mock_adapter.get_schema.return_value = [
            MagicMock(name="id", data_type="int64"),
        ]
        conn._adapters = {"test": mock_adapter}
        conn._schema_cache = None  # Disable schema cache to force adapter usage
        return conn
    
    def test_get_adapters(self, mock_connection):
        """Test getting adapters."""
        catalog = PGCatalogEmulator(mock_connection)
        
        adapters = catalog._get_adapters()
        
        assert "test" in adapters
    
    def test_get_adapter_tables(self, mock_connection):
        """Test getting tables from adapter."""
        catalog = PGCatalogEmulator(mock_connection)
        adapter = mock_connection._adapters["test"]
        
        tables = catalog._get_adapter_tables("test", adapter)
        
        assert "users" in tables
        assert "orders" in tables
    
    def test_get_adapter_tables_error(self, mock_connection):
        """Test handling adapter error when getting tables."""
        catalog = PGCatalogEmulator(mock_connection)
        
        adapter = MagicMock()
        adapter.list_tables.side_effect = Exception("Connection failed")
        
        tables = catalog._get_adapter_tables("broken", adapter)
        
        assert tables == []
    
    def test_get_table_columns(self, mock_connection):
        """Test getting columns from table."""
        catalog = PGCatalogEmulator(mock_connection)
        adapter = mock_connection._adapters["test"]
        
        columns = catalog._get_table_columns("test", "users", adapter)
        
        assert len(columns) >= 1
    
    def test_get_table_columns_error(self, mock_connection):
        """Test handling error when getting columns."""
        catalog = PGCatalogEmulator(mock_connection)
        
        adapter = MagicMock()
        adapter.get_schema.side_effect = Exception("Schema error")
        
        columns = catalog._get_table_columns("broken", "table", adapter)
        
        assert columns == []
    
    def test_map_column_type_to_oid_string(self, mock_connection):
        """Test mapping string column to OID."""
        catalog = PGCatalogEmulator(mock_connection)
        
        column = MagicMock()
        column.data_type = "string"
        column.arrow_type = None
        
        oid = catalog._map_column_type_to_oid(column)
        
        assert isinstance(oid, int)
        assert oid == 25  # TEXT OID
    
    def test_map_column_type_to_oid_int(self, mock_connection):
        """Test mapping integer column to OID."""
        catalog = PGCatalogEmulator(mock_connection)
        
        column = MagicMock()
        column.data_type = "int64"
        column.arrow_type = None
        
        oid = catalog._map_column_type_to_oid(column)
        
        assert isinstance(oid, int)
        assert oid == 20  # INT8 OID
    
    def test_map_column_type_to_oid_bool(self, mock_connection):
        """Test mapping boolean column to OID."""
        catalog = PGCatalogEmulator(mock_connection)
        
        column = MagicMock()
        column.data_type = "bool"
        column.arrow_type = None
        
        oid = catalog._map_column_type_to_oid(column)
        
        assert isinstance(oid, int)


class TestPGCatalogEmulatorIsCatalogQuery:
    """Tests for is_catalog_query method."""
    
    @pytest.fixture
    def mock_connection(self):
        conn = MagicMock()
        conn._adapters = {}
        return conn
    
    def test_is_catalog_query_pg_namespace(self, mock_connection):
        """Test detecting pg_namespace query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        assert catalog.is_catalog_query("SELECT * FROM pg_namespace")
        assert catalog.is_catalog_query("SELECT * FROM pg_catalog.pg_namespace")
    
    def test_is_catalog_query_pg_class(self, mock_connection):
        """Test detecting pg_class query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        assert catalog.is_catalog_query("SELECT * FROM pg_class")
    
    def test_is_catalog_query_information_schema(self, mock_connection):
        """Test detecting information_schema query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        result = catalog.is_catalog_query("SELECT * FROM information_schema.tables")
        # May or may not be considered catalog query
        assert isinstance(result, bool)
    
    def test_is_catalog_query_regular_table(self, mock_connection):
        """Test regular table query is not catalog query."""
        catalog = PGCatalogEmulator(mock_connection)
        
        assert not catalog.is_catalog_query("SELECT * FROM users")
        assert not catalog.is_catalog_query("SELECT * FROM servicenow.incident")


class TestPGCatalogEmulatorVersionInfo:
    """Tests for get_version_info method."""
    
    @pytest.fixture
    def mock_connection(self):
        conn = MagicMock()
        conn._adapters = {}
        return conn
    
    def test_get_version_info(self, mock_connection):
        """Test getting version info."""
        catalog = PGCatalogEmulator(mock_connection)
        
        version_info = catalog.get_version_info()
        
        assert isinstance(version_info, dict)
        assert "version" in version_info or "server_version" in version_info


class TestPGCatalogEmulatorIntegration:
    """Integration tests for PGCatalogEmulator."""
    
    @pytest.fixture
    def full_mock_connection(self):
        """Create fully mocked connection."""
        conn = MagicMock()
        
        # Create realistic adapter mock
        adapter = MagicMock()
        adapter.adapter_name = "servicenow"
        adapter.list_tables.return_value = ["incident", "sys_user"]
        
        from waveql.schema_cache import ColumnInfo
        adapter.get_schema.return_value = [
            ColumnInfo(name="sys_id", data_type="string", nullable=False),
            ColumnInfo(name="number", data_type="string", nullable=True),
            ColumnInfo(name="sys_created_on", data_type="timestamp", nullable=True),
        ]
        
        conn._adapters = {"servicenow": adapter}
        conn._schema_cache = None
        return conn
    
    def test_full_catalog_discovery(self, full_mock_connection):
        """Test full catalog discovery flow."""
        catalog = PGCatalogEmulator(full_mock_connection)
        
        # Get schemas
        namespaces = catalog.handle_catalog_query("pg_namespace")
        assert len(namespaces) >= 1
        
        # Get tables
        tables = catalog.handle_catalog_query("pg_class")
        assert len(tables) >= 1
        
        # Get columns
        attributes = catalog.handle_catalog_query("pg_attribute")
        assert len(attributes) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
