"""
Tests for PostgreSQL CDC Provider

These tests verify the PostgreSQL WAL-based CDC implementation.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from datetime import datetime

from waveql.cdc.postgres import PostgresCDCProvider
from waveql.cdc.models import Change, ChangeType


class TestPostgresCDCProviderInit:
    """Tests for PostgresCDCProvider initialization."""

    def test_init_defaults(self):
        """Test provider initialization with defaults."""
        adapter = Mock()
        adapter._connection_string = "postgresql://localhost/test"
        
        provider = PostgresCDCProvider(adapter)
        
        assert provider.adapter == adapter
        assert provider._slot_name == "waveql_cdc"
        assert provider._output_plugin == "wal2json"
        assert provider._create_slot is True
        assert provider._include_transaction is False

    def test_init_custom_options(self):
        """Test provider initialization with custom options."""
        adapter = Mock()
        
        provider = PostgresCDCProvider(
            adapter=adapter,
            connection_string="postgresql://custom/db",
            slot_name="my_slot",
            output_plugin="test_decoding",
            create_slot=False,
            include_transaction=True,
        )
        
        assert provider._connection_string == "postgresql://custom/db"
        assert provider._slot_name == "my_slot"
        assert provider._output_plugin == "test_decoding"
        assert provider._create_slot is False
        assert provider._include_transaction is True

    def test_repr(self):
        """Test string representation."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, slot_name="test_slot")
        
        assert "test_slot" in repr(provider)
        assert "wal2json" in repr(provider)


class TestPostgresCDCProviderConnectionString:
    """Tests for connection string resolution."""

    def test_explicit_connection_string(self):
        """Test explicit connection string parameter."""
        adapter = Mock()
        provider = PostgresCDCProvider(
            adapter=adapter,
            connection_string="postgresql://explicit/db"
        )
        
        assert provider._get_connection_string() == "postgresql://explicit/db"

    def test_connection_string_from_adapter(self):
        """Test extracting connection string from adapter."""
        adapter = Mock()
        adapter._connection_string = "postgresql://from_adapter/db"
        
        provider = PostgresCDCProvider(adapter=adapter)
        
        assert provider._get_connection_string() == "postgresql://from_adapter/db"

    def test_connection_string_from_adapter_host(self):
        """Test extracting connection string from adapter host."""
        adapter = Mock(spec=['_host'])
        adapter._host = "postgresql://from_host/db"
        
        provider = PostgresCDCProvider(adapter=adapter)
        
        assert provider._get_connection_string() == "postgresql://from_host/db"

    def test_connection_string_missing(self):
        """Test error when no connection string available."""
        adapter = Mock(spec=[])  # No _connection_string or _host
        provider = PostgresCDCProvider(adapter=adapter)
        
        with pytest.raises(ValueError, match="No connection string"):
            provider._get_connection_string()


class TestPostgresCDCProviderTableParsing:
    """Tests for table name parsing."""

    def test_parse_simple_table(self):
        """Test parsing simple table name."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        schema, table = provider._parse_table_name("users")
        
        assert schema is None
        assert table == "users"

    def test_parse_schema_qualified_table(self):
        """Test parsing schema.table name."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        schema, table = provider._parse_table_name("public.users")
        
        assert schema == "public"
        assert table == "users"

    def test_parse_quoted_table(self):
        """Test parsing quoted table name."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        schema, table = provider._parse_table_name('"my_schema"."my_table"')
        
        assert schema == "my_schema"
        assert table == "my_table"


class TestWal2JsonParsing:
    """Tests for wal2json message parsing."""

    def test_parse_insert(self):
        """Test parsing INSERT message."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        payload = '''
        {
            "action": "I",
            "schema": "public",
            "table": "users",
            "columns": [
                {"name": "id", "value": 1},
                {"name": "name", "value": "John"}
            ],
            "pk": [{"name": "id", "value": 1}]
        }
        '''
        
        changes = provider._parse_wal2json(payload, None)
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.INSERT
        assert changes[0].table == "public.users"
        assert changes[0].key == 1
        assert changes[0].data == {"id": 1, "name": "John"}

    def test_parse_update(self):
        """Test parsing UPDATE message."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        payload = '''
        {
            "action": "U",
            "schema": "public",
            "table": "users",
            "columns": [
                {"name": "id", "value": 1},
                {"name": "name", "value": "Jane"}
            ],
            "identity": [
                {"name": "name", "value": "John"}
            ],
            "pk": [{"name": "id", "value": 1}]
        }
        '''
        
        changes = provider._parse_wal2json(payload, None)
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.UPDATE
        assert changes[0].data["name"] == "Jane"
        assert changes[0].old_data["name"] == "John"

    def test_parse_delete(self):
        """Test parsing DELETE message."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        payload = '''
        {
            "action": "D",
            "schema": "public",
            "table": "users",
            "identity": [
                {"name": "id", "value": 1}
            ],
            "pk": [{"name": "id", "value": 1}]
        }
        '''
        
        changes = provider._parse_wal2json(payload, None)
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.DELETE
        assert changes[0].key == 1

    def test_parse_with_table_filter(self):
        """Test filtering changes by table."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        payload = '''
        {
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [{"name": "id", "value": 1}],
            "pk": [{"name": "id", "value": 1}]
        }
        '''
        
        # Filter for 'users' table - should not match 'orders'
        changes = provider._parse_wal2json(payload, "users")
        assert len(changes) == 0
        
        # Filter for 'orders' table - should match
        changes = provider._parse_wal2json(payload, "orders")
        assert len(changes) == 1

    def test_parse_composite_primary_key(self):
        """Test parsing table with composite primary key."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        payload = '''
        {
            "action": "I",
            "schema": "public",
            "table": "order_items",
            "columns": [
                {"name": "order_id", "value": 100},
                {"name": "item_id", "value": 5},
                {"name": "qty", "value": 2}
            ],
            "pk": [
                {"name": "order_id", "value": 100},
                {"name": "item_id", "value": 5}
            ]
        }
        '''
        
        changes = provider._parse_wal2json(payload, None)
        
        assert len(changes) == 1
        # Composite key should be a dict
        assert changes[0].key == {"order_id": 100, "item_id": 5}

    def test_parse_batch_changes(self):
        """Test parsing batch of changes."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        payload = '''
        {
            "change": [
                {"action": "I", "schema": "public", "table": "users", "columns": [{"name": "id", "value": 1}], "pk": [{"name": "id", "value": 1}]},
                {"action": "U", "schema": "public", "table": "users", "columns": [{"name": "id", "value": 2}], "pk": [{"name": "id", "value": 2}]},
                {"action": "D", "schema": "public", "table": "users", "identity": [{"name": "id", "value": 3}], "pk": [{"name": "id", "value": 3}]}
            ]
        }
        '''
        
        changes = provider._parse_wal2json(payload, None)
        
        assert len(changes) == 3
        assert changes[0].operation == ChangeType.INSERT
        assert changes[1].operation == ChangeType.UPDATE
        assert changes[2].operation == ChangeType.DELETE

    def test_skip_transaction_messages(self):
        """Test that BEGIN/COMMIT are skipped by default."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, include_transaction=False)
        
        payload = '{"action": "B"}'  # BEGIN
        changes = provider._parse_wal2json(payload, None)
        assert len(changes) == 0
        
        payload = '{"action": "C"}'  # COMMIT
        changes = provider._parse_wal2json(payload, None)
        assert len(changes) == 0

    def test_invalid_json(self):
        """Test handling of invalid JSON."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        changes = provider._parse_wal2json("not valid json", None)
        assert len(changes) == 0


class TestTestDecodingParsing:
    """Tests for test_decoding message parsing."""

    def test_parse_insert(self):
        """Test parsing INSERT message."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, output_plugin="test_decoding")
        
        payload = "table public.users: INSERT: id[integer]:1 name[text]:'John'"
        
        changes = provider._parse_test_decoding(payload, None)
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.INSERT
        assert changes[0].table == "public.users"

    def test_parse_update(self):
        """Test parsing UPDATE message."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, output_plugin="test_decoding")
        
        payload = "table public.users: UPDATE: id[integer]:1 name[text]:'Jane'"
        
        changes = provider._parse_test_decoding(payload, None)
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.UPDATE

    def test_parse_delete(self):
        """Test parsing DELETE message."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, output_plugin="test_decoding")
        
        payload = "table public.users: DELETE: id[integer]:1"
        
        changes = provider._parse_test_decoding(payload, None)
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.DELETE

    def test_skip_begin_commit(self):
        """Test skipping BEGIN/COMMIT messages."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, output_plugin="test_decoding")
        
        assert len(provider._parse_test_decoding("BEGIN 12345", None)) == 0
        assert len(provider._parse_test_decoding("COMMIT 12345", None)) == 0


class TestPluginOptions:
    """Tests for output plugin options building."""

    def test_wal2json_options_default(self):
        """Test wal2json options without table filter."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        options = provider._build_plugin_options(None, None)
        
        assert options['include-xids'] == '1'
        assert options['include-timestamp'] == '1'
        assert options['format-version'] == '2'

    def test_wal2json_options_with_table(self):
        """Test wal2json options with table filter."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        options = provider._build_plugin_options("public", "users")
        
        assert options['add-tables'] == 'public.users'

    def test_wal2json_options_with_table_no_schema(self):
        """Test wal2json options with table filter but no schema."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        options = provider._build_plugin_options(None, "users")
        
        assert options['add-tables'] == '*.users'

    def test_test_decoding_options(self):
        """Test test_decoding options are minimal."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter, output_plugin="test_decoding")
        
        options = provider._build_plugin_options("public", "users")
        
        # test_decoding has minimal options
        assert len(options) == 0


class TestProviderMetadata:
    """Tests for provider metadata."""

    def test_provider_name(self):
        """Test provider name is 'postgres'."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        assert provider.provider_name == "postgres"

    def test_supports_delete_detection(self):
        """Test delete detection is supported."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        assert provider.supports_delete_detection is True

    def test_supports_old_data(self):
        """Test old data (before-image) is supported."""
        adapter = Mock()
        provider = PostgresCDCProvider(adapter)
        
        assert provider.supports_old_data is True


class TestProviderRegistry:
    """Tests for provider registration."""

    def test_postgres_in_registry(self):
        """Test PostgresCDCProvider is registered."""
        from waveql.cdc.providers import CDC_PROVIDERS
        
        assert "postgres" in CDC_PROVIDERS
        assert "postgresql" in CDC_PROVIDERS

    def test_get_cdc_provider_postgres(self):
        """Test getting PostgreSQL CDC provider."""
        from waveql.cdc.providers import get_cdc_provider
        
        adapter = Mock()
        provider = get_cdc_provider("postgres", adapter)
        
        assert provider is not None
        assert isinstance(provider, PostgresCDCProvider)
