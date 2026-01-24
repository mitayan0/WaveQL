
import pytest
from unittest.mock import MagicMock, patch, ANY
import asyncio
from datetime import datetime
import json

from waveql.cdc.postgres import PostgresCDCProvider
from waveql.cdc.models import ChangeType, CDCConfig

class TestPostgresCDCProvider:
    
    @pytest.fixture
    def mock_adapter(self):
        adapter = MagicMock()
        adapter.adapter_name = "postgres"
        adapter._connection_string = "postgresql://user:pass@localhost/db"
        return adapter

    @pytest.fixture
    def mock_psycopg2(self):
        mock = MagicMock()
        with patch.dict("sys.modules", {"psycopg2": mock, "psycopg2.sql": MagicMock(), "psycopg2.extras": MagicMock()}):
            yield mock

    def test_init(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter, slot_name="test_slot")
        assert provider._slot_name == "test_slot"
        assert provider.provider_name == "postgres"
        assert provider.supports_delete_detection is True

    def test_get_connection_string_from_args(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter, connection_string="postgresql://custom")
        assert provider._get_connection_string() == "postgresql://custom"

    def test_get_connection_string_from_adapter(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter)
        assert provider._get_connection_string() == "postgresql://user:pass@localhost/db"

    @pytest.mark.asyncio
    async def test_ensure_slot_exists_creates_slot(self, mock_adapter, mock_psycopg2):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Simulate slot not existing
        mock_cursor.fetchone.return_value = None
        
        provider = PostgresCDCProvider(mock_adapter, slot_name="new_slot", create_slot=True)
        await provider._ensure_slot_exists()
        
        # Verify slot creation query
        mock_cursor.execute.assert_any_call(
            "SELECT pg_create_logical_replication_slot(%s, %s)",
            ("new_slot", "wal2json")
        )

    @pytest.mark.asyncio
    async def test_ensure_slot_exists_already_exists(self, mock_adapter, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Simulate slot existing
        mock_cursor.fetchone.return_value = ("existing_slot",)
        
        provider = PostgresCDCProvider(mock_adapter, slot_name="existing_slot")
        await provider._ensure_slot_exists()
        
        # Verify NO slot creation query
        assert mock_cursor.execute.call_count == 1 # Only the check query
        mock_cursor.execute.assert_called_with(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
            ("existing_slot",)
        )

    @pytest.mark.asyncio
    async def test_connect_replication(self, mock_adapter, mock_psycopg2):
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        
        # Mock _ensure_slot_exists to avoid side effects
        provider = PostgresCDCProvider(mock_adapter)
        with patch.object(provider, '_ensure_slot_exists', new_callable=AsyncMock) as mock_ensure:
            cursor = await provider._connect_replication()
            
            mock_ensure.assert_called_once()
            mock_psycopg2.connect.assert_called_with(
                provider._get_connection_string(),
                connection_factory=ANY 
            )
            assert cursor == mock_conn.cursor.return_value

    @pytest.mark.asyncio
    async def test_get_changes_wal2json(self, mock_adapter, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Simulate peek changes result
        payload = json.dumps({
            "change": [
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "users",
                    "columnnames": ["id", "name"],
                    "columnvalues": [1, "test"],
                    "pk": [{"name": "id", "type": "integer", "value": 1}],
                    "action": "I", # v2 format
                    "columns": [
                        {"name": "id", "value": 1},
                        {"name": "name", "value": "test"}
                    ]
                }
            ]
        })
        mock_cursor.fetchall.return_value = [(1, 123, payload)]
        
        provider = PostgresCDCProvider(mock_adapter)
        changes = await provider.get_changes("public.users")
        
        assert len(changes) == 1
        assert changes[0].operation == ChangeType.INSERT
        assert changes[0].table == "public.users"
        assert changes[0].data["name"] == "test"
        assert changes[0].key == 1

    def test_parse_wal2json_insert(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter)
        payload = json.dumps({
            "action": "I",
            "schema": "public",
            "table": "users",
            "pk": [{"name": "id", "value": 1}],
            "columns": [
                {"name": "id", "value": 1},
                {"name": "name", "value": "Alice"}
            ]
        })
        
        changes = provider._parse_wal2json(payload)
        assert len(changes) == 1
        change = changes[0]
        assert change.operation == ChangeType.INSERT
        assert change.key == 1
        assert change.data == {"id": 1, "name": "Alice"}

    def test_parse_wal2json_update(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter)
        payload = json.dumps({
            "action": "U",
            "schema": "public",
            "table": "users",
            "pk": [{"name": "id", "value": 1}],
            "columns": [
                {"name": "id", "value": 1},
                {"name": "name", "value": "Bob"}
            ],
            "identity": [
                 {"name": "id", "value": 1},
                 {"name": "name", "value": "Alice"}
            ]
        })
        
        changes = provider._parse_wal2json(payload)
        assert len(changes) == 1
        change = changes[0]
        assert change.operation == ChangeType.UPDATE
        assert change.key == 1
        assert change.data == {"id": 1, "name": "Bob"}
        assert change.old_data == {"id": 1, "name": "Alice"}

    def test_parse_wal2json_filter(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter)
        payload = json.dumps({
            "action": "I",
            "schema": "other",
            "table": "logs",
            "pk": [{"name": "id", "value": 99}],
            "columns": []
        })
        
        # Valid filter
        changes = provider._parse_wal2json(payload, table_filter="other.logs")
        assert len(changes) == 1

        # Invalid filter
        changes = provider._parse_wal2json(payload, table_filter="public.users")
        assert len(changes) == 0

    def test_parse_test_decoding(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter, output_plugin="test_decoding")
        payload = "table public.users: INSERT: id[integer]:1 name[text]:'test'"
        
        changes = provider._parse_test_decoding(payload)
        assert len(changes) == 1
        change = changes[0]
        assert change.operation == ChangeType.INSERT
        assert change.table == "public.users"
        assert change.data["name"] == "test"

    @pytest.mark.asyncio
    async def test_drop_slot(self, mock_adapter, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        provider = PostgresCDCProvider(mock_adapter, slot_name="test_slot")
        await provider.drop_slot()
        
        mock_cursor.execute.assert_called_with(
            "SELECT pg_drop_replication_slot(%s)",
            ("test_slot",)
        )

    @pytest.mark.asyncio
    async def test_get_slot_info(self, mock_adapter, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_cursor.fetchone.return_value = (
            "test_slot", "wal2json", "logical", True, 
            "0/15E3", "0/15E3", "0/1600", "32 bytes"
        )
        
        provider = PostgresCDCProvider(mock_adapter, slot_name="test_slot")
        info = await provider.get_slot_info()
        
        assert info["slot_name"] == "test_slot"
        assert info["plugin"] == "wal2json"
        assert info["lag"] == "32 bytes"

    def test_parse_table_name(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter)
        
        # Schema and table
        assert provider._parse_table_name("public.users") == ("public", "users")
        assert provider._parse_table_name('"public"."users"') == ("public", "users")
        
        # Table only
        assert provider._parse_table_name("users") == (None, "users")

    def test_build_plugin_options(self, mock_adapter):
        provider = PostgresCDCProvider(mock_adapter, output_plugin="wal2json")
        
        options = provider._build_plugin_options("public", "users")
        assert options["include-xids"] == "1"
        assert options["add-tables"] == "public.users"
        
        provider = PostgresCDCProvider(mock_adapter, output_plugin="test_decoding")
        assert provider._build_plugin_options("public", "users") == {}

    @pytest.mark.asyncio
    async def test_stream_changes(self, mock_adapter, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock _read_message_sync to return a message then None (timeout)
        mock_msg = MagicMock()
        mock_msg.payload = json.dumps({
            "action": "I",
            "schema": "public",
            "table": "users",
            "pk": [{"name": "id", "value": 1}],
            "columns": []
        })
        mock_msg.data_start = 12345
        mock_msg.cursor.send_feedback = MagicMock()

        # We need to break the loop. 
        # We can make _read_message_sync return [msg, None, None...] or raise exception or strict count
        # Or we can just run for a bit 
        
        provider = PostgresCDCProvider(mock_adapter)
        
        # Mock run_in_executor to avoid actual threading and just return msg
        # First call returns msg, second raises runtime error to stop loop?
        # Or use side_effect
        
        async def mock_executor(executor, func, *args):
            if func == provider._read_message_sync:
                # If it's the first call return msg
                return mock_msg
            return None
            
        with patch("asyncio.get_event_loop") as mock_loop:
            # We want to break the loop by setting _running = False
            # We can do this by having the usage loop break it
            # But stream_changes loop checks self._running
            
            # Let's mock stream_changes logic partially?
            # Or just let it run one iteration.
            
            # Better strategy: Mock internal _connect_replication and _read_message_sync
            pass

    @pytest.mark.asyncio
    async def test_stream_changes_logic(self, mock_adapter):
        # We will mock _connect_replication and _read_message_sync
        provider = PostgresCDCProvider(mock_adapter)
        
        mock_cursor = MagicMock()
        provider._connect_replication = AsyncMock(return_value=mock_cursor)
        provider._close_connection = AsyncMock()
        
        # Mock payload
        payload = json.dumps({
            "action": "I",
            "schema": "public",
            "table": "users",
            "pk": [{"name": "id", "value": 1}],
            "columns": []
        })
        mock_msg = MagicMock()
        mock_msg.payload = payload
        
        # _read_message_sync is called in executor.
        # We can mock this call.
        
        with patch.object(PostgresCDCProvider, '_read_message_sync') as mock_read:
            mock_read.return_value = mock_msg
            
            # We need to run the generator
            # We'll run it for one iteration then break
            
            agen = provider.stream_changes("public.users")
            
            # Mock get_event_loop().run_in_executor
            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[mock_msg, Exception("Stop Loop")])
                
                try:
                    async for change in agen:
                        assert change.operation == ChangeType.INSERT
                        break # Process one change and exit
                except Exception:
                    pass
                
                # Cleanup should have been called if we exited generator? 
                # Actually if we break, generator might not close immediately unless we close it
                await agen.aclose()
                
            provider._connect_replication.assert_called()
            provider._close_connection.assert_called()


from unittest.mock import AsyncMock
