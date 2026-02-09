
import pytest
import json
from datetime import datetime
from unittest.mock import MagicMock, patch, AsyncMock
from waveql.cdc.postgres import PostgresCDCProvider
from waveql.cdc.models import ChangeType, CDCConfig

@pytest.fixture
def mock_adapter():
    adapter = MagicMock()
    adapter._connection_string = "postgresql://user:pass@localhost:5432/db"
    return adapter

@pytest.fixture
def provider(mock_adapter):
    return PostgresCDCProvider(mock_adapter)

def test_postgres_provider_init(provider):
    assert provider._slot_name == "waveql_cdc"
    assert provider._output_plugin == "wal2json"
    assert repr(provider) == "<PostgresCDCProvider slot=waveql_cdc plugin=wal2json>"

def test_get_connection_string(provider, mock_adapter):
    # From _connection_string
    assert provider._get_connection_string() == mock_adapter._connection_string
    
    # From override
    p2 = PostgresCDCProvider(mock_adapter, connection_string="override")
    assert p2._get_connection_string() == "override"
    
    # From _host
    mock_adapter._connection_string = None
    mock_adapter._host = "host"
    assert provider._get_connection_string() == "host"
    
    # No connection string
    mock_adapter._host = None
    with pytest.raises(ValueError, match="No connection string available"):
        provider._get_connection_string()

@pytest.mark.asyncio
async def test_ensure_slot_exists(provider):
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = mock_conn.cursor.return_value.__enter__.return_value
        
        # Case 1: Slot exists
        mock_cur.fetchone.return_value = ("waveql_cdc",)
        await provider._ensure_slot_exists()
        assert mock_cur.execute.called
        
        # Case 2: Slot missing, create_slot=True
        mock_cur.fetchone.return_value = None
        await provider._ensure_slot_exists()
        assert any("pg_create_logical_replication_slot" in str(call) for call in mock_cur.execute.call_args_list)
        
        # Case 3: Slot missing, create_slot=False
        provider._create_slot = False
        with pytest.raises(ValueError, match="does not exist"):
            await provider._ensure_slot_exists()

@pytest.mark.asyncio
async def test_connect_replication(provider):
    with patch("psycopg2.connect") as mock_connect:
        with patch.object(PostgresCDCProvider, "_ensure_slot_exists", new_callable=AsyncMock) as mock_ensure:
            await provider._connect_replication()
            mock_ensure.assert_called_once()
            assert mock_connect.called
            assert provider._replication_conn is not None

@pytest.mark.asyncio
async def test_get_changes_peek(provider):
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = mock_conn.cursor.return_value.__enter__.return_value
        
        # Mock peek changes result with more than batch_size
        mock_cur.fetchall.return_value = [
            ("0/1", 1, '{"action": "I", "table": "users", "columns": [{"name": "id", "value": 1}]}'),
            ("0/2", 2, '{"action": "I", "table": "users", "columns": [{"name": "id", "value": 2}]}')
        ]
        
        config = CDCConfig(batch_size=1)
        changes = await provider.get_changes("public.users", config=config)
        assert len(changes) == 1
        assert mock_conn.close.called

def test_parse_wal2json_v2_batch(provider):
    payload = json.dumps({
        "change": [
            {"action": "I", "schema": "public", "table": "users", "columns": [{"name": "id", "value": 1}]},
            {"action": "U", "schema": "public", "table": "users", "columns": [{"name": "id", "value": 1}, {"name": "v", "value": 2}], "identity": [{"name": "id", "value": 1}]},
            {"action": "D", "schema": "public", "table": "users", "identity": [{"name": "id", "value": 1}]}
        ]
    })
    
    changes = provider._parse_wal2json(payload, "public.users")
    assert len(changes) == 3

def test_parse_wal2json_pk(provider):
    payload = json.dumps({"action": "I", "table": "users", "pk": [{"name": "id", "value": 1}]})
    assert provider._parse_wal2json(payload)[0].key == 1
    
    payload2 = json.dumps({"action": "I", "table": "users", "pk": [{"name": "id", "value": 1}, {"name": "a", "value": 2}]})
    assert provider._parse_wal2json(payload2)[0].key == {"id": 1, "a": 2}

def test_parse_wal2json_timestamp(provider):
    payload = json.dumps({"action": "I", "table": "users", "timestamp": "2023-01-01T00:00:00Z"})
    assert provider._parse_wal2json(payload)[0].timestamp.year == 2023
    
    payload2 = json.dumps({"action": "I", "table": "users", "timestamp": "invalid"})
    assert isinstance(provider._parse_wal2json(payload2)[0].timestamp, datetime)

def test_parse_test_decoding(provider):
    provider._output_plugin = "test_decoding"
    
    # INSERT
    payload = "table public.users: INSERT: id[integer]:1 name[text]:'John'"
    changes = provider._parse_wal_message(payload, "public.users")
    assert changes[0].operation == ChangeType.INSERT
    assert changes[0].data["name"] == "John"
    
    # UPDATE
    payload_u = "table public.users: UPDATE: id[integer]:1"
    changes_u = provider._parse_wal_message(payload_u)
    assert changes_u[0].operation == ChangeType.UPDATE
    
    # DELETE
    payload_d = "table public.users: DELETE: id[integer]:1"
    changes_d = provider._parse_wal_message(payload_d)
    assert changes_d[0].operation == ChangeType.DELETE

@pytest.mark.asyncio
async def test_stream_changes_flow(provider):
    with patch.object(PostgresCDCProvider, "_connect_replication", new_callable=AsyncMock) as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_cursor
        
        mock_msg = MagicMock()
        mock_msg.payload = json.dumps({"action": "I", "table": "users", "columns": [{"name": "id", "value": 1}]})
        mock_msg.data_start = "0/456"
        mock_msg.cursor = mock_cursor
        
        provider._running = True
        read_side_effect = MagicMock(side_effect=[None, mock_msg])
        
        def mock_read(*args, **kwargs):
            res = read_side_effect()
            if res is not None:
                provider._running = False
            return res
            
        with patch.object(provider, "_read_message_sync", side_effect=mock_read):
            changes = []
            async for c in provider.stream_changes("public.users"):
                changes.append(c)
                
            assert len(changes) == 1
            assert mock_cursor.start_replication.called

@pytest.mark.asyncio
async def test_drop_slot(provider):
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = mock_conn.cursor.return_value.__enter__.return_value
        await provider.drop_slot(force=True)
        assert mock_cur.execute.called

@pytest.mark.asyncio
async def test_get_slot_info(provider):
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = mock_conn.cursor.return_value.__enter__.return_value
        mock_cur.fetchone.return_value = ("waveql_cdc", "wal2json", "logical", True, "0/1", "0/2", "0/3", "10 MB")
        info = await provider.get_slot_info()
        assert info["slot_name"] == "waveql_cdc"
        mock_cur.fetchone.return_value = None
        assert await provider.get_slot_info() is None

@pytest.mark.asyncio
async def test_stream_error_retry(provider):
    with patch.object(provider, "_connect_replication", new_callable=AsyncMock) as mock_connect_repl:
        mock_cursor = MagicMock()
        mock_connect_repl.return_value = mock_cursor
        provider.max_retries = 1
        provider.retry_delay = 0
        def side_effect(*args, **kwargs):
            if provider._consecutive_errors >= 1:
                provider._running = False
            raise Exception("BOOM")
        with patch.object(provider, "_read_message_sync", side_effect=side_effect):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                try:
                    async for _ in provider.stream_changes("users"):
                        pass
                except Exception:
                    pass

@pytest.mark.asyncio
async def test_read_message_sync_select(provider):
    mock_cursor = MagicMock()
    mock_cursor.connection = MagicMock()
    with patch("select.select", return_value=([mock_cursor.connection], [], [])):
        provider._read_message_sync(mock_cursor, 0.1)
    with patch("select.select", return_value=([], [], [])):
        assert provider._read_message_sync(mock_cursor, 0.1) is None

def test_build_plugin_options_test_decoding(provider):
    provider._output_plugin = "test_decoding"
    assert provider._build_plugin_options("public", "users") == {}

def test_parse_wal2json_transaction_msgs(provider):
    provider._include_transaction = False
    payload = json.dumps({"change": [{"action": "B"}, {"action": "C"}]})
    assert provider._parse_wal2json(payload) == []
    provider._include_transaction = True
    payload2 = json.dumps({"change": [{"action": "B"}, {"action": "C"}, {"action": "X", "table": "t"}]})
    assert len(provider._parse_wal2json(payload2)) == 1

def test_parse_wal2json_filter_mismatch(provider):
    # Schema mismatch
    payload = json.dumps({"action": "I", "schema": "other", "table": "users"})
    assert provider._parse_wal2json(payload, "public.users") == []
    # Table mismatch
    payload2 = json.dumps({"action": "I", "schema": "public", "table": "other"})
    assert provider._parse_wal2json(payload2, "public.users") == []

def test_parse_wal2json_no_pk_no_cols(provider):
    payload = json.dumps({"action": "I", "schema": "public", "table": "users"})
    changes = provider._parse_wal2json(payload)
    assert changes[0].key is None
    assert changes[0].data is None

def test_parse_test_decoding_errors(provider):
    provider._output_plugin = "test_decoding"
    assert provider._parse_test_decoding("BEGIN") == []
    assert provider._parse_test_decoding("not table") == []
    assert provider._parse_test_decoding("table t") == []
    assert provider._parse_test_decoding("table t: UNK: cols") == []
    
    # SCHEMA mismatch (hits line 566)
    assert provider._parse_test_decoding("table other.t: INSERT: id:1", "public.t") == []
    # TABLE mismatch (hits line 568)
    assert provider._parse_test_decoding("table public.other: INSERT: id:1", "public.t") == []
    
    res = provider._parse_test_decoding("table public.t: INSERT: malformed")
    assert len(res) == 1 and res[0].data is None
    with patch("waveql.cdc.postgres.PostgresCDCProvider._parse_table_name", side_effect=Exception("FAIL")):
        assert provider._parse_test_decoding("table p.t: INSERT: id[int]:1") == []

@pytest.mark.asyncio
async def test_close_connection_full(provider):
    provider._cursor = MagicMock()
    provider._replication_conn = MagicMock()
    await provider._close_connection()
    assert provider._cursor is None
    provider._cursor = MagicMock()
    provider._cursor.close.side_effect = Exception("FAIL")
    await provider._close_connection()

@pytest.mark.asyncio
async def test_stop(provider):
    provider._running = True
    with patch.object(provider, "_close_connection", new_callable=AsyncMock) as mock_close:
        await provider.stop()
        assert provider._running is False
        assert mock_close.called

@pytest.mark.asyncio
async def test_drop_slot_not_exists(provider):
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = mock_conn.cursor.return_value.__enter__.return_value
        from psycopg2 import Error
        mock_cur.execute.side_effect = Error("slot \"foo\" does not exist")
        assert await provider.drop_slot() is False
        mock_cur.execute.side_effect = Error("OTHER ERROR")
        with pytest.raises(Error):
            await provider.drop_slot()

def test_parse_wal2json_errors(provider):
    assert provider._parse_wal2json("invalid") == []
    assert provider._parse_wal2json('{"a":1}') == []
