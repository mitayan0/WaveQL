"""
Tests for the PostgreSQL Wire Protocol Server (pg_wire)
"""

from __future__ import annotations
import struct
import pytest
import pyarrow as pa

from waveql.pg_wire.protocol import (
    MessageType,
    FrontendMessageType,
    AuthType,
    FormatCode,
    TransactionStatus,
    ColumnDescription,
    MessageReader,
    MessageWriter,
    ProtocolError,
    StartupMessage,
    parse_startup_message,
    build_authentication_ok,
    build_authentication_md5,
    build_parameter_status,
    build_backend_key_data,
    build_ready_for_query,
    build_row_description,
    build_data_row,
    build_command_complete,
    build_error_response,
)
from waveql.pg_wire.type_mapping import (
    PG_TYPES,
    arrow_to_pg_oid,
    pg_oid_to_arrow,
    encode_text_value,
    encode_binary_value,
)
from waveql.pg_wire.catalog import PGCatalogEmulator


class TestProtocolMessages:
    """Test protocol message parsing and building."""
    
    def test_message_writer_basic(self):
        """Test MessageWriter builds correct format."""
        writer = MessageWriter(MessageType.READY_FOR_QUERY)
        writer.write_byte(ord('I'))
        msg = writer.build()
        
        # Should be: type byte (1) + length (4) + payload (1) = 6 bytes
        assert len(msg) == 6
        assert msg[0] == MessageType.READY_FOR_QUERY
        
        # Length should be 5 (4 bytes for length itself + 1 byte payload)
        length = struct.unpack("!I", msg[1:5])[0]
        assert length == 5
    
    def test_message_reader_basic(self):
        """Test MessageReader can read basic types."""
        data = b'\x00\x01\x00\x02\x00\x00\x00\x03hello\x00'
        reader = MessageReader(data)
        
        assert reader.read_int16() == 1
        assert reader.read_int16() == 2
        assert reader.read_int32() == 3
        assert reader.read_string() == "hello"
    
    def test_message_reader_buffer_underflow(self):
        """Test MessageReader raises on underflow."""
        reader = MessageReader(b'\x00\x01')
        reader.read_int16()  # OK
        
        with pytest.raises(ProtocolError):
            reader.read_int32()  # Should fail - not enough bytes
    
    def test_parse_startup_message(self):
        """Test parsing a startup message."""
        # Build a startup message payload
        # Protocol version 3.0 = 0x00030000
        version = struct.pack("!I", 0x00030000)
        params = b"user\x00postgres\x00database\x00testdb\x00application_name\x00DBeaver\x00\x00"
        payload = version + params
        
        startup = parse_startup_message(payload)
        
        assert startup.protocol_version == (3, 0)
        assert startup.user == "postgres"
        assert startup.database == "testdb"
        assert startup.application_name == "DBeaver"
    
    def test_build_authentication_ok(self):
        """Test AuthenticationOk message."""
        msg = build_authentication_ok()
        
        assert msg[0] == MessageType.AUTHENTICATION
        # Parse the auth type (after type+length)
        auth_type = struct.unpack("!I", msg[5:9])[0]
        assert auth_type == AuthType.OK
    
    def test_build_authentication_md5(self):
        """Test AuthenticationMD5Password message."""
        salt = b'\x01\x02\x03\x04'
        msg = build_authentication_md5(salt)
        
        assert msg[0] == MessageType.AUTHENTICATION
        auth_type = struct.unpack("!I", msg[5:9])[0]
        assert auth_type == AuthType.MD5_PASSWORD
        assert msg[9:13] == salt
    
    def test_build_parameter_status(self):
        """Test ParameterStatus message."""
        msg = build_parameter_status("server_version", "15.0")
        
        assert msg[0] == MessageType.PARAMETER_STATUS
        # Payload should contain null-terminated strings
        assert b"server_version\x00" in msg
        assert b"15.0\x00" in msg
    
    def test_build_ready_for_query(self):
        """Test ReadyForQuery message."""
        msg = build_ready_for_query(TransactionStatus.IDLE)
        
        assert msg[0] == MessageType.READY_FOR_QUERY
        assert msg[-1] == ord('I')  # IDLE
    
    def test_build_row_description(self):
        """Test RowDescription message."""
        columns = [
            ColumnDescription(name="id", type_oid=23),  # int4
            ColumnDescription(name="name", type_oid=25),  # text
        ]
        msg = build_row_description(columns)
        
        assert msg[0] == MessageType.ROW_DESCRIPTION
        # Should contain column count (2)
        col_count = struct.unpack("!h", msg[5:7])[0]
        assert col_count == 2
    
    def test_build_data_row(self):
        """Test DataRow message."""
        values = [b"1", b"hello", None]  # id, name, null
        msg = build_data_row(values)
        
        assert msg[0] == MessageType.DATA_ROW
        # Column count
        col_count = struct.unpack("!h", msg[5:7])[0]
        assert col_count == 3
        
        # First value (length 1, "1")
        len1 = struct.unpack("!i", msg[7:11])[0]
        assert len1 == 1
        assert msg[11:12] == b"1"
        
        # Third value should be NULL (-1)
        # Position depends on previous values
        # value1: 4 bytes len + 1 byte = 5
        # value2: 4 bytes len + 5 bytes "hello" = 9
        # total: 7 (header) + 5 + 9 = 21
        null_len = struct.unpack("!i", msg[21:25])[0]
        assert null_len == -1
    
    def test_build_command_complete(self):
        """Test CommandComplete message."""
        msg = build_command_complete("SELECT 42")
        
        assert msg[0] == MessageType.COMMAND_COMPLETE
        assert b"SELECT 42\x00" in msg
    
    def test_build_error_response(self):
        """Test ErrorResponse message."""
        msg = build_error_response(
            severity="ERROR",
            code="42P01",
            message="Table not found"
        )
        
        assert msg[0] == MessageType.ERROR_RESPONSE
        assert b"ERROR\x00" in msg
        assert b"42P01\x00" in msg
        assert b"Table not found\x00" in msg


class TestTypeMapping:
    """Test Arrow <-> PostgreSQL type mapping."""
    
    def test_pg_types_basic(self):
        """Test basic PG_TYPES definitions."""
        assert PG_TYPES["text"].oid == 25
        assert PG_TYPES["int4"].oid == 23
        assert PG_TYPES["bool"].oid == 16
        assert PG_TYPES["float8"].oid == 701
    
    def test_arrow_to_pg_oid_integers(self):
        """Test Arrow integer type mapping."""
        assert arrow_to_pg_oid(pa.int16()) == PG_TYPES["int2"].oid
        assert arrow_to_pg_oid(pa.int32()) == PG_TYPES["int4"].oid
        assert arrow_to_pg_oid(pa.int64()) == PG_TYPES["int8"].oid
    
    def test_arrow_to_pg_oid_floats(self):
        """Test Arrow float type mapping."""
        assert arrow_to_pg_oid(pa.float32()) == PG_TYPES["float4"].oid
        assert arrow_to_pg_oid(pa.float64()) == PG_TYPES["float8"].oid
    
    def test_arrow_to_pg_oid_strings(self):
        """Test Arrow string type mapping."""
        assert arrow_to_pg_oid(pa.string()) == PG_TYPES["text"].oid
        assert arrow_to_pg_oid(pa.utf8()) == PG_TYPES["text"].oid
        assert arrow_to_pg_oid(pa.large_string()) == PG_TYPES["text"].oid
    
    def test_arrow_to_pg_oid_bool(self):
        """Test Arrow boolean type mapping."""
        assert arrow_to_pg_oid(pa.bool_()) == PG_TYPES["bool"].oid
    
    def test_arrow_to_pg_oid_timestamp(self):
        """Test Arrow timestamp type mapping."""
        assert arrow_to_pg_oid(pa.timestamp("us")) == PG_TYPES["timestamp"].oid
        assert arrow_to_pg_oid(pa.timestamp("us", tz="UTC")) == PG_TYPES["timestamptz"].oid
    
    def test_arrow_to_pg_oid_list(self):
        """Test Arrow list type mapping to array OID."""
        # List of int4 -> int4 array (OID 1007)
        list_type = pa.list_(pa.int32())
        oid = arrow_to_pg_oid(list_type)
        assert oid == PG_TYPES["int4"].array_oid
    
    def test_arrow_to_pg_oid_struct(self):
        """Test Arrow struct maps to JSONB."""
        struct_type = pa.struct([("a", pa.int32()), ("b", pa.string())])
        assert arrow_to_pg_oid(struct_type) == PG_TYPES["jsonb"].oid
    
    def test_pg_oid_to_arrow(self):
        """Test PostgreSQL OID to Arrow type mapping."""
        assert pg_oid_to_arrow(23) == pa.int32()  # int4
        assert pg_oid_to_arrow(25) == pa.string()  # text
        assert pg_oid_to_arrow(16) == pa.bool_()  # bool
    
    def test_encode_text_value_basic(self):
        """Test encoding values to text format."""
        assert encode_text_value(True, PG_TYPES["bool"].oid) == b"t"
        assert encode_text_value(False, PG_TYPES["bool"].oid) == b"f"
        assert encode_text_value(42, PG_TYPES["int4"].oid) == b"42"
        assert encode_text_value("hello", PG_TYPES["text"].oid) == b"hello"
        assert encode_text_value(None, PG_TYPES["text"].oid) is None
    
    def test_encode_binary_value_integers(self):
        """Test encoding integers to binary format."""
        # int2 (16-bit)
        assert encode_binary_value(42, PG_TYPES["int2"].oid) == struct.pack("!h", 42)
        
        # int4 (32-bit)
        assert encode_binary_value(42, PG_TYPES["int4"].oid) == struct.pack("!i", 42)
        
        # int8 (64-bit)
        assert encode_binary_value(42, PG_TYPES["int8"].oid) == struct.pack("!q", 42)
    
    def test_encode_binary_value_bool(self):
        """Test encoding boolean to binary format."""
        assert encode_binary_value(True, PG_TYPES["bool"].oid) == b'\x01'
        assert encode_binary_value(False, PG_TYPES["bool"].oid) == b'\x00'


class TestCatalogEmulator:
    """Test pg_catalog emulation."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a mock WaveQL connection."""
        class MockAdapter:
            adapter_name = "servicenow"
            
            def list_tables(self):
                return ["incident", "sys_user", "cmdb_ci"]
            
            def get_schema(self, table):
                from waveql.schema_cache import ColumnInfo
                return [
                    ColumnInfo(name="sys_id", data_type="string"),
                    ColumnInfo(name="short_description", data_type="string"),
                ]
        
        class MockConnection:
            _adapters = {"servicenow": MockAdapter(), "default": MockAdapter()}
            _schema_cache = None
            
            def list_materialized_views(self):
                return []
        
        return MockConnection()
    
    def test_pg_namespace(self, mock_connection):
        """Test pg_namespace generation."""
        catalog = PGCatalogEmulator(mock_connection)
        result = catalog.handle_catalog_query("pg_namespace")
        
        assert result is not None
        assert "nspname" in result.column_names
        
        schemas = result.column("nspname").to_pylist()
        assert "pg_catalog" in schemas
        assert "public" in schemas
        assert "servicenow" in schemas
    
    def test_pg_class(self, mock_connection):
        """Test pg_class generation."""
        catalog = PGCatalogEmulator(mock_connection)
        result = catalog.handle_catalog_query("pg_class")
        
        assert result is not None
        assert "relname" in result.column_names
        assert "relkind" in result.column_names
        
        tables = result.column("relname").to_pylist()
        assert "incident" in tables
        assert "sys_user" in tables
    
    def test_pg_type(self, mock_connection):
        """Test pg_type generation."""
        catalog = PGCatalogEmulator(mock_connection)
        result = catalog.handle_catalog_query("pg_type")
        
        assert result is not None
        assert "typname" in result.column_names
        
        types = result.column("typname").to_pylist()
        assert "text" in types
        assert "int4" in types
        assert "bool" in types
    
    def test_pg_database(self, mock_connection):
        """Test pg_database generation."""
        catalog = PGCatalogEmulator(mock_connection)
        result = catalog.handle_catalog_query("pg_database")
        
        assert result is not None
        assert "datname" in result.column_names
        
        databases = result.column("datname").to_pylist()
        assert "waveql" in databases
    
    def test_pg_settings(self, mock_connection):
        """Test pg_settings generation."""
        catalog = PGCatalogEmulator(mock_connection)
        result = catalog.handle_catalog_query("pg_settings")
        
        assert result is not None
        assert "name" in result.column_names
        assert "setting" in result.column_names
        
        settings = dict(zip(
            result.column("name").to_pylist(),
            result.column("setting").to_pylist()
        ))
        assert settings["server_version"] == "15.0"
        assert settings["server_encoding"] == "UTF8"
    
    def test_is_catalog_query(self, mock_connection):
        """Test catalog query detection."""
        catalog = PGCatalogEmulator(mock_connection)
        
        assert catalog.is_catalog_query("SELECT * FROM pg_catalog.pg_namespace")
        assert catalog.is_catalog_query("SELECT * FROM information_schema.tables")
        assert catalog.is_catalog_query("SELECT * FROM pg_type")
        assert not catalog.is_catalog_query("SELECT * FROM incident")
    
    def test_get_version_info(self, mock_connection):
        """Test version info generation."""
        catalog = PGCatalogEmulator(mock_connection)
        info = catalog.get_version_info()
        
        assert info["server_version"] == "15.0"
        assert info["server_encoding"] == "UTF8"
        assert info["client_encoding"] == "UTF8"
    
    def test_unknown_catalog_table(self, mock_connection):
        """Test handling of unknown catalog table."""
        catalog = PGCatalogEmulator(mock_connection)
        result = catalog.handle_catalog_query("pg_nonexistent")
        
        assert result is None


class TestPGWireServerIntegration:
    """Integration tests for the full server."""
    
    def test_server_starts(self):
        """Test that server can start and accept connections."""
        import asyncio
        import socket
        import waveql
        from waveql.pg_wire import PGWireServer
        
        async def run_test():
            conn = waveql.connect()
            server = PGWireServer(conn)
            
            # Find a free port
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', 0))
                port = s.getsockname()[1]
            
            # Start server in background
            task = asyncio.create_task(server.serve("127.0.0.1", port))
            
            # Give it a moment to start
            await asyncio.sleep(0.1)
            
            # Verify it's serving
            assert server.is_serving
            
            # Stop server
            await server.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        asyncio.run(run_test())
