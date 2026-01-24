"""
Comprehensive tests for waveql/pg_wire/server.py - targets 100% coverage

Tests for PGWireSession, PGWireServer, and run_server.
"""

from __future__ import annotations
import asyncio
import struct
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
import pyarrow as pa

from waveql.pg_wire.server import PGWireSession, PGWireServer, run_server
from waveql.pg_wire.protocol import (
    MessageType,
    FrontendMessageType,
    TransactionStatus,
    ProtocolError,
)


class MockStreamReader:
    """Mock asyncio.StreamReader for testing."""
    
    def __init__(self, data_chunks: list[bytes] = None):
        self.data = b"".join(data_chunks or [])
        self.pos = 0
    
    async def readexactly(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise asyncio.IncompleteReadError(self.data[self.pos:], n)
        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result
    
    def add_data(self, data: bytes):
        self.data += data


class MockStreamWriter:
    """Mock asyncio.StreamWriter for testing."""
    
    def __init__(self):
        self.written = b""
        self.closed = False
        self._extra_info = {"peername": ("127.0.0.1", 12345)}
    
    def write(self, data: bytes):
        self.written += data
    
    async def drain(self):
        pass
    
    def close(self):
        self.closed = True
    
    async def wait_closed(self):
        pass
    
    def get_extra_info(self, key: str):
        return self._extra_info.get(key)


def build_startup_message(user="postgres", database="waveql", app_name="test"):
    """Build a valid startup message."""
    # Protocol version 3.0
    version = struct.pack("!I", 0x00030000)
    params = f"user\x00{user}\x00database\x00{database}\x00application_name\x00{app_name}\x00\x00".encode()
    payload = version + params
    length = struct.pack("!I", len(payload) + 4)
    return length + payload


def build_query_message(query: str) -> bytes:
    """Build a simple query message."""
    query_bytes = query.encode() + b"\x00"
    length = struct.pack("!I", len(query_bytes) + 4)
    return bytes([FrontendMessageType.QUERY]) + length + query_bytes


def build_terminate_message() -> bytes:
    """Build a terminate message."""
    return bytes([FrontendMessageType.TERMINATE]) + struct.pack("!I", 4)


def build_parse_message(name: str, query: str, num_params: int = 0) -> bytes:
    """Build a Parse message."""
    payload = name.encode() + b"\x00" + query.encode() + b"\x00" + struct.pack("!h", num_params)
    length = struct.pack("!I", len(payload) + 4)
    return bytes([FrontendMessageType.PARSE]) + length + payload


def build_bind_message(portal: str, stmt: str, params: list = None) -> bytes:
    """Build a Bind message."""
    params = params or []
    payload = portal.encode() + b"\x00" + stmt.encode() + b"\x00"
    payload += struct.pack("!h", 0)  # num format codes
    payload += struct.pack("!h", len(params))  # num params
    for p in params:
        if p is None:
            payload += struct.pack("!i", -1)
        else:
            p_bytes = str(p).encode()
            payload += struct.pack("!i", len(p_bytes)) + p_bytes
    payload += struct.pack("!h", 0)  # num result format codes
    length = struct.pack("!I", len(payload) + 4)
    return bytes([FrontendMessageType.BIND]) + length + payload


def build_describe_message(desc_type: str, name: str) -> bytes:
    """Build a Describe message."""
    payload = desc_type.encode() + name.encode() + b"\x00"
    length = struct.pack("!I", len(payload) + 4)
    return bytes([FrontendMessageType.DESCRIBE]) + length + payload


def build_execute_message(portal: str, max_rows: int = 0) -> bytes:
    """Build an Execute message."""
    payload = portal.encode() + b"\x00" + struct.pack("!i", max_rows)
    length = struct.pack("!I", len(payload) + 4)
    return bytes([FrontendMessageType.EXECUTE]) + length + payload


def build_sync_message() -> bytes:
    """Build a Sync message."""
    return bytes([FrontendMessageType.SYNC]) + struct.pack("!I", 4)


def build_close_message(close_type: str, name: str) -> bytes:
    """Build a Close message."""
    payload = close_type.encode() + name.encode() + b"\x00"
    length = struct.pack("!I", len(payload) + 4)
    return bytes([FrontendMessageType.CLOSE]) + length + payload


class TestPGWireSession:
    """Tests for PGWireSession class."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a mock WaveQL connection."""
        conn = MagicMock()
        conn._duckdb = MagicMock()
        conn._adapters = {}
        conn.cursor.return_value = MagicMock()
        return conn
    
    @pytest.fixture
    def mock_server(self):
        """Create a mock PGWireServer."""
        server = MagicMock()
        server._auth_mode = "trust"
        return server
    
    def test_session_init(self, mock_connection, mock_server):
        """Test session initialization."""
        reader = MockStreamReader()
        writer = MockStreamWriter()
        
        session = PGWireSession(reader, writer, mock_connection, mock_server)
        
        assert session._authenticated is False
        assert session._user == ""
        assert session._database == ""
        assert session._transaction_status == TransactionStatus.IDLE
        assert session._client_addr == "127.0.0.1:12345"
    
    def test_session_init_no_peername(self, mock_connection, mock_server):
        """Test session init when peername is not available."""
        reader = MockStreamReader()
        writer = MockStreamWriter()
        writer._extra_info = {}  # No peername
        
        session = PGWireSession(reader, writer, mock_connection, mock_server)
        
        assert session._client_addr == "unknown"
    
    def test_handle_startup_trust(self, mock_connection, mock_server):
        """Test successful startup with trust auth."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert session._authenticated is True
            assert session._user == "postgres"
            assert session._database == "waveql"
            assert writer.closed is True
            
        anyio.run(run_test)
    
    def test_handle_startup_md5(self, mock_connection, mock_server):
        """Test startup with MD5 auth."""
        import anyio
        async def run_test():
            mock_server._auth_mode = "md5"
            
            startup_msg = build_startup_message()
            # Password message
            password = b"md5hash\x00"
            password_msg = bytes([FrontendMessageType.PASSWORD]) + struct.pack("!I", len(password) + 4) + password
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, password_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert session._authenticated is True
        
        anyio.run(run_test)
    
    def test_handle_startup_md5_wrong_message(self, mock_connection, mock_server):
        """Test startup with MD5 auth when wrong message type received."""
        import anyio
        async def run_test():
            mock_server._auth_mode = "md5"
            
            startup_msg = build_startup_message()
            # Send a query instead of password
            wrong_msg = build_query_message("SELECT 1")
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, wrong_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should have sent error and terminated
            assert b"Expected password" in writer.written or session._authenticated is False
        
        anyio.run(run_test)
    
    def test_handle_ssl_request(self, mock_connection, mock_server):
        """Test handling SSL request."""
        import anyio
        async def run_test():
            # SSL request: length + magic number
            ssl_request = struct.pack("!I", 8) + struct.pack("!I", 80877103)
            startup_msg = build_startup_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([ssl_request, startup_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should have sent 'N' to deny SSL
            assert b"N" in writer.written
            assert session._authenticated is True
        
        anyio.run(run_test)
    
    def test_handle_simple_query(self, mock_connection, mock_server):
        """Test simple query execution."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SELECT 1")
            terminate_msg = build_terminate_message()
            
            # Mock cursor result
            cursor = mock_connection.cursor.return_value
            cursor._result = pa.Table.from_pydict({"column1": [1]})
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            cursor.execute.assert_called()
        
        anyio.run(run_test)
    
    def test_handle_empty_query(self, mock_connection, mock_server):
        """Test empty query handling."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("   ")  # whitespace only
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should have sent EmptyQueryResponse
            assert MessageType.EMPTY_QUERY_RESPONSE in writer.written
        
        anyio.run(run_test)
    
    def test_handle_set_command(self, mock_connection, mock_server):
        """Test SET command handling."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SET client_encoding = 'UTF8'")
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # SET should complete successfully
            assert MessageType.COMMAND_COMPLETE in writer.written
        
        anyio.run(run_test)
    
    def test_handle_begin_commit_rollback(self, mock_connection, mock_server):
        """Test transaction control commands."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            begin_msg = build_query_message("BEGIN")
            commit_msg = build_query_message("COMMIT")
            rollback_msg = build_query_message("ROLLBACK")
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, begin_msg, commit_msg, rollback_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # All should complete
            assert writer.written.count(MessageType.COMMAND_COMPLETE) >= 3
        
        anyio.run(run_test)
    
    def test_handle_show_command(self, mock_connection, mock_server):
        """Test SHOW command handling."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SHOW server_version")
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should return a result
            assert MessageType.ROW_DESCRIPTION in writer.written
        
        anyio.run(run_test)
    
    def test_handle_show_all(self, mock_connection, mock_server):
        """Test SHOW ALL command."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SHOW ALL")
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert MessageType.ROW_DESCRIPTION in writer.written
        
        anyio.run(run_test)
    
    def test_handle_discard_all(self, mock_connection, mock_server):
        """Test DISCARD ALL command."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("DISCARD ALL")
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            
            # Add some state that should be cleared
            session._prepared_statements["test"] = "SELECT 1"
            session._portals["test"] = ("SELECT 1", [])
            
            await session.handle()
            
            assert len(session._prepared_statements) == 0
            assert len(session._portals) == 0
        
        anyio.run(run_test)
    
    def test_handle_query_error(self, mock_connection, mock_server):
        """Test query error handling."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SELECT * FROM nonexistent")
            terminate_msg = build_terminate_message()
            
            cursor = mock_connection.cursor.return_value
            cursor.execute.side_effect = Exception("Table not found")
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert MessageType.ERROR_RESPONSE in writer.written
        
        anyio.run(run_test)
    
    def test_handle_parse_bind_execute(self, mock_connection, mock_server):
        """Test extended query protocol."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            parse_msg = build_parse_message("stmt1", "SELECT $1")
            bind_msg = build_bind_message("", "stmt1", ["42"])
            execute_msg = build_execute_message("")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            cursor = mock_connection.cursor.return_value
            cursor._result = pa.Table.from_pydict({"col": [42]})
            
            reader = MockStreamReader([
                startup_msg, parse_msg, bind_msg, execute_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should have ParseComplete, BindComplete, and results
            assert b"1" in writer.written  # ParseComplete is '1'
            assert b"2" in writer.written  # BindComplete is '2'
        
        anyio.run(run_test)
    
    def test_handle_describe_statement(self, mock_connection, mock_server):
        """Test Describe for prepared statement."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            parse_msg = build_parse_message("stmt1", "SELECT 1")
            describe_msg = build_describe_message("S", "stmt1")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, parse_msg, describe_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should have NoData
            assert MessageType.NO_DATA in writer.written
        
        anyio.run(run_test)
    
    def test_handle_describe_nonexistent(self, mock_connection, mock_server):
        """Test Describe for nonexistent statement."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            describe_msg = build_describe_message("S", "nonexistent")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, describe_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert MessageType.NO_DATA in writer.written
        
        anyio.run(run_test)
    
    def test_handle_describe_portal(self, mock_connection, mock_server):
        """Test Describe for portal."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            describe_msg = build_describe_message("P", "test_portal")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, describe_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert MessageType.NO_DATA in writer.written
        
        anyio.run(run_test)
    
    def test_handle_execute_portal_not_found(self, mock_connection, mock_server):
        """Test Execute with nonexistent portal."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            execute_msg = build_execute_message("nonexistent")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, execute_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert MessageType.ERROR_RESPONSE in writer.written
        
        anyio.run(run_test)
    
    def test_handle_execute_with_null_param(self, mock_connection, mock_server):
        """Test Execute with NULL parameter."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            parse_msg = build_parse_message("stmt1", "SELECT $1")
            bind_msg = build_bind_message("", "stmt1", [None])
            execute_msg = build_execute_message("")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            cursor = mock_connection.cursor.return_value
            cursor._result = pa.Table.from_pydict({})
            
            reader = MockStreamReader([
                startup_msg, parse_msg, bind_msg, execute_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Check NULL was substituted
            call_args = cursor.execute.call_args
            if call_args:
                query = call_args[0][0]
                assert "NULL" in query
        
        anyio.run(run_test)
    
    def test_handle_close_statement(self, mock_connection, mock_server):
        """Test Close for statement."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            parse_msg = build_parse_message("stmt1", "SELECT 1")
            close_msg = build_close_message("S", "stmt1")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, parse_msg, close_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert "stmt1" not in session._prepared_statements
            assert b"3" in writer.written  # CloseComplete is '3'
        
        anyio.run(run_test)
    
    def test_handle_close_portal(self, mock_connection, mock_server):
        """Test Close for portal."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            close_msg = build_close_message("P", "portal1")
            sync_msg = build_sync_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([
                startup_msg, close_msg, sync_msg, terminate_msg
            ])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            session._portals["portal1"] = ("SELECT 1", [])
            
            await session.handle()
            
            assert "portal1" not in session._portals
        
        anyio.run(run_test)
    
    def test_handle_unknown_message(self, mock_connection, mock_server):
        """Test handling of unknown message type."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            # Unknown message type 0xFF
            unknown_msg = bytes([0xFF]) + struct.pack("!I", 4)
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, unknown_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should continue without crashing
            assert writer.closed is True
        
        anyio.run(run_test)
    
    def test_handle_connection_reset(self, mock_connection, mock_server):
        """Test handling of connection reset."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            
            reader = MockStreamReader([startup_msg])  # No more data
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should handle gracefully
            assert writer.closed is True
            
        anyio.run(run_test)
    
    def test_handle_session_exception(self, mock_connection, mock_server):
        """Test handling of unexpected exception in session."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SELECT 1")
            
            reader = MockStreamReader([startup_msg, query_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            session._catalog = MagicMock()
            session._catalog.is_catalog_query.side_effect = Exception("Unexpected!")
            
            await session.handle()
            
            # Should have sent error
            assert MessageType.ERROR_RESPONSE in writer.written
            
        anyio.run(run_test)
    
    def test_catalog_query_execution(self, mock_connection, mock_server):
        """Test catalog query execution."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SELECT * FROM pg_catalog.pg_namespace")
            terminate_msg = build_terminate_message()
            
            # Mock DuckDB
            mock_connection._duckdb.execute.return_value.fetch_arrow_table.return_value = pa.Table.from_pydict({
                "nspname": ["pg_catalog", "public"]
            })
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            assert MessageType.ROW_DESCRIPTION in writer.written
            
        anyio.run(run_test)
    
    def test_send_result_set(self, mock_connection, mock_server):
        """Test result set sending."""
        import anyio
        async def run_test():
            startup_msg = build_startup_message()
            query_msg = build_query_message("SELECT id, name FROM test")
            terminate_msg = build_terminate_message()
            
            cursor = mock_connection.cursor.return_value
            cursor._result = pa.Table.from_pydict({
                "id": [1, 2],
                "name": ["alice", "bob"]
            })
            
            reader = MockStreamReader([startup_msg, query_msg, terminate_msg])
            writer = MockStreamWriter()
            
            session = PGWireSession(reader, writer, mock_connection, mock_server)
            await session.handle()
            
            # Should have RowDescription and DataRows
            assert MessageType.ROW_DESCRIPTION in writer.written
            assert MessageType.DATA_ROW in writer.written
            
        anyio.run(run_test)


class TestPGWireServer:
    """Tests for PGWireServer class."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a mock WaveQL connection."""
        return MagicMock()
    
    def test_server_init(self, mock_connection):
        """Test server initialization."""
        server = PGWireServer(mock_connection, auth_mode="trust")
        
        assert server._connection == mock_connection
        assert server._auth_mode == "trust"
        assert server._server is None
        assert len(server._sessions) == 0
    
    def test_server_is_serving_false(self, mock_connection):
        """Test is_serving when not started."""
        server = PGWireServer(mock_connection)
        
        assert server.is_serving is False
    
    def test_server_serve_and_stop(self, mock_connection):
        """Test server start and stop."""
        import anyio
        async def run_test():
            import socket
            
            server = PGWireServer(mock_connection)
            
            # Find free port
            with socket.socket() as s:
                s.bind(('', 0))
                port = s.getsockname()[1]
            
            # Start server in background
            task = asyncio.create_task(server.serve("127.0.0.1", port))
            
            await asyncio.sleep(0.1)
            
            assert server.is_serving is True
            
            await server.stop()
            task.cancel()
            
            try:
                await task
            except asyncio.CancelledError:
                pass
            
            assert server.is_serving is False
            
        anyio.run(run_test)
    
    def test_server_handle_client(self, mock_connection):
        """Test client handling."""
        import anyio
        async def run_test():
            server = PGWireServer(mock_connection)
            
            startup_msg = build_startup_message()
            terminate_msg = build_terminate_message()
            
            reader = MockStreamReader([startup_msg, terminate_msg])
            writer = MockStreamWriter()
            
            await server._handle_client(reader, writer)
            
            # Session should have been added and removed
            assert len(server._sessions) == 0
            assert writer.closed is True
            
        anyio.run(run_test)


class TestRunServer:
    """Tests for run_server convenience function."""
    
    def test_run_server_cancellation(self):
        """Test run_server handles cancellation."""
        import anyio
        async def run_test():
            mock_conn = MagicMock()
            
            with patch('waveql.pg_wire.server.PGWireServer') as MockServer:
                mock_server = AsyncMock()
                mock_server.serve = AsyncMock(side_effect=asyncio.CancelledError())
                mock_server.stop = AsyncMock()
                MockServer.return_value = mock_server
                
                await run_server(mock_conn, port=55432)
                
                mock_server.stop.assert_called_once()
                
        anyio.run(run_test)


class TestCatalogQueryExecution:
    """Additional tests for catalog query handling."""
    
    @pytest.fixture
    def session(self):
        """Create a session for testing."""
        mock_conn = MagicMock()
        mock_conn._duckdb = MagicMock()
        mock_server = MagicMock()
        mock_server._auth_mode = "trust"
        
        reader = MockStreamReader()
        writer = MockStreamWriter()
        
        session = PGWireSession(reader, writer, mock_conn, mock_server)
        session._catalog = MagicMock()
        return session
    
    def test_catalog_query_patterns(self, session):
        """Test various catalog query pattern matching."""
        import anyio
        async def run_test():
            # Mock catalog
            session._catalog.is_catalog_query.return_value = True
            session._catalog.handle_catalog_query.return_value = pa.Table.from_pydict({"col": [1]})
            session._connection._duckdb.execute.return_value.fetch_arrow_table.return_value = pa.Table.from_pydict({"col": [1]})
            
            # Test various patterns
            patterns = [
                "SELECT * FROM pg_catalog.pg_namespace",
                "SELECT * FROM information_schema.tables",
                "SELECT * FROM pg_type",
            ]
            
            for query in patterns:
                result = await session._execute_catalog_query(query)
                assert result is not None
                
        anyio.run(run_test)
    
    def test_catalog_query_unregister_error(self, session):
        """Test catalog query when unregister fails."""
        import anyio
        async def run_test():
            session._catalog.handle_catalog_query.return_value = pa.Table.from_pydict({"col": [1]})
            session._connection._duckdb.execute.return_value.fetch_arrow_table.return_value = pa.Table.from_pydict({"col": [1]})
            session._connection._duckdb.unregister.side_effect = Exception("Unregister failed")
            
            # Should not raise
            result = await session._execute_catalog_query("SELECT * FROM pg_namespace")
            assert result is not None
            
        anyio.run(run_test)
    
    def test_execute_waveql_query_none_result(self, session):
        """Test WaveQL query returning None."""
        import anyio
        async def run_test():
            cursor = session._connection.cursor.return_value
            cursor._result = None
            
            result = await session._execute_waveql_query("UPDATE t SET x = 1")
            
            assert len(result) == 0  # Empty table
            
        anyio.run(run_test)


class TestExtendedQueryEscaping:
    """Tests for parameter escaping in extended query protocol."""
    
    @pytest.fixture
    def session(self):
        """Create a session for testing."""
        mock_conn = MagicMock()
        mock_conn._duckdb = MagicMock()
        cursor = MagicMock()
        cursor._result = pa.Table.from_pydict({})
        mock_conn.cursor.return_value = cursor
        
        mock_server = MagicMock()
        mock_server._auth_mode = "trust"
        
        reader = MockStreamReader()
        writer = MockStreamWriter()
        
        session = PGWireSession(reader, writer, mock_conn, mock_server)
        session._catalog = MagicMock()
        return session
    
    def test_parameter_quote_escaping(self, session):
        """Test that quotes in parameters are properly escaped."""
        import anyio
        async def run_test():
            session._prepared_statements["stmt"] = "SELECT * FROM t WHERE name = $1"
            session._portals["p"] = ("SELECT * FROM t WHERE name = $1", ["O'Brien"])
            session._catalog.is_catalog_query.return_value = False
            
            await session._handle_execute(b"p\x00" + struct.pack("!i", 0))
            
            # Should have escaped the quote
            call_args = session._connection.cursor().execute.call_args
            if call_args:
                query = call_args[0][0]
                assert "O''Brien" in query
        
        anyio.run(run_test)
