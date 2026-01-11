"""
WaveQL PostgreSQL Wire Protocol Server

Async TCP server that speaks PostgreSQL's wire protocol, allowing standard
PostgreSQL clients (psql, Tableau, PowerBI, DBeaver) to connect to WaveQL.

Usage:
    import waveql
    from waveql.pg_wire import PGWireServer
    
    conn = waveql.connect("servicenow://instance.service-now.com",
                          username="admin", password="secret")
    
    server = PGWireServer(conn)
    await server.serve(host="0.0.0.0", port=5432)
    
Or using the CLI:
    waveql-server --port 5432

Architecture:
    Client <--TCP--> PGWireServer
                        |
                        v
                   Session Handler
                        |
                   +----+----+
                   |         |
               Catalog   WaveQL
               Queries   Queries
                   |         |
                   v         v
              PGCatalog  DuckDB + Adapters
"""

from __future__ import annotations
import asyncio
import hashlib
import logging
import os
import random
import re
import struct
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pyarrow as pa

from waveql.pg_wire.protocol import (
    MessageType,
    FrontendMessageType,
    AuthType,
    FormatCode,
    TransactionStatus,
    ColumnDescription,
    MessageReader,
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
    build_empty_query_response,
    build_parse_complete,
    build_bind_complete,
    build_close_complete,
    build_no_data,
)
from waveql.pg_wire.catalog import PGCatalogEmulator
from waveql.pg_wire.type_mapping import (
    arrow_to_pg_oid,
    encode_text_value,
    PG_TYPES,
)

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


class PGWireSession:
    """
    Handles a single client session/connection.
    
    Manages the protocol state machine for one PostgreSQL client connection,
    including authentication, query execution, and result streaming.
    """
    
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        connection: "WaveQLConnection",
        server: "PGWireServer",
    ):
        self._reader = reader
        self._writer = writer
        self._connection = connection
        self._server = server
        self._catalog = PGCatalogEmulator(connection)
        
        # Session state
        self._authenticated = False
        self._user: str = ""
        self._database: str = ""
        self._application_name: str = ""
        self._process_id = random.randint(1, 2**31 - 1)
        self._secret_key = random.randint(1, 2**31 - 1)
        self._transaction_status = TransactionStatus.IDLE
        
        # Extended query protocol state
        self._prepared_statements: Dict[str, str] = {}  # name -> query
        self._portals: Dict[str, Tuple[str, List[Any]]] = {}  # name -> (query, params)
        
        # Client address for logging
        peername = writer.get_extra_info("peername")
        self._client_addr = f"{peername[0]}:{peername[1]}" if peername else "unknown"
    
    async def handle(self):
        """Main session handler loop."""
        try:
            # 1. Handle startup message
            if not await self._handle_startup():
                return
            
            # 2. Main message loop
            while True:
                try:
                    msg_type, payload = await self._read_message()
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    logger.debug(f"Client {self._client_addr} disconnected")
                    break
                
                if msg_type == FrontendMessageType.QUERY:
                    await self._handle_simple_query(payload)
                elif msg_type == FrontendMessageType.PARSE:
                    await self._handle_parse(payload)
                elif msg_type == FrontendMessageType.BIND:
                    await self._handle_bind(payload)
                elif msg_type == FrontendMessageType.DESCRIBE:
                    await self._handle_describe(payload)
                elif msg_type == FrontendMessageType.EXECUTE:
                    await self._handle_execute(payload)
                elif msg_type == FrontendMessageType.SYNC:
                    await self._handle_sync()
                elif msg_type == FrontendMessageType.CLOSE:
                    await self._handle_close(payload)
                elif msg_type == FrontendMessageType.TERMINATE:
                    logger.debug(f"Client {self._client_addr} terminated")
                    break
                else:
                    logger.warning(f"Unknown message type: {msg_type}")
        
        except Exception as e:
            logger.error(f"Session error for {self._client_addr}: {e}", exc_info=True)
            try:
                await self._send_error("FATAL", "XX000", str(e))
            except Exception:
                pass
        finally:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
    
    async def _read_message(self) -> Tuple[int, bytes]:
        """Read a single message from the client."""
        # Read message type byte
        type_byte = await self._reader.readexactly(1)
        msg_type = type_byte[0]
        
        # Read length (4 bytes, includes itself)
        length_bytes = await self._reader.readexactly(4)
        length = struct.unpack("!I", length_bytes)[0]
        
        # Read payload
        payload_length = length - 4
        if payload_length > 0:
            payload = await self._reader.readexactly(payload_length)
        else:
            payload = b""
        
        return msg_type, payload
    
    async def _send(self, data: bytes):
        """Send data to client."""
        self._writer.write(data)
        await self._writer.drain()
    
    async def _handle_startup(self) -> bool:
        """Handle initial startup message and authentication."""
        # Read startup message (special format: length + payload, no type byte)
        length_bytes = await self._reader.readexactly(4)
        length = struct.unpack("!I", length_bytes)[0]
        payload = await self._reader.readexactly(length - 4)
        
        try:
            startup = parse_startup_message(payload)
        except ProtocolError as e:
            if "SSL" in str(e):
                # SSL request - send 'N' for not supported
                await self._send(b"N")
                # Try again for real startup
                return await self._handle_startup()
            raise
        
        self._user = startup.user
        self._database = startup.database
        self._application_name = startup.application_name
        
        logger.info(
            f"Connection from {self._client_addr}: user={self._user}, "
            f"database={self._database}, app={self._application_name}"
        )
        
        # Authentication (trust mode for now)
        # TODO: Support MD5 password auth
        if self._server._auth_mode == "trust":
            await self._send(build_authentication_ok())
        elif self._server._auth_mode == "md5":
            salt = os.urandom(4)
            await self._send(build_authentication_md5(salt))
            
            # Read password response
            msg_type, payload = await self._read_message()
            if msg_type != FrontendMessageType.PASSWORD:
                await self._send_error("FATAL", "28P01", "Expected password message")
                return False
            
            # For now, accept any password in MD5 mode
            # TODO: Implement actual password verification
            await self._send(build_authentication_ok())
        
        self._authenticated = True
        
        # Send parameter statuses
        for name, value in self._catalog.get_version_info().items():
            await self._send(build_parameter_status(name, value))
        
        # Send backend key data
        await self._send(build_backend_key_data(self._process_id, self._secret_key))
        
        # Ready for query
        await self._send(build_ready_for_query(self._transaction_status))
        
        return True
    
    async def _handle_simple_query(self, payload: bytes):
        """Handle simple query protocol (Q message)."""
        # Query is null-terminated string
        query = payload[:-1].decode("utf-8")
        
        if not query.strip():
            await self._send(build_empty_query_response())
            await self._send(build_ready_for_query(self._transaction_status))
            return
        
        logger.debug(f"Query from {self._client_addr}: {query[:100]}...")
        
        try:
            # Check for special commands
            result = await self._execute_query(query)
            
            if result is None:
                # Command without result set (e.g., SET)
                await self._send(build_command_complete("OK"))
            elif len(result) == 0:
                # Empty result
                await self._send_result_set(result)
                await self._send(build_command_complete(f"SELECT 0"))
            else:
                # Send result set
                await self._send_result_set(result)
                await self._send(build_command_complete(f"SELECT {len(result)}"))
        
        except Exception as e:
            logger.error(f"Query error: {e}", exc_info=True)
            await self._send_error("ERROR", "42000", str(e))
        
        await self._send(build_ready_for_query(self._transaction_status))
    
    async def _execute_query(self, query: str) -> Optional[pa.Table]:
        """
        Execute a SQL query and return results.
        
        Handles:
        - Catalog queries (pg_catalog.*, information_schema.*)
        - Special commands (SET, SHOW, BEGIN, COMMIT, ROLLBACK)
        - Regular WaveQL queries
        """
        query_stripped = query.strip()
        query_upper = query_stripped.upper()
        
        # Handle transaction control (no-op, we don't have real transactions)
        if query_upper.startswith("BEGIN") or query_upper.startswith("START TRANSACTION"):
            self._transaction_status = TransactionStatus.IN_TRANSACTION
            return None
        
        if query_upper in ("COMMIT", "END"):
            self._transaction_status = TransactionStatus.IDLE
            return None
        
        if query_upper == "ROLLBACK":
            self._transaction_status = TransactionStatus.IDLE
            return None
        
        # Handle SET commands
        if query_upper.startswith("SET "):
            # Parse and ignore most SET commands
            # e.g., SET client_encoding = 'UTF8'
            return None
        
        # Handle SHOW commands
        show_match = re.match(r"SHOW\s+(\w+)", query_stripped, re.IGNORECASE)
        if show_match:
            param_name = show_match.group(1).lower()
            version_info = self._catalog.get_version_info()
            
            if param_name == "all":
                return pa.Table.from_pydict({
                    "name": list(version_info.keys()),
                    "setting": list(version_info.values()),
                })
            
            value = version_info.get(param_name, "")
            return pa.Table.from_pydict({param_name: [value]})
        
        # Handle DISCARD ALL (reset session)
        if query_upper == "DISCARD ALL":
            self._prepared_statements.clear()
            self._portals.clear()
            return None
        
        # Handle catalog queries
        if self._catalog.is_catalog_query(query):
            return await self._execute_catalog_query(query)
        
        # Execute via WaveQL
        return await self._execute_waveql_query(query)
    
    async def _execute_catalog_query(self, query: str) -> pa.Table:
        """Execute a query against the emulated pg_catalog."""
        # Try to extract table name from simple patterns
        # This is a simplified parser - real implementation would use sqlglot
        
        table_patterns = [
            r"FROM\s+pg_catalog\.(\w+)",
            r"FROM\s+information_schema\.(\w+)",
            r"FROM\s+(pg_\w+)",
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                result = self._catalog.handle_catalog_query(table_name)
                if result is not None:
                    # Apply remaining SQL logic via DuckDB
                    # Register and query
                    try:
                        self._connection._duckdb.register("__pg_catalog_temp", result)
                        modified_query = re.sub(
                            pattern,
                            "FROM __pg_catalog_temp",
                            query,
                            count=1,
                            flags=re.IGNORECASE
                        )
                        duck_result = self._connection._duckdb.execute(modified_query).fetch_arrow_table()
                        return duck_result
                    finally:
                        try:
                            self._connection._duckdb.unregister("__pg_catalog_temp")
                        except Exception:
                            pass
        
        # If we can't handle it, return empty result
        return pa.Table.from_pydict({})
    
    async def _execute_waveql_query(self, query: str) -> pa.Table:
        """Execute a query via WaveQL."""
        # Use WaveQL cursor
        cursor = self._connection.cursor()
        
        # Execute in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cursor.execute, query)
        
        # Get result as Arrow table
        if cursor._result is not None:
            return cursor._result
        
        # For non-SELECT queries, return empty table
        return pa.Table.from_pydict({})
    
    async def _send_result_set(self, table: pa.Table):
        """Send a result set to the client."""
        # Build column descriptions
        columns = []
        for i, field in enumerate(table.schema):
            type_oid = arrow_to_pg_oid(field.type)
            columns.append(ColumnDescription(
                name=field.name,
                type_oid=type_oid,
                format_code=FormatCode.TEXT,
            ))
        
        # Send row description
        await self._send(build_row_description(columns))
        
        # Send data rows
        for batch in table.to_batches():
            for row_idx in range(batch.num_rows):
                values = []
                for col_idx, col in enumerate(batch.columns):
                    value = col[row_idx].as_py()
                    type_oid = columns[col_idx].type_oid
                    encoded = encode_text_value(value, type_oid)
                    values.append(encoded)
                
                await self._send(build_data_row(values))
    
    async def _send_error(self, severity: str, code: str, message: str):
        """Send an error response."""
        await self._send(build_error_response(severity=severity, code=code, message=message))
    
    # Extended Query Protocol handlers
    
    async def _handle_parse(self, payload: bytes):
        """Handle Parse message (prepared statement)."""
        reader = MessageReader(payload)
        stmt_name = reader.read_string()
        query = reader.read_string()
        
        # Read parameter type OIDs (we ignore these for now)
        num_params = reader.read_int16()
        for _ in range(num_params):
            reader.read_int32()  # param type OID
        
        self._prepared_statements[stmt_name] = query
        await self._send(build_parse_complete())
    
    async def _handle_bind(self, payload: bytes):
        """Handle Bind message (bind parameters to portal)."""
        reader = MessageReader(payload)
        portal_name = reader.read_string()
        stmt_name = reader.read_string()
        
        # Read format codes
        num_format_codes = reader.read_int16()
        format_codes = [reader.read_int16() for _ in range(num_format_codes)]
        
        # Read parameter values
        num_params = reader.read_int16()
        params = []
        for _ in range(num_params):
            param_len = reader.read_int32()
            if param_len == -1:
                params.append(None)
            else:
                # Get format code (text or binary)
                # For now, assume text format
                param_bytes = reader.read_bytes(param_len)
                params.append(param_bytes.decode("utf-8"))
        
        # Read result format codes (ignore for now)
        num_result_format_codes = reader.read_int16()
        for _ in range(num_result_format_codes):
            reader.read_int16()
        
        # Store portal
        query = self._prepared_statements.get(stmt_name, "")
        self._portals[portal_name] = (query, params)
        
        await self._send(build_bind_complete())
    
    async def _handle_describe(self, payload: bytes):
        """Handle Describe message (get metadata for statement/portal)."""
        reader = MessageReader(payload)
        describe_type = chr(reader.read_byte())  # 'S' for statement, 'P' for portal
        name = reader.read_string()
        
        if describe_type == 'S':
            # Describe prepared statement
            query = self._prepared_statements.get(name, "")
            if not query:
                await self._send(build_no_data())
                return
            
            # Try to get schema without executing
            # For now, just send NoData
            await self._send(build_no_data())
        
        elif describe_type == 'P':
            # Describe portal
            portal = self._portals.get(name)
            if not portal:
                await self._send(build_no_data())
                return
            
            # Simplified: just send NoData
            # Full implementation would parse the query and return column info
            await self._send(build_no_data())
    
    async def _handle_execute(self, payload: bytes):
        """Handle Execute message (execute a portal)."""
        reader = MessageReader(payload)
        portal_name = reader.read_string()
        max_rows = reader.read_int32()  # 0 = no limit
        
        portal = self._portals.get(portal_name)
        if not portal:
            await self._send_error("ERROR", "34000", f"Portal not found: {portal_name}")
            return
        
        query, params = portal
        
        try:
            # Substitute parameters into query
            # Simple $1, $2 style substitution
            final_query = query
            for i, param in enumerate(params, start=1):
                placeholder = f"${i}"
                if param is None:
                    final_query = final_query.replace(placeholder, "NULL")
                elif isinstance(param, str):
                    # Escape single quotes
                    escaped = param.replace("'", "''")
                    final_query = final_query.replace(placeholder, f"'{escaped}'")
                else:
                    final_query = final_query.replace(placeholder, str(param))
            
            result = await self._execute_query(final_query)
            
            if result is None or len(result) == 0:
                await self._send(build_command_complete("SELECT 0"))
            else:
                await self._send_result_set(result)
                await self._send(build_command_complete(f"SELECT {len(result)}"))
        
        except Exception as e:
            logger.error(f"Execute error: {e}", exc_info=True)
            await self._send_error("ERROR", "42000", str(e))
    
    async def _handle_sync(self):
        """Handle Sync message (end of extended query sequence)."""
        await self._send(build_ready_for_query(self._transaction_status))
    
    async def _handle_close(self, payload: bytes):
        """Handle Close message (close statement/portal)."""
        reader = MessageReader(payload)
        close_type = chr(reader.read_byte())  # 'S' for statement, 'P' for portal
        name = reader.read_string()
        
        if close_type == 'S':
            self._prepared_statements.pop(name, None)
        elif close_type == 'P':
            self._portals.pop(name, None)
        
        await self._send(build_close_complete())


class PGWireServer:
    """
    PostgreSQL wire protocol server.
    
    Accepts TCP connections and handles them using the PostgreSQL
    frontend/backend protocol, enabling standard PostgreSQL clients
    to connect to WaveQL.
    """
    
    def __init__(
        self,
        connection: "WaveQLConnection",
        auth_mode: str = "trust",
    ):
        """
        Initialize the server.
        
        Args:
            connection: WaveQL connection to use for queries
            auth_mode: Authentication mode ("trust", "md5")
        """
        self._connection = connection
        self._auth_mode = auth_mode
        self._server: Optional[asyncio.AbstractServer] = None
        self._sessions: List[PGWireSession] = []
    
    async def serve(
        self,
        host: str = "0.0.0.0",
        port: int = 5432,
    ):
        """
        Start the server and listen for connections.
        
        Args:
            host: Host to bind to
            port: Port to listen on (default: 5432)
        """
        self._server = await asyncio.start_server(
            self._handle_client,
            host,
            port,
        )
        
        addrs = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info(f"WaveQL PostgreSQL server listening on {addrs}")
        print(f"🐘 WaveQL PostgreSQL server listening on {addrs}")
        print(f"   Connect with: psql -h {host} -p {port} -U postgres -d waveql")
        
        async with self._server:
            await self._server.serve_forever()
    
    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """Handle a new client connection."""
        session = PGWireSession(reader, writer, self._connection, self)
        self._sessions.append(session)
        
        try:
            await session.handle()
        finally:
            self._sessions.remove(session)
    
    async def stop(self):
        """Stop the server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("WaveQL PostgreSQL server stopped")
    
    @property
    def is_serving(self) -> bool:
        """Check if server is running."""
        return self._server is not None and self._server.is_serving()


async def run_server(
    connection: "WaveQLConnection",
    host: str = "0.0.0.0",
    port: int = 5432,
    auth_mode: str = "trust",
):
    """
    Convenience function to run the server.
    
    Example:
        import waveql
        from waveql.pg_wire import run_server
        
        conn = waveql.connect("servicenow://instance.service-now.com",
                              username="admin", password="secret")
        
        asyncio.run(run_server(conn, port=5432))
    """
    server = PGWireServer(connection, auth_mode=auth_mode)
    try:
        await server.serve(host, port)
    except asyncio.CancelledError:
        await server.stop()
