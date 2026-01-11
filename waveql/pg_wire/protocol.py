"""
PostgreSQL Wire Protocol Message Definitions (Protocol Version 3.0)

This module implements the binary message format used by PostgreSQL clients
to communicate with the server. Reference: PostgreSQL docs, "Frontend/Backend Protocol".

Message Format:
- Frontend (client) messages: 1-byte type + 4-byte length + payload
- Backend (server) messages: 1-byte type + 4-byte length + payload (except Startup)

Type OIDs are mapped in type_mapping.py for Arrow <-> Postgres conversion.
"""

from __future__ import annotations
import struct
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class MessageType(IntEnum):
    """PostgreSQL message type bytes (Backend -> Frontend)."""
    # Authentication
    AUTHENTICATION = ord('R')
    
    # Backend key data (for cancel requests)
    BACKEND_KEY_DATA = ord('K')
    
    # Command complete (e.g., "SELECT 5")
    COMMAND_COMPLETE = ord('C')
    
    # Data row
    DATA_ROW = ord('D')
    
    # Error response
    ERROR_RESPONSE = ord('E')
    
    # Notice response (warnings)
    NOTICE_RESPONSE = ord('N')
    
    # Parameter status (e.g., server_version)
    PARAMETER_STATUS = ord('S')
    
    # Ready for query
    READY_FOR_QUERY = ord('Z')
    
    # Row description (column metadata)
    ROW_DESCRIPTION = ord('T')
    
    # Parse complete
    PARSE_COMPLETE = ord('1')
    
    # Bind complete
    BIND_COMPLETE = ord('2')
    
    # Close complete
    CLOSE_COMPLETE = ord('3')
    
    # Portal suspended (for streaming)
    PORTAL_SUSPENDED = ord('s')
    
    # No data
    NO_DATA = ord('n')
    
    # Empty query response
    EMPTY_QUERY_RESPONSE = ord('I')


class FrontendMessageType(IntEnum):
    """PostgreSQL message type bytes (Frontend -> Backend)."""
    # Startup message (no type byte, special format)
    STARTUP = 0
    
    # Simple query
    QUERY = ord('Q')
    
    # Extended query protocol
    PARSE = ord('P')
    BIND = ord('B')
    DESCRIBE = ord('D')
    EXECUTE = ord('E')
    CLOSE = ord('C')
    SYNC = ord('S')
    FLUSH = ord('H')
    
    # Termination
    TERMINATE = ord('X')
    
    # Password message (response to auth)
    PASSWORD = ord('p')
    
    # Cancel request (special)
    CANCEL_REQUEST = 0


class AuthType(IntEnum):
    """PostgreSQL authentication types."""
    OK = 0
    KERBEROS_V5 = 2
    CLEARTEXT_PASSWORD = 3
    MD5_PASSWORD = 5
    SCM_CREDENTIAL = 6
    GSS = 7
    GSS_CONTINUE = 8
    SSPI = 9
    SASL = 10
    SASL_CONTINUE = 11
    SASL_FINAL = 12


class FormatCode(IntEnum):
    """Data format codes."""
    TEXT = 0
    BINARY = 1


class TransactionStatus(IntEnum):
    """Transaction status indicators."""
    IDLE = ord('I')
    IN_TRANSACTION = ord('T')
    FAILED_TRANSACTION = ord('E')


@dataclass
class ColumnDescription:
    """Column metadata for RowDescription message."""
    name: str
    table_oid: int = 0  # 0 if not from a table
    column_attr_num: int = 0  # 0 if not from a table
    type_oid: int = 25  # Default to TEXT (OID 25)
    type_size: int = -1  # -1 for variable-length
    type_modifier: int = -1  # -1 for no modifier
    format_code: FormatCode = FormatCode.TEXT


@dataclass
class StartupMessage:
    """Parsed startup message from client."""
    protocol_version: Tuple[int, int]  # (major, minor)
    parameters: Dict[str, str] = field(default_factory=dict)
    
    @property
    def user(self) -> str:
        return self.parameters.get("user", "")
    
    @property
    def database(self) -> str:
        return self.parameters.get("database", self.user)
    
    @property
    def application_name(self) -> str:
        return self.parameters.get("application_name", "")


class ProtocolError(Exception):
    """Protocol-level error."""
    pass


class MessageReader:
    """Read and parse PostgreSQL binary messages."""
    
    def __init__(self, buffer: bytes):
        self._buffer = buffer
        self._pos = 0
    
    def _check_remaining(self, n: int):
        if self._pos + n > len(self._buffer):
            raise ProtocolError(f"Buffer underflow: need {n} bytes, have {len(self._buffer) - self._pos}")
    
    def read_byte(self) -> int:
        """Read a single byte."""
        self._check_remaining(1)
        val = self._buffer[self._pos]
        self._pos += 1
        return val
    
    def read_int16(self) -> int:
        """Read a 16-bit signed integer (network byte order)."""
        self._check_remaining(2)
        val = struct.unpack("!h", self._buffer[self._pos:self._pos+2])[0]
        self._pos += 2
        return val
    
    def read_int32(self) -> int:
        """Read a 32-bit signed integer (network byte order)."""
        self._check_remaining(4)
        val = struct.unpack("!i", self._buffer[self._pos:self._pos+4])[0]
        self._pos += 4
        return val
    
    def read_string(self) -> str:
        """Read a null-terminated string."""
        end = self._buffer.find(b'\x00', self._pos)
        if end == -1:
            raise ProtocolError("Unterminated string")
        val = self._buffer[self._pos:end].decode("utf-8")
        self._pos = end + 1
        return val
    
    def read_bytes(self, n: int) -> bytes:
        """Read n bytes."""
        self._check_remaining(n)
        val = self._buffer[self._pos:self._pos+n]
        self._pos += n
        return val
    
    @property
    def remaining(self) -> int:
        return len(self._buffer) - self._pos


class MessageWriter:
    """Build PostgreSQL binary messages."""
    
    def __init__(self, msg_type: MessageType = None):
        self._parts: List[bytes] = []
        self._msg_type = msg_type
    
    def write_byte(self, val: int) -> "MessageWriter":
        """Write a single byte."""
        self._parts.append(struct.pack("!B", val))
        return self
    
    def write_int16(self, val: int) -> "MessageWriter":
        """Write a 16-bit signed integer."""
        self._parts.append(struct.pack("!h", val))
        return self
    
    def write_int32(self, val: int) -> "MessageWriter":
        """Write a 32-bit signed integer."""
        self._parts.append(struct.pack("!i", val))
        return self
    
    def write_string(self, val: str) -> "MessageWriter":
        """Write a null-terminated string."""
        self._parts.append(val.encode("utf-8") + b'\x00')
        return self
    
    def write_bytes(self, val: bytes) -> "MessageWriter":
        """Write raw bytes."""
        self._parts.append(val)
        return self
    
    def build(self) -> bytes:
        """Build the complete message with type byte and length prefix."""
        payload = b''.join(self._parts)
        # Length includes itself (4 bytes) but not the type byte
        length = len(payload) + 4
        
        if self._msg_type is not None:
            return struct.pack("!cI", bytes([self._msg_type]), length) + payload
        else:
            # For messages without type byte (like startup response)
            return struct.pack("!I", length) + payload


def parse_startup_message(data: bytes) -> StartupMessage:
    """
    Parse a startup message from the client.
    
    Startup message format:
    - 4 bytes: length (including self)
    - 4 bytes: protocol version (2 bytes major, 2 bytes minor)
    - Null-terminated key-value pairs
    - Final null terminator
    
    Args:
        data: Raw message bytes (without length prefix if already stripped)
        
    Returns:
        Parsed StartupMessage
    """
    reader = MessageReader(data)
    
    # Protocol version
    version = reader.read_int32()
    major = (version >> 16) & 0xFFFF
    minor = version & 0xFFFF
    
    # Check for special cases
    if major == 1234:
        # SSL or Cancel request
        if minor == 5679:
            raise ProtocolError("SSL request - not supported")
        elif minor == 5678:
            raise ProtocolError("Cancel request")
        elif minor == 5680:
            raise ProtocolError("GSS encryption request - not supported")
    
    # Parse parameters
    params = {}
    while reader.remaining > 1:
        key = reader.read_string()
        if not key:  # Empty key = end of parameters
            break
        value = reader.read_string()
        params[key] = value
    
    return StartupMessage(
        protocol_version=(major, minor),
        parameters=params
    )


def build_authentication_ok() -> bytes:
    """Build AuthenticationOk message."""
    return (
        MessageWriter(MessageType.AUTHENTICATION)
        .write_int32(AuthType.OK)
        .build()
    )


def build_authentication_md5(salt: bytes) -> bytes:
    """Build AuthenticationMD5Password message with 4-byte salt."""
    assert len(salt) == 4, "MD5 salt must be 4 bytes"
    return (
        MessageWriter(MessageType.AUTHENTICATION)
        .write_int32(AuthType.MD5_PASSWORD)
        .write_bytes(salt)
        .build()
    )


def build_authentication_cleartext() -> bytes:
    """Build AuthenticationCleartextPassword message."""
    return (
        MessageWriter(MessageType.AUTHENTICATION)
        .write_int32(AuthType.CLEARTEXT_PASSWORD)
        .build()
    )


def build_parameter_status(name: str, value: str) -> bytes:
    """Build ParameterStatus message."""
    return (
        MessageWriter(MessageType.PARAMETER_STATUS)
        .write_string(name)
        .write_string(value)
        .build()
    )


def build_backend_key_data(process_id: int, secret_key: int) -> bytes:
    """Build BackendKeyData message."""
    return (
        MessageWriter(MessageType.BACKEND_KEY_DATA)
        .write_int32(process_id)
        .write_int32(secret_key)
        .build()
    )


def build_ready_for_query(status: TransactionStatus = TransactionStatus.IDLE) -> bytes:
    """Build ReadyForQuery message."""
    return (
        MessageWriter(MessageType.READY_FOR_QUERY)
        .write_byte(status)
        .build()
    )


def build_row_description(columns: List[ColumnDescription]) -> bytes:
    """Build RowDescription message."""
    writer = MessageWriter(MessageType.ROW_DESCRIPTION)
    writer.write_int16(len(columns))
    
    for col in columns:
        writer.write_string(col.name)
        writer.write_int32(col.table_oid)
        writer.write_int16(col.column_attr_num)
        writer.write_int32(col.type_oid)
        writer.write_int16(col.type_size)
        writer.write_int32(col.type_modifier)
        writer.write_int16(col.format_code)
    
    return writer.build()


def build_data_row(values: List[Optional[bytes]]) -> bytes:
    """
    Build DataRow message.
    
    Args:
        values: List of column values as bytes. None = NULL.
    """
    writer = MessageWriter(MessageType.DATA_ROW)
    writer.write_int16(len(values))
    
    for val in values:
        if val is None:
            writer.write_int32(-1)  # NULL indicator
        else:
            writer.write_int32(len(val))
            writer.write_bytes(val)
    
    return writer.build()


def build_command_complete(tag: str) -> bytes:
    """
    Build CommandComplete message.
    
    Tag examples: "SELECT 5", "INSERT 0 1", "UPDATE 3"
    """
    return (
        MessageWriter(MessageType.COMMAND_COMPLETE)
        .write_string(tag)
        .build()
    )


def build_error_response(
    severity: str = "ERROR",
    code: str = "XX000",
    message: str = "",
    detail: str = None,
    hint: str = None,
    position: int = None,
) -> bytes:
    """
    Build ErrorResponse message.
    
    SQL state codes: https://www.postgresql.org/docs/current/errcodes-appendix.html
    """
    writer = MessageWriter(MessageType.ERROR_RESPONSE)
    
    # Severity (S or V for verbose)
    writer.write_byte(ord('S'))
    writer.write_string(severity)
    
    # Severity (non-localized, V)
    writer.write_byte(ord('V'))
    writer.write_string(severity)
    
    # SQL state code (C)
    writer.write_byte(ord('C'))
    writer.write_string(code)
    
    # Message (M)
    writer.write_byte(ord('M'))
    writer.write_string(message)
    
    if detail:
        writer.write_byte(ord('D'))
        writer.write_string(detail)
    
    if hint:
        writer.write_byte(ord('H'))
        writer.write_string(hint)
    
    if position:
        writer.write_byte(ord('P'))
        writer.write_string(str(position))
    
    # Terminator
    writer.write_byte(0)
    
    return writer.build()


def build_notice_response(severity: str, message: str) -> bytes:
    """Build NoticeResponse message (warnings)."""
    writer = MessageWriter(MessageType.NOTICE_RESPONSE)
    
    writer.write_byte(ord('S'))
    writer.write_string(severity)
    writer.write_byte(ord('M'))
    writer.write_string(message)
    writer.write_byte(0)
    
    return writer.build()


def build_empty_query_response() -> bytes:
    """Build EmptyQueryResponse message."""
    return MessageWriter(MessageType.EMPTY_QUERY_RESPONSE).build()


def build_parse_complete() -> bytes:
    """Build ParseComplete message."""
    return MessageWriter(MessageType.PARSE_COMPLETE).build()


def build_bind_complete() -> bytes:
    """Build BindComplete message."""
    return MessageWriter(MessageType.BIND_COMPLETE).build()


def build_close_complete() -> bytes:
    """Build CloseComplete message."""
    return MessageWriter(MessageType.CLOSE_COMPLETE).build()


def build_no_data() -> bytes:
    """Build NoData message."""
    return MessageWriter(MessageType.NO_DATA).build()
