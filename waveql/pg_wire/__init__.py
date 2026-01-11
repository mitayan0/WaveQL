"""
WaveQL PostgreSQL Wire Protocol Server

Implements PostgreSQL's wire protocol (version 3.0) to enable BI tools like
Tableau, PowerBI, and DBeaver to connect to WaveQL as if it were a standard
PostgreSQL database.

Key Features:
- TCP server on port 5432 (default)
- pg_catalog emulation for schema introspection
- Binary tuple encoding for efficient data transfer
- Authentication support (trust, md5, scram-sha-256 placeholder)

Usage:
    from waveql.pg_wire import PGWireServer
    
    server = PGWireServer(waveql_connection)
    await server.serve(host="0.0.0.0", port=5432)
"""

from waveql.pg_wire.server import PGWireServer, run_server
from waveql.pg_wire.protocol import (
    MessageType,
    AuthType,
    FormatCode,
)
from waveql.pg_wire.catalog import PGCatalogEmulator
from waveql.pg_wire.type_mapping import (
    arrow_to_pg_oid,
    pg_oid_to_arrow,
    PG_TYPES,
)

__all__ = [
    "PGWireServer",
    "run_server",
    "MessageType",
    "AuthType",
    "FormatCode",
    "PGCatalogEmulator",
    "arrow_to_pg_oid",
    "pg_oid_to_arrow",
    "PG_TYPES",
]
