"""
Change Data Capture (CDC) - Stream changes from data sources

This module provides real-time change streaming functionality,
allowing users to receive incremental updates from data sources
instead of polling full datasets.

Supported Providers:
- ServiceNow (polling-based)
- Salesforce (polling-based)
- Jira (polling-based)
- PostgreSQL (WAL-based logical replication)

State Backends:
- SQLite (local file-based)
- Redis (distributed)
- Memory (ephemeral, for testing)
"""

from waveql.cdc.models import Change, ChangeType, ChangeStream
from waveql.cdc.stream import CDCStream, CDCConfig
from waveql.cdc.providers import (
    BaseCDCProvider,
    ServiceNowCDCProvider,
    SalesforceCDCProvider,
    JiraCDCProvider,
)
from waveql.cdc.state import (
    StateBackend,
    StreamPosition,
    SQLiteStateBackend,
    MemoryStateBackend,
    RedisStateBackend,
    create_state_backend,
)

# PostgreSQL CDC provider (optional - requires psycopg2)
try:
    from waveql.cdc.postgres import PostgresCDCProvider
except ImportError:
    PostgresCDCProvider = None

__all__ = [
    "Change",
    "ChangeType",
    "ChangeStream",
    "CDCStream",
    "CDCConfig",
    "BaseCDCProvider",
    "ServiceNowCDCProvider",
    "SalesforceCDCProvider",
    "JiraCDCProvider",
    "PostgresCDCProvider",
    # State backends
    "StateBackend",
    "StreamPosition",
    "SQLiteStateBackend",
    "MemoryStateBackend",
    "RedisStateBackend",
    "create_state_backend",
]

