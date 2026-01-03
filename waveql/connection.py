"""
WaveQL Connection - DB-API 2.0 compliant connection class
"""

from __future__ import annotations
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

import duckdb

from waveql.exceptions import ConnectionError, AdapterError
from waveql.schema_cache import SchemaCache
from waveql.auth.manager import AuthManager
from waveql.connection_base import ConnectionMixin

if TYPE_CHECKING:
    from waveql.cursor import WaveQLCursor
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class WaveQLConnection(ConnectionMixin):
    """
    DB-API 2.0 compliant connection wrapping DuckDB with adapter support.
    
    Provides:
    - Virtual table registration for adapters
    - Schema caching
    - Authentication management
    - Transaction support (where applicable)
    """
    
    def __init__(
        self,
        connection_string: str = None,
        adapter: str = None,
        host: str = None,
        username: str = None,
        password: str = None,
        api_key: str = None,
        oauth_token: str = None,
        **kwargs
    ):
        # Parse connection string if provided
        if connection_string:
            parsed = self.parse_connection_string(connection_string)
            adapter = adapter or parsed.get("adapter")
            host = host or parsed.get("host")
            # Merge parsed kwargs
            kwargs = {**parsed.get("params", {}), **kwargs}
        
        self._adapter_name = adapter
        self._host = host
        self._kwargs = kwargs
        self._closed = False
        
        # Initialize DuckDB (in-memory by default)
        self._duckdb = duckdb.connect(":memory:")
        
        # Initialize schema cache
        self._schema_cache = SchemaCache()
        
        # Extract OAuth parameters and create auth manager
        oauth_params = self.extract_oauth_params(**kwargs)
        self._auth_manager = self.create_auth_manager_from_params(
            username=username,
            password=password,
            api_key=api_key,
            oauth_token=oauth_token,
            **oauth_params
        )
        
        # Registered adapters for this connection
        self._adapters: Dict[str, BaseAdapter] = {}
        
        # If adapter specified, initialize it
        if adapter:
            self._init_default_adapter(adapter, host, **kwargs)
        
        logger.debug("WaveQLConnection created: adapter=%s, host=%s", adapter, host)
    
    def _init_default_adapter(self, adapter_name: str, host: str, **kwargs):
        """Initialize the default adapter based on connection parameters."""
        from waveql.adapters import get_adapter_class
        
        adapter_class = get_adapter_class(adapter_name)
        if not adapter_class:
            raise AdapterError(f"Unknown adapter: {adapter_name}")
        
        adapter = adapter_class(
            host=host,
            auth_manager=self._auth_manager,
            schema_cache=self._schema_cache,
            **kwargs
        )
        self._adapters["default"] = adapter
    
    def cursor(self) -> "WaveQLCursor":
        """Create a new cursor for executing queries."""
        from waveql.cursor import WaveQLCursor
        
        if self._closed:
            raise ConnectionError("Connection is closed")
        
        return WaveQLCursor(self)
    
    def register_adapter(self, name: str, adapter: "BaseAdapter"):
        """
        Register an adapter with a name for use in queries.
        
        Args:
            name: Schema/prefix name for the adapter (e.g., "sales" for sales.Account)
            adapter: Adapter instance
        """
        adapter.set_auth_manager(self._auth_manager)
        adapter.set_schema_cache(self._schema_cache)
        self._adapters[name] = adapter
    
    def get_adapter(self, name: str = "default") -> Optional["BaseAdapter"]:
        """Get a registered adapter by name."""
        return self._adapters.get(name)
    
    def commit(self):
        """Commit current transaction (no-op for most API adapters)."""
        pass
    
    def rollback(self):
        """Rollback current transaction (no-op for most API adapters)."""
        pass
    
    def ping(self) -> bool:
        """
        Test if the connection is alive.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        if self._closed:
            return False
        try:
            self._duckdb.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    @property
    def is_closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed
    
    def close(self):
        """Close the connection and release resources."""
        if not self._closed:
            self._duckdb.close()
            self._schema_cache.close()
            self._closed = True
            logger.debug("WaveQLConnection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    @property
    def duckdb(self):
        """Access underlying DuckDB connection."""
        return self._duckdb
    
    @property
    def schema_cache(self) -> SchemaCache:
        """Access schema cache."""
        return self._schema_cache
    
    @property
    def auth_manager(self) -> AuthManager:
        """Access auth manager."""
        return self._auth_manager
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "closed" if self._closed else "open"
        return f"<WaveQLConnection adapter={self._adapter_name} host={self._host} status={status}>"
