"""
WaveQL Async Connection - Asynchronous DB-API 2.0 style connection class

Provides async/await support for querying APIs with SQL.
"""

from __future__ import annotations
import logging
from typing import Dict, Optional, TYPE_CHECKING

import duckdb

from waveql.exceptions import ConnectionError, AdapterError
from waveql.schema_cache import SchemaCache
from waveql.auth.manager import AuthManager
from waveql.async_cursor import AsyncWaveQLCursor
from waveql.connection_base import ConnectionMixin
from waveql.cache import QueryCache, CacheConfig, CacheStats, create_cache

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class AsyncWaveQLConnection(ConnectionMixin):
    """
    Async version of WaveQLConnection.
    
    Provides async/await support for:
    - Query execution
    - Connection lifecycle management
    - Adapter operations
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
        # Cache configuration
        cache_ttl: float = None,
        cache_config: CacheConfig = None,
        enable_cache: bool = True,
        **kwargs
    ):
        # Parse connection string using shared mixin method
        if connection_string:
            parsed = self.parse_connection_string(connection_string)
            adapter = adapter or parsed.get("adapter")
            host = host or parsed.get("host")
            # Use URL-embedded credentials if not explicitly provided
            username = username or parsed.get("username")
            password = password or parsed.get("password")
            kwargs = {**parsed.get("params", {}), **kwargs}
        
        self._adapter_name = adapter
        self._host = host
        self._kwargs = kwargs
        self._closed = False
        self._duckdb = duckdb.connect(":memory:")
        self._schema_cache = SchemaCache()
        
        # Initialize query result cache
        self._cache = self._init_cache(cache_config, cache_ttl, enable_cache)
        
        # Extract OAuth parameters and create auth manager using mixin
        oauth_params = self.extract_oauth_params(**kwargs)
        self._auth_manager = self.create_auth_manager_from_params(
            username=username,
            password=password,
            api_key=api_key,
            oauth_token=oauth_token,
            **oauth_params
        )
        
        self._adapters: Dict[str, BaseAdapter] = {}
        if adapter:
            self._init_default_adapter(adapter, host, **kwargs)
        
        logger.debug(
            "AsyncWaveQLConnection created: adapter=%s, host=%s, cache=%s",
            adapter, host, "enabled" if self._cache.config.enabled else "disabled"
        )
    
    def _init_cache(
        self,
        cache_config: CacheConfig = None,
        cache_ttl: float = None,
        enable_cache: bool = True,
    ) -> QueryCache:
        """Initialize the query result cache."""
        if cache_config is not None:
            return QueryCache(cache_config)
        if cache_ttl is not None:
            return create_cache(enabled=enable_cache, ttl=cache_ttl)
        return create_cache(enabled=enable_cache, ttl=300.0)

    def _init_default_adapter(self, adapter_name: str, host: str, **kwargs):
        """Initialize the default adapter."""
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
        logger.debug("Initialized %s adapter for host: %s", adapter_name, host)

    async def cursor(self) -> AsyncWaveQLCursor:
        """Create a new async cursor for executing queries."""
        if self._closed:
            raise ConnectionError("Connection is closed")
        return AsyncWaveQLCursor(self)

    def get_adapter(self, name: str = "default") -> Optional["BaseAdapter"]:
        """Get a registered adapter by name."""
        return self._adapters.get(name)

    def register_adapter(self, name: str, adapter: "BaseAdapter"):
        """Register an adapter with a name for use in queries."""
        adapter.set_auth_manager(self._auth_manager)
        adapter.set_schema_cache(self._schema_cache)
        self._adapters[name] = adapter

    async def close(self):
        """Close the connection and release resources."""
        if not self._closed:
            self._duckdb.close()
            self._schema_cache.close()
            self._closed = True
            logger.debug("AsyncWaveQLConnection closed")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
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
    
    # =========================================================================
    # Query Result Cache
    # =========================================================================
    
    @property
    def cache(self) -> QueryCache:
        """Access query result cache."""
        return self._cache
    
    @property
    def cache_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._cache.stats
    
    def invalidate_cache(
        self,
        adapter: str = None,
        table: str = None,
    ) -> int:
        """Invalidate cache entries."""
        return self._cache.invalidate(adapter=adapter, table=table)
    
    def set_cache_ttl(self, adapter: str, ttl: float) -> None:
        """Set TTL for a specific adapter."""
        self._cache.config.adapter_ttl[adapter] = ttl

    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "closed" if self._closed else "open"
        return f"<AsyncWaveQLConnection adapter={self._adapter_name} host={self._host} status={status}>"
