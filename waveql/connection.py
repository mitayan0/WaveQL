"""
WaveQL Connection - DB-API 2.0 compliant connection class
"""

from __future__ import annotations
import logging
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

import duckdb

from waveql.exceptions import ConnectionError, AdapterError
from waveql.schema_cache import SchemaCache
from waveql.auth.manager import AuthManager
from waveql.connection_base import ConnectionMixin
from waveql.cache import QueryCache, CacheConfig, CacheStats, create_cache

if TYPE_CHECKING:
    from waveql.cursor import WaveQLCursor
    from waveql.adapters.base import BaseAdapter
    from waveql.materialized_view.manager import MaterializedViewManager

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
        # Cache configuration
        cache_ttl: float = None,
        cache_config: CacheConfig = None,
        enable_cache: bool = True,
        **kwargs
    ):
        # Parse connection string if provided
        if connection_string:
            parsed = self.parse_connection_string(connection_string)
            adapter = adapter or parsed.get("adapter")
            host = host or parsed.get("host")
            # Use URL-embedded credentials if not explicitly provided
            username = username or parsed.get("username")
            password = password or parsed.get("password")
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
        
        # Initialize query result cache
        self._cache = self._init_cache(cache_config, cache_ttl, enable_cache)
        
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
        
        # Initialize materialized view manager (lazy loaded)
        self._view_manager: Optional["MaterializedViewManager"] = None
        
        logger.debug(
            "WaveQLConnection created: adapter=%s, host=%s, cache=%s",
            adapter, host, "enabled" if self._cache.config.enabled else "disabled"
        )
    
    def _init_cache(
        self,
        cache_config: CacheConfig = None,
        cache_ttl: float = None,
        enable_cache: bool = True,
    ) -> QueryCache:
        """
        Initialize the query result cache.
        
        Args:
            cache_config: Full cache configuration (takes precedence)
            cache_ttl: Simple TTL in seconds (used if cache_config not provided)
            enable_cache: Whether caching is enabled
            
        Returns:
            Configured QueryCache instance
        """
        if cache_config is not None:
            return QueryCache(cache_config)
        
        if cache_ttl is not None:
            return create_cache(enabled=enable_cache, ttl=cache_ttl)
        
        # Default: enabled with 5 minute TTL
        return create_cache(enabled=enable_cache, ttl=300.0)
    
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
    
    # =========================================================================
    # Materialized Views
    # =========================================================================
    
    @property
    def view_manager(self) -> "MaterializedViewManager":
        """Get the materialized view manager (lazy initialized)."""
        if self._view_manager is None:
            from waveql.materialized_view.manager import MaterializedViewManager
            self._view_manager = MaterializedViewManager(self)
        return self._view_manager
    
    def create_materialized_view(
        self,
        name: str,
        query: str,
        refresh_strategy: str = "full",
        sync_column: str = None,
        if_not_exists: bool = False,
    ) -> None:
        """
        Create a materialized view.
        
        Args:
            name: Unique name for the view
            query: SQL query defining the view content
            refresh_strategy: 'full' or 'incremental'
            sync_column: Column for incremental sync (auto-detected if not provided)
            if_not_exists: If True, don't error if view already exists
            
        Example:
            conn.create_materialized_view(
                name="incident_cache",
                query="SELECT * FROM servicenow.incident WHERE state != 7",
                refresh_strategy="incremental",
                sync_column="sys_updated_on"
            )
        """
        self.view_manager.create(
            name=name,
            query=query,
            refresh_strategy=refresh_strategy,
            sync_column=sync_column,
            if_not_exists=if_not_exists,
        )
    
    def refresh_materialized_view(
        self,
        name: str,
        mode: str = None,
        force_full: bool = False,
    ) -> dict:
        """
        Refresh a materialized view.
        
        Args:
            name: View name
            mode: Override refresh mode ('full' or 'incremental')
            force_full: If True, always do full refresh
            
        Returns:
            Dict with refresh statistics
        """
        stats = self.view_manager.refresh(name, mode=mode, force_full=force_full)
        return stats.to_dict()
    
    def drop_materialized_view(self, name: str, if_exists: bool = False) -> bool:
        """
        Drop a materialized view.
        
        Args:
            name: View name
            if_exists: If True, don't error if view doesn't exist
            
        Returns:
            True if dropped, False if not found
        """
        return self.view_manager.drop(name, if_exists=if_exists)
    
    def list_materialized_views(self) -> list:
        """
        List all materialized views.
        
        Returns:
            List of view info dictionaries with name, query, row_count, etc.
        """
        return self.view_manager.list_all()
    
    def get_materialized_view(self, name: str) -> Optional[dict]:
        """
        Get information about a materialized view.
        
        Args:
            name: View name
            
        Returns:
            View info dict or None if not found
        """
        info = self.view_manager.get(name)
        return info.to_dict() if info else None
    
    # =========================================================================
    # Change Data Capture (CDC)
    # =========================================================================
    
    def stream_changes(
        self,
        table: str,
        since: "datetime" = None,
        poll_interval: float = 5.0,
        batch_size: int = 100,
    ):
        """
        Create a CDC stream to watch for changes.
        
        Args:
            table: Table to watch (e.g., 'servicenow.incident')
            since: Only get changes after this timestamp
            poll_interval: Seconds between polling
            batch_size: Max changes per batch
            
        Returns:
            CDCStream object that can be used with 'async for'
            
        Example:
            ```python
            stream = conn.stream_changes("incident", since=last_sync)
            async for change in stream:
                print(f"{change.operation}: {change.key}")
            ```
        """
        from waveql.cdc.stream import CDCStream
        from waveql.cdc.models import CDCConfig
        
        config = CDCConfig(
            poll_interval=poll_interval,
            batch_size=batch_size,
            since=since,
        )
        
        return CDCStream(self, table, config)
    
    def stream_changes_wal(
        self,
        table: str,
        slot_name: str = "waveql_cdc",
        output_plugin: str = "wal2json",
        create_slot: bool = True,
    ):
        """
        Create a PostgreSQL WAL-based CDC stream for zero-latency change detection.
        
        This uses PostgreSQL's Logical Replication to stream changes directly
        from the Write-Ahead Log, providing millisecond-level latency without
        any polling overhead.
        
        Requirements:
        - PostgreSQL 9.4+ with wal_level=logical
        - User with REPLICATION privilege
        - wal2json or test_decoding output plugin
        
        Args:
            table: Table to watch (e.g., 'public.users' or just 'users')
            slot_name: Logical replication slot name
            output_plugin: 'wal2json' (recommended) or 'test_decoding'
            create_slot: Auto-create slot if it doesn't exist
            
        Returns:
            PostgresCDCProvider that can be used with 'async for'
            
        Example:
            ```python
            # Connect to PostgreSQL
            conn = waveql.connect("postgresql://user:pass@localhost/db")
            
            # Stream changes in real-time (zero polling!)
            provider = conn.stream_changes_wal("users")
            async for change in provider.stream_changes("users"):
                print(f"{change.operation}: {change.data}")
            ```
            
        Note:
            Unlike stream_changes() which uses polling, this method provides
            true push-based streaming with guaranteed delivery. Changes are
            never missed even if your application restarts.
        """
        from waveql.cdc.postgres import PostgresCDCProvider
        
        adapter = self.get_adapter("default")
        if adapter is None:
            raise ValueError("No adapter configured for this connection")
        
        # Build connection string - try to get it from adapter or use our stored host
        connection_string = None
        if hasattr(adapter, '_connection_string'):
            connection_string = adapter._connection_string
        elif hasattr(adapter, '_host') and adapter._host:
            connection_string = adapter._host
        elif self._host:
            # If host looks like a connection string (starts with postgres://)
            if self._host.startswith("postgres"):
                connection_string = self._host
            else:
                # Try to build one from components
                # This is a fallback for when we have separate host/user/pass
                connection_string = self._host
        
        return PostgresCDCProvider(
            adapter=adapter,
            connection_string=connection_string,
            slot_name=slot_name,
            output_plugin=output_plugin,
            create_slot=create_slot,
        )
    
    async def get_changes(
        self,
        table: str,
        since: "datetime" = None,
        limit: int = 100,
    ) -> list:
        """
        Get all changes since a timestamp (one-shot, not streaming).
        
        Args:
            table: Table to get changes from
            since: Only get changes after this timestamp
            limit: Maximum number of changes to return
            
        Returns:
            List of Change objects
            
        Example:
            ```python
            changes = await conn.get_changes("incident", since=last_sync)
            for change in changes:
                print(f"{change.operation}: {change.data}")
            ```
        """
        from waveql.cdc.stream import CDCStream
        from waveql.cdc.models import CDCConfig
        
        config = CDCConfig(
            batch_size=limit,
            since=since,
        )
        
        stream = CDCStream(self, table, config)
        return await stream.get_changes(since)
    
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
            # Close view manager if initialized
            if self._view_manager is not None:
                self._view_manager.close()
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
    
    # =========================================================================
    # Query Result Cache
    # =========================================================================
    
    @property
    def cache(self) -> QueryCache:
        """Access query result cache."""
        return self._cache
    
    @property
    def cache_stats(self) -> CacheStats:
        """
        Get cache statistics.
        
        Returns:
            CacheStats with hits, misses, evictions, size, and hit rate
        """
        return self._cache.stats
    
    def invalidate_cache(
        self,
        adapter: str = None,
        table: str = None,
    ) -> int:
        """
        Invalidate cache entries.
        
        Args:
            adapter: Invalidate entries for this adapter only
            table: Invalidate entries for this table only
            
        Returns:
            Number of entries invalidated
            
        Examples:
            # Clear all cache
            conn.invalidate_cache()
            
            # Clear only ServiceNow entries
            conn.invalidate_cache(adapter="servicenow")
            
            # Clear only incident table entries
            conn.invalidate_cache(table="incident")
        """
        return self._cache.invalidate(adapter=adapter, table=table)
    
    def set_cache_ttl(self, adapter: str, ttl: float) -> None:
        """
        Set TTL for a specific adapter.
        
        Args:
            adapter: Adapter name (e.g., "servicenow")
            ttl: TTL in seconds
        """
        self._cache.config.adapter_ttl[adapter] = ttl
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "closed" if self._closed else "open"
        return f"<WaveQLConnection adapter={self._adapter_name} host={self._host} status={status}>"
