"""
Base Adapter - Abstract base class for all data source adapters

Adapters are responsible for:
1. Fetching data from the source (with predicate pushdown)
2. Inserting/Updating/Deleting records
3. Schema discovery
4. Converting data to Arrow format
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa

if TYPE_CHECKING:
    from waveql.auth.manager import AuthManager
    from waveql.schema_cache import SchemaCache, ColumnInfo
    from waveql.query_planner import Predicate
    import requests
    import httpx


class BaseAdapter(ABC):
    """
    Abstract base class for WaveQL adapters.
    
    Subclasses must implement:
    - fetch(): Retrieve data with optional filtering
    - get_schema(): Discover table schema
    
    Optional overrides for CRUD:
    - insert()
    - update()
    - delete()
    - execute_batch()
    
    Connection Pooling:
    - Set use_connection_pool=True to use the global connection pool
    - This enables connection reuse across multiple requests
    """
    
    # Adapter metadata
    adapter_name: str = "base"
    supports_predicate_pushdown: bool = True
    supports_insert: bool = False
    supports_update: bool = False
    supports_delete: bool = False
    supports_batch: bool = False
    
    def __init__(
        self,
        host: str = None,
        auth_manager: "AuthManager" = None,
        schema_cache: "SchemaCache" = None,
        max_retries: int = 5,
        retry_base_delay: float = 1.0,
        use_connection_pool: bool = True,
        **kwargs
    ):
        self._host = host
        self._auth_manager = auth_manager
        self._schema_cache = schema_cache
        self._config = kwargs
        self._use_connection_pool = use_connection_pool
        
        # Extract host for connection pool key
        self._pool_host = self._extract_host(host) if host else "default"
        
        # Initialize rate limiter for automatic retry on rate limits
        from waveql.utils.rate_limiter import RateLimiter
        self._rate_limiter = RateLimiter(
            max_retries=max_retries,
            base_delay=retry_base_delay,
        )
        
        # Lazy-loaded local session (when not using pool)
        self._local_session: Optional["requests.Session"] = None
    
    def _extract_host(self, url: str) -> str:
        """Extract hostname from URL for pool keying."""
        if not url:
            return "default"
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        parsed = urlparse(url)
        return parsed.netloc or parsed.path.split("/")[0]
    
    @contextmanager
    def _get_session(self) -> "requests.Session":
        """
        Get an HTTP session from the connection pool or create a local one.
        
        Usage:
            with self._get_session() as session:
                response = session.get(url)
        
        Returns:
            Context manager yielding a requests.Session
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_sync_pool
            pool = get_sync_pool()
            with pool.get_session(self._pool_host) as session:
                yield session
        else:
            # Use local session (backward compatible)
            if self._local_session is None:
                import requests
                self._local_session = requests.Session()
            yield self._local_session
    
    def _get_session_direct(self) -> "requests.Session":
        """
        Get an HTTP session directly (without context manager).
        
        Use this when you need to keep the session across multiple operations.
        Remember to call _return_session() when done if using the pool.
        
        Returns:
            requests.Session instance
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_sync_pool
            pool = get_sync_pool()
            return pool.get_session_direct(self._pool_host)
        else:
            if self._local_session is None:
                import requests
                self._local_session = requests.Session()
            return self._local_session
    
    def _return_session(self, session: "requests.Session"):
        """
        Return a session to the pool (for use with _get_session_direct).
        
        Args:
            session: The session to return
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_sync_pool
            pool = get_sync_pool()
            pool.return_session(self._pool_host, session)
    
    def _get_async_client(self) -> "httpx.AsyncClient":
        """
        Get an async HTTP client from the connection pool.
        
        The async pool shares clients per host, so this returns
        a shared client that should NOT be closed by the caller.
        
        Returns:
            httpx.AsyncClient instance
        """
        if self._use_connection_pool:
            from waveql.utils.connection_pool import get_async_pool
            pool = get_async_pool()
            return pool.get_client(self._pool_host)
        else:
            # Create a new client (caller manages lifecycle)
            import httpx
            return httpx.AsyncClient()
    
    def set_auth_manager(self, auth_manager: "AuthManager"):
        """Set the authentication manager."""
        self._auth_manager = auth_manager
    
    def set_schema_cache(self, schema_cache: "SchemaCache"):
        """Set the schema cache."""
        self._schema_cache = schema_cache
    
    @abstractmethod
    def fetch(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List["Aggregate"] = None,
    ) -> pa.Table:
        """
        Fetch data from the source.
        
        Args:
            table: Table/resource name
            columns: Columns to retrieve (None = all)
            predicates: WHERE clause predicates for pushdown
            limit: Max rows to return
            offset: Row offset
            order_by: List of (column, direction) tuples
            
        Returns:
            PyArrow Table with results
        """
        pass
    
    @abstractmethod
    def get_schema(self, table: str) -> List["ColumnInfo"]:
        """Discover schema for a table."""
        pass
    
    async def fetch_async(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List["Aggregate"] = None,
    ) -> pa.Table:
        """Fetch data from the source (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support fetch_async")

    async def get_schema_async(self, table: str) -> List["ColumnInfo"]:
        """Discover schema for a table (async)."""
        raise NotImplementedError(f"{self.adapter_name} does not support get_schema_async")
    
    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """
        Insert a record.
        
        Args:
            table: Table name
            values: Column-value pairs
            parameters: Additional parameters
            
        Returns:
            Number of rows inserted
        """
        raise NotImplementedError(f"{self.adapter_name} does not support INSERT")
    
    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """
        Update records.
        
        Args:
            table: Table name
            values: Column-value pairs to update
            predicates: WHERE conditions
            parameters: Additional parameters
            
        Returns:
            Number of rows updated
        """
        raise NotImplementedError(f"{self.adapter_name} does not support UPDATE")
    
    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """
        Delete records.
        
        Args:
            table: Table name
            predicates: WHERE conditions
            parameters: Additional parameters
            
        Returns:
            Number of rows deleted
        """
        raise NotImplementedError(f"{self.adapter_name} does not support DELETE")
    
    def execute_batch(
        self,
        query_info,
        seq_of_parameters: Sequence[Sequence],
    ) -> int:
        """
        Execute batch operation.
        
        Args:
            query_info: Parsed query info
            seq_of_parameters: Sequence of parameter sets
            
        Returns:
            Total rows affected
        """
        raise NotImplementedError(f"{self.adapter_name} does not support batch operations")
    
    def list_tables(self) -> List[str]:
        """
        List available tables/resources.
        
        Returns:
            List of table names
        """
        return []
    
    def _get_cached_schema(self, table: str) -> Optional[List["ColumnInfo"]]:
        """Get schema from cache if available."""
        if self._schema_cache:
            cached = self._schema_cache.get(self.adapter_name, table)
            if cached:
                return cached.columns
        return None
    
    def _cache_schema(self, table: str, columns: List["ColumnInfo"], ttl: int = 3600):
        """Cache discovered schema."""
        if self._schema_cache:
            self._schema_cache.set(self.adapter_name, table, columns, ttl)
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers from auth manager."""
        if self._auth_manager:
            return self._auth_manager.get_headers()
        return {}

    async def _get_auth_headers_async(self) -> Dict[str, str]:
        """Get authentication headers from auth manager (async)."""
        if self._auth_manager:
            return await self._auth_manager.get_headers_async()
        return {}
    
    def _request_with_retry(self, request_func, *args, **kwargs) -> Any:
        """
        Execute an HTTP request with automatic retry on rate limits.
        
        Args:
            request_func: Function to execute (e.g., session.get, session.post)
            *args, **kwargs: Arguments for the request function
            
        Returns:
            Response from the request
            
        Raises:
            Original exception if all retries fail
        """
        return self._rate_limiter.execute_with_retry(request_func, *args, **kwargs)

