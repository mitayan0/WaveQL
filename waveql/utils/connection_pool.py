"""
Connection Pool - HTTP Connection Pooling for WaveQL Adapters

Provides thread-safe connection pooling for both synchronous (requests)
and asynchronous (httpx) HTTP clients.

Features:
- Reusable HTTP sessions with keep-alive
- Configurable pool size and connection limits
- Per-host connection limits
- Automatic connection recycling
- Thread-safe for sync operations
- Async-safe for async operations
"""

from __future__ import annotations
import atexit
import threading
import time
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, field
from queue import Queue, Empty
from typing import Any, Dict, Optional, TYPE_CHECKING

import requests
import httpx

if TYPE_CHECKING:
    pass


@dataclass
class PoolConfig:
    """Configuration for connection pool."""
    
    # Maximum number of connections per host
    max_connections_per_host: int = 10
    
    # Maximum total connections in the pool
    max_total_connections: int = 100
    
    # Connection timeout in seconds
    connect_timeout: float = 10.0
    
    # Read timeout in seconds
    read_timeout: float = 30.0
    
    # Maximum time a connection can be idle before being recycled (seconds)
    max_idle_time: float = 300.0
    
    # Keep-alive settings
    keep_alive: bool = True
    
    # HTTP/2 support for async client
    http2: bool = True
    
    # Retry configuration
    max_retries: int = 3
    retry_backoff_factor: float = 0.5
    
    # SSL verification
    verify_ssl: bool = True


@dataclass
class PooledConnection:
    """Wrapper for a pooled connection with metadata."""
    
    session: Any  # requests.Session or httpx.AsyncClient
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    use_count: int = 0
    host: str = ""
    
    def touch(self):
        """Update last used timestamp."""
        self.last_used = time.time()
        self.use_count += 1
    
    def is_expired(self, max_idle_time: float) -> bool:
        """Check if connection has been idle too long."""
        return (time.time() - self.last_used) > max_idle_time


class SyncConnectionPool:
    """
    Thread-safe connection pool for synchronous HTTP requests.
    
    Uses requests.Session for connection reuse and keep-alive.
    """
    
    _instance: Optional["SyncConnectionPool"] = None
    _lock = threading.Lock()
    
    def __new__(cls, config: Optional[PoolConfig] = None):
        """Singleton pattern for global pool access."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance
    
    def __init__(self, config: Optional[PoolConfig] = None):
        if self._initialized:
            return
        
        self._config = config or PoolConfig()
        self._pools: Dict[str, Queue] = {}  # host -> Queue[PooledConnection]
        self._pool_locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()
        self._total_connections = 0
        self._closed = False
        
        # Register cleanup on exit
        atexit.register(self.close)
        
        self._initialized = True
    
    def _get_pool_for_host(self, host: str) -> Queue:
        """Get or create a pool for a specific host."""
        if host not in self._pools:
            with self._global_lock:
                if host not in self._pools:
                    self._pools[host] = Queue(maxsize=self._config.max_connections_per_host)
                    self._pool_locks[host] = threading.Lock()
        return self._pools[host]
    
    def _create_session(self, host: str) -> PooledConnection:
        """Create a new requests.Session with optimal configuration."""
        session = requests.Session()
        
        # Configure connection pooling at the adapter level
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self._config.max_connections_per_host,
            pool_maxsize=self._config.max_connections_per_host,
            max_retries=self._config.max_retries,
            pool_block=False,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Configure timeouts
        session.timeout = (self._config.connect_timeout, self._config.read_timeout)
        
        # SSL verification
        session.verify = self._config.verify_ssl
        
        return PooledConnection(session=session, host=host)
    
    @contextmanager
    def get_session(self, host: str):
        """
        Get a session from the pool (context manager).
        
        Usage:
            with pool.get_session("api.example.com") as session:
                response = session.get(url)
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        pool = self._get_pool_for_host(host)
        connection: Optional[PooledConnection] = None
        
        # Try to get an existing connection
        try:
            connection = pool.get_nowait()
            
            # Check if connection is expired
            if connection.is_expired(self._config.max_idle_time):
                try:
                    connection.session.close()
                except Exception:
                    pass
                self._total_connections -= 1
                connection = None
        except Empty:
            pass
        
        # Create new connection if needed
        if connection is None:
            with self._global_lock:
                if self._total_connections < self._config.max_total_connections:
                    connection = self._create_session(host)
                    self._total_connections += 1
                else:
                    # Wait for a connection to become available
                    connection = pool.get(timeout=self._config.connect_timeout)
        
        try:
            connection.touch()
            yield connection.session
        finally:
            # Return connection to pool
            if connection and not self._closed:
                try:
                    pool.put_nowait(connection)
                except:
                    # Pool is full, close the connection
                    try:
                        connection.session.close()
                    except Exception:
                        pass
                    self._total_connections -= 1
    
    def get_session_direct(self, host: str) -> requests.Session:
        """
        Get a session without context manager (caller must return it).
        
        Prefer using get_session() context manager instead.
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        pool = self._get_pool_for_host(host)
        
        try:
            connection = pool.get_nowait()
            if connection.is_expired(self._config.max_idle_time):
                try:
                    connection.session.close()
                except Exception:
                    pass
                self._total_connections -= 1
                connection = self._create_session(host)
                self._total_connections += 1
        except Empty:
            with self._global_lock:
                if self._total_connections < self._config.max_total_connections:
                    connection = self._create_session(host)
                    self._total_connections += 1
                else:
                    connection = pool.get(timeout=self._config.connect_timeout)
        
        connection.touch()
        return connection.session
    
    def return_session(self, host: str, session: requests.Session):
        """Return a session to the pool (for use with get_session_direct)."""
        if self._closed:
            try:
                session.close()
            except Exception:
                pass
            return
        
        pool = self._get_pool_for_host(host)
        connection = PooledConnection(session=session, host=host)
        
        try:
            pool.put_nowait(connection)
        except:
            try:
                session.close()
            except Exception:
                pass
            self._total_connections -= 1
    
    def close(self):
        """Close all connections in the pool."""
        if self._closed:
            return
        
        self._closed = True
        
        with self._global_lock:
            for host, pool in self._pools.items():
                while True:
                    try:
                        connection = pool.get_nowait()
                        try:
                            connection.session.close()
                        except Exception:
                            pass
                    except Empty:
                        break
            
            self._pools.clear()
            self._pool_locks.clear()
            self._total_connections = 0
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        pool_stats = {}
        for host, pool in self._pools.items():
            pool_stats[host] = {
                "available": pool.qsize(),
                "max_size": self._config.max_connections_per_host,
            }
        
        return {
            "total_connections": self._total_connections,
            "max_total_connections": self._config.max_total_connections,
            "pools": pool_stats,
            "closed": self._closed,
        }
    
    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance (useful for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close()
                cls._instance = None


class AsyncConnectionPool:
    """
    Async-safe connection pool for asynchronous HTTP requests.
    
    Uses httpx.AsyncClient for connection reuse, HTTP/2, and keep-alive.
    """
    
    _instance: Optional["AsyncConnectionPool"] = None
    _lock = threading.Lock()
    
    def __new__(cls, config: Optional[PoolConfig] = None):
        """Singleton pattern for global pool access."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance
    
    def __init__(self, config: Optional[PoolConfig] = None):
        if self._initialized:
            return
        
        self._config = config or PoolConfig()
        self._clients: Dict[str, httpx.AsyncClient] = {}
        self._client_locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()
        self._closed = False
        
        self._initialized = True
    
    def _create_client(self, host: str) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient with optimal configuration."""
        # Configure limits
        limits = httpx.Limits(
            max_connections=self._config.max_total_connections,
            max_keepalive_connections=self._config.max_connections_per_host,
            keepalive_expiry=self._config.max_idle_time,
        )
        
        # Configure timeouts
        timeout = httpx.Timeout(
            connect=self._config.connect_timeout,
            read=self._config.read_timeout,
            write=self._config.read_timeout,
            pool=self._config.connect_timeout,
        )
        
        # Try to use HTTP/2 if configured and available
        use_http2 = self._config.http2
        if use_http2:
            try:
                # Test if h2 package is available
                import h2  # noqa: F401
            except ImportError:
                # Fall back to HTTP/1.1 if h2 is not installed
                use_http2 = False
        
        client = httpx.AsyncClient(
            limits=limits,
            timeout=timeout,
            http2=use_http2,
            verify=self._config.verify_ssl,
        )
        
        return client
    
    def get_client(self, host: str) -> httpx.AsyncClient:
        """
        Get or create a shared AsyncClient for a host.
        
        httpx.AsyncClient handles connection pooling internally,
        so we typically use one client per host.
        """
        if self._closed:
            raise RuntimeError("Async connection pool is closed")
        
        if host not in self._clients:
            with self._global_lock:
                if host not in self._clients:
                    self._clients[host] = self._create_client(host)
        
        return self._clients[host]
    
    @asynccontextmanager
    async def get_client_context(self, host: str):
        """
        Get a client as an async context manager.
        
        Usage:
            async with pool.get_client_context("api.example.com") as client:
                response = await client.get(url)
        """
        client = self.get_client(host)
        try:
            yield client
        finally:
            pass  # Client is shared, don't close it
    
    async def close(self):
        """Close all clients in the pool."""
        if self._closed:
            return
        
        self._closed = True
        
        with self._global_lock:
            for host, client in self._clients.items():
                try:
                    await client.aclose()
                except Exception:
                    pass
            
            self._clients.clear()
    
    def close_sync(self):
        """Close all clients synchronously (for cleanup at exit)."""
        if self._closed:
            return
        
        self._closed = True
        
        import asyncio
        
        with self._global_lock:
            for host, client in list(self._clients.items()):
                try:
                    # Try to close asynchronously if event loop is running
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(client.aclose())
                    else:
                        loop.run_until_complete(client.aclose())
                except Exception:
                    pass
            
            self._clients.clear()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            "hosts": list(self._clients.keys()),
            "num_clients": len(self._clients),
            "closed": self._closed,
            "config": {
                "max_connections": self._config.max_total_connections,
                "max_keepalive": self._config.max_connections_per_host,
                "http2": self._config.http2,
            }
        }
    
    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance (useful for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close_sync()
                cls._instance = None


# Global pool instances (lazy initialization)
_sync_pool: Optional[SyncConnectionPool] = None
_async_pool: Optional[AsyncConnectionPool] = None


def get_sync_pool(config: Optional[PoolConfig] = None) -> SyncConnectionPool:
    """Get the global synchronous connection pool."""
    global _sync_pool
    if _sync_pool is None:
        _sync_pool = SyncConnectionPool(config)
    return _sync_pool


def get_async_pool(config: Optional[PoolConfig] = None) -> AsyncConnectionPool:
    """Get the global asynchronous connection pool."""
    global _async_pool
    if _async_pool is None:
        _async_pool = AsyncConnectionPool(config)
    return _async_pool


def configure_pools(config: PoolConfig):
    """
    Configure both sync and async pools with the same settings.
    
    Should be called before any connections are made.
    """
    global _sync_pool, _async_pool
    
    # Reset existing pools
    if _sync_pool is not None:
        SyncConnectionPool.reset_instance()
    if _async_pool is not None:
        AsyncConnectionPool.reset_instance()
    
    _sync_pool = SyncConnectionPool(config)
    _async_pool = AsyncConnectionPool(config)


def close_all_pools():
    """Close all connection pools."""
    global _sync_pool, _async_pool
    
    if _sync_pool is not None:
        _sync_pool.close()
        _sync_pool = None
    
    if _async_pool is not None:
        _async_pool.close_sync()
        _async_pool = None


# Register cleanup on exit
atexit.register(close_all_pools)
