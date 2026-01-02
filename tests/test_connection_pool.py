"""
Tests for Connection Pool functionality.
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from waveql.utils.connection_pool import (
    PoolConfig,
    SyncConnectionPool,
    AsyncConnectionPool,
    get_sync_pool,
    get_async_pool,
    configure_pools,
    close_all_pools,
    PooledConnection,
)


class TestPoolConfig:
    """Tests for PoolConfig dataclass."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = PoolConfig()
        
        assert config.max_connections_per_host == 10
        assert config.max_total_connections == 100
        assert config.connect_timeout == 10.0
        assert config.read_timeout == 30.0
        assert config.max_idle_time == 300.0
        assert config.keep_alive is True
        assert config.http2 is True
        assert config.max_retries == 3
        assert config.verify_ssl is True
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = PoolConfig(
            max_connections_per_host=20,
            max_total_connections=200,
            connect_timeout=5.0,
            http2=False,
        )
        
        assert config.max_connections_per_host == 20
        assert config.max_total_connections == 200
        assert config.connect_timeout == 5.0
        assert config.http2 is False


class TestPooledConnection:
    """Tests for PooledConnection wrapper."""
    
    def test_touch_updates_stats(self):
        """Test that touch updates last_used and use_count."""
        conn = PooledConnection(session=Mock())
        initial_time = conn.last_used
        initial_count = conn.use_count
        
        time.sleep(0.01)  # Small delay
        conn.touch()
        
        assert conn.last_used > initial_time
        assert conn.use_count == initial_count + 1
    
    def test_is_expired(self):
        """Test expiration detection."""
        conn = PooledConnection(session=Mock())
        conn.last_used = time.time() - 400  # 400 seconds ago
        
        assert conn.is_expired(300.0) is True
        assert conn.is_expired(500.0) is False


class TestSyncConnectionPool:
    """Tests for synchronous connection pool."""
    
    def setup_method(self):
        """Reset singleton before each test."""
        SyncConnectionPool.reset_instance()
    
    def teardown_method(self):
        """Clean up after each test."""
        SyncConnectionPool.reset_instance()
    
    def test_singleton_pattern(self):
        """Test that pool is a singleton."""
        pool1 = SyncConnectionPool()
        pool2 = SyncConnectionPool()
        
        assert pool1 is pool2
    
    def test_get_session_context_manager(self):
        """Test getting a session via context manager."""
        pool = SyncConnectionPool()
        
        with pool.get_session("api.example.com") as session:
            assert session is not None
            # Session should have proper adapters
            assert hasattr(session, "get")
            assert hasattr(session, "post")
    
    def test_session_reuse(self):
        """Test that sessions are reused when returned to pool."""
        pool = SyncConnectionPool()
        host = "test.example.com"
        
        # Get a session
        with pool.get_session(host) as session1:
            session1_id = id(session1)
        
        # Get another session - should be the same one
        with pool.get_session(host) as session2:
            assert id(session2) == session1_id
    
    def test_pool_stats(self):
        """Test pool statistics."""
        pool = SyncConnectionPool()
        
        with pool.get_session("stats.example.com") as _:
            pass
        
        stats = pool.stats
        assert "total_connections" in stats
        assert "pools" in stats
        assert stats["closed"] is False
    
    def test_close_pool(self):
        """Test closing the pool."""
        pool = SyncConnectionPool()
        
        with pool.get_session("close.example.com") as _:
            pass
        
        pool.close()
        
        assert pool.stats["closed"] is True
        
        # Getting a session should raise
        with pytest.raises(RuntimeError):
            with pool.get_session("close.example.com") as _:
                pass
    
    def test_thread_safety(self):
        """Test that pool is thread-safe."""
        pool = SyncConnectionPool()
        sessions = []
        errors = []
        
        def worker(host_suffix):
            try:
                with pool.get_session(f"thread{host_suffix}.example.com") as session:
                    sessions.append(session)
                    time.sleep(0.01)  # Simulate some work
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert len(sessions) == 10


class TestAsyncConnectionPool:
    """Tests for asynchronous connection pool."""
    
    def setup_method(self):
        """Reset singleton before each test."""
        AsyncConnectionPool.reset_instance()
    
    def teardown_method(self):
        """Clean up after each test."""
        AsyncConnectionPool.reset_instance()
    
    def test_singleton_pattern(self):
        """Test that async pool is a singleton."""
        pool1 = AsyncConnectionPool()
        pool2 = AsyncConnectionPool()
        
        assert pool1 is pool2
    
    def test_get_client(self):
        """Test getting an async client."""
        pool = AsyncConnectionPool()
        client = pool.get_client("async.example.com")
        
        assert client is not None
        # Should have async methods
        assert hasattr(client, "get")
        assert hasattr(client, "post")
    
    def test_client_reuse(self):
        """Test that clients are reused for the same host."""
        pool = AsyncConnectionPool()
        host = "reuse.example.com"
        
        client1 = pool.get_client(host)
        client2 = pool.get_client(host)
        
        assert client1 is client2
    
    def test_different_hosts_different_clients(self):
        """Test that different hosts get different clients."""
        pool = AsyncConnectionPool()
        
        client1 = pool.get_client("host1.example.com")
        client2 = pool.get_client("host2.example.com")
        
        assert client1 is not client2
    
    def test_pool_stats(self):
        """Test async pool statistics."""
        pool = AsyncConnectionPool()
        pool.get_client("stats.example.com")
        
        stats = pool.stats
        assert "hosts" in stats
        assert "num_clients" in stats
        assert stats["closed"] is False


class TestGlobalPoolFunctions:
    """Tests for global pool access functions."""
    
    def setup_method(self):
        """Reset pools before each test."""
        close_all_pools()
        SyncConnectionPool.reset_instance()
        AsyncConnectionPool.reset_instance()
    
    def teardown_method(self):
        """Clean up after each test."""
        close_all_pools()
    
    def test_get_sync_pool(self):
        """Test getting the global sync pool."""
        pool = get_sync_pool()
        assert pool is not None
        assert isinstance(pool, SyncConnectionPool)
    
    def test_get_async_pool(self):
        """Test getting the global async pool."""
        pool = get_async_pool()
        assert pool is not None
        assert isinstance(pool, AsyncConnectionPool)
    
    def test_configure_pools(self):
        """Test configuring pools with custom config."""
        config = PoolConfig(max_connections_per_host=5)
        configure_pools(config)
        
        sync_pool = get_sync_pool()
        async_pool = get_async_pool()
        
        assert sync_pool._config.max_connections_per_host == 5
        assert async_pool._config.max_connections_per_host == 5


class TestAdapterIntegration:
    """Test integration with adapters."""
    
    def setup_method(self):
        """Reset pools before each test."""
        close_all_pools()
        SyncConnectionPool.reset_instance()
        AsyncConnectionPool.reset_instance()
    
    def teardown_method(self):
        """Clean up after each test."""
        close_all_pools()
    
    def test_base_adapter_uses_pool(self):
        """Test that BaseAdapter uses the connection pool."""
        from waveql.adapters.base import BaseAdapter
        
        # Create a concrete adapter for testing
        class TestAdapter(BaseAdapter):
            adapter_name = "test"
            
            def fetch(self, table, **kwargs):
                pass
            
            def get_schema(self, table):
                return []
        
        adapter = TestAdapter(host="https://test.example.com")
        
        # Should use connection pool by default
        assert adapter._use_connection_pool is True
        
        # Pool host should be extracted correctly
        assert adapter._pool_host == "test.example.com"
    
    def test_adapter_pool_disabled(self):
        """Test that connection pool can be disabled."""
        from waveql.adapters.base import BaseAdapter
        
        class TestAdapter(BaseAdapter):
            adapter_name = "test"
            
            def fetch(self, table, **kwargs):
                pass
            
            def get_schema(self, table):
                return []
        
        adapter = TestAdapter(
            host="https://test.example.com",
            use_connection_pool=False
        )
        
        assert adapter._use_connection_pool is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
