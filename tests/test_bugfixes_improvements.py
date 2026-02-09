"""
Tests for Bug Fixes and Improvements

These tests cover:
1. CacheConfig adapter_ttl validation
2. Connection pool race condition fixes and health checks  
3. REST adapter async fallback methods
"""

import pytest
import time
import threading
from unittest.mock import MagicMock

import pyarrow as pa


class TestCacheConfigValidation:
    """Tests for enhanced CacheConfig validation."""
    
    def test_valid_adapter_ttl(self):
        """Test that valid adapter TTL values are accepted."""
        from waveql.cache import CacheConfig
        
        config = CacheConfig(
            adapter_ttl={
                "servicenow": 60,
                "jira": 120,
                "salesforce": 300,
            }
        )
        
        assert config.get_ttl_for_adapter("servicenow") == 60
        assert config.get_ttl_for_adapter("jira") == 120
        assert config.get_ttl_for_adapter("unknown") == config.default_ttl
    
    def test_negative_adapter_ttl_raises(self):
        """Test that negative adapter TTL values raise ValueError."""
        from waveql.cache import CacheConfig
        
        with pytest.raises(ValueError, match="TTL for adapter 'jira' must be non-negative"):
            CacheConfig(adapter_ttl={"jira": -1})
    
    def test_zero_adapter_ttl_allowed(self):
        """Test that zero TTL (disable caching for adapter) is allowed."""
        from waveql.cache import CacheConfig
        
        config = CacheConfig(adapter_ttl={"servicenow": 0})
        assert config.get_ttl_for_adapter("servicenow") == 0
    
    def test_negative_default_ttl_raises(self):
        """Test that negative default TTL raises ValueError."""
        from waveql.cache import CacheConfig
        
        with pytest.raises(ValueError, match="default_ttl must be non-negative"):
            CacheConfig(default_ttl=-1)


class TestConnectionPoolHealthCheck:
    """Tests for connection pool health checks."""
    
    def setup_method(self):
        """Reset singleton before each test."""
        from waveql.utils.connection_pool import SyncConnectionPool
        SyncConnectionPool.reset_instance()
    
    def teardown_method(self):
        """Cleanup after each test."""
        from waveql.utils.connection_pool import SyncConnectionPool
        SyncConnectionPool.reset_instance()
    
    def test_health_check_returns_true_for_new_connection(self):
        """Test that a newly created connection is considered healthy."""
        from waveql.utils.connection_pool import SyncConnectionPool, PoolConfig
        
        config = PoolConfig(max_idle_time=300)
        pool = SyncConnectionPool(config)
        
        # Create a fresh connection
        connection = pool._create_session("test.example.com")
        
        # Should be healthy
        assert pool._is_connection_healthy(connection, "test.example.com") is True
    
    def test_health_check_returns_false_for_stale_connection(self):
        """Test that a stale connection is considered unhealthy."""
        from waveql.utils.connection_pool import SyncConnectionPool, PoolConfig
        
        config = PoolConfig(max_idle_time=10)  # 10 seconds
        pool = SyncConnectionPool(config)
        
        # Create a connection that looks old
        connection = pool._create_session("test.example.com")
        connection.last_used = time.time() - 15  # 15 seconds ago (beyond 80% threshold)
        
        # Should be unhealthy (stale)
        assert pool._is_connection_healthy(connection, "test.example.com") is False
    
    def test_health_check_returns_false_for_closed_session(self):
        """Test that a connection with no adapters is unhealthy."""
        from waveql.utils.connection_pool import SyncConnectionPool, PoolConfig, PooledConnection
        import requests
        
        config = PoolConfig()
        pool = SyncConnectionPool(config)
        
        # Create a connection with empty adapters
        session = requests.Session()
        session.adapters.clear()  # Remove all adapters
        connection = PooledConnection(session=session, host="test.example.com")
        
        # Should be unhealthy
        assert pool._is_connection_healthy(connection, "test.example.com") is False


class TestConnectionPoolThreadSafety:
    """Tests for connection pool thread safety (race condition fixes)."""
    
    def setup_method(self):
        """Reset singleton before each test."""
        from waveql.utils.connection_pool import SyncConnectionPool
        SyncConnectionPool.reset_instance()
    
    def teardown_method(self):
        """Cleanup after each test."""
        from waveql.utils.connection_pool import SyncConnectionPool
        SyncConnectionPool.reset_instance()
    
    def test_concurrent_session_access(self):
        """Test that concurrent session access doesn't corrupt connection count."""
        from waveql.utils.connection_pool import SyncConnectionPool, PoolConfig
        
        config = PoolConfig(max_total_connections=10, max_connections_per_host=5)
        pool = SyncConnectionPool(config)
        
        errors = []
        
        def worker(thread_id):
            try:
                for _ in range(5):
                    with pool.get_session("test.example.com"):
                        # Simulate some work
                        time.sleep(0.01)
            except Exception as e:
                errors.append((thread_id, e))
        
        # Start multiple threads
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should be no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # Connection count should be non-negative
        assert pool._total_connections >= 0
    
    def test_stats_consistency(self):
        """Test that pool stats remain consistent under concurrent access."""
        from waveql.utils.connection_pool import SyncConnectionPool, PoolConfig
        
        config = PoolConfig(max_total_connections=20)
        pool = SyncConnectionPool(config)
        
        # Get some sessions
        with pool.get_session("host1.example.com"):
            with pool.get_session("host2.example.com"):
                stats = pool.stats
                
                # Stats should be valid
                assert stats["total_connections"] >= 0
                assert stats["total_connections"] <= config.max_total_connections
                assert not stats["closed"]


class TestRESTAdapterAsync:
    """Tests for REST adapter async fallback methods."""
    
    @pytest.mark.asyncio
    async def test_fetch_async_is_available(self):
        """Test that fetch_async method exists and is callable."""
        from waveql.adapters.rest_adapter import RESTAdapter
        
        adapter = RESTAdapter(host="https://jsonplaceholder.typicode.com")
        
        # Method should exist
        assert hasattr(adapter, 'fetch_async')
        assert callable(adapter.fetch_async)
    
    @pytest.mark.asyncio
    async def test_get_schema_async_is_available(self):
        """Test that get_schema_async method exists and is callable."""
        from waveql.adapters.rest_adapter import RESTAdapter
        
        adapter = RESTAdapter(host="https://jsonplaceholder.typicode.com")
        
        # Method should exist
        assert hasattr(adapter, 'get_schema_async')
        assert callable(adapter.get_schema_async)
    
    @pytest.mark.asyncio
    async def test_insert_async_is_available(self):
        """Test that insert_async method exists and is callable."""
        from waveql.adapters.rest_adapter import RESTAdapter
        
        adapter = RESTAdapter(host="https://jsonplaceholder.typicode.com")
        
        # Method should exist
        assert hasattr(adapter, 'insert_async')
        assert callable(adapter.insert_async)
    
    @pytest.mark.asyncio
    async def test_update_async_is_available(self):
        """Test that update_async method exists and is callable."""
        from waveql.adapters.rest_adapter import RESTAdapter
        
        adapter = RESTAdapter(host="https://jsonplaceholder.typicode.com")
        
        # Method should exist
        assert hasattr(adapter, 'update_async')
        assert callable(adapter.update_async)
    
    @pytest.mark.asyncio
    async def test_delete_async_is_available(self):
        """Test that delete_async method exists and is callable."""
        from waveql.adapters.rest_adapter import RESTAdapter
        
        adapter = RESTAdapter(host="https://jsonplaceholder.typicode.com")
        
        # Method should exist
        assert hasattr(adapter, 'delete_async')
        assert callable(adapter.delete_async)
    
    @pytest.mark.asyncio
    async def test_fetch_async_delegates_to_sync(self):
        """Test that fetch_async properly calls the sync fetch method."""
        from waveql.adapters.rest_adapter import RESTAdapter
        
        adapter = RESTAdapter(host="https://jsonplaceholder.typicode.com")
        
        # Mock the sync fetch method
        mock_result = pa.table({"id": [1, 2], "name": ["a", "b"]})
        adapter.fetch = MagicMock(return_value=mock_result)
        
        # Call async version
        result = await adapter.fetch_async("test_table", columns=["id", "name"])
        
        # Should have called sync method
        adapter.fetch.assert_called_once()
        assert result == mock_result


class TestQueryLogging:
    """Tests for query logging in adapters."""
    
    def test_rest_adapter_has_logger(self):
        """Test that REST adapter has a logger."""
        from waveql.adapters.rest_adapter import logger
        import logging
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "waveql.adapters.rest_adapter"
    
    def test_servicenow_adapter_has_logger(self):
        """Test that ServiceNow adapter has a logger."""
        from waveql.adapters.servicenow import logger
        import logging
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "waveql.adapters.servicenow"
    
    def test_jira_adapter_has_logger(self):
        """Test that Jira adapter has a logger."""
        from waveql.adapters.jira import logger
        import logging
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "waveql.adapters.jira"
    
    def test_salesforce_adapter_has_logger(self):
        """Test that Salesforce adapter has a logger."""
        from waveql.adapters.salesforce import logger
        import logging
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "waveql.adapters.salesforce"
