"""Tests for connection pool."""

import pytest
from waveql.utils.connection_pool import SyncConnectionPool, AsyncConnectionPool, PoolConfig


class TestPoolConfig:
    def test_defaults(self):
        config = PoolConfig()
        assert config.max_connections_per_host == 10


class TestSyncConnectionPool:
    def test_singleton(self):
        SyncConnectionPool.reset_instance()
        p1 = SyncConnectionPool()
        p2 = SyncConnectionPool()
        assert p1 is p2


class TestAsyncConnectionPool:
    def test_singleton(self):
        AsyncConnectionPool.reset_instance()
        p1 = AsyncConnectionPool()
        p2 = AsyncConnectionPool()
        assert p1 is p2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
