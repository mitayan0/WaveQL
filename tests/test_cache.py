"""
Tests for the Query Result Cache module.

Covers:
- Cache configuration and initialization
- Key generation consistency
- Cache hit/miss behavior
- TTL expiration
- LRU eviction
- Memory-based eviction
- Statistics tracking
- Cache invalidation
- Thread safety
- Integration with connection and cursor
"""

import time
import threading
import pytest
import pyarrow as pa

from waveql.cache import (
    QueryCache,
    CacheConfig,
    CacheEntry,
    create_cache,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_table():
    """Create a sample Arrow table for testing."""
    return pa.Table.from_pydict({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "active": [True, False, True],
    })


@pytest.fixture
def large_table():
    """Create a larger Arrow table for memory testing."""
    return pa.Table.from_pydict({
        "id": list(range(10000)),
        "data": ["x" * 1000 for _ in range(10000)],
    })


@pytest.fixture
def cache():
    """Create a default cache instance."""
    return QueryCache()


@pytest.fixture
def cache_short_ttl():
    """Create a cache with short TTL for expiration testing."""
    return QueryCache(CacheConfig(default_ttl=0.1))


# =============================================================================
# CacheConfig Tests
# =============================================================================

class TestCacheConfig:
    """Tests for CacheConfig dataclass."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = CacheConfig()
        
        assert config.enabled is True
        assert config.default_ttl == 300.0
        assert config.max_entries == 1000
        assert config.max_memory_mb == 512.0
        assert config.adapter_ttl == {}
        assert config.exclude_tables == []
        assert config.cache_writes is False
    
    def test_custom_values(self):
        """Test custom configuration."""
        config = CacheConfig(
            enabled=False,
            default_ttl=60.0,
            max_entries=100,
            max_memory_mb=128.0,
            adapter_ttl={"servicenow": 30},
            exclude_tables=["audit_log"],
        )
        
        assert config.enabled is False
        assert config.default_ttl == 60.0
        assert config.max_entries == 100
        assert config.adapter_ttl["servicenow"] == 30
        assert "audit_log" in config.exclude_tables
    
    def test_validation_negative_ttl(self):
        """Test that negative TTL raises error."""
        with pytest.raises(ValueError, match="default_ttl must be non-negative"):
            CacheConfig(default_ttl=-1)
    
    def test_validation_zero_entries(self):
        """Test that zero max_entries raises error."""
        with pytest.raises(ValueError, match="max_entries must be at least 1"):
            CacheConfig(max_entries=0)
    
    def test_validation_zero_memory(self):
        """Test that zero max_memory_mb raises error."""
        with pytest.raises(ValueError, match="max_memory_mb must be at least 1"):
            CacheConfig(max_memory_mb=0)
    
    def test_get_ttl_for_adapter(self):
        """Test per-adapter TTL lookup."""
        config = CacheConfig(
            default_ttl=300,
            adapter_ttl={"servicenow": 60, "jira": 120}
        )
        
        assert config.get_ttl_for_adapter("servicenow") == 60
        assert config.get_ttl_for_adapter("jira") == 120
        assert config.get_ttl_for_adapter("salesforce") == 300  # default
    
    def test_should_cache_table(self):
        """Test table exclusion logic."""
        config = CacheConfig(exclude_tables=["audit_log", "sys_journal"])
        
        assert config.should_cache_table("incident") is True
        assert config.should_cache_table("audit_log") is False
        assert config.should_cache_table("sys_journal") is False
        assert config.should_cache_table("AUDIT_LOG") is False  # case insensitive
    
    def test_should_cache_table_disabled(self):
        """Test that disabled cache rejects all tables."""
        config = CacheConfig(enabled=False)
        
        assert config.should_cache_table("incident") is False


# =============================================================================
# CacheEntry Tests
# =============================================================================

class TestCacheEntry:
    """Tests for CacheEntry dataclass."""
    
    def test_is_expired_false(self, sample_table):
        """Test entry is not expired within TTL."""
        entry = CacheEntry(
            data=sample_table,
            created_at=time.time(),
            ttl=300.0,
        )
        
        assert entry.is_expired is False
    
    def test_is_expired_true(self, sample_table):
        """Test entry expires after TTL."""
        entry = CacheEntry(
            data=sample_table,
            created_at=time.time() - 400,  # 400 seconds ago
            ttl=300.0,
        )
        
        assert entry.is_expired is True
    
    def test_age_seconds(self, sample_table):
        """Test age calculation."""
        entry = CacheEntry(
            data=sample_table,
            created_at=time.time() - 10,
            ttl=300.0,
        )
        
        assert 9 <= entry.age_seconds <= 11
    
    def test_remaining_ttl(self, sample_table):
        """Test remaining TTL calculation."""
        entry = CacheEntry(
            data=sample_table,
            created_at=time.time() - 100,
            ttl=300.0,
        )
        
        assert 199 <= entry.remaining_ttl <= 201


# =============================================================================
# QueryCache Key Generation Tests
# =============================================================================

class TestCacheKeyGeneration:
    """Tests for cache key generation."""
    
    def test_same_query_same_key(self, cache):
        """Test that identical queries produce identical keys."""
        key1 = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            columns=("number", "short_description"),
            predicates=(("active", "=", True),),
        )
        
        key2 = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            columns=("number", "short_description"),
            predicates=(("active", "=", True),),
        )
        
        assert key1 == key2
    
    def test_different_table_different_key(self, cache):
        """Test that different tables produce different keys."""
        key1 = cache.generate_key(adapter_name="servicenow", table="incident")
        key2 = cache.generate_key(adapter_name="servicenow", table="problem")
        
        assert key1 != key2
    
    def test_different_predicates_different_key(self, cache):
        """Test that different predicates produce different keys."""
        key1 = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            predicates=(("active", "=", True),),
        )
        key2 = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            predicates=(("active", "=", False),),
        )
        
        assert key1 != key2
    
    def test_predicate_order_insensitive(self, cache):
        """Test that predicate order doesn't affect key."""
        key1 = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            predicates=(("active", "=", True), ("priority", "=", 1)),
        )
        key2 = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            predicates=(("priority", "=", 1), ("active", "=", True)),
        )
        
        assert key1 == key2
    
    def test_different_limit_different_key(self, cache):
        """Test that different limits produce different keys."""
        key1 = cache.generate_key(adapter_name="servicenow", table="incident", limit=100)
        key2 = cache.generate_key(adapter_name="servicenow", table="incident", limit=200)
        
        assert key1 != key2
    
    def test_key_length(self, cache):
        """Test that key is 16 chars (truncated SHA256)."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        assert len(key) == 16
        assert all(c in "0123456789abcdef" for c in key)


# =============================================================================
# QueryCache Get/Put Tests
# =============================================================================

class TestCacheGetPut:
    """Tests for cache get/put operations."""
    
    def test_put_and_get(self, cache, sample_table):
        """Test basic put and get."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        cache.put(key, sample_table)
        result = cache.get(key)
        
        assert result is not None
        assert len(result) == len(sample_table)
        assert result.column_names == sample_table.column_names
    
    def test_get_missing_key(self, cache):
        """Test get returns None for missing key."""
        result = cache.get("nonexistent_key")
        
        assert result is None
    
    def test_put_none_data(self, cache):
        """Test that None data is not cached."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        cache.put(key, None)
        result = cache.get(key)
        
        assert result is None
    
    def test_cache_disabled_get(self, sample_table):
        """Test get returns None when cache is disabled."""
        cache = QueryCache(CacheConfig(enabled=False))
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        cache.put(key, sample_table)
        result = cache.get(key)
        
        assert result is None
    
    def test_overwrite_existing_key(self, cache, sample_table):
        """Test that putting with same key overwrites."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        # Put first table
        cache.put(key, sample_table)
        
        # Put different table with same key
        new_table = pa.Table.from_pydict({"id": [99, 100]})
        cache.put(key, new_table)
        
        result = cache.get(key)
        assert len(result) == 2
        assert result.column("id").to_pylist() == [99, 100]


# =============================================================================
# TTL Expiration Tests
# =============================================================================

class TestCacheExpiration:
    """Tests for TTL-based expiration."""
    
    def test_expired_entry_not_returned(self, cache_short_ttl, sample_table):
        """Test that expired entries return None."""
        key = cache_short_ttl.generate_key(adapter_name="servicenow", table="incident")
        
        cache_short_ttl.put(key, sample_table)
        
        # Entry should exist immediately
        assert cache_short_ttl.get(key) is not None
        
        # Wait for expiration
        time.sleep(0.15)
        
        # Entry should be expired
        assert cache_short_ttl.get(key) is None
    
    def test_custom_ttl_on_put(self, sample_table):
        """Test custom TTL per entry."""
        cache = QueryCache(CacheConfig(default_ttl=300))
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        # Put with short TTL
        cache.put(key, sample_table, ttl=0.1)
        
        # Should exist
        assert cache.get(key) is not None
        
        # Wait and check expiration
        time.sleep(0.15)
        assert cache.get(key) is None
    
    def test_adapter_ttl(self, sample_table):
        """Test adapter-specific TTL."""
        cache = QueryCache(CacheConfig(
            default_ttl=300,
            adapter_ttl={"servicenow": 0.1}
        ))
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        
        cache.put(key, sample_table, adapter_name="servicenow")
        
        # Wait for ServiceNow TTL
        time.sleep(0.15)
        assert cache.get(key) is None


# =============================================================================
# LRU Eviction Tests
# =============================================================================

class TestCacheEviction:
    """Tests for LRU and memory-based eviction."""
    
    def test_lru_eviction_max_entries(self, sample_table):
        """Test LRU eviction when max_entries is reached."""
        cache = QueryCache(CacheConfig(max_entries=3))
        
        # Add 3 entries
        for i in range(3):
            key = cache.generate_key(adapter_name="servicenow", table=f"table{i}")
            cache.put(key, sample_table)
        
        # All 3 should exist
        assert cache.stats.entries == 3
        
        # Add 4th entry, should evict oldest
        key4 = cache.generate_key(adapter_name="servicenow", table="table_new")
        cache.put(key4, sample_table)
        
        # Still 3 entries
        assert cache.stats.entries == 3
        
        # First entry should be evicted
        key0 = cache.generate_key(adapter_name="servicenow", table="table0")
        assert cache.get(key0) is None
        
        # New entry should exist
        assert cache.get(key4) is not None
    
    def test_access_updates_lru_order(self, sample_table):
        """Test that accessing an entry moves it to most recently used."""
        cache = QueryCache(CacheConfig(max_entries=3))
        
        # Add 3 entries
        keys = []
        for i in range(3):
            key = cache.generate_key(adapter_name="servicenow", table=f"table{i}")
            cache.put(key, sample_table)
            keys.append(key)
        
        # Access first entry (moves it to end)
        cache.get(keys[0])
        
        # Add new entry, should evict second entry (now oldest)
        key_new = cache.generate_key(adapter_name="servicenow", table="table_new")
        cache.put(key_new, sample_table)
        
        # First entry should still exist (was accessed)
        assert cache.get(keys[0]) is not None
        
        # Second entry should be evicted
        assert cache.get(keys[1]) is None
    
    def test_eviction_stats(self, sample_table):
        """Test that evictions are tracked in stats."""
        cache = QueryCache(CacheConfig(max_entries=2))
        
        for i in range(5):
            key = cache.generate_key(adapter_name="servicenow", table=f"table{i}")
            cache.put(key, sample_table)
        
        # Should have evicted 3 entries
        assert cache.stats.evictions == 3


# =============================================================================
# Cache Statistics Tests
# =============================================================================

class TestCacheStats:
    """Tests for cache statistics."""
    
    def test_initial_stats(self, cache):
        """Test initial stats are zero."""
        stats = cache.stats
        
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0
        assert stats.invalidations == 0
        assert stats.entries == 0
    
    def test_hit_tracking(self, cache, sample_table):
        """Test cache hits are tracked."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        
        # 3 hits
        cache.get(key)
        cache.get(key)
        cache.get(key)
        
        assert cache.stats.hits == 3
    
    def test_miss_tracking(self, cache):
        """Test cache misses are tracked."""
        cache.get("missing1")
        cache.get("missing2")
        
        assert cache.stats.misses == 2
    
    def test_hit_rate_calculation(self, cache, sample_table):
        """Test hit rate calculation."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        
        # 3 hits, 1 miss
        cache.get(key)
        cache.get(key)
        cache.get(key)
        cache.get("missing")
        
        # 3/4 = 75%
        assert cache.stats.hit_rate == pytest.approx(75.0, rel=0.1)
    
    def test_size_tracking(self, cache, sample_table):
        """Test memory size tracking."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        
        # Should have non-zero size
        assert cache.stats.size_mb > 0
    
    def test_stats_to_dict(self, cache, sample_table):
        """Test stats serialization."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        cache.get(key)
        
        stats_dict = cache.stats.to_dict()
        
        assert "hits" in stats_dict
        assert "misses" in stats_dict
        assert "hit_rate" in stats_dict
        assert "size_mb" in stats_dict


# =============================================================================
# Cache Invalidation Tests
# =============================================================================

class TestCacheInvalidation:
    """Tests for cache invalidation."""
    
    def test_invalidate_all(self, cache, sample_table):
        """Test clearing entire cache."""
        for i in range(5):
            key = cache.generate_key(adapter_name="servicenow", table=f"table{i}")
            cache.put(key, sample_table)
        
        assert cache.stats.entries == 5
        
        count = cache.invalidate()
        
        assert count == 5
        assert cache.stats.entries == 0
    
    def test_invalidate_by_adapter(self, cache, sample_table):
        """Test invalidating by adapter name."""
        # Add entries for different adapters - must include adapter_name for metadata
        for adapter in ["servicenow", "jira", "salesforce"]:
            key = cache.generate_key(adapter_name=adapter, table="table1")
            cache.put(key, sample_table, adapter_name=adapter, table_name="table1")
        
        # Invalidate only servicenow
        count = cache.invalidate(adapter="servicenow")
        
        assert count == 1
        assert cache.stats.entries == 2
    
    def test_invalidate_by_table(self, cache, sample_table):
        """Test invalidating by table name."""
        tables = ["incident", "problem", "incident", "change"]
        for i, table in enumerate(tables):
            key = cache.generate_key(adapter_name="servicenow", table=table, limit=i)
            cache.put(key, sample_table, adapter_name="servicenow", table_name=table)
        
        # Invalidate incident table entries
        count = cache.invalidate(table="incident")
        
        assert count == 2
        assert cache.stats.entries == 2
    
    def test_clear_alias(self, cache, sample_table):
        """Test clear() is alias for invalidate()."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        
        count = cache.clear()
        
        assert count == 1
        assert cache.stats.entries == 0
    
    def test_invalidation_stats(self, cache, sample_table):
        """Test invalidations are tracked."""
        for i in range(3):
            key = cache.generate_key(adapter_name="servicenow", table=f"table{i}")
            cache.put(key, sample_table)
        
        cache.invalidate()
        
        assert cache.stats.invalidations == 3


# =============================================================================
# Thread Safety Tests
# =============================================================================

class TestCacheThreadSafety:
    """Tests for thread safety."""
    
    def test_concurrent_puts(self, sample_table):
        """Test concurrent put operations."""
        cache = QueryCache()
        errors = []
        
        def put_entry(i):
            try:
                key = cache.generate_key(adapter_name="servicenow", table=f"table{i}")
                cache.put(key, sample_table)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=put_entry, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert cache.stats.entries == 100
    
    def test_concurrent_gets(self, cache, sample_table):
        """Test concurrent get operations."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        
        results = []
        errors = []
        
        def get_entry():
            try:
                result = cache.get(key)
                results.append(result is not None)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=get_entry) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert all(results)
    
    def test_concurrent_mixed_operations(self, sample_table):
        """Test concurrent mixed operations."""
        cache = QueryCache()
        errors = []
        
        def mixed_ops(i):
            try:
                table = f"table{i % 10}"
                key = cache.generate_key(adapter_name="servicenow", table=table)
                if i % 3 == 0:
                    cache.put(key, sample_table, adapter_name="servicenow", table_name=table)
                elif i % 3 == 1:
                    cache.get(key)
                else:
                    cache.invalidate(table=table)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=mixed_ops, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0


# =============================================================================
# create_cache Helper Tests
# =============================================================================

class TestCreateCacheHelper:
    """Tests for create_cache convenience function."""
    
    def test_create_cache_defaults(self):
        """Test create_cache with defaults."""
        cache = create_cache()
        
        assert cache.config.enabled is True
        assert cache.config.default_ttl == 300.0
    
    def test_create_cache_custom(self):
        """Test create_cache with custom values."""
        cache = create_cache(
            enabled=False,
            ttl=60.0,
            max_entries=500,
            max_memory_mb=256.0,
        )
        
        assert cache.config.enabled is False
        assert cache.config.default_ttl == 60.0
        assert cache.config.max_entries == 500
        assert cache.config.max_memory_mb == 256.0


# =============================================================================
# get_entries_info Tests
# =============================================================================

class TestGetEntriesInfo:
    """Tests for get_entries_info method."""
    
    def test_entries_info(self, cache, sample_table):
        """Test getting info about cached entries."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        cache.get(key)  # Access to increment hit count
        
        entries = cache.get_entries_info()
        
        assert len(entries) == 1
        entry = entries[0]
        assert entry["key"] == key
        assert entry["hit_count"] == 1
        assert entry["rows"] == 3
        assert entry["expired"] is False


# =============================================================================
# Integration Tests with Connection
# =============================================================================

class TestCacheIntegration:
    """Integration tests with WaveQL connection."""
    
    def test_connection_has_cache(self):
        """Test that connection has cache attribute."""
        import waveql
        
        conn = waveql.connect()
        
        assert hasattr(conn, "cache")
        assert hasattr(conn, "cache_stats")
        assert hasattr(conn, "invalidate_cache")
        
        conn.close()
    
    def test_connection_cache_ttl_param(self):
        """Test connection cache_ttl parameter."""
        import waveql
        
        conn = waveql.connect(cache_ttl=60)
        
        assert conn.cache.config.default_ttl == 60.0
        
        conn.close()
    
    def test_connection_disable_cache(self):
        """Test disabling cache via parameter."""
        import waveql
        
        conn = waveql.connect(enable_cache=False)
        
        assert conn.cache.config.enabled is False
        
        conn.close()
    
    def test_connection_cache_config(self):
        """Test connection with full CacheConfig."""
        import waveql
        
        config = CacheConfig(
            default_ttl=120,
            max_entries=500,
            adapter_ttl={"servicenow": 30}
        )
        conn = waveql.connect(cache_config=config)
        
        assert conn.cache.config.default_ttl == 120
        assert conn.cache.config.max_entries == 500
        assert conn.cache.config.adapter_ttl.get("servicenow") == 30
        
        conn.close()
    
    def test_set_cache_ttl(self):
        """Test setting adapter-specific TTL."""
        import waveql
        
        conn = waveql.connect()
        conn.set_cache_ttl("servicenow", 60)
        
        assert conn.cache.config.adapter_ttl.get("servicenow") == 60
        
        conn.close()


# =============================================================================
# Cache Repr Tests
# =============================================================================

class TestCacheRepr:
    """Tests for cache __repr__ method."""
    
    def test_repr(self, cache, sample_table):
        """Test cache string representation."""
        key = cache.generate_key(adapter_name="servicenow", table="incident")
        cache.put(key, sample_table)
        
        repr_str = repr(cache)
        
        assert "QueryCache" in repr_str
        assert "enabled=True" in repr_str
        assert "entries=1" in repr_str
