"""
Query Result Cache - LRU cache with TTL support for API query results.

Provides transparent caching of query results to:
- Reduce API calls and improve response times
- Honor rate limits by minimizing redundant requests
- Enable dashboard-style repeated queries

Usage:
    from waveql.cache import QueryCache, CacheConfig
    
    cache = QueryCache(CacheConfig(default_ttl=300))
    
    # Or via connection:
    conn = waveql.connect("servicenow://...", cache_ttl=300)
"""

from __future__ import annotations
import hashlib
import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from collections import OrderedDict

import pyarrow as pa

if TYPE_CHECKING:
    from waveql.query_planner import Predicate

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """
    A cached query result with metadata.
    
    Attributes:
        data: The cached Arrow table
        created_at: Unix timestamp when entry was created
        ttl: Time-to-live in seconds
        hit_count: Number of times this entry has been accessed
        size_bytes: Approximate memory size of the cached data
        adapter_name: Name of the adapter that created this entry
        table_name: Name of the table this entry is for
    """
    data: pa.Table
    created_at: float
    ttl: float
    hit_count: int = 0
    size_bytes: int = 0
    adapter_name: str = ""
    table_name: str = ""
    
    @property
    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        return time.time() - self.created_at > self.ttl
    
    @property
    def age_seconds(self) -> float:
        """Get the age of this entry in seconds."""
        return time.time() - self.created_at
    
    @property
    def remaining_ttl(self) -> float:
        """Get remaining TTL in seconds (negative if expired)."""
        return self.ttl - self.age_seconds


@dataclass
class CacheConfig:
    """
    Configuration for the query cache.
    
    Attributes:
        enabled: Whether caching is enabled
        default_ttl: Default time-to-live in seconds (default: 5 minutes)
        max_entries: Maximum number of cache entries
        max_memory_mb: Maximum cache size in megabytes
        adapter_ttl: Per-adapter TTL overrides (e.g., {"servicenow": 60})
        exclude_tables: Tables to never cache (e.g., ["audit_log"])
        cache_writes: Whether to cache INSERT/UPDATE/DELETE results
    """
    enabled: bool = True
    default_ttl: float = 300.0  # 5 minutes
    max_entries: int = 1000
    max_memory_mb: float = 512.0
    adapter_ttl: Dict[str, float] = field(default_factory=dict)
    exclude_tables: List[str] = field(default_factory=list)
    cache_writes: bool = False
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.default_ttl < 0:
            raise ValueError("default_ttl must be non-negative")
        if self.max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        if self.max_memory_mb < 1:
            raise ValueError("max_memory_mb must be at least 1")
    
    def get_ttl_for_adapter(self, adapter_name: str) -> float:
        """Get the TTL for a specific adapter."""
        return self.adapter_ttl.get(adapter_name, self.default_ttl)
    
    def should_cache_table(self, table_name: str) -> bool:
        """Check if a table should be cached."""
        if not self.enabled:
            return False
        # Strip schema prefix for matching
        clean_name = table_name.split(".")[-1].strip('"') if table_name else ""
        return clean_name.lower() not in [t.lower() for t in self.exclude_tables]


@dataclass
class CacheStats:
    """
    Statistics about cache usage.
    
    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        evictions: Number of entries evicted
        invalidations: Number of manual invalidations
        entries: Current number of cached entries
        size_mb: Current cache size in MB
    """
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    invalidations: int = 0
    entries: int = 0
    size_mb: float = 0.0
    
    @property
    def hit_rate(self) -> float:
        """Calculate hit rate as a percentage."""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "invalidations": self.invalidations,
            "entries": self.entries,
            "size_mb": round(self.size_mb, 2),
            "hit_rate": f"{self.hit_rate:.1f}%",
        }


class QueryCache:
    """
    Thread-safe LRU cache for query results with TTL support.
    
    Features:
    - LRU eviction when max_entries is reached
    - TTL-based expiration per entry
    - Memory-aware eviction
    - Cache key generation from query components
    - Statistics tracking
    - Thread-safe operations
    
    Example:
        cache = QueryCache(CacheConfig(default_ttl=300))
        
        # Generate key from query components
        key = cache.generate_key(
            adapter_name="servicenow",
            table="incident",
            columns=("number", "short_description"),
            predicates=(("active", "=", True),),
        )
        
        # Check cache
        result = cache.get(key)
        if result is None:
            # Cache miss - fetch from source
            result = adapter.fetch(...)
            cache.put(key, result)
    """
    
    def __init__(self, config: CacheConfig = None):
        """
        Initialize the query cache.
        
        Args:
            config: Cache configuration. Uses defaults if not provided.
        """
        self.config = config or CacheConfig()
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()
        self._total_size = 0
        
        logger.debug(
            "QueryCache initialized: enabled=%s, default_ttl=%s, max_entries=%s, max_memory_mb=%s",
            self.config.enabled,
            self.config.default_ttl,
            self.config.max_entries,
            self.config.max_memory_mb,
        )
    
    def generate_key(
        self,
        adapter_name: str,
        table: str,
        columns: Tuple[str, ...] = None,
        predicates: Tuple[Tuple[str, str, Any], ...] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[Tuple[Tuple[str, str], ...]] = None,
        group_by: Optional[Tuple[str, ...]] = None,
    ) -> str:
        """
        Generate a deterministic cache key from query components.
        
        The key is a SHA-256 hash of all query parameters, ensuring
        that identical queries produce identical keys.
        
        Args:
            adapter_name: Name of the adapter (e.g., "servicenow")
            table: Table name being queried
            columns: Tuple of column names (use ("*",) for all)
            predicates: Tuple of (column, operator, value) tuples
            limit: Query limit
            offset: Query offset
            order_by: Tuple of (column, direction) tuples
            group_by: Tuple of column names for grouping
            
        Returns:
            16-character hex string cache key
        """
        # Normalize inputs for consistent hashing
        columns = columns or ("*",)
        predicates = predicates or ()
        order_by = order_by or ()
        group_by = group_by or ()
        
        # Sort predicates for consistency (order shouldn't matter for WHERE)
        sorted_predicates = tuple(sorted(predicates, key=lambda p: (p[0], p[1], str(p[2]))))
        
        key_parts = [
            f"adapter:{adapter_name}",
            f"table:{table}",
            f"columns:{columns}",
            f"predicates:{sorted_predicates}",
            f"limit:{limit}",
            f"offset:{offset}",
            f"order_by:{order_by}",
            f"group_by:{group_by}",
        ]
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]
    
    def get(self, key: str) -> Optional[pa.Table]:
        """
        Get a cached result if it exists and hasn't expired.
        
        Args:
            key: Cache key (from generate_key)
            
        Returns:
            Cached Arrow table or None if not found/expired
        """
        if not self.config.enabled:
            # Still track misses when cache is disabled for accurate statistics
            self._stats.misses += 1
            return None
        
        with self._lock:
            if key not in self._cache:
                self._stats.misses += 1
                logger.debug("Cache miss: key=%s", key)
                return None
            
            entry = self._cache[key]
            
            # Check expiration
            if entry.is_expired:
                self._remove_entry(key)
                self._stats.misses += 1
                logger.debug("Cache expired: key=%s, age=%.1fs", key, entry.age_seconds)
                return None
            
            # Move to end (most recently used) for LRU
            self._cache.move_to_end(key)
            entry.hit_count += 1
            self._stats.hits += 1
            
            logger.debug(
                "Cache hit: key=%s, hit_count=%d, remaining_ttl=%.1fs",
                key, entry.hit_count, entry.remaining_ttl
            )
            
            return entry.data
    
    def put(
        self, 
        key: str, 
        data: pa.Table, 
        ttl: Optional[float] = None,
        adapter_name: str = None,
        table_name: str = None,
    ) -> None:
        """
        Store a query result in cache.
        
        Args:
            key: Cache key (from generate_key)
            data: Arrow table to cache
            ttl: Optional TTL override (uses adapter or default TTL if not provided)
            adapter_name: Adapter name for per-adapter TTL lookup and invalidation
            table_name: Table name for invalidation filtering
        """
        if not self.config.enabled:
            return
        
        if data is None:
            return
        
        with self._lock:
            # Determine TTL
            if ttl is None:
                if adapter_name:
                    ttl = self.config.get_ttl_for_adapter(adapter_name)
                else:
                    ttl = self.config.default_ttl
            
            # Calculate size
            size_bytes = data.nbytes
            
            # Evict entries if needed to make room
            self._evict_if_needed(size_bytes)
            
            # Create and store entry with metadata
            entry = CacheEntry(
                data=data,
                created_at=time.time(),
                ttl=ttl,
                size_bytes=size_bytes,
                adapter_name=adapter_name or "",
                table_name=table_name or "",
            )
            
            # If key exists, remove old entry first
            if key in self._cache:
                self._remove_entry(key)
            
            self._cache[key] = entry
            self._total_size += size_bytes
            self._update_stats()
            
            logger.debug(
                "Cache put: key=%s, size=%.2fMB, ttl=%.1fs, total_entries=%d",
                key, size_bytes / (1024 * 1024), ttl, len(self._cache)
            )
    
    def invalidate(
        self, 
        pattern: str = None, 
        adapter: str = None,
        table: str = None,
    ) -> int:
        """
        Invalidate cache entries matching criteria.
        
        Args:
            pattern: Invalidate entries where key contains this pattern
            adapter: Invalidate all entries for this adapter
            table: Invalidate all entries for this table
            
        Returns:
            Number of entries invalidated
            
        Note:
            If all arguments are None, clears the entire cache.
        """
        with self._lock:
            # Clear all if no filters specified
            if pattern is None and adapter is None and table is None:
                count = len(self._cache)
                self._cache.clear()
                self._total_size = 0
                self._stats.invalidations += count
                self._update_stats()
                logger.info("Cache cleared: %d entries invalidated", count)
                return count
            
            # Find matching keys using entry metadata
            keys_to_remove = []
            for key, entry in list(self._cache.items()):
                should_remove = False
                
                if pattern and pattern in key:
                    should_remove = True
                
                # Use stored metadata for adapter/table matching
                if adapter and entry.adapter_name == adapter:
                    should_remove = True
                
                if table and entry.table_name == table:
                    should_remove = True
                
                if should_remove:
                    keys_to_remove.append(key)
            
            # Remove matching entries
            for key in keys_to_remove:
                self._remove_entry(key)
                self._stats.invalidations += 1
            
            self._update_stats()
            
            if keys_to_remove:
                logger.info(
                    "Cache invalidated: %d entries removed (pattern=%s, adapter=%s, table=%s)",
                    len(keys_to_remove), pattern, adapter, table
                )
            
            return len(keys_to_remove)
    
    def clear(self) -> int:
        """
        Clear all cache entries.
        
        Returns:
            Number of entries cleared
        """
        return self.invalidate()
    
    def _evict_if_needed(self, incoming_size: int) -> None:
        """
        Evict oldest entries if cache is full.
        
        Uses LRU (Least Recently Used) eviction policy.
        Evicts when either max_entries or max_memory_mb is exceeded.
        """
        max_bytes = self.config.max_memory_mb * 1024 * 1024
        
        # First, evict expired entries
        expired_keys = [k for k, v in self._cache.items() if v.is_expired]
        for key in expired_keys:
            self._remove_entry(key)
        
        # Then evict LRU entries until we have room
        while self._cache:
            # Check if we need to evict
            entries_ok = len(self._cache) < self.config.max_entries
            memory_ok = self._total_size + incoming_size <= max_bytes
            
            if entries_ok and memory_ok:
                break
            
            # Evict oldest (first) entry
            oldest_key = next(iter(self._cache))
            self._remove_entry(oldest_key)
            self._stats.evictions += 1
            
            logger.debug("Cache eviction: key=%s, total_entries=%d", oldest_key, len(self._cache))
    
    def _remove_entry(self, key: str) -> None:
        """Remove an entry from cache and update size tracking."""
        if key in self._cache:
            entry = self._cache[key]
            self._total_size -= entry.size_bytes
            del self._cache[key]
    
    def _update_stats(self) -> None:
        """Update the current stats snapshot."""
        self._stats.entries = len(self._cache)
        self._stats.size_mb = self._total_size / (1024 * 1024)
    
    @property
    def stats(self) -> CacheStats:
        """
        Get cache statistics.
        
        Returns:
            CacheStats object with current metrics
        """
        with self._lock:
            self._update_stats()
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
                invalidations=self._stats.invalidations,
                entries=self._stats.entries,
                size_mb=self._stats.size_mb,
            )
    
    def get_entries_info(self) -> List[Dict[str, Any]]:
        """
        Get information about all cache entries.
        
        Returns:
            List of entry info dictionaries
        """
        with self._lock:
            entries = []
            for key, entry in self._cache.items():
                entries.append({
                    "key": key,
                    "size_mb": round(entry.size_bytes / (1024 * 1024), 3),
                    "age_seconds": round(entry.age_seconds, 1),
                    "remaining_ttl": round(entry.remaining_ttl, 1),
                    "hit_count": entry.hit_count,
                    "expired": entry.is_expired,
                    "rows": len(entry.data) if entry.data is not None else 0,
                })
            return entries
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        stats = self.stats
        return (
            f"<QueryCache enabled={self.config.enabled} "
            f"entries={stats.entries} size={stats.size_mb:.1f}MB "
            f"hit_rate={stats.hit_rate:.1f}%>"
        )


# Convenience function for creating cache from simple parameters
def create_cache(
    enabled: bool = True,
    ttl: float = 300.0,
    max_entries: int = 1000,
    max_memory_mb: float = 512.0,
    **kwargs,
) -> QueryCache:
    """
    Create a QueryCache with simple parameters.
    
    Args:
        enabled: Whether caching is enabled
        ttl: Default TTL in seconds
        max_entries: Maximum cache entries
        max_memory_mb: Maximum cache size in MB
        **kwargs: Additional CacheConfig parameters
        
    Returns:
        Configured QueryCache instance
    """
    config = CacheConfig(
        enabled=enabled,
        default_ttl=ttl,
        max_entries=max_entries,
        max_memory_mb=max_memory_mb,
        **kwargs,
    )
    return QueryCache(config)
