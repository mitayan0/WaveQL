# Query Result Caching

WaveQL includes a built-in query result cache that significantly improves performance for repeated queries while reducing API call volume.

## Overview

The cache stores query results in memory using an **LRU (Least Recently Used)** eviction strategy with **TTL (Time-To-Live)** expiration. This is particularly valuable when:

- Running dashboard-style repeated queries
- Developing and testing queries iteratively
- Working with rate-limited APIs
- Building applications that query the same data frequently

## Quick Start

Caching is **enabled by default** with a 5-minute TTL:

```python
import waveql

# Default: caching enabled, 5-minute TTL
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    username="admin",
    password="secret"
)

# First query - fetches from API
cursor = conn.cursor()
cursor.execute("SELECT * FROM incident WHERE active=true LIMIT 100")
print(f"First query: {cursor.rowcount} rows")

# Second query - served from cache (instant!)
cursor.execute("SELECT * FROM incident WHERE active=true LIMIT 100")
print(f"Second query: {cursor.rowcount} rows (from cache)")

# Check cache statistics
stats = conn.cache_stats
print(f"Cache hits: {stats.hits}, misses: {stats.misses}, hit rate: {stats.hit_rate:.1f}%")
```

## Configuration Options

### Simple TTL Configuration

```python
# Set cache TTL to 1 minute
conn = waveql.connect("servicenow://...", cache_ttl=60)

# Disable caching entirely
conn = waveql.connect("servicenow://...", enable_cache=False)
```

### Advanced Configuration with CacheConfig

For fine-grained control, use the `CacheConfig` class:

```python
from waveql import CacheConfig

config = CacheConfig(
    enabled=True,             # Enable/disable caching
    default_ttl=300,          # Default TTL: 5 minutes
    max_entries=1000,         # Maximum cached queries
    max_memory_mb=512,        # Maximum cache size in MB
    adapter_ttl={             # Per-adapter TTL overrides
        "servicenow": 60,     # ServiceNow: 1 minute
        "jira": 120,          # Jira: 2 minutes
    },
    exclude_tables=[          # Tables to never cache
        "audit_log",
        "sys_journal",
    ],
)

conn = waveql.connect("servicenow://...", cache_config=config)
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable/disable caching |
| `default_ttl` | `float` | `300.0` | Default TTL in seconds (5 minutes) |
| `max_entries` | `int` | `1000` | Maximum number of cached queries |
| `max_memory_mb` | `float` | `512.0` | Maximum cache size in megabytes |
| `adapter_ttl` | `Dict[str, float]` | `{}` | Per-adapter TTL overrides |
| `exclude_tables` | `List[str]` | `[]` | Tables to never cache |

## Cache Statistics

Monitor cache performance with the `cache_stats` property:

```python
stats = conn.cache_stats

print(f"Hits: {stats.hits}")
print(f"Misses: {stats.misses}")
print(f"Hit Rate: {stats.hit_rate:.1f}%")
print(f"Evictions: {stats.evictions}")
print(f"Entries: {stats.entries}")
print(f"Size: {stats.size_mb:.2f} MB")

# Or as a dictionary
print(stats.to_dict())
# {'hits': 15, 'misses': 3, 'evictions': 0, 'hit_rate': '83.3%', 'size_mb': 1.25}
```

## Cache Invalidation

### Manual Invalidation

Invalidate cache entries when you know data has changed:

```python
# Clear all cache
conn.invalidate_cache()

# Clear cache for specific adapter
conn.invalidate_cache(adapter="servicenow")

# Clear cache for specific table
conn.invalidate_cache(table="incident")
```

### Automatic Invalidation

WaveQL automatically invalidates cache entries when you perform write operations:

```python
# This query result is cached
cursor.execute("SELECT * FROM incident")

# INSERT/UPDATE/DELETE automatically invalidates the cache for that table
cursor.execute("INSERT INTO incident (short_description) VALUES ('New issue')")

# Next SELECT will fetch fresh data from API
cursor.execute("SELECT * FROM incident")
```

### Runtime TTL Updates

Adjust per-adapter TTL at runtime:

```python
# Set ServiceNow cache to expire after 30 seconds
conn.set_cache_ttl("servicenow", 30)

# Set Jira cache to expire after 2 minutes
conn.set_cache_ttl("jira", 120)
```

## How It Works

### Cache Key Generation

Cache keys are generated from query components:

- Adapter name (e.g., "servicenow")
- Table name
- Selected columns
- WHERE predicates (order-independent)
- LIMIT and OFFSET
- ORDER BY clause
- GROUP BY clause

Two queries with identical components will share the same cache entry.

### LRU Eviction

When the cache reaches `max_entries` or `max_memory_mb`, the least recently used entries are evicted first. Accessing a cached entry moves it to the "most recently used" position.

### TTL Expiration

Each cache entry has an individual TTL. When accessed after expiration, the entry is automatically removed and fresh data is fetched from the source.

### Thread Safety

The cache is fully thread-safe, using `threading.RLock` for all operations. Multiple threads can safely read from and write to the cache concurrently.

## Best Practices

### 1. Match TTL to Data Volatility

```python
# Frequently changing data - short TTL
conn.set_cache_ttl("servicenow", 60)  # 1 minute

# Slowly changing reference data - longer TTL
conn.set_cache_ttl("cmdb", 3600)  # 1 hour
```

### 2. Exclude Audit/Log Tables

```python
config = CacheConfig(
    exclude_tables=["audit_log", "sys_journal", "history"]
)
```

### 3. Monitor Cache Performance

```python
# Log cache stats periodically
import logging

def log_cache_stats(conn):
    stats = conn.cache_stats
    logging.info(
        f"Cache: {stats.entries} entries, "
        f"{stats.size_mb:.1f}MB, "
        f"{stats.hit_rate:.1f}% hit rate"
    )
```

### 4. Invalidate Before Critical Reads

```python
# Ensure fresh data for important operations
conn.invalidate_cache(table="incident")
cursor.execute("SELECT * FROM incident WHERE priority=1")
```

### 5. Size Memory Appropriately

Estimate cache memory based on:
- Average query result size
- Number of unique queries
- Available system memory

```python
# For memory-constrained environments
config = CacheConfig(
    max_entries=100,
    max_memory_mb=64
)

# For high-performance environments
config = CacheConfig(
    max_entries=5000,
    max_memory_mb=2048
)
```

## Direct Cache Access

For advanced use cases, access the cache directly:

```python
# Access the cache object
cache = conn.cache

# Get detailed entry information
entries = cache.get_entries_info()
for entry in entries:
    print(f"Key: {entry['key']}")
    print(f"  Size: {entry['size_mb']:.3f} MB")
    print(f"  Rows: {entry['rows']}")
    print(f"  Age: {entry['age_seconds']:.1f}s")
    print(f"  Remaining TTL: {entry['remaining_ttl']:.1f}s")
    print(f"  Hit Count: {entry['hit_count']}")
```

## Async Support

Caching works identically with async connections:

```python
import asyncio
from waveql import connect_async, CacheConfig

async def main():
    config = CacheConfig(default_ttl=60)
    
    async with await connect_async(
        "servicenow://...",
        cache_config=config
    ) as conn:
        cursor = await conn.cursor()
        
        # First query - API call
        await cursor.execute("SELECT * FROM incident")
        
        # Second query - from cache
        await cursor.execute("SELECT * FROM incident")
        
        print(conn.cache_stats.to_dict())

asyncio.run(main())
```

## Disabling Cache for Specific Queries

If you need fresh data for a specific query while keeping caching enabled for others:

```python
# Option 1: Invalidate before query
conn.invalidate_cache(table="incident")
cursor.execute("SELECT * FROM incident")

# Option 2: Use a separate connection with caching disabled
fresh_conn = waveql.connect("servicenow://...", enable_cache=False)
fresh_cursor = fresh_conn.cursor()
fresh_cursor.execute("SELECT * FROM incident")  # Always hits API
```

## Performance Impact

| Scenario | Without Cache | With Cache |
|----------|---------------|------------|
| First query | ~500ms (API) | ~500ms (API) |
| Repeated query | ~500ms (API) | ~1ms (cache) |
| Dashboard refresh (10 queries) | ~5000ms | ~504ms |
| Development iteration (50 queries) | ~25000ms | ~549ms |

*Times are illustrative and depend on network latency and API response times.*
