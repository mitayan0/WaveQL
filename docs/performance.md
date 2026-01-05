# Performance Tuning

Querying APIs via SQL introduces different performance characteristics than querying a local database.

## 1. Predicate Pushdown (The #1 Rule)
Always try to filter **as much as possible** in the `WHERE` clause.

*   **Bad**:
    ```sql
    SELECT * FROM incident
    -- Fetches ALL incidents, then filters locally python-side
    ```
    *Note: In worst cases, this might try to download 1M records.*

*   **Good**:
    ```sql
    SELECT * FROM incident WHERE created_on > '2025-01-01'
    -- Only fetches records from this year
    ```

WaveQL aggressively attempts to translate your `WHERE` clause into the API's native query language.

## 2. Column Selection
Only select the columns you need.
```sql
-- Bad
SELECT * FROM incident

-- Good
SELECT number, short_description FROM incident
```
Many APIs (like ServiceNow) will return extensive metadata, links, and text blobs for every record if `*` is used, significantly increasing network latency and memory usage.

## 3. Pagination & Batch Sizes
WaveQL handles pagination automatically, but you can tune the batch size.

*   **Default**: usually `limit=100` or API default.
*   **Tuning**: Larger batches are generally better for throughput, but increase memory pressure.

```python
# Pass custom params to adapter
conn = waveql.connect(..., page_size=1000)
```

## 4. Parallel Fetching (ServiceNow)
For large datasets, the ServiceNow adapter automatically triggers parallel fetching (fetching multiple pages concurrently) when requesting more data than a single page.
*   **Default**: 4 worker threads.
*   **Tuning**: Increase `max_parallel` for higher throughput on high-bandwidth connections.

```python
conn = waveql.connect(
    "servicenow://...",
    max_parallel=10
)
```

## 5. Connection Pooling
WaveQL uses a persistent `httpx.Client` session. Re-using the `WaveQLConnection` object across multiple queries allows you to re-use the underlying TCP connection (Keep-Alive), saving SSL handshake time.

```python
# Create connection ONCE
conn = waveql.connect(...)

for i in range(10):
    # Re-use it
    conn.execute(...)
```

## 6. Async for Concurrency
If you need to query multiple tables or instances, use the `async` interface.

```python
async with waveql.connect_async(...) as conn:
    # Run these two concurrently
    task1 = conn.execute("SELECT ...")
    task2 = conn.execute("SELECT ...")
    await asyncio.gather(task1, task2)
```

## 7. Query Result Caching

WaveQL includes a built-in query cache that dramatically improves performance for repeated queries.

### Default Caching

Caching is enabled by default with a 5-minute TTL:

```python
conn = waveql.connect("servicenow://...")

# First query: ~500ms (API call)
cursor.execute("SELECT * FROM incident WHERE active=true")

# Second query: ~1ms (from cache!)
cursor.execute("SELECT * FROM incident WHERE active=true")
```

### Configure TTL

```python
# 1 minute cache for frequently changing data
conn = waveql.connect("servicenow://...", cache_ttl=60)

# Disable caching for real-time data
conn = waveql.connect("servicenow://...", enable_cache=False)
```

### Per-Adapter TTL

Different data sources have different freshness requirements:

```python
from waveql import CacheConfig

config = CacheConfig(
    default_ttl=300,
    adapter_ttl={
        "servicenow": 60,   # Fast-changing tickets: 1 min
        "cmdb": 3600,       # Slow-changing CMDB: 1 hour
    }
)
conn = waveql.connect("servicenow://...", cache_config=config)
```

### Monitor Cache Performance

```python
stats = conn.cache_stats
print(f"Hit rate: {stats.hit_rate:.1f}%")
print(f"Size: {stats.size_mb:.1f}MB")
print(f"Entries: {stats.entries}")
```

### Cache Invalidation

```python
# Clear all cache
conn.invalidate_cache()

# Clear specific table
conn.invalidate_cache(table="incident")

# Automatically invalidated on INSERT/UPDATE/DELETE
cursor.execute("UPDATE incident SET priority=1 WHERE number='INC001'")
# Cache for 'incident' table is automatically cleared
```

See the [Caching Documentation](caching.md) for complete details.

