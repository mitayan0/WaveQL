# Result Caching

In-memory LRU cache with TTL. Thread-safe.

## Configuration

```python
from waveql import CacheConfig

config = CacheConfig(
    enabled=True,
    default_ttl=300,
    max_entries=1000,
    max_memory_mb=512,
    adapter_ttl={"servicenow": 60},
    exclude_tables=["audit_log"]
)
conn = waveql.connect(..., cache_config=config)
```

## Keys
Key = `Hash(Adapter + Table + Columns + WHERE + LIMIT + ORDER + GROUP)`

## Invalidation
*   **Write-based**: `INSERT`/`UPDATE`/`DELETE` automatically invalidates the table.
*   **Manual**:
    ```python
    conn.invalidate_cache()             # All
    conn.invalidate_cache(table="inc")  # Specific
    ```
*   **Stats**:
    ```python
    print(conn.cache_stats.hit_rate)
    ```
