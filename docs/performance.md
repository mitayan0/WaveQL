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

## 4. Connection Pooling
WaveQL uses a persistent `httpx.Client` session. Re-using the `WaveQLConnection` object across multiple queries allows you to re-use the underlying TCP connection (Keep-Alive), saving SSL handshake time.

```python
# Create connection ONCE
conn = waveql.connect(...)

for i in range(10):
    # Re-use it
    conn.execute(...)
```

## 5. Async for Concurrency
If you need to query multiple tables or instances, use the `async` interface.

```python
async with waveql.connect_async(...) as conn:
    # Run these two concurrently
    task1 = conn.execute("SELECT ...")
    task2 = conn.execute("SELECT ...")
    await asyncio.gather(task1, task2)
```
