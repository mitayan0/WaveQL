# Performance Tuning

## 1. Pushdown (Critical)
Always filter in SQL to reduce network I/O.
*   ✅ `SELECT * FROM incident WHERE active=true` (Fetches subset)
*   ❌ `SELECT * FROM incident` (Fetches ALL)

## 2. Column Selection
*   ✅ `SELECT number, priority FROM incident` (Fast)
*   ❌ `SELECT * FROM incident` (Slow, fetches huge text blobs)

## 3. Concurrency
Use `async` for parallel requests across multiple adapters.
```python
async with waveql.connect_async(...) as conn:
    await asyncio.gather(
        conn.execute("SELECT ... FROM servicenow..."),
        conn.execute("SELECT ... FROM salesforce...")
    )
```

## 4. Connection Reuse
Keep `conn` object alive. It reuses the TCP connection (Keep-Alive) and SSL session.

## 5. Aggregation
| Method | Performance | Note |
| :--- | :--- | :--- |
| `COUNT(*)` | ⚡ Instant | Uses API metadata (e.g. `X-Total-Count`) |
| `GROUP BY` | 🐢 Slow | Requires full table scan + client-side PyArrow |

## 6. Pagination
Default batch size is 100. Increase for throughput:
```python
conn = waveql.connect(..., page_size=1000)
```
