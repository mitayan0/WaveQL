# WaveQL for Agents

## Philosophy
*   **Universal SQL:** If it can be a table, it IS a table.
*   **Zero-Copy:** Use Arrow/DuckDB internal buffers. Do NOT convert to Python objects unless asked.
*   **Pushdown:** Always push `WHERE`, `ORDER BY`, `LIMIT` to the API.
*   **Async Native:** All I/O is `async`. Sync wrappers are just conveniences.

## Codebase Map
| Path | Purpose |
| :--- | :--- |
| `waveql/adapters/base.py` | The contract. All adapters inherit from `BaseAdapter`. |
| `waveql/adapters/` | Implementations (ServiceNow, Salesforce, etc.). |
| `waveql/query_planner.py` | `sqlglot` -> `QueryInfo` (predicate extraction). |
| `waveql/connection.py` | DB-API 2.0 Entry point. |
| `waveql/transaction/` | Saga pattern coordinator (not 2PC). |

## Common Tasks

### 1. Add Adapter
Inherit `BaseAdapter` or `HttpAdapter`. Implement `fetch()` and `get_schema()`.
```python
class MyAdapter(HttpAdapter):
    def get_schema(self, table): ...
    async def fetch_async(self, table, cols, preds, limit, ...): ...
```

### 2. Add Test
Use `respx` for async HTTP mocking.
```python
@respx.mock
async def test_fetch():
    respx.get("...").respond(json={...})
    await cursor.execute("SELECT ...")
```

### 3. Run Tests
`pytest tests/`

## Constraints
*   **No Pandas in Core:** Core logic uses PyArrow/DuckDB. Pandas is only for user export (`.to_df()`).
*   **No Global State:** Everything hangs off `WaveQLConnection`.
*   **SecretStr:** All credentials must be wrapped in `SecretStr`.
