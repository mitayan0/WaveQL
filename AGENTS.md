# WaveQL agents

Hello agent. You are one of the most talented programmers of your generation.

You are looking forward to putting those talents to use to improve WaveQL.

## philosophy

WaveQL is a **universal SQL connector** focused on unifying all APIs under a single, zero-copy SQL interface.

*   **Universal SQL:** If it can be a table, it IS a table.
*   **Zero-Copy:** Use `pyarrow` and `duckdb`. Avoid Python loops/dicts where possible.
*   **Pushdown:** Always push `WHERE`, `ORDER BY`, `LIMIT` to the API.
*   **Async Native:** All I/O is `async`. Sync wrappers are just conveniences.

Every line must earn its keep. Prefer readability over cleverness. We believe that if carefully designed, 10 lines can have the impact of 1000.

## style

Use **4-space indentation**, and keep lines to a maximum of **100 characters**. Match the existing style.

## codebase

| Path | Purpose |
| :--- | :--- |
| `waveql/adapters/base.py` | **The Contract**. All adapters inherit `BaseAdapter`. |
| `waveql/connection.py` | DB-API 2.0 Entry point. |
| `waveql/query_planner.py` | `sqlglot` -> `QueryInfo` (predicate extraction). |
| `waveql/auth/` | `AuthManager` implementations. |
| `tests/` | `pytest` suite. Use `respx` for mocking. |

## implementation guide

### Create an Adapter

Inherit from `waveql.adapters.base.BaseAdapter`.
**CRITICAL:** You MUST implement both `get_schema` and `fetch`.
Since `fetch` is abstract in `BaseAdapter`, providing only `fetch_async` will cause instantiation to fail. Use this wrapper pattern:

```python
from waveql.adapters.base import BaseAdapter
from waveql.schema_cache import ColumnInfo
from waveql.query_planner import Predicate
from typing import List, Any
import pyarrow as pa
import asyncio

class MyAdapter(BaseAdapter):
    adapter_name = "my_adapter"

    def get_schema(self, table: str) -> List[ColumnInfo]:
        # Return list of ColumnInfo(name, type)
        # e.g. return [ColumnInfo("id", pa.string())]
        ...

    async def fetch_async(
        self, 
        table: str, 
        columns: List[str] = None, 
        predicates: List[Predicate] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        # 1. Translate predicates to API params
        # 2. Fetch using self._get_async_client()
        # 3. Return PyArrow Table
        ...

    def fetch(self, *args, **kwargs) -> pa.Table:
        # Required sync wrapper
        return asyncio.run(self.fetch_async(*args, **kwargs))
```

### Constraints

*   **No Pandas in Core:** Use `pyarrow` for internal data movement.
*   **No Global State:** Attach state to the `Connection` object.
*   **Secrets:** All credentials must be wrapped in `pydantic.SecretStr` or `waveql.auth.SecretStr`.
*   **Type Safety:** Use `from __future__ import annotations`.
