# Adapter Protocol Specification v1

All Adapters MUST inherit from `BaseAdapter` and implement the following.

## 1. Schema Discovery
`get_schema(self) -> Dict[str, pyarrow.Schema]`

**Requirements**:
-   **Must be Lazy**: Do not fetch schema until called.
-   **Must be Cached**: Subsequent calls must return memory-resident schema.
-   **Types**: MUST utilize `pyarrow` types (e.g., `pa.string()`, `pa.int64()`), NOT Python types.

## 2. The Fetch Method
`fetch(self, query: Query) -> Iterator[pyarrow.RecordBatch]`

**Input**:
-   `query`: A dataclass containing:
    -   `table`: str
    -   `columns`: List[str] (Projection)
    -   `filters`: List[Filter] (Predicates)
    -   `limit`: Optional[int]

**Behavior**:
-   **Generator**: MUST `yield` batches. Do not accumulate all data in a list.
-   **Pushdown Compliance**:
    -   IF the adapter supports filtering, it MUST apply `query.filters` remotely.
    -   IF it cannot apply a specific filter, it MUST ignore it (the engine will double-check locally), BUT it should log a warning.

## 3. Authentication
-   Credentials MUST be passed via `__init__`.
-   Adapters must NOT read `os.environ` directly. The `Config` singleton handles this before instantiation.

## 4. Error Handling
-   Network errors -> Raise `AdapterConnectionError`
-   Auth errors -> Raise `AdapterAuthError`
-   Missing table -> Raise `AdapterTableNotFoundError`

## Recommended Folder Structure
```
waveql/
  adapters/
    my_service/
      __init__.py
      adapter.py  <-- Implementation
      tests/      <-- Unit tests
```
