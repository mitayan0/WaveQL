# Adapters

## Capability Matrix

| Adapter | URI Scheme | Pushdown | Aggregation | Write | Schema | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ServiceNow** | `servicenow://` | `sysparm_query` | ✅ Server | ✅ CRUD | ✅ Dynamic | Parallel fetch, CDC |
| **Salesforce** | `salesforce://` | `SOQL` | ✅ Server | ✅ CRUD | ✅ Dynamic | Bulk API |
| **Jira** | `jira://` | `JQL` | ✅ Client | ✅ Issue | ✅ Dynamic | Projects/Issues/Users |
| **HubSpot** | `hubspot://` | Search API | ✅ Smart* | ✅ CRUD | ✅ Dynamic | *Optimized COUNT(*), Native Async |
| **Shopify** | `shopify://` | Partial | ✅ Smart* | ✅ CRUD | ⚠️ Inferred | *Count Endpoint |
| **Zendesk** | `zendesk://` | Search API | ✅ Smart* | ✅ CRUD | ⚠️ Inferred | |
| **Stripe** | `stripe://` | Search/List | ✅ Smart* | ✅ CRUD | ⚠️ Inferred | *Robust retry logic |
| **Generic REST** | `http://` | Params | ❌ | ✅ CRUD | ✅ Config | *ORDER BY support |
| **SQL DB** | `postgresql://` | Full SQL | ✅ Server | ✅ CRUD | ✅ Dynamic | SQLAlchemy wrapper |
| **Cloud Storage** | `s3://`, `gs://` | DuckDB | ✅ DuckDB | ❌ Read-Only | ✅ Parquet | Delta Lake / Iceberg |
| **Generic REST** | `http://` | Params | ❌ | ✅ CRUD | ✅ Config | |

---

## 1. ServiceNow
*   **URI**: `servicenow://instance.service-now.com`
*   **Pushdown**: `sysparm_query` (e.g., `active=true^priority=1`)
*   **Tables**: All system tables (`incident`, `cmdb_ci`, etc.)
*   **Notes**: `display_value` supported in connection params.

```sql
SELECT number, short_description 
FROM incident 
WHERE active = true AND priority IN (1, 2)
```

## 2. Salesforce
*   **URI**: `salesforce://instance.salesforce.com`
*   **Pushdown**: Native SOQL.
*   **Bulk API**: Auto-switches to Bulk API 2.0 for large INSERTs.
*   **Async**: Full `async/await` support.

```sql
SELECT Id, Name FROM Account WHERE Industry = 'Tech'
```

## 3. Jira
*   **URI**: `jira://domain.atlassian.net`
*   **Pushdown**: JQL translation (`WHERE status='Done'` -> `status = "Done"`).
*   **Virtual Tables**: `issues` (the main one), `projects`, `users`, `comments`.

```sql
SELECT key, summary FROM issues WHERE project = 'KAN'
```

## 4. HubSpot
*   **URI**: `hubspot://api.hubapi.com`
*   **Pushdown**: Search API v3 filters.
*   **Pagination**: Cursor-based.
*   **Smart COUNT**: `SELECT COUNT(*) FROM contacts` -> 1 API call (uses metadata).
*   **Async**: Full native async support with `httpx.AsyncClient`.

```sql
SELECT firstname, email FROM contacts WHERE email LIKE '%@example.com'
```

### HubSpot Async Operations

```python
# Async fetch
result = await adapter.fetch_async("contacts", limit=100)

# Async CRUD
await adapter.insert_async("contacts", {"email": "new@example.com"})
await adapter.update_async("contacts", {"firstname": "John"},
                           [Predicate(column="id", operator="=", value="123")])
await adapter.delete_async("contacts",
                           [Predicate(column="id", operator="=", value="123")])
```

## 5. Cloud Storage (S3/GCS)
*   **URI**: `s3://bucket/`, `gs://bucket/`, `azure://container/`
*   **Engine**: Embedded DuckDB.
*   **Formats**: Parquet, CSV, JSON, **Delta Lake**, **Iceberg**.

```sql
SELECT * FROM "s3://logs/*.parquet" WHERE year = 2024
```

## 6. Generic REST
*   **URI**: `http://api.example.com`
*   **Config**: Map tables to endpoints manually via `RESTAdapter`.
*   **Pushdown**: Param-based filtering.
*   **Features**:
  - `list_tables()` returns configured endpoints
  - Client-side `ORDER BY` support
  - Full CRUD operations (INSERT/UPDATE/DELETE)

```python
adapter = RESTAdapter(
    "https://api.com",
    endpoints={"users": "/v1/users"},
    data_path="results"  # JSON path to data array
)
cursor.execute("SELECT * FROM users WHERE role = 'admin' ORDER BY name")
```

### RESTAdapter Configuration

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `endpoints` | `Dict[str, Dict]` | Table name to endpoint config mapping |
| `data_path` | `str` | JSON path to data array (e.g., "results", "data.items") |
| `timeout` | `int` | Request timeout in seconds (default: 30) |
| `max_auto_fetch` | `int` | Safety limit for auto-pagination (default: 5000) |

### Endpoint Configuration

```python
endpoints = {
    "users": {
        "path": "/v1/users",
        "method": "GET",
        "supports_filter": True,
        "supports_limit": True,
        "supports_offset": True,
        "supports_like": False,
        "filter_format": "query",  # or "json"
        "id_field": "id",  # for UPDATE/DELETE
    }
}
```

## Custom Adapters
Inherit `BaseAdapter`. See `AGENTS.md` for implementation details.
```python
from waveql.adapters import BaseAdapter, register_adapter
class MyAdapter(BaseAdapter): ...
register_adapter("myservice", MyAdapter)
```
