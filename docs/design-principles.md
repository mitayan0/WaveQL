# WaveQL Design Principles

> **The technical philosophy behind WaveQL's architecture and design decisions.**

---

## Core Philosophy

**"SQL is the universal language for data. Every data source should speak it."**

WaveQL exists because data engineers shouldn't need to learn a new API for every SaaS platform. If you know SQL, you should be able to query ServiceNow, Salesforce, Jira, and any REST API using the same skills you've used for decades.

---

## 1. SQL as the Universal Interface

**Principle:** *Meet developers where they are — don't force them to learn new query languages.*

### Why SQL?

- **40+ years of battle-tested syntax** — developers already know it
- **Tooling ecosystem** — works with SQLAlchemy, Pandas, Superset, Jupyter
- **Declarative** — describe *what* you want, not *how* to get it
- **Composable** — JOINs, subqueries, CTEs work across any source

### Implementation

```python
# The same SQL works for any adapter
cursor.execute("SELECT * FROM servicenow.incident WHERE priority = 1")
cursor.execute("SELECT * FROM jira.issues WHERE project = 'PROJ'")
cursor.execute("SELECT * FROM salesforce.Account WHERE Industry = 'Tech'")
```

**Design Decision:** We implement [DB-API 2.0](https://peps.python.org/pep-0249/) for maximum compatibility with existing Python tooling.

---

## 2. Push Work to the Edge (Predicate Pushdown)

**Principle:** *Never fetch data you don't need. Optimize at the source, not the client.*

### The Problem

Fetching 100,000 rows from an API to filter down to 50 results is:
- **Slow** — network latency dominates
- **Expensive** — API rate limits get consumed
- **Wasteful** — memory and CPU used for nothing

### The Solution

WaveQL translates SQL predicates into native API filters:

| SQL | ServiceNow | Salesforce | Jira |
|-----|------------|------------|------|
| `WHERE status = 'open'` | `sysparm_query=status=open` | `WHERE Status = 'open'` | `status = "open"` |
| `WHERE priority < 3` | `priority<3` | `Priority < 3` | `priority < 3` |
| `LIMIT 100` | `sysparm_limit=100` | `LIMIT 100` | `maxResults=100` |
| `ORDER BY created DESC` | `sysparm_orderby=created` | `ORDER BY CreatedDate DESC` | `ORDER BY created DESC` |

### Capability Detection

Not all APIs support all operations. WaveQL detects adapter capabilities:

```python
adapter_capabilities = {
    "servicenow": {"supports_or": True, "supports_aggregation": True},
    "jira": {"supports_or": True, "supports_aggregation": False},
    "rest": {"supports_or": False, "supports_aggregation": False},
}
```

**Fallback Strategy:** When an adapter doesn't support an operation, WaveQL falls back to local DuckDB execution with a warning logged.

---

## 3. Adapter Pattern with Capability Detection

**Principle:** *One interface, many implementations. Abstract the interface, expose the capabilities.*

### Architecture

```
BaseAdapter (Abstract)
    ├── ServiceNowAdapter
    ├── SalesforceAdapter  
    ├── JiraAdapter
    ├── RESTAdapter
    ├── FileAdapter
    └── SQLAdapter
```

### Each Adapter Declares

```python
class ServiceNowAdapter(BaseAdapter):
    adapter_name = "servicenow"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    supports_batch = True
```

### Benefits

1. **Consistent API** — All adapters use `fetch()`, `insert()`, `update()`, `delete()`
2. **Self-describing** — Capabilities are queryable at runtime
3. **Extensible** — New adapters plug in without changing core code
4. **Testable** — Mock adapters work identically to real ones

---

## 4. Apache Arrow as the Data Backbone

**Principle:** *Use modern data primitives, not legacy formats.*

### Why Arrow?

| Feature | Benefit |
|---------|---------|
| **Columnar format** | Efficient for analytics queries |
| **Zero-copy** | Share data with Pandas/DuckDB without serialization |
| **Type-rich** | Timestamps, decimals, nested structs |
| **Language-agnostic** | Same format in Python, Rust, C++, Java |

### Data Flow

```
API Response (JSON)
       ↓
   PyArrow Table
       ↓
   ┌─────────────────────────────────────┐
   │  Zero-copy to:                      │
   │  • Pandas DataFrame                 │
   │  • DuckDB for JOINs                 │
   │  • Parquet for caching              │
   └─────────────────────────────────────┘
```

**Design Decision:** All adapters return `pyarrow.Table`. Never raw dicts or lists.

---

## 5. DuckDB as the Local Compute Engine

**Principle:** *Leverage specialized tools rather than reinventing wheels.*

### The Challenge

APIs don't support `JOIN`. How do you join ServiceNow incidents with a local CSV of VIP users?

### The Solution

WaveQL embeds DuckDB as an in-memory SQL engine:

```python
# 1. Fetch from ServiceNow (predicate pushed)
incidents = servicenow_adapter.fetch("incident", predicates=[...])

# 2. Load local CSV
vips = duckdb.read_csv("vips.csv")

# 3. Execute JOIN in DuckDB
result = duckdb.execute("""
    SELECT i.*, v.name 
    FROM incidents i
    JOIN vips v ON i.caller_id = v.user_id
""")
```

### Why DuckDB?

- **Zero-copy Arrow integration** — no serialization overhead
- **Full SQL support** — window functions, CTEs, arrays
- **Embedded** — no server, no setup
- **Fast** — vectorized execution engine

---

## 6. Async-First, Sync Supported

**Principle:** *Design for scale, support for convenience.*

### Dual API

```python
# Sync (simple scripts)
conn = waveql.connect("servicenow://...")
cursor = conn.cursor()
cursor.execute("SELECT * FROM incident")

# Async (high-throughput apps)
conn = await waveql.connect_async("servicenow://...")
cursor = await conn.cursor()
await cursor.execute("SELECT * FROM incident")
```

### Implementation

- **HTTP Layer:** `httpx` for both sync and async
- **Concurrency:** `anyio` for portable async
- **Connection Pool:** Separate sync/async pools with HTTP/2 support

---

## 7. Incremental Enhancement

**Principle:** *Stability over velocity. Ship when ready, not when possible.*

### Versioning Strategy

We follow [Semantic Versioning](https://semver.org/) with a conservative approach:

| Version | Meaning |
|---------|---------|
| `0.x.y` | Pre-1.0 development (breaking changes allowed) |
| Patch (`0.1.x`) | Bug fixes AND new backward-compatible features |
| Minor (`0.x.0`) | Major feature milestone completion |
| Major (`1.0.0`) | Production-ready, stable API guarantee |

### Release Philosophy

- **Small, focused releases** — Each release has a clear theme
- **All tests pass** — No release with failing tests
- **Documentation included** — Features aren't done until documented
- **Examples provided** — Show, don't just tell

---

## 8. Graceful Degradation

**Principle:** *Fail gracefully, never crash the user.*

### Fallback Hierarchy

```
1. Try optimized path (predicate pushdown)
   ↓ If not supported
2. Fall back to local execution (DuckDB)
   ↓ If data too large
3. Stream with pagination
   ↓ If rate limited
4. Retry with exponential backoff
   ↓ If all retries fail
5. Raise clear, actionable exception
```

### Error Design

```python
# Bad
raise Exception("API call failed")

# Good
raise RateLimitError(
    "ServiceNow rate limit exceeded",
    retry_after=60,
    suggestion="Consider using cache_ttl=300 to reduce API calls"
)
```

---

## 9. Caching as a First-Class Citizen

**Principle:** *Repeated queries should be instant.*

### Cache Hierarchy

```
Query → Cache Key Generation → SQLite Lookup
                                    ↓
                            (Miss) → Fetch from API → Store in Cache
                            (Hit)  → Return cached Arrow Table
```

### Design Decisions

- **SQLite for metadata** — Durable, no external dependencies
- **Arrow/Parquet for data** — Efficient, type-preserving
- **Per-adapter TTL** — Different sources have different freshness needs
- **Automatic invalidation** — Writes invalidate related cache entries

---

## 10. Schema Validation with Pydantic

**Principle:** *Validate early, fail fast with clear messages.*

### Why Pydantic (vs YAML)?

| Aspect | Pydantic | YAML |
|--------|----------|------|
| **Type safety** | ✅ Native Python types | ❌ Strings everywhere |
| **IDE support** | ✅ Autocomplete, type hints | ❌ No intellisense |
| **Validation** | ✅ Built-in, extensible | ❌ Requires separate library |
| **Error messages** | ✅ Detailed, field-level | ❌ Generic parse errors |
| **Serialization** | ✅ JSON, dict, ORM | ⚠️ Manual conversion |
| **Documentation** | ✅ Auto-generated JSON Schema | ❌ Manual |

### Implementation Pattern

```python
from pydantic import BaseModel, Field, validator

class DataContract(BaseModel):
    """Schema contract for a table."""
    table: str
    adapter: str
    columns: List[ColumnSchema]
    
    @validator('columns')
    def validate_columns(cls, v):
        if not v:
            raise ValueError("At least one column required")
        return v

class ColumnSchema(BaseModel):
    name: str
    type: Literal["string", "integer", "float", "boolean", "datetime"]
    nullable: bool = True
    constraints: Optional[List[str]] = None
```

**Decision:** Pydantic for programmatic contracts, with optional YAML/JSON export for human editing.

---

## 11. Streaming for Scale

**Principle:** *Process any data size without memory exhaustion.*

Large exports or analysis shouldn't require loading entire datasets into memory. WaveQL provides generator-based streaming with backpressure as a core primitive:

- **`stream_batches()`** - yields RecordBatches page-by-page as they arrive from the source.
- **`stream_to_file()`** - direct-to-Parquet export that pipes data directly to disk.
- **`BufferedAsyncStream`** - prefetching batches in the background while the consumer processes the current one.

### Implementation Pattern

```python
# Process millions of rows with 10MB memory usage
async for batch in cursor.stream_batches_async("SELECT * FROM large_table"):
    process_batch(batch) # Arrow zero-copy processing
```

---

## 12. Cloud-Native & Data Lakes

**Principle:** *Data lives everywhere. Access it where it is.*

WaveQL bridges the gap between SaaS APIs and modern data lakes. By leveraging DuckDB extension ecosystem, we treat object storage as a first-class data source:

- **S3, GCS, Azure Blob** - native httpfs support with predicate pushdown.
- **Delta Lake** - version-aware queries using the Delta Log.
- **Apache Iceberg** - standardized metadata-driven access.

---

## 13. Unified Authentication & Credential Chain

**Principle:** *Security should be robust yet effortless.*

Adapters shouldn't care where credentials come from. We implement a unified resolution chain for all cloud and API authentication:

- **Order of Resolution:** Explicit params → Environment variables → Config files (`credentials.yaml`) → IAM roles/Workload Identity.
- **Dynamic Refresh:** All tokens are auto-refreshed via the `AuthManager` before expiration.

---

## 14. Change Data Capture (CDC)

**Principle:** *Don't pull history when you only need updates.*

WaveQL moves beyond snapshots with a standard pattern for real-time data integration:

- **`stream_changes()`** - an async iterator that abstracts away polling and timestamp tracking.
- **State Persistence** - markers are managed to ensure "exactly-once" style delivery where possible.

---

## Summary

| Principle | One-liner |
|-----------|-----------|
| SQL Universality | If you know SQL, you can query anything |
| Predicate Pushdown | Optimize at the source, not the client |
| Adapter Pattern | One interface, many implementations |
| Arrow Backbone | Modern columnar format, zero-copy everywhere |
| DuckDB Engine | Specialized tools over reinvention |
| Async-First | Design for scale, support convenience |
| Incremental Releases | Ship stable, ship documented |
| Graceful Degradation | Fail with actionable messages |
| Cache First-Class | Repeated queries should be instant |
| Pydantic Validation | Type-safe, IDE-friendly contracts |
| Streaming for Scale | Process millions of rows with MBs of RAM |
| Cloud-Native | Direct access to S3/GCS/Delta/Iceberg |
| Unified Auth | Seamless credential provider chain |
| CDC Pattern | Real-time updates over full snapshots |

---

## Contributing

When contributing to WaveQL, please keep these principles in mind:

1. **New adapters** should follow the BaseAdapter pattern
2. **New features** should have corresponding tests
3. **Breaking changes** require major version bump justification
4. **Performance** should be measured, not assumed

See [CONTRIBUTING.md](../CONTRIBUTING.md) for detailed guidelines.
