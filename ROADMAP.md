# WaveQL Project Roadmap

> **Last Updated:** 2026-01-07  
> **Current Version:** 0.1.6

---

## ✅ Completed Features

### Core Architecture
- [x] DB-API 2.0 Compliance (Connection, Cursor)
- [x] DuckDB Integration
- [x] SQL Query Planner (Predicate Pushdown)
- [x] Caching System (SQLite-based)

### Authentication
- [x] Universal AuthManager
- [x] OAuth2 Support (Client Credentials, Password, Refresh Token)
- [x] Auto-refresh & Thread safety

### Adapters
- [x] **ServiceNow Adapter**
  - [x] REST Table API, CRUD, Auto-schema
  - [x] Display Values (Readable labels via `sysparm_display_value`)
  - [x] Attachment API (Virtual table `sys_attachment_content`)
- [x] **Salesforce Adapter**
  - [x] Simple Object Query Language (SOQL) support
  - [x] CRUD Operations
  - [x] OAuth2 User-Agent/Web Server flow support
  - [x] Bulk API support for large datasets (Ingest)
- [x] **Jira Adapter**
  - [x] JQL predicate pushdown
  - [x] Issues, Projects, Users tables
  - [x] Full CRUD operations
  - [x] Async support
- [x] **SQL Pass-through** (MySQL, PostgreSQL, SQL Server)
- [x] Generic REST Adapter
- [x] File Adapter (CSV, Parquet)

### Performance & Infrastructure
- [x] Rate Limiter Integration (Exponential Backoff)
- [x] Parallel Fetching Utility
- [x] **Connection Pooling**
  - [x] Thread-safe sync connection pool (`requests.Session` reuse)
  - [x] Async connection pool (`httpx.AsyncClient` with HTTP/2)
  - [x] Per-host connection limits and automatic recycling
  - [x] Configurable pool settings (`PoolConfig`)

### Advanced Features
- [x] Virtual Joins (Cross-adapter joins via DuckDB)
- [x] Schema-qualified table support (e.g., `sales.Account`)
- [x] **Aggregation Pushdown**
  - [x] Support for `count`, `sum`, `min`, `max`, `avg` pushed to source APIs
  - [x] `GROUP BY` pushdown
- [x] SQLAlchemy Dialect
- [x] Async Support (`connect_async`)

### Observability
- [x] `EXPLAIN` support for execution plans
- [x] Logging of actual API queries sent to sources
- [x] Performance timing (API Latency vs local processing)

### Query Optimizer
- [x] **Semi-Join Pushdown**: Push `JOIN` predicates to remote adapters using `IN` filters

---

## ✅ Completed in v0.1.5

### Materialized Views ✅ NEW
- [x] **Materialized Views**: Local Parquet snapshots of remote tables
  - [x] `CREATE MATERIALIZED VIEW` support
  - [x] Full and incremental refresh strategies
  - [x] SQLite-based view registry
  - [x] Automatic DuckDB integration
- [x] **Incremental Sync**: Managed syncing based on timestamps (e.g., `sys_updated_on`)

### Change Data Capture (CDC) ✅ NEW
- [x] **Real-Time CDC**: Stream changes from APIs
  - [x] `stream_changes()` async iterator
  - [x] `get_changes()` one-shot fetching
  - [x] Provider support: ServiceNow, Salesforce, Jira
  - [x] Configurable polling intervals
  - [x] Change type detection (insert/update/delete)

### Query Optimizer Enhancements ✅ NEW
- [x] **Complex predicate extraction** (nested `OR` support)
  - [x] OR-to-IN conversion for same-column equality conditions
  - [x] Compound predicate representation for multi-column OR
  - [x] API-specific filter generation (ServiceNow, Salesforce, Jira)
- [x] **Subquery pushdown** for single-adapter sources
  - [x] Subquery detection and analysis
  - [x] Same-adapter optimization (push entire query)
  - [x] Cross-adapter materialization strategy

### Integration & Ecosystem ✅ NEW
- [x] **SQLAlchemy/Pandas integration guide** (`pd.read_sql`)
  - [x] Complete Pandas integration examples
  - [x] SQLAlchemy ORM usage patterns
  - [x] BI tool integration (Superset, Metabase, Jupyter)
  - [x] Performance optimization guide
  - [x] ETL pipeline examples

---

## ✅ Completed in v0.1.6

### Data Contracts & Validation ✅
- [x] **Data Contracts**: Pydantic-based schema validation
  - [x] `DataContract` model for table schemas
  - [x] `ColumnContract` model for column definitions
  - [x] Runtime validation of API responses
  - [x] JSON Schema export for documentation
  - [x] YAML/JSON file-based contract loading
- [x] **Schema Change Detection**: Alert on API schema changes
  - [x] Compare contract vs live schema
  - [x] Emit warnings for breaking changes
  - [x] Configurable strict modes (columns and types)
- [x] **Contract Registry**: Centralized contract management
  - [x] File-based loading from directories
  - [x] Schema drift detection
  - [x] Contract generation from Arrow schemas

### Streaming & Scalability ✅
- [x] **Generator-based streaming**: RecordBatch yielding for large result sets
  - [x] `cursor.stream_batches()` - sync iterator for batched processing
  - [x] `cursor.stream_batches_async()` - async iterator with backpressure
  - [x] `BufferedAsyncStream` - prefetching for maximum throughput
- [x] **Memory-efficient fetching**: Process million-row exports without loading into memory
  - [x] `cursor.stream_to_file()` - direct-to-Parquet export
  - [x] `cursor.stream_to_file_async()` - async version
  - [x] Configurable batch sizes and compression
- [x] **Progress tracking**: Callbacks for long-running operations
  - [x] `StreamConfig` with progress_callback support
  - [x] `StreamStats` for operation statistics

### Salesforce Async CRUD ✅
- [x] Full async support for Salesforce adapter
  - [x] `fetch_async`, `insert_async`, `update_async`, `delete_async`
  - [x] `get_schema_async` for async schema discovery

### Cloud Storage & Data Lakes ✅ NEW
- [x] **Cloud Storage Adapter** (S3, GCS, Azure Blob via DuckDB httpfs)
  - [x] `CloudStorageAdapter` - unified adapter for all cloud providers
  - [x] Automatic provider detection from URI
  - [x] Factory functions: `s3_adapter()`, `gcs_adapter()`, `azure_adapter()`
- [x] **Delta Lake Support**
  - [x] `delta_table()` factory for Delta Lake tables
  - [x] Version-aware queries via DuckDB delta extension
- [x] **Apache Iceberg Support**
  - [x] `iceberg_table()` factory for Iceberg tables
  - [x] Catalog integration (Glue, Hive, REST)
- [x] **Google Sheets Adapter**
  - [x] Query spreadsheets with SQL
  - [x] Sheet tabs as tables
  - [x] Automatic type inference
  - [x] OAuth2 and Service Account authentication
- [x] **Credential Provider Chain**
  - [x] Explicit parameters (highest priority)
  - [x] Environment variables
  - [x] Config file (~/.waveql/credentials.yaml)
  - [x] `CloudCredentials` dataclass for unified credential management

### SaaS Expansion ✅ NEW
- [x] **HubSpot Adapter** (CRM, Contacts, Deals, Tickets)
  - [x] Search API pushdown
  - [x] Full CRUD support (Create, Read, Update, Delete)
  - [x] Auto-pagination
- [x] **Shopify Adapter** (Orders, Products, Customers)
  - [x] Link header pagination
  - [x] CRUD support
- [x] **Zendesk Adapter** (Tickets, Users, Organizations)
  - [x] Search API integration
  - [x] CRUD support
- [x] **Stripe Adapter** (Payments, Subscriptions, Customers)
  - [x] Smart API switching (Search vs List)
  - [x] CRUD support



---


---

## Development Roadmap (Next Steps)

### v0.1.7 - The "Intelligence Layer" (Current)
Building the bridge between raw APIs and AI agents.

**AI Readiness (The Semantic Layer)** ✅
- [x] **LLM Context Generation**: `.to_llm_context()` for prompt-ready schema descriptions.
- [x] **Semantic Metadata**: Description fields in `DataContract` and `ColumnContract`.
- [x] **Relationship Discovery**:
    - [x] `RelationshipContract` for cross-adapter join hints.
    - [x] `discover_relationships()` for automated cross-adapter link discovery.

**Advanced Query Optimization (CBO)** ⚡
- [x] **Latency Tracking**: Record `avg_latency_per_row` for all adapters.
- [x] **Parallel Scan Foundations**: `get_parallel_plan()` primitive in `BaseAdapter`.
- [x] **Cost-Based Planner**: Engine logic to re-order joins based on adapter latency.

**Semantic Features** ✅
- [x] **Virtual Views**: Defined in `WaveQLConnection`, expanded by `QueryPlanner`.
- [x] **dbt Integration**: Expose models as tables (Basic support).
- [x] **Vector Search (VSS)**:
    - Wrapper for DuckDB's `array_distance` and similarity functions.
    - `VectorSearchManager` with HNSW index support.
- [x] **AI Functions**:
    - `register_ai_functions()`: Register embedding providers (OpenAI, Ollama, Mock).
    - `ai.embed(text)`: Generate embeddings inline.
    - `ai.vector_search()`: Similarity search on any table.

**Hybrid Querying (Cache + Live)** ✅
- [x] `/*+ HYBRID */` hint detection in `QueryPlanner`.
- [x] Engine to merge Materialized Views (Historical) with Live API records.
- [x] PK-based deduplication during merge logic.

**Observability & DX** ✅
- [x] **WaveQL CLI**: Interactive SQL shell with auto-completion.
- [x] **REPL Diagnostics**: `.stats` command for real-time cache and CBO latency monitoring.
- [x] **SQL Query Logging**: Log exact native API calls (SOQL, JQL) generated by the engine.

**Reliability Improvements** 🛠️
- [x] **Connection Health Checks**: Active validation of pooled connections before reuse.
- [x] **Async Fallback Pattern**: Thread-based async wrappers for sync-only adapters (REST).
- [x] **Enhanced Retry Logic**: Automatic 429 backoff for REST adapter.

**Big Data & Ecosystem** 🚀
- [ ] **PySpark Integration**: Create `WaveQLDataSource` for distributed Spark pipelines.
- [ ] **Ray Integration**: Support for partitioned parallel fetching into Ray clusters.

**Reliability & Future Research** 🛠️
- [x] **Stateful CDC Backend**:
    - `StateBackend` abstract class with SQLite, Redis, and Memory implementations.
    - `StreamPosition` for tracking LSN/offset/last_key across restarts.
    - `create_state_backend()` factory function.
- [ ] **Atomic Writes / 2PC (Research)**: Investigating atomic cross-adapter transactions.

- [x] **Webhook Listener**:
    - New `WaveQLWebhookServer` to ingest real-time pushes (Shopify/Stripe).
    - Invalidate/Upsert local cache immediately upon receipt.
- [x] **Singer Protocol Bridge**:
    - `SingerAdapter` class to wrap existing Tap executables.
- [x] **Wasm Compilation**:
    - [x] Port core logic (Planner/Adapters) to run inside Pyodide.

---

## 🏗️ Architectural Pillars (2026)

1.  **Intelligence Layer**: We are not just a connector; we are the *semantic* layer that makes APIs understandable to LLMs.
2.  **State Persistence**: Moving from "script" to "service" requires durability (CDC checkpoints).
3.  **Adaptive Schema**: Validating data (Contracts) is good; *evolving* with it (Adaptive Contracts) is better.
4.  **Cost-Based Optimization**: Track adapter latency and throughput to optimize join ordering.

## 🔍 Critical Implementation Tasks

- [x] **State Persistence**:
  - [x] *Goal*: `stream_changes()` must survive a crash.
  - [x] *Tech*: Add `.waveql_state` sqlite DB to track source offsets.

- [x] **Schema Drift Handling**:
  - [x] *Goal*: Don't crash when HubSpot adds a field.
  - [x] *Tech*: `AdaptiveModel` base class for Pydantic that implements `model_config['extra'] = 'allow'`.

- [x] **Semantic Metadata Export**:
  - [x] *Goal*: Let Metabase/Looker understand our table relationships.
  - [x] *Tech*: Export `DataContract` registry to dbt-compatible `.yaml`.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute to WaveQL development.
