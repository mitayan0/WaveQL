# WaveQL Project Roadmap

## Active Development
- [x] **Salesforce Adapter** (Priority: High)
  - [x] Simple Object Query Language (SOQL) support
  - [x] CRUD Operations
  - [x] OAuth2 User-Agent/Web Server flow support (Provided by AuthManager)
  - [x] Bulk API support for large datasets (Ingest)

## Completed Features
- [x] **Core Architecture**
  - [x] DB-API 2.0 Compliance (Connection, Cursor)
  - [x] DuckDB Integration
  - [x] SQL Query Planner (Predicate Pushdown)
  - [x] Caching System (SQLite-based)

- [x] **Authentication**
  - [x] Universal AuthManager
  - [x] OAuth2 Support (Client Credentials, Password, Refresh Token)
  - [x] Auto-refresh & Thread safety

- [x] **Adapters**
  - [x] ServiceNow Adapter
    - [x] REST Table API, CRUD, Auto-schema
    - [x] **Display Values** (Readable labels via `sysparm_display_value`)
    - [x] **Attachment API** (Virtual table `sys_attachment_content`)
  - [x] Generic REST Adapter
  - [x] File Adapter (CSV, Parquet)
  - [x] Rate Limiter Integration (Exponential Backoff)
  - [x] Parallel Fetching Utility

- [x] **Features**
  - [x] Virtual Joins (Cross-adapter joins via DuckDB)
  - [x] Schema-qualified table support (e.g., `sales.Account`)

## Planned Features

### Phase 2: Advanced Capabilities
- [x] **Aggregation Pushdown**
  - [x] Support for `count`, `sum`, `min`, `max`, `avg` pushed to source APIs
  - [x] `GROUP BY` pushdown
- [x] SQLAlchemy Dialect
- [x] Async Support (`connect_async`)
- [x] **Connection Pooling**
  - [x] Thread-safe sync connection pool (`requests.Session` reuse)
  - [x] Async connection pool (`httpx.AsyncClient` with HTTP/2)
  - [x] Per-host connection limits and automatic recycling
  - [x] Configurable pool settings (`PoolConfig`)
  - Allow WaveQL to be used as an engine in SQLAlchemy/Pandas (`pd.read_sql`)
  - Integration with BI tools (Superset, Metabase)

### Phase 3: Enterprise & Performance
- [x] **Query Metrics & Observability**
  - [x] `EXPLAIN` support for execution plans
  - [x] Logging of actual API queries sent to sources
  - [x] Performance timing (API Latency vs local processing)
- [ ] **Query Optimizer**
  - [ ] **Semi-Join Pushdown**: Push `JOIN` predicates to remote adapters using `IN` filters
  - [ ] Complex predicate extraction (nested `OR` support)
  - [ ] Subquery pushdown for single-adapter sources
- [ ] **Caching & Durability**
  - [ ] **Materialized Views**: Local Parquet snapshots of remote tables
  - [ ] **Incremental Sync Utility**: Managed syncing based on timestamps (e.g., `sys_updated_on`)
- [ ] **Streaming & Scalability**
  - [ ] Generator-based streaming for large result sets (RecordBatch yielding)
  - [ ] Memory-efficient fetching for million-row exports
- [ ] **Integration Tests**
  - Live testing against real sandbox environments

### Phase 4: More Adapters
- [x] **Jira Adapter**
  - [x] JQL predicate pushdown
  - [x] Issues, Projects, Users tables
  - [x] Full CRUD operations
  - [x] Async support
- [ ] **SQL Pass-through** (MySQL, PostgreSQL, SQL Server)
- [ ] **Cloud Storage** (S3, GCS, Azure Blob via DuckDB)
- [ ] **SaaS Expansion**: HubSpot, Shopify, Zendesk, Stripe
- [ ] **Google Sheets Adapter**

