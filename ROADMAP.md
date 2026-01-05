# WaveQL Project Roadmap

> **Last Updated:** 2026-01-05  
> **Current Version:** 0.1.5

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

## 📋 Planned Features

### v0.1.6 - Data Contracts & Validation
- [ ] **Data Contracts**: Pydantic-based schema validation
  - [ ] `DataContract` model for table schemas
  - [ ] Runtime validation of API responses
  - [ ] JSON Schema export for documentation
- [ ] **Schema Change Detection**: Alert on API schema changes
  - [ ] Compare cached vs live schema
  - [ ] Emit warnings for breaking changes
  - [ ] Optional strict mode (fail on schema mismatch)

### v0.1.7 - Streaming & Scalability
- [ ] Generator-based streaming for large result sets (RecordBatch yielding)
- [ ] Memory-efficient fetching for million-row exports
- [ ] Backpressure support for async streams

### v0.1.8 - Cloud Storage Adapters
- [ ] **Cloud Storage** (S3, GCS, Azure Blob via DuckDB)
- [ ] **Google Sheets Adapter**
- [ ] Credential provider chain (env vars, config files, IAM roles)

### v0.1.9 - SaaS Expansion
- [ ] **HubSpot Adapter** (CRM, Contacts, Deals)
- [ ] **Shopify Adapter** (Orders, Products, Customers)
- [ ] **Zendesk Adapter** (Tickets, Users, Organizations)
- [ ] **Stripe Adapter** (Payments, Subscriptions, Customers)

### v0.2.0 - Production Ready
- [ ] **Integration Tests**: Live testing against real sandbox environments
- [ ] **GraphQL Adapter**: Generic GraphQL source support
- [ ] **Plugin System**: Custom adapter SDK for third-party extensions
- [ ] Stable API guarantee (no breaking changes in 0.2.x)

---

## 📊 Feature Matrix

| Adapter | Predicate Pushdown | Aggregation | CRUD | Async |
|---------|-------------------|-------------|------|-------|
| ServiceNow | ✅ | ✅ | ✅ | ✅ |
| Salesforce | ✅ | ✅ | ✅ | ✅ |
| Jira | ✅ | ❌ | ✅ | ✅ |
| SQL (MySQL/PostgreSQL/MSSQL) | ✅ | ✅ | ✅ | ❌ |
| REST | ✅ | ❌ | ✅ | ✅ |
| File (CSV/Parquet) | ✅ | ✅ | ❌ | ❌ |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute to WaveQL development.
