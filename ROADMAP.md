# WaveQL Project Roadmap

> **Last Updated:** 2026-01-04  
> **Current Version:** 0.1.1

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

## 🚧 In Progress (v0.2.0)

### Query Optimizer Enhancements
- [ ] Complex predicate extraction (nested `OR` support)
- [ ] Subquery pushdown for single-adapter sources

### Integration & Ecosystem
- [ ] SQLAlchemy/Pandas integration guide (`pd.read_sql`)
- [ ] BI tool integration (Superset, Metabase)

---

## 📋 Planned Features

### v0.3.0 - Caching & Durability
- [ ] **Materialized Views**: Local Parquet snapshots of remote tables
- [ ] **Incremental Sync Utility**: Managed syncing based on timestamps (e.g., `sys_updated_on`)

### v0.4.0 - Streaming & Scalability
- [ ] Generator-based streaming for large result sets (RecordBatch yielding)
- [ ] Memory-efficient fetching for million-row exports

### v0.5.0 - More Adapters
- [ ] **Cloud Storage** (S3, GCS, Azure Blob via DuckDB)
- [ ] **Google Sheets Adapter**
- [ ] **SaaS Expansion**: HubSpot, Shopify, Zendesk, Stripe

### Future
- [ ] **Integration Tests**: Live testing against real sandbox environments
- [ ] GraphQL adapter support
- [ ] Custom adapter SDK / plugin system

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
