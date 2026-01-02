# Changelog

All notable changes to WaveQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-03

### Added

- **Core Architecture**
  - DB-API 2.0 compliant Connection and Cursor classes
  - DuckDB integration for cross-source JOINs and analytics
  - SQL Query Planner with predicate extraction
  - SQLite-based schema caching

- **Authentication**
  - Universal AuthManager supporting multiple auth methods
  - OAuth2 support (Client Credentials, Password, Refresh Token)
  - Auto-refresh and thread safety

- **Adapters**
  - **ServiceNow Adapter**
    - REST Table API with full CRUD
    - ServiceNow Query predicate pushdown
    - Stats API aggregation pushdown
    - Display values support
    - Attachment API support
  - **Salesforce Adapter**
    - SOQL predicate pushdown
    - Bulk API for large datasets
    - Full CRUD operations
  - **Jira Adapter**
    - JQL predicate pushdown
    - Issues, Projects, Users tables
    - Full CRUD for issues
  - **REST Adapter**
    - Generic REST API support
    - Configurable endpoints
  - **File Adapter**
    - CSV and Parquet file support
    - DuckDB-powered queries

- **Features**
  - Virtual JOINs across different adapters
  - Schema-qualified table support (e.g., `sales.Account`)
  - Aggregation pushdown (COUNT, SUM, AVG, MIN, MAX)
  - GROUP BY pushdown

- **Async Support**
  - Full async/await support for all adapters
  - AsyncCursor for non-blocking queries

- **Connection Pooling**
  - Thread-safe sync connection pool
  - Async connection pool with HTTP/2 support
  - Per-host connection limits
  - Automatic connection recycling

- **SQLAlchemy Integration**
  - Custom WaveQL dialect
  - Works with Pandas `read_sql()`
  - Superset compatible

- **Utilities**
  - Rate limiter with exponential backoff
  - Parallel fetching for large datasets
  - Configurable pool settings

### Security

- Credentials never logged
- SSL verification enabled by default
- API tokens supported for all adapters

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.1.0 | 2026-01-03 | Initial release |

[Unreleased]: https://github.com/yourusername/waveql/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yourusername/waveql/releases/tag/v0.1.0
