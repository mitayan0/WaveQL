# Changelog

All notable changes to WaveQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.5] - 2026-01-05

### Added

- **Complex Predicate Extraction** (Query Optimizer Enhancement)
  - Support for nested OR conditions with intelligent optimization
  - Automatic conversion of OR groups to IN predicates when all conditions target the same column
    - Example: `status = 'open' OR status = 'closed'` → `status IN ('open', 'closed')`
  - New `CompoundPredicate` class for representing complex predicate trees
  - API-specific filter generation for ServiceNow (^OR), Salesforce (OR), and Jira
  - Adapter capability detection for optimal pushdown strategy

- **Subquery Pushdown Optimization**
  - Detection and analysis of subqueries in WHERE clauses
  - Same-adapter optimization: pushes entire subquery when inner and outer tables are on same adapter
  - Cross-adapter strategy: materializes inner query results for outer query IN predicates
  - New `SubqueryInfo` and `SubqueryPushdownOptimizer` classes

- **SQLAlchemy & Pandas Integration Guide** (`docs/pandas-sqlalchemy-guide.md`)
  - Complete Pandas integration examples with `pd.read_sql()`
  - SQLAlchemy ORM usage patterns and connection string formats
  - BI tool integration guides (Superset, Metabase, Jupyter, Streamlit)
  - Performance optimization best practices
  - ETL pipeline examples with real-world patterns
  - Async support with Pandas
  - Chunked reading for large datasets

### Changed

- **QueryPlanner**: Enhanced `_parse_condition()` to handle OR groups and parenthesized expressions
- **QueryInfo**: Added `compound_predicates`, `subqueries`, and `has_complex_or` fields

### Tests

- Added comprehensive test suite for complex predicates (`test_complex_predicates.py`)
  - 24 new tests covering OR extraction, IN conversion, and subquery analysis


## [0.1.1] - 2026-01-03

### Added

- **Quoted Identifier Support**
  - SQL parser now supports quoted table identifiers (e.g., `"schema"."table"`)
  - Support for mixed quoted/unquoted identifiers
  - All SQL operations (SELECT, INSERT, UPDATE, DELETE, JOIN) support quoted identifiers

### Fixed

- **Query Planner**: Updated regex patterns to correctly parse schema-qualified table names with quotes
- **Cursor**: Added `_clean_table_name()` helper to strip quotes and extract table names for adapters
- **Adapter Resolution**: Schema lookup now correctly strips quotes before resolving adapters

### Tests

- Added comprehensive test suite for quoted identifiers (`test_quoted_identifiers.py`)
  - 21 new unit tests covering parsing, cleaning, and integration scenarios

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
| 0.1.1 | 2026-01-03 | Quoted identifier support, bug fixes |
| 0.1.0 | 2026-01-03 | Initial release |

[Unreleased]: https://github.com/mitayan0/WaveQL/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/mitayan0/WaveQL/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mitayan0/WaveQL/releases/tag/v0.1.0
