# Changelog

All notable changes to WaveQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.6] - 2026-01-07

### Added

- **Universal Client-Side Aggregation Support**
  - New `_compute_client_side_aggregates()` helper in `BaseAdapter` for adapters without native aggregation APIs
  - Supports COUNT, SUM, AVG, MIN, MAX with optional GROUP BY
  - All SaaS adapters now support aggregation queries (computed locally after fetching)
  
- **Adapter Feature Parity Enhancements**
  - **HubSpot**: Added `supports_aggregation=True`, `supports_batch=True`, sync CRUD wrappers
  - **Shopify**: Added `supports_aggregation=True`, `supports_batch=True`, sync CRUD wrappers
  - **Zendesk**: Added `supports_aggregation=True`, `supports_batch=True`, sync CRUD wrappers
  - **Stripe**: Added `supports_aggregation=True`, `supports_batch=True`, sync CRUD wrappers
  - **Google Sheets**: Added `supports_aggregation=True`, `supports_batch=True`
  - **Jira**: Changed from `NotImplementedError` to client-side aggregation support

- **BaseAdapter Enhancements**
  - New `supports_aggregation` class attribute for capability detection
  - Aggregation support documented in Feature Matrix (Server vs Client distinctions)

- **Smart COUNT Optimization** ⚡
  - **HubSpot**: Uses `total` field from Search API response for `COUNT(*)` queries
  - **Shopify**: Uses dedicated `/count.json` endpoint for efficient counting
  - **Zendesk**: Uses `count` field from Search API response
  - **Stripe**: Uses `total_count` from Search API for searchable resources
  - Result: `SELECT COUNT(*) FROM hubspot.contacts` now executes in **1 API call** instead of 100+

- **Memory-Efficient Streaming Aggregation** (Inspired by Trino/Presto)
  - Uses PyArrow compute functions directly instead of loading entire table into Pandas
  - Reduced memory footprint for non-GROUP BY aggregations
  - Separates `_streaming_aggregate()` for simple cases from `_aggregate_with_groupby()` for grouped queries

- **Approximate Aggregation Support** (New)
  - New `_compute_approximate_aggregates()` method for very large datasets
  - Uses random sampling with automatic ratio adjustment for COUNT/SUM
  - Configurable sample size via `APPROXIMATE_AGGREGATION_SAMPLE_SIZE` (default: 10,000 rows)

- **PostgreSQL Change Data Capture (CDC)** (New) 🚀
  - Zero-latency WAL-based streaming using Logical Replication
  - Support for `wal2json` and `test_decoding` plugins
  - Fully async implementation (`async for change in provider.stream_changes()`)
  - Integration with WaveQL connection (`conn.stream_changes_wal()`)
  - Capture of INSERT, UPDATE, DELETE events with full before/after data
  - Automated replication slot management (create/drop/monitor)
  - Retry logic with exponential backoff for resilient streaming

- **Performance Warnings**
  - Logs warning when client-side aggregation processes >5,000 rows
  - Helps users identify queries that could benefit from LIMIT or filtering



### Added

- **Streaming & Scalability**
  - New `cursor.stream_batches()` and `cursor.stream_batches_async()` for memory-efficient processing of large result sets
  - New `cursor.stream_to_file()` and `cursor.stream_to_file_async()` for direct-to-Parquet exports
  - New `BufferedAsyncStream` with prefetching and backpressure support for maximum throughput
  - Configurable batch sizes and progress tracking via `StreamConfig` and `StreamStats`

- **Cloud Storage & Data Lakes**
  - New `CloudStorageAdapter` supporting S3, GCS, and Azure Blob storage via DuckDB
  - Native support for **Delta Lake** and **Apache Iceberg** table formats
  - Automatic provider detection from URIs (s3://, gs://, azure://)
  - Factory functions for easy setup: `s3_adapter()`, `gcs_adapter()`, `azure_adapter()`, `delta_table()`, `iceberg_table()`

- **Google Sheets Adapter**
  - New `GoogleSheetsAdapter` for querying spreadsheets using standard SQL
  - Support for multiple sheets (tabs) as tables
  - Automatic type inference from spreadsheet data
  - Support for Service Account and OAuth2 authentication

- **Credential Provider Chain**
  - Unified credential resolution: Explicit parameters → Environment variables → Config file (`~/.waveql/credentials.yaml`) → IAM roles
  - New `CloudCredentials` and `GoogleSheetsCredentials` models

- **Salesforce Async CRUD**
  - Full async support for Salesforce adapter (`fetch_async`, `insert_async`, `update_async`, `delete_async`)
  - Async schema discovery with `get_schema_async`

- **Data Contracts** (Pydantic-based Schema Validation)
  - New `DataContract` model for defining table schemas with type safety
  - New `ColumnContract` model for column-level definitions
  - Support for all common types: string, integer, float, boolean, datetime, timestamp, etc.
  - Nested structure support: `struct` and `list` column types
  - Column aliases for schema evolution handling
  - JSON Schema export for documentation generation
  - YAML/JSON file-based contract definitions

- **Contract Validation**
  - New `ContractValidator` class for runtime validation of Arrow tables
  - Violation types: `MISSING_COLUMN`, `EXTRA_COLUMN`, `TYPE_MISMATCH`, `NULL_VIOLATION`
  - Configurable strict modes for columns and types
  - Detailed violation reporting with `ContractValidationResult`

- **Contract Registry**
  - Centralized contract management with `ContractRegistry`
  - File-based loading from JSON/YAML directories
  - Schema drift detection between contracts and live schemas
  - Contract generation from existing Arrow schemas

- **New Exception**: `ContractViolationError` (E011) for validation failures

### Documentation

- New documentation: `docs/contracts.md` - Complete guide to Data Contracts
- New example: `examples/contracts_demo.py` - Interactive demo of all contract features
- Updated `docs/index.md` with Data Contracts section

### Tests

- Added comprehensive test suite (`test_contracts.py`)
  - 35+ tests covering models, validation, registry, and integration

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
| 0.1.6 | 2026-01-06 | Data Contracts & Validation (Pydantic-based) |
| 0.1.5 | 2026-01-05 | Complex predicates, subquery pushdown, Pandas/SQLAlchemy guide |
| 0.1.1 | 2026-01-03 | Quoted identifier support, bug fixes |
| 0.1.0 | 2026-01-03 | Initial release |

[Unreleased]: https://github.com/mitayan0/WaveQL/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/mitayan0/WaveQL/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/mitayan0/WaveQL/compare/v0.1.1...v0.1.5
[0.1.1]: https://github.com/mitayan0/WaveQL/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mitayan0/WaveQL/releases/tag/v0.1.0
