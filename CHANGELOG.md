# Changelog

## [0.1.7] - 2026-01-11

### Added
- **PostgreSQL Wire Protocol**: `waveql.pg_wire` module enables BI tools (Tableau, PowerBI, DBeaver) to connect via standard PostgreSQL drivers.
  - `PGWireServer`: Async TCP server on port 5432.
  - `PGCatalogEmulator`: Mock `pg_catalog` tables for schema introspection.
  - Type mapping between PyArrow and PostgreSQL OIDs.
  - Simple and Extended Query protocol support.
  - CLI: `waveql-server` command to start the server.
- **Semantic Layer**: `VirtualView` and `SavedQuery` support. dbt project integration (`manifest.json` parsing).
- **Credentials**: `SecretStr` checks to prevent accidental PII logging.
- **Async**: Wrappers for sync REST adapters.
- **Health Checks**: `HEAD` checks in connection pool.
- **Observability**: SQL-to-API logging (JQL/SOQL debugging).
- **Join Optimizer**: Cost-based join re-ordering with real-time latency stats (`JoinOptimizer`). Features per-table latency tracking, selectivity estimation, rate limit awareness, and semi-join pushdown detection.

### Fixed
- Connection pool race condition in `_total_connections`.
- Jira adapter offset warning.
- REST adapter pagination logic.

## [0.1.6] - 2026-01-07

### Added
- **Client-Side Aggregation**: Fallback for APIs without `GROUP BY` (HubSpot, Shopify, Zendesk).
- **Optimization**: "Smart COUNT" uses metadata endpoints (e.g., Salesforce `totalSize`) instead of fetching rows.
- **Streaming**: `cursor.stream_batches()` yields RecordBatches. `stream_to_file()` for Parquet export.
- **CDC**: `wal2json` support for PostgreSQL logical replication.
- **Cloud Adapters**: S3, GCS, Azure Blob support (read via DuckDB).
- **Data Contracts**: Schema validation (`ContractValidator`) and registry.

## [0.1.5] - 2026-01-05

### Added
- **Query Optimizer**: OR-group to IN-list conversion. Subquery pushdown optimization.
- **Docs**: SQLAlchemy & Pandas integration guide.

## [0.1.0] - 2026-01-03

- Initial Release.
- DuckDB-backed SQL engine.
- Adapters: ServiceNow, Salesforce, Jira, Generic REST.
- AsyncIO support (`anyio`).
- OAuth2 auth manager.
