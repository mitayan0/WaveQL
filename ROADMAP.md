# WaveQL Project Roadmap

> **Current Version:** 0.1.8
> **Focus:** The "Intelligence Layer" & Universal Access

This document tracks the active development goals and future research directions of WaveQL.
For a history of completed features, see [CHANGELOG.md](CHANGELOG.md).

---

## 🚧 Active Development (v0.1.9)

We are currently building the bridge between raw APIs and AI agents.

### 1. Query Provenance for API Federation (RESEARCH)
**Goal**: Track data lineage across heterogeneous API backends - **novel research area**.
- [x] **Core Provenance Engine**: Tracker, Where/Why/How provenance capture (Implemented in v0.1.7).
- [ ] **SQL Extension**: `SELECT *, PROVENANCE() FROM table`.
- [ ] **Lineage Visualization**: D3.js interactive graph export.

*See [docs/research/query_provenance.md](docs/research/query_provenance.md) for full research plan.*

### 2. Enterprise Features
- [ ] **Time Travel**: `SELECT * FROM table FOR SYSTEM_TIME AS OF '2023-01-01'`.
- [ ] **GraphQL Adapter**: SQL-to-GraphQL AST transpiler.

---

## 📦 Optional Modules
- **`waveql[ai]`**: Vector Search (HNSW), Embeddings generation.
- **`waveql[spark]`**: Distributed pipeline integration.
- **`waveql[observability]`**: OpenTelemetry tracing.

---

## ✅ Implemented Capabilities (Summary)
*For full details, refer to the documentation.*

| Capabilities | Status | Details |
| :--- | :--- | :--- |
| **Core Adapters** | 🟢 Ready | ServiceNow, Salesforce, Jira, HubSpot, Zendesk, Shopify, Stripe, Google Sheets |
| **Connectors** | 🟢 Ready | Postgres, MySQL, SQL Server, Excel, CSV, Parquet, JSON |
| **Cloud Storage** | 🟢 Ready | S3, GCS, Azure Blob, Delta Lake, Iceberg |
| **Engine** | 🟢 Ready | Predicate Pushdown, Aggregation Pushdown, Virtual Joins, Async I/O |
| **CDC** | 🟢 Ready | Real-time streaming & Polling |
| **Validation** | 🟢 Ready | Data Contracts (Pydantic), Schema Drift Detection |
| **Query Optimizer** | 🟢 Ready | CBO, Join Re-ordering, Subquery Pushdown |
| **Systems** | 🟢 Ready | PG Wire Protocol, Adaptive Pagination, Resource Budgeting |
