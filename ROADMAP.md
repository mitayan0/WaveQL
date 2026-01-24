# WaveQL Project Roadmap

> **Current Version:** 0.1.7-dev
> **Focus:** The "Intelligence Layer" & Universal Access

This document tracks the active development goals and future research directions of WaveQL.
For a history of completed features, see [CHANGELOG.md](CHANGELOG.md).

---

## 🚧 Active Development (v0.1.7)

### 🚨 Critical Remediation (Priority High) ✅
**Goal**: Address correctness gaps in Predicate Pushdown and unify the Optimizer.
- [x] **Optimizer Integration**: Wire up the unused `QueryOptimizer` to `WaveQLCursor`.
    - Replaced ad-hoc predicate parsing in `QueryPlanner` with `QueryOptimizer` logic.
    - Properly distinguish between "Pushable" and "Residual" predicates via `_classify_predicates()`.
- [x] **Safety Net Implementation**:
    - Implemented Client-Side Filtering Fallback via `_apply_residual_filter()` for predicates that cannot be pushed down.
    - Prevent silent dropping of complex logic (e.g., `OR` conditions).
- [x] **Test Suite Reality Check**:
    - Created `test_optimizer_integration.py` to test actual execution paths.
    - Added regression tests for Complex Boolean Logic (DeMorgan's laws, mixed AND/OR).

We are currently building the bridge between raw APIs and AI agents.

### 1. The Intelligence Layer
- [x] **LLM Context Generation**: `.to_llm_context()` for prompt-ready schema.
- [x] **Relationship Discovery**: Automated cross-adapter link discovery (`RelationshipContract`).
- [x] **Cost-Based Optimization (CBO)**:
    - [x] Latency tracking (`avg_latency_per_row`).
    - [x] Parallel scan primitives.
    - [x] **Join Re-ordering**: Based on real-time latency stats and cardinality.

### 2. High-Performance Hybrid Querying
- [x] **Hybrid Hints**: `/*+ HYBRID */` detection to merge live API data with local cache.
- [x] **Bind Joins (Smart Chunking)**:
    - [x] Auto-split large `IN (...)` predicates into micro-batches to avoid `414 URI Too Long`.
    - [x] Parallel execution of chunks.

### 3. Reliability & Systems
- [x] **Saga Pattern**: Distributed transaction support with compensating transactions.
- [x] **Webhook Listener**: Real-time cache invalidation via `WaveQLWebhookServer`.
- [x] **Wasm Support**: Core engine ported to run inside Pyodide (Browser).



---

## 🔮 Future Horizons

### Phase 1: Universal Access (Postgres Wire Protocol)
**Goal**: Trick Tableau, PowerBI, and DBeaver into thinking WaveQL is a standard Postgres DB.
- [x] **Postgres Server Emulation**: Implement `pg_wire` (TCP 5432).
- [x] **Catalog Emulation**: Mock `pg_catalog` tables so BI tools can inspect schemas.
- [x] **Binary Tiling**: Map API JSON types to Postgres Binary Tuples.

### Phase 2: Low-Resource Systems Engineering
**Goal**: Run efficiently on constrained environments (Serverless / Edge).
- [x] **Statistical Cardinality Estimator**: Predict result sizes without counting rows (using history).
- [x] **Adaptive Pagination**: Dynamic page sizing (AIMD algorithm) based on network throughput.
- [x] **Budget-Constrained Planning**: `SELECT ... WITH BUDGET 500ms`.
- [x] **Resource Aware Execution**: Unified resource tracking and optimization loop.

### Phase 3: Query Provenance for API Federation (RESEARCH)
**Goal**: Track data lineage across heterogeneous API backends - **novel research area**.
- [x] **Provenance Tracker**: Thread-safe capture of API calls during query execution.
- [x] **Where-Provenance**: Track which API/table each row originated from.
- [x] **Why-Provenance**: Record predicates that caused row inclusion.
- [x] **How-Provenance**: Document join paths and transformations.
- [ ] **SQL Extension**: `SELECT *, PROVENANCE() FROM table`.
- [ ] **Lineage Visualization**: D3.js interactive graph export.

*See [docs/research/query_provenance.md](docs/research/query_provenance.md) for full research plan.*

### Phase 4: Enterprise Features
- [x] **Row-Level Security**: Policies like `conn.add_policy(table, "department = 'sales'")`.
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
