# WaveQL Internals: The Engine Room

This directory contains deep technical specifications for WaveQL contributors and architects. 
It assumes familiarity with Compiler Theory (ASTs), Distributed Systems (CAP theorem, 2PC), and Database Internals (Query Planning).

## 📚 Table of Contents

### 1. [Query Lifecycle](lifecycle.md)
Trace a SQL query from `conn.execute()` to `pyarrow.Table`.
-   **Parsing**: SQL -> SQLGlot Expression
-   **Optimization**: Pushdown rules, Join reordering
-   **Execution**: Federation strategy, AsyncIO loop management

### 2. [Adapter Protocol Specification](adapter_protocol.md)
The strict interface required for new adapters.
-   `get_schema()` caching strategies
-   `fetch()` generator protocols
-   Type marshaling (JSON -> Arrow types)

### 3. [The Optimizer](optimizer.md)
How we rewrite queries for minimal data transfer.
-   Rule-based optimizations
-   Cost-based estimation (Cardinality)
-   Partial vs. Full Pushdown

### 4. [Wire Protocol](wire_protocol.md) (Draft)
Architecture for `pgwire` compatibility to standard BI tools.

---

> **Note**: If you just want to *use* WaveQL, go back to `../index.md`. This is for people who want to *break* it.
