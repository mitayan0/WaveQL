# WaveQL Architecture

This document describes the internal architecture of WaveQL, detailing how it bridges the gap between Relational SQL and RESTful APIs.

## System Overview

WaveQL functions as a **transpiler and execution engine**. It parses SQL input, optimizes the query plan by pushing operations down to the source API, validates schemas, and manages the HTTP transport of data.

```mermaid
graph TD
    User[User / Application] -->|SQL Query| Connection[WaveQL Connection]
    Connection -->|Cursor| Parser[SQL Parser]
    
    subgraph Engine [Execution Engine]
        Parser -->|AST| Optimizer[Query Optimizer]
        Optimizer -->|Plan| Executor[Adapter Executor]
    end
    
    subgraph Adapters [Adapter Layer]
        Executor -->|Native Query (SOQL/JQL)| SN[ServiceNow Adapter]
        Executor -->|Native Query| SF[Salesforce Adapter]
        Executor -->|Native Query| Jira[Jira Adapter]
    end
    
    SN -->|HTTP Request| API1[ServiceNow API]
    SF -->|HTTP Request| API2[Salesforce API]
    Jira -->|HTTP Request| API3[Jira API]
    
    API1 -->|JSON| Transf[Result Transformer]
    API2 -->|JSON| Transf
    API3 -->|JSON| Transf
    
    Transf -->|Arrow Table| User
```

## Core Components

### 1. Connection Manager (`WaveQLConnection`)
The entry point for the library. It creates standard DB-API cursors and manages the lifecycle of the underlying `httpx.Client`. It handles:
*   Connection Pooling (via `httpx` and `anyio`)
*   Authentication State Management
*   Adapter Factory pattern resolution

### 2. The Predicate Pushdown Engine
One of WaveQL's most critical features is its ability to "push down" SQL `WHERE` clauses to the remote API. This prevents fetching excessive data and filtering it locally (slow) vs asking the API for exactly what is needed (fast).

**Process:**
1.  **Parsing**: The SQL `WHERE` clause is parsed into an Abstract Syntax Tree (AST).
2.  **Visitor Traversal**: An adapter-specific "Visitor" traverses the AST.
3.  **Translation**: 
    *   For **Salesforce**, `WHERE status = 'New'` becomes `SELECT ... WHERE Status = 'New'` (SOQL).
    *   For **Jira**, `WHERE project = 'PROJ'` becomes `project = "PROJ"` (JQL).
    *   For **ServiceNow**, `WHERE priority < 3` becomes `priority<3` (sysparm_query).

### 3. Virtual Joins (DuckDB Engine)
APIs do not support SQL `JOIN` operations natively. WaveQL solves this by embedding an in-memory **DuckDB** instance.

When a query involving a `JOIN` is detected:
1.  **Disaggregation**: The query is split into sub-queries for each table source.
2.  **Parallel Fetch**: WaveQL asynchronously fetches the relevant filtered data from each API.
3.  **Arrow Loading**: The results are zero-copy loaded into DuckDB as Arrow tables.
4.  **Local Execution**: The final `JOIN` and aggregation logic is executed locally within DuckDB.

### 4. Async Concurrency Model
WaveQL is optimized for high-throughput environments. 
*   **I/O Bound**: Since API calls are I/O bound, we use standard `asyncio` / `anyio`.
*   **Pagination**: Pagination pages are fetched proactively (prefetching) in background tasks to ensure the cursor always has data ready for the consumer.

## Data Model

WaveQL normalizes all API responses into **Apache Arrow** tables.
*   **Why Arrow?** It provides a columnar memory format that is standard across modern data tools (Pandas, Polars, Spark).
*   **Type Consistency**: Adapters are responsible for mapping API-specific types (e.g., ServiceNow `GlideDateTime`) to standard Arrow timestamps.
