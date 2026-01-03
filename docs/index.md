# WaveQL Documentation

Welcome to the official documentation for **WaveQL**, the universal SQL connector for modern APIs.

WaveQL allows data engineers, developers, and data scientists to query SaaS platforms (ServiceNow, Salesforce, Jira) and generic REST APIs using standard ANSI SQL. By abstracting the complexities of API pagination, authentication, and filter syntax, WaveQL provides a unified data access layer for your organization.

## Content Overview

### [Architecture & Design](architecture.md)
Understand how WaveQL translates SQL into API calls, handles connection pooling, and leverages Apache Arrow for high-performance data transport.
*   [Query Lifecycle](architecture.md#query-lifecycle)
*   [Predicate Pushdown Engine](architecture.md#predicate-pushdown-engine)
*   [Virtual Joins (DuckDB)](architecture.md#virtual-joins)

### [Adapter Guide](adapters.md)
Detailed documentation on built-in adapters and instructions for building custom connectors.
*   [ServiceNow](adapters.md#servicenow)
*   [Salesforce](adapters.md#salesforce)
*   [Jira](adapters.md#jira)
*   [Implementing Custom Adapters](adapters.md#custom-adapters)

### [Authentication](auth.md)
Securely managing credentials and authentication flows.
*   [AuthManager](auth.md#authmanager)
*   [OAuth2 Flows](auth.md#oauth2)
*   [API Key & Basic Auth](auth.md#basic-auth)

### [Performance Tuning](performance.md)
Best practices for optimizing query performance and minimizing API usage.
*   [Pagination Strategies](performance.md#pagination)
*   [Memory Management](performance.md#memory)
*   [Async Concurrency](performance.md#concurrency)

### [Tutorial](tutorial.md)
Step-by-step guide to getting started with WaveQL, handling queries, and using advanced features like CDC.

### [API Reference](api.md)
Comprehensive class and function reference for the WaveQL SDK.

### [Change Data Capture (CDC)](cdc.md)
Real-time streaming of data changes from your sources.
*   [Streaming Concepts](cdc.md#concepts)
*   [Configuration](cdc.md#configuration)
*   [Async Integration](cdc.md#async)

---

## Technical Philosophy

WaveQL is built on the belief that **data location should be transparent to the analyst**. Whether your data lives in a high-performance database, a SaaS API, a local spreadsheet, or a CSV file, you should be able to query and join it using a single, unified SQL interface.

**1. Universal Connectivity**
WaveQL connects to *anything*:
*   **APIs**: ServiceNow, Salesforce, Jira, REST
*   **Databases**: PostgreSQL, MySQL, SQLite (via SQLAlchemy/DuckDB)
*   **Files**: CSV, Parquet, JSON, Excel (XLSX)

**2. The "Join Global" Engine**
WaveQL embeds a powerful in-memory analytical engine (DuckDB) that allows you to perform **federated queries**. You can join a table from ServiceNow with a local Excel file and a PostgreSQL database in a single SQL statement.

**3. Pushdown Optimization**
While we allow you to join anything, we respect the source's capabilities. WaveQL intelligently pushes down filters (`WHERE` clauses) and aggregations to the source system whenever possible to minimize data transfer.

**4. Zero-Copy Transport**
We utilize Apache Arrow to move data efficiently between systems, minimizing serialization overhead and ensuring high performance for data science workflows.
