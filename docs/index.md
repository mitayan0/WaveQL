# WaveQL Documentation

Welcome to the official documentation for **WaveQL**, the universal SQL connector for modern APIs.

WaveQL allows data engineers, developers, and data scientists to query SaaS platforms (ServiceNow, Salesforce, Jira) and generic REST APIs using standard ANSI SQL. By abstracting the complexities of API pagination, authentication, and filter syntax, WaveQL provides a unified data access layer for your organization.

## Content Overview

### 📘 [Architecture & Design](architecture.md)
Understand how WaveQL translates SQL into API calls, handles connection pooling, and leverages Apache Arrow for high-performance data transport.
*   [Query Lifecycle](architecture.md#query-lifecycle)
*   [Predicate Pushdown Engine](architecture.md#predicate-pushdown-engine)
*   [Virtual Joins (DuckDB)](architecture.md#virtual-joins)

### 🔌 [Adapter Guide](adapters.md)
Detailed documentation on built-in adapters and instructions for building custom connectors.
*   [ServiceNow](adapters.md#servicenow)
*   [Salesforce](adapters.md#salesforce)
*   [Jira](adapters.md#jira)
*   [Implementing Custom Adapters](adapters.md#custom-adapters)

### 🔐 [Authentication](auth.md)
Securely managing credentials and authentication flows.
*   [AuthManager](auth.md#authmanager)
*   [OAuth2 Flows](auth.md#oauth2)
*   [API Key & Basic Auth](auth.md#basic-auth)

### ⚡ [Performance Tuning](performance.md)
Best practices for optimizing query performance and minimizing API usage.
*   [Pagination Strategies](performance.md#pagination)
*   [Memory Management](performance.md#memory)
*   [Async Concurrency](performance.md#concurrency)

### 📚 [API Reference](api.md)
Comprehensive class and function reference for the WaveQL SDK.

---

## Technical Philosophy

WaveQL follows the **Python DB-API 2.0 (PEP 249)** standard, ensuring compatibility with the broader Python data ecosystem (SQLAlchemy, Pandas, Superset).

Key design principles include:
1.  **Pushdown First**: Whenever possible, compute should happen at the source. `WHERE` clauses are translated to native API filters (e.g., JQL, SOQL).
2.  **Zero-Copy Transport**: We utilize PyArrow to minimize serialization overhead when moving data from network responses to analytics frames.
3.  **Universal Interface**: The consumer of the data should not need to know the underlying implementation details of the API.
