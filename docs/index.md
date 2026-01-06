# WaveQL Documentation

Welcome to the official documentation for **WaveQL**, the universal SQL connector for modern APIs.

WaveQL allows data engineers, developers, and data scientists to query SaaS platforms (ServiceNow, Salesforce, Jira) and generic REST APIs using standard ANSI SQL. By abstracting the complexities of API pagination, authentication, and filter syntax, WaveQL provides a unified data access layer for your organization.

## Getting Started

### [⚡ Quick Start Guide](quickstart.md) ← Start Here!
Complete guide to connecting and querying all supported data sources.
*   [Connection Methods](quickstart.md#connection-methods) - Connection strings & parameters
*   [ServiceNow](quickstart.md#servicenow) - Setup, auth, and query examples
*   [Jira](quickstart.md#jira) - API tokens and JQL translation
*   [Salesforce](quickstart.md#salesforce) - OAuth and SOQL
*   [REST API](quickstart.md#rest-api-generic) - Generic JSON APIs
*   [Files](quickstart.md#files-csv-parquet-excel) - CSV, Parquet, Excel
*   [Cloud Storage & Data Lakes](quickstart.md#cloud-storage--data-lakes) ✨ NEW - S3, GCS, Delta, Iceberg
*   [Google Sheets](quickstart.md#google-sheets) ✨ NEW - Query spreadsheets with SQL
*   [Cross-Source Joins](quickstart.md#cross-source-joins) - Join anything!
*   [Streaming & Scalability](quickstart.md#streaming-large-datasets) ✨ NEW - Processing million-row datasets

## Core Concepts

### [Architecture & Design](architecture.md)
Understand how WaveQL translates SQL into API calls, handles connection pooling, and leverages Apache Arrow for high-performance data transport.
*   [Query Lifecycle](architecture.md#query-lifecycle)
*   [Predicate Pushdown Engine](architecture.md#predicate-pushdown-engine)
*   [Virtual Joins (DuckDB)](architecture.md#virtual-joins)

### [Adapter Reference](adapters.md)
Detailed documentation on built-in adapters and instructions for building custom connectors.
*   [ServiceNow](adapters.md#servicenow)
*   [Salesforce](adapters.md#salesforce)
*   [Jira](adapters.md#jira)
*   [Implementing Custom Adapters](adapters.md#custom-adapters)

### [Salesforce Guide](salesforce.md) ✨ NEW
Complete guide to connecting to Salesforce.
*   [Authentication Methods](salesforce.md#authentication-methods) - Password Flow, Access Token, OAuth
*   [Connected App Setup](salesforce.md#setting-up-a-connected-app) - Step-by-step configuration
*   [CRUD Operations](salesforce.md#crud-operations) - INSERT, UPDATE, DELETE
*   [Bulk API](salesforce.md#bulk-operations) - Batch inserts for large datasets
*   [Troubleshooting](salesforce.md#troubleshooting) - Common errors and solutions

### [Authentication](auth.md)
Securely managing credentials and authentication flows.
*   [AuthManager](auth.md#authmanager)
*   [OAuth2 Flows](auth.md#oauth2)
*   [API Key & Basic Auth](auth.md#basic-auth)

## Advanced Features

### [Schema Inference & Nested JSON](schema-inference.md)
Automatic schema discovery with native support for nested JSON structures.
*   [Multi-Sample Schema Inference](schema-inference.md#multi-sample-schema-inference)
*   [Native Struct Columns](schema-inference.md#how-it-works)
*   [Schema Evolution Detection](schema-inference.md#schema-evolution)
*   [Dot-Notation Queries](schema-inference.md#usage-examples)

### [Change Data Capture (CDC)](cdc.md)
Real-time streaming of data changes from your sources.
*   [Streaming Concepts](cdc.md#concepts)
*   [Configuration](cdc.md#configuration)
*   [Async Integration](cdc.md#async)

### [Performance Tuning](performance.md)
Best practices for optimizing query performance and minimizing API usage.
*   [Predicate Pushdown](performance.md#1-predicate-pushdown-the-1-rule)
*   [Pagination Strategies](performance.md#3-pagination--batch-sizes)
*   [Parallel Fetching](performance.md#4-parallel-fetching-servicenow)
*   [Query Caching](performance.md#7-query-result-caching)
*   [Async Concurrency](performance.md#6-async-for-concurrency)

### [Query Result Caching](caching.md)
Built-in LRU cache with TTL support for reducing API calls and improving response times.
*   [Configuration](caching.md#configuration-options) - TTL, memory limits, per-adapter settings
*   [Statistics](caching.md#cache-statistics) - Monitor hit rate and memory usage
*   [Invalidation](caching.md#cache-invalidation) - Manual and automatic cache clearing
*   [Best Practices](caching.md#best-practices) - Matching TTL to data volatility

### [Error Handling](error-handling.md)
Rich, developer-friendly error messages with actionable suggestions.
*   [Error Codes Reference](error-handling.md#error-codes-reference)
*   [Rate Limit Handling](error-handling.md#rate-limit-handling)
*   [Schema Evolution Errors](error-handling.md#schema-evolution-errors)

### [Data Contracts](contracts.md) ✨ NEW
Pydantic-based schema validation for type-safe data pipelines.
*   [Defining Contracts](contracts.md#quick-start) - Type-safe column definitions
*   [Runtime Validation](contracts.md#validate-data) - Catch mismatches early
*   [Schema Drift Detection](contracts.md#schema-drift-detection) - Alert on API changes
*   [JSON Schema Export](contracts.md#json-schema-export) - Auto-generated documentation

## Reference

### [API Reference](api.md)
Comprehensive class and function reference for the WaveQL SDK.

### [Tutorial](tutorial.md)
Step-by-step guide with examples for common use cases.


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

**3. Pushdown-First Philosophy**
We optimize at the source, not the client. WaveQL intelligently pushes down filters (`WHERE` clauses) and aggregations to the source system whenever possible, falling back to local processing only when necessary to minimize data transfer.

**4. Zero-Copy Transport**
We utilize Apache Arrow to move data efficiently between systems, minimizing serialization overhead and ensuring high performance for data science workflows.

### [Read our full Design Principles →](design-principles.md)
Explore our deep-dive on why we chose SQL, how our predicate pushdown works, and why we use Apache Arrow and Pydantic.
