# Adapter Reference

Adapters are the pluggable components that teach WaveQL how to talk to a specific API or data source.

## Feature Matrix

| Adapter | URI Scheme | Fetch & Pushdown | Insert | Update | Delete | Schema | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ServiceNow** | `servicenow://` | ✅ Full (`sysparm_query`) | ✅ | ✅ | ✅ | ✅ Dynamic | Parallel fetching, CDC. |
| **Salesforce** | `salesforce://` | ✅ Full (SOQL) | ✅ | ✅ | ✅ | ✅ Dynamic | Bulk API support. |
| **Jira** | `jira://` | ✅ Full (JQL) | ✅ | ✅ | ✅ | ✅ Dynamic | Supports Projects, Issues, Users. |
| **HubSpot** | `hubspot://` | ✅ Full (Search API) | ✅ | ✅ | ✅ | ✅ Dynamic | CRUD on Contacts, Companies, Deals. |
| **Shopify** | `shopify://` | ✅ Partial | ✅ | ✅ | ✅ | ⚠️ Inferred | Pagination via Headers. |
| **Zendesk** | `zendesk://` | ✅ Full (Search API) | ✅ | ✅ | ✅ | ⚠️ Inferred | Tickets, Users, Orgs. |
| **Stripe** | `stripe://` | ✅ Full (Search/List) | ✅ | ✅ | ✅ | ⚠️ Inferred | Auto-switches Search/List APIs. |
| **SQL Database**| `postgresql://`, etc.| ✅ Full (SQLAlchemy) | ✅ | ✅ | ✅ | ✅ Dynamic | Supports any SQLAlchemy dialect. |
| **Cloud Storage**| `s3://`, `gs://` | ✅ Full (DuckDB) | ❌ | ❌ | ❌ | ✅ DuckDB | Parquet, CSV, JSON, **Delta**, **Iceberg**. |
| **Google Sheets**| `google_sheets://` | ⚠️ Client-side | ✅ | ⚠️ Partial | ❌ | ⚠️ Inferred | Sheets as tables. |

---

## 1. ServiceNow Adapter
Connects to the ServiceNow Table API.

*   **URI**: `servicenow://instance.service-now.com`
*   **Capabilities**:
    *   Full `sysparm_query` support for filtering.
    *   **Aggregation Pushdown**: `COUNT`, `MIN`, `MAX`, `AVG`, `SUM`.
    *   **Change Data Capture**: Stream `insert`/`update` events.
    *   **Parallel Fetching**: Automatically fetches pages in parallel.

**Example:**
```sql
SELECT number, short_description 
FROM incident 
WHERE active = true AND priority IN (1, 2)
```

## 2. Salesforce Adapter
Connects to Salesforce REST API / SOQL.

*   **URI**: `salesforce://your-instance.my.salesforce.com`
*   **Capabilities**:
    *   Native translation to SOQL.
    *   **Bulk API 2.0**: Automatic usage for large inserts.
    *   **Async Support**: Fully async-native.

**Example:**
```sql
SELECT Id, Name, Industry FROM Account WHERE Type = 'Customer'
```

## 3. Jira Adapter
Connects to Jira Cloud API v3.

*   **URI**: `jira://domain.atlassian.net`
*   **Capabilities**:
    *   Translates SQL `WHERE` to JQL.
    *   **Virtual Tables**: `issues`, `projects`, `users`, `comments`.

**Example:**
```sql
SELECT key, summary FROM issues WHERE project = 'KAN' AND status != 'Done'
```

## 4. HubSpot Adapter
Connects to HubSpot CRM (v3 API).

*   **URI**: `hubspot://api.hubapi.com`
*   **Capabilities**:
    *   **Search API**: Pushes down filters for Contacts, Companies, Deals, etc.
    *   **Pagination**: Cursor-based.

**Example:**
```sql
SELECT firstname, email FROM contacts WHERE email LIKE '%@example.com'
```

## 5. Shopify Adapter
Connects to Shopify Admin REST API.

*   **URI**: `shopify://shop-name.myshopify.com`
*   **Capabilities**:
    *   **Fields**: `orders`, `products`, `customers`.
    *   **Optimization**: Uses specialized filter params (`status`, `ids`) when possible.

**Example:**
```sql
SELECT id, total_price FROM orders WHERE created_at >= '2024-01-01'
```

## 6. Zendesk Adapter
Connects to Zendesk Support API (v2).

*   **URI**: `zendesk://subdomain.zendesk.com`
*   **Capabilities**:
    *   Translates SQL to Zendesk Search queries (`type:ticket status:open`).

**Example:**
```sql
SELECT id, subject FROM tickets WHERE status = 'open'
```

## 7. Stripe Adapter
Connects to Stripe API (v1).

*   **URI**: `stripe://api.stripe.com`
*   **Capabilities**:
    *   **Hybrid Fetch**: Uses basic List API for full scans, Search API for filtered queries.
    *   **Tables**: `charges`, `customers`, `invoices`, `subscriptions`.

**Example:**
```sql
SELECT id, amount, status FROM charges LIMIT 50
```

## 8. SQL Database Adapter
Connects to any SQLAlchemy-supported database (Postgres, MySQL, SQL Server, Oracle, etc.).

*   **URI**: Standard connection strings (e.g., `postgresql://user:pass@localhost/db`)
*   **Capabilities**:
    *   Pass-through SQL execution.
    *   Predicate pushdown for `WHERE`, `ORDER BY`, `LIMIT`.
    *   Full CRUD support.

**Example:**
```sql
SELECT * FROM users WHERE active = true
```

## 9. Cloud Storage Adapter
Query data directly from object storage and Data Lakes.

*   **URIs**: `s3://`, `gs://`, `azure://`, `az://`
*   **Formats**: Parquet, CSV, JSON, **Delta Lake**, **Apache Iceberg**.
*   **Capabilities**:
    *   **Engine**: Powered by embedded DuckDB.
    *   **Read-Only**: Optimized for analytics (SELECT-only).

**Example:**
```sql
-- Query Parquet
SELECT * FROM "s3://bucket/data/*.parquet" WHERE year = 2024

-- Query Delta Table
SELECT * FROM delta_table('s3://bucket/delta-table/')
```

## 10. Google Sheets Adapter
Query spreadsheets as tables.

*   **URI**: Spreadsheet ID or `google_sheets://ID`
*   **Capabilities**:
    *   Treats each tab ("Sheet1") as a table.
    *   **Client-side Filtering**: Fetches data then filters in memory.

**Example:**
```sql
SELECT Name, Email FROM Sheet1 WHERE Status = 'Active'
```

## Developing Custom Adapters

Implement `BaseAdapter` to support new APIs.

```python
from waveql.adapters import BaseAdapter, register_adapter

class MyAdapter(BaseAdapter):
    def fetch(self, table, predicates=None, ...):
        # Implementation
        pass

register_adapter("myservice", MyAdapter)
```
