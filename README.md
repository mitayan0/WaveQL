# WaveQL

<p align="center">
  <img src="assets/WaveQL.png" width="400" alt="WaveQL Logo" />
</p>

<p align="center">
  <strong>The Universal SQL Connector for Modern APIs</strong><br>
  <em>Query ServiceNow, Salesforce, Jira, and more using standard SQL.</em>
</p>

<p align="center">
  <a href="https://pypi.org/project/waveql/"><img src="https://img.shields.io/pypi/v/waveql?color=00d4ff&style=flat-square" alt="PyPI"></a>
  <a href="https://github.com/mitayan0/WaveQL/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue?style=flat-square" alt="License"></a>
  <a href="#"><img src="https://img.shields.io/badge/python-3.9+-3776ab?style=flat-square&logo=python&logoColor=white" alt="Python Version"></a>
  <a href="#"><img src="https://img.shields.io/badge/async-supported-green?style=flat-square" alt="Async Support"></a>
</p>

---

**WaveQL** is the **Universal SQL Connector** for your modern data stack.

It unifies **SaaS APIs** (ServiceNow, Salesforce, Jira), **Databases** (Postgres, MySQL), and **Files** (CSV, Excel/XLSX, Parquet) under a single, standard SQL interface.

Instead of writing custom scripts for every data source, use WaveQL to:
*   **Query** live API data using SQL.
*   **Join** complex data sources (e.g., "Join ServiceNow Incidents with a local Excel sheet of VIP users").
*   **Stream** changes in real-time.

Built for data engineers and developers, it translates your SQL queries into optimized API calls (pushing down predicates like `WHERE` and `ORDER BY`) and handles authentications automatically.

## Why WaveQL?

*   **Universal Adapter System**: Connect to ServiceNow, Salesforce, Jira, or generic REST APIs with a unified interface.
*   **Intelligent Query Pushdown**: We don't just fetch all data. `WHERE` clauses are translated into native API filters (e.g., JQL, SOQL) for maximum performance.
*   **Change Data Capture (CDC)**: Real-time streaming of table changes (Inserts, Updates) directly from your SaaS apps.
*   **Cross-Source JOINs**: Seamlessly join data between your local CSVs, a Jira backlog, and ServiceNow incidents using our DuckDB-powered engine.
*   **Async Built-in**: Built on `httpx` and `anyio` for high-concurrency, non-blocking applications.
*   **Data Science Ready**: Native integrations with Pandas, PyArrow, and SQLAlchemy (works with Superset!).

## Installation

```bash
pip install waveql
```

Or install from source:

```bash
git clone https://github.com/mitayan0/WaveQL.git
cd WaveQL
pip install -e .
```

## Quick Start

### 1. Querying ServiceNow

```python
import waveql

# Connect securely
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    username="admin",
    password="your-password"
)

# Execute standard SQL
cursor = conn.cursor()
cursor.execute("""
    SELECT number, short_description, priority 
    FROM incident 
    WHERE state = 1 AND priority <= 2
    ORDER BY number DESC
    LIMIT 10
""")

# Work with results
for row in cursor:
    print(f"[{row.number}] {row.short_description}")

# Or get a Pandas DataFrame instantly
df = cursor.fetchall().to_df()
print(df.head())
```

### 2. Async Support & CDC

Building a modern event-driven app?

```python
import asyncio
from waveql import connect_async

async def main():
    async with await connect_async("servicenow://...") as conn:
        # 1. Async Query
        cursor = conn.cursor()
        await cursor.execute("SELECT count(*) FROM incident")
        print(await cursor.fetchone())
        
        # 2. Stream Changes (CDC)
        async for change in conn.stream_changes("incident"):
            print(f"Update on {change.key}: {change.operation}")

asyncio.run(main())
```

### 3. The Power of "Join Global"

Combine data from APIs, Files, and Databases in one query.

```python
# 1. Register a local Excel file
conn.execute("CREATE TABLE vip_users AS SELECT * FROM 'vips.xlsx'")

# 2. Join ServiceNow Incidents with the Excel file
# Find high-priority incidents affecting VIP users
cursor.execute("""
    SELECT 
        sn.number as ticket,
        sn.short_description,
        vip.name as vip_name,
        vip.department
    FROM servicenow.incident sn
    JOIN vip_users vip ON sn.caller_id = vip.user_id
    WHERE sn.priority = 1
""")

for row in cursor:
    print(f"VIP Alert: {row.vip_name} has ticket {row.ticket}")
```

## Supported Adapters

| Adapter | URI Scheme | Features |
|:--------|:-----------|:---------|
| **ServiceNow** | `servicenow://` | Table API, **Aggregates** (SUM/COUNT/AVG), CDC, CRUD |
| **Salesforce** | `salesforce://` | SOQL Pushdown, Bulk API support, CRUD |
| **Jira** | `jira://` | JQL Pushdown, Pagination, CRUD |
| **REST** | `rest://` | Generic JSON querying |
| **File** | `file://` | CSV, Parquet, JSON (via DuckDB) |

## SQL Syntax Support

WaveQL supports ANSI SQL with full compatibility for **schema-qualified** and **quoted identifiers**:

```sql
-- All of these are equivalent and fully supported:
SELECT * FROM incident
SELECT * FROM servicenow.incident
SELECT * FROM "servicenow"."incident"
SELECT * FROM servicenow."incident"
```

**Supports:** `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`


## Authentication

WaveQL takes the headache out of auth headers.

*   **Basic Auth**: Simple username/password.
*   **API Key**: Custom headers or query params.
*   **OAuth2**: Full flow support including token refresh.

```python
from waveql.auth import AuthManager

# OAuth2 Example
auth = AuthManager(
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="your_client_id",
    oauth_client_secret="your_client_secret"
)
conn = waveql.connect("salesforce://login.salesforce.com", auth_manager=auth)
```

## Contributing

We love contributions! Whether it's a new adapter, a bug fix, or a docs improvement, please join us.

1.  Fork the repository
2.  Create your feature branch (`git checkout -b feature/amazing-feature`)
3.  Commit your changes (`git commit -m 'Add some amazing feature'`)
4.  Push to the branch (`git push origin feature/amazing-feature`)
5.  Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
