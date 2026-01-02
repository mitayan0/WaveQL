# WaveQL

<p align="center">
  <img src="assets/logo.png" width="200" alt="WaveQL Logo" />
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

**WaveQL** transforms the way you interact with SaaS APIs. Instead of wrestling with complex, proprietary REST endpoints and pagination logic, WaveQL lets you query your business data using the language you already know: **SQL**.

Built for data engineers and developers, it translates your SQL queries into optimized API calls (pushing down predicates like `WHERE` and `ORDER BY`), handles authentications, and returns high-performance **Arrow** or **Pandas** dataframes.

## 🚀 Why WaveQL?

*   **🔌 Universal Adapter System**: Connect to ServiceNow, Salesforce, Jira, or generic REST APIs with a unified interface.
*   **⚡ Intelligent Query Pushdown**: We don't just fetch all data. `WHERE` clauses are translated into native API filters (e.g., JQL, SOQL) for maximum performance.
*   **🔄 Cross-Source JOINs**: Seamlessly join data between your local CSVs, a Jira backlog, and ServiceNow incidents using our DuckDB-powered engine.
*   **⚡ Async Built-in**: Built on `httpx` and `anyio` for high-concurrency, non-blocking applications.
*   **🐼 Data Science Ready**: Native integrations with Pandas, PyArrow, and SQLAlchemy (works with Superset!).

## 📦 Installation

```bash
pip install waveql
```

Or install from source:

```bash
git clone https://github.com/mitayan0/WaveQL.git
cd WaveQL
pip install -e .
```

## ⚡ Quick Start

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

### 2. Async Support

Building a modern FastAPI or async app? We've got you covered.

```python
import asyncio
from waveql import connect_async

async def main():
    conn = await connect_async(
        "jira://your-domain.atlassian.net",
        username="user@example.com",
        password="api-token"
    )
    
    cursor = conn.cursor()
    # Predicates are pushed down to JQL!
    await cursor.execute("SELECT key, summary FROM issues WHERE project = 'PROJ'")
    
    results = await cursor.fetchall()
    print(results)

asyncio.run(main())
```

### 3. The Power of Cross-Source Joins

Combine data from anywhere.

```python
conn.execute("""
    SELECT 
        sn.number as ticket_id,
        jira.key as engineering_task,
        sn.short_description
    FROM servicenow.incident sn
    JOIN jira.issues jira ON sn.correlation_id = jira.key
    WHERE sn.priority = 1
""")
```

## 🛠 Supported Adapters

| Adapter | URI Scheme | Features |
|:--------|:-----------|:---------|
| **ServiceNow** | `servicenow://` | ✅ Table API, ✅ Aggregates, ✅ Write (CRUD) |
| **Salesforce** | `salesforce://` | ✅ SOQL Pushdown, ✅ Bulk API support |
| **Jira** | `jira://` | ✅ JQL Pushdown, ✅ Pagination |
| **REST** | `rest://` | ⚠️ Generic JSON querying |
| **File** | `file://` | ✅ CSV, Parquet, JSON (via DuckDB) |

## 🔐 Authentication

WaveQL takes the headache out of auth headers.

*   **Basic Auth**: Simple username/password.
*   **API Key**: Custom headers or query params.
*   **OAuth2**: Full flow support including token refresh.

```python
from waveql.auth import AuthManager

# OAuth2 Example
auth = AuthManager(
    oauth2_token_url="https://login.salesforce.com/services/oauth2/token",
    client_id="your_client_id",
    client_secret="your_client_secret"
)
conn = waveql.connect("salesforce://login.salesforce.com", auth_manager=auth)
```

## 🤝 Contributing

We love contributions! Whether it's a new adapter, a bug fix, or a docs improvement, please join us.

1.  Fork the repository
2.  Create your feature branch (`git checkout -b feature/amazing-feature`)
3.  Commit your changes (`git commit -m 'Add some amazing feature'`)
4.  Push to the branch (`git push origin feature/amazing-feature`)
5.  Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2026 **Mitayan Chakma**.

---


