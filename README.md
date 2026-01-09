# WaveQL

<p align="center">
  <img src="assets/WaveQL.png" width="400" alt="WaveQL Logo" />
</p>

<p align="center">
  <strong>The Universal SQL Connector for Modern APIs</strong><br>
  <em>Query ServiceNow, Salesforce, Jira, and more using standard SQL.</em>
</p>

---

WaveQL unifies **SaaS APIs** (ServiceNow, Salesforce, Hubspot), **Databases**, and **Files** under a standard SQL interface. It translates your SQL into optimized API calls (pushing down predicates like `WHERE` and `ORDER BY`) and handles pagination/auth automatically.

## 🚀 Quick Start

```bash
pip install waveql
```

```python
import waveql

# Connect to any source
conn = waveql.connect("servicenow://instance.service-now.com", username="admin", password="password")

# Run standard SQL (We handle the API translation)
cursor = conn.cursor()
cursor.execute("""
    SELECT number, short_description 
    FROM incident 
    WHERE priority = 1 
    ORDER BY number DESC 
    LIMIT 5
""")

print(cursor.to_df())  # Returns Pandas DataFrame
```

## 📚 Documentation

| Topic | Description |
| :--- | :--- |
| **[Adapters](docs/adapters.md)** | Supported sources (ServiceNow, Salesforce, etc.) |
| **[Architecture](docs/architecture.md)** | System diagram & components |
| **[Configuration](docs/configuration.md)** | Auth, Caching, and Paths |
| **[Transactions](docs/transactions.md)** | Saga Pattern & Atomic Writes |
| **[CDC](docs/cdc.md)** | Real-time Change Data Capture |
| **[Semantic Layer](docs/semantic-layer.md)** | Virtual Views & dbt integration |

> **Contributors**: Read [AGENTS.md](AGENTS.md).

## ✨ Key Features
*   **Universal SQL**: Join `servicenow.incident` with `salesforce.account`.
*   **Smart Pushdown**: `WHERE` converts to JQL/SOQL automatically.
*   **Zero-Copy**: Built on DuckDB + PyArrow.
*   **Async Native**: `async/await` throughout.
*   **Production Ready**: Retries, rate-limiting, and pooling.

## License
MIT.
