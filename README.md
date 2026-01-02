# WaveQL

<p align="center">
  <strong>Query APIs using SQL — An open-source alternative to CData</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#adapters">Adapters</a> •
  <a href="#documentation">Documentation</a>
</p>

---

**WaveQL** is a Python library that lets you query REST APIs using standard SQL. It translates SQL queries into optimized API calls with predicate pushdown, handles pagination automatically, and returns results as Arrow tables for high-performance analytics.

## Features

- 🔌 **Multiple Adapters**: ServiceNow, Salesforce, Jira, REST APIs, CSV/Parquet files
- ⚡ **Predicate Pushdown**: SQL WHERE clauses are pushed to APIs (JQL, SOQL, ServiceNow Query)
- 🔄 **Aggregation Pushdown**: COUNT, SUM, AVG pushed to source when supported
- 🔗 **Cross-Source JOINs**: Join data across different APIs using DuckDB
- 🔐 **Authentication**: OAuth2, API Keys, Basic Auth via AuthManager
- 🚀 **Async Support**: Full async/await support for non-blocking I/O
- 🏊 **Connection Pooling**: Thread-safe HTTP session reuse
- 📊 **SQLAlchemy Dialect**: Use with Pandas, Superset, and other tools
- 🎯 **DB-API 2.0 Compliant**: Standard Python database interface

## Installation

```bash
# From GitHub
pip install git+https://github.com/mitayan0/WaveQL.git

# Or clone and install locally
git clone https://github.com/mitayan0/WaveQL.git
cd WaveQL
pip install -e .
```

### Requirements

- Python 3.9+
- requests, httpx, pyarrow, duckdb, sqlalchemy, anyio

## Quick Start

### ServiceNow

```python
from waveql import connect

# Connect to ServiceNow
conn = connect(
    "servicenow://your-instance.service-now.com",
    username="admin",
    password="your-password"
)

# Query incidents
cursor = conn.cursor()
cursor.execute("""
    SELECT number, short_description, priority 
    FROM incident 
    WHERE state = 1 
    LIMIT 100
""")

# Fetch results
for row in cursor:
    print(row)

# Or convert to pandas
df = cursor.fetchall().to_df()
```

### Salesforce

```python
from waveql import connect

conn = connect(
    "salesforce://login.salesforce.com",
    client_id="your_client_id",
    client_secret="your_client_secret",
    username="user@example.com",
    password="password+security_token"
)

cursor = conn.cursor()
cursor.execute("SELECT Id, Name, Industry FROM Account WHERE Industry = 'Technology'")
accounts = cursor.fetchall()
```

### Jira

```python
from waveql import connect

conn = connect(
    "jira://your-domain.atlassian.net",
    username="email@example.com",
    password="your-api-token"
)

cursor = conn.cursor()
cursor.execute("""
    SELECT key, summary, status, assignee 
    FROM issues 
    WHERE project = 'PROJ' AND status = 'Open'
""")
issues = cursor.fetchall()
```

### Cross-Source JOIN

```python
# Join ServiceNow incidents with Jira issues
conn.execute("""
    SELECT 
        sn.number as incident,
        jira.key as issue,
        sn.short_description
    FROM servicenow.incident sn
    JOIN jira.issues jira ON sn.correlation_id = jira.key
    WHERE sn.state = 1
""")
```

### Async Support

```python
import asyncio
from waveql import connect_async

async def main():
    conn = await connect_async(
        "servicenow://instance.service-now.com",
        username="admin",
        password="password"
    )
    
    cursor = conn.cursor()
    await cursor.execute("SELECT * FROM incident LIMIT 10")
    results = await cursor.fetchall()
    print(results)

asyncio.run(main())
```

## Adapters

| Adapter | Predicate Pushdown | Aggregation | CRUD | Async |
|---------|-------------------|-------------|------|-------|
| **ServiceNow** | ✅ ServiceNow Query | ✅ Stats API | ✅ | ✅ |
| **Salesforce** | ✅ SOQL | ✅ SOQL | ✅ | ✅ |
| **Jira** | ✅ JQL | ❌ | ✅ | ✅ |
| **REST** | ⚠️ Configurable | ❌ | ✅ | ❌ |
| **File** | ✅ DuckDB | ✅ DuckDB | ❌ | ❌ |

## Authentication

WaveQL supports multiple authentication methods:

```python
from waveql.auth import AuthManager

# Basic Auth
auth = AuthManager(username="user", password="pass")

# API Token
auth = AuthManager(api_key="your-api-key", api_key_header="Authorization")

# OAuth2 Client Credentials
auth = AuthManager(
    oauth2_token_url="https://login.example.com/oauth/token",
    client_id="your_client_id",
    client_secret="your_client_secret"
)

# Use with adapter
conn = connect("servicenow://instance.service-now.com", auth_manager=auth)
```

## Connection Pooling

WaveQL automatically pools HTTP connections for better performance:

```python
from waveql.utils import PoolConfig, configure_pools

# Configure pool settings
config = PoolConfig(
    max_connections_per_host=20,
    max_total_connections=200,
    connect_timeout=10.0,
    read_timeout=30.0,
)
configure_pools(config)

# All connections now use the shared pool
conn1 = connect("servicenow://instance1.service-now.com", ...)
conn2 = connect("servicenow://instance2.service-now.com", ...)
```

## SQLAlchemy Integration

Use WaveQL with Pandas, Superset, or any SQLAlchemy-compatible tool:

```python
from sqlalchemy import create_engine
import pandas as pd

# Create engine
engine = create_engine(
    "waveql.servicenow://instance.service-now.com",
    connect_args={"username": "admin", "password": "pass"}
)

# Query with pandas
df = pd.read_sql("SELECT * FROM incident LIMIT 100", engine)
```

## CRUD Operations

```python
# INSERT
cursor.execute("""
    INSERT INTO incident (short_description, priority)
    VALUES ('New issue', 2)
""")

# UPDATE
cursor.execute("""
    UPDATE incident 
    SET priority = 1 
    WHERE sys_id = 'abc123'
""")

# DELETE
cursor.execute("DELETE FROM incident WHERE sys_id = 'abc123'")
```

## Documentation

- [API Reference](docs/api.md)
- [Adapter Guide](docs/adapters.md)
- [Authentication](docs/auth.md)
- [Performance Tuning](docs/performance.md)

## Roadmap

See [ROADMAP.md](ROADMAP.md) for planned features.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  Made with ❤️ for the data engineering community
</p>
