# Quick Start Guide

This guide covers how to connect to and query all supported data sources in WaveQL.

## Installation

```bash
pip install waveql
```

Or from source:
```bash
git clone https://github.com/mitayan0/WaveQL.git
cd WaveQL
pip install -e .
```

---

## Connection Methods

WaveQL supports two ways to connect:

### 1. Connection String (Recommended)

```python
import waveql

# Credentials in URL
conn = waveql.connect("servicenow://admin:password@dev.service-now.com")

# Credentials as parameters
conn = waveql.connect(
    "servicenow://dev.service-now.com",
    username="admin",
    password="password"
)
```

### 2. Explicit Parameters

```python
conn = waveql.connect(
    adapter="servicenow",
    host="dev.service-now.com",
    username="admin",
    password="password"
)
```

### Connection String Format

```
adapter://[username:password@]host[:port][?param1=value1&param2=value2]
```

| Component | Required | Example |
|-----------|----------|---------|
| `adapter` | Yes | `servicenow`, `jira`, `salesforce`, `rest`, `file` |
| `username:password@` | No | `admin:secret@` |
| `host` | Yes | `dev.service-now.com` |
| `:port` | No | `:443` |
| `?params` | No | `?display_value=all` |

---

## ServiceNow

### Connection

```python
# Basic Auth
conn = waveql.connect("servicenow://admin:password@dev12345.service-now.com")

# With display_value parameter (shows display values instead of sys_ids)
conn = waveql.connect(
    "servicenow://admin:password@dev12345.service-now.com?display_value=all"
)

# OAuth (for production)
conn = waveql.connect(
    "servicenow://dev12345.service-now.com",
    oauth_client_id="your_client_id",
    oauth_client_secret="your_client_secret",
    oauth_token_url="https://dev12345.service-now.com/oauth_token.do"
)
```

### Query Examples

```python
cursor = conn.cursor()

# Simple query
cursor.execute("""
    SELECT number, short_description, priority, state 
    FROM incident 
    WHERE active = true 
    LIMIT 10
""")

# With filters (pushed to API)
cursor.execute("""
    SELECT number, short_description, assigned_to.display_value AS assignee
    FROM incident 
    WHERE priority <= 2 AND state != 7
    ORDER BY sys_created_on DESC
""")

# Aggregations (uses ServiceNow Stats API)
cursor.execute("""
    SELECT priority, COUNT(*) as count 
    FROM incident 
    GROUP BY priority
""")

# Fetch results
for row in cursor.fetchall():
    print(row)

# Or convert to Pandas
df = cursor.to_df()
```

### Supported Tables

All ServiceNow tables are accessible: `incident`, `problem`, `change_request`, `sys_user`, `cmdb_ci`, etc.

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `display_value` | `true`, `false`, or `all` | `false` |
| `exclude_reference_link` | Exclude reference links | `true` |
| `sysparm_limit` | Page size | `1000` |

---

## Jira

### Connection

```python
# API Token Authentication (recommended for Jira Cloud)
conn = waveql.connect(
    "jira://your-email@company.com:your_api_token@company.atlassian.net"
)

# Or with explicit parameters
conn = waveql.connect(
    "jira://company.atlassian.net",
    username="your-email@company.com",
    api_key="your_api_token"  # API token goes here
)
```

**Getting an API Token:**
1. Go to https://id.atlassian.com/manage-profile/security/api-tokens
2. Create a new token
3. Use your email as username and the token as password/api_key

### Query Examples

```python
cursor = conn.cursor()

# Fetch issues
cursor.execute("""
    SELECT key, summary, status.name AS status, priority.name AS priority
    FROM issues 
    WHERE project = 'PROJ' AND status != 'Done'
    ORDER BY created DESC
    LIMIT 20
""")

# Access nested fields with dot notation
cursor.execute("""
    SELECT 
        key,
        fields.summary,
        fields.reporter.displayName AS reporter,
        fields.assignee.displayName AS assignee,
        fields.status.name AS status
    FROM issues
    WHERE fields.priority.name = 'High'
""")

# Fetch projects
cursor.execute("SELECT key, name FROM projects")

# Create an issue
cursor.execute("""
    INSERT INTO issues (project, issuetype, summary, description)
    VALUES ('PROJ', 'Task', 'New task title', 'Task description')
""")
```

### Supported Tables

| Table | Description |
|-------|-------------|
| `issues` / `issue` | Jira issues (uses JQL search) |
| `projects` / `project` | Jira projects |
| `users` / `user` | Jira users |

### JQL Translation

WaveQL automatically translates SQL `WHERE` clauses to JQL:

| SQL | JQL |
|-----|-----|
| `status = 'Open'` | `status = "Open"` |
| `status IN ('Open', 'In Progress')` | `status IN ("Open", "In Progress")` |
| `summary LIKE '%bug%'` | `summary ~ "bug"` |
| `assignee IS NULL` | `assignee IS EMPTY` |

---

## Salesforce

### Connection

```python
# Username + Password + Security Token
conn = waveql.connect(
    "salesforce://user@company.com:password+security_token@login.salesforce.com"
)

# OAuth (recommended for production)
conn = waveql.connect(
    "salesforce://myorg.my.salesforce.com",
    oauth_client_id="your_connected_app_id",
    oauth_client_secret="your_connected_app_secret",
    oauth_token_url="https://login.salesforce.com/services/oauth2/token"
)

# With API version
conn = waveql.connect(
    "salesforce://myorg.my.salesforce.com?api_version=v58.0"
)
```

### Query Examples

```python
cursor = conn.cursor()

# Query Accounts
cursor.execute("""
    SELECT Id, Name, Industry, AnnualRevenue 
    FROM Account 
    WHERE Industry = 'Technology'
    LIMIT 100
""")

# Relationship queries (SOQL style)
cursor.execute("""
    SELECT Id, Name, Account.Name AS AccountName
    FROM Contact
    WHERE Account.Industry = 'Finance'
""")

# Aggregations
cursor.execute("""
    SELECT Industry, COUNT(Id) as count 
    FROM Account 
    GROUP BY Industry
""")
```

### Supported Objects

All Salesforce standard and custom objects: `Account`, `Contact`, `Opportunity`, `Lead`, `Case`, `CustomObject__c`, etc.

---

## REST API (Generic)

For any REST API that returns JSON arrays.

### Connection

```python
# Basic REST endpoint
conn = waveql.connect("rest://api.example.com")

# With authentication
conn = waveql.connect(
    "rest://api.example.com",
    api_key="your_api_key"
)

# With custom data path (if data is nested in response)
conn = waveql.connect(
    "rest://api.example.com?data_path=results.items"
)
```

### Configuration

```python
from waveql.adapters import RESTAdapter

adapter = RESTAdapter(
    host="https://api.example.com",
    endpoints={
        "users": "/api/v1/users",
        "orders": "/api/v1/orders",
    },
    data_path="data",  # Extract from response.data
    pagination_type="offset",  # or "cursor", "page"
    page_size=100,
)

conn = waveql.connect()
conn.register_adapter("myapi", adapter)

# Now query
cursor = conn.cursor()
cursor.execute("SELECT * FROM myapi.users WHERE active = true")
```

### Query Examples

```python
cursor.execute("""
    SELECT id, name, email 
    FROM users 
    WHERE status = 'active'
    LIMIT 50
""")

# Access nested data with dot notation
cursor.execute("""
    SELECT 
        id,
        profile.name,
        profile.email,
        settings.notifications
    FROM users
""")
```

---

## Files (CSV, Parquet, Excel)

### Connection

```python
# CSV file
conn = waveql.connect("file:///C:/data/customers.csv")
conn = waveql.connect("file:///home/user/data.csv")

# Parquet file
conn = waveql.connect("file:///data/sales.parquet")

# Excel file
conn = waveql.connect("file:///reports/quarterly.xlsx")
```

### Query Examples

```python
cursor = conn.cursor()

# Query the file directly
cursor.execute("SELECT * FROM data LIMIT 100")

# With filtering
cursor.execute("""
    SELECT customer_id, name, total_spent 
    FROM data 
    WHERE total_spent > 1000
    ORDER BY total_spent DESC
""")
```

### DuckDB Direct Access

For more complex file operations, use DuckDB directly:

```python
conn = waveql.connect()

# Query CSV directly with DuckDB
cursor = conn.cursor()
cursor.execute("""
    SELECT * FROM 'sales_2024.csv' 
    WHERE region = 'West'
""")

# Query Parquet
cursor.execute("SELECT * FROM 'data/*.parquet'")  # Glob patterns work!

# Query Excel
cursor.execute("SELECT * FROM 'report.xlsx'")
```

---

## Cross-Source Joins

One of WaveQL's superpowers: join data from different sources!

```python
conn = waveql.connect()

# Register multiple adapters
from waveql.adapters import ServiceNowAdapter, JiraAdapter

conn.register_adapter("snow", ServiceNowAdapter(
    host="dev.service-now.com",
    username="admin",
    password="password"
))

conn.register_adapter("jira", JiraAdapter(
    host="company.atlassian.net",
    username="email@company.com",
    api_key="api_token"
))

# Load a CSV into DuckDB
conn.duckdb.execute("CREATE TABLE employees AS SELECT * FROM 'employees.csv'")

# Join ServiceNow incidents with Jira issues and local CSV!
cursor = conn.cursor()
cursor.execute("""
    SELECT 
        inc.number AS incident,
        j.key AS jira_key,
        e.name AS assignee_name
    FROM snow.incident inc
    JOIN jira.issues j ON inc.correlation_id = j.key
    JOIN employees e ON inc.assigned_to = e.employee_id
    WHERE inc.priority = 1
""")
```

---

## Async Support

For high-performance applications:

```python
import asyncio
from waveql import connect_async

async def main():
    conn = await connect_async(
        "servicenow://admin:password@dev.service-now.com"
    )
    
    async with conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT * FROM incident LIMIT 10")
        results = await cursor.fetchall()
        print(results)

asyncio.run(main())
```

---

## Authentication Options

### Basic Auth
```python
conn = waveql.connect("adapter://host", username="user", password="pass")
```

### API Key
```python
conn = waveql.connect("adapter://host", api_key="your_api_key")
```

### OAuth 2.0
```python
conn = waveql.connect(
    "adapter://host",
    oauth_client_id="client_id",
    oauth_client_secret="client_secret",
    oauth_token_url="https://host/oauth/token",
    oauth_grant_type="client_credentials"  # or "password", "refresh_token"
)
```

### Bearer Token
```python
conn = waveql.connect("adapter://host", oauth_token="your_bearer_token")
```

---

## Error Handling

WaveQL provides rich error messages:

```python
from waveql.exceptions import AdapterError, RateLimitError, AuthenticationError

try:
    cursor.execute("SELECT * FROM incident")
except RateLimitError as e:
    print(f"Rate limited! Retry in {e.retry_after} seconds")
except AuthenticationError as e:
    print(f"Auth failed: {e.message}")
    print(f"Suggestion: {e.suggestion}")
except AdapterError as e:
    print(f"API Error [{e.error_code}]: {e.message}")
```

See [Error Handling](error-handling.md) for complete reference.

---

## Next Steps

- [Schema Inference & Nested JSON](schema-inference.md) - Query nested data with dot notation
- [Change Data Capture (CDC)](cdc.md) - Stream real-time changes
- [Performance Tuning](performance.md) - Optimize for large datasets
- [API Reference](api.md) - Full SDK documentation
