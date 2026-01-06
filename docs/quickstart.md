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

WaveQL supports two authentication methods for Salesforce:

**Method 1: Password Flow (Production/Sandbox orgs)**
```python
# Password + Security Token
conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    username="user@company.com",
    password="yourpassword" + "security_token",  # Concatenate password + token
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="your_consumer_key",
    oauth_client_secret="your_consumer_secret",
    oauth_grant_type="password",
)
```

**Method 2: Access Token (All org types, including Dev/Trailhead)**
```python
# Pre-obtained access token
conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    oauth_token="your_access_token",
    oauth_refresh_token="your_refresh_token",  # Optional, for auto-refresh
    oauth_token_url="https://your-instance.my.salesforce.com/services/oauth2/token",
    oauth_client_id="your_consumer_key",
)
```

**Getting tokens for Method 2:**
Run the included OAuth helper script:
```bash
python playground/salesforce_oauth_setup.py
```
This opens a browser for you to log in and captures the tokens automatically.

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

# Aggregations
cursor.execute("""
    SELECT Industry, COUNT(Id) cnt 
    FROM Account 
    GROUP BY Industry
""")

# CRUD Operations
cursor.execute("INSERT INTO Account (Name, Industry) VALUES ('ACME', 'Tech')")
cursor.execute("UPDATE Account SET Industry = 'Finance' WHERE Id = '001xxx'")
cursor.execute("DELETE FROM Account WHERE Id = '001xxx'")
```

### Bulk Insert

For large batch inserts:
```python
adapter = conn._get_adapter()
records = [
    {"Name": "Account 1", "Industry": "Technology"},
    {"Name": "Account 2", "Industry": "Finance"},
]
result = adapter.insert_bulk("Account", records)
print(f"Processed: {result['numberRecordsProcessed']}")
```

### Supported Objects

All Salesforce standard and custom objects: `Account`, `Contact`, `Opportunity`, `Lead`, `Case`, `CustomObject__c`, etc.

📖 **See [Salesforce Guide](salesforce.md) for complete setup instructions and troubleshooting.**

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

---

## Cloud Storage & Data Lakes ✨ NEW

Query files directly from S3, GCS, or Azure Blob Storage, including Data Lake formats like Delta Lake and Iceberg.

### Connection

```python
# S3 Parquet Files
conn = waveql.connect("s3://my-bucket/data/")

# Azure Blob Storage
conn = waveql.connect("azure://container@account.blob.core.windows.net/path/")

# Delta Lake Table
conn = waveql.connect("s3://my-bucket/delta-table/", format="delta")

# Iceberg Table
conn = waveql.connect("s3://my-bucket/iceberg/", format="iceberg", iceberg_catalog="glue")
```

### Factory Functions (Simplified)

```python
from waveql.adapters import s3_adapter, gcs_adapter, delta_table, iceberg_table

# S3 with specific credentials
adapter = s3_adapter(
    bucket="my-bucket",
    prefix="logs/",
    access_key="AKIA...",
    secret_key="secret..."
)

# Delta Lake table
adapter = delta_table("s3://bucket/delta-table/")
```

### Authentication (Credential Chain)

WaveQL automatically looks for credentials in:
1. `connect()` parameters
2. Environment variables (`AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.)
3. Config file `~/.waveql/credentials.yaml`
4. IAM Roles / Workload Identity (Automatic)

---

## Google Sheets ✨ NEW

Query any Google Spreadsheet using SQL. Each tab (sheet) in the file is treated as a table.

### Connection

```python
# Connect via Spreadsheet ID
conn = waveql.connect(
    "google_sheets://1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
    service_account_json="path/to/creds.json"
)

# Connect via full URL
conn = waveql.connect("https://docs.google.com/spreadsheets/d/1BxiMVs...")
```

### Querying

```python
cursor = conn.cursor()

# Query Sheet1
cursor.execute("SELECT Name, Email FROM Sheet1 WHERE Status = 'Active'")

# Append a row
cursor.execute("""
    INSERT INTO Sheet1 (Name, Email, Status) 
    VALUES ('Jane Smith', 'jane@example.com', 'Pending')
""")
```

---

## Streaming Large Datasets ✨ NEW

For million-row result sets that exceed available memory, use WaveQL's streaming API.

### 1. Batch Streaming (Arrow)

Yields data in `pyarrow.RecordBatch` chunks as they arrive from the source.

```python
# Sync streaming
cursor.execute("SELECT * FROM large_table")
for batch in cursor.stream_batches():
    print(f"Processing batch of {batch.num_rows} rows")
    process(batch)

# Async streaming with prefetching and backpressure
async for batch in cursor.stream_batches_async("SELECT * FROM large_table"):
    await process_async(batch)
```

### 2. Direct-to-File Export

Stream results directly to disk without ever loading more than one batch into memory.

```python
# Export 10M rows from Salesforce to Parquet
cursor.stream_to_file(
    "SELECT * FROM Opportunity",
    output_path="opportunities_export.parquet",
    format="parquet"
)
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
