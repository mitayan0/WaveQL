# SQLAlchemy & Pandas Integration Guide

> **Complete guide to using WaveQL with SQLAlchemy, Pandas, and BI tools**

WaveQL provides seamless integration with the Python data ecosystem. This guide covers everything from basic Pandas usage to advanced BI tool connections.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Pandas Integration](#pandas-integration)
3. [SQLAlchemy Integration](#sqlalchemy-integration)
4. [BI Tool Integration](#bi-tool-integration)
5. [Advanced Patterns](#advanced-patterns)
6. [Performance Optimization](#performance-optimization)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Installation

```bash
pip install waveql pandas sqlalchemy
```

### Basic Pandas Usage (5 minutes)

```python
import waveql
import pandas as pd

# Connect to your data source
conn = waveql.connect(
    "servicenow://your-instance.service-now.com",
    username="admin",
    password="your-password"
)

# Read data directly into Pandas
df = pd.read_sql(
    "SELECT * FROM incident WHERE active = true LIMIT 100",
    conn
)

print(df.head())
```

---

## Pandas Integration

### Method 1: Direct `pd.read_sql()` (Recommended)

The simplest way to use WaveQL with Pandas:

```python
import pandas as pd
import waveql

# Create connection
conn = waveql.connect("servicenow://instance.service-now.com", 
                      username="user", password="pass")

# Read with predicate pushdown (filters pushed to API)
df = pd.read_sql("""
    SELECT number, short_description, priority, state 
    FROM incident 
    WHERE priority = 1 AND state = 2
    LIMIT 1000
""", conn)

# Full Pandas power
print(df.describe())
print(df.groupby('state').count())
```

### Method 2: Cursor to DataFrame

For more control over query execution:

```python
import waveql

conn = waveql.connect("servicenow://instance.service-now.com",
                      username="user", password="pass")
cursor = conn.cursor()

# Execute query
cursor.execute("""
    SELECT sys_id, number, short_description 
    FROM incident 
    WHERE active = true
""")

# Convert to DataFrame using cursor's built-in method
df = cursor.to_df()

# Or use Arrow for zero-copy
arrow_table = cursor.to_arrow()
df = arrow_table.to_pandas()  # Zero-copy when possible
```

### Method 3: Arrow-Native Path (Best Performance)

For maximum performance with large datasets:

```python
import waveql
import pyarrow as pa

conn = waveql.connect("servicenow://instance.service-now.com",
                      username="user", password="pass")
cursor = conn.cursor()

cursor.execute("SELECT * FROM incident WHERE active = true")

# Get Arrow table (native format, no conversion)
arrow_table = cursor.to_arrow()

# Convert to Pandas with zero-copy optimization
df = arrow_table.to_pandas(
    split_blocks=True,      # Avoid memory consolidation
    self_destruct=True      # Free Arrow memory as we convert
)
```

### Joining Multiple Data Sources

WaveQL's superpower is joining data across different systems:

```python
import pandas as pd
import waveql

# Connect with multiple adapters
conn = waveql.connect()
conn.register_adapter("servicenow", waveql.adapters.ServiceNowAdapter(
    host="instance.service-now.com",
    auth_manager=waveql.auth.BasicAuth("user", "pass")
))
conn.register_adapter("salesforce", waveql.adapters.SalesforceAdapter(
    host="login.salesforce.com",
    auth_manager=waveql.auth.OAuth2(client_id="...", client_secret="...")
))

# Join across systems!
df = pd.read_sql("""
    SELECT 
        i.number, 
        i.short_description,
        a.Name as customer_name,
        a.Industry
    FROM servicenow.incident i
    JOIN salesforce.Account a ON i.account_id = a.Id
    WHERE i.priority = 1
""", conn)
```

### Exporting Results

```python
# To CSV
df.to_csv("incidents.csv", index=False)

# To Excel
df.to_excel("incidents.xlsx", index=False)

# To Parquet (recommended for large datasets)
df.to_parquet("incidents.parquet")

# Back to WaveQL/DuckDB for further processing
import duckdb
duckdb.register('incidents', df)
result = duckdb.execute("SELECT * FROM incidents WHERE priority = 1").fetchdf()
```

---

## SQLAlchemy Integration

### Basic Engine Setup

```python
from sqlalchemy import create_engine, text

# Create SQLAlchemy engine
engine = create_engine(
    "waveql+servicenow://user:password@instance.service-now.com"
)

# Execute queries
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM incident LIMIT 10"))
    for row in result:
        print(row)
```

### Connection String Formats

```python
# ServiceNow
engine = create_engine(
    "waveql+servicenow://username:password@instance.service-now.com"
)

# Salesforce
engine = create_engine(
    "waveql+salesforce://client_id:client_secret@login.salesforce.com"
)

# Jira
engine = create_engine(
    "waveql+jira://user:api_token@your-domain.atlassian.net"
)

# Multiple adapters (advanced)
engine = create_engine(
    "waveql://",
    connect_args={
        "adapters": {
            "servicenow": {
                "type": "servicenow",
                "host": "instance.service-now.com",
                "username": "user",
                "password": "pass"
            },
            "jira": {
                "type": "jira",
                "host": "domain.atlassian.net",
                "api_token": "..."
            }
        }
    }
)
```

### ORM Usage

```python
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

# Define model matching ServiceNow table structure
class Incident(Base):
    __tablename__ = 'incident'
    __table_args__ = {'schema': 'servicenow'}
    
    sys_id = Column(String, primary_key=True)
    number = Column(String)
    short_description = Column(String)
    priority = Column(Integer)
    state = Column(Integer)

# Create engine and session
engine = create_engine("waveql+servicenow://user:pass@instance.service-now.com")
session = Session(engine)

# Query using ORM
incidents = session.query(Incident).filter(
    Incident.priority == 1,
    Incident.state == 2
).limit(10).all()

for inc in incidents:
    print(f"{inc.number}: {inc.short_description}")
```

### Using with Pandas and SQLAlchemy

```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("waveql+servicenow://user:pass@instance.service-now.com")

# Method 1: Direct read_sql with engine
df = pd.read_sql("SELECT * FROM incident LIMIT 100", engine)

# Method 2: With SQL expression
from sqlalchemy import select, MetaData, Table

metadata = MetaData()
incident = Table('incident', metadata, autoload_with=engine, schema='servicenow')

query = select(incident.c.number, incident.c.short_description).where(
    incident.c.priority == 1
).limit(100)

df = pd.read_sql(query, engine)
```

---

## BI Tool Integration

### Apache Superset

1. **Install the WaveQL dialect:**
   ```bash
   pip install waveql
   ```

2. **Add database connection in Superset:**
   - Go to Data → Databases → + Database
   - Select "Other" database
   - Use connection string:
     ```
     waveql+servicenow://user:password@instance.service-now.com
     ```

3. **Configure in `superset_config.py`:**
   ```python
   # Allow WaveQL connections
   ADDITIONAL_DATABASES = ["waveql"]
   ```

4. **Create datasets and charts as usual**

### Metabase

Metabase requires a JDBC driver. Use the WaveQL JDBC bridge:

1. **Deploy WaveQL API server:**
   ```python
   # waveql_server.py
   from flask import Flask, request, jsonify
   import waveql
   
   app = Flask(__name__)
   
   @app.route('/query', methods=['POST'])
   def query():
       sql = request.json['sql']
       conn = waveql.connect(...)  # Configure your connection
       cursor = conn.cursor()
       cursor.execute(sql)
       return jsonify(cursor.fetchall())
   
   if __name__ == '__main__':
       app.run(port=5000)
   ```

2. **Connect via HTTP in Metabase** using the API endpoint

### Jupyter Notebooks

```python
# Install ipython-sql for magic commands
# pip install ipython-sql

%load_ext sql

# Connect
%sql waveql+servicenow://user:password@instance.service-now.com

# Query with magic
%%sql
SELECT number, short_description, priority
FROM incident
WHERE active = true
LIMIT 10
```

### Streamlit Dashboard

```python
import streamlit as st
import pandas as pd
import waveql

st.title("ServiceNow Dashboard")

# Cache connection
@st.cache_resource
def get_connection():
    return waveql.connect(
        "servicenow://instance.service-now.com",
        username=st.secrets["sn_user"],
        password=st.secrets["sn_pass"]
    )

conn = get_connection()

# Query with caching
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_incidents(priority):
    return pd.read_sql(f"""
        SELECT number, short_description, state, sys_created_on
        FROM incident
        WHERE priority = {priority} AND active = true
        ORDER BY sys_created_on DESC
        LIMIT 100
    """, conn)

# UI
priority = st.slider("Priority", 1, 5, 1)
df = get_incidents(priority)

st.dataframe(df)
st.bar_chart(df.groupby('state').size())
```

---

## Advanced Patterns

### Connection Pooling

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    "waveql+servicenow://user:pass@instance.service-now.com",
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600  # Recycle connections after 1 hour
)
```

### Async with Pandas

```python
import asyncio
import waveql
import pandas as pd

async def fetch_data():
    conn = await waveql.connect_async(
        "servicenow://instance.service-now.com",
        username="user", password="pass"
    )
    
    cursor = await conn.cursor()
    await cursor.execute("SELECT * FROM incident LIMIT 100")
    
    # Get Arrow table (async-safe)
    arrow_table = await cursor.to_arrow_async()
    return arrow_table.to_pandas()

# Run
df = asyncio.run(fetch_data())
```

### Parameterized Queries

```python
import pandas as pd
import waveql

conn = waveql.connect("servicenow://instance.service-now.com",
                      username="user", password="pass")

# Safe parameterized query
df = pd.read_sql(
    "SELECT * FROM incident WHERE priority = ? AND state = ?",
    conn,
    params=[1, 2]
)
```

### Chunked Reading for Large Datasets

```python
import pandas as pd
import waveql

conn = waveql.connect("servicenow://instance.service-now.com",
                      username="user", password="pass")

# Read in chunks for memory efficiency
chunks = pd.read_sql(
    "SELECT * FROM incident",
    conn,
    chunksize=10000
)

# Process each chunk
for i, chunk in enumerate(chunks):
    print(f"Processing chunk {i}: {len(chunk)} rows")
    # Process chunk...
    chunk.to_parquet(f"incidents_chunk_{i}.parquet")
```

---

## Performance Optimization

### 1. Select Only Needed Columns

```python
# Bad - fetches all columns
df = pd.read_sql("SELECT * FROM incident", conn)

# Good - fetches only needed columns  
df = pd.read_sql("""
    SELECT number, short_description, priority 
    FROM incident
""", conn)
```

### 2. Use Predicate Pushdown

```python
# Bad - fetches all data, filters in Python
df = pd.read_sql("SELECT * FROM incident", conn)
df = df[df['priority'] == 1]

# Good - filter pushed to API
df = pd.read_sql("""
    SELECT * FROM incident 
    WHERE priority = 1
""", conn)
```

### 3. Enable Caching

```python
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    username="user", password="pass",
    cache_config={
        "enabled": True,
        "default_ttl": 300,  # 5 minutes
        "max_memory_mb": 512
    }
)
```

### 4. Use Arrow for Zero-Copy

```python
cursor = conn.cursor()
cursor.execute("SELECT * FROM incident")

# Zero-copy conversion
arrow_table = cursor.to_arrow()
df = arrow_table.to_pandas(split_blocks=True, self_destruct=True)
```

### 5. Leverage Indexes

```python
# If you know the indexed columns, use them in WHERE
df = pd.read_sql("""
    SELECT * FROM incident 
    WHERE sys_id = 'abc123'  -- sys_id is indexed in ServiceNow
""", conn)
```

---

## Troubleshooting

### Common Issues

#### "No module named 'waveql'"
```bash
pip install waveql
```

#### "Connection refused" 
- Check your network/VPN connection
- Verify the hostname is correct
- Ensure credentials are valid

#### "Rate limit exceeded"
WaveQL handles rate limiting automatically. If you still see issues:
```python
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    max_retries=10,
    retry_base_delay=2.0  # Increase backoff
)
```

#### Slow queries
1. Check if predicate pushdown is working:
   ```python
   cursor.execute("EXPLAIN SELECT * FROM incident WHERE priority = 1")
   print(cursor.fetchone())
   ```

2. Enable caching for repeated queries

3. Reduce data volume with filters and LIMIT

#### Memory issues with large datasets
```python
# Use chunked reading
for chunk in pd.read_sql("SELECT * FROM incident", conn, chunksize=10000):
    process(chunk)

# Or use Arrow streaming (coming in v0.3.0)
```

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Now all WaveQL operations will be logged
conn = waveql.connect(...)
```

---

## Examples

### Complete ETL Pipeline

```python
"""
ETL Pipeline: ServiceNow → Pandas → Parquet → DuckDB Local
"""
import waveql
import pandas as pd
import duckdb
from datetime import datetime, timedelta

# Extract
print("Extracting from ServiceNow...")
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    username="user", password="pass"
)

yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

df = pd.read_sql(f"""
    SELECT 
        sys_id, number, short_description, priority, state,
        assignment_group, assigned_to, sys_created_on, sys_updated_on
    FROM incident
    WHERE sys_updated_on > '{yesterday}'
""", conn)

print(f"Extracted {len(df)} records")

# Transform
print("Transforming...")
df['priority_label'] = df['priority'].map({
    1: 'Critical', 2: 'High', 3: 'Medium', 4: 'Low', 5: 'Planning'
})
df['sys_created_on'] = pd.to_datetime(df['sys_created_on'])
df['sys_updated_on'] = pd.to_datetime(df['sys_updated_on'])

# Load
print("Loading to local DuckDB...")
df.to_parquet("incidents_staging.parquet")

local_db = duckdb.connect("analytics.duckdb")
local_db.execute("""
    CREATE TABLE IF NOT EXISTS incident_history AS 
    SELECT * FROM 'incidents_staging.parquet'
    WHERE 1=0
""")
local_db.execute("""
    INSERT INTO incident_history 
    SELECT * FROM 'incidents_staging.parquet'
""")

print("ETL complete!")
```

### Real-Time Dashboard Data

```python
"""
Flask API for real-time dashboard
"""
from flask import Flask, jsonify
import waveql
import pandas as pd

app = Flask(__name__)

# Shared connection with caching
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    username="user", password="pass",
    cache_config={"enabled": True, "default_ttl": 60}
)

@app.route('/api/incidents/summary')
def incident_summary():
    df = pd.read_sql("""
        SELECT priority, COUNT(*) as count
        FROM incident
        WHERE active = true
        GROUP BY priority
    """, conn)
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/incidents/recent')
def recent_incidents():
    df = pd.read_sql("""
        SELECT number, short_description, priority, state
        FROM incident
        WHERE active = true
        ORDER BY sys_created_on DESC
        LIMIT 20
    """, conn)
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
```

---

## Next Steps

- [Caching Guide](caching.md) - Optimize performance with query caching
- [CDC Guide](cdc.md) - Real-time change data capture
- [API Reference](api.md) - Complete API documentation
