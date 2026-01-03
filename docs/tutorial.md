# WaveQL Tutorial

This tutorial will guide you through setting up WaveQL, connecting to data sources, running queries, and using advanced features like Change Data Capture (CDC).

## 1. Installation

First, install WaveQL using pip:

```bash
pip install waveql
```

Or from source:

```bash
git clone https://github.com/mitayan0/WaveQL.git
cd WaveQL
pip install -e .
```

## 2. Basic Connection & Querying

Let's connect to a ServiceNow instance and fetch some data.

```python
import waveql

# 1. Connect
# Supports Basic Auth, OAuth, or API Keys depending on the adapter
conn = waveql.connect(
    "servicenow://dev12345.service-now.com",
    username="admin",
    password="password"
)

# 2. Create a Cursor
cursor = conn.cursor()

# 3. Execute SQL
# WaveQL pushes down filters to the API!
query = """
    SELECT number, short_description, priority, state 
    FROM incident 
    WHERE active = true 
      AND priority <= 2
    ORDER BY number DESC
    LIMIT 5
"""
cursor.execute(query)

# 4. Fetch Results
results = cursor.fetchall()
for row in results:
    print(f"[{row.number}] {row.short_description} (Priority: {row.priority})")

# 5. Convert to Pandas (optional)
df = cursor.to_df()
print(df.head())
```

## 3. Aggregations & Analytics

WaveQL supports standard SQL aggregations. For ServiceNow, these are pushed down to the `stats` API.

```python
cursor.execute("""
    SELECT priority, COUNT(*) as count, AVG(impact) as avg_impact
    FROM incident
    GROUP BY priority
    ORDER BY count DESC
""")

for row in cursor:
    print(f"Priority {row.priority}: {row.count} incidents")
```

## 4. Cross-Source Joins

You can join data from different sources (e.g., ServiceNow and a local CSV or another API). This is handled by DuckDB locally after efficient fetching.

```python
# Assume we have a CSV of critical users
conn.execute("CREATE TABLE vip_users AS SELECT * FROM 'users.csv'")

# Join ServiceNow incidents with VIP users
cursor.execute("""
    SELECT 
        inc.number,
        inc.short_description,
        vip.email
    FROM servicenow.incident inc
    JOIN vip_users vip ON inc.caller_id = vip.sys_id
    WHERE inc.priority = 1
""")
```

## 5. Async Support

For high-performance applications (like FastAPI), use the async interface.

```python
import asyncio
from waveql import connect_async

async def main():
    async with await connect_async("servicenow://...") as conn:
        cursor = conn.cursor()
        
        # Parallel or Concurrent queries
        await cursor.execute("SELECT count(*) FROM incident")
        print(await cursor.fetchone())

asyncio.run(main())
```

## 6. Change Data Capture (CDC)

WaveQL can stream changes from your data sources in real-time. This is perfect for building event-driven architectures.

```python
import asyncio
from waveql import connect_async

async def watch_incidents():
    async with await connect_async("servicenow://...") as conn:
        print("Watching for new incidents...")
        
        # Stream changes
        async for change in conn.stream_changes("incident"):
            if change.operation == "insert":
                print(f"New Incident! {change.data['number']}: {change.data['short_description']}")
            elif change.operation == "update":
                print(f"Incident {change.key} updated.")

asyncio.run(watch_incidents())
```

## 7. Data Modification (CRUD)

You can also Insert, Update, and Delete records using standard SQL.

```python
# INSERT
cursor.execute("""
    INSERT INTO incident (short_description, priority)
    VALUES ('Server down', 1)
""")

# UPDATE
cursor.execute("""
    UPDATE incident 
    SET state = 2 
    WHERE number = 'INC0010001'
""")

# DELETE
cursor.execute("DELETE FROM incident WHERE number = 'INC0010001'")
```
