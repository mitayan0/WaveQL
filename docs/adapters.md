# Adapter Reference

Adapters are the pluggable components that teach WaveQL how to talk to a specific API.

## Built-in Adapters

### 1. ServiceNow Adapter
Connects to the ServiceNow Table API.

*   **URI**: `servicenow://instance.service-now.com`
*   **Capabilities**:
    *   Full `sysparm_query` support for filtering.
    *   Field selection (`sysparm_fields`).
    *   Limit/Offset pagination.
    *   **Aggregation Pushdown**: `COUNT`, `MIN`, `MAX`, `AVG`, `SUM` are executed on the server via the Stats API.
    *   **Change Data Capture**: Stream `insert`/`update` events in real-time.
    *   **Write Support**: `INSERT`, `UPDATE`, `DELETE` operations.
    *   **Parallel Fetching**: Automatically fetches pages in parallel for large datasets.

**Example:**
```sql
-- Simple query
SELECT number, short_description 
FROM incident 
WHERE active = true AND priority IN (1, 2)

-- With explicit schema prefix (both formats work)
SELECT * FROM servicenow.incident WHERE state = 1
SELECT * FROM "servicenow"."incident" WHERE state = 1
```

### 2. Salesforce Adapter
Connects to the Salesforce REST API / SOQL Endpoint.

*   **URI**: `salesforce://login.salesforce.com` (or specific instance domain)
*   **Capabilities**:
    *   Native translation to SOQL.
    *   Relationship queries (e.g., `SELECT Account.Name FROM Contact`).
    *   Bulk API 2.0 support for large extracts (automatic switchover for >50k rows).

**Example:**
```sql
SELECT Id, Name, (SELECT LastName FROM Contacts)
FROM Account
WHERE Industry = 'Tech'
```

### 3. Jira Adapter
Connects to Jira Cloud API v3.

*   **URI**: `jira://domain.atlassian.net`
*   **Capabilities**:
    *   Translates SQL `WHERE` to JQL.
    *   Supports `ORDER BY` mapping.
    *   Automatic expansion of nested fields like `fields.status.name`.

**Example:**
```sql
SELECT key, summary 
FROM issues 
WHERE project = 'KAN' AND status != 'Done'
```

## Developing Custom Adapters

You can implement `BaseAdapter` to support internal or obscure APIs.

### The Interface

```python
from waveql.adapters import BaseAdapter, register_adapter

class MyCustomAdapter(BaseAdapter):
    def connect(self):
        # Setup session/auth
        pass

    def execute_query(self, query: str, params: dict = None):
        # 1. Parse SQL
        # 2. Extract predicates
        # 3. Call API
        # 4. Return Arrow Table / Generator
        pass

# Register it
register_adapter("myservice", MyCustomAdapter)
```

### Usage
```python
conn = waveql.connect("myservice://api.internal.corp")
```
