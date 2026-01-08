# Semantic Layer Integration

WaveQL provides semantic layer capabilities to make your SQL queries more reusable and maintainable.

## Features

### 1. Virtual Views

Define reusable SQL views over API data that can be queried like tables.

```python
import waveql
from waveql.semantic import VirtualView, VirtualViewRegistry

# Connect
conn = waveql.connect("servicenow://instance.service-now.com", ...)

# Define views
registry = VirtualViewRegistry()

registry.register(VirtualView(
    name="active_incidents",
    sql="SELECT * FROM incident WHERE active = true",
    description="All active incidents"
))

registry.register(VirtualView(
    name="critical_incidents",
    sql="SELECT * FROM active_incidents WHERE priority <= 2",
    description="P1 and P2 active incidents",
    dependencies=["active_incidents"]  # Created after active_incidents
))

# Register all views
conn.register_views(registry)

# Now query them like tables!
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM critical_incidents")
```

### Single View Registration

```python
# Quick inline view
conn.register_view(
    "open_tickets",
    "SELECT * FROM tickets WHERE status = 'open'"
)

cursor.execute("SELECT * FROM open_tickets LIMIT 10")
```

### Loading from Files

```yaml
# views.yaml
version: "1.0"
views:
  - name: vip_users
    sql: "SELECT * FROM users WHERE tier = 'premium'"
    description: Premium tier users
    tags: [user, premium]
    
  - name: vip_incidents
    sql: "SELECT i.* FROM incident i JOIN vip_users v ON i.caller_id = v.user_id"
    dependencies: [vip_users]
```

```python
registry = VirtualViewRegistry.from_file("views.yaml")
conn.register_views(registry)
```

---

## 2. Saved Queries

Parameterized SQL templates with type validation.

```python
from waveql.semantic import SavedQuery

# Define a parameterized query
query = SavedQuery(
    name="incidents_by_priority",
    sql="SELECT * FROM incident WHERE priority <= :max_priority AND state = :state",
    parameters={
        "max_priority": {
            "type": "int",
            "default": 2,
            "description": "Maximum priority level (1-5)"
        },
        "state": {
            "type": "str",
            "choices": ["open", "in_progress", "resolved"],
            "description": "Incident state filter"
        }
    },
    description="Get incidents filtered by priority and state"
)

# Execute with parameters
cursor = conn.execute_saved(query, max_priority=1, state="open")
results = cursor.fetchall()
```

### Parameter Types

| Type | Description | Example |
|------|-------------|---------|
| `str` | String (auto-quoted) | `'value'` |
| `int` | Integer | `42` |
| `float` | Decimal | `3.14` |
| `bool` | Boolean | `TRUE` / `FALSE` |
| `list` | IN clause | `('a', 'b', 'c')` |
| `date` | Date string | `'2025-01-01'` |

### Saving Queries to Files

```yaml
# queries.yaml
version: "1.0"
queries:
  - name: recent_orders
    sql: "SELECT * FROM orders WHERE created_at >= :since LIMIT :limit"
    description: Get recent orders
    parameters:
      since:
        type: date
        description: Start date
      limit:
        type: int
        default: 100
```

```python
from waveql.semantic import SavedQueryRegistry

registry = SavedQueryRegistry.from_file("queries.yaml")
sql = registry.render("recent_orders", since="2025-01-01", limit=50)
```

---

## 3. dbt Integration

Read dbt `manifest.json` to expose dbt models as queryable tables.

### Quick Start

```python
from waveql.semantic import DbtManifest

# Load manifest.json from dbt project
manifest = DbtManifest.from_file("target/manifest.json")

# List all models
for model in manifest.models:
    print(f"{model.name}: {model.materialized} - {model.description}")

# Register models as WaveQL views
conn.register_dbt_models(manifest)

# Now query dbt models directly!
cursor.execute("SELECT * FROM stg_customers WHERE is_active = true")
```

### One-Liner Project Loading

```python
# Load from project directory (looks for target/manifest.json)
conn.load_dbt_project("/path/to/my_dbt_project")

# Query your dbt models
cursor.execute("SELECT * FROM dim_users JOIN fct_orders USING (user_id)")
```

### Advanced Usage

```python
# Get model lineage
lineage = manifest.get_model_lineage("dim_users")
print(f"Upstream: {lineage['upstream']}")
print(f"Downstream: {lineage['downstream']}")

# Filter models
staging_models = manifest.list_models(tag="staging")
marts = manifest.list_models(materialized="table", schema="marts")

# Exclude certain tags
conn.register_dbt_models(manifest, exclude_tags=["deprecated", "wip"])
```

### How It Works

1. **Parse Manifest**: WaveQL reads the `manifest.json` that dbt generates on `dbt compile` or `dbt run`.
2. **Extract Compiled SQL**: Each model's `compiled_sql` (with Jinja resolved) is extracted.
3. **Create Views**: Models are registered as DuckDB views in dependency order.
4. **Query**: You can now query the views, which execute the compiled SQL.

> ⚠️ **Note**: You must run `dbt compile` or `dbt run` before loading models into WaveQL to generate the `manifest.json`.

---

## API Reference

### VirtualView

```python
VirtualView(
    name: str,              # View name (must be valid SQL identifier)
    sql: str,               # SQL query defining the view
    description: str = "",  # Human-readable description
    schema: str = None,     # Optional schema prefix
    dependencies: List[str] = [],  # Other views this depends on
    tags: List[str] = [],   # Tags for filtering
    metadata: Dict = {}     # Custom metadata
)
```

### SavedQuery

```python
SavedQuery(
    name: str,              # Query name
    sql: str,               # SQL with :param placeholders
    parameters: Dict,       # Parameter definitions
    description: str = "",  # Human-readable description
    tags: List[str] = []    # Tags for filtering
)

# Parameter definition
{
    "param_name": {
        "type": "int",       # str, int, float, bool, list, date
        "default": 42,       # Default value (optional)
        "required": True,    # Is parameter required?
        "choices": [1, 2, 3], # Allowed values (optional)
        "description": "..."  # Documentation
    }
}
```

### DbtManifest

```python
DbtManifest.from_file(path)        # Load from manifest.json
DbtManifest.from_project(path)     # Load from dbt project directory

manifest.models                     # List of DbtModel
manifest.sources                    # List of DbtSource
manifest.get_model(name)           # Get specific model
manifest.list_models(tag=, schema=, materialized=)
manifest.get_model_lineage(name)   # Get upstream/downstream
manifest.to_view_registry()        # Convert to VirtualViewRegistry
```
