# Semantic Layer

Organize complex logic into reusable Views and Saved Queries.

## Virtual Views
Reusable SQL abstractions over API data.

```python
from waveql.semantic import VirtualView

# Define
registry.register(VirtualView(
    name="critical_incidents",
    sql="SELECT * FROM incident WHERE priority <= 2"
))
conn.register_views(registry)

# Query
cursor.execute("SELECT COUNT(*) FROM critical_incidents")
```

## Saved Queries
Parameterized SQL templates.

```python
from waveql.semantic import SavedQuery

# Define
query = SavedQuery(
    name="incidents_by_priority",
    sql="SELECT * FROM incident WHERE priority = :p",
    parameters={"p": {"type": "int", "default": 2}}
)

# Execute
cursor.execute_saved(query, p=1)
```

## dbt Integration
Expose dbt models as queryable tables.

```python
# Load from dbt project
conn.load_dbt_project("/path/to/dbt_project")

# Query models
cursor.execute("SELECT * FROM dim_users JOIN fct_orders USING (user_id)")
```
*   Requires `dbt compile` to generate `manifest.json`.
*   Uses `compiled_sql` from dbt to create DuckDB views.
