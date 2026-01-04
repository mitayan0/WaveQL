# Schema Inference & Nested JSON Support

WaveQL provides automatic schema inference with native support for nested JSON structures. This enables you to query deeply nested API responses using SQL dot-notation—no ETL or manual flattening required.

## Overview

When you query an API that returns nested JSON like:

```json
{
  "id": 1,
  "user": {
    "name": "Alice",
    "email": "alice@example.com",
    "profile": {
      "active": true,
      "score": 95
    }
  }
}
```

WaveQL automatically infers the schema and creates **native struct columns** in the underlying DuckDB engine. This means you can query nested fields directly:

```sql
SELECT 
    id,
    user.name AS username,
    user.profile.score AS score
FROM api.users
WHERE user.profile.active = true
```

## How It Works

### 1. Multi-Sample Schema Inference

Unlike simple single-row inference, WaveQL samples **5 records** (configurable) spread across the dataset to ensure robust type detection:

```python
from waveql.utils.schema import infer_schema_from_records

# Samples records at indices: 0, 25, 50, 75, 99 (for 100 records)
schema = infer_schema_from_records(records, sample_size=5)
```

This prevents issues where:
- The first record has a null value for a field
- Different records have different types for the same field
- Some records are missing fields that others have

### 2. Type Inference Rules

| Python Type | Arrow Type | Notes |
|-------------|-----------|-------|
| `None` | `null` → promoted to actual type | Null is promoted when merged with any other type |
| `bool` | `bool_` | |
| `int` | `int64` | |
| `float` | `float64` | |
| `str` | `string` | Default fallback type |
| `dict` | `struct<...>` | **Native struct for dot-notation queries** |
| `list` | `list<...>` | Element type is inferred recursively |

### 3. Type Conflict Resolution

When different records have different types for the same field, WaveQL uses intelligent type merging:

```python
from waveql.utils.schema import merge_arrow_types
import pyarrow as pa

# int + float → float (widening)
merge_arrow_types(pa.int64(), pa.float64())  # → float64

# null + any → any (promotion)
merge_arrow_types(pa.null(), pa.string())  # → string

# struct + struct → merged struct (all fields)
# incompatible types → string (fallback)
```

## Usage Examples

### Basic Query with Nested Fields

```python
import waveql

conn = waveql.connect()
conn.register_adapter("jira", JiraAdapter(host="company.atlassian.net", ...))

# Query nested fields with dot notation
cursor = conn.execute("""
    SELECT 
        key,
        fields.summary,
        fields.reporter.displayName AS reporter,
        fields.priority.name AS priority
    FROM jira.issues
    WHERE fields.status.name = 'Open'
    ORDER BY fields.created DESC
    LIMIT 10
""")

for row in cursor:
    print(row)
```

### Accessing Deeply Nested Data

```python
# ServiceNow with display_value=all returns nested objects
cursor = conn.execute("""
    SELECT 
        number,
        assigned_to.display_value AS assignee,
        caller_id.display_value AS caller,
        assignment_group.display_value AS team
    FROM servicenow.incident
    WHERE priority <= 2
""")
```

### Working with Arrays

```python
# Jira labels are arrays
cursor = conn.execute("""
    SELECT 
        key,
        fields.labels,  -- Returns as list<string>
        array_length(fields.labels) AS label_count
    FROM jira.issues
    WHERE array_contains(fields.labels, 'bug')
""")
```

## Schema Evolution

WaveQL includes built-in schema evolution detection. When an API adds new fields:

```python
from waveql.utils.schema import detect_schema_changes, evolve_schema

# Detect what changed
changes = detect_schema_changes(cached_schema, new_schema)
for change in changes:
    print(change)
    # SchemaChange(ADDED: new_field -> string)
    # SchemaChange(TYPE_CHANGED: score from int64 to float64)

# Merge schemas (forward-compatible)
evolved = evolve_schema(cached_schema, new_schema)
# Contains all fields from both schemas
```

### Schema Evolution Strategies

| Strategy | Behavior |
|----------|----------|
| **Forward-Compatible** (default) | New fields added, removed fields kept, types widened |
| **Strict** | Raise `SchemaEvolutionError` on any change |
| **Replace** | Always use the new schema, discard cached |

## Configuration

### Adjusting Sample Size

For very large or highly variable datasets:

```python
from waveql.utils.schema import infer_schema_from_records

# Sample more records for heterogeneous data
schema = infer_schema_from_records(records, sample_size=10)

# Or fewer for consistent API responses
schema = infer_schema_from_records(records, sample_size=3)
```

### Disabling Struct Inference

If you prefer flat string columns (legacy behavior):

```python
# Currently, use the old _flatten_issue pattern or 
# post-process with json_extract in SQL:
cursor = conn.execute("""
    SELECT json_extract(raw_user, '$.name') AS name
    FROM api.users
""")
```

## Supported Adapters

All WaveQL adapters now support native struct columns:

| Adapter | Struct Support | Notes |
|---------|---------------|-------|
| **ServiceNow** | ✅ Full | Use `display_value=all` for nested objects |
| **Jira** | ✅ Full | `fields.*` are preserved as structs |
| **Salesforce** | ✅ Full | SOQL relationship queries return structs |
| **REST** | ✅ Full | Any JSON API with nested objects |

## Performance Considerations

1. **Struct columns are efficient** - DuckDB handles them natively without serialization overhead
2. **Sampling is fast** - Only 5 records are inspected, not the full dataset
3. **Schema caching** - Schemas are cached to avoid repeated inference
4. **Type inference is lazy** - Only performed on first query to a table

## Troubleshooting

### "Cannot access field 'x' on type string"

The field wasn't inferred as a struct. This happens when:
- All sampled records had null/empty values for that field
- The API returned the value as a JSON string instead of an object

**Solution**: Clear the schema cache and retry, or check your API settings.

### Inconsistent column types

Different queries return different types for the same field.

**Solution**: Increase the sample size or clear the cache after API schema changes.

### Missing nested fields

Some nested fields are null in the result.

**Solution**: The API may not include empty fields. Use `COALESCE` or check for nulls.

```sql
SELECT COALESCE(user.email, 'N/A') AS email FROM api.users
```
