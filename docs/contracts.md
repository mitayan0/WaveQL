# Data Contracts

> **Define, validate, and document your data schemas with type-safe contracts.**

Data Contracts in WaveQL provide a way to define explicit expectations for your data structures. They allow you to validate API responses at runtime, detect schema drift, and generate documentation automatically.

## Why Use Contracts?

When working with external APIs like ServiceNow, Salesforce, or Jira, you're at the mercy of their schema changes. Contracts give you:

1. **Early Error Detection**: Catch type mismatches and missing columns before they cause downstream failures
2. **Type Safety**: Ensure your data pipelines receive correctly typed data
3. **Documentation as Code**: Auto-generate JSON Schema documentation from your contracts
4. **Schema Drift Detection**: Alert when live API schemas diverge from your expectations

## Quick Start

### Define a Contract

```python
from waveql import DataContract, ColumnContract

# Define what you expect from the 'incident' table
contract = DataContract(
    table="incident",
    adapter="servicenow",
    description="ServiceNow incident records",
    columns=[
        ColumnContract(name="sys_id", type="string", nullable=False, primary_key=True),
        ColumnContract(name="number", type="string", nullable=False),
        ColumnContract(name="short_description", type="string"),
        ColumnContract(name="priority", type="integer"),
        ColumnContract(name="state", type="integer"),
        ColumnContract(name="assigned_to", type="string", description="User sys_id"),
        ColumnContract(name="sys_created_on", type="timestamp"),
    ]
)
```

### Validate Data

```python
from waveql import ContractValidator
import pyarrow as pa

# Fetch data from your source
data = pa.table({
    "sys_id": ["abc123", "def456"],
    "number": ["INC0001", "INC0002"],
    "short_description": ["Issue 1", "Issue 2"],
    "priority": [1, 2],
    "state": [1, 2],
    "assigned_to": ["user1", "user2"],
    "sys_created_on": ["2024-01-01", "2024-01-02"],
})

# Validate against contract
validator = ContractValidator(contract)
result = validator.validate(data)

if result.valid:
    print("✓ Data matches contract!")
else:
    for violation in result.violations:
        print(f"✗ {violation}")
```

### Contract Registry

Manage multiple contracts in a centralized registry:

```python
from waveql import ContractRegistry

# Create or get the global registry
registry = ContractRegistry()

# Register contracts
registry.register(incident_contract)
registry.register(user_contract)

# Load contracts from files
registry.load_from_directory("./contracts/")

# Validate data through registry
result = registry.validate(data, "incident", "servicenow")

# Detect schema drift
drift = registry.detect_drift(live_schema, "incident", "servicenow")
if drift["has_drift"]:
    print(f"New columns: {drift['added_columns']}")
    print(f"Removed columns: {drift['removed_columns']}")
```

## Column Types

| Type | Arrow Type | Description |
|------|------------|-------------|
| `string` | `pa.string()` | Text data |
| `integer` | `pa.int64()` | Whole numbers |
| `float` | `pa.float64()` | Decimal numbers |
| `boolean` | `pa.bool_()` | True/False |
| `datetime` | `pa.timestamp('us')` | Date and time |
| `date` | `pa.date32()` | Date only |
| `timestamp` | `pa.timestamp('us')` | Same as datetime |
| `binary` | `pa.binary()` | Binary data |
| `json` | `pa.string()` | JSON (stored as string) |
| `struct` | `pa.struct()` | Nested object |
| `list` | `pa.list_()` | Array of values |
| `any` | - | No type enforcement |

## Nested Structures

Define contracts for nested JSON objects:

```python
# Struct column
metadata_contract = ColumnContract(
    name="metadata",
    type="struct",
    nested_columns=[
        ColumnContract(name="source", type="string"),
        ColumnContract(name="priority_score", type="float"),
    ]
)

# List column
tags_contract = ColumnContract(
    name="tags",
    type="list",
    nested_type="string"
)
```

## Validation Options

### Strict Column Mode

By default, extra columns in your data are allowed. Enable strict mode to reject unexpected columns:

```python
contract = DataContract(
    table="users",
    strict_columns=True,  # Fail if data has columns not in contract
    columns=[...]
)
```

### Strict Type Mode

Type checking is enabled by default. Disable to allow type coercion:

```python
contract = DataContract(
    table="users",
    strict_types=False,  # Allow type mismatches
    columns=[...]
)
```

## JSON Schema Export

Export your contracts as JSON Schema for documentation or external tools:

```python
json_schema = contract.to_json_schema()
print(json.dumps(json_schema, indent=2))
```

Output:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "incident",
  "description": "ServiceNow incident records",
  "type": "object",
  "properties": {
    "sys_id": {
      "type": "string"
    },
    "number": {
      "type": "string"
    },
    "priority": {
      "type": "integer"
    }
  },
  "required": ["sys_id", "number"]
}
```

## File-Based Contracts

Store contracts as JSON or YAML files:

**`contracts/incident.json`**
```json
{
  "table": "incident",
  "adapter": "servicenow",
  "columns": [
    {"name": "sys_id", "type": "string", "nullable": false},
    {"name": "number", "type": "string", "nullable": false},
    {"name": "priority", "type": "integer"}
  ]
}
```

**`contracts/incident.yaml`**
```yaml
table: incident
adapter: servicenow
columns:
  - name: sys_id
    type: string
    nullable: false
  - name: number
    type: string
    nullable: false
  - name: priority
    type: integer
```

Load and use:
```python
registry = ContractRegistry()
registry.load_from_directory("./contracts/")
```

## Schema Drift Detection

Detect when an API's schema changes from your contract:

```python
# Get live schema from an API fetch
live_schema = table.schema

# Compare to contract
drift = registry.detect_drift(live_schema, "incident", "servicenow")

if drift["has_drift"]:
    print("⚠️ Schema drift detected!")
    
    for col in drift["added_columns"]:
        print(f"  + New column: {col}")
    
    for col in drift["removed_columns"]:
        print(f"  - Removed column: {col}")
    
    for col, (old, new) in drift["type_changes"].items():
        print(f"  ~ Type changed: {col} ({old} → {new})")
```

## Bootstrapping Contracts

Generate a contract from existing data:

```python
# From Arrow schema
contract = DataContract.from_arrow_schema(
    schema=existing_table.schema,
    table="discovered_table",
    adapter="rest"
)

# Save for future use
registry.save_to_file(contract, "contracts/discovered_table.json")
```

## Adaptive Schema Support

Data contracts in WaveQL are **adaptive**. They inherit from `AdaptiveModel`, which means they are resilient to upstream API changes.

*   **Extra Fields**: If an API adds a new field that is *not* in your contract, validation will **pass** (the extra field is ignored during validation but present in the data).
*   **Missing Fields**: If a *required* field is missing, validation fails.
*   **Type Mismatches**: If a field type changes, validation fails.

This approach ensures "Schema Drift Handling" — your integration doesn't crash just because HubSpot added a `custom_field_xyz` overnight, but it *will* alert you if critical data structures break.

## Integration with dbt

WaveQL bridges the gap between operational APIs and your analytical warehouse. You can export your Data Contracts directly to **dbt** source definitions.

```python
# Export all registered contracts to a dbt sources.yml
registry.export_to_dbt("./models/sources.yml")
```

**Generated `sources.yml`:**
```yaml
version: 2
sources:
  - name: servicenow
    tables:
      - name: incident
        description: ServiceNow incident records
        columns:
          - name: sys_id
            tests:
              - unique
              - not_null
          - name: number
            tests:
              - not_null
```

This ensures your dbt tests match your operational data contracts, creating a unified semantic layer.

## Best Practices

1. **Version your contracts**: Use the `version` field to track breaking changes
2. **Start lenient, tighten gradually**: Begin with `strict_columns=False`, then enable once stable
3. **Store contracts in source control**: Treat contracts as code
4. **Run validation in CI/CD**: Catch schema drift before production
5. **Use aliases for evolution**: Handle column renames without breaking pipelines

```python
ColumnContract(
    name="user_id",
    type="string",
    aliases=["sys_id", "id"]  # Accept old names
)
```

## API Reference

### `DataContract`
- `table: str` - Table name (required)
- `columns: List[ColumnContract]` - Column definitions (required)
- `adapter: Optional[str]` - Adapter name
- `version: str` - Contract version (default: "1.0.0")
- `description: str` - Table description
- `strict_columns: bool` - Reject extra columns (default: False)
- `strict_types: bool` - Enforce type matching (default: True)

### `ColumnContract`
- `name: str` - Column name (required)
- `type: ColumnType` - Data type (default: "string")
- `nullable: bool` - Allow NULLs (default: True)
- `primary_key: bool` - Primary key column (default: False)
- `description: str` - Column description
- `constraints: List[str]` - Validation constraints
- `aliases: List[str]` - Alternative column names

### `ContractValidator`
- `validate(table: pa.Table) -> ContractValidationResult`
- `validate_schema(schema: pa.Schema) -> ContractValidationResult`

### `ContractRegistry`
- `register(contract: DataContract)`
- `get(table: str, adapter: Optional[str]) -> Optional[DataContract]`
- `validate(table: pa.Table, table_name: str, adapter: str) -> ContractValidationResult`
- `detect_drift(schema: pa.Schema, table: str, adapter: str) -> Dict`
- `load_from_file(path: str) -> DataContract`
- `load_from_directory(directory: str) -> List[DataContract]`
