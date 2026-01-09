# Data Contracts

Type-safe schema validation and drift detection.

## Definition

```python
from waveql import DataContract, ColumnContract

contract = DataContract(
    table="incident",
    columns=[
        # Primitives
        ColumnContract("sys_id", "string", pk=True),
        ColumnContract("priority", "integer"),
        
        # Nested
        ColumnContract("metadata", "struct", nested_columns=[
            ColumnContract("source", "string")
        ]),
        ColumnContract("tags", "list", nested_type="string")
    ],
    strict_columns=False  # Allow extra fields (default)
)
```

## Validation

```python
from waveql import ContractValidator

result = ContractValidator(contract).validate(arrow_table)
if not result.valid:
    print(result.violations)
```

## Types

| Type | Arrow | Notes |
| :--- | :--- | :--- |
| `string` | `pa.string()` | |
| `integer` | `pa.int64()` | |
| `float` | `pa.float64()` | |
| `boolean` | `pa.bool_()` | |
| `timestamp` | `pa.timestamp('us')` | |
| `date` | `pa.date32()` | |
| `struct` | `pa.struct()` | Use `nested_columns` |
| `list` | `pa.list_()` | Use `nested_type` |

## dbt Export
```python
# Convert all registered contracts to sources.yml
registry.register(contract)
registry.export_to_dbt("./models/sources.yml")
```
