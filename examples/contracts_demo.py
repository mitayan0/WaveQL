#!/usr/bin/env python
"""
WaveQL Data Contracts Demo

This example demonstrates the v0.1.6 Data Contracts feature:
1. Defining contracts with Pydantic models
2. Validating Arrow tables against contracts
3. Using the ContractRegistry for centralized management
4. Detecting schema drift
5. Generating JSON Schema documentation

Data contracts provide type-safe validation for your data pipelines,
catching schema mismatches before they cause downstream failures.
"""

import pyarrow as pa
import json

from waveql import (
    DataContract,
    ColumnContract,
    ContractValidator,
    ContractRegistry,
)


def demo_basic_contract():
    """Demonstrate defining and using a basic contract."""
    print("=" * 60)
    print("Demo 1: Basic Data Contract")
    print("=" * 60)
    print()
    
    # Define a contract for ServiceNow incidents
    contract = DataContract(
        table="incident",
        adapter="servicenow",
        version="1.0.0",
        description="ServiceNow incident records",
        columns=[
            ColumnContract(
                name="sys_id",
                type="string",
                nullable=False,
                primary_key=True,
                description="Unique identifier"
            ),
            ColumnContract(
                name="number",
                type="string",
                nullable=False,
                description="Incident number (INC0001234)"
            ),
            ColumnContract(
                name="short_description",
                type="string",
                description="Brief summary of the incident"
            ),
            ColumnContract(
                name="priority",
                type="integer",
                description="1=Critical, 2=High, 3=Moderate, 4=Low, 5=Planning"
            ),
            ColumnContract(
                name="state",
                type="integer",
                description="Incident state code"
            ),
            ColumnContract(
                name="active",
                type="boolean",
                description="Whether incident is active"
            ),
        ]
    )
    
    print(f"Contract: {contract.table}")
    print(f"Adapter: {contract.adapter}")
    print(f"Version: {contract.version}")
    print(f"Columns: {len(contract.columns)}")
    print()
    
    # Show required columns
    required = contract.get_required_columns()
    print(f"Required columns: {required}")
    
    # Show primary keys
    pks = contract.get_primary_keys()
    print(f"Primary keys: {pks}")
    print()
    
    return contract


def demo_validation():
    """Demonstrate validating data against a contract."""
    print("=" * 60)
    print("Demo 2: Data Validation")
    print("=" * 60)
    print()
    
    # Define contract
    contract = DataContract(
        table="users",
        columns=[
            ColumnContract(name="id", type="integer", nullable=False),
            ColumnContract(name="name", type="string", nullable=False),
            ColumnContract(name="email", type="string"),
            ColumnContract(name="active", type="boolean"),
        ]
    )
    
    # Valid data
    valid_data = pa.table({
        "id": pa.array([1, 2, 3]),
        "name": pa.array(["Alice", "Bob", "Charlie"]),
        "email": pa.array(["a@test.com", "b@test.com", None]),
        "active": pa.array([True, False, True]),
    })
    
    print("Testing valid data...")
    validator = ContractValidator(contract)
    result = validator.validate(valid_data)
    
    print(f"  Valid: {result.valid}")
    print(f"  Violations: {len(result.violations)}")
    print()
    
    # Invalid data - type mismatch
    invalid_data = pa.table({
        "id": pa.array(["a", "b", "c"]),  # Should be integer!
        "name": pa.array(["Alice", "Bob", "Charlie"]),
        "email": pa.array(["a@test.com", "b@test.com", "c@test.com"]),
        "active": pa.array([True, False, True]),
    })
    
    print("Testing invalid data (type mismatch)...")
    result2 = validator.validate(invalid_data)
    
    print(f"  Valid: {result2.valid}")
    print(f"  Violations: {len(result2.violations)}")
    for v in result2.violations:
        print(f"    - {v}")
    print()


def demo_registry():
    """Demonstrate the ContractRegistry for managing multiple contracts."""
    print("=" * 60)
    print("Demo 3: Contract Registry")
    print("=" * 60)
    print()
    
    registry = ContractRegistry()
    
    # Register multiple contracts
    incident_contract = DataContract(
        table="incident",
        adapter="servicenow",
        columns=[
            ColumnContract(name="sys_id", type="string"),
            ColumnContract(name="number", type="string"),
        ]
    )
    
    account_contract = DataContract(
        table="Account",
        adapter="salesforce",
        columns=[
            ColumnContract(name="Id", type="string"),
            ColumnContract(name="Name", type="string"),
        ]
    )
    
    registry.register(incident_contract)
    registry.register(account_contract)
    
    print(f"Registered {len(registry)} contracts")
    print()
    
    # Look up contracts
    found = registry.get("incident", "servicenow")
    print(f"Found incident contract: {found is not None}")
    
    # Get all for adapter
    sn_contracts = registry.get_all(adapter="servicenow")
    print(f"ServiceNow contracts: {len(sn_contracts)}")
    print()
    
    # Validate through registry
    data = pa.table({
        "sys_id": ["abc123"],
        "number": ["INC0001"],
    })
    
    result = registry.validate(data, "incident", "servicenow")
    print(f"Validation through registry: valid={result.valid}")
    print()


def demo_schema_drift():
    """Demonstrate schema drift detection."""
    print("=" * 60)
    print("Demo 4: Schema Drift Detection")
    print("=" * 60)
    print()
    
    registry = ContractRegistry()
    
    # Original contract
    contract = DataContract(
        table="orders",
        adapter="rest",
        columns=[
            ColumnContract(name="id", type="integer"),
            ColumnContract(name="customer", type="string"),
            ColumnContract(name="total", type="float"),
        ]
    )
    registry.register(contract)
    
    print("Original contract columns: id, customer, total")
    print()
    
    # Simulated "live" schema from API (with changes)
    live_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("customer", pa.string()),
        # 'total' is removed
        pa.field("subtotal", pa.float64()),  # New column
        pa.field("tax", pa.float64()),       # New column
    ])
    
    print("Live API schema: id, customer, subtotal, tax")
    print()
    
    # Detect drift
    drift = registry.detect_drift(live_schema, "orders", "rest")
    
    print(f"Has drift: {drift['has_drift']}")
    print(f"Added columns: {drift['added_columns']}")
    print(f"Removed columns: {drift['removed_columns']}")
    print(f"Type changes: {drift['type_changes']}")
    print()


def demo_json_schema():
    """Demonstrate JSON Schema export."""
    print("=" * 60)
    print("Demo 5: JSON Schema Export")
    print("=" * 60)
    print()
    
    contract = DataContract(
        table="product",
        description="E-commerce product catalog",
        columns=[
            ColumnContract(
                name="sku",
                type="string",
                nullable=False,
                description="Stock Keeping Unit"
            ),
            ColumnContract(
                name="name",
                type="string",
                nullable=False,
                description="Product name"
            ),
            ColumnContract(
                name="price",
                type="float",
                description="Price in USD"
            ),
            ColumnContract(
                name="in_stock",
                type="boolean",
                description="Availability status"
            ),
        ]
    )
    
    json_schema = contract.to_json_schema()
    print(json.dumps(json_schema, indent=2))
    print()


def demo_bootstrap_from_arrow():
    """Demonstrate creating a contract from existing Arrow schema."""
    print("=" * 60)
    print("Demo 6: Bootstrap Contract from Arrow Schema")
    print("=" * 60)
    print()
    
    # Imagine this schema came from a live API fetch
    live_schema = pa.schema([
        pa.field("order_id", pa.int64()),
        pa.field("customer_name", pa.string()),
        pa.field("order_date", pa.timestamp("us")),
        pa.field("total_amount", pa.float64()),
        pa.field("shipped", pa.bool_()),
    ])
    
    print("Creating contract from live schema...")
    contract = DataContract.from_arrow_schema(
        schema=live_schema,
        table="orders",
        adapter="rest"
    )
    
    print(f"Generated contract for: {contract.table}")
    print("Columns:")
    for col in contract.columns:
        print(f"  - {col.name}: {col.type}")
    print()


def demo_nested_structures():
    """Demonstrate contracts with nested structures."""
    print("=" * 60)
    print("Demo 7: Nested Structures (Struct & List)")
    print("=" * 60)
    print()
    
    contract = DataContract(
        table="complex_data",
        columns=[
            ColumnContract(name="id", type="integer"),
            
            # Nested struct
            ColumnContract(
                name="metadata",
                type="struct",
                nested_columns=[
                    ColumnContract(name="source", type="string"),
                    ColumnContract(name="version", type="integer"),
                ]
            ),
            
            # List of strings
            ColumnContract(
                name="tags",
                type="list",
                nested_type="string"
            ),
        ]
    )
    
    print("Contract with nested types:")
    for col in contract.columns:
        arrow_type = col.to_arrow_type()
        print(f"  - {col.name}: {col.type} -> Arrow: {arrow_type}")
    print()


def main():
    print("\n" + "=" * 60)
    print("WaveQL v0.1.6 - Data Contracts Demo")
    print("=" * 60 + "\n")
    
    demo_basic_contract()
    demo_validation()
    demo_registry()
    demo_schema_drift()
    demo_json_schema()
    demo_bootstrap_from_arrow()
    demo_nested_structures()
    
    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
