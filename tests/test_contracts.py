"""
Tests for WaveQL Data Contracts - Pydantic-based schema validation.
"""

import pytest
import pyarrow as pa
import json
from pathlib import Path
import tempfile

from waveql.contracts import (
    DataContract,
    ColumnContract,
    ContractValidator,
    ContractRegistry,
    ViolationType,
)
from waveql.exceptions import ContractViolationError
from pydantic import ValidationError


class TestColumnContract:
    """Tests for ColumnContract model."""
    
    def test_basic_column(self):
        col = ColumnContract(name="id", type="integer", nullable=False)
        
        assert col.name == "id"
        assert col.type == "integer"
        assert col.nullable is False
    
    def test_default_values(self):
        col = ColumnContract(name="name")
        
        assert col.type == "string"
        assert col.nullable is True
        assert col.primary_key is False
        assert col.description == ""
        assert col.constraints == []
    
    def test_to_arrow_type_primitives(self):
        assert ColumnContract(name="a", type="string").to_arrow_type() == pa.string()
        assert ColumnContract(name="b", type="integer").to_arrow_type() == pa.int64()
        assert ColumnContract(name="c", type="float").to_arrow_type() == pa.float64()
        assert ColumnContract(name="d", type="boolean").to_arrow_type() == pa.bool_()
    
    def test_to_arrow_type_list(self):
        col = ColumnContract(name="tags", type="list", nested_type="string")
        arrow_type = col.to_arrow_type()
        
        assert pa.types.is_list(arrow_type)
        assert arrow_type.value_type == pa.string()
    
    def test_to_arrow_type_struct(self):
        col = ColumnContract(
            name="metadata",
            type="struct",
            nested_columns=[
                ColumnContract(name="key", type="string"),
                ColumnContract(name="value", type="integer"),
            ]
        )
        arrow_type = col.to_arrow_type()
        
        assert pa.types.is_struct(arrow_type)
        assert len(list(arrow_type)) == 2
    
    def test_matches_arrow_type_exact(self):
        col = ColumnContract(name="id", type="integer")
        assert col.matches_arrow_type(pa.int64()) is True
        assert col.matches_arrow_type(pa.string()) is False
    
    def test_matches_arrow_type_promotion(self):
        # Float should accept integers
        col = ColumnContract(name="value", type="float")
        assert col.matches_arrow_type(pa.int64()) is True
        assert col.matches_arrow_type(pa.float64()) is True
    
    def test_matches_arrow_type_any(self):
        col = ColumnContract(name="data", type="any")
        assert col.matches_arrow_type(pa.string()) is True
        assert col.matches_arrow_type(pa.int64()) is True
        assert col.matches_arrow_type(pa.struct([pa.field("a", pa.int64())])) is True
    
    def test_column_with_aliases(self):
        col = ColumnContract(
            name="sys_id",
            type="string",
            aliases=["id", "system_id"]
        )
        assert "id" in col.aliases
        assert "system_id" in col.aliases


class TestDataContract:
    """Tests for DataContract model."""
    
    def test_basic_contract(self):
        contract = DataContract(
            table="incident",
            adapter="servicenow",
            columns=[
                ColumnContract(name="sys_id", type="string", nullable=False),
                ColumnContract(name="number", type="string"),
            ]
        )
        
        assert contract.table == "incident"
        assert contract.adapter == "servicenow"
        assert len(contract.columns) == 2
        assert contract.version == "1.0.0"
    
    def test_contract_requires_columns(self):
        # Pydantic v2 raises ValidationError, not ValueError
        with pytest.raises((ValueError, ValidationError)):
            DataContract(table="test", columns=[])
    
    def test_contract_no_duplicate_columns(self):
        with pytest.raises(ValueError, match="Duplicate column"):
            DataContract(
                table="test",
                columns=[
                    ColumnContract(name="id"),
                    ColumnContract(name="id"),  # Duplicate
                ]
            )
    
    def test_get_column(self):
        contract = DataContract(
            table="test",
            columns=[
                ColumnContract(name="id", type="integer"),
                ColumnContract(name="name", type="string"),
            ]
        )
        
        assert contract.get_column("id").type == "integer"
        assert contract.get_column("name").type == "string"
        assert contract.get_column("unknown") is None
    
    def test_get_column_by_alias(self):
        contract = DataContract(
            table="test",
            columns=[
                ColumnContract(name="sys_id", type="string", aliases=["id"]),
            ]
        )
        
        assert contract.get_column("sys_id") is not None
        assert contract.get_column("id") is not None
    
    def test_get_primary_keys(self):
        contract = DataContract(
            table="test",
            columns=[
                ColumnContract(name="id", primary_key=True),
                ColumnContract(name="name"),
            ]
        )
        
        pks = contract.get_primary_keys()
        assert pks == ["id"]
    
    def test_get_required_columns(self):
        contract = DataContract(
            table="test",
            columns=[
                ColumnContract(name="id", nullable=False),
                ColumnContract(name="name", nullable=True),
            ]
        )
        
        required = contract.get_required_columns()
        assert required == ["id"]
    
    def test_to_arrow_schema(self):
        contract = DataContract(
            table="test",
            columns=[
                ColumnContract(name="id", type="integer", nullable=False),
                ColumnContract(name="name", type="string"),
            ]
        )
        
        schema = contract.to_arrow_schema()
        
        assert len(schema) == 2
        assert schema.field("id").type == pa.int64()
        assert schema.field("id").nullable is False
        assert schema.field("name").type == pa.string()
    
    def test_to_json_schema(self):
        contract = DataContract(
            table="incident",
            description="ServiceNow incidents",
            columns=[
                ColumnContract(name="number", type="string", nullable=False, description="Ticket number"),
                ColumnContract(name="priority", type="integer"),
            ]
        )
        
        json_schema = contract.to_json_schema()
        
        assert json_schema["title"] == "incident"
        assert json_schema["description"] == "ServiceNow incidents"
        assert "number" in json_schema["properties"]
        assert json_schema["properties"]["number"]["type"] == "string"
        assert "number" in json_schema["required"]
    
    def test_serialization_json(self):
        contract = DataContract(
            table="test",
            adapter="servicenow",
            columns=[ColumnContract(name="id", type="integer")]
        )
        
        json_str = contract.to_json()
        loaded = DataContract.from_json(json_str)
        
        assert loaded.table == "test"
        assert loaded.adapter == "servicenow"
        assert loaded.columns[0].name == "id"
    
    def test_from_arrow_schema(self):
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("active", pa.bool_()),
        ])
        
        contract = DataContract.from_arrow_schema(schema, "users", "rest")
        
        assert contract.table == "users"
        assert contract.adapter == "rest"
        assert len(contract.columns) == 3
        assert contract.get_column("id").type == "integer"
        assert contract.get_column("name").type == "string"
        assert contract.get_column("active").type == "boolean"
    
    def test_hash(self):
        contract1 = DataContract(
            table="test",
            columns=[ColumnContract(name="id", type="integer")]
        )
        contract2 = DataContract(
            table="test",
            columns=[ColumnContract(name="id", type="integer")]
        )
        contract3 = DataContract(
            table="test",
            columns=[ColumnContract(name="id", type="string")]  # Different type
        )
        
        assert contract1.hash() == contract2.hash()
        assert contract1.hash() != contract3.hash()


class TestContractValidator:
    """Tests for ContractValidator."""
    
    @pytest.fixture
    def sample_contract(self):
        return DataContract(
            table="users",
            columns=[
                ColumnContract(name="id", type="integer", nullable=False),
                ColumnContract(name="name", type="string", nullable=False),
                ColumnContract(name="email", type="string"),
                ColumnContract(name="active", type="boolean"),
            ]
        )
    
    @pytest.fixture
    def valid_table(self):
        return pa.table({
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["Alice", "Bob", "Charlie"]),
            "email": pa.array(["a@test.com", "b@test.com", None]),
            "active": pa.array([True, False, True]),
        })
    
    def test_validate_success(self, sample_contract, valid_table):
        validator = ContractValidator(sample_contract)
        result = validator.validate(valid_table)
        
        assert result.valid is True
        assert len(result.violations) == 0
    
    def test_validate_missing_required_column(self, sample_contract):
        # Missing 'name' column
        table = pa.table({
            "id": pa.array([1, 2]),
            "email": pa.array(["a@test.com", "b@test.com"]),
        })
        
        validator = ContractValidator(sample_contract)
        result = validator.validate(table)
        
        assert result.valid is False
        assert any(
            v.type == ViolationType.MISSING_COLUMN and v.column == "name"
            for v in result.violations
        )
    
    def test_validate_type_mismatch(self, sample_contract):
        # 'id' should be integer, not string
        table = pa.table({
            "id": pa.array(["a", "b", "c"]),  # String instead of int
            "name": pa.array(["Alice", "Bob", "Charlie"]),
            "email": pa.array(["a@test.com", "b@test.com", "c@test.com"]),
            "active": pa.array([True, False, True]),
        })
        
        validator = ContractValidator(sample_contract)
        result = validator.validate(table)
        
        assert result.valid is False
        assert any(
            v.type == ViolationType.TYPE_MISMATCH and v.column == "id"
            for v in result.violations
        )
    
    def test_validate_extra_column_strict(self, sample_contract):
        sample_contract.strict_columns = True
        
        table = pa.table({
            "id": pa.array([1, 2]),
            "name": pa.array(["Alice", "Bob"]),
            "email": pa.array(["a@test.com", "b@test.com"]),
            "active": pa.array([True, False]),
            "unknown_column": pa.array(["x", "y"]),  # Extra column
        })
        
        validator = ContractValidator(sample_contract)
        result = validator.validate(table)
        
        assert result.valid is False
        assert any(
            v.type == ViolationType.EXTRA_COLUMN and v.column == "unknown_column"
            for v in result.violations
        )
    
    def test_validate_null_violation(self, sample_contract):
        # 'name' is non-nullable but has NULLs
        table = pa.table({
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["Alice", None, "Charlie"]),  # NULL in non-nullable
            "email": pa.array(["a@test.com", "b@test.com", "c@test.com"]),
            "active": pa.array([True, False, True]),
        })
        
        validator = ContractValidator(sample_contract)
        result = validator.validate(table)
        
        assert result.valid is False
        assert any(
            v.type == ViolationType.NULL_VIOLATION and v.column == "name"
            for v in result.violations
        )
    
    def test_raise_on_error(self, sample_contract):
        table = pa.table({
            "id": pa.array(["a", "b"]),  # Wrong type
            "name": pa.array(["Alice", "Bob"]),
        })
        
        validator = ContractValidator(sample_contract)
        result = validator.validate(table)
        
        with pytest.raises(ContractViolationError):
            result.raise_on_error()
    
    def test_result_to_dict(self, sample_contract, valid_table):
        validator = ContractValidator(sample_contract)
        result = validator.validate(valid_table)
        
        result_dict = result.to_dict()
        
        assert "valid" in result_dict
        assert "violations" in result_dict
        assert "warnings" in result_dict


class TestContractRegistry:
    """Tests for ContractRegistry."""
    
    @pytest.fixture
    def sample_contracts(self):
        return [
            DataContract(
                table="incident",
                adapter="servicenow",
                columns=[ColumnContract(name="sys_id", type="string")]
            ),
            DataContract(
                table="Account",
                adapter="salesforce",
                columns=[ColumnContract(name="Id", type="string")]
            ),
        ]
    
    def test_register_and_get(self, sample_contracts):
        registry = ContractRegistry()
        
        for contract in sample_contracts:
            registry.register(contract)
        
        assert len(registry) == 2
        
        incident = registry.get("incident", "servicenow")
        assert incident is not None
        assert incident.table == "incident"
        
        account = registry.get("Account", "salesforce")
        assert account is not None
    
    def test_get_not_found(self):
        registry = ContractRegistry()
        
        assert registry.get("nonexistent") is None
    
    def test_unregister(self, sample_contracts):
        registry = ContractRegistry()
        registry.register(sample_contracts[0])
        
        assert registry.has_contract("incident", "servicenow")
        
        registry.unregister("incident", "servicenow")
        
        assert not registry.has_contract("incident", "servicenow")
    
    def test_get_all(self, sample_contracts):
        registry = ContractRegistry()
        for c in sample_contracts:
            registry.register(c)
        
        all_contracts = registry.get_all()
        assert len(all_contracts) == 2
        
        sn_contracts = registry.get_all(adapter="servicenow")
        assert len(sn_contracts) == 1
    
    def test_load_from_json_file(self):
        contract_data = {
            "table": "test_table",
            "adapter": "rest",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"}
            ]
        }
        
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(contract_data, f)
            temp_path = f.name
        
        try:
            registry = ContractRegistry()
            contract = registry.load_from_file(temp_path)
            
            assert contract.table == "test_table"
            assert registry.has_contract("test_table", "rest")
        finally:
            Path(temp_path).unlink()
    
    def test_save_to_file(self, sample_contracts):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            temp_path = f.name
        
        try:
            registry = ContractRegistry()
            registry.save_to_file(sample_contracts[0], temp_path)
            
            # Verify file was created
            assert Path(temp_path).exists()
            
            # Load it back
            content = json.loads(Path(temp_path).read_text())
            assert content["table"] == "incident"
        finally:
            Path(temp_path).unlink()
    
    def test_validate_through_registry(self, sample_contracts):
        registry = ContractRegistry()
        registry.register(sample_contracts[0])
        
        table = pa.table({
            "sys_id": pa.array(["abc123", "def456"])
        })
        
        result = registry.validate(table, "incident", "servicenow")
        assert result.valid is True
    
    def test_validate_no_contract_raises(self):
        registry = ContractRegistry()
        
        table = pa.table({"a": [1, 2]})
        
        with pytest.raises(KeyError, match="No contract registered"):
            registry.validate(table, "nonexistent")
    
    def test_detect_drift(self, sample_contracts):
        registry = ContractRegistry()
        registry.register(sample_contracts[0])
        
        # Live schema has extra column
        live_schema = pa.schema([
            pa.field("sys_id", pa.string()),
            pa.field("new_column", pa.int64()),  # Added
        ])
        
        drift = registry.detect_drift(live_schema, "incident", "servicenow")
        
        assert drift["has_drift"] is True
        assert "new_column" in drift["added_columns"]
    
    def test_generate_contract_from_schema(self):
        registry = ContractRegistry()
        
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        
        contract = registry.generate_contract_from_schema(
            schema, "auto_table", "rest", register=True
        )
        
        assert contract.table == "auto_table"
        assert registry.has_contract("auto_table", "rest")
    
    def test_clear(self, sample_contracts):
        registry = ContractRegistry()
        for c in sample_contracts:
            registry.register(c)
        
        assert len(registry) == 2
        
        registry.clear()
        
        assert len(registry) == 0


class TestIntegration:
    """Integration tests for contracts with WaveQL."""
    
    def test_import_from_waveql(self):
        import waveql
        
        assert hasattr(waveql, "DataContract")
        assert hasattr(waveql, "ColumnContract")
        assert hasattr(waveql, "ContractValidator")
        assert hasattr(waveql, "ContractViolationError")
    
    def test_end_to_end_validation(self):
        # Define contract
        contract = DataContract(
            table="orders",
            adapter="rest",
            columns=[
                ColumnContract(name="order_id", type="integer", nullable=False, primary_key=True),
                ColumnContract(name="customer_name", type="string", nullable=False),
                ColumnContract(name="total", type="float"),
                ColumnContract(name="created_at", type="string"),  # Use string for simplicity
            ]
        )
        
        # Create valid data
        valid_data = pa.table({
            "order_id": pa.array([1, 2, 3]),
            "customer_name": pa.array(["Alice", "Bob", "Charlie"]),
            "total": pa.array([100.50, 200.75, 50.00]),
            "created_at": pa.array([
                "2024-01-01 10:00:00",
                "2024-01-02 11:00:00",
                "2024-01-03 12:00:00"
            ]),
        })
        
        validator = ContractValidator(contract)
        result = validator.validate(valid_data)
        
        assert result.valid is True
        
        # Now test with invalid data
        invalid_data = pa.table({
            "order_id": pa.array(["a", "b", "c"]),  # Should be int
            "customer_name": pa.array([None, "Bob", None]),  # Nulls not allowed
        })
        
        result2 = validator.validate(invalid_data)
        assert result2.valid is False
        assert len(result2.violations) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
