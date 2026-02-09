"""
Additional tests for WaveQL contracts/registry module.

This covers the 60% uncovered paths in waveql/contracts/registry.py
"""

import pytest
import json
import tempfile
from pathlib import Path

import pyarrow as pa

from waveql.contracts import DataContract, ColumnContract, ContractRegistry


class TestContractRegistryFileLoading:
    """Tests for file loading edge cases."""
    
    def test_load_from_file_not_found(self):
        """Test loading from non-existent file."""
        registry = ContractRegistry()
        
        with pytest.raises(FileNotFoundError):
            registry.load_from_file("/nonexistent/path/contract.json")
    
    def test_load_from_file_unsupported_format(self):
        """Test loading from unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as f:
            f.write("not a contract")
            temp_path = f.name
        
        try:
            registry = ContractRegistry()
            with pytest.raises(ValueError, match="Unsupported file format"):
                registry.load_from_file(temp_path)
        finally:
            Path(temp_path).unlink()
    
    def test_load_from_yaml_file(self):
        """Test loading from YAML file."""
        yaml = pytest.importorskip("yaml")
        
        contract_data = {
            "table": "yaml_table",
            "adapter": "rest",
            "columns": [
                {"name": "id", "type": "integer"}
            ]
        }
        
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            yaml.dump(contract_data, f)
            temp_path = f.name
        
        try:
            registry = ContractRegistry()
            contract = registry.load_from_file(temp_path)
            assert contract.table == "yaml_table"
        finally:
            Path(temp_path).unlink()
    
    def test_load_from_yml_extension(self):
        """Test loading from .yml extension."""
        yaml = pytest.importorskip("yaml")
        
        contract_data = {
            "table": "yml_table",
            "columns": [{"name": "id", "type": "string"}]
        }
        
        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False, mode="w") as f:
            yaml.dump(contract_data, f)
            temp_path = f.name
        
        try:
            registry = ContractRegistry()
            contract = registry.load_from_file(temp_path)
            assert contract.table == "yml_table"
        finally:
            Path(temp_path).unlink()


class TestContractRegistryDirectoryLoading:
    """Tests for directory loading."""
    
    def test_load_from_directory_not_found(self):
        """Test loading from non-existent directory."""
        registry = ContractRegistry()
        
        with pytest.raises(NotADirectoryError):
            registry.load_from_directory("/nonexistent/dir")
    
    def test_load_from_directory_with_invalid_files(self, tmp_path):
        """Test loading from directory with some invalid files."""
        # Create valid contract
        valid_contract = {
            "table": "valid_table",
            "columns": [{"name": "id", "type": "string"}]
        }
        (tmp_path / "valid.json").write_text(json.dumps(valid_contract))
        
        # Create invalid JSON
        (tmp_path / "invalid.json").write_text("not valid json")
        
        registry = ContractRegistry()
        contracts = registry.load_from_directory(tmp_path)
        
        # Should load valid contract and skip invalid
        assert len(contracts) == 1
        assert contracts[0].table == "valid_table"
    
    def test_load_from_directory_recursive(self, tmp_path):
        """Test recursive directory loading."""
        # Create subdirectory
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        
        # Create contracts in both locations
        contract1 = {"table": "root_table", "columns": [{"name": "id"}]}
        contract2 = {"table": "sub_table", "columns": [{"name": "id"}]}
        
        (tmp_path / "root.json").write_text(json.dumps(contract1))
        (subdir / "sub.json").write_text(json.dumps(contract2))
        
        registry = ContractRegistry()
        contracts = registry.load_from_directory(tmp_path, recursive=True)
        
        assert len(contracts) == 2
    
    def test_load_from_directory_non_recursive(self, tmp_path):
        """Test non-recursive directory loading."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        
        contract1 = {"table": "root_table", "columns": [{"name": "id"}]}
        contract2 = {"table": "sub_table", "columns": [{"name": "id"}]}
        
        (tmp_path / "root.json").write_text(json.dumps(contract1))
        (subdir / "sub.json").write_text(json.dumps(contract2))
        
        registry = ContractRegistry()
        contracts = registry.load_from_directory(tmp_path, recursive=False)
        
        # Should only load root level
        assert len(contracts) == 1
        assert contracts[0].table == "root_table"


class TestContractRegistrySaveToFile:
    """Tests for save_to_file method."""
    
    def test_save_to_file_json(self, tmp_path):
        """Test saving contract as JSON."""
        contract = DataContract(
            table="test",
            adapter="rest",
            columns=[ColumnContract(name="id", type="integer")]
        )
        
        registry = ContractRegistry()
        out_path = tmp_path / "contract.json"
        registry.save_to_file(contract, out_path)
        
        assert out_path.exists()
        loaded = json.loads(out_path.read_text())
        assert loaded["table"] == "test"
    
    def test_save_to_file_yaml(self, tmp_path):
        """Test saving contract as YAML."""
        yaml = pytest.importorskip("yaml")
        
        contract = DataContract(
            table="yaml_test",
            columns=[ColumnContract(name="id", type="string")]
        )
        
        registry = ContractRegistry()
        out_path = tmp_path / "contract.yaml"
        registry.save_to_file(contract, out_path, format="yaml")
        
        assert out_path.exists()
        loaded = yaml.safe_load(out_path.read_text())
        assert loaded["table"] == "yaml_test"
    
    def test_save_to_file_creates_dirs(self, tmp_path):
        """Test that save_to_file creates parent directories."""
        contract = DataContract(
            table="nested",
            columns=[ColumnContract(name="id")]
        )
        
        registry = ContractRegistry()
        out_path = tmp_path / "deep" / "nested" / "dir" / "contract.json"
        registry.save_to_file(contract, out_path)
        
        assert out_path.exists()


class TestContractRegistryDbtExport:
    """Tests for dbt export functionality."""
    
    def test_export_to_dbt(self, tmp_path):
        """Test exporting contracts to dbt sources.yml."""
        yaml = pytest.importorskip("yaml")
        
        registry = ContractRegistry()
        
        # Register some contracts
        registry.register(DataContract(
            table="users",
            adapter="postgres",
            columns=[
                ColumnContract(name="id", type="integer", primary_key=True, nullable=False),
                ColumnContract(name="email", type="string", nullable=False),
                ColumnContract(name="name", type="string"),
            ]
        ))
        registry.register(DataContract(
            table="orders",
            adapter="postgres",
            columns=[
                ColumnContract(name="id", type="integer", primary_key=True),
                ColumnContract(name="user_id", type="integer"),
            ]
        ))
        
        out_path = tmp_path / "sources.yml"
        registry.export_to_dbt(out_path)
        
        assert out_path.exists()
        
        dbt_content = yaml.safe_load(out_path.read_text())
        
        assert dbt_content["version"] == 2
        assert "sources" in dbt_content
        
        # Check that tables are present
        source = dbt_content["sources"][0]
        assert source["name"] == "postgres"
        assert len(source["tables"]) == 2
    
    def test_export_to_dbt_with_tests(self, tmp_path):
        """Test that dbt export includes proper tests."""
        yaml = pytest.importorskip("yaml")
        
        registry = ContractRegistry()
        registry.register(DataContract(
            table="test_table",
            adapter="test",
            columns=[
                ColumnContract(name="pk", type="integer", primary_key=True, nullable=False),
                ColumnContract(name="required", type="string", nullable=False),
                ColumnContract(name="optional", type="string", nullable=True),
            ]
        ))
        
        out_path = tmp_path / "sources.yml"
        registry.export_to_dbt(out_path)
        
        dbt_content = yaml.safe_load(out_path.read_text())
        
        table = dbt_content["sources"][0]["tables"][0]
        columns = {c["name"]: c for c in table["columns"]}
        
        # Primary key should have unique and not_null
        assert "unique" in columns["pk"]["tests"]
        assert "not_null" in columns["pk"]["tests"]
        
        # Required should have not_null
        assert "not_null" in columns["required"]["tests"]
        
        # Optional should have no tests (or tests key missing)
        assert "tests" not in columns["optional"] or len(columns["optional"]["tests"]) == 0


class TestContractRegistryValidation:
    """Tests for validation edge cases."""
    
    def test_validate_raise_on_error(self):
        """Test validate with raise_on_error=True."""
        from waveql.exceptions import ContractViolationError
        
        registry = ContractRegistry()
        registry.register(DataContract(
            table="strict_table",
            adapter="test",
            columns=[
                ColumnContract(name="id", type="integer", nullable=False)
            ]
        ))
        
        # Invalid data - wrong type
        table = pa.table({"id": ["not", "integers"]})
        
        with pytest.raises(ContractViolationError):
            registry.validate(table, "strict_table", "test", raise_on_error=True)


class TestContractRegistryDriftDetection:
    """Tests for schema drift detection."""
    
    def test_detect_drift_no_contract(self):
        """Test drift detection when no contract is registered."""
        registry = ContractRegistry()
        
        live_schema = pa.schema([pa.field("id", pa.int64())])
        
        with pytest.raises(KeyError):
            registry.detect_drift(live_schema, "nonexistent")
    
    def test_detect_drift_removed_columns(self):
        """Test detecting removed columns."""
        registry = ContractRegistry()
        registry.register(DataContract(
            table="test",
            columns=[
                ColumnContract(name="id", type="integer"),
                ColumnContract(name="old_column", type="string"),
            ]
        ))
        
        # Live schema is missing old_column
        live_schema = pa.schema([pa.field("id", pa.int64())])
        
        drift = registry.detect_drift(live_schema, "test")
        
        assert drift["has_drift"] is True
        assert "old_column" in drift["removed_columns"]
    
    def test_detect_drift_type_changes(self):
        """Test detecting type changes."""
        registry = ContractRegistry()
        registry.register(DataContract(
            table="test",
            columns=[
                ColumnContract(name="id", type="integer"),  # Contract says integer
            ]
        ))
        
        # Live schema has string instead
        live_schema = pa.schema([pa.field("id", pa.string())])
        
        drift = registry.detect_drift(live_schema, "test")
        
        assert drift["has_drift"] is True
        assert "id" in drift["type_changes"]


class TestContractRegistryUtilities:
    """Tests for utility methods."""
    
    def test_len(self):
        """Test __len__ method."""
        registry = ContractRegistry()
        assert len(registry) == 0
        
        registry.register(DataContract(
            table="t1",
            columns=[ColumnContract(name="id")]
        ))
        assert len(registry) == 1
    
    def test_contains(self):
        """Test __contains__ method."""
        registry = ContractRegistry()
        registry.register(DataContract(
            table="test",
            adapter="mydb",
            columns=[ColumnContract(name="id")]
        ))
        
        assert ("mydb", "test") in registry
        assert ("other", "test") not in registry
    
    def test_repr(self):
        """Test __repr__ method."""
        registry = ContractRegistry()
        registry.register(DataContract(
            table="test",
            columns=[ColumnContract(name="id")]
        ))
        
        repr_str = repr(registry)
        assert "ContractRegistry" in repr_str
        assert "contracts=1" in repr_str
    
    def test_clear(self):
        """Test clear method."""
        registry = ContractRegistry()
        registry.register(DataContract(
            table="t1",
            columns=[ColumnContract(name="id")]
        ))
        registry.register(DataContract(
            table="t2",
            columns=[ColumnContract(name="id")]
        ))
        
        assert len(registry) == 2
        
        registry.clear()
        
        assert len(registry) == 0


class TestContractRegistryLookup:
    """Tests for contract lookup behavior."""
    
    def test_get_falls_back_to_default_adapter(self):
        """Test that get falls back to default adapter."""
        registry = ContractRegistry()
        
        # Register with no specific adapter
        registry.register(DataContract(
            table="shared_table",
            columns=[ColumnContract(name="id")]
        ))
        
        # Should find it even when requesting specific adapter
        contract = registry.get("shared_table", adapter="specific_adapter")
        assert contract is not None
        assert contract.table == "shared_table"
    
    def test_register_overwrites_existing(self):
        """Test that registering same table overwrites."""
        registry = ContractRegistry()
        
        # Register first version
        registry.register(DataContract(
            table="test",
            version="1.0.0",
            columns=[ColumnContract(name="id")]
        ))
        
        # Register second version
        registry.register(DataContract(
            table="test",
            version="2.0.0",
            columns=[ColumnContract(name="id"), ColumnContract(name="name")]
        ))
        
        contract = registry.get("test")
        assert contract.version == "2.0.0"
        assert len(contract.columns) == 2


class TestGlobalRegistry:
    """Tests for global registry functions."""
    
    def test_get_global_registry(self):
        """Test getting global registry."""
        from waveql.contracts.registry import get_global_registry
        
        registry = get_global_registry()
        assert isinstance(registry, ContractRegistry)
        
        # Should return same instance
        registry2 = get_global_registry()
        assert registry is registry2
    
    def test_register_contract_global(self):
        """Test registering contract globally."""
        from waveql.contracts.registry import register_contract, get_contract, get_global_registry
        
        # Clear first
        get_global_registry().clear()
        
        contract = DataContract(
            table="global_test",
            columns=[ColumnContract(name="id")]
        )
        register_contract(contract)
        
        retrieved = get_contract("global_test")
        assert retrieved is not None
        assert retrieved.table == "global_test"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
