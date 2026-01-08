"""
Contract Registry - Centralized storage and management of DataContracts.

Provides:
- In-memory contract registration
- File-based contract loading (JSON/YAML)
- Contract lookup by table/adapter
- Schema drift detection between registered and live contracts
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from collections import defaultdict

import pyarrow as pa

from waveql.contracts.models import (
    DataContract,
    ContractValidationResult,
)
from waveql.contracts.validator import ContractValidator

logger = logging.getLogger(__name__)


class ContractRegistry:
    """
    Central registry for managing DataContracts.
    
    Stores contracts indexed by table name and adapter, allowing for:
    - Quick lookup of contracts when processing queries
    - Automatic validation of API responses
    - Schema drift detection
    
    Example:
        registry = ContractRegistry()
        
        # Register contracts
        registry.register(incident_contract)
        registry.register(user_contract)
        
        # Or load from directory
        registry.load_from_directory("./contracts/")
        
        # Lookup and validate
        contract = registry.get("incident", adapter="servicenow")
        result = registry.validate(table, "incident", "servicenow")
    """
    
    def __init__(self):
        """Initialize an empty registry."""
        # Contracts indexed by (adapter, table)
        self._contracts: Dict[tuple, DataContract] = {}
        
        # Index by table name only (for adapter-agnostic lookup)
        self._by_table: Dict[str, List[DataContract]] = defaultdict(list)
        
        # Track loaded files
        self._loaded_files: Set[str] = set()
    
    # =========================================================================
    # Registration
    # =========================================================================
    
    def register(self, contract: DataContract) -> None:
        """
        Register a contract in the registry.
        
        Args:
            contract: DataContract to register
        """
        key = (contract.adapter or "_default", contract.table)
        
        if key in self._contracts:
            logger.warning(
                "Overwriting existing contract for %s.%s",
                contract.adapter or "default",
                contract.table
            )
        
        self._contracts[key] = contract
        self._by_table[contract.table].append(contract)
        
        logger.debug(
            "Registered contract for %s.%s (v%s)",
            contract.adapter or "default",
            contract.table,
            contract.version
        )
    
    def unregister(self, table: str, adapter: Optional[str] = None) -> bool:
        """
        Remove a contract from the registry.
        
        Args:
            table: Table name
            adapter: Optional adapter name
            
        Returns:
            True if a contract was removed, False otherwise
        """
        key = (adapter or "_default", table)
        
        if key in self._contracts:
            contract = self._contracts.pop(key)
            self._by_table[table] = [
                c for c in self._by_table[table] if c != contract
            ]
            logger.debug("Unregistered contract for %s.%s", adapter or "default", table)
            return True
        
        return False
    
    # =========================================================================
    # Lookup
    # =========================================================================
    
    def get(
        self,
        table: str,
        adapter: Optional[str] = None,
    ) -> Optional[DataContract]:
        """
        Get a contract by table name and optional adapter.
        
        Args:
            table: Table name
            adapter: Optional adapter name
            
        Returns:
            DataContract if found, None otherwise
        """
        # Try exact match first
        key = (adapter or "_default", table)
        if key in self._contracts:
            return self._contracts[key]
        
        # Try default adapter if specific not found
        if adapter:
            default_key = ("_default", table)
            if default_key in self._contracts:
                return self._contracts[default_key]
        
        return None
    
    def get_all(self, adapter: Optional[str] = None) -> List[DataContract]:
        """
        Get all registered contracts.
        
        Args:
            adapter: Optional filter by adapter
            
        Returns:
            List of DataContracts
        """
        if adapter:
            return [
                c for (a, t), c in self._contracts.items()
                if a == adapter
            ]
        return list(self._contracts.values())
    
    def has_contract(self, table: str, adapter: Optional[str] = None) -> bool:
        """Check if a contract is registered for the given table."""
        return self.get(table, adapter) is not None
    
    # =========================================================================
    # File Loading
    # =========================================================================
    
    def load_from_file(self, path: Union[str, Path]) -> DataContract:
        """
        Load and register a contract from a JSON or YAML file.
        
        Args:
            path: Path to contract file
            
        Returns:
            The loaded DataContract
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is invalid
        """
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found: {path}")
        
        content = path.read_text(encoding="utf-8")
        
        if path.suffix.lower() in (".yaml", ".yml"):
            contract = DataContract.from_yaml(content)
        elif path.suffix.lower() == ".json":
            contract = DataContract.from_json(content)
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")
        
        self.register(contract)
        self._loaded_files.add(str(path.absolute()))
        
        logger.info("Loaded contract from %s: %s.%s", path, contract.adapter, contract.table)
        
        return contract
    
    def load_from_directory(
        self,
        directory: Union[str, Path],
        recursive: bool = True,
    ) -> List[DataContract]:
        """
        Load all contracts from a directory.
        
        Args:
            directory: Directory containing contract files
            recursive: Whether to search subdirectories
            
        Returns:
            List of loaded DataContracts
        """
        directory = Path(directory)
        
        if not directory.exists():
            raise NotADirectoryError(f"Contract directory not found: {directory}")
        
        contracts = []
        pattern = "**/*" if recursive else "*"
        
        for path in directory.glob(pattern):
            if path.suffix.lower() in (".json", ".yaml", ".yml"):
                try:
                    contract = self.load_from_file(path)
                    contracts.append(contract)
                except Exception as e:
                    logger.warning("Failed to load contract from %s: %s", path, e)
        
        logger.info("Loaded %d contracts from %s", len(contracts), directory)
        
        return contracts
    
    def save_to_file(
        self,
        contract: DataContract,
        path: Union[str, Path],
        format: str = "json",
    ) -> None:
        """
        Save a contract to a file.
        
        Args:
            contract: DataContract to save
            path: Output file path
            format: Output format ("json" or "yaml")
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == "yaml":
            try:
                import yaml
                data = json.loads(contract.to_json())
                content = yaml.dump(data, default_flow_style=False, sort_keys=False)
            except ImportError:
                raise ImportError("PyYAML is required for YAML export: pip install pyyaml")
        else:
            content = contract.to_json(indent=2)
        
        path.write_text(content, encoding="utf-8")
        logger.info("Saved contract to %s", path)

    def export_to_dbt(self, path: Union[str, Path]) -> None:
        """
        Export all registered contracts to a dbt sources.yml file.
        
        Maps WaveQL adapters to dbt sources, and tables/columns to their 
        dbt definitions. automatically includes 'unique' and 'not_null' 
        tests based on contract constraints.
        
        Args:
            path: Output file path (e.g. "models/sources.yml")
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Group contracts by adapter
        sources = defaultdict(list)
        for contract in self.get_all():
            adapter = contract.adapter or "default"
            sources[adapter].append(contract)
            
        dbt_sources = []
        for adapter_name, contracts in sources.items():
            tables = []
            for contract in contracts:
                columns = []
                for col in contract.columns:
                    col_def = {
                        "name": col.name,
                        "description": col.description or "",
                    }
                    
                    # Map constraints to dbt tests
                    tests = []
                    if not col.nullable:
                        if "not_null" not in tests:
                            tests.append("not_null")
                    if col.primary_key:
                        if "unique" not in tests:
                            tests.append("unique")
                        if "not_null" not in tests:
                            tests.append("not_null")
                            
                    if tests:
                        col_def["tests"] = tests
                        
                    columns.append(col_def)
                
                tables.append({
                    "name": contract.table,
                    "description": contract.description or "",
                    "columns": columns
                })
            
            dbt_sources.append({
                "name": adapter_name,
                "tables": tables
            })
            
        dbt_output = {
            "version": 2,
            "sources": dbt_sources
        }
        
        try:
            import yaml
            content = yaml.dump(dbt_output, default_flow_style=False, sort_keys=False)
            path.write_text(content, encoding="utf-8")
            logger.info("Exported dbt sources to %s", path)
        except ImportError:
            raise ImportError("PyYAML is required for dbt export: pip install pyyaml")
    
    # =========================================================================
    # Validation
    # =========================================================================
    
    def validate(
        self,
        table_data: pa.Table,
        table: str,
        adapter: Optional[str] = None,
        *,
        raise_on_error: bool = False,
    ) -> ContractValidationResult:
        """
        Validate table data against its registered contract.
        
        Args:
            table_data: PyArrow Table to validate
            table: Table name
            adapter: Optional adapter name
            raise_on_error: If True, raise exception on validation failure
            
        Returns:
            ContractValidationResult
            
        Raises:
            KeyError: If no contract is registered for the table
            ContractViolationError: If raise_on_error=True and validation fails
        """
        contract = self.get(table, adapter)
        
        if contract is None:
            raise KeyError(
                f"No contract registered for table '{table}' "
                f"(adapter: {adapter or 'any'})"
            )
        
        validator = ContractValidator(contract)
        result = validator.validate(table_data)
        
        if raise_on_error:
            result.raise_on_error()
        
        return result
    
    # =========================================================================
    # Schema Drift Detection
    # =========================================================================
    
    def detect_drift(
        self,
        live_schema: pa.Schema,
        table: str,
        adapter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Detect schema drift between registered contract and live schema.
        
        Args:
            live_schema: PyArrow Schema from live data
            table: Table name
            adapter: Optional adapter name
            
        Returns:
            Dict with drift analysis:
                - has_drift: bool
                - added_columns: List of new column names
                - removed_columns: List of missing column names
                - type_changes: Dict of column -> (old_type, new_type)
        """
        contract = self.get(table, adapter)
        
        if contract is None:
            raise KeyError(f"No contract registered for table '{table}'")
        
        contract_columns = {c.name: c for c in contract.columns}
        live_columns = {f.name: f for f in live_schema}
        
        # Detect additions
        added = [
            name for name in live_columns
            if name not in contract_columns
        ]
        
        # Detect removals
        removed = [
            name for name in contract_columns
            if name not in live_columns
        ]
        
        # Detect type changes
        type_changes = {}
        for name, col_contract in contract_columns.items():
            if name in live_columns:
                live_type = live_columns[name].type
                if not col_contract.matches_arrow_type(live_type):
                    type_changes[name] = (col_contract.type, str(live_type))
        
        has_drift = bool(added or removed or type_changes)
        
        result = {
            "has_drift": has_drift,
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changes,
        }
        
        if has_drift:
            logger.warning(
                "Schema drift detected for %s.%s: +%d columns, -%d columns, %d type changes",
                adapter or "default",
                table,
                len(added),
                len(removed),
                len(type_changes)
            )
        
        return result
    
    def generate_contract_from_schema(
        self,
        schema: pa.Schema,
        table: str,
        adapter: Optional[str] = None,
        register: bool = False,
    ) -> DataContract:
        """
        Generate a contract from a live PyArrow schema.
        
        Useful for bootstrapping contracts from existing data.
        
        Args:
            schema: PyArrow Schema
            table: Table name
            adapter: Optional adapter name
            register: If True, automatically register the contract
            
        Returns:
            Generated DataContract
        """
        contract = DataContract.from_arrow_schema(schema, table, adapter)
        
        if register:
            self.register(contract)
        
        return contract
    
    # =========================================================================
    # Utilities
    # =========================================================================
    
    def __len__(self) -> int:
        """Return number of registered contracts."""
        return len(self._contracts)
    
    def __contains__(self, key: tuple) -> bool:
        """Check if (adapter, table) is registered."""
        return key in self._contracts
    
    def __repr__(self) -> str:
        return f"<ContractRegistry contracts={len(self._contracts)}>"
    
    def clear(self) -> None:
        """Remove all registered contracts."""
        self._contracts.clear()
        self._by_table.clear()
        self._loaded_files.clear()
        logger.debug("Contract registry cleared")


# Global registry instance
_global_registry: Optional[ContractRegistry] = None


def get_global_registry() -> ContractRegistry:
    """Get or create the global contract registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = ContractRegistry()
    return _global_registry


def register_contract(contract: DataContract) -> None:
    """Register a contract in the global registry."""
    get_global_registry().register(contract)


def get_contract(table: str, adapter: Optional[str] = None) -> Optional[DataContract]:
    """Get a contract from the global registry."""
    return get_global_registry().get(table, adapter)
