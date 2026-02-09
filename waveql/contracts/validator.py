"""
Contract Validator - Runtime validation of Arrow tables against DataContracts.

Provides efficient validation of PyArrow tables against defined contracts,
with detailed error reporting and optional strict/lenient modes.
"""

from __future__ import annotations
import logging
from typing import List

import pyarrow as pa

from waveql.contracts.models import (
    DataContract,
    ContractValidationResult,
    ContractViolation,
    ViolationType,
)

logger = logging.getLogger(__name__)


class ContractValidator:
    """
    Validates PyArrow tables against DataContracts.
    
    Features:
    - Schema-level validation (columns, types)
    - Row-level validation (null checks, constraints)
    - Configurable strictness
    - Detailed violation reporting
    
    Example:
        validator = ContractValidator(contract)
        result = validator.validate(arrow_table)
        
        if not result.valid:
            for violation in result.violations:
                print(f"{violation.column}: {violation.message}")
    """
    
    def __init__(
        self,
        contract: DataContract,
        *,
        check_nulls: bool = True,
        max_row_violations: int = 100,
        sample_rows_for_type_check: int = 0,
    ):
        """
        Initialize validator with a contract.
        
        Args:
            contract: The DataContract to validate against
            check_nulls: Whether to validate null constraints at row level
            max_row_violations: Maximum row-level violations to report
            sample_rows_for_type_check: Number of rows to sample for deep type checking (0 = schema only)
        """
        self.contract = contract
        self.check_nulls = check_nulls
        self.max_row_violations = max_row_violations
        self.sample_rows_for_type_check = sample_rows_for_type_check
    
    def validate(self, table: pa.Table) -> ContractValidationResult:
        """
        Validate an Arrow table against the contract.
        
        Args:
            table: PyArrow Table to validate
            
        Returns:
            ContractValidationResult with validation status and any violations
        """
        violations: List[ContractViolation] = []
        warnings: List[str] = []
        
        schema = table.schema
        contract_columns = {c.name: c for c in self.contract.columns}
        actual_columns = {f.name: f for f in schema}
        
        # Track what was checked
        columns_checked = 0
        
        # =====================================================================
        # 1. Check for missing columns (in contract but not in data)
        # =====================================================================
        for col_name, col_contract in contract_columns.items():
            # Check for column or its aliases
            found = col_name in actual_columns
            if not found:
                for alias in col_contract.aliases:
                    if alias in actual_columns:
                        found = True
                        warnings.append(
                            f"Column '{col_name}' found via alias '{alias}'"
                        )
                        break
            
            if not found:
                # Missing required column
                if not col_contract.nullable and col_contract.default is None:
                    violations.append(ContractViolation(
                        type=ViolationType.MISSING_COLUMN,
                        column=col_name,
                        message=f"Required column '{col_name}' is missing from data",
                        expected=col_contract.type,
                        actual=None,
                    ))
                else:
                    # Optional column missing is just a warning
                    warnings.append(f"Optional column '{col_name}' not present in data")
        
        # =====================================================================
        # 2. Check for extra columns (in data but not in contract)
        # =====================================================================
        if self.contract.strict_columns:
            contract_names = set(contract_columns.keys())
            # Include aliases
            for col in self.contract.columns:
                contract_names.update(col.aliases)
            
            for col_name in actual_columns:
                if col_name not in contract_names:
                    violations.append(ContractViolation(
                        type=ViolationType.EXTRA_COLUMN,
                        column=col_name,
                        message=f"Unexpected column '{col_name}' not defined in contract",
                        expected=None,
                        actual=str(actual_columns[col_name].type),
                    ))
        
        # =====================================================================
        # 3. Type validation for matching columns
        # =====================================================================
        for col_name, col_contract in contract_columns.items():
            if col_name not in actual_columns:
                continue
            
            columns_checked += 1
            actual_field = actual_columns[col_name]
            
            if self.contract.strict_types:
                if not col_contract.matches_arrow_type(actual_field.type):
                    violations.append(ContractViolation(
                        type=ViolationType.TYPE_MISMATCH,
                        column=col_name,
                        message=f"Type mismatch: expected '{col_contract.type}', got '{actual_field.type}'",
                        expected=col_contract.type,
                        actual=str(actual_field.type),
                    ))
        
        # =====================================================================
        # 4. Row-level null validation (if enabled)
        # =====================================================================
        rows_checked = 0
        if self.check_nulls and len(table) > 0:
            required_columns = [c for c in self.contract.columns if not c.nullable]
            
            for col_contract in required_columns:
                if col_contract.name not in actual_columns:
                    continue
                
                column = table.column(col_contract.name)
                null_count = column.null_count
                
                if null_count > 0:
                    violations.append(ContractViolation(
                        type=ViolationType.NULL_VIOLATION,
                        column=col_contract.name,
                        message=f"Non-nullable column has {null_count} NULL values",
                        expected="no nulls",
                        actual=f"{null_count} nulls",
                    ))
            
            rows_checked = len(table)
        
        # =====================================================================
        # 5. Build result
        # =====================================================================
        is_valid = len(violations) == 0
        
        result = ContractValidationResult(
            valid=is_valid,
            violations=violations,
            warnings=warnings,
            rows_checked=rows_checked,
            columns_checked=columns_checked,
        )
        
        if not is_valid:
            logger.warning(
                "Contract validation failed for '%s': %d violations",
                self.contract.table,
                len(violations)
            )
        else:
            logger.debug(
                "Contract validation passed for '%s' (%d columns, %d rows)",
                self.contract.table,
                columns_checked,
                rows_checked
            )
        
        return result
    
    def validate_schema(self, schema: pa.Schema) -> ContractValidationResult:
        """
        Validate just the schema (no data) against the contract.
        
        Useful for pre-flight checks before fetching data.
        
        Args:
            schema: PyArrow Schema to validate
            
        Returns:
            ContractValidationResult with schema-level validation
        """
        # Create empty table with the schema
        empty_table = pa.table({f.name: [] for f in schema}, schema=schema)
        
        # Run validation with null checking disabled (no rows to check)
        original_check_nulls = self.check_nulls
        self.check_nulls = False
        
        try:
            result = self.validate(empty_table)
        finally:
            self.check_nulls = original_check_nulls
        
        return result


def validate_table(
    table: pa.Table,
    contract: DataContract,
    *,
    strict: bool = True,
    raise_on_error: bool = False,
) -> ContractValidationResult:
    """
    Convenience function to validate a table against a contract.
    
    Args:
        table: PyArrow Table to validate
        contract: DataContract to validate against
        strict: If True, use strict type and column checking
        raise_on_error: If True, raise ContractViolationError on failure
        
    Returns:
        ContractValidationResult
        
    Raises:
        ContractViolationError: If raise_on_error=True and validation fails
    """
    # Apply strict settings to contract
    if strict:
        contract.strict_columns = True
        contract.strict_types = True
    
    validator = ContractValidator(contract)
    result = validator.validate(table)
    
    if raise_on_error:
        result.raise_on_error()
    
    return result
