"""
WaveQL Data Contracts - Pydantic-based schema validation for API responses.

Define, validate, and document your data schemas with type-safe contracts.

Example:
    from waveql.contracts import DataContract, ColumnContract
    
    # Define a contract
    contract = DataContract(
        table="incident",
        adapter="servicenow",
        columns=[
            ColumnContract(name="sys_id", type="string", nullable=False),
            ColumnContract(name="number", type="string", nullable=False),
            ColumnContract(name="priority", type="integer", nullable=True),
        ]
    )
    
    # Validate data against contract
    result = contract.validate(arrow_table)
    if not result.valid:
        print(result.errors)
"""

from waveql.contracts.models import (
    DataContract,
    ColumnContract,
    ContractValidationResult,
    ContractViolation,
    ViolationType,
)
from waveql.contracts.validator import ContractValidator
from waveql.contracts.registry import ContractRegistry

__all__ = [
    "DataContract",
    "ColumnContract",
    "ContractValidationResult",
    "ContractViolation",
    "ViolationType",
    "ContractValidator",
    "ContractRegistry",
]
