"""
Data Contract Models - Pydantic-based schema definitions for WaveQL.

Provides type-safe, auto-validating schema contracts that can be:
- Defined in Python with IDE autocomplete
- Exported to JSON Schema for documentation
- Used for runtime validation of API responses
- Compared against live schemas to detect drift
"""

from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Set, Union
from dataclasses import dataclass, field
import json
import hashlib

try:
    from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
    PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator, root_validator
    PYDANTIC_V2 = False

import pyarrow as pa


# =============================================================================
# Type Definitions
# =============================================================================

# Supported column types that map to Arrow types
ColumnType = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime",
    "date",
    "timestamp",
    "binary",
    "json",      # Stored as string, but semantically JSON
    "struct",    # Nested object
    "list",      # Array
    "any",       # No type enforcement
]


# Arrow type mapping
ARROW_TYPE_MAP: Dict[str, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "datetime": pa.timestamp("us"),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "binary": pa.binary(),
    "json": pa.string(),  # JSON stored as string
}


class ViolationType(str, Enum):
    """Types of contract violations."""
    MISSING_COLUMN = "missing_column"
    EXTRA_COLUMN = "extra_column"
    TYPE_MISMATCH = "type_mismatch"
    NULL_VIOLATION = "null_violation"
    CONSTRAINT_VIOLATION = "constraint_violation"


# =============================================================================
# Column Contract
# =============================================================================

class ColumnContract(BaseModel):
    """
    Contract for a single column in a table.
    
    Attributes:
        name: Column name (required)
        type: Data type (string, integer, float, boolean, datetime, etc.)
        nullable: Whether NULL values are allowed
        primary_key: Whether this is a primary key column
        description: Human-readable description
        constraints: Additional validation constraints
        default: Default value if missing
        aliases: Alternative names (for schema evolution)
    """
    
    if PYDANTIC_V2:
        model_config = ConfigDict(extra='forbid')
    else:
        class Config:
            extra = 'forbid'
    
    name: str = Field(..., description="Column name")
    type: ColumnType = Field(default="string", description="Data type")
    nullable: bool = Field(default=True, description="Allow NULL values")
    primary_key: bool = Field(default=False, description="Primary key column")
    description: str = Field(default="", description="Column description")
    constraints: List[str] = Field(default_factory=list, description="Validation constraints")
    default: Optional[Any] = Field(default=None, description="Default value")
    aliases: List[str] = Field(default_factory=list, description="Alternative column names")
    
    # Nested type definition (for struct/list types)
    nested_type: Optional[str] = Field(default=None, description="Element type for list columns")
    nested_columns: Optional[List["ColumnContract"]] = Field(
        default=None, 
        description="Nested columns for struct types"
    )
    
    def to_arrow_type(self) -> pa.DataType:
        """Convert to PyArrow DataType."""
        if self.type == "struct" and self.nested_columns:
            fields = [
                pa.field(col.name, col.to_arrow_type())
                for col in self.nested_columns
            ]
            return pa.struct(fields)
        
        if self.type == "list":
            element_type = ARROW_TYPE_MAP.get(self.nested_type or "string", pa.string())
            return pa.list_(element_type)
        
        return ARROW_TYPE_MAP.get(self.type, pa.string())
    
    def matches_arrow_type(self, arrow_type: pa.DataType) -> bool:
        """Check if an Arrow type is compatible with this contract."""
        expected = self.to_arrow_type()
        
        # Exact match
        if expected.equals(arrow_type):
            return True
        
        # Allow numeric promotion (int -> float)
        if self.type == "float" and pa.types.is_integer(arrow_type):
            return True
        
        # Allow timestamp variations
        if self.type in ("datetime", "timestamp"):
            if pa.types.is_timestamp(arrow_type):
                return True
        
        # String accepts anything (fallback type)
        if self.type == "string":
            return True
        
        # Any type matches everything
        if self.type == "any":
            return True
        
        return False


# Enable forward references for nested columns
if PYDANTIC_V2:
    ColumnContract.model_rebuild()


# =============================================================================
# Data Contract
# =============================================================================

class DataContract(BaseModel):
    """
    Complete data contract for a table.
    
    Defines the expected schema, validation rules, and metadata for a table.
    Can be used to:
    - Validate Arrow tables at runtime
    - Document expected API response structure
    - Detect schema drift between contract and live data
    - Generate JSON Schema for external documentation
    
    Example:
        contract = DataContract(
            table="incident",
            adapter="servicenow",
            version="1.0.0",
            columns=[
                ColumnContract(name="sys_id", type="string", nullable=False),
                ColumnContract(name="number", type="string", nullable=False),
                ColumnContract(name="priority", type="integer"),
            ]
        )
    """
    
    if PYDANTIC_V2:
        model_config = ConfigDict(extra='forbid')
    else:
        class Config:
            extra = 'forbid'
    
    # Required fields
    table: str = Field(..., description="Table name")
    columns: List[ColumnContract] = Field(..., description="Column definitions")
    
    # Optional metadata
    adapter: Optional[str] = Field(default=None, description="Adapter name (servicenow, salesforce, etc.)")
    version: str = Field(default="1.0.0", description="Contract version")
    description: str = Field(default="", description="Table description")
    
    # Validation options
    strict_columns: bool = Field(
        default=False, 
        description="If True, extra columns in data cause validation failure"
    )
    strict_types: bool = Field(
        default=True, 
        description="If True, type mismatches cause validation failure"
    )
    
    # Validators
    if PYDANTIC_V2:
        @field_validator('columns')
        @classmethod
        def validate_columns(cls, v: List[ColumnContract]) -> List[ColumnContract]:
            if not v:
                raise ValueError("At least one column is required")
            names = [c.name for c in v]
            if len(names) != len(set(names)):
                raise ValueError("Duplicate column names are not allowed")
            return v
    else:
        @validator('columns')
        def validate_columns(cls, v):
            if not v:
                raise ValueError("At least one column is required")
            names = [c.name for c in v]
            if len(names) != len(set(names)):
                raise ValueError("Duplicate column names are not allowed")
            return v
    
    # =========================================================================
    # Core Methods
    # =========================================================================
    
    def get_column(self, name: str) -> Optional[ColumnContract]:
        """Get a column contract by name."""
        for col in self.columns:
            if col.name == name:
                return col
            # Check aliases
            if name in col.aliases:
                return col
        return None
    
    def get_primary_keys(self) -> List[str]:
        """Get list of primary key column names."""
        return [c.name for c in self.columns if c.primary_key]
    
    def get_required_columns(self) -> List[str]:
        """Get list of non-nullable column names."""
        return [c.name for c in self.columns if not c.nullable]
    
    def to_arrow_schema(self) -> pa.Schema:
        """Convert contract to PyArrow Schema."""
        fields = [
            pa.field(col.name, col.to_arrow_type(), nullable=col.nullable)
            for col in self.columns
        ]
        return pa.schema(fields)
    
    def to_json_schema(self) -> Dict[str, Any]:
        """Export contract as JSON Schema for documentation."""
        properties = {}
        required = []
        
        type_map = {
            "string": "string",
            "integer": "integer",
            "float": "number",
            "boolean": "boolean",
            "datetime": "string",
            "date": "string",
            "timestamp": "string",
            "binary": "string",
            "json": "object",
            "any": {},
        }
        
        for col in self.columns:
            prop: Dict[str, Any] = {
                "type": type_map.get(col.type, "string"),
            }
            if col.description:
                prop["description"] = col.description
            if col.type in ("datetime", "date", "timestamp"):
                prop["format"] = "date-time"
            if col.type == "list":
                prop["type"] = "array"
                prop["items"] = {"type": type_map.get(col.nested_type or "string", "string")}
            if col.type == "struct" and col.nested_columns:
                prop["type"] = "object"
                prop["properties"] = {
                    nc.name: {"type": type_map.get(nc.type, "string")}
                    for nc in col.nested_columns
                }
            
            properties[col.name] = prop
            if not col.nullable:
                required.append(col.name)
        
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": self.table,
            "description": self.description,
            "type": "object",
            "properties": properties,
            "required": required,
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Serialize contract to JSON string."""
        if PYDANTIC_V2:
            return self.model_dump_json(indent=indent)
        else:
            return self.json(indent=indent)
    
    @classmethod
    def from_json(cls, json_str: str) -> "DataContract":
        """Deserialize contract from JSON string."""
        if PYDANTIC_V2:
            return cls.model_validate_json(json_str)
        else:
            return cls.parse_raw(json_str)
    
    @classmethod
    def from_yaml(cls, yaml_str: str) -> "DataContract":
        """Deserialize contract from YAML string."""
        try:
            import yaml
            data = yaml.safe_load(yaml_str)
            if PYDANTIC_V2:
                return cls.model_validate(data)
            else:
                return cls.parse_obj(data)
        except ImportError:
            raise ImportError("PyYAML is required for YAML support: pip install pyyaml")
    
    @classmethod
    def from_arrow_schema(
        cls,
        schema: pa.Schema,
        table: str,
        adapter: Optional[str] = None,
    ) -> "DataContract":
        """
        Create a contract from an existing PyArrow schema.
        
        Useful for bootstrapping contracts from live data.
        """
        arrow_to_type = {
            pa.string(): "string",
            pa.int64(): "integer",
            pa.int32(): "integer",
            pa.float64(): "float",
            pa.float32(): "float",
            pa.bool_(): "boolean",
            pa.date32(): "date",
            pa.binary(): "binary",
        }
        
        columns = []
        for field in schema:
            col_type = "string"  # Default
            
            # Check exact match
            if field.type in arrow_to_type:
                col_type = arrow_to_type[field.type]
            elif pa.types.is_timestamp(field.type):
                col_type = "timestamp"
            elif pa.types.is_integer(field.type):
                col_type = "integer"
            elif pa.types.is_floating(field.type):
                col_type = "float"
            elif pa.types.is_struct(field.type):
                col_type = "struct"
            elif pa.types.is_list(field.type):
                col_type = "list"
            
            columns.append(ColumnContract(
                name=field.name,
                type=col_type,
                nullable=field.nullable,
            ))
        
        return cls(
            table=table,
            adapter=adapter,
            columns=columns,
        )
    
    def hash(self) -> str:
        """Generate a hash for quick equality checking."""
        parts = [f"{c.name}:{c.type}:{c.nullable}" for c in self.columns]
        schema_str = "|".join(sorted(parts))
        return hashlib.md5(schema_str.encode()).hexdigest()


# =============================================================================
# Validation Result
# =============================================================================

@dataclass
class ContractViolation:
    """A single validation violation."""
    type: ViolationType
    column: str
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    row_index: Optional[int] = None
    
    def __str__(self) -> str:
        if self.row_index is not None:
            return f"[Row {self.row_index}] {self.column}: {self.message}"
        return f"{self.column}: {self.message}"


@dataclass
class ContractValidationResult:
    """Result of validating data against a contract."""
    valid: bool
    violations: List[ContractViolation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Statistics
    rows_checked: int = 0
    columns_checked: int = 0
    
    def raise_on_error(self) -> None:
        """Raise an exception if validation failed."""
        if not self.valid:
            errors = "\n".join(str(v) for v in self.violations[:10])
            if len(self.violations) > 10:
                errors += f"\n... and {len(self.violations) - 10} more violations"
            from waveql.exceptions import ContractViolationError
            raise ContractViolationError(
                f"Contract validation failed with {len(self.violations)} violations:\n{errors}"
            )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization."""
        return {
            "valid": self.valid,
            "violations": [
                {
                    "type": v.type.value,
                    "column": v.column,
                    "message": v.message,
                    "expected": str(v.expected) if v.expected else None,
                    "actual": str(v.actual) if v.actual else None,
                }
                for v in self.violations
            ],
            "warnings": self.warnings,
            "rows_checked": self.rows_checked,
            "columns_checked": self.columns_checked,
        }
