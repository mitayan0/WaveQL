"""
Schema Inference Utilities - Recursive type inference for nested JSON structures.

This module provides utilities to automatically infer PyArrow schemas from
JSON/dict records, supporting:
- Native struct types for nested objects (enabling dot-notation queries)
- List types for arrays
- Multi-record sampling for robust type inference
- Type conflict resolution
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Union
from collections import defaultdict

import pyarrow as pa


# Type priority for conflict resolution (higher = preferred)
TYPE_PRIORITY = {
    pa.null(): 0,
    pa.bool_(): 1,
    pa.int64(): 2,
    pa.float64(): 3,
    pa.string(): 4,  # String is the fallback, so it has highest priority
}


def infer_arrow_type(value: Any, max_depth: int = 10) -> pa.DataType:
    """
    Recursively infer PyArrow type from a Python value.
    
    Args:
        value: Any Python value (dict, list, scalar, None)
        max_depth: Maximum recursion depth for nested structures
        
    Returns:
        PyArrow DataType that best represents the value
        
    Examples:
        >>> infer_arrow_type({"name": "Alice", "age": 30})
        StructType([('name', string), ('age', int64)])
        
        >>> infer_arrow_type([1, 2, 3])
        ListType(int64)
    """
    if max_depth <= 0:
        # Prevent infinite recursion, fallback to string
        return pa.string()
    
    if value is None:
        return pa.null()
    
    if isinstance(value, bool):
        return pa.bool_()
    
    if isinstance(value, int):
        return pa.int64()
    
    if isinstance(value, float):
        return pa.float64()
    
    if isinstance(value, str):
        return pa.string()
    
    if isinstance(value, dict):
        if not value:
            # Empty dict -> treat as empty struct or string
            return pa.string()
        
        # Build struct type from dict keys
        fields = []
        for key, val in value.items():
            field_type = infer_arrow_type(val, max_depth - 1)
            fields.append(pa.field(str(key), field_type))
        
        return pa.struct(fields)
    
    if isinstance(value, (list, tuple)):
        if not value:
            # Empty list -> list of strings (safest default)
            return pa.list_(pa.string())
        
        # Infer element type from all list items and merge
        element_types = [infer_arrow_type(item, max_depth - 1) for item in value]
        merged_type = element_types[0]
        for t in element_types[1:]:
            merged_type = merge_arrow_types(merged_type, t)
        
        return pa.list_(merged_type)
    
    # Default fallback for unknown types
    return pa.string()


def merge_arrow_types(type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
    """
    Merge two Arrow types into a compatible unified type.
    
    Used when sampling multiple records that may have different types
    for the same field.
    
    Args:
        type1: First Arrow type
        type2: Second Arrow type
        
    Returns:
        A type that can represent both inputs
        
    Examples:
        >>> merge_arrow_types(pa.int64(), pa.float64())
        float64
        
        >>> merge_arrow_types(pa.int64(), pa.string())
        string
    """
    # Same type -> return as is
    if type1.equals(type2):
        return type1
    
    # Null can be promoted to anything
    if pa.types.is_null(type1):
        return type2
    if pa.types.is_null(type2):
        return type1
    
    # Both are structs -> merge fields
    if pa.types.is_struct(type1) and pa.types.is_struct(type2):
        return _merge_struct_types(type1, type2)
    
    # Both are lists -> merge element types
    if pa.types.is_list(type1) and pa.types.is_list(type2):
        merged_element = merge_arrow_types(type1.value_type, type2.value_type)
        return pa.list_(merged_element)
    
    # Numeric promotion: int -> float
    if pa.types.is_integer(type1) and pa.types.is_floating(type2):
        return pa.float64()
    if pa.types.is_floating(type1) and pa.types.is_integer(type2):
        return pa.float64()
    
    # Everything else -> fallback to string (safest)
    return pa.string()


def _merge_struct_types(type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
    """Merge two struct types, combining all fields."""
    fields1 = {f.name: f.type for f in type1}
    fields2 = {f.name: f.type for f in type2}
    
    all_keys = set(fields1.keys()) | set(fields2.keys())
    merged_fields = []
    
    for key in sorted(all_keys):
        if key in fields1 and key in fields2:
            # Both have this field -> merge types
            merged_type = merge_arrow_types(fields1[key], fields2[key])
            merged_fields.append(pa.field(key, merged_type))
        elif key in fields1:
            # Only in type1 -> nullable
            merged_fields.append(pa.field(key, fields1[key]))
        else:
            # Only in type2 -> nullable
            merged_fields.append(pa.field(key, fields2[key]))
    
    return pa.struct(merged_fields)


def infer_schema_from_records(
    records: List[Dict[str, Any]],
    sample_size: int = 5,
    max_depth: int = 10,
) -> pa.Schema:
    """
    Infer PyArrow schema by sampling multiple records.
    
    This is more robust than single-record inference because it can:
    - Find non-null values for fields that are null in some records
    - Detect type variations and choose compatible types
    - Discover all possible fields across records
    
    Args:
        records: List of dict records from API response
        sample_size: Number of records to sample (default 5)
        max_depth: Maximum recursion depth for nested structures
        
    Returns:
        PyArrow Schema representing all fields
        
    Example:
        >>> records = [
        ...     {"id": 1, "name": "Alice", "meta": {"active": True}},
        ...     {"id": 2, "name": "Bob", "meta": {"active": False, "score": 95}},
        ... ]
        >>> schema = infer_schema_from_records(records)
        >>> print(schema)
        id: int64
        name: string
        meta: struct<active: bool, score: int64>
    """
    if not records:
        return pa.schema([])
    
    # Sample records (spread across the dataset for variety)
    sample_indices = _get_sample_indices(len(records), sample_size)
    sampled = [records[i] for i in sample_indices]
    
    # Collect field types across all samples
    field_types: Dict[str, List[pa.DataType]] = defaultdict(list)
    
    for record in sampled:
        for key, value in record.items():
            inferred = infer_arrow_type(value, max_depth)
            field_types[key].append(inferred)
    
    # Merge types for each field
    schema_fields = []
    for key in sorted(field_types.keys()):
        types = field_types[key]
        merged = types[0]
        for t in types[1:]:
            merged = merge_arrow_types(merged, t)
        
        # Null type means all samples were null -> default to string
        if pa.types.is_null(merged):
            merged = pa.string()
        
        schema_fields.append(pa.field(key, merged))
    
    return pa.schema(schema_fields)


def _get_sample_indices(total: int, sample_size: int) -> List[int]:
    """Get evenly distributed sample indices across the dataset."""
    if total <= sample_size:
        return list(range(total))
    
    # Include first, last, and evenly spaced middle samples
    step = (total - 1) / (sample_size - 1)
    return [int(i * step) for i in range(sample_size)]


def convert_record_to_arrow_row(
    record: Dict[str, Any],
    schema: pa.Schema,
) -> Dict[str, Any]:
    """
    Convert a single record to match the expected Arrow schema.
    
    Handles:
    - Missing fields (returns None)
    - Nested dicts (converts to proper structure)
    - Type coercion where possible
    
    Args:
        record: Single dict record
        schema: Target PyArrow schema
        
    Returns:
        Dict with values converted to match schema types
    """
    result = {}
    
    for field in schema:
        value = record.get(field.name)
        result[field.name] = _convert_value(value, field.type)
    
    return result


def _convert_value(value: Any, target_type: pa.DataType) -> Any:
    """Convert a value to match the target Arrow type."""
    if value is None:
        return None
    
    # Struct conversion
    if pa.types.is_struct(target_type):
        if isinstance(value, dict):
            result = {}
            for field in target_type:
                child_value = value.get(field.name)
                result[field.name] = _convert_value(child_value, field.type)
            return result
        else:
            # Non-dict for struct field -> return None
            return None
    
    # List conversion
    if pa.types.is_list(target_type):
        if isinstance(value, (list, tuple)):
            return [_convert_value(item, target_type.value_type) for item in value]
        else:
            return None
    
    # Scalar conversions
    if pa.types.is_string(target_type):
        return str(value) if value is not None else None
    
    if pa.types.is_integer(target_type):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    if pa.types.is_floating(target_type):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    if pa.types.is_boolean(target_type):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)
    
    return value


def records_to_arrow_table(
    records: List[Dict[str, Any]],
    schema: Optional[pa.Schema] = None,
    sample_size: int = 5,
) -> pa.Table:
    """
    Convert a list of dict records to a PyArrow Table with struct support.
    
    This is the main entry point for converting API responses to Arrow tables
    with native struct columns for nested JSON.
    
    Args:
        records: List of dict records
        schema: Optional pre-defined schema. If None, will be inferred.
        sample_size: Number of records to sample for schema inference
        
    Returns:
        PyArrow Table with proper typing including structs
        
    Example:
        >>> records = [
        ...     {"id": 1, "user": {"name": "Alice", "active": True}},
        ...     {"id": 2, "user": {"name": "Bob", "active": False}},
        ... ]
        >>> table = records_to_arrow_table(records)
        >>> # Now you can query: SELECT user.name FROM table
    """
    if not records:
        if schema:
            return pa.table({f.name: [] for f in schema})
        return pa.table({})
    
    # Infer schema if not provided
    if schema is None:
        schema = infer_schema_from_records(records, sample_size)
    
    # Build column arrays
    columns = {}
    for field in schema:
        values = []
        for record in records:
            raw_value = record.get(field.name)
            converted = _convert_value(raw_value, field.type)
            values.append(converted)
        
        # Create Arrow array with proper type
        try:
            columns[field.name] = pa.array(values, type=field.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            # Fallback: convert everything to strings
            str_values = [str(v) if v is not None else None for v in values]
            columns[field.name] = pa.array(str_values, type=pa.string())
    
    return pa.table(columns)


# =============================================================================
# Schema Evolution Detection
# =============================================================================

class SchemaChange:
    """Represents a detected schema change."""
    
    def __init__(
        self,
        change_type: str,
        field_name: str,
        old_type: Optional[pa.DataType] = None,
        new_type: Optional[pa.DataType] = None,
    ):
        self.change_type = change_type  # 'added', 'removed', 'type_changed'
        self.field_name = field_name
        self.old_type = old_type
        self.new_type = new_type
    
    def __repr__(self):
        if self.change_type == 'added':
            return f"SchemaChange(ADDED: {self.field_name} -> {self.new_type})"
        elif self.change_type == 'removed':
            return f"SchemaChange(REMOVED: {self.field_name} was {self.old_type})"
        else:
            return f"SchemaChange(CHANGED: {self.field_name} from {self.old_type} to {self.new_type})"


def detect_schema_changes(
    cached_schema: pa.Schema,
    new_schema: pa.Schema,
) -> List[SchemaChange]:
    """
    Detect changes between a cached schema and a newly inferred schema.
    
    This is used for schema evolution detection - when an API adds new fields
    or changes field types.
    
    Args:
        cached_schema: Previously cached Arrow schema
        new_schema: Newly inferred schema from current data
        
    Returns:
        List of SchemaChange objects describing the differences
        
    Example:
        >>> old = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        >>> new = pa.schema([pa.field("id", pa.int64()), pa.field("email", pa.string())])
        >>> changes = detect_schema_changes(old, new)
        >>> # Returns: [SchemaChange(REMOVED: name), SchemaChange(ADDED: email)]
    """
    changes = []
    
    cached_fields = {f.name: f.type for f in cached_schema}
    new_fields = {f.name: f.type for f in new_schema}
    
    # Check for removed fields
    for name, old_type in cached_fields.items():
        if name not in new_fields:
            changes.append(SchemaChange('removed', name, old_type=old_type))
    
    # Check for added or changed fields
    for name, new_type in new_fields.items():
        if name not in cached_fields:
            changes.append(SchemaChange('added', name, new_type=new_type))
        elif not cached_fields[name].equals(new_type):
            changes.append(SchemaChange(
                'type_changed', name,
                old_type=cached_fields[name],
                new_type=new_type
            ))
    
    return changes


def evolve_schema(
    cached_schema: pa.Schema,
    new_schema: pa.Schema,
    allow_type_changes: bool = True,
) -> pa.Schema:
    """
    Evolve a schema by merging new fields without losing existing ones.
    
    This implements a "forward-compatible" schema evolution strategy:
    - New fields are added
    - Removed fields are kept (marked as nullable)
    - Type changes use the merged (compatible) type
    
    Args:
        cached_schema: Previously cached Arrow schema
        new_schema: Newly inferred schema from current data
        allow_type_changes: If True, allow type widening (int -> float)
        
    Returns:
        Evolved schema that is compatible with both old and new data
        
    Example:
        >>> old = pa.schema([pa.field("id", pa.int64())])
        >>> new = pa.schema([pa.field("id", pa.int64()), pa.field("email", pa.string())])
        >>> evolved = evolve_schema(old, new)
        >>> # Returns schema with both 'id' and 'email'
    """
    cached_fields = {f.name: f.type for f in cached_schema}
    new_fields = {f.name: f.type for f in new_schema}
    
    all_field_names = set(cached_fields.keys()) | set(new_fields.keys())
    
    evolved_fields = []
    for name in sorted(all_field_names):
        if name in cached_fields and name in new_fields:
            # Both have this field
            if allow_type_changes:
                merged_type = merge_arrow_types(cached_fields[name], new_fields[name])
            else:
                merged_type = cached_fields[name]  # Keep old type
            evolved_fields.append(pa.field(name, merged_type))
        elif name in cached_fields:
            # Only in cached (removed in new data) - keep it
            evolved_fields.append(pa.field(name, cached_fields[name]))
        else:
            # Only in new - add it
            evolved_fields.append(pa.field(name, new_fields[name]))
    
    return pa.schema(evolved_fields)


def schema_hash(schema: pa.Schema) -> str:
    """
    Generate a hash for a schema to quickly detect changes.
    
    Used for efficient cache invalidation - if the hash matches,
    no detailed comparison is needed.
    
    Args:
        schema: PyArrow schema
        
    Returns:
        Hash string representing the schema structure
    """
    import hashlib
    
    # Build a normalized string representation
    parts = []
    for field in schema:
        parts.append(f"{field.name}:{str(field.type)}")
    
    schema_str = "|".join(sorted(parts))
    return hashlib.md5(schema_str.encode()).hexdigest()

