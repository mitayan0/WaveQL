"""
Tests for WaveQL utils/schema module.

This covers the 57% uncovered module waveql/utils/schema.py
"""

import pytest
import pyarrow as pa

from waveql.utils.schema import (
    infer_arrow_type,
    merge_arrow_types,
    infer_schema_from_records,
    convert_record_to_arrow_row,
    records_to_arrow_table,
    SchemaChange,
    detect_schema_changes,
    evolve_schema,
    schema_hash,
    _get_sample_indices,
    _merge_struct_types,
    _convert_value,
)


class TestInferArrowType:
    """Tests for infer_arrow_type function."""
    
    def test_infer_null(self):
        """Test inferring null type."""
        assert infer_arrow_type(None).equals(pa.null())
    
    def test_infer_bool(self):
        """Test inferring boolean type."""
        assert infer_arrow_type(True).equals(pa.bool_())
        assert infer_arrow_type(False).equals(pa.bool_())
    
    def test_infer_int(self):
        """Test inferring integer type."""
        assert infer_arrow_type(42).equals(pa.int64())
        assert infer_arrow_type(-100).equals(pa.int64())
    
    def test_infer_float(self):
        """Test inferring float type."""
        assert infer_arrow_type(3.14).equals(pa.float64())
        assert infer_arrow_type(-2.5).equals(pa.float64())
    
    def test_infer_string(self):
        """Test inferring string type."""
        assert infer_arrow_type("hello").equals(pa.string())
        assert infer_arrow_type("").equals(pa.string())
    
    def test_infer_empty_dict(self):
        """Test inferring empty dict (returns string)."""
        assert infer_arrow_type({}).equals(pa.string())
    
    def test_infer_dict_struct(self):
        """Test inferring dict as struct."""
        result = infer_arrow_type({"name": "Alice", "age": 30})
        assert pa.types.is_struct(result)
        field_names = [f.name for f in result]
        assert "name" in field_names
        assert "age" in field_names
    
    def test_infer_nested_dict(self):
        """Test inferring nested dict."""
        result = infer_arrow_type({
            "user": {"name": "Alice", "active": True}
        })
        assert pa.types.is_struct(result)
        user_field = result.field("user")
        assert pa.types.is_struct(user_field.type)
    
    def test_infer_empty_list(self):
        """Test inferring empty list."""
        result = infer_arrow_type([])
        assert pa.types.is_list(result)
    
    def test_infer_list_of_ints(self):
        """Test inferring list of integers."""
        result = infer_arrow_type([1, 2, 3])
        assert pa.types.is_list(result)
        assert result.value_type.equals(pa.int64())
    
    def test_infer_list_mixed_types(self):
        """Test inferring list with mixed types."""
        result = infer_arrow_type([1, 2.5, 3])
        assert pa.types.is_list(result)
        # Should merge int and float to float
        assert result.value_type.equals(pa.float64())
    
    def test_infer_max_depth_exceeded(self):
        """Test that max depth returns string."""
        deeply_nested = {"a": {"b": {"c": {"d": {"e": {}}}}}}
        result = infer_arrow_type(deeply_nested, max_depth=1)
        # At max_depth=1, inner structures become strings
        assert pa.types.is_struct(result)
    
    def test_infer_tuple_as_list(self):
        """Test that tuples are treated as lists."""
        result = infer_arrow_type((1, 2, 3))
        assert pa.types.is_list(result)
    
    def test_infer_unknown_type(self):
        """Test that unknown types fallback to string."""
        class CustomObject:
            pass
        result = infer_arrow_type(CustomObject())
        assert result.equals(pa.string())


class TestMergeArrowTypes:
    """Tests for merge_arrow_types function."""
    
    def test_merge_same_types(self):
        """Test merging identical types."""
        assert merge_arrow_types(pa.int64(), pa.int64()).equals(pa.int64())
        assert merge_arrow_types(pa.string(), pa.string()).equals(pa.string())
    
    def test_merge_null_with_other(self):
        """Test merging null with other types."""
        assert merge_arrow_types(pa.null(), pa.int64()).equals(pa.int64())
        assert merge_arrow_types(pa.string(), pa.null()).equals(pa.string())
    
    def test_merge_int_float(self):
        """Test merging int and float to float."""
        assert merge_arrow_types(pa.int64(), pa.float64()).equals(pa.float64())
        assert merge_arrow_types(pa.float64(), pa.int64()).equals(pa.float64())
    
    def test_merge_incompatible_to_string(self):
        """Test merging incompatible types to string."""
        assert merge_arrow_types(pa.int64(), pa.string()).equals(pa.string())
        assert merge_arrow_types(pa.bool_(), pa.int64()).equals(pa.string())
    
    def test_merge_structs(self):
        """Test merging struct types."""
        struct1 = pa.struct([pa.field("a", pa.int64())])
        struct2 = pa.struct([pa.field("b", pa.string())])
        result = merge_arrow_types(struct1, struct2)
        assert pa.types.is_struct(result)
        field_names = [f.name for f in result]
        assert "a" in field_names
        assert "b" in field_names
    
    def test_merge_lists(self):
        """Test merging list types."""
        list1 = pa.list_(pa.int64())
        list2 = pa.list_(pa.float64())
        result = merge_arrow_types(list1, list2)
        assert pa.types.is_list(result)
        assert result.value_type.equals(pa.float64())


class TestMergeStructTypes:
    """Tests for _merge_struct_types function."""
    
    def test_merge_disjoint_structs(self):
        """Test merging structs with no common fields."""
        s1 = pa.struct([pa.field("a", pa.int64())])
        s2 = pa.struct([pa.field("b", pa.string())])
        result = _merge_struct_types(s1, s2)
        assert len(list(result)) == 2
    
    def test_merge_overlapping_structs(self):
        """Test merging structs with common fields."""
        s1 = pa.struct([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        s2 = pa.struct([pa.field("id", pa.int64()), pa.field("email", pa.string())])
        result = _merge_struct_types(s1, s2)
        field_names = [f.name for f in result]
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names


class TestInferSchemaFromRecords:
    """Tests for infer_schema_from_records function."""
    
    def test_empty_records(self):
        """Test with empty records list."""
        schema = infer_schema_from_records([])
        assert len(schema) == 0
    
    def test_single_record(self):
        """Test with single record."""
        records = [{"id": 1, "name": "Alice"}]
        schema = infer_schema_from_records(records)
        assert len(schema) == 2
        field_names = [f.name for f in schema]
        assert "id" in field_names
        assert "name" in field_names
    
    def test_multiple_records_same_schema(self):
        """Test with multiple records having same schema."""
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        schema = infer_schema_from_records(records)
        assert len(schema) == 2
    
    def test_records_with_nulls(self):
        """Test with records containing null values."""
        records = [
            {"id": 1, "name": None},
            {"id": 2, "name": "Bob"},
        ]
        schema = infer_schema_from_records(records)
        # Name should be inferred as string from non-null value
        name_field = schema.field("name")
        assert name_field.type.equals(pa.string())
    
    def test_records_all_null_field(self):
        """Test when a field is null in all records."""
        records = [
            {"id": 1, "maybe": None},
            {"id": 2, "maybe": None},
        ]
        schema = infer_schema_from_records(records)
        # All-null fields default to string
        maybe_field = schema.field("maybe")
        assert maybe_field.type.equals(pa.string())
    
    def test_nested_records(self):
        """Test with nested record structure."""
        records = [
            {"id": 1, "meta": {"active": True}},
            {"id": 2, "meta": {"active": False, "score": 95}},
        ]
        schema = infer_schema_from_records(records)
        meta_field = schema.field("meta")
        assert pa.types.is_struct(meta_field.type)


class TestGetSampleIndices:
    """Tests for _get_sample_indices function."""
    
    def test_sample_smaller_than_total(self):
        """Test when sample size is smaller than total."""
        indices = _get_sample_indices(10, 3)
        assert len(indices) == 3
        assert 0 in indices  # First
        assert 9 in indices  # Last
    
    def test_sample_larger_than_total(self):
        """Test when sample size is larger than total."""
        indices = _get_sample_indices(3, 10)
        assert indices == [0, 1, 2]
    
    def test_sample_equals_total(self):
        """Test when sample size equals total."""
        indices = _get_sample_indices(5, 5)
        assert indices == [0, 1, 2, 3, 4]


class TestConvertValue:
    """Tests for _convert_value function."""
    
    def test_convert_none(self):
        """Test converting None."""
        assert _convert_value(None, pa.string()) is None
    
    def test_convert_to_string(self):
        """Test converting to string."""
        assert _convert_value(42, pa.string()) == "42"
        assert _convert_value(True, pa.string()) == "True"
    
    def test_convert_to_int(self):
        """Test converting to integer."""
        assert _convert_value("42", pa.int64()) == 42
        assert _convert_value(42.9, pa.int64()) == 42
    
    def test_convert_to_int_invalid(self):
        """Test converting invalid value to int returns None."""
        assert _convert_value("not a number", pa.int64()) is None
    
    def test_convert_to_float(self):
        """Test converting to float."""
        assert _convert_value("3.14", pa.float64()) == 3.14
        assert _convert_value(42, pa.float64()) == 42.0
    
    def test_convert_to_bool(self):
        """Test converting to boolean."""
        assert _convert_value(True, pa.bool_()) == True
        assert _convert_value("true", pa.bool_()) == True
        assert _convert_value("yes", pa.bool_()) == True
        assert _convert_value("false", pa.bool_()) == False
    
    def test_convert_struct(self):
        """Test converting dict to struct."""
        target = pa.struct([pa.field("name", pa.string())])
        result = _convert_value({"name": "Alice"}, target)
        assert result == {"name": "Alice"}
    
    def test_convert_struct_non_dict(self):
        """Test converting non-dict to struct returns None."""
        target = pa.struct([pa.field("name", pa.string())])
        result = _convert_value("not a dict", target)
        assert result is None
    
    def test_convert_list(self):
        """Test converting list."""
        target = pa.list_(pa.int64())
        result = _convert_value([1, 2, 3], target)
        assert result == [1, 2, 3]
    
    def test_convert_list_non_list(self):
        """Test converting non-list to list returns None."""
        target = pa.list_(pa.int64())
        result = _convert_value("not a list", target)
        assert result is None


class TestConvertRecordToArrowRow:
    """Tests for convert_record_to_arrow_row function."""
    
    def test_basic_conversion(self):
        """Test basic record conversion."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        record = {"id": 1, "name": "Alice"}
        result = convert_record_to_arrow_row(record, schema)
        assert result == {"id": 1, "name": "Alice"}
    
    def test_missing_fields(self):
        """Test conversion with missing fields."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        record = {"id": 1}  # Missing 'name'
        result = convert_record_to_arrow_row(record, schema)
        assert result["id"] == 1
        assert result["name"] is None


class TestRecordsToArrowTable:
    """Tests for records_to_arrow_table function."""
    
    def test_empty_records(self):
        """Test with empty records."""
        table = records_to_arrow_table([])
        assert len(table) == 0
    
    def test_empty_records_with_schema(self):
        """Test empty records with pre-defined schema."""
        schema = pa.schema([pa.field("id", pa.int64())])
        table = records_to_arrow_table([], schema=schema)
        assert len(table) == 0
        assert "id" in table.column_names
    
    def test_basic_records(self):
        """Test with basic records."""
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        table = records_to_arrow_table(records)
        assert len(table) == 2
        assert "id" in table.column_names
        assert "name" in table.column_names
    
    def test_nested_records(self):
        """Test with nested records."""
        records = [
            {"id": 1, "user": {"name": "Alice"}},
            {"id": 2, "user": {"name": "Bob"}},
        ]
        table = records_to_arrow_table(records)
        assert len(table) == 2


class TestSchemaChange:
    """Tests for SchemaChange class."""
    
    def test_repr_added(self):
        """Test repr for added field."""
        change = SchemaChange("added", "email", new_type=pa.string())
        assert "ADDED" in repr(change)
        assert "email" in repr(change)
    
    def test_repr_removed(self):
        """Test repr for removed field."""
        change = SchemaChange("removed", "old_field", old_type=pa.int64())
        assert "REMOVED" in repr(change)
        assert "old_field" in repr(change)
    
    def test_repr_changed(self):
        """Test repr for changed field."""
        change = SchemaChange("type_changed", "id", old_type=pa.int64(), new_type=pa.string())
        assert "CHANGED" in repr(change)
        assert "id" in repr(change)


class TestDetectSchemaChanges:
    """Tests for detect_schema_changes function."""
    
    def test_no_changes(self):
        """Test with identical schemas."""
        schema = pa.schema([pa.field("id", pa.int64())])
        changes = detect_schema_changes(schema, schema)
        assert len(changes) == 0
    
    def test_added_field(self):
        """Test detecting added field."""
        old = pa.schema([pa.field("id", pa.int64())])
        new = pa.schema([pa.field("id", pa.int64()), pa.field("email", pa.string())])
        changes = detect_schema_changes(old, new)
        added = [c for c in changes if c.change_type == "added"]
        assert len(added) == 1
        assert added[0].field_name == "email"
    
    def test_removed_field(self):
        """Test detecting removed field."""
        old = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        new = pa.schema([pa.field("id", pa.int64())])
        changes = detect_schema_changes(old, new)
        removed = [c for c in changes if c.change_type == "removed"]
        assert len(removed) == 1
        assert removed[0].field_name == "name"
    
    def test_type_changed(self):
        """Test detecting type change."""
        old = pa.schema([pa.field("value", pa.int64())])
        new = pa.schema([pa.field("value", pa.string())])
        changes = detect_schema_changes(old, new)
        changed = [c for c in changes if c.change_type == "type_changed"]
        assert len(changed) == 1
        assert changed[0].field_name == "value"


class TestEvolveSchema:
    """Tests for evolve_schema function."""
    
    def test_add_new_field(self):
        """Test evolving schema with new field."""
        old = pa.schema([pa.field("id", pa.int64())])
        new = pa.schema([pa.field("id", pa.int64()), pa.field("email", pa.string())])
        evolved = evolve_schema(old, new)
        assert "id" in [f.name for f in evolved]
        assert "email" in [f.name for f in evolved]
    
    def test_keep_removed_field(self):
        """Test that removed fields are kept in evolved schema."""
        old = pa.schema([pa.field("id", pa.int64()), pa.field("legacy", pa.string())])
        new = pa.schema([pa.field("id", pa.int64())])
        evolved = evolve_schema(old, new)
        assert "legacy" in [f.name for f in evolved]
    
    def test_type_widening(self):
        """Test type widening (int -> float)."""
        old = pa.schema([pa.field("value", pa.int64())])
        new = pa.schema([pa.field("value", pa.float64())])
        evolved = evolve_schema(old, new, allow_type_changes=True)
        assert evolved.field("value").type.equals(pa.float64())
    
    def test_no_type_changes(self):
        """Test preserving old types when type changes not allowed."""
        old = pa.schema([pa.field("value", pa.int64())])
        new = pa.schema([pa.field("value", pa.float64())])
        evolved = evolve_schema(old, new, allow_type_changes=False)
        assert evolved.field("value").type.equals(pa.int64())


class TestSchemaHash:
    """Tests for schema_hash function."""
    
    def test_same_schema_same_hash(self):
        """Test that identical schemas produce same hash."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        hash1 = schema_hash(schema)
        hash2 = schema_hash(schema)
        assert hash1 == hash2
    
    def test_different_schema_different_hash(self):
        """Test that different schemas produce different hashes."""
        schema1 = pa.schema([pa.field("id", pa.int64())])
        schema2 = pa.schema([pa.field("id", pa.string())])
        assert schema_hash(schema1) != schema_hash(schema2)
    
    def test_order_independent(self):
        """Test that field order doesn't affect hash."""
        schema1 = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        schema2 = pa.schema([pa.field("b", pa.string()), pa.field("a", pa.int64())])
        # Hashes should be same because we sort parts
        assert schema_hash(schema1) == schema_hash(schema2)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
