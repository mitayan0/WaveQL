"""
Tests for the schema inference utility with native struct support.
"""

import pytest
import pyarrow as pa

from waveql.utils.schema import (
    infer_arrow_type,
    merge_arrow_types,
    infer_schema_from_records,
    records_to_arrow_table,
)


class TestInferArrowType:
    """Test type inference for individual values."""
    
    def test_infer_null(self):
        assert infer_arrow_type(None) == pa.null()
    
    def test_infer_bool(self):
        assert infer_arrow_type(True) == pa.bool_()
        assert infer_arrow_type(False) == pa.bool_()
    
    def test_infer_int(self):
        assert infer_arrow_type(42) == pa.int64()
        assert infer_arrow_type(-100) == pa.int64()
    
    def test_infer_float(self):
        assert infer_arrow_type(3.14) == pa.float64()
        assert infer_arrow_type(-0.001) == pa.float64()
    
    def test_infer_string(self):
        assert infer_arrow_type("hello") == pa.string()
        assert infer_arrow_type("") == pa.string()
    
    def test_infer_nested_dict(self):
        """Test that nested dicts become struct types."""
        value = {"name": "Alice", "age": 30}
        result = infer_arrow_type(value)
        
        assert pa.types.is_struct(result)
        assert len(result) == 2
        
        # Check fields
        field_names = {f.name for f in result}
        assert field_names == {"name", "age"}
    
    def test_infer_deeply_nested_dict(self):
        """Test deeply nested structures."""
        value = {
            "user": {
                "profile": {
                    "name": "Alice",
                    "active": True
                },
                "score": 95
            }
        }
        result = infer_arrow_type(value)
        
        assert pa.types.is_struct(result)
        user_field = result.field("user")
        assert pa.types.is_struct(user_field.type)
    
    def test_infer_list(self):
        """Test list inference."""
        result = infer_arrow_type([1, 2, 3])
        assert pa.types.is_list(result)
        assert result.value_type == pa.int64()
    
    def test_infer_list_of_dicts(self):
        """Test list of nested objects."""
        value = [{"id": 1}, {"id": 2}]
        result = infer_arrow_type(value)
        
        assert pa.types.is_list(result)
        assert pa.types.is_struct(result.value_type)


class TestMergeArrowTypes:
    """Test type conflict resolution."""
    
    def test_same_type_returns_same(self):
        assert merge_arrow_types(pa.int64(), pa.int64()) == pa.int64()
        assert merge_arrow_types(pa.string(), pa.string()) == pa.string()
    
    def test_null_promotes_to_other(self):
        assert merge_arrow_types(pa.null(), pa.int64()) == pa.int64()
        assert merge_arrow_types(pa.string(), pa.null()) == pa.string()
    
    def test_int_float_promotes_to_float(self):
        assert merge_arrow_types(pa.int64(), pa.float64()) == pa.float64()
        assert merge_arrow_types(pa.float64(), pa.int64()) == pa.float64()
    
    def test_incompatible_types_fallback_to_string(self):
        assert merge_arrow_types(pa.int64(), pa.string()) == pa.string()
        assert merge_arrow_types(pa.bool_(), pa.int64()) == pa.string()


class TestInferSchemaFromRecords:
    """Test multi-record schema inference."""
    
    def test_simple_records(self):
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        schema = infer_schema_from_records(records)
        
        assert len(schema) == 2
        assert schema.field("id").type == pa.int64()
        assert schema.field("name").type == pa.string()
    
    def test_nested_records(self):
        """Test that nested objects become struct columns."""
        records = [
            {"id": 1, "user": {"name": "Alice", "active": True}},
            {"id": 2, "user": {"name": "Bob", "active": False}},
        ]
        schema = infer_schema_from_records(records)
        
        user_type = schema.field("user").type
        assert pa.types.is_struct(user_type)
        assert user_type.field("name").type == pa.string()
        assert user_type.field("active").type == pa.bool_()
    
    def test_null_in_some_records(self):
        """Test that null values don't prevent type inference."""
        records = [
            {"id": 1, "value": None},
            {"id": 2, "value": 42},
        ]
        schema = infer_schema_from_records(records, sample_size=2)
        
        # Should infer int64 from the non-null record
        # After merging null with int64
        value_type = schema.field("value").type
        assert value_type == pa.int64()
    
    def test_missing_fields_across_records(self):
        """Test records with different fields."""
        records = [
            {"id": 1, "a": "hello"},
            {"id": 2, "b": "world"},
        ]
        schema = infer_schema_from_records(records, sample_size=2)
        
        # Should have all fields from all records
        field_names = {f.name for f in schema}
        assert field_names == {"id", "a", "b"}


class TestRecordsToArrowTable:
    """Test the main conversion function."""
    
    def test_simple_conversion(self):
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        table = records_to_arrow_table(records)
        
        assert len(table) == 2
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("name").to_pylist() == ["Alice", "Bob"]
    
    def test_nested_struct_conversion(self):
        """The premium feature: nested objects become queryable structs."""
        records = [
            {"id": 1, "user": {"name": "Alice", "active": True}},
            {"id": 2, "user": {"name": "Bob", "active": False}},
        ]
        table = records_to_arrow_table(records)
        
        assert len(table) == 2
        
        # Verify struct column exists
        user_col = table.column("user")
        assert pa.types.is_struct(user_col.type)
        
        # Verify struct values
        user_data = user_col.to_pylist()
        assert user_data[0]["name"] == "Alice"
        assert user_data[0]["active"] == True
        assert user_data[1]["name"] == "Bob"
        assert user_data[1]["active"] == False
    
    def test_empty_records(self):
        table = records_to_arrow_table([])
        assert len(table) == 0
    
    def test_with_predefined_schema(self):
        """Test using a predefined schema."""
        records = [{"id": "1", "name": "Alice"}]  # id is string in data
        
        # But we want id as int
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        
        table = records_to_arrow_table(records, schema=schema)
        
        # Should coerce string "1" to int 1
        assert table.column("id").to_pylist() == [1]


class TestDuckDBIntegration:
    """Test that struct columns work with DuckDB dot-notation queries."""
    
    def test_dot_notation_query(self):
        """The ultimate test: can we query nested fields with dot notation?"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("DuckDB not available")
        
        records = [
            {"id": 1, "user": {"name": "Alice", "score": 95}},
            {"id": 2, "user": {"name": "Bob", "score": 87}},
        ]
        table = records_to_arrow_table(records)
        
        conn = duckdb.connect()
        conn.register("test_table", table)
        
        # Query using dot notation on struct column
        result = conn.execute("""
            SELECT id, user.name as username, user.score 
            FROM test_table 
            WHERE user.score > 90
        """).fetchall()
        
        assert len(result) == 1
        assert result[0] == (1, "Alice", 95)
        
        conn.close()
