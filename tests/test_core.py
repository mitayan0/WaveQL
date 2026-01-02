"""
Tests for WaveQL core functionality
"""

import pytest
import pyarrow as pa
import tempfile
import csv
from pathlib import Path

import waveql
from waveql.query_planner import QueryPlanner, Predicate

from waveql.schema_cache import SchemaCache, ColumnInfo
from waveql.adapters.base import BaseAdapter
from typing import Any, List


class TestQueryPlanner:
    """Tests for SQL parsing and predicate extraction."""
    
    def test_parse_simple_select(self):
        planner = QueryPlanner()
        info = planner.parse("SELECT * FROM users")
        
        assert info.operation == "SELECT"
        assert info.table == "users"
        assert info.columns == ["*"]
    
    def test_parse_select_with_columns(self):
        planner = QueryPlanner()
        info = planner.parse("SELECT id, name, email FROM users")
        
        assert info.columns == ["id", "name", "email"]
        assert info.table == "users"
    
    def test_parse_select_with_where(self):
        planner = QueryPlanner()
        info = planner.parse("SELECT * FROM users WHERE status = 'active'")
        
        assert len(info.predicates) == 1
        assert info.predicates[0].column == "status"
        assert info.predicates[0].operator == "="
        assert info.predicates[0].value == "active"
    
    def test_parse_select_with_limit_offset(self):
        planner = QueryPlanner()
        info = planner.parse("SELECT * FROM users LIMIT 10 OFFSET 20")
        
        assert info.limit == 10
        assert info.offset == 20
    
    def test_parse_insert(self):
        planner = QueryPlanner()
        info = planner.parse("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')")
        
        assert info.operation == "INSERT"
        assert info.table == "users"
        assert info.values.get("name") == "John"
        assert info.values.get("email") == "john@example.com"
    
    def test_parse_update(self):
        planner = QueryPlanner()
        info = planner.parse("UPDATE users SET status = 'inactive' WHERE id = 123")
        
        assert info.operation == "UPDATE"
        assert info.table == "users"
        assert info.values.get("status") == "inactive"
        assert len(info.predicates) == 1
    
    def test_parse_delete(self):
        planner = QueryPlanner()
        info = planner.parse("DELETE FROM users WHERE id = 456")
        
        assert info.operation == "DELETE"
        assert info.table == "users"
        assert info.predicates[0].value == 456


class TestSchemaCache:
    """Tests for schema caching."""
    
    def test_cache_set_get(self):
        cache = SchemaCache()
        columns = [
            ColumnInfo(name="id", data_type="integer", nullable=False, primary_key=True),
            ColumnInfo(name="name", data_type="string", nullable=True),
        ]
        
        cache.set("test_adapter", "users", columns)
        schema = cache.get("test_adapter", "users")
        
        assert schema is not None
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "id"
        
        cache.close()
    
    def test_cache_list_tables(self):
        cache = SchemaCache()
        
        cache.set("adapter1", "table1", [ColumnInfo(name="col", data_type="string")])
        cache.set("adapter1", "table2", [ColumnInfo(name="col", data_type="string")])
        cache.set("adapter2", "table3", [ColumnInfo(name="col", data_type="string")])
        
        tables = cache.list_tables("adapter1")
        assert len(tables) == 2
        assert "table1" in tables
        assert "table2" in tables
        
        cache.close()
    
    def test_cache_invalidate(self):
        cache = SchemaCache()
        cache.set("test", "table1", [ColumnInfo(name="col", data_type="string")])
        
        cache.invalidate("test", "table1")
        schema = cache.get("test", "table1")
        
        assert schema is None
        cache.close()


class TestFileAdapter:
    """Tests for CSV/Parquet file adapter."""
    
    def test_csv_read(self):
        # Create temp CSV - must close file before DuckDB can read on Windows
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
        temp_path = temp_file.name
        writer = csv.writer(temp_file)
        writer.writerow(["id", "name", "value"])
        writer.writerow([1, "Alice", 100])
        writer.writerow([2, "Bob", 200])
        writer.writerow([3, "Charlie", 300])
        temp_file.flush()  # Ensure data is written
        temp_file.close()  # Must close before DuckDB reads
        
        try:
            conn = waveql.connect(f"file://{temp_path}")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM data")
            
            result = cursor.to_arrow()
            assert len(result) == 3
            
            conn.close()
        finally:
            Path(temp_path).unlink()
    
    def test_csv_with_predicate(self):
        # Must close file before DuckDB can read on Windows
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
        temp_path = temp_file.name
        writer = csv.writer(temp_file)
        writer.writerow(["id", "name", "value"])
        writer.writerow([1, "Alice", 100])
        writer.writerow([2, "Bob", 200])
        writer.writerow([3, "Charlie", 300])
        temp_file.flush()  # Ensure data is written
        temp_file.close()  # Must close before DuckDB reads
        
        try:
            conn = waveql.connect(f"file://{temp_path}")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM data WHERE value > 150")
            
            result = cursor.fetchall()
            assert len(result) == 2  # Bob and Charlie
            
            conn.close()
        finally:
            Path(temp_path).unlink()


class TestConnection:
    """Tests for WaveQL connection."""
    
    def test_connection_context_manager(self):
        # Must close file before DuckDB can read on Windows
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
        temp_path = temp_file.name
        writer = csv.writer(temp_file)
        writer.writerow(["col1"])
        writer.writerow(["val1"])
        temp_file.flush()  # Ensure data is written
        temp_file.close()  # Must close before DuckDB reads
        
        try:
            with waveql.connect(f"file://{temp_path}") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM data")
                result = cursor.fetchall()
                assert len(result) == 1
        finally:
            Path(temp_path).unlink()
    
    def test_cursor_iteration(self):
        # Must close file before DuckDB can read on Windows
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
        temp_path = temp_file.name
        writer = csv.writer(temp_file)
        writer.writerow(["id"])
        for i in range(5):
            writer.writerow([i])
        temp_file.flush()  # Ensure data is written
        temp_file.close()  # Must close before DuckDB reads
        
        try:
            conn = waveql.connect(f"file://{temp_path}")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM data")
            
            count = 0
            for row in cursor:
                count += 1
            
            assert count == 5
            conn.close()
        finally:
            Path(temp_path).unlink()


class TestPredicate:
    """Tests for predicate to API filter conversion."""
    
    def test_predicate_to_servicenow(self):
        pred = Predicate(column="priority", operator="=", value=1)
        snq = pred.to_api_filter("servicenow")
        assert snq == "priority=1"
    
    def test_predicate_like(self):
        pred = Predicate(column="name", operator="LIKE", value="%test%")
        snq = pred.to_api_filter("servicenow")
        assert "LIKE" in snq


class TestAggregationFallback:
    """Test fallback logic when adapter rejects aggregation."""
    
    class FallbackAdapter(BaseAdapter):
        adapter_name = "fallback"
        supports_predicate_pushdown = True
        
        def fetch(self, table, columns=None, predicates=None, limit=None, offset=None, order_by=None, group_by=None, aggregates=None):
            if group_by or aggregates:
                raise NotImplementedError("Aggregates not supported")
            
            # Return raw data: groups A, A, B
            data = [
                {"grp": "A", "val": 10},
                {"grp": "A", "val": 20},
                {"grp": "B", "val": 5},
            ]
            return pa.Table.from_pylist(data)

        def get_schema(self, table):
            return [ColumnInfo("grp", "string"), ColumnInfo("val", "integer")]

    def test_fallback_execution(self):
        # We access the class directly for testing
        conn = waveql.connect()
        adapter = self.FallbackAdapter(host="dummy")
        # Inject adapter
        conn._adapters = {"fallback": adapter}
        
        cursor = conn.cursor()
        
        # 1. Simple Aggregation (SUM) 
        # Note: We must implement SUM in fallback logic (DuckDB handles it)
        cursor.execute("SELECT SUM(val) FROM fallback.Table")
        row = cursor.fetchone()
        assert row[0] == 35 # 10+20+5
        
        # 2. Group By
        # Note: Order by ensures deterministic result order
        cursor.execute("SELECT grp, SUM(val) as s FROM fallback.Table GROUP BY grp ORDER BY grp")
        rows = cursor.fetchall()
        # Expect A: 30, B: 5
        assert rows[0][0] == "A"
        assert rows[0][1] == 30
        assert rows[1][0] == "B"
        assert rows[1][1] == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
