"""
Tests for WaveQL adapters/file_adapter module.

This covers the 38% uncovered module waveql/adapters/file_adapter.py
"""

import pytest
import pyarrow as pa
from pathlib import Path

from waveql.adapters.file_adapter import FileAdapter
from waveql.query_planner import Predicate
from waveql.exceptions import AdapterError, QueryError


class TestFileAdapterInit:
    """Tests for FileAdapter initialization."""
    
    def test_init_with_file_path(self, tmp_path):
        """Test initialization with file path."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob")
        
        adapter = FileAdapter(host=str(csv_file))
        
        assert adapter.adapter_name == "file"
        assert adapter._path == Path(csv_file)
    
    def test_init_with_directory(self, tmp_path):
        """Test initialization with directory path."""
        adapter = FileAdapter(host=str(tmp_path))
        
        assert adapter._path == Path(tmp_path)
    
    def test_init_explicit_file_type(self, tmp_path):
        """Test initialization with explicit file type."""
        adapter = FileAdapter(
            host=str(tmp_path),
            file_type="parquet",
        )
        
        assert adapter._file_type == "parquet"
    
    def test_detect_file_type_csv(self, tmp_path):
        """Test file type detection for CSV."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice")
        
        adapter = FileAdapter(host=str(csv_file))
        
        assert adapter._file_type == "csv"
    
    def test_detect_file_type_parquet(self, tmp_path):
        """Test file type detection for Parquet."""
        parquet_file = tmp_path / "data.parquet"
        table = pa.table({"id": [1, 2], "name": ["A", "B"]})
        import pyarrow.parquet as pq
        pq.write_table(table, str(parquet_file))
        
        adapter = FileAdapter(host=str(parquet_file))
        
        assert adapter._file_type == "parquet"
    
    def test_detect_file_type_json(self, tmp_path):
        """Test file type detection for JSON."""
        json_file = tmp_path / "data.json"
        json_file.write_text('[{"id": 1, "name": "Alice"}]')
        
        adapter = FileAdapter(host=str(json_file))
        
        assert adapter._file_type == "json"


class TestFileAdapterFetch:
    """Tests for FileAdapter fetch method."""
    
    @pytest.fixture
    def csv_file(self, tmp_path):
        """Create a test CSV file."""
        csv_path = tmp_path / "test_data.csv"
        csv_path.write_text(
            "id,name,age,city\n"
            "1,Alice,30,NYC\n"
            "2,Bob,25,LA\n"
            "3,Charlie,35,SF\n"
        )
        return csv_path
    
    @pytest.fixture
    def parquet_file(self, tmp_path):
        """Create a test Parquet file."""
        parquet_path = tmp_path / "test_data.parquet"
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        })
        import pyarrow.parquet as pq
        pq.write_table(table, str(parquet_path))
        return parquet_path
    
    def test_fetch_csv_all(self, csv_file):
        """Test fetching all data from CSV."""
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(table="test_data.csv")
        
        assert isinstance(result, pa.Table)
        assert len(result) == 3
    
    def test_fetch_csv_with_columns(self, csv_file):
        """Test fetching specific columns from CSV."""
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(
            table="test_data.csv",
            columns=["id", "name"],
        )
        
        assert "id" in result.column_names
        assert "name" in result.column_names
        # Other columns may or may not be included depending on implementation
    
    def test_fetch_csv_with_limit(self, csv_file):
        """Test fetching with limit."""
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(
            table="test_data.csv",
            limit=2,
        )
        
        assert len(result) == 2
    
    def test_fetch_csv_with_offset(self, csv_file):
        """Test fetching with offset."""
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(
            table="test_data.csv",
            offset=1,
        )
        
        assert len(result) == 2  # Skip first row
    
    def test_fetch_csv_with_predicates(self, csv_file):
        """Test fetching with predicates."""
        adapter = FileAdapter(host=str(csv_file))
        
        predicates = [
            Predicate(column="age", operator=">", value=25),
        ]
        
        result = adapter.fetch(
            table="test_data.csv",
            predicates=predicates,
        )
        
        assert len(result) == 2  # Alice and Charlie
    
    def test_fetch_parquet(self, parquet_file):
        """Test fetching from Parquet file."""
        adapter = FileAdapter(host=str(parquet_file))
        
        result = adapter.fetch(table="test_data.parquet")
        
        assert isinstance(result, pa.Table)
        assert len(result) == 3
    
    def test_fetch_with_order_by(self, csv_file):
        """Test fetching with ORDER BY."""
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(
            table="test_data.csv",
            order_by=[("age", "DESC")],
        )
        
        # First row should be Charlie (age 35)
        ages = result.column("age").to_pylist()
        assert ages == [35, 30, 25]
    
    def test_fetch_async(self, csv_file):
        """Test async fetch."""
        import anyio
        
        async def _run_test():
            adapter = FileAdapter(host=str(csv_file))
            
            result = await adapter.fetch_async(table="test_data.csv")
            
            # Async wrapper returns sync result
            assert isinstance(result, pa.Table)
            
        anyio.run(_run_test)


class TestFileAdapterResolve:
    """Tests for path resolution."""
    
    def test_resolve_path_file(self, tmp_path):
        """Test resolving path when host is file."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id\n1")
        
        adapter = FileAdapter(host=str(csv_file))
        
        resolved = adapter._resolve_path("data.csv")
        assert resolved == str(csv_file)
    
    def test_resolve_path_directory(self, tmp_path):
        """Test resolving path when host is directory."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id\n1")
        
        adapter = FileAdapter(host=str(tmp_path))
        
        resolved = adapter._resolve_path("data.csv")
        assert resolved == str(csv_file)
    
    def test_resolve_path_not_found(self, tmp_path):
        """Test resolving non-existent path."""
        adapter = FileAdapter(host=str(tmp_path))
        
        with pytest.raises((FileNotFoundError, AdapterError, QueryError)):
            adapter._resolve_path("nonexistent.csv")


class TestFileAdapterBuildQuery:
    """Tests for query building."""
    
    def test_build_query_basic(self, tmp_path):
        """Test building basic query."""
        adapter = FileAdapter(host=str(tmp_path))
        
        query = adapter._build_query(
            file_path="test.csv",
            columns=None,
            predicates=None,
            limit=None,
            offset=None,
            order_by=None,
        )
        
        assert "test.csv" in query
        assert "SELECT" in query.upper()
    
    def test_build_query_with_columns(self, tmp_path):
        """Test building query with columns."""
        adapter = FileAdapter(host=str(tmp_path))
        
        query = adapter._build_query(
            file_path="test.csv",
            columns=["id", "name"],
            predicates=None,
            limit=None,
            offset=None,
            order_by=None,
        )
        
        assert "id" in query
        assert "name" in query
    
    def test_build_query_with_predicates(self, tmp_path):
        """Test building query with predicates."""
        adapter = FileAdapter(host=str(tmp_path))
        
        predicates = [
            Predicate(column="age", operator=">", value=25),
        ]
        
        query = adapter._build_query(
            file_path="test.csv",
            columns=None,
            predicates=predicates,
            limit=None,
            offset=None,
            order_by=None,
        )
        
        assert "WHERE" in query.upper()
        assert "age" in query
    
    def test_build_query_with_limit_offset(self, tmp_path):
        """Test building query with limit and offset."""
        adapter = FileAdapter(host=str(tmp_path))
        
        query = adapter._build_query(
            file_path="test.csv",
            columns=None,
            predicates=None,
            limit=10,
            offset=5,
            order_by=None,
        )
        
        assert "LIMIT" in query.upper()
        assert "OFFSET" in query.upper()
    
    def test_build_query_with_order_by(self, tmp_path):
        """Test building query with ORDER BY."""
        adapter = FileAdapter(host=str(tmp_path))
        
        query = adapter._build_query(
            file_path="test.csv",
            columns=None,
            predicates=None,
            limit=None,
            offset=None,
            order_by=[("created_at", "DESC")],
        )
        
        assert "ORDER BY" in query.upper()
    
    
    def test_build_query_with_group_by(self, tmp_path):
        """Test building query with GROUP BY."""
        from waveql.query_planner import Aggregate
        adapter = FileAdapter(host=str(tmp_path))
        
        query = adapter._build_query(
            file_path="test.csv",
            columns=["category"],
            predicates=None,
            limit=None,
            offset=None,
            order_by=None,
            group_by=["category"],
            aggregates=[Aggregate(func="COUNT", column="*")],
        )
        
        assert "GROUP BY" in query.upper()


class TestFileAdapterSchema:
    """Tests for schema discovery."""
    
    def test_get_schema_csv(self, tmp_path):
        """Test getting schema from CSV."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,age\n1,Alice,30")
        
        adapter = FileAdapter(host=str(csv_file))
        
        schema = adapter.get_schema("data.csv")
        
        assert schema is not None
        assert len(schema) >= 3
    
    def test_get_schema_parquet(self, tmp_path):
        """Test getting schema from Parquet."""
        parquet_file = tmp_path / "data.parquet"
        table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["A", "B"], type=pa.string()),
        })
        import pyarrow.parquet as pq
        pq.write_table(table, str(parquet_file))
        
        adapter = FileAdapter(host=str(parquet_file))
        
        schema = adapter.get_schema("data.parquet")
        
        assert schema is not None
    
    def test_get_schema_async(self, tmp_path):
        """Test async schema retrieval."""
        import anyio
        
        async def _run():
            csv_file = tmp_path / "data.csv"
            csv_file.write_text("id,name\n1,Alice")
            
            adapter = FileAdapter(host=str(csv_file))
            
            schema = await adapter.get_schema_async("data.csv")
            
            assert schema is not None
            
        anyio.run(_run)


class TestFileAdapterInsert:
    """Tests for insert/append operations."""
    
    def test_insert_to_csv(self, tmp_path):
        """Test inserting/appending to CSV."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice")
        
        adapter = FileAdapter(host=str(csv_file))
        
        adapter.insert(
            table="data.csv",
            values={"id": 2, "name": "Bob"},
        )
        
        # Verify insert
        content = csv_file.read_text()
        assert "2" in content
        assert "Bob" in content
    
    def test_insert_async(self, tmp_path):
        """Test async insert."""
        import anyio
        
        async def _run():
            csv_file = tmp_path / "data.csv"
            csv_file.write_text("id,name\n1,Alice")
            
            adapter = FileAdapter(host=str(csv_file))
            
            await adapter.insert_async(
                table="data.csv",
                values={"id": 3, "name": "Charlie"},
            )
            
        anyio.run(_run)


class TestFileAdapterListTables:
    """Tests for listing tables (files)."""
    
    def test_list_tables_directory(self, tmp_path):
        """Test listing tables in directory."""
        (tmp_path / "data1.csv").write_text("id\n1")
        (tmp_path / "data2.csv").write_text("id\n2")
        (tmp_path / "data3.parquet").write_bytes(b"")  # Empty file
        
        adapter = FileAdapter(host=str(tmp_path))
        
        tables = adapter.list_tables()
        
        assert isinstance(tables, list)
        assert len(tables) >= 2
    
    def test_list_tables_single_file(self, tmp_path):
        """Test listing tables when host is single file."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id\n1")
        
        adapter = FileAdapter(host=str(csv_file))
        
        tables = adapter.list_tables()
        
        assert isinstance(tables, list)
        assert "data.csv" in tables or len(tables) >= 1


class TestFileAdapterEdgeCases:
    """Edge case tests for FileAdapter."""
    
    def test_empty_csv(self, tmp_path):
        """Test handling empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("id,name\n")  # Headers only
        
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(table="empty.csv")
        
        assert len(result) == 0
    
    def test_csv_with_special_chars(self, tmp_path):
        """Test CSV with special characters."""
        csv_file = tmp_path / "special.csv"
        csv_file.write_text('id,name\n1,"Hello, World"\n2,"Test ""quoted"""\n')
        
        adapter = FileAdapter(host=str(csv_file))
        
        result = adapter.fetch(table="special.csv")
        
        assert len(result) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
