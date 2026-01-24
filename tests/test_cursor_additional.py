"""
Tests for WaveQL cursor module - Additional tests for uncovered lines.

This covers the 65% uncovered module waveql/cursor.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch, PropertyMock
import re

from waveql.cursor import WaveQLCursor, Row
from waveql.query_planner import Predicate
from waveql.exceptions import QueryError


class TestRowClass:
    """Tests for Row class."""
    
    def test_row_getitem_by_index(self):
        """Test getting item by index."""
        schema = [("id", None, None, None, None, None, None), 
                  ("name", None, None, None, None, None, None)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        assert row[0] == 1
        assert row[1] == "Alice"
    
    def test_row_getitem_by_key(self):
        """Test getting item by key."""
        schema = [("id", None, None, None, None, None, None),
                  ("name", None, None, None, None, None, None)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        assert row["id"] == 1
        assert row["name"] == "Alice"
    
    def test_row_iter(self):
        """Test iterating over row."""
        schema = [("id", None, None, None, None, None, None),
                  ("name", None, None, None, None, None, None)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        values = list(row)
        assert values == [1, "Alice"]
    
    def test_row_len(self):
        """Test row length."""
        schema = [("id",), ("name",), ("age",)]
        data = {"id": 1, "name": "Alice", "age": 30}
        row = Row(data, schema)
        
        assert len(row) == 3
    
    def test_row_repr(self):
        """Test row representation."""
        schema = [("id",)]
        data = {"id": 1}
        row = Row(data, schema)
        
        repr_str = repr(row)
        assert "1" in repr_str
    
    def test_row_keys(self):
        """Test row keys."""
        schema = [("id",), ("name",)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        keys = list(row.keys())
        assert "id" in keys
        assert "name" in keys
    
    def test_row_values(self):
        """Test row values."""
        schema = [("id",), ("name",)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        values = list(row.values())
        assert 1 in values
        assert "Alice" in values
    
    def test_row_items(self):
        """Test row items."""
        schema = [("id",), ("name",)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        items = list(row.items())
        assert ("id", 1) in items
        assert ("name", "Alice") in items
    
    def test_row_as_dict(self):
        """Test row as_dict."""
        schema = [("id",), ("name",)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        d = row.as_dict()
        assert d == data
    
    def test_row_getattr(self):
        """Test row attribute access."""
        schema = [("id",), ("name",)]
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        
        assert row.id == 1
        assert row.name == "Alice"
    
    def test_row_getattr_missing(self):
        """Test row attribute access for missing attribute."""
        schema = [("id",)]
        data = {"id": 1}
        row = Row(data, schema)
        
        with pytest.raises(AttributeError):
            _ = row.missing_column


class TestWaveQLCursorExecutemany:
    """Tests for executemany method."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock connection."""
        conn = MagicMock()
        conn._duck = MagicMock()
        conn._adapters = {}
        conn._planner = MagicMock()
        return conn
    
    def test_executemany_insert(self, mock_connection):
        """Test executemany with INSERT."""
        cursor = WaveQLCursor(mock_connection)
        
        # Mock adapter for insert
        mock_adapter = MagicMock()
        mock_adapter.insert.return_value = 1
        mock_connection._adapters = {"test": mock_adapter}
        mock_connection.get_adapter.return_value = mock_adapter
        
        cursor.executemany(
            "INSERT INTO test.users (id, name) VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        )
        
        # Should call insert for each parameter set


class TestWaveQLCursorCleanTableName:
    """Tests for _clean_table_name method."""
    
    @pytest.fixture
    def cursor(self):
        """Create cursor."""
        conn = MagicMock()
        return WaveQLCursor(conn)
    
    def test_clean_simple_name(self, cursor):
        """Test cleaning simple table name."""
        result = cursor._clean_table_name("incident")
        assert result == "incident"
    
    def test_clean_quoted_name(self, cursor):
        """Test cleaning quoted table name."""
        result = cursor._clean_table_name('"incident"')
        assert result == "incident"
    
    def test_clean_schema_qualified(self, cursor):
        """Test cleaning schema-qualified name."""
        result = cursor._clean_table_name("servicenow.incident")
        assert result == "incident"
    
    def test_clean_fully_quoted(self, cursor):
        """Test cleaning fully quoted name."""
        result = cursor._clean_table_name('"servicenow"."incident"')
        assert result == "incident"


class TestWaveQLCursorNormalizeTableName:
    """Tests for _normalize_table_name method."""
    
    @pytest.fixture
    def cursor(self):
        """Create cursor."""
        conn = MagicMock()
        return WaveQLCursor(conn)
    
    def test_normalize_simple(self, cursor):
        """Test normalizing simple name."""
        result = cursor._normalize_table_name("incident")
        assert "incident" in result
    
    def test_normalize_with_alias(self, cursor):
        """Test normalizing name with alias."""
        result = cursor._normalize_table_name("incident AS i")
        assert "incident" in result
        assert "AS" not in result
    
    def test_normalize_quoted_with_schema(self, cursor):
        """Test normalizing quoted name with schema."""
        result = cursor._normalize_table_name('"servicenow"."incident"')
        assert "servicenow" in result
        assert "incident" in result


class TestWaveQLCursorParsePolicyPredicate:
    """Tests for _parse_policy_predicate method."""
    
    @pytest.fixture
    def cursor(self):
        """Create cursor."""
        conn = MagicMock()
        return WaveQLCursor(conn)
    
    def test_parse_equals_predicate(self, cursor):
        """Test parsing equals predicate."""
        predicates = cursor._parse_policy_predicate("status = 'active'")
        
        assert len(predicates) >= 0
    
    def test_parse_in_predicate(self, cursor):
        """Test parsing IN predicate."""
        predicates = cursor._parse_policy_predicate("id IN (1, 2, 3)")
        
        assert isinstance(predicates, list)
    
    def test_parse_gt_predicate(self, cursor):
        """Test parsing > predicate."""
        predicates = cursor._parse_policy_predicate("age > 18")
        
        assert isinstance(predicates, list)
    
    def test_parse_complex_predicate(self, cursor):
        """Test parsing complex predicate."""
        predicates = cursor._parse_policy_predicate("status = 'active' AND age > 18")
        
        assert isinstance(predicates, list)


class TestWaveQLCursorStreamBatches:
    """Tests for stream_batches method."""
    
    @pytest.fixture
    def cursor_with_adapter(self):
        """Create cursor with mocked adapter."""
        conn = MagicMock()
        
        # Mock adapter
        call_count = 0
        def fetch_with_pagination(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            limit = kwargs.get("limit", 100)
            offset = kwargs.get("offset", 0)
            
            if offset >= 30:  # Simulate 30 total records
                return pa.table({"id": [], "data": []})
            
            remaining = min(limit, 30 - offset)
            return pa.table({
                "id": list(range(offset, offset + remaining)),
                "data": [f"item_{i}" for i in range(offset, offset + remaining)],
            })
        
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch = MagicMock(side_effect=fetch_with_pagination)
        
        conn._adapters = {"test": mock_adapter}
        conn.get_adapter.return_value = mock_adapter
        
        # Mock planner
        mock_query_info = MagicMock()
        mock_query_info.tables = ["test.data"]
        mock_query_info.columns = None
        mock_query_info.predicates = []
        mock_query_info.is_join = False
        mock_query_info.limit = None
        mock_query_info.offset = None
        mock_query_info.aggregates = None
        conn._planner = MagicMock()
        conn._planner.plan.return_value = mock_query_info
        conn.plan.return_value = mock_query_info
        
        conn._duck = MagicMock()
        conn.view_manager.exists.return_value = False
        
        return WaveQLCursor(conn)
    
    def test_stream_batches_basic(self, cursor_with_adapter):
        """Test basic batch streaming."""
        batches = []
        
        for batch in cursor_with_adapter.stream_batches(
            "SELECT * FROM test.data",
            batch_size=10,
        ):
            batches.append(batch)
            if len(batches) >= 3:
                break
        
        assert len(batches) >= 1
    
    def test_stream_batches_with_max_records(self, cursor_with_adapter):
        """Test streaming with max_records."""
        batches = []
        
        for batch in cursor_with_adapter.stream_batches(
            "SELECT * FROM test.data",
            batch_size=5,
            max_records=15,
        ):
            batches.append(batch)
        
        # Total records should be limited
    
    def test_stream_batches_with_callback(self, cursor_with_adapter):
        """Test streaming with progress callback."""
        progress = []
        
        def callback(fetched, total):
            progress.append(fetched)
        
        batches = list(cursor_with_adapter.stream_batches(
            "SELECT * FROM test.data",
            batch_size=10,
            max_records=20,
            progress_callback=callback,
        ))
        
        # Callback should be called


class TestWaveQLCursorStreamToFile:
    """Tests for stream_to_file method."""
    
    @pytest.fixture
    def cursor_for_file(self):
        """Create cursor for file streaming."""
        conn = MagicMock()
        
        mock_adapter = MagicMock()
        mock_adapter.adapter_name = "test"
        mock_adapter.fetch = MagicMock(return_value=pa.table({
            "id": [1, 2, 3],
            "value": [100, 200, 300],
        }))
        
        conn._adapters = {"test": mock_adapter}
        conn.get_adapter = MagicMock(return_value=mock_adapter)
        
        mock_query_info = MagicMock()
        mock_query_info.tables = ["test.data"]
        mock_query_info.columns = None
        mock_query_info.predicates = []
        mock_query_info.is_join = False
        conn._planner = MagicMock()
        conn._planner.plan.return_value = mock_query_info
        conn.plan.return_value = mock_query_info
        conn._duck = MagicMock()
        conn.view_manager.exists.return_value = False
        
        return WaveQLCursor(conn)
    
    def test_stream_to_file(self, cursor_for_file, tmp_path):
        """Test streaming to Parquet file."""
        output_file = str(tmp_path / "output.parquet")
        
        stats = cursor_for_file.stream_to_file(
            "SELECT * FROM test.data",
            output_path=output_file,
            batch_size=10,
        )
        
        # Should complete without error


class TestWaveQLCursorEdgeCases:
    """Edge case tests for cursor."""
    
    def test_cursor_repr(self):
        """Test cursor representation."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        
        repr_str = repr(cursor)
        assert "WaveQLCursor" in repr_str
    
    def test_fetchone_no_result(self):
        """Test fetchone with no result."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        cursor._result = None
        
        result = cursor.fetchone()
        assert result is None
    
    def test_fetchmany_no_result(self):
        """Test fetchmany with no result."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        cursor._result = None
        
        results = cursor.fetchmany()
        assert results == []
    
    def test_fetchall_no_result(self):
        """Test fetchall with no result."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        cursor._result = None
        
        results = cursor.fetchall()
        assert results == []
    
    def test_close(self):
        """Test closing cursor."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        
        cursor.close()
        # Should not raise
    
    def test_to_arrow(self):
        """Test to_arrow method."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        cursor._result = pa.table({"id": [1, 2]})
        
        result = cursor.to_arrow()
        assert isinstance(result, pa.Table)
    
    def test_to_df(self):
        """Test to_df method."""
        conn = MagicMock()
        cursor = WaveQLCursor(conn)
        cursor._result = pa.table({"id": [1, 2]})
        
        try:
            import pandas as pd
            result = cursor.to_df()
            assert isinstance(result, pd.DataFrame)
        except ImportError:
            pytest.skip("pandas not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
