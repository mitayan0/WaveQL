"""
Tests for quoted identifier support in WaveQL

These tests verify that WaveQL correctly handles:
1. Quoted table names in SQL queries (e.g., "schema"."table")
2. Mixed quoted/unquoted identifiers
3. Table name cleaning for adapter calls
"""

import pytest
import pyarrow as pa
from typing import List, Any

from waveql.query_planner import QueryPlanner
from waveql.cursor import WaveQLCursor
from waveql.adapters.base import BaseAdapter
from waveql.schema_cache import ColumnInfo
import waveql


class TestQuotedIdentifierParsing:
    """Tests for QueryPlanner's ability to parse quoted identifiers."""
    
    def test_parse_quoted_table_name(self):
        """Test parsing SELECT with quoted table name."""
        planner = QueryPlanner()
        info = planner.parse('SELECT * FROM "users"')
        
        assert info.operation == "SELECT"
        assert info.table == '"users"'
        assert info.columns == ["*"]
    
    def test_parse_quoted_schema_and_table(self):
        """Test parsing SELECT with fully quoted schema.table."""
        planner = QueryPlanner()
        info = planner.parse('SELECT * FROM "servicenow"."incident"')
        
        assert info.operation == "SELECT"
        assert info.table == '"servicenow"."incident"'
    
    def test_parse_mixed_quoted_unquoted(self):
        """Test parsing with quoted schema, unquoted table."""
        planner = QueryPlanner()
        info = planner.parse('SELECT * FROM "servicenow".incident')
        
        assert info.operation == "SELECT"
        assert info.table == '"servicenow".incident'
    
    def test_parse_unquoted_schema_quoted_table(self):
        """Test parsing with unquoted schema, quoted table."""
        planner = QueryPlanner()
        info = planner.parse('SELECT * FROM servicenow."incident"')
        
        assert info.operation == "SELECT"
        assert info.table == 'servicenow."incident"'
    
    def test_parse_quoted_with_where(self):
        """Test parsing quoted identifiers with WHERE clause."""
        planner = QueryPlanner()
        info = planner.parse(
            'SELECT number, priority FROM "servicenow"."incident" WHERE state = 1'
        )
        
        assert info.table == '"servicenow"."incident"'
        assert len(info.predicates) == 1
        assert info.predicates[0].column == "state"
        assert info.predicates[0].value == 1
    
    def test_parse_quoted_with_limit_offset(self):
        """Test parsing quoted identifiers with LIMIT/OFFSET."""
        planner = QueryPlanner()
        info = planner.parse('SELECT * FROM "schema"."table" LIMIT 10 OFFSET 5')
        
        assert info.table == '"schema"."table"'
        assert info.limit == 10
        assert info.offset == 5
    
    def test_parse_quoted_insert(self):
        """Test parsing INSERT with quoted table name."""
        planner = QueryPlanner()
        info = planner.parse(
            'INSERT INTO "servicenow"."incident" (number, priority) VALUES (\'INC001\', 1)'
        )
        
        assert info.operation == "INSERT"
        assert info.table == '"servicenow"."incident"'
    
    def test_parse_quoted_update(self):
        """Test parsing UPDATE with quoted table name."""
        planner = QueryPlanner()
        info = planner.parse(
            'UPDATE "servicenow"."incident" SET priority = 2 WHERE number = \'INC001\''
        )
        
        assert info.operation == "UPDATE"
        assert info.table == '"servicenow"."incident"'
        assert info.values.get("priority") == 2
    
    def test_parse_quoted_delete(self):
        """Test parsing DELETE with quoted table name."""
        planner = QueryPlanner()
        info = planner.parse('DELETE FROM "servicenow"."incident" WHERE number = \'INC001\'')
        
        assert info.operation == "DELETE"
        assert info.table == '"servicenow"."incident"'


class TestTableNameCleaning:
    """Tests for the cursor's table name cleaning logic."""
    
    @pytest.fixture
    def cursor(self):
        """Create a cursor for testing (no actual connection needed for unit tests)."""
        conn = waveql.connect()
        return conn.cursor()
    
    def test_clean_simple_table_name(self, cursor):
        """Test cleaning a simple table name."""
        assert cursor._clean_table_name("incident") == "incident"
    
    def test_clean_quoted_table_name(self, cursor):
        """Test cleaning a quoted table name."""
        assert cursor._clean_table_name('"incident"') == "incident"
    
    def test_clean_schema_qualified_name(self, cursor):
        """Test cleaning a schema-qualified name (unquoted)."""
        assert cursor._clean_table_name("servicenow.incident") == "incident"
    
    def test_clean_quoted_schema_qualified_name(self, cursor):
        """Test cleaning a fully quoted schema-qualified name."""
        assert cursor._clean_table_name('"servicenow"."incident"') == "incident"
    
    def test_clean_mixed_quoted_schema(self, cursor):
        """Test cleaning mixed quoted/unquoted."""
        assert cursor._clean_table_name('"servicenow".incident') == "incident"
    
    def test_clean_mixed_quoted_table(self, cursor):
        """Test cleaning with unquoted schema and quoted table."""
        assert cursor._clean_table_name('servicenow."incident"') == "incident"
    
    def test_clean_empty_name(self, cursor):
        """Test cleaning an empty/None table name."""
        assert cursor._clean_table_name(None) is None
        assert cursor._clean_table_name("") == ""


class TestQuotedIdentifierIntegration:
    """Integration tests for adapter resolution with quoted identifiers."""
    
    class MockAdapter(BaseAdapter):
        """Mock adapter that records the table names it receives."""
        adapter_name = "mock"
        supports_predicate_pushdown = True
        
        # Class variable to track calls
        last_table_name = None
        
        def fetch(self, table, columns=None, predicates=None, limit=None, 
                  offset=None, order_by=None, group_by=None, aggregates=None):
            # Record the table name we received
            TestQuotedIdentifierIntegration.MockAdapter.last_table_name = table
            return pa.Table.from_pylist([{"id": 1, "name": "test"}])
        
        def get_schema(self, table):
            return [ColumnInfo("id", "integer"), ColumnInfo("name", "string")]
    
    def test_adapter_receives_clean_table_name(self):
        """Test that adapters receive clean table names without quotes."""
        conn = waveql.connect()
        adapter = self.MockAdapter(host="dummy")
        
        # Register adapter under schema name
        conn._adapters = {"servicenow": adapter}
        
        cursor = conn.cursor()
        
        # Reset tracking
        self.MockAdapter.last_table_name = None
        
        # Execute query with quoted identifiers
        cursor.execute('SELECT * FROM "servicenow"."incident"')
        
        # Adapter should receive clean table name
        assert self.MockAdapter.last_table_name == "incident"
        conn.close()
    
    def test_adapter_receives_clean_name_unquoted(self):
        """Test that unquoted schema.table also results in clean table name."""
        conn = waveql.connect()
        adapter = self.MockAdapter(host="dummy")
        conn._adapters = {"servicenow": adapter}
        
        cursor = conn.cursor()
        self.MockAdapter.last_table_name = None
        
        cursor.execute("SELECT * FROM servicenow.incident")
        
        assert self.MockAdapter.last_table_name == "incident"
        conn.close()
    
    def test_adapter_resolution_with_quoted_schema(self):
        """Test that adapter is correctly resolved even with quoted schema."""
        conn = waveql.connect()
        adapter = self.MockAdapter(host="dummy")
        
        # Register under unquoted name
        conn._adapters = {"servicenow": adapter}
        
        cursor = conn.cursor()
        
        # Query with quoted schema - should still resolve correctly
        cursor.execute('SELECT * FROM "servicenow"."incident"')
        
        # If we got here without error, adapter was resolved
        assert cursor.to_arrow() is not None
        conn.close()


class TestOrderByWithQuotedTables:
    """Test ORDER BY clause parsing with quoted identifiers."""
    
    def test_parse_order_by_with_quoted_table(self):
        """Test parsing ORDER BY with quoted table."""
        planner = QueryPlanner()
        info = planner.parse(
            'SELECT * FROM "servicenow"."incident" ORDER BY priority DESC'
        )
        
        assert info.table == '"servicenow"."incident"'
        assert len(info.order_by) == 1
        assert info.order_by[0][1] == "DESC"


class TestJoinWithQuotedTables:
    """Test JOIN parsing with quoted identifiers."""
    
    def test_detect_join_with_quoted_tables(self):
        """Test that JOINs with quoted tables are detected."""
        planner = QueryPlanner()
        info = planner.parse(
            'SELECT * FROM "sales"."Account" JOIN "servicenow"."incident" ON 1=1'
        )
        
        assert info.table == '"sales"."Account"'
        assert len(info.joins) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
