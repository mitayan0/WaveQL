"""
Tests for ServiceNow Adapter

Uses responses library to mock ServiceNow Table API endpoints.
"""

import pytest
import responses
import pyarrow as pa

from waveql.adapters.servicenow import ServiceNowAdapter
from waveql.query_planner import Predicate
from waveql.schema_cache import SchemaCache
from waveql.exceptions import AdapterError, QueryError, RateLimitError


# Test data - simulating ServiceNow incident records
MOCK_INCIDENTS = [
    {
        "sys_id": "abc123",
        "number": "INC0001",
        "short_description": "Server down",
        "priority": 1,
        "active": True,
        "assigned_to": "admin",
    },
    {
        "sys_id": "def456",
        "number": "INC0002",
        "short_description": "Network issue",
        "priority": 2,
        "active": True,
        "assigned_to": "network_team",
    },
    {
        "sys_id": "ghi789",
        "number": "INC0003",
        "short_description": "Password reset",
        "priority": 3,
        "active": False,
        "assigned_to": "helpdesk",
    },
]


class TestServiceNowAdapter:
    """Tests for ServiceNow adapter functionality."""

    @responses.activate
    def test_fetch_all_records(self):
        """Test fetching all records from a table."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": MOCK_INCIDENTS},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.fetch("incident")

        assert isinstance(result, pa.Table)
        assert len(result) == 3
        assert "number" in result.column_names
        assert "short_description" in result.column_names

    @responses.activate
    def test_fetch_with_column_selection(self):
        """Test fetching specific columns."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [
                {"number": "INC0001", "priority": 1},
                {"number": "INC0002", "priority": 2},
            ]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.fetch("incident", columns=["number", "priority"])

        # Check that sysparm_fields was included in request
        assert "sysparm_fields" in responses.calls[0].request.url
        assert "number" in responses.calls[0].request.url
        assert "priority" in responses.calls[0].request.url

    @responses.activate
    def test_predicate_pushdown_equals(self):
        """Test predicate pushdown with equals operator."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [MOCK_INCIDENTS[0]]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="priority", operator="=", value=1)]
        result = adapter.fetch("incident", predicates=predicates)

        # Check that sysparm_query was included
        assert "sysparm_query" in responses.calls[0].request.url
        assert "priority%3D1" in responses.calls[0].request.url  # URL encoded priority=1

    @responses.activate
    def test_predicate_pushdown_comparison(self):
        """Test predicate pushdown with comparison operators."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": MOCK_INCIDENTS[:2]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="priority", operator="<", value=3)]
        result = adapter.fetch("incident", predicates=predicates)

        assert "sysparm_query" in responses.calls[0].request.url
        assert len(result) == 2

    @responses.activate
    def test_predicate_pushdown_like(self):
        """Test predicate pushdown with LIKE operator."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [MOCK_INCIDENTS[0]]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="short_description", operator="LIKE", value="%Server%")]
        result = adapter.fetch("incident", predicates=predicates)

        assert "LIKE" in responses.calls[0].request.url

    @responses.activate
    def test_predicate_multiple_conditions(self):
        """Test multiple predicates combined with AND (^)."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [MOCK_INCIDENTS[0]]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [
            Predicate(column="priority", operator="=", value=1),
            Predicate(column="active", operator="=", value="true"),
        ]
        result = adapter.fetch("incident", predicates=predicates)

        # ServiceNow uses ^ to join conditions
        url = responses.calls[0].request.url
        assert "priority" in url
        assert "active" in url
        assert "%5E" in url  # URL encoded ^

    @responses.activate
    def test_limit_and_offset(self):
        """Test LIMIT and OFFSET parameters."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [MOCK_INCIDENTS[1]]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.fetch("incident", limit=1, offset=1)

        url = responses.calls[0].request.url
        assert "sysparm_limit=1" in url
        assert "sysparm_offset=1" in url

    @responses.activate
    def test_order_by(self):
        """Test ORDER BY clause translation."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": MOCK_INCIDENTS},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.fetch("incident", order_by=[("priority", "ASC")])

        assert "ORDERBY" in responses.calls[0].request.url

    @responses.activate
    def test_insert_record(self):
        """Test INSERT operation."""
        responses.add(
            responses.POST,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": {"sys_id": "new123", "number": "INC0004"}},
            status=201,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.insert("incident", {
            "short_description": "New ticket",
            "priority": 2,
        })

        assert result == 1
        assert responses.calls[0].request.method == "POST"

    @responses.activate
    def test_update_record(self):
        """Test UPDATE operation."""
        responses.add(
            responses.PATCH,
            "https://test.service-now.com/api/now/table/incident/abc123",
            json={"result": {"sys_id": "abc123", "priority": 1}},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="sys_id", operator="=", value="abc123")]
        result = adapter.update("incident", {"priority": 1}, predicates=predicates)

        assert result == 1
        assert responses.calls[0].request.method == "PATCH"

    @responses.activate
    def test_update_requires_sys_id(self):
        """Test that UPDATE fails without sys_id."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="priority", operator="=", value=1)]

        with pytest.raises(QueryError, match="sys_id"):
            adapter.update("incident", {"priority": 2}, predicates=predicates)

    @responses.activate
    def test_delete_record(self):
        """Test DELETE operation."""
        responses.add(
            responses.DELETE,
            "https://test.service-now.com/api/now/table/incident/abc123",
            status=204,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="sys_id", operator="=", value="abc123")]
        result = adapter.delete("incident", predicates=predicates)

        assert result == 1
        assert responses.calls[0].request.method == "DELETE"

    @responses.activate
    def test_delete_requires_sys_id(self):
        """Test that DELETE fails without sys_id."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="number", operator="=", value="INC0001")]

        with pytest.raises(QueryError, match="sys_id"):
            adapter.delete("incident", predicates=predicates)

    @responses.activate
    def test_rate_limit_handling(self):
        """Test rate limit error handling."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            status=429,
            headers={"Retry-After": "60"},
        )

        adapter = ServiceNowAdapter(host="test.service-now.com", max_retries=0)

        with pytest.raises(RateLimitError) as exc_info:
            adapter.fetch("incident")

        assert exc_info.value.retry_after == 60

    @responses.activate
    def test_empty_result(self):
        """Test handling empty results."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": []},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.fetch("incident")

        assert isinstance(result, pa.Table)
        assert len(result) == 0

    @responses.activate
    def test_schema_discovery(self):
        """Test dynamic schema discovery from API response."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": MOCK_INCIDENTS},
            status=200,
        )

        cache = SchemaCache()  # In-memory cache
        adapter = ServiceNowAdapter(host="test.service-now.com", schema_cache=cache)
        result = adapter.fetch("incident")

        # Schema should be discovered from first record
        assert "sys_id" in result.column_names
        assert "number" in result.column_names
        assert "priority" in result.column_names

    @responses.activate
    def test_schema_caching(self):
        """Test that schema is cached after first discovery."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [MOCK_INCIDENTS[0]]},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": [MOCK_INCIDENTS[1]]},
            status=200,
        )

        cache = SchemaCache()
        adapter = ServiceNowAdapter(host="test.service-now.com", schema_cache=cache)

        # First fetch - discovers schema
        adapter.fetch("incident")
        # Second fetch - uses cached schema
        adapter.fetch("incident")

        # Both should work with cached schema
        assert len(responses.calls) == 2

    @responses.activate
    def test_host_normalization(self):
        """Test that host URLs are normalized correctly."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": []},
            status=200,
        )

        # Test without https://
        adapter = ServiceNowAdapter(host="test.service-now.com")
        adapter.fetch("incident")
        assert "https://test.service-now.com" in responses.calls[0].request.url

    @responses.activate
    def test_table_name_with_schema(self):
        """Test table name extraction from schema.table format."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": MOCK_INCIDENTS},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        result = adapter.fetch("servicenow.incident")  # with schema prefix

        # Should extract just 'incident'
        assert "/table/incident" in responses.calls[0].request.url

    @responses.activate
    def test_list_tables(self):
        """Test listing available tables."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/sys_db_object",
            json={"result": [
                {"name": "incident", "label": "Incident"},
                {"name": "problem", "label": "Problem"},
                {"name": "change_request", "label": "Change Request"},
            ]},
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        tables = adapter.list_tables()

        assert "incident" in tables
        assert "problem" in tables
        assert "change_request" in tables

    @responses.activate
    def test_display_value_parameter(self):
        """Test that sysparm_display_value is injected correctly."""
        responses.add(
            responses.GET,
            "https://test.service-now.com/api/now/table/incident",
            json={"result": []},
            status=200,
        )

        # 1. Test boolean True
        adapter = ServiceNowAdapter(host="test.service-now.com", display_value=True)
        adapter.fetch("incident")
        assert "sysparm_display_value=true" in responses.calls[0].request.url

        # 2. Test string 'all'
        adapter = ServiceNowAdapter(host="test.service-now.com", display_value="all")
        adapter.fetch("incident")
        assert "sysparm_display_value=all" in responses.calls[1].request.url

    @responses.activate
    def test_fetch_attachment_content(self):
        """Test fetching binary content from sys_attachment_content virtual table."""
        attachment_id = "test_attachment_123"
        mock_content = b"fake binary data"
        
        responses.add(
            responses.GET,
            f"https://test.service-now.com/api/now/attachment/{attachment_id}/file",
            body=mock_content,
            status=200,
        )

        adapter = ServiceNowAdapter(host="test.service-now.com")
        predicates = [Predicate(column="sys_id", operator="=", value=attachment_id)]
        
        result = adapter.fetch("sys_attachment_content", predicates=predicates)

        assert len(result) == 1
        assert result.column("content")[0].as_buffer() == mock_content
        assert result.column("sys_id")[0].as_py() == attachment_id


class TestPredicateConversion:
    """Tests specifically for predicate to ServiceNow query conversion."""

    def test_is_null_predicate(self):
        """Test IS NULL conversion."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        pred = Predicate(column="assigned_to", operator="IS NULL", value=None)
        query = adapter._predicate_to_query(pred)
        assert query == "assigned_toISEMPTY"

    def test_is_not_null_predicate(self):
        """Test IS NOT NULL conversion."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        pred = Predicate(column="assigned_to", operator="IS NOT NULL", value=None)
        query = adapter._predicate_to_query(pred)
        assert query == "assigned_toISNOTEMPTY"

    def test_in_predicate_with_list(self):
        """Test IN operator with list values."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        pred = Predicate(column="priority", operator="IN", value=[1, 2, 3])
        query = adapter._predicate_to_query(pred)
        assert query == "priorityIN1,2,3"

    def test_not_equals_predicate(self):
        """Test != operator."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        pred = Predicate(column="active", operator="!=", value="false")
        query = adapter._predicate_to_query(pred)
        assert query == "active!=false"


class TestArrowConversion:
    """Tests for converting ServiceNow records to Arrow tables."""

    def test_type_inference_integer(self):
        """Test integer type inference."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        assert adapter._infer_type(42) == "integer"

    def test_type_inference_float(self):
        """Test float type inference."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        assert adapter._infer_type(3.14) == "float"

    def test_type_inference_boolean(self):
        """Test boolean type inference."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        assert adapter._infer_type(True) == "boolean"

    def test_type_inference_string(self):
        """Test string type inference."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        assert adapter._infer_type("hello") == "string"

    def test_type_inference_none(self):
        """Test None value defaults to string."""
        adapter = ServiceNowAdapter(host="test.service-now.com")
        assert adapter._infer_type(None) == "string"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
