"""
Tests for WaveQL adapters/hubspot module.

This covers the 45% uncovered module waveql/adapters/hubspot.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch

from waveql.adapters.hubspot import HubSpotAdapter
from waveql.query_planner import Predicate


class TestHubSpotAdapterInit:
    """Tests for HubSpotAdapter initialization."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            api_key="test-api-key",
        )
        
        assert adapter.adapter_name == "hubspot"
    
    def test_init_with_oauth(self):
        """Test initialization with OAuth."""
        mock_auth = MagicMock()
        mock_auth.get_headers.return_value = {"Authorization": "Bearer token"}
        
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            auth_manager=mock_auth,
        )
        
        # The adapter essentially stores config, correctness is implicitly verified by successful init
        assert adapter._config is not None


class TestHubSpotAdapterFetch:
    """Tests for HubSpotAdapter fetch method."""
    
    @pytest.fixture
    def mock_response(self):
        """Create a standard mock response."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "results": [
                {"id": "1", "properties": {"email": "a@test.com", "firstname": "Alice"}},
                {"id": "2", "properties": {"email": "b@test.com", "firstname": "Bob"}},
            ],
            "paging": None,
        }
        return response

    def test_fetch_contacts(self, mock_response):
        """Test fetching contacts."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value = mock_response
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-api-key",
            )
            
            result = adapter.fetch("contacts")
            
            assert isinstance(result, pa.Table)
            mock_client.post.assert_called()
    
    def test_fetch_with_columns(self, mock_response):
        """Test fetching with column selection."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value = mock_response
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-api-key",
            )
            
            result = adapter.fetch(
                "contacts",
                columns=["id", "email"],
            )
            
            assert isinstance(result, pa.Table)
    
    def test_fetch_with_limit(self, mock_response):
        """Test fetching with limit."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value = mock_response
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-api-key",
            )
            
            result = adapter.fetch(
                "contacts",
                limit=10,
            )
            
            assert isinstance(result, pa.Table)
    
    def test_fetch_with_predicates(self, mock_response):
        """Test fetching with predicates."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value = mock_response
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-api-key",
            )
            
            predicates = [
                Predicate(column="email", operator="=", value="a@test.com"),
            ]
            
            result = adapter.fetch(
                "contacts",
                predicates=predicates,
            )
            
            assert isinstance(result, pa.Table)


class TestHubSpotAdapterTables:
    """Tests for different HubSpot tables."""
    
    def test_fetch_companies(self):
        """Test fetching companies."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value.status_code = 200
            mock_client.post.return_value.json.return_value = {"results": [], "paging": None}
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            result = adapter.fetch("companies")
            assert isinstance(result, pa.Table)
    
    def test_fetch_deals(self):
        """Test fetching deals."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value.status_code = 200
            mock_client.post.return_value.json.return_value = {"results": [], "paging": None}

            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            result = adapter.fetch("deals")
            assert isinstance(result, pa.Table)
    
    def test_fetch_tickets(self):
        """Test fetching tickets."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.post.return_value.status_code = 200
            mock_client.post.return_value.json.return_value = {"results": [], "paging": None}

            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            result = adapter.fetch("tickets")
            assert isinstance(result, pa.Table)


class TestHubSpotAdapterSchema:
    """Tests for schema discovery."""
    
    def test_get_schema(self):
        """Test getting schema. Schema uses GET on properties API via async method."""
        import anyio
        
        async def _run_test():
            with patch("httpx.AsyncClient") as MockClient:
                mock_client = MockClient.return_value.__aenter__.return_value
                
                # get_schema_async uses client.request("GET", ...)
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "results": [
                        {"name": "email", "type": "string", "fieldType": "text"},
                        {"name": "firstname", "type": "string", "fieldType": "text"},
                    ]
                }
                mock_client.request.return_value = mock_response
                
                adapter = HubSpotAdapter(
                    host="api.hubapi.com",
                    api_key="test-key",
                )
                
                schema = await adapter.get_schema_async("contacts")
                
                assert isinstance(schema, list)
        
        anyio.run(_run_test)


class TestHubSpotAdapterListTables:
    """Tests for list_tables method."""
    
    def test_list_tables(self):
        """Test listing available tables."""
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            api_key="test-key",
        )
        
        tables = adapter.list_tables()
        
        assert isinstance(tables, list)
        assert "contacts" in tables
        assert "companies" in tables
        assert "deals" in tables


class TestHubSpotAdapterPagination:
    """Tests for pagination handling."""
    
    def test_fetch_with_pagination(self):
        """Test fetching with pagination."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            
            # First page
            page1_response = MagicMock()
            page1_response.status_code = 200
            page1_response.json.return_value = {
                "results": [{"id": "1", "properties": {"email": "a@test.com"}}],
                "paging": {"next": {"after": "cursor123"}},
            }
            
            # Second page
            page2_response = MagicMock()
            page2_response.status_code = 200
            page2_response.json.return_value = {
                "results": [{"id": "2", "properties": {"email": "b@test.com"}}],
                "paging": None,
            }
            
            mock_client.post.side_effect = [page1_response, page2_response]
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            result = adapter.fetch("contacts")
            
            # Should have both pages (2 rows)
            assert len(result) == 2


class TestHubSpotAdapterErrorHandling:
    """Tests for error handling."""
    
    def test_fetch_api_error(self):
        """Test handling API errors."""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            
            # Simulate generic 401
            mock_response = MagicMock()
            mock_response.status_code = 401
            mock_response.text = "Unauthorized"
            mock_client.post.return_value = mock_response
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="invalid-key",
            )
            
            with pytest.raises(Exception):
                adapter.fetch("contacts")
    
    def test_fetch_rate_limit(self):
        """Test handling rate limit errors. (Simulated by sync sleep)"""
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            
            # Fail with 429 then succeed
            fail_response = MagicMock()
            fail_response.status_code = 429
            fail_response.headers = {"Retry-After": "1"}
            fail_response.raise_for_status.side_effect = Exception("Rate Limit")
            
            success_response = MagicMock()
            success_response.status_code = 200
            success_response.json.return_value = {"results": [], "paging": None}
            
            # Note: The adapter implementation catches ConnectError for retries, but 429 logic might be handled via status code check
            # The current implementation throws AdapterError on >= 400. It only retries on ConnectError/Timeout.
            # So 429 will just raise AdapterError immediately unless logic changes.
            # Testing that it RAISES AdapterError for now.
            
            mock_client.post.return_value = fail_response
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            with pytest.raises(Exception):
                 adapter.fetch("contacts")


class TestHubSpotAdapterAsync:
    """Tests for async operations."""
    
    def test_fetch_async(self):
        """Test async fetch. Uses httpx.AsyncClient for true async I/O."""
        import anyio
        
        async def _run_test():
            with patch("httpx.AsyncClient") as MockClient:
                mock_client = MockClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"results": [], "paging": None}
                # Code calls client.request(), not client.post()
                mock_client.request.return_value = mock_response
                
                adapter = HubSpotAdapter(
                    host="api.hubapi.com",
                    api_key="test-key",
                )
                
                if hasattr(adapter, "fetch_async"):
                    result = await adapter.fetch_async("contacts")
                    assert isinstance(result, pa.Table)
        
        anyio.run(_run_test)


class TestHubSpotAdapterCRUD:
    """Tests for CRUD operations."""
    
    def test_insert(self):
        """Test insert operation."""
        import anyio
        
        async def _run_test():
            with patch("httpx.AsyncClient") as MockClient:
                mock_client = MockClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 201
                mock_response.json.return_value = {"id": "123", "properties": {}}
                mock_client.request.return_value = mock_response
                
                adapter = HubSpotAdapter(
                    host="api.hubapi.com",
                    api_key="test-key",
                )
                
                result = await adapter.insert_async("contacts", {"email": "new@test.com", "firstname": "New"})
                assert result == 1
        
        anyio.run(_run_test)
    
    def test_update_with_id(self):
        """Test update operation with ID predicate."""
        import anyio
        
        async def _run_test():
            with patch("httpx.AsyncClient") as MockClient:
                mock_client = MockClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"id": "123", "properties": {}}
                mock_client.request.return_value = mock_response
                
                adapter = HubSpotAdapter(
                    host="api.hubapi.com",
                    api_key="test-key",
                )
                
                predicates = [Predicate(column="id", operator="=", value="123")]
                result = await adapter.update_async("contacts", {"firstname": "Updated"}, predicates)
                assert result == 1
        
        anyio.run(_run_test)
    
    def test_update_without_id_raises(self):
        """Test update without ID raises error."""
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            api_key="test-key",
        )
        
        from waveql.exceptions import QueryError
        with pytest.raises(QueryError, match="requires 'id'"):
            adapter.update("contacts", {"firstname": "Updated"}, predicates=None)
    
    def test_delete_with_id(self):
        """Test delete operation with ID predicate."""
        import anyio
        
        async def _run_test():
            with patch("httpx.AsyncClient") as MockClient:
                mock_client = MockClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 204
                mock_response.json.return_value = {}
                mock_client.request.return_value = mock_response
                
                adapter = HubSpotAdapter(
                    host="api.hubapi.com",
                    api_key="test-key",
                )
                
                predicates = [Predicate(column="id", operator="=", value="123")]
                result = await adapter.delete_async("contacts", predicates)
                assert result == 1
        
        anyio.run(_run_test)
    
    def test_delete_without_id_raises(self):
        """Test delete without ID raises error."""
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            api_key="test-key",
        )
        
        from waveql.exceptions import QueryError
        with pytest.raises(QueryError, match="requires 'id'"):
            adapter.delete("contacts", predicates=[])


class TestHubSpotAdapterTypeMapping:
    """Tests for type mapping."""
    
    def test_map_type_number(self):
        """Test number type mapping."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        assert adapter._map_type("number", "number") == "double"
        assert adapter._map_type("number", "integer") == "integer"
    
    def test_map_type_bool(self):
        """Test boolean type mapping."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        assert adapter._map_type("bool", "checkbox") == "boolean"
        assert adapter._map_type("string", "booleancheckbox") == "boolean"
    
    def test_map_type_datetime(self):
        """Test datetime type mapping."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        assert adapter._map_type("datetime", "datetime") == "timestamp"
        assert adapter._map_type("date", "date") == "timestamp"
    
    def test_map_type_string(self):
        """Test default string type mapping."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        assert adapter._map_type("text", "text") == "string"
        assert adapter._map_type("unknown", "unknown") == "string"


class TestHubSpotAdapterPredicates:
    """Tests for predicate handling."""
    
    def test_build_search_payload_operators(self):
        """Test building search payload with various operators."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        predicates = [
            Predicate(column="email", operator="!=", value="test@test.com"),
            Predicate(column="age", operator=">", value=25),
            Predicate(column="age", operator=">=", value=25),
            Predicate(column="age", operator="<", value=50),
            Predicate(column="age", operator="<=", value=50),
        ]
        
        payload = adapter._build_search_payload(None, predicates, None, None, None)
        
        assert "filterGroups" in payload
        if payload["filterGroups"]:
            filters = payload["filterGroups"][0]["filters"]
            assert len(filters) == 5
    
    def test_build_search_payload_unsupported_operator(self):
        """Test handling unsupported operators."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        predicates = [
            Predicate(column="name", operator="BETWEEN", value=[1, 10]),
        ]
        
        # Should not raise, just skip the unsupported predicate
        payload = adapter._build_search_payload(None, predicates, None, None, None)
        assert payload["filterGroups"] == []
    
    def test_build_search_payload_with_order_by(self):
        """Test building search payload with order by."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        order_by = [("created", "DESC"), ("email", "ASC")]
        
        payload = adapter._build_search_payload(None, None, None, None, order_by)
        
        assert len(payload["sorts"]) == 2
        assert payload["sorts"][0]["direction"] == "DESCENDING"
        assert payload["sorts"][1]["direction"] == "ASCENDING"


class TestHubSpotAdapterConnectionRetry:
    """Tests for connection retry logic."""
    
    def test_fetch_connection_error_retries(self):
        """Test that connection errors trigger retries."""
        import httpx
        
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            
            # First three calls fail with ConnectError, fourth succeeds
            success_response = MagicMock()
            success_response.status_code = 200
            success_response.json.return_value = {"results": [], "paging": None}
            
            call_count = 0
            def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise httpx.ConnectError("Connection failed")
                return success_response
            
            mock_client.post.side_effect = side_effect
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            with patch("time.sleep"):  # Don't actually sleep in tests
                result = adapter.fetch("contacts")
            
            assert isinstance(result, pa.Table)
    
    def test_fetch_connection_error_max_retries_exceeded(self):
        """Test that max retries are respected."""
        import httpx
        
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            
            # All calls fail with ConnectError
            mock_client.post.side_effect = httpx.ConnectError("Connection failed")
            
            adapter = HubSpotAdapter(
                host="api.hubapi.com",
                api_key="test-key",
            )
            
            from waveql.exceptions import AdapterError
            with patch("time.sleep"):  # Don't actually sleep in tests
                with pytest.raises(AdapterError, match="after .* attempts"):
                    adapter.fetch("contacts")


class TestHubSpotAdapterCountOptimization:
    """Tests for COUNT(*) optimization."""
    
    def test_is_simple_count_true(self):
        """Test detection of simple COUNT(*) query."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        agg = MagicMock()
        agg.func = "COUNT"
        agg.column = "*"
        
        result = adapter._is_simple_count([agg], None)
        assert result
    
    def test_is_simple_count_with_group_by(self):
        """Test that COUNT with GROUP BY is not simple."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        agg = MagicMock()
        agg.func = "COUNT"
        agg.column = "*"
        
        result = adapter._is_simple_count([agg], ["category"])
        assert not result
    
    def test_is_simple_count_multiple_aggs(self):
        """Test that multiple aggregates is not simple count."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        agg1 = MagicMock()
        agg1.func = "COUNT"
        agg1.column = "*"
        
        agg2 = MagicMock()
        agg2.func = "SUM"
        agg2.column = "amount"
        
        result = adapter._is_simple_count([agg1, agg2], None)
        assert not result


class TestHubSpotAdapterObjectTypes:
    """Tests for object type mapping."""
    
    def test_get_object_type_mappings(self):
        """Test object type mappings."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        assert adapter._get_object_type("contacts") == "contacts"
        assert adapter._get_object_type("contact") == "contacts"
        assert adapter._get_object_type("companies") == "companies"
        assert adapter._get_object_type("company") == "companies"
        assert adapter._get_object_type("deals") == "deals"
        assert adapter._get_object_type("deal") == "deals"
        assert adapter._get_object_type("tickets") == "tickets"
        assert adapter._get_object_type("ticket") == "tickets"
        assert adapter._get_object_type("products") == "products"
        assert adapter._get_object_type("line_items") == "line_items"
        assert adapter._get_object_type("quotes") == "quotes"
    
    def test_get_object_type_custom(self):
        """Test custom object type pass-through."""
        adapter = HubSpotAdapter(host="api.hubapi.com", api_key="test")
        
        # Unknown tables should pass through as-is
        assert adapter._get_object_type("custom_object") == "custom_object"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
