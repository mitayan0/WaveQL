"""
Tests for WaveQL adapters/stripe module.

This covers the 51% uncovered module waveql/adapters/stripe.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from waveql.adapters.stripe import StripeAdapter
from waveql.query_planner import Predicate


class TestStripeAdapterInit:
    """Tests for StripeAdapter initialization."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx",
        )
        
        assert adapter.adapter_name == "stripe"
    
    def test_init_with_api_version(self):
        """Test initialization with API version."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx",
            api_version="2023-10-16",
        )
        
        assert adapter._api_version == "2023-10-16"



class TestStripeAdapterFetch:
    """Tests for StripeAdapter fetch method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked HTTP client."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "object": "list",
                "data": [
                    {"id": "cus_1", "email": "a@test.com", "name": "Alice"},
                    {"id": "cus_2", "email": "b@test.com", "name": "Bob"},
                ],
                "has_more": False,
            }
            mock_client.get.return_value = mock_response
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            yield adapter
    
    def test_fetch_customers(self, mock_adapter):
        """Test fetching customers."""
        result = mock_adapter.fetch("customers")
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_limit(self, mock_adapter):
        """Test fetching with limit."""
        result = mock_adapter.fetch("customers", limit=10)
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_predicates(self, mock_adapter):
        """Test fetching with predicates."""
        predicates = [Predicate(column="email", operator="=", value="a@test.com")]
        result = mock_adapter.fetch("customers", predicates=predicates)
        assert isinstance(result, pa.Table)


class TestStripeAdapterTables:
    """Tests for different Stripe tables."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked responses."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            
            def mock_get(url, **kwargs):
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {
                    "object": "list",
                    "data": [],
                    "has_more": False,
                }
                return resp
            
            mock_client.get = mock_get
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            yield adapter
    
    def test_fetch_charges(self, mock_adapter):
        result = mock_adapter.fetch("charges")
        assert isinstance(result, pa.Table)
    
    def test_fetch_invoices(self, mock_adapter):
        result = mock_adapter.fetch("invoices")
        assert isinstance(result, pa.Table)
    
    def test_fetch_subscriptions(self, mock_adapter):
        result = mock_adapter.fetch("subscriptions")
        assert isinstance(result, pa.Table)
    
    def test_fetch_payment_intents(self, mock_adapter):
        result = mock_adapter.fetch("payment_intents")
        assert isinstance(result, pa.Table)


class TestStripeAdapterSchema:
    """Tests for schema discovery."""
    
    def test_get_schema(self):
        """Test getting schema."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx",
        )
        schema = adapter.get_schema("customers")
        assert isinstance(schema, list)


class TestStripeAdapterListTables:
    """Tests for list_tables method."""
    
    def test_list_tables(self):
        """Test listing available tables."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx",
        )
        tables = adapter.list_tables()
        assert "customers" in tables


class TestStripeAdapterPagination:
    """Tests for pagination handling."""
    
    def test_fetch_with_pagination(self):
        """Test fetching with cursor-based pagination."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            call_count = 0
            def mock_get(url, **kwargs):
                nonlocal call_count
                call_count += 1
                resp = MagicMock()
                resp.status_code = 200
                if call_count == 1:
                    resp.json.return_value = {
                        "object": "list",
                        "data": [{"id": "cus_1"}],
                        "has_more": True,
                    }
                else:
                    resp.json.return_value = {
                        "object": "list",
                        "data": [{"id": "cus_2"}],
                        "has_more": False,
                    }
                return resp
            
            mock_client.get = mock_get
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            result = adapter.fetch("customers")


class TestStripeAdapterErrorHandling:
    """Tests for error handling."""
    
    def test_fetch_api_error(self):
        """Test handling API errors."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            mock_response = MagicMock()
            mock_response.status_code = 401
            mock_response.json.return_value = {
                "error": {
                    "message": "Invalid API Key",
                    "type": "authentication_error",
                }
            }
            # mock_response.raise_for_status.side_effect = Exception("Unauthorized") # Adapter checks status code manually??
            # Checking StripeAdapter logic: if response.status_code >= 400: raise AdapterError
            
            mock_client.get.return_value = mock_response
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_invalid",
            )
            
            from waveql.exceptions import AdapterError
            with pytest.raises(AdapterError):
                adapter.fetch("customers")
    
    def test_fetch_rate_limit(self):
        """Test handling rate limit errors."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            resp1 = MagicMock()
            resp1.status_code = 429
            resp1.headers = {"Retry-After": "0.1"}
            
            resp2 = MagicMock()
            resp2.status_code = 200
            resp2.json.return_value = {"object": "list", "data": [], "has_more": False}
            
            mock_client.get.return_value = resp1
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            from waveql.exceptions import AdapterError
            with pytest.raises(AdapterError):
                adapter.fetch("customers")


class TestStripeAdapterDateFiltering:
    """Tests for date-based filtering."""
    
    def test_fetch_with_date_filter(self):
        """Test fetching with date predicates."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "object": "list",
                "data": [],
                "has_more": False,
            }
            mock_client.get.return_value = mock_response
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            predicates = [
                Predicate(column="created", operator=">=", value=datetime(2024, 1, 1)),
            ]
            
            result = adapter.fetch("charges", predicates=predicates)
            assert isinstance(result, pa.Table)


class TestStripeAdapterExpand:
    """Tests for expand parameter support."""
    
    def test_fetch_with_expand(self):
        """Test fetching with expand parameter."""
        with patch("httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "object": "list",
                "data": [
                    {
                        "id": "ch_1",
                        "customer": {"id": "cus_1", "email": "test@test.com"},
                    }
                ],
                "has_more": False,
            }
            mock_client.get.return_value = mock_response
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            # Fetch with expanded customer
            result = adapter.fetch("charges", columns=["id", "customer"])


class TestStripeAdapterCRUD:
    """Tests for CRUD operations."""
    
    def test_insert(self):
        """Test insert operation via mocking _request_async."""
        import anyio
        
        async def run_test():
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"id": "cus_123"}
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await adapter.insert_async("customers", {"email": "new@test.com", "name": "New"})
                assert result == 1
        
        anyio.run(run_test)
    
    def test_update_with_id(self):
        """Test update operation with ID predicate."""
        import anyio
        
        async def run_test():
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"id": "cus_123"}
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                predicates = [Predicate(column="id", operator="=", value="cus_123")]
                result = await adapter.update_async("customers", {"name": "Updated"}, predicates)
                assert result == 1
        
        anyio.run(run_test)
    
    def test_update_without_id_raises(self):
        """Test update without ID raises error."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx",
        )
        
        from waveql.exceptions import QueryError
        with pytest.raises(QueryError):
            adapter.update("customers", {"name": "Updated"}, predicates=None)
    
    def test_delete_with_id(self):
        """Test delete operation with ID predicate."""
        import anyio
        
        async def run_test():
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"id": "cus_123", "deleted": True}
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                predicates = [Predicate(column="id", operator="=", value="cus_123")]
                result = await adapter.delete_async("customers", predicates)
                assert result == 1
        
        anyio.run(run_test)
    
    def test_delete_without_id_raises(self):
        """Test delete without ID raises error."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx",
        )
        
        from waveql.exceptions import QueryError
        with pytest.raises(QueryError):
            adapter.delete("customers", predicates=[])


class TestStripeAdapterSearchAPI:
    """Tests for Stripe Search API."""
    
    def test_fetch_via_search_with_predicates(self):
        """Test fetching via Search API with predicates."""
        import anyio
        
        async def run_test():
            with patch("httpx.AsyncClient") as MockAsyncClient:
                mock_client = MockAsyncClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "object": "search_result",
                    "data": [
                        {"id": "cus_1", "email": "a@test.com"},
                    ],
                    "has_more": False,
                    "total_count": 1,
                }
                mock_client.request.return_value = mock_response
                
                adapter = StripeAdapter(
                    host="api.stripe.com",
                    api_key="sk_test_xxx",
                )
                
                predicates = [Predicate(column="email", operator="=", value="a@test.com")]
                result = await adapter.fetch_async("customers", predicates=predicates)
                
                assert isinstance(result, pa.Table)
        
        anyio.run(run_test)


class TestStripeAdapterCountOptimization:
    """Tests for COUNT(*) optimization."""
    
    def test_is_simple_count_true(self):
        """Test detection of simple COUNT(*) query."""
        adapter = StripeAdapter(host="api.stripe.com", api_key="sk_test_xxx")
        
        agg = MagicMock()
        agg.func = "COUNT"
        agg.column = "*"
        
        result = adapter._is_simple_count([agg], None)
        assert result == True
    
    def test_is_simple_count_false_with_group_by(self):
        """Test COUNT with GROUP BY is not simple."""
        adapter = StripeAdapter(host="api.stripe.com", api_key="sk_test_xxx")
        
        agg = MagicMock()
        agg.func = "COUNT"
        agg.column = "*"
        
        result = adapter._is_simple_count([agg], ["status"])
        assert result == False
    
    def test_is_simple_count_false_non_count(self):
        """Test non-COUNT aggregate is not simple count."""
        adapter = StripeAdapter(host="api.stripe.com", api_key="sk_test_xxx")
        
        agg = MagicMock()
        agg.func = "SUM"
        agg.column = "amount"
        
        result = adapter._is_simple_count([agg], None)
        assert result == False


class TestStripeAdapterConnectionRetry:
    """Tests for connection retry logic."""
    
    def test_fetch_connection_error_retries(self):
        """Test that connection errors trigger retries."""
        import httpx
        
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            
            success_response = MagicMock()
            success_response.status_code = 200
            success_response.json.return_value = {"data": [], "has_more": False}
            
            call_count = 0
            def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise httpx.ConnectError("Connection failed")
                return success_response
            
            mock_client.get.side_effect = side_effect
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            with patch("time.sleep"):
                result = adapter.fetch("customers")
            
            assert isinstance(result, pa.Table)
    
    def test_fetch_connection_error_max_retries_exceeded(self):
        """Test that max retries are respected."""
        import httpx
        
        with patch("httpx.Client") as MockClient:
            mock_client = MockClient.return_value.__enter__.return_value
            mock_client.get.side_effect = httpx.ConnectError("Connection failed")
            
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            from waveql.exceptions import AdapterError
            with patch("time.sleep"):
                with pytest.raises(AdapterError, match="after .* attempts"):
                    adapter.fetch("customers")


class TestStripeAdapterSchemaInference:
    """Tests for schema inference."""
    
    def test_get_schema_infers_types(self):
        """Test schema inference from sample data."""
        import anyio
        
        async def run_test():
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": [
                    {
                        "id": "cus_1",
                        "name": "Test",
                        "active": True,
                        "balance": 1000,
                        "amount": 99.99,
                        "metadata": {"key": "value"},
                    }
                ],
                "has_more": False,
            }
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                schema = await adapter.get_schema_async("customers")
                
                assert isinstance(schema, list)
                # Check that types are inferred correctly
                type_map = {col.name: col.data_type for col in schema}
                assert type_map["id"] == "string"
                assert type_map["active"] == "boolean"
                assert type_map["balance"] == "integer"
                assert type_map["amount"] == "double"
                assert type_map["metadata"] == "struct"
        
        anyio.run(run_test)
    
    def test_get_schema_empty_table(self):
        """Test schema for empty table."""
        import anyio
        
        async def run_test():
            adapter = StripeAdapter(
                host="api.stripe.com",
                api_key="sk_test_xxx",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"data": [], "has_more": False}
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                schema = await adapter.get_schema_async("customers")
                assert schema == []
        
        anyio.run(run_test)


class TestStripeAdapterResourceMapping:
    """Tests for resource mapping."""
    
    def test_resource_map_singular_plural(self):
        """Test that singular and plural names work."""
        adapter = StripeAdapter(host="api.stripe.com", api_key="sk_test_xxx")
        
        # Test that both singular and plural map to the same endpoint
        assert adapter.RESOURCE_MAP.get("customer") == "customers"
        assert adapter.RESOURCE_MAP.get("customers") == "customers"
        assert adapter.RESOURCE_MAP.get("charge") == "charges"
        assert adapter.RESOURCE_MAP.get("charges") == "charges"
    
    def test_list_tables(self):
        """Test listing available tables."""
        adapter = StripeAdapter(host="api.stripe.com", api_key="sk_test_xxx")
        tables = adapter.list_tables()
        
        assert "customers" in tables
        assert "charges" in tables
        assert "invoices" in tables
        assert "subscriptions" in tables


class TestStripeAdapterAsync:
    """Tests for async operations."""
    
    def test_fetch_async_searchable_resource(self):
        """Test async fetch for searchable resources."""
        import anyio
        
        async def run_test():
            with patch("httpx.AsyncClient") as MockAsyncClient:
                mock_client = MockAsyncClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "object": "search_result",
                    "data": [{"id": "cus_1"}],
                    "has_more": False,
                    "total_count": 1,
                }
                mock_client.request.return_value = mock_response
                
                adapter = StripeAdapter(
                    host="api.stripe.com",
                    api_key="sk_test_xxx",
                )
                
                # customers is searchable
                predicates = [Predicate(column="email", operator="=", value="test@test.com")]
                result = await adapter.fetch_async("customers", predicates=predicates)
                
                assert isinstance(result, pa.Table)
        
        anyio.run(run_test)
    
    def test_fetch_async_list_resource(self):
        """Test async fetch for non-searchable resources."""
        import anyio
        
        async def run_test():
            with patch("httpx.AsyncClient") as MockAsyncClient:
                mock_client = MockAsyncClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "data": [{"id": "py_1"}],
                    "has_more": False,
                }
                mock_client.request.return_value = mock_response
                
                adapter = StripeAdapter(
                    host="api.stripe.com",
                    api_key="sk_test_xxx",
                )
                
                # payouts is not searchable
                result = await adapter.fetch_async("payouts")
                
                assert isinstance(result, pa.Table)
        
        anyio.run(run_test)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
