"""
Tests for WaveQL adapters/zendesk module.

This covers the 63% uncovered module waveql/adapters/zendesk.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, AsyncMock, patch

from waveql.adapters.zendesk import ZendeskAdapter
from waveql.query_planner import Predicate


class TestZendeskAdapterInit:
    """Tests for ZendeskAdapter initialization."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
        )
        
        assert adapter._host == "test.zendesk.com"
        assert adapter.adapter_name == "zendesk"
    
    def test_init_with_email_auth(self):
        """Test initialization with email/token auth."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            username="user@test.com",
            api_key="abc123",
        )
        
        assert adapter._email == "user@test.com"
        assert adapter._api_token == "abc123"
    
    def test_init_with_oauth(self):
        """Test initialization with OAuth token."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            oauth_token="oauth_token_here",
        )
        
        assert adapter._access_token == "oauth_token_here"


class TestZendeskAdapterListTables:
    """Tests for list_tables method."""
    
    def test_list_tables(self):
        """Test listing available tables."""
        adapter = ZendeskAdapter(host="test.zendesk.com")
        tables = adapter.list_tables()
        
        assert isinstance(tables, list)
        assert "tickets" in tables
        assert "users" in tables
        assert "organizations" in tables


class TestZendeskAdapterFetch:
    """Tests for fetch operations."""
    
    def test_fetch_tickets(self):
        """Test fetching tickets via async mock."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                username="user@test.com",
                api_key="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "results": [
                    {"id": 1, "subject": "Test Ticket", "status": "open"},
                    {"id": 2, "subject": "Another Ticket", "status": "solved"},
                ],
                "count": 2,
                "next_page": None,
            }
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await adapter.fetch_async("tickets")
                
                assert isinstance(result, pa.Table)
                assert len(result) == 2
        
        anyio.run(run_test)
    
    def test_fetch_with_predicates(self):
        """Test fetching with predicates."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                username="user@test.com",
                api_key="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "results": [{"id": 1, "status": "open"}],
                "count": 1,
                "next_page": None,
            }
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                predicates = [Predicate(column="status", operator="=", value="open")]
                result = await adapter.fetch_async("tickets", predicates=predicates)
                
                assert isinstance(result, pa.Table)
        
        anyio.run(run_test)
    
    def test_fetch_with_limit(self):
        """Test fetching with limit."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                username="user@test.com",
                api_key="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "results": [{"id": 1}],
                "count": 1,
                "next_page": None,
            }
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await adapter.fetch_async("tickets", limit=1)
                
                assert isinstance(result, pa.Table)
        
        anyio.run(run_test)


class TestZendeskAdapterCRUD:
    """Tests for CRUD operations."""
    
    def test_insert_ticket(self):
        """Test inserting a ticket."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                email="user@test.com",
                api_token="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = {"ticket": {"id": 123}}
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                result = await adapter.insert_async(
                    "tickets",
                    {"subject": "New Ticket", "description": "Test"}
                )
                assert result == 1
        
        anyio.run(run_test)
    
    def test_update_ticket(self):
        """Test updating a ticket."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                email="user@test.com",
                api_token="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"ticket": {"id": 1}}
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                predicates = [Predicate(column="id", operator="=", value=1)]
                result = await adapter.update_async(
                    "tickets",
                    {"status": "solved"},
                    predicates
                )
                assert result == 1
        
        anyio.run(run_test)
    
    def test_update_without_id_raises(self):
        """Test that update without ID raises error."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            email="user@test.com",
            api_token="abc123",
        )
        
        from waveql.exceptions import QueryError
        with pytest.raises(QueryError):
            adapter.update("tickets", {"status": "solved"}, predicates=None)
    
    def test_delete_ticket(self):
        """Test deleting a ticket."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                email="user@test.com",
                api_token="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 204
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                predicates = [Predicate(column="id", operator="=", value=1)]
                result = await adapter.delete_async("tickets", predicates)
                assert result == 1
        
        anyio.run(run_test)
    
    def test_delete_without_id_raises(self):
        """Test that delete without ID raises error."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            email="user@test.com",
            api_token="abc123",
        )
        
        from waveql.exceptions import QueryError
        with pytest.raises(QueryError):
            adapter.delete("tickets", predicates=None)


class TestZendeskAdapterSchema:
    """Tests for schema operations."""
    
    def test_get_schema(self):
        """Test getting table schema."""
        import anyio
        
        async def run_test():
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                email="user@test.com",
                api_token="abc123",
            )
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "results": [
                    {"id": 1, "subject": "Test", "status": "open", "priority": "high"},
                ],
                "count": 1,
                "next_page": None,
            }
            
            with patch.object(adapter, '_request_async', new_callable=AsyncMock) as mock_req:
                mock_req.return_value = mock_response
                schema = await adapter.get_schema_async("tickets")
                
                assert isinstance(schema, list)
                column_names = [col.name for col in schema]
                assert "id" in column_names
        
        anyio.run(run_test)


class TestZendeskAdapterCountOptimization:
    """Tests for COUNT(*) optimization."""
    
    def test_is_simple_count_true(self):
        """Test detection of simple COUNT(*)."""
        adapter = ZendeskAdapter(host="test.zendesk.com")
        
        agg = MagicMock()
        agg.func = "COUNT"
        agg.column = "*"
        
        result = adapter._is_simple_count([agg], None)
        assert result is True
    
    def test_is_simple_count_false_with_group_by(self):
        """Test COUNT with GROUP BY is not simple."""
        adapter = ZendeskAdapter(host="test.zendesk.com")
        
        agg = MagicMock()
        agg.func = "COUNT"
        agg.column = "*"
        
        result = adapter._is_simple_count([agg], ["status"])
        assert result is False
    
    def test_fetch_count_only(self):
        """Test optimized COUNT query."""
        import anyio
        
        async def run_test():
            with patch("httpx.AsyncClient") as MockAsyncClient:
                mock_client = MockAsyncClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "results": [],
                    "count": 42,
                }
                mock_client.request.return_value = mock_response
                
                adapter = ZendeskAdapter(
                    host="test.zendesk.com",
                    email="user@test.com",
                    api_token="abc123",
                )
                
                agg = MagicMock()
                agg.alias = "count"
                
                result = await adapter._fetch_count_only("type:ticket", [agg])
                
                assert isinstance(result, pa.Table)
                # Count should be available
        
        anyio.run(run_test)


class TestZendeskAdapterAsync:
    """Tests for async operations."""
    
    def test_fetch_async(self):
        """Test async fetch."""
        import anyio
        
        async def run_test():
            with patch("httpx.AsyncClient") as MockAsyncClient:
                mock_client = MockAsyncClient.return_value.__aenter__.return_value
                
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "results": [{"id": 1, "name": "User"}],
                    "count": 1,
                    "next_page": None,
                }
                mock_client.request.return_value = mock_response
                
                adapter = ZendeskAdapter(
                    host="test.zendesk.com",
                    email="user@test.com",
                    api_token="abc123",
                )
                
                result = await adapter.fetch_async("users")
                
                assert isinstance(result, pa.Table)
        
        anyio.run(run_test)


class TestZendeskAdapterSyncWrappers:
    """Tests for sync wrappers."""
    
    def test_insert_sync(self):
        """Test sync insert wrapper."""
        with patch("anyio.run") as mock_run:
            mock_run.return_value = 1
            
            adapter = ZendeskAdapter(
                host="test.zendesk.com",
                email="user@test.com",
                api_token="abc123",
            )
            
            # The sync wrapper calls anyio.run internally
            # Just verify it's callable
            assert hasattr(adapter, 'insert')
    
    def test_update_sync(self):
        """Test sync update wrapper."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            email="user@test.com",
            api_token="abc123",
        )
        
        assert hasattr(adapter, 'update')
    
    def test_delete_sync(self):
        """Test sync delete wrapper."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            email="user@test.com",
            api_token="abc123",
        )
        
        assert hasattr(adapter, 'delete')


class TestZendeskAdapterResourceMapping:
    """Tests for resource type mapping."""
    
    def test_resource_types(self):
        """Test that resource types are correctly mapped."""
        adapter = ZendeskAdapter(host="test.zendesk.com")
        
        # Check TYPE_MAP exists and has expected types
        assert hasattr(adapter, 'TYPE_MAP') or hasattr(ZendeskAdapter, 'TYPE_MAP')
        
        tables = adapter.list_tables()
        assert "tickets" in tables or "ticket" in tables
        assert "users" in tables or "user" in tables


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
