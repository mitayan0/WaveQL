
import pytest
import respx
from httpx import Response
import pyarrow as pa
from waveql.adapters.salesforce import SalesforceAdapter
from waveql.query_planner import Predicate
from waveql.exceptions import AdapterError, QueryError, RateLimitError

MOCK_DESCRIBE_ACCOUNT = {
    "name": "Account",
    "fields": [
        {"name": "Id", "type": "id", "nillable": False},
        {"name": "Name", "type": "string", "nillable": True},
        {"name": "Type", "type": "picklist", "nillable": True},
        {"name": "AnnualRevenue", "type": "currency", "nillable": True},
    ]
}

@pytest.fixture
async def adapter():
    a = SalesforceAdapter(host="https://test.salesforce.com", use_connection_pool=False)
    yield a
    if hasattr(a, "_client") and a._client:
        await a._client.aclose()

@pytest.mark.asyncio
async def test_fetch_async_simple(adapter):
    with respx.mock:
        # Mock Describe
        respx.get("https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe").mock(
            return_value=Response(200, json=MOCK_DESCRIBE_ACCOUNT)
        )
        
        # Mock Query
        route = respx.get("https://test.salesforce.com/services/data/v57.0/query").mock(
            return_value=Response(200, json={
                "done": True, 
                "totalSize": 1, 
                "records": [{"Id": "1", "Name": "Test"}]
            })
        )
        
        result = await adapter.fetch_async("Account")
        
        assert len(result) == 1
        assert result["Name"][0].as_py() == "Test"
        assert route.called

@pytest.mark.asyncio
async def test_fetch_async_columns(adapter):
    # If columns specified, describe might be skipped if we don't infer schema
    with respx.mock:
        respx.get("https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe").mock(
            return_value=Response(200, json=MOCK_DESCRIBE_ACCOUNT)
        )
        route = respx.get("https://test.salesforce.com/services/data/v57.0/query").mock(
            return_value=Response(200, json={
                "done": True, 
                "totalSize": 1, 
                "records": [{"Name": "Test"}]
            })
        )
        
        await adapter.fetch_async("Account", columns=["Name"], limit=5)
        
        url = str(route.calls.last.request.url)
        assert "SELECT+Name+FROM+Account" in url or "SELECT%20Name%20FROM%20Account" in url
        assert "LIMIT+5" in url or "LIMIT%205" in url

@pytest.mark.asyncio
async def test_insert_async(adapter):
    with respx.mock:
        route = respx.post("https://test.salesforce.com/services/data/v57.0/sobjects/Account").mock(
            return_value=Response(201, json={"id": "new_id", "success": True})
        )
        
        count = await adapter.insert_async("Account", {"Name": "New"})
        assert count == 1
        assert route.called

@pytest.mark.asyncio
async def test_update_async(adapter):
    with respx.mock:
        route = respx.patch("https://test.salesforce.com/services/data/v57.0/sobjects/Account/sys_1").mock(
            return_value=Response(204)
        )
        
        pred = [Predicate("Id", "=", "sys_1")]
        count = await adapter.update_async("Account", {"Name": "Upd"}, predicates=pred)
        assert count == 1
        assert route.called

@pytest.mark.asyncio
async def test_delete_async(adapter):
    with respx.mock:
        route = respx.delete("https://test.salesforce.com/services/data/v57.0/sobjects/Account/sys_1").mock(
            return_value=Response(204)
        )
        
        pred = [Predicate("Id", "=", "sys_1")]
        count = await adapter.delete_async("Account", predicates=pred)
        assert count == 1
        assert route.called

@pytest.mark.asyncio
async def test_get_schema_async(adapter):
    with respx.mock:
        respx.get("https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe").mock(
            return_value=Response(200, json=MOCK_DESCRIBE_ACCOUNT)
        )
        
        cols = await adapter.get_schema_async("Account")
        assert len(cols) == 4
        assert cols[0].name == "Id"
        assert cols[1].name == "Name"

