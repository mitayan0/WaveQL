
import pytest
import respx
import requests
import httpx
from httpx import Response
import pyarrow as pa
from unittest.mock import MagicMock, patch, ANY

from waveql.adapters.servicenow import ServiceNowAdapter
from waveql.query_planner import Predicate, Aggregate
from waveql.exceptions import AdapterError, QueryError, RateLimitError

@pytest.fixture
def adapter_sync():
    """Sync adapter fixture."""
    # Ensure parallel execution is disabled or safe for tests
    return ServiceNowAdapter(
        host="https://instance.service-now.com",
        access_token="test_token",
        page_size=10,
        max_parallel=1,
        use_connection_pool=False
    )

@pytest.fixture
async def adapter():
    """Async adapter fixture."""
    # ... (Keep existing async adapter fixture)
    a = ServiceNowAdapter(
        host="https://instance.service-now.com",
        access_token="test_token",
        page_size=10,
        max_parallel=1,
        use_connection_pool=False
    )
    yield a
    if hasattr(a, "_client") and a._client:
        await a._client.aclose()

# --- Async Tests ---
# (Keep existing async tests)
@pytest.mark.asyncio
async def test_fetch_async_simple(adapter):
    with respx.mock:
        route = respx.get("https://instance.service-now.com/api/now/table/incident").mock(
            return_value=Response(200, json={
                "result": [
                    {"sys_id": "1", "number": "INC001", "short_description": "Test"}
                ]
            })
        )
        
        result = await adapter.fetch_async("incident")
        
        assert len(result) == 1
        assert result.column("number")[0].as_py() == "INC001"
        assert route.called

@pytest.mark.asyncio
async def test_fetch_async_predicate_pushdown(adapter):
    with respx.mock:
        route = respx.get("https://instance.service-now.com/api/now/table/incident").mock(
            return_value=Response(200, json={"result": [{"sys_id": "1"}]})
        )
        
        predicates = [
            Predicate("priority", "=", 1),
            Predicate("active", "=", True),
            Predicate("short_description", "LIKE", "server")
        ]
        
        await adapter.fetch_async("incident", predicates=predicates)
        
        request = route.calls.last.request
        query = request.url.params["sysparm_query"]
        assert "priority=1" in query
        assert "active=True" in query # or true/1
        assert "short_descriptionLIKEserver" in query

@pytest.mark.asyncio
async def test_fetch_async_pagination(adapter):
    # page_size is 10
    with respx.mock:
        # Page 1
        respx.get("https://instance.service-now.com/api/now/table/incident", params={"sysparm_offset": "0", "sysparm_limit": "10"}).mock(
            return_value=Response(200, json={
                "result": [{"sys_id": str(i)} for i in range(10)]
            })
        )
        # Page 2
        respx.get("https://instance.service-now.com/api/now/table/incident", params={"sysparm_offset": "10", "sysparm_limit": "10"}).mock(
            return_value=Response(200, json={
                "result": [{"sys_id": str(i)} for i in range(10, 15)]
            })
        )
        
        result = await adapter.fetch_async("incident")
        
        assert len(result) == 15
    
@pytest.mark.asyncio
async def test_insert_async(adapter):
    with respx.mock:
        route = respx.post("https://instance.service-now.com/api/now/table/incident").mock(
            return_value=Response(201, json={"result": {"sys_id": "new_id"}})
        )
        
        count = await adapter.insert_async("incident", {"short_description": "New Issue"})
        assert count == 1
        assert route.called

@pytest.mark.asyncio
async def test_update_async_single(adapter):
    with respx.mock:
        route = respx.patch("https://instance.service-now.com/api/now/table/incident/sys_1").mock(
            return_value=Response(200, json={"result": {"sys_id": "sys_1"}})
        )
        
        predicates = [Predicate("sys_id", "=", "sys_1")]
        count = await adapter.update_async("incident", {"state": 2}, predicates=predicates)
        assert count == 1
        assert route.called

@pytest.mark.asyncio
async def test_update_async_bulk(adapter):
    with respx.mock:
        r1 = respx.patch("https://instance.service-now.com/api/now/table/incident/sys_1").mock(return_value=Response(200))
        r2 = respx.patch("https://instance.service-now.com/api/now/table/incident/sys_2").mock(return_value=Response(200))
        
        predicates = [Predicate("sys_id", "IN", ["sys_1", "sys_2"])]
        count = await adapter.update_async("incident", {"state": 3}, predicates=predicates)
        assert count == 2
        assert r1.called
        assert r2.called

@pytest.mark.asyncio
async def test_delete_async_bulk(adapter):
    with respx.mock:
        r1 = respx.delete("https://instance.service-now.com/api/now/table/incident/sys_1").mock(return_value=Response(204))
        r2 = respx.delete("https://instance.service-now.com/api/now/table/incident/sys_2").mock(return_value=Response(204))
        
        predicates = [Predicate("sys_id", "IN", ["sys_1", "sys_2"])]
        count = await adapter.delete_async("incident", predicates=predicates)
        assert count == 2

@pytest.mark.asyncio
async def test_fetch_async_stats(adapter):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/stats/incident").mock(
            return_value=Response(200, json={
                "result": {
                    "stats": {"count": "10"}, 
                    "groupby_fields": [{"field": "priority", "value": "1"}]
                }
            })
        )
        
        aggs = [Aggregate("COUNT", "sys_id", "cnt")]
        result = await adapter.fetch_async("incident", group_by=["priority"], aggregates=aggs)
        
        assert len(result) == 1
        assert result["cnt"][0].as_py() == 10
        assert result["priority"][0].as_py() == "1"

@pytest.mark.asyncio
async def test_fetch_async_attachments(adapter):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/attachment/sys_att/file").mock(
            return_value=Response(200, content=b"content")
        )
        
        predicates = [Predicate("sys_id", "=", "sys_att")]
        result = await adapter.fetch_async("sys_attachment_content", predicates=predicates)
        
        assert len(result) == 1
        assert result["content"][0].as_py() == b"content"

@pytest.mark.asyncio
async def test_list_tables_async(adapter):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/table/sys_db_object").mock(
            return_value=Response(200, json={"result": [{"name": "table1"}]})
        )
        
        tables = await adapter.list_tables_async()
        assert tables == ["table1"]

# --- Sync Tests ---

def test_fetch_sync_simple(adapter_sync):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/table/incident").mock(
            return_value=Response(200, json={"result": [{"sys_id": "1"}]})
        )
        result = adapter_sync.fetch("incident")
        assert len(result) == 1

def test_insert_sync(adapter_sync):
    with respx.mock:
        respx.post("https://instance.service-now.com/api/now/table/incident").mock(
            return_value=Response(201, json={"result": {"sys_id": "new"}})
        )
        adapter_sync.insert("incident", {"a": 1})

def test_update_sync(adapter_sync):
    with respx.mock:
        respx.patch("https://instance.service-now.com/api/now/table/incident/s1").mock(return_value=Response(200))
        pred = [Predicate("sys_id", "=", "s1")]
        adapter_sync.update("incident", {"a": 2}, predicates=pred)

def test_delete_sync(adapter_sync):
    with respx.mock:
        respx.delete("https://instance.service-now.com/api/now/table/incident/s1").mock(return_value=Response(204))
        pred = [Predicate("sys_id", "=", "s1")]
        adapter_sync.delete("incident", predicates=pred)

def test_list_tables_sync(adapter_sync):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/table/sys_db_object").mock(
            return_value=Response(200, json={"result": [{"name": "t1"}]})
        )
        assert adapter_sync.list_tables() == ["t1"]

def test_fetch_stats_sync(adapter_sync):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/stats/incident").mock(
            return_value=Response(200, json={"result": {"stats": {"count": "5"}}})
        )
        aggs = [Aggregate("COUNT", "sys_id", "cnt")]
        res = adapter_sync.fetch("incident", aggregates=aggs)
        assert res["cnt"][0].as_py() == 5

def test_fetch_attachments_sync(adapter_sync):
    with respx.mock:
        respx.get("https://instance.service-now.com/api/now/attachment/s1/file").mock(
            return_value=Response(200, content=b"data")
        )
        pred = [Predicate("sys_id", "=", "s1")]
        res = adapter_sync.fetch("sys_attachment_content", predicates=pred)
        assert res["content"][0].as_py() == b"data"

# --- Utils Tests ---
# (Keep existing utils tests)
def test_build_query_params(adapter_sync):
    params = adapter_sync._build_query_params(
        columns=["number", "state"],
        predicates=[Predicate("priority", ">", 2)],
        limit=5,
        offset=10,
        order_by=[("created", "DESC")]
    )
    
    assert params["sysparm_fields"] == "number,state"
    assert params["sysparm_limit"] == "5" # min(5, 10)
    assert params["sysparm_offset"] == "10"
    assert "priority>2" in params["sysparm_query"]
    assert "ORDERBYDESCcreated" in params["sysparm_query"]

def test_predicate_to_query_conversions(adapter_sync):
    assert adapter_sync._predicate_to_query(Predicate("a", "=", 1)) == "a=1"
    assert adapter_sync._predicate_to_query(Predicate("a", "!=", 1)) == "a!=1"
    assert adapter_sync._predicate_to_query(Predicate("a", "IS NULL", None)) == "aISEMPTY"
    assert adapter_sync._predicate_to_query(Predicate("a", "IS NOT NULL", None)) == "aISNOTEMPTY"
    assert adapter_sync._predicate_to_query(Predicate("a", "IN", [1, 2])) == "aIN1,2"
    assert adapter_sync._predicate_to_query(Predicate("a", "LIKE", "%val%")) == "aLIKEval"

def test_extract_table_name(adapter_sync):
    assert adapter_sync._extract_table_name("foo") == "foo"
    assert adapter_sync._extract_table_name("public.foo") == "foo"
    assert adapter_sync._extract_table_name('"foo"') == "foo"


# --- Error Handling Tests ---

@pytest.mark.asyncio
async def test_fetch_async_rate_limit(adapter):
    with respx.mock:
        route = respx.get("https://instance.service-now.com/api/now/table/incident").mock(
            side_effect=[
                Response(429, headers={"Retry-After": "1"}),
                Response(200, json={"result": [{"sys_id": "1", "col": "val"}]})
            ]
        )
        
        # Mock sleep to avoid waiting
        with patch("anyio.sleep", return_value=None):
            result = await adapter.fetch_async("incident")
        
        # Calls: 1 (429) + 1 (200) = 2
        assert route.call_count == 2
        assert len(result) == 1

def test_fetch_sync_rate_limit(adapter_sync):
    with respx.mock:
        route = respx.get("https://instance.service-now.com/api/now/table/incident").mock(
            side_effect=[
                Response(429, headers={"Retry-After": "1"}),
                Response(200, json={"result": []})
            ]
        )
        
        # Mock sleep
        with patch("time.sleep", return_value=None):
            adapter_sync.fetch("incident")
            
        assert route.call_count == 2

# --- Advanced Fetch Tests ---

@pytest.mark.asyncio
async def test_fetch_async_parallel_pages(adapter):
    # Setup adapter with small page size to force pagination
    adapter._page_size = 5
    adapter._max_parallel = 2
    
    with respx.mock:
        # Page 1 (offset 0)
        respx.get("https://instance.service-now.com/api/now/table/incident", params={"sysparm_offset": "0", "sysparm_limit": "5"}).mock(
            return_value=Response(200, json={"result": [{"id": f"p1_{i}"} for i in range(5)]})
        )
        # Page 2 (offset 5) - fetched in parallel
        respx.get("https://instance.service-now.com/api/now/table/incident", params={"sysparm_offset": "5", "sysparm_limit": "5"}).mock(
            return_value=Response(200, json={"result": [{"id": f"p2_{i}"} for i in range(5)]})
        )
        # Page 3 (offset 10) - fetched in parallel
        respx.get("https://instance.service-now.com/api/now/table/incident", params={"sysparm_offset": "10", "sysparm_limit": "5"}).mock(
            return_value=Response(200, json={"result": [{"id": f"p3_{i}"} for i in range(5)]})
        )
        # Page 4 (offset 15) - empty/short to stop
        respx.get("https://instance.service-now.com/api/now/table/incident", params={"sysparm_offset": "15", "sysparm_limit": "5"}).mock(
            return_value=Response(200, json={"result": []})
        )
        
        # We need to ensure loop runs. 
        # _fetch_all_pages_async fetches first page, then calculates remaining.
        # If we ask for limit=15, result len=5. remaining=10. max requests=2.
        # It should spawn requests for offset 5 and 10.
        
        result = await adapter.fetch_async("incident", limit=15)
        
        assert len(result) == 15
        
        # Verify order if possible, though parallel might shuffle. 
        # But our list extends in order of completion or index. 
        # The adapter sorts by batch index.
        ids = [x.as_py() for x in result["id"]]
        # usage of "params" in respx matches exactly, so we can be sure 0, 5, 10 were called.
