"""
Unit tests for SaaS Adapters (HubSpot, Shopify, Zendesk, Stripe).
Directly tests the adapters using mocked HTTP responses.
"""

import pytest
import respx
import httpx
from waveql.adapters.hubspot import HubSpotAdapter
from waveql.adapters.shopify import ShopifyAdapter
from waveql.adapters.zendesk import ZendeskAdapter
from waveql.adapters.stripe import StripeAdapter
from waveql.query_planner import Predicate

# --- HubSpot Tests ---

@pytest.mark.asyncio
@respx.mock
async def test_hubspot_fetch():
    adapter = HubSpotAdapter(api_key="fake_key")
    
    # Mock search response
    respx.post("https://api.hubapi.com/crm/v3/objects/contacts/search").mock(
        return_value=httpx.Response(200, json={
            "results": [
                {"id": "1", "properties": {"firstname": "John", "lastname": "Doe", "email": "john@example.com"}},
                {"id": "2", "properties": {"firstname": "Jane", "lastname": "Smith", "email": "jane@example.com"}}
            ],
            "paging": {}
        })
    )
    
    table = await adapter.fetch_async("contacts", columns=["firstname", "lastname", "email"])
    
    assert table.num_rows == 2
    assert "firstname" in table.column_names
    assert table.to_pylist()[0]["firstname"] == "John"

@pytest.mark.asyncio
@respx.mock
async def test_hubspot_predicate_pushdown():
    adapter = HubSpotAdapter(api_key="fake_key")
    
    mock_search = respx.post("https://api.hubapi.com/crm/v3/objects/contacts/search").mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    
    predicates = [Predicate(column="email", operator="=", value="test@example.com")]
    await adapter.fetch_async("contacts", predicates=predicates)
    
    # Verify pushdown
    assert mock_search.called
    import json
    sent_payload = json.loads(mock_search.calls.last.request.content.decode())
    # Check the filter was pushed down correctly
    filters = sent_payload["filterGroups"][0]["filters"]
    assert any(f["propertyName"] == "email" and f["operator"] == "EQ" and f["value"] == "test@example.com" for f in filters)

# --- Shopify Tests ---

@pytest.mark.asyncio
@respx.mock
async def test_shopify_fetch():
    adapter = ShopifyAdapter(host="test-shop", api_key="fake_token")
    
    respx.get("https://test-shop.myshopify.com/admin/api/2024-01/orders.json").mock(
        return_value=httpx.Response(200, json={
            "orders": [
                {"id": 101, "order_number": "ORD-1", "total_price": "50.00"},
                {"id": 102, "order_number": "ORD-2", "total_price": "75.00"}
            ]
        })
    )
    
    table = await adapter.fetch_async("orders")
    assert table.num_rows == 2
    assert table.to_pylist()[0]["order_number"] == "ORD-1"

@pytest.mark.asyncio
@respx.mock
async def test_shopify_pagination():
    adapter = ShopifyAdapter(host="test-shop", api_key="fake_token")
    
    # Use a list to track calls and return different responses
    responses = [
        # First page - with Link header for next page
        httpx.Response(
            200, 
            json={"orders": [{"id": 1}]},
            headers={"Link": '<https://test-shop.myshopify.com/admin/api/2024-01/orders.json?page_info=abc>; rel="next"'}
        ),
        # Second page - no Link header (last page)
        httpx.Response(200, json={"orders": [{"id": 2}]}),
    ]
    call_count = [0]
    
    def side_effect(request):
        idx = min(call_count[0], len(responses) - 1)
        call_count[0] += 1
        return responses[idx]
    
    # Mock all GET requests to orders.json (with or without params)
    respx.get(url__startswith="https://test-shop.myshopify.com/admin/api/2024-01/orders.json").mock(
        side_effect=side_effect
    )
    
    table = await adapter.fetch_async("orders")
    assert table.num_rows == 2
    assert call_count[0] == 2  # Verify both pages were fetched

@pytest.mark.asyncio
@respx.mock
async def test_shopify_update_delete():
    adapter = ShopifyAdapter(host="test-shop", api_key="fake")
    
    # Mock Update (PUT)
    mock_put = respx.put("https://test-shop.myshopify.com/admin/api/2024-01/orders/123.json").mock(
         return_value=httpx.Response(200, json={"order": {"id": 123}})
    )
    
    # Mock Delete (DELETE)
    mock_delete = respx.delete("https://test-shop.myshopify.com/admin/api/2024-01/orders/123.json").mock(
         return_value=httpx.Response(200)
    )

    # Test Update
    await adapter.update_async(
        "orders", 
        {"note": "Updated"}, 
        predicates=[Predicate("id", "=", "123")]
    )
    assert mock_put.called
    
    # Test Delete
    await adapter.delete_async(
        "orders", 
        predicates=[Predicate("id", "=", "123")]
    )
    assert mock_delete.called

# --- Zendesk Tests ---

@pytest.mark.asyncio
@respx.mock
async def test_zendesk_search_pushdown():
    adapter = ZendeskAdapter(host="test.zendesk.com", api_key="fake")
    
    mock_search = respx.get("https://test.zendesk.com/api/v2/search.json").mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    
    predicates = [
        Predicate(column="status", operator="=", value="open"),
        Predicate(column="priority", operator="=", value="high")
    ]
    await adapter.fetch_async("tickets", predicates=predicates)
    
    assert mock_search.called
    query_param = mock_search.calls.last.request.url.params["query"]
    assert "type:ticket" in query_param
    assert "status:open" in query_param
    assert "priority:high" in query_param

@pytest.mark.asyncio
@respx.mock
async def test_zendesk_update_delete():
    adapter = ZendeskAdapter(host="test.zendesk.com", api_key="fake")
    
    # Mock Update
    mock_put = respx.put("https://test.zendesk.com/api/v2/tickets/1.json").mock(
        return_value=httpx.Response(200, json={"ticket": {"id": 1}})
    )
    
    # Mock Delete
    mock_delete = respx.delete("https://test.zendesk.com/api/v2/tickets/1.json").mock(
        return_value=httpx.Response(200)
    )

    await adapter.update_async("tickets", {"status": "solved"}, predicates=[Predicate("id", "=", "1")])
    assert mock_put.called
    
    await adapter.delete_async("tickets", predicates=[Predicate("id", "=", "1")])
    assert mock_delete.called

# --- Stripe Tests ---

@pytest.mark.asyncio
@respx.mock
async def test_stripe_search_vs_list():
    adapter = StripeAdapter(api_key="sk_test")
    
    # Mock Search API for customers
    mock_search = respx.get("https://api.stripe.com/v1/customers/search").mock(
        return_value=httpx.Response(200, json={"data": [{"id": "cust_1"}]})
    )
    
    # 1. Search when predicates exist
    predicates = [Predicate(column="email", operator="=", value="test@example.com")]
    await adapter.fetch_async("customers", predicates=predicates)
    assert mock_search.called
    
    # 2. List when no predicates
    mock_list = respx.get("https://api.stripe.com/v1/customers").mock(
        return_value=httpx.Response(200, json={"data": [{"id": "cust_2"}]})
    )
    await adapter.fetch_async("customers")
    assert mock_list.called

@pytest.mark.asyncio
@respx.mock
async def test_stripe_insert():
    adapter = StripeAdapter(api_key="sk_test")
    
    mock_post = respx.post("https://api.stripe.com/v1/customers").mock(
        return_value=httpx.Response(200, json={"id": "cust_new"})
    )
    
    values = {"email": "new@example.com", "name": "New User"}
    await adapter.insert_async("customers", values)
    
    assert mock_post.called
    # Stripe uses form-data
    sent_body = mock_post.calls.last.request.content.decode()
    assert "email=new%40example.com" in sent_body
    assert "name=New+User" in sent_body

@pytest.mark.asyncio
@respx.mock
async def test_stripe_update_delete():
    adapter = StripeAdapter(api_key="sk_test")
    
    # Mock Update (POST)
    mock_post = respx.post("https://api.stripe.com/v1/customers/cust_1").mock(
        return_value=httpx.Response(200, json={"id": "cust_1"})
    )
    
    # Mock Delete (DELETE)
    mock_delete = respx.delete("https://api.stripe.com/v1/customers/cust_1").mock(
        return_value=httpx.Response(200, json={"id": "cust_1", "deleted": True})
    )

    await adapter.update_async("customers", {"name": "Updated Name"}, predicates=[Predicate("id", "=", "cust_1")])
    assert mock_post.called
    
    await adapter.delete_async("customers", predicates=[Predicate("id", "=", "cust_1")])
    assert mock_delete.called
