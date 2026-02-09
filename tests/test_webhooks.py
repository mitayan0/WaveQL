
import pytest
import json
import hmac
import hashlib
import base64
import time
import requests
import httpx
from unittest.mock import MagicMock, patch
from waveql.webhooks import (
    WebhookServer, WebhookEvent, ShopifyWebhookHandler, StripeWebhookHandler, GenericWebhookHandler
)

# --- Handler Unit Tests ---

def test_shopify_signature_verification():
    handler = ShopifyWebhookHandler()
    secret = "secret"
    body = b'{"test": "data"}'
    
    # Compute signature
    computed = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    sig = base64.b64encode(computed).decode()
    
    event = WebhookEvent("shopify", "test", {}, headers={"X-Shopify-Hmac-SHA256": sig}, raw_body=body)
    assert handler.verify_signature(event, secret) is True
    
    # Invalid secret
    assert handler.verify_signature(event, "wrong") is False

def test_stripe_signature_verification():
    handler = StripeWebhookHandler()
    secret = "whsec_secret"
    body = b'{"type": "charge.succeeded"}'
    timestamp = str(int(time.time()))
    
    signed_payload = f"{timestamp}.{body.decode('utf-8')}"
    computed = hmac.new(secret.encode(), signed_payload.encode(), hashlib.sha256).hexdigest()
    
    sig = f"t={timestamp},v1={computed}"
    
    event = WebhookEvent("stripe", "test", {}, headers={"Stripe-Signature": sig}, raw_body=body)
    assert handler.verify_signature(event, secret) is True
    
    # Old timestamp or bad signature
    bad_sig = f"t={timestamp},v1=wrongsceof"
    bad_event = WebhookEvent("stripe", "test", {}, headers={"Stripe-Signature": bad_sig}, raw_body=body)
    assert handler.verify_signature(bad_event, secret) is False

def test_shopify_parse():
    handler = ShopifyWebhookHandler()
    headers = {"X-Shopify-Topic": "orders/create"}
    body = b'{"id": 123}'
    
    event = handler.parse_event(body, headers)
    assert event.source == "shopify"
    assert event.event_type == "orders/create"
    assert event.payload["id"] == 123
    assert event.raw_body == body

# --- Server Integration Tests ---

@pytest.fixture
def webhook_server():
    # Start on port 0 (ephemeral)
    server = WebhookServer(port=0)
    server.start()
    yield server
    server.stop()

def test_server_invalid_path(webhook_server):
    port = webhook_server.server_port
    url = f"http://localhost:{port}/something"
    
    resp = requests.post(url)
    assert resp.status_code == 404

def test_server_unknown_source(webhook_server):
    port = webhook_server.server_port
    url = f"http://localhost:{port}/webhook/unknown"
    
    resp = requests.post(url)
    assert resp.status_code == 404

def test_server_handler_flow(webhook_server):
    port = webhook_server.server_port
    
    # Register mock handler
    handler = GenericWebhookHandler("generic")
    webhook_server.register_handler("generic", handler)
    
    url = f"http://localhost:{port}/webhook/generic"
    data = {"foo": "bar"}
    headers = {"X-Event-Type": "test_event"}
    
    resp = requests.post(url, json=data, headers=headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    
    # Verify event recorded
    events = webhook_server.get_events("generic")
    assert len(events) == 1
    assert events[0].payload == data
    assert events[0].event_type == "test_event"

def test_server_signature_check(webhook_server):
    port = webhook_server.server_port
    
    # Register handlers with secret
    shopify_handler = ShopifyWebhookHandler()
    webhook_server.register_handler("shopify", shopify_handler)
    webhook_server.set_secret("shopify", "my_secret")
    
    url = f"http://localhost:{port}/webhook/shopify"
    data = {"id": 1}
    body = json.dumps(data).encode()
    
    # Request WITHOUT signature
    resp = requests.post(url, data=body, headers={"X-Shopify-Topic": "test"})
    assert resp.status_code == 401
    
    # Request WITH valid signature
    computed = hmac.new("my_secret".encode(), body, hashlib.sha256).digest()
    sig = base64.b64encode(computed).decode()
    
    resp = requests.post(url, data=body, headers={
        "X-Shopify-Topic": "test",
        "X-Shopify-Hmac-SHA256": sig
    })
    assert resp.status_code == 200

def test_health_check(webhook_server):
    port = webhook_server.server_port
    # Register a handler so it appears in the health check
    webhook_server.register_handler("shopify", ShopifyWebhookHandler())
    resp = requests.get(f"http://localhost:{port}/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "shopify" in data["handlers"]

def test_webhook_event_repr():
    event = WebhookEvent(source="test", event_type="update", payload={})
    assert repr(event) == "WebhookEvent(test:update)"

def test_handler_callbacks():
    mock_cb = MagicMock()
    handler = ShopifyWebhookHandler(on_event=mock_cb)
    event = WebhookEvent(source="shopify", event_type="orders/create", payload={})
    handler.handle(event)
    mock_cb.assert_called_with(event)
    
    # Generic handler callback
    from waveql.webhooks import GenericWebhookHandler
    handler2 = GenericWebhookHandler(source="gen", on_event=mock_cb)
    event2 = WebhookEvent(source="gen", event_type="x", payload={})
    handler2.handle(event2)
    mock_cb.assert_called_with(event2)

def test_stripe_signature_edge_cases():
    handler = StripeWebhookHandler()
    event = WebhookEvent(source="stripe", event_type="x", payload={}, headers={})
    # Missing signature
    assert handler.verify_signature(event, "secret") is False
    
    # Missing t or v1
    event.headers = {"Stripe-Signature": "t=1,v2=foo"}
    assert handler.verify_signature(event, "secret") is False

def test_generic_handler_parse_error():
    from waveql.webhooks import GenericWebhookHandler
    handler = GenericWebhookHandler(source="gen")
    event = handler.parse_event(b"invalid json", {})
    assert event.payload == {"raw": "invalid json"}

def test_server_edge_cases(webhook_server):
    server_url = f"http://localhost:{webhook_server.server_port}"
    # GET 404
    response = httpx.get(f"{server_url}/invalid")
    assert response.status_code == 404
    
    # POST 404 (wrong path)
    response = httpx.post(f"{server_url}/wrong/shopify", content=b"{}")
    assert response.status_code == 404
    
    # POST 404 (unknown source)
    response = httpx.post(f"{server_url}/webhook/ghost", content=b"{}")
    assert response.status_code == 404

def test_server_stop_not_running():
    # Calling stop on non-running server should return immediately
    from waveql.webhooks import WebhookServer
    s = WebhookServer(port=0) # port 0 for random
    s.stop() # Should not raise

def test_shopify_handle_cache_invalidation():
    mock_conn = MagicMock()
    handler = ShopifyWebhookHandler(connection=mock_conn)
    
    event = WebhookEvent("shopify", "orders/create", {}, headers={})
    handler.handle(event)
    mock_conn.invalidate_cache.assert_called_with(table="orders")
    
    # Unknown event
    mock_conn.reset_mock()
    event = WebhookEvent("shopify", "unknown", {}, headers={})
    handler.handle(event)
    mock_conn.invalidate_cache.assert_not_called()

def test_stripe_handle_cache_invalidation():
    mock_conn = MagicMock()
    handler = StripeWebhookHandler(connection=mock_conn)
    
    event = WebhookEvent("stripe", "customer.updated", {}, headers={})
    handler.handle(event)
    mock_conn.invalidate_cache.assert_called_with(table="customers")

def test_server_event_truncation():
    server = WebhookServer(port=0)
    for i in range(1100):
        server.record_event(WebhookEvent("src", "type", {"i": i}))
        
    events = server.get_events()
    assert len(events) == 100
    assert len(server._events) == 1000
    assert server._events[0].payload["i"] == 100 # Oldest should be index 100 (since 0-1099, last 1000 is 100-1099)

def test_server_error_handling(webhook_server):
    port = webhook_server.server_port
    
    # Handler that raises exception
    mock_handler = MagicMock()
    mock_handler.parse_event.side_effect = Exception("Parsing Boom")
    
    webhook_server.register_handler("boom", mock_handler)
    
    url = f"http://localhost:{port}/webhook/boom"
    resp = requests.post(url, data={})
    assert resp.status_code == 500
    assert "Parsing Boom" in resp.text

def test_handler_source_names():
    assert ShopifyWebhookHandler().source_name == "shopify"
    assert StripeWebhookHandler().source_name == "stripe"
    assert GenericWebhookHandler("gen").source_name == "gen"

def test_stripe_parse():
    handler = StripeWebhookHandler()
    body = b'{"type": "evt", "id": "1"}'
    event = handler.parse_event(body, {})
    assert event.source == "stripe"
    assert event.event_type == "evt"

def test_generic_signature_logic():
    handler = GenericWebhookHandler("gen")
    assert handler.verify_signature(None, "secret") is True

def test_server_blocking_start():
    server = WebhookServer(port=0)
    with patch.object(server, 'serve_forever') as mock_serve, \
         patch.object(server, 'shutdown') as mock_shutdown:
        server.start(blocking=True)
        mock_serve.assert_called_once()
        
        server.stop()
        mock_shutdown.assert_called_once()
