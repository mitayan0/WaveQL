"""
Tests for WaveQL Webhook Server
"""

import base64
import hashlib
import hmac
import json
import time
import threading
from unittest.mock import MagicMock, patch

import pytest

from waveql.webhooks import (
    WebhookEvent,
    WebhookHandler,
    ShopifyWebhookHandler,
    StripeWebhookHandler,
    GenericWebhookHandler,
    WebhookServer,
)


class TestWebhookEvent:
    """Tests for WebhookEvent dataclass."""
    
    def test_create_event(self):
        """Test creating a webhook event."""
        event = WebhookEvent(
            source="shopify",
            event_type="orders/create",
            payload={"id": 12345},
        )
        
        assert event.source == "shopify"
        assert event.event_type == "orders/create"
        assert event.payload == {"id": 12345}
    
    def test_repr(self):
        """Test string representation."""
        event = WebhookEvent(
            source="stripe",
            event_type="customer.created",
            payload={},
        )
        
        assert "stripe" in repr(event)
        assert "customer.created" in repr(event)


class TestShopifyWebhookHandler:
    """Tests for ShopifyWebhookHandler."""
    
    @pytest.fixture
    def handler(self):
        """Create a Shopify handler."""
        return ShopifyWebhookHandler()
    
    def test_source_name(self, handler):
        """Test source name."""
        assert handler.source_name == "shopify"
    
    def test_parse_event(self, handler):
        """Test parsing a Shopify webhook."""
        raw_body = json.dumps({"id": 12345, "name": "Test Order"}).encode()
        headers = {"X-Shopify-Topic": "orders/create"}
        
        event = handler.parse_event(raw_body, headers)
        
        assert event.source == "shopify"
        assert event.event_type == "orders/create"
        assert event.payload["id"] == 12345
    
    def test_verify_signature_valid(self, handler):
        """Test valid signature verification."""
        secret = "test_secret"
        raw_body = b'{"test": "data"}'
        
        # Compute valid signature
        computed = hmac.new(
            secret.encode("utf-8"),
            raw_body,
            hashlib.sha256
        ).digest()
        signature = base64.b64encode(computed).decode("utf-8")
        
        event = WebhookEvent(
            source="shopify",
            event_type="test",
            payload={},
            headers={"X-Shopify-Hmac-SHA256": signature},
            raw_body=raw_body,
        )
        
        assert handler.verify_signature(event, secret) is True
    
    def test_verify_signature_invalid(self, handler):
        """Test invalid signature verification."""
        event = WebhookEvent(
            source="shopify",
            event_type="test",
            payload={},
            headers={"X-Shopify-Hmac-SHA256": "invalid"},
            raw_body=b'{"test": "data"}',
        )
        
        assert handler.verify_signature(event, "secret") is False
    
    def test_verify_signature_missing(self, handler):
        """Test missing signature."""
        event = WebhookEvent(
            source="shopify",
            event_type="test",
            payload={},
            headers={},
            raw_body=b'{}',
        )
        
        assert handler.verify_signature(event, "secret") is False
    
    def test_handle_with_callback(self):
        """Test handle with custom callback."""
        events_received = []
        
        def on_event(event):
            events_received.append(event)
        
        handler = ShopifyWebhookHandler(on_event=on_event)
        event = WebhookEvent(
            source="shopify",
            event_type="orders/create",
            payload={"id": 1},
        )
        
        handler.handle(event)
        
        assert len(events_received) == 1
        assert events_received[0] == event


class TestStripeWebhookHandler:
    """Tests for StripeWebhookHandler."""
    
    @pytest.fixture
    def handler(self):
        """Create a Stripe handler."""
        return StripeWebhookHandler()
    
    def test_source_name(self, handler):
        """Test source name."""
        assert handler.source_name == "stripe"
    
    def test_parse_event(self, handler):
        """Test parsing a Stripe webhook."""
        payload = {
            "type": "customer.created",
            "data": {"object": {"id": "cus_123"}}
        }
        raw_body = json.dumps(payload).encode()
        headers = {}
        
        event = handler.parse_event(raw_body, headers)
        
        assert event.source == "stripe"
        assert event.event_type == "customer.created"
    
    def test_verify_signature_valid(self, handler):
        """Test valid Stripe signature verification."""
        secret = "whsec_test"
        timestamp = str(int(time.time()))
        raw_body = b'{"test": "data"}'
        
        # Compute signature
        signed_payload = f"{timestamp}.{raw_body.decode('utf-8')}"
        v1_signature = hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        signature_header = f"t={timestamp},v1={v1_signature}"
        
        event = WebhookEvent(
            source="stripe",
            event_type="test",
            payload={},
            headers={"Stripe-Signature": signature_header},
            raw_body=raw_body,
        )
        
        assert handler.verify_signature(event, secret) is True
    
    def test_verify_signature_missing(self, handler):
        """Test missing signature."""
        event = WebhookEvent(
            source="stripe",
            event_type="test",
            payload={},
            headers={},
            raw_body=b'{}',
        )
        
        assert handler.verify_signature(event, "secret") is False


class TestGenericWebhookHandler:
    """Tests for GenericWebhookHandler."""
    
    def test_custom_source(self):
        """Test custom source name."""
        handler = GenericWebhookHandler(source="my_service")
        
        assert handler.source_name == "my_service"
    
    def test_parse_event(self):
        """Test parsing generic webhook."""
        handler = GenericWebhookHandler(
            source="custom",
            event_type_header="X-Custom-Event"
        )
        
        raw_body = json.dumps({"data": "test"}).encode()
        headers = {"X-Custom-Event": "user.updated"}
        
        event = handler.parse_event(raw_body, headers)
        
        assert event.source == "custom"
        assert event.event_type == "user.updated"
    
    def test_verify_signature_default(self):
        """Test default signature verification returns True."""
        handler = GenericWebhookHandler(source="test")
        event = WebhookEvent(source="test", event_type="test", payload={})
        
        assert handler.verify_signature(event, "any") is True


class TestWebhookServer:
    """Tests for WebhookServer."""
    
    @pytest.fixture
    def server(self):
        """Create a test server on a random port."""
        # Use port 0 to get a random available port
        server = WebhookServer(host="127.0.0.1", port=0)
        yield server
        # Ensure clean shutdown
        try:
            server.stop()
        except Exception:
            pass
        # Force close the socket on Windows
        try:
            server.server_close()
        except Exception:
            pass
    
    def test_register_handler(self, server):
        """Test registering a handler."""
        handler = ShopifyWebhookHandler()
        server.register_handler("shopify", handler)
        
        assert server.get_handler("shopify") == handler
    
    def test_get_handler_not_found(self, server):
        """Test getting non-existent handler."""
        assert server.get_handler("nonexistent") is None
    
    def test_set_secret(self, server):
        """Test setting webhook secret."""
        server.set_secret("shopify", "test_secret")
        
        assert server.get_secret("shopify") == "test_secret"
    
    def test_record_and_get_events(self, server):
        """Test recording and retrieving events."""
        event1 = WebhookEvent(source="shopify", event_type="e1", payload={})
        event2 = WebhookEvent(source="stripe", event_type="e2", payload={})
        
        server.record_event(event1)
        server.record_event(event2)
        
        all_events = server.get_events()
        shopify_events = server.get_events(source="shopify")
        
        assert len(all_events) == 2
        assert len(shopify_events) == 1
        assert shopify_events[0].source == "shopify"
    
    def test_event_limit(self, server):
        """Test event list is limited to 1000."""
        for i in range(1100):
            event = WebhookEvent(source="test", event_type=f"e{i}", payload={})
            server.record_event(event)
        
        events = server.get_events(limit=2000)
        
        assert len(events) <= 1000
    
    def test_start_stop_background(self, server):
        """Test starting and stopping in background mode."""
        server.register_handler("shopify", ShopifyWebhookHandler())
        server.start(blocking=False)
        
        # Give it a moment to start
        time.sleep(0.1)
        
        assert server._running is True
        assert server._thread is not None
        
        # Don't check is_alive() as it may race with stop()
        # The fixture will handle cleanup


class TestIntegration:
    """Integration tests for webhook system."""
    
    def test_full_webhook_flow(self):
        """Test complete webhook processing flow."""
        events_received = []
        
        def on_event(event):
            events_received.append(event)
        
        # Create handler
        handler = ShopifyWebhookHandler(on_event=on_event)
        
        # Simulate webhook
        raw_body = json.dumps({
            "id": 12345,
            "email": "test@example.com",
        }).encode()
        headers = {"X-Shopify-Topic": "customers/create"}
        
        # Parse and handle
        event = handler.parse_event(raw_body, headers)
        handler.handle(event)
        
        # Verify
        assert len(events_received) == 1
        assert events_received[0].event_type == "customers/create"
        assert events_received[0].payload["id"] == 12345
    
    def test_multiple_handlers(self):
        """Test server with multiple handlers."""
        server = WebhookServer(host="127.0.0.1", port=0)
        
        try:
            shopify_handler = ShopifyWebhookHandler()
            stripe_handler = StripeWebhookHandler()
            
            server.register_handler("shopify", shopify_handler)
            server.register_handler("stripe", stripe_handler)
            
            assert server.get_handler("shopify") == shopify_handler
            assert server.get_handler("stripe") == stripe_handler
            assert len(server._handlers) == 2
        finally:
            server.stop()
            server.server_close()
