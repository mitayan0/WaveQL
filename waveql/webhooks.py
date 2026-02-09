"""
WaveQL Webhook Server - Real-time event ingestion from SaaS platforms

This module provides a lightweight HTTP server for receiving webhooks
from platforms like Shopify, Stripe, and others. When events arrive,
the server can invalidate cache entries or update local DuckDB tables.

Usage:
    from waveql.webhooks import WebhookServer, WebhookHandler
    
    server = WebhookServer(port=8080)
    server.register_handler("shopify", ShopifyHandler(connection))
    server.start()
"""

from __future__ import annotations
import hashlib
import hmac
import json
import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class WebhookEvent:
    """
    Represents an incoming webhook event.
    
    Attributes:
        source: Source platform (e.g., "shopify", "stripe")
        event_type: Type of event (e.g., "order.created")
        payload: Event payload data
        headers: HTTP headers
        timestamp: When the event was received
        raw_body: Raw request body for signature verification
    """
    source: str
    event_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    raw_body: bytes = b""
    
    def __repr__(self) -> str:
        return f"WebhookEvent({self.source}:{self.event_type})"


class WebhookHandler(ABC):
    """
    Abstract base class for webhook handlers.
    
    Each platform (Shopify, Stripe, etc.) should have its own handler.
    """
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the source platform name."""
        pass
    
    @abstractmethod
    def verify_signature(self, event: WebhookEvent, secret: str) -> bool:
        """
        Verify the webhook signature.
        
        Args:
            event: The webhook event with raw_body and headers
            secret: The webhook secret for this source
            
        Returns:
            True if signature is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def parse_event(self, raw_body: bytes, headers: Dict[str, str]) -> WebhookEvent:
        """
        Parse raw request into a WebhookEvent.
        
        Args:
            raw_body: Raw HTTP body
            headers: HTTP headers
            
        Returns:
            Parsed WebhookEvent
        """
        pass
    
    @abstractmethod
    def handle(self, event: WebhookEvent) -> None:
        """
        Handle the webhook event.
        
        This is where you implement cache invalidation, upserts, etc.
        
        Args:
            event: The parsed webhook event
        """
        pass


class ShopifyWebhookHandler(WebhookHandler):
    """Handler for Shopify webhooks."""
    
    def __init__(self, connection=None, on_event: Optional[Callable] = None):
        """
        Initialize Shopify handler.
        
        Args:
            connection: WaveQL connection for cache invalidation
            on_event: Optional callback for custom event handling
        """
        self._connection = connection
        self._on_event = on_event
    
    @property
    def source_name(self) -> str:
        return "shopify"
    
    def verify_signature(self, event: WebhookEvent, secret: str) -> bool:
        """Verify Shopify HMAC signature."""
        signature = event.headers.get("X-Shopify-Hmac-SHA256", "")
        if not signature:
            return False
        
        computed = hmac.new(
            secret.encode("utf-8"),
            event.raw_body,
            hashlib.sha256
        ).digest()
        
        import base64
        expected = base64.b64encode(computed).decode("utf-8")
        return hmac.compare_digest(signature, expected)
    
    def parse_event(self, raw_body: bytes, headers: Dict[str, str]) -> WebhookEvent:
        """Parse Shopify webhook."""
        payload = json.loads(raw_body.decode("utf-8"))
        event_type = headers.get("X-Shopify-Topic", "unknown")
        
        return WebhookEvent(
            source="shopify",
            event_type=event_type,
            payload=payload,
            headers=headers,
            raw_body=raw_body,
        )
    
    def handle(self, event: WebhookEvent) -> None:
        """Handle Shopify webhook - invalidate cache for affected table."""
        logger.info(f"Shopify webhook: {event.event_type}")
        
        # Map event types to tables
        table_map = {
            "orders/create": "orders",
            "orders/updated": "orders",
            "orders/cancelled": "orders",
            "products/create": "products",
            "products/update": "products",
            "products/delete": "products",
            "customers/create": "customers",
            "customers/update": "customers",
        }
        
        table = table_map.get(event.event_type)
        
        if table and self._connection:
            # Invalidate cache for this table
            self._connection.invalidate_cache(table=table)
            logger.info(f"Invalidated cache for table: {table}")
        
        if self._on_event:
            self._on_event(event)


class StripeWebhookHandler(WebhookHandler):
    """Handler for Stripe webhooks."""
    
    def __init__(self, connection=None, on_event: Optional[Callable] = None):
        """
        Initialize Stripe handler.
        
        Args:
            connection: WaveQL connection for cache invalidation
            on_event: Optional callback for custom event handling
        """
        self._connection = connection
        self._on_event = on_event
    
    @property
    def source_name(self) -> str:
        return "stripe"
    
    def verify_signature(self, event: WebhookEvent, secret: str) -> bool:
        """Verify Stripe signature."""
        signature = event.headers.get("Stripe-Signature", "")
        if not signature:
            return False
        
        # Parse the signature header
        parts = dict(item.split("=") for item in signature.split(","))
        timestamp = parts.get("t", "")
        v1_signature = parts.get("v1", "")
        
        if not timestamp or not v1_signature:
            return False
        
        # Compute expected signature
        signed_payload = f"{timestamp}.{event.raw_body.decode('utf-8')}"
        computed = hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(v1_signature, computed)
    
    def parse_event(self, raw_body: bytes, headers: Dict[str, str]) -> WebhookEvent:
        """Parse Stripe webhook."""
        payload = json.loads(raw_body.decode("utf-8"))
        event_type = payload.get("type", "unknown")
        
        return WebhookEvent(
            source="stripe",
            event_type=event_type,
            payload=payload,
            headers=headers,
            raw_body=raw_body,
        )
    
    def handle(self, event: WebhookEvent) -> None:
        """Handle Stripe webhook - invalidate cache for affected table."""
        logger.info(f"Stripe webhook: {event.event_type}")
        
        # Map event types to tables
        table_map = {
            "customer.created": "customers",
            "customer.updated": "customers",
            "customer.deleted": "customers",
            "payment_intent.succeeded": "payment_intents",
            "payment_intent.failed": "payment_intents",
            "invoice.paid": "invoices",
            "invoice.payment_failed": "invoices",
            "subscription.created": "subscriptions",
            "subscription.updated": "subscriptions",
            "subscription.deleted": "subscriptions",
        }
        
        table = table_map.get(event.event_type)
        
        if table and self._connection:
            self._connection.invalidate_cache(table=table)
            logger.info(f"Invalidated cache for table: {table}")
        
        if self._on_event:
            self._on_event(event)


class GenericWebhookHandler(WebhookHandler):
    """Generic handler for any webhook source."""
    
    def __init__(
        self,
        source: str,
        connection=None,
        on_event: Optional[Callable] = None,
        event_type_header: str = "X-Event-Type",
    ):
        """
        Initialize generic handler.
        
        Args:
            source: Source name for this handler
            connection: WaveQL connection
            on_event: Event callback
            event_type_header: Header containing event type
        """
        self._source = source
        self._connection = connection
        self._on_event = on_event
        self._event_type_header = event_type_header
    
    @property
    def source_name(self) -> str:
        return self._source
    
    def verify_signature(self, event: WebhookEvent, secret: str) -> bool:
        """Generic signature verification (always returns True by default)."""
        # Override in subclass for specific verification
        return True
    
    def parse_event(self, raw_body: bytes, headers: Dict[str, str]) -> WebhookEvent:
        """Parse generic webhook."""
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            payload = {"raw": raw_body.decode("utf-8", errors="replace")}
        
        event_type = headers.get(self._event_type_header, "unknown")
        
        return WebhookEvent(
            source=self._source,
            event_type=event_type,
            payload=payload,
            headers=headers,
            raw_body=raw_body,
        )
    
    def handle(self, event: WebhookEvent) -> None:
        """Handle generic webhook."""
        logger.info(f"{self._source} webhook: {event.event_type}")
        
        if self._on_event:
            self._on_event(event)


class WebhookRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for webhooks."""
    
    server: "WebhookServer"  # Type hint for parent server
    
    def log_message(self, format, *args):
        """Override to use logger instead of stderr."""
        logger.debug(f"Webhook request: {format % args}")
    
    def do_POST(self):
        """Handle POST requests (webhooks)."""
        # Parse URL to get handler route
        parsed = urlparse(self.path)
        path_parts = parsed.path.strip("/").split("/")
        
        if len(path_parts) < 2 or path_parts[0] != "webhook":
            self.send_error(404, "Not found")
            return
        
        source = path_parts[1]
        
        # Get handler for this source
        handler = self.server.get_handler(source)
        if not handler:
            self.send_error(404, f"No handler for source: {source}")
            return
        
        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length)
        
        # Convert headers to dict
        headers = {k: v for k, v in self.headers.items()}
        
        try:
            # Parse event
            event = handler.parse_event(raw_body, headers)
            
            # Verify signature if secret is configured
            secret = self.server.get_secret(source)
            if secret:
                if not handler.verify_signature(event, secret):
                    logger.warning(f"Invalid signature for {source} webhook")
                    self.send_error(401, "Invalid signature")
                    return
            
            # Handle event
            handler.handle(event)
            
            # Record event
            self.server.record_event(event)
            
            # Send success response
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
            
        except Exception as e:
            logger.error(f"Error handling webhook: {e}")
            self.send_error(500, str(e))
    
    def do_GET(self):
        """Handle GET requests (health check)."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "healthy",
                "handlers": list(self.server._handlers.keys()),
            }).encode())
        else:
            self.send_error(404, "Not found")


class WebhookServer(HTTPServer):
    """
    HTTP server for receiving webhooks.
    
    Supports multiple sources (Shopify, Stripe, etc.) on different routes:
    - POST /webhook/shopify
    - POST /webhook/stripe
    - GET /health
    
    Example:
        server = WebhookServer(port=8080)
        server.register_handler("shopify", ShopifyWebhookHandler(conn))
        server.set_secret("shopify", "shpss_...")
        server.start()  # Starts in background thread
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8080):
        """
        Initialize webhook server.
        
        Args:
            host: Host to bind to (default: all interfaces)
            port: Port to listen on
        """
        super().__init__((host, port), WebhookRequestHandler)
        self._handlers: Dict[str, WebhookHandler] = {}
        self._secrets: Dict[str, str] = {}
        self._events: List[WebhookEvent] = []
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
        
        logger.info(f"WebhookServer initialized on {host}:{port}")
    
    def register_handler(self, source: str, handler: WebhookHandler) -> None:
        """
        Register a webhook handler for a source.
        
        Args:
            source: Source name (e.g., "shopify")
            handler: Handler instance
        """
        self._handlers[source] = handler
        logger.info(f"Registered webhook handler for: {source}")
    
    def get_handler(self, source: str) -> Optional[WebhookHandler]:
        """Get handler for a source."""
        return self._handlers.get(source)
    
    def set_secret(self, source: str, secret: str) -> None:
        """
        Set the webhook secret for signature verification.
        
        Args:
            source: Source name
            secret: Webhook signing secret
        """
        self._secrets[source] = secret
    
    def get_secret(self, source: str) -> Optional[str]:
        """Get secret for a source."""
        return self._secrets.get(source)
    
    def record_event(self, event: WebhookEvent) -> None:
        """Record an event for later retrieval."""
        with self._lock:
            self._events.append(event)
            # Keep only last 1000 events
            if len(self._events) > 1000:
                self._events = self._events[-1000:]
    
    def get_events(self, source: Optional[str] = None, limit: int = 100) -> List[WebhookEvent]:
        """
        Get recorded events.
        
        Args:
            source: Optional filter by source
            limit: Maximum events to return
            
        Returns:
            List of WebhookEvent objects
        """
        with self._lock:
            events = self._events.copy()
        
        if source:
            events = [e for e in events if e.source == source]
        
        return events[-limit:]
    
    def start(self, blocking: bool = False) -> None:
        """
        Start the webhook server.
        
        Args:
            blocking: If True, run in foreground. If False, run in background thread.
        """
        self._running = True
        
        if blocking:
            logger.info(f"Starting webhook server (blocking) on port {self.server_port}")
            self.serve_forever()
        else:
            self._thread = threading.Thread(target=self.serve_forever, daemon=True)
            self._thread.start()
            logger.info(f"Started webhook server (background) on port {self.server_port}")
    
    def stop(self) -> None:
        """Stop the webhook server."""
        if not self._running:
            return
            
        self._running = False
        self.shutdown()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Webhook server stopped")
