"""
Unified Authentication Manager

Supports:
- Basic Auth (username/password)
- API Key (header or query param)
- OAuth2 (multiple grant types with automatic token refresh)
- JWT (static token)

OAuth2 Grant Types Supported:
- client_credentials: Machine-to-machine (M2M) authentication
- password: Resource owner password credentials  
- refresh_token: Refresh existing access tokens
- authorization_code: (requires external browser flow)
"""

from __future__ import annotations
import base64
import logging
import threading
import time
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

import requests
import httpx

from waveql.exceptions import AuthenticationError

logger = logging.getLogger(__name__)


@dataclass
class TokenInfo:
    """OAuth2 token information."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    expires_at: float = 0.0
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    
    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """Check if token is expired (with buffer)."""
        if self.expires_at == 0:
            return False  # No expiry set
        return time.time() >= (self.expires_at - buffer_seconds)
    
    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "TokenInfo":
        """Create TokenInfo from OAuth2 token response."""
        expires_at = data.get("expires_at", 0)
        if not expires_at and "expires_in" in data:
            expires_at = time.time() + data["expires_in"]
        
        return cls(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_in=data.get("expires_in", 3600),
            expires_at=expires_at,
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope"),
        )


class BaseAuthManager(ABC):
    """Abstract base class for authentication managers."""
    
    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """Get authentication headers for requests."""
        pass
    
    async def get_headers_async(self) -> Dict[str, str]:
        """Get authentication headers for requests (async)."""
        return self.get_headers()
    
    @property
    @abstractmethod
    def auth_type(self) -> str:
        """Return the authentication type."""
        pass
    
    @property
    def is_authenticated(self) -> bool:
        """Check if credentials are configured."""
        return True


class BasicAuthManager(BaseAuthManager):
    """
    Basic Authentication (RFC 7617).
    
    Encodes username:password as Base64 in Authorization header.
    """
    
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        self._encoded = base64.b64encode(
            f"{username}:{password}".encode()
        ).decode()
    
    def get_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Basic {self._encoded}"}
    
    @property
    def auth_type(self) -> str:
        return "basic"


class APIKeyAuthManager(BaseAuthManager):
    """
    API Key Authentication.
    
    Supports placing API key in:
    - Custom header (default: X-API-Key)
    - Authorization header as Bearer token
    - Query parameter (handled separately)
    """
    
    def __init__(
        self,
        api_key: str,
        header_name: str = "X-API-Key",
        prefix: str = "",
    ):
        """
        Initialize API Key auth.
        
        Args:
            api_key: The API key value
            header_name: Header name to use (e.g., "X-API-Key", "Authorization")
            prefix: Optional prefix (e.g., "Bearer ", "ApiKey ")
        """
        self._api_key = api_key
        self._header_name = header_name
        self._prefix = prefix
    
    def get_headers(self) -> Dict[str, str]:
        value = f"{self._prefix}{self._api_key}" if self._prefix else self._api_key
        return {self._header_name: value}
    
    @property
    def auth_type(self) -> str:
        return "api_key"
    
    def get_query_params(self) -> Dict[str, str]:
        """Get API key as query parameter (alternative to header)."""
        return {"api_key": self._api_key}


class OAuth2Manager(BaseAuthManager):
    """
    OAuth2 Authentication Manager.
    
    Features:
    - Multiple grant types (client_credentials, password, refresh_token)
    - Automatic token refresh before expiry
    - Thread-safe token management
    - Token refresh callbacks for persistence
    
    Example Usage:
        # Client Credentials (M2M)
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="secret",
            grant_type="client_credentials",
        )
        
        # Password Grant
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="secret",
            grant_type="password",
            username="user@example.com",
            password="userpass",
        )
        
        # Pre-existing Token with Refresh
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="secret",
            access_token="existing-token",
            refresh_token="refresh-token",
        )
    """
    
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str = None,
        grant_type: str = "client_credentials",
        scope: str = None,
        # For password grant
        username: str = None,
        password: str = None,
        # For existing tokens
        access_token: str = None,
        refresh_token: str = None,
        expires_at: float = None,
        # Configuration
        refresh_buffer_seconds: int = 60,
        timeout: int = 30,
        verify_ssl: bool = True,
        extra_token_params: Dict[str, str] = None,
        # For authorization code flow
        authorization_url: str = None,
        redirect_uri: str = None,
    ):
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._grant_type = grant_type
        self._scope = scope
        self._username = username
        self._password = password
        self._authorization_url = authorization_url
        self._redirect_uri = redirect_uri
        self._refresh_buffer = refresh_buffer_seconds
        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._extra_params = extra_token_params or {}
        
        # Token state
        self._token: Optional[TokenInfo] = None
        self._token_lock = threading.Lock()
        self._on_token_refresh: Optional[Callable[[TokenInfo], None]] = None
        
        # Initialize with existing token if provided
        if access_token:
            self._token = TokenInfo(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=expires_at or 0,
            )
        # Otherwise, fetch initial token for client_credentials if configured
        elif grant_type == "client_credentials":
            self._fetch_token()

    def get_authorization_url(self, state: str = None) -> str:
        """
        Generate URL for user consent (Authorization Code flow).
        
        Args:
            state: Optional state parameter for CSRF protection
            
        Returns:
            Full authorization URL to redirect user to
        """
        if not self._authorization_url:
            raise ValueError("authorization_url must be provided for Authorization Code flow")
            
        params = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
        }
        if self._scope:
            params["scope"] = self._scope
        if state:
            params["state"] = state
            
        return f"{self._authorization_url}?{urllib.parse.urlencode(params)}"

    def exchange_code(self, code: str) -> TokenInfo:
        """
        Exchange authorization code for access token.
        
        Args:
            code: The authorization code received from the callback
            
        Returns:
            TokenInfo object
        """
        data = {
            "grant_type": "authorization_code",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "code": code,
            "redirect_uri": self._redirect_uri,
            **self._extra_params
        }
        
        return self._request_token(data)
    
    def get_headers(self) -> Dict[str, str]:
        """Get Authorization header with valid access token."""
        self._ensure_valid_token()
        
        if self._token:
            return {"Authorization": f"Bearer {self._token.access_token}"}
        return {}

    async def get_headers_async(self) -> Dict[str, str]:
        """Get Authorization header with valid access token (async)."""
        await self._ensure_valid_token_async()
        
        if self._token:
            return {"Authorization": f"Bearer {self._token.access_token}"}
        return {}
    
    @property
    def auth_type(self) -> str:
        return "oauth2"
    
    @property
    def is_authenticated(self) -> bool:
        return self._token is not None
    
    @property
    def token(self) -> Optional[TokenInfo]:
        """Get current token info."""
        return self._token
    
    def set_token_refresh_callback(self, callback: Callable[[TokenInfo], None]):
        """
        Set callback for when token is refreshed.
        
        Use this to persist tokens to storage.
        
        Args:
            callback: Function called with new TokenInfo after refresh
        """
        self._on_token_refresh = callback
    
    def force_refresh(self) -> TokenInfo:
        """Force refresh the access token."""
        with self._token_lock:
            if self._token and self._token.refresh_token:
                return self._refresh_token()
            else:
                return self._fetch_token()
    
    def _ensure_valid_token(self):
        """Ensure we have a valid, non-expired token."""
        if not self._token:
            self._fetch_token()
            return
        
        if self._token.is_expired(self._refresh_buffer):
            with self._token_lock:
                # Double-check after acquiring lock
                if self._token.is_expired(self._refresh_buffer):
                    if self._token.refresh_token:
                        self._refresh_token()
                    else:
                        self._fetch_token()

    async def _ensure_valid_token_async(self):
        """Ensure we have a valid, non-expired token (async)."""
        if not self._token:
            await self._fetch_token_async()
            return
        
        if self._token.is_expired(self._refresh_buffer):
            # We don't use threading.Lock for async to keep it simple, 
            # but ideally use anyio.Lock
            if self._token.refresh_token:
                await self._refresh_token_async()
            else:
                await self._fetch_token_async()
    
    def _fetch_token(self) -> TokenInfo:
        """Fetch new access token using configured grant type."""
        data = self._get_token_request_data()
        return self._request_token(data)

    async def _fetch_token_async(self) -> TokenInfo:
        """Fetch new access token using configured grant type (async)."""
        data = self._get_token_request_data()
        return await self._request_token_async(data)

    def _get_token_request_data(self) -> Dict:
        """Helper to build token request data."""
        data = {
            "grant_type": self._grant_type,
            "client_id": self._client_id,
            **self._extra_params,
        }
        
        if self._client_secret:
            data["client_secret"] = self._client_secret
        
        if self._scope:
            data["scope"] = self._scope
        
        if self._grant_type == "password":
            if not self._username or not self._password:
                raise ValueError("Password grant requires username and password")
            data["username"] = self._username
            data["password"] = self._password
        return data
    
    def _refresh_token(self) -> TokenInfo:
        """Refresh access token using refresh token."""
        data = self._get_refresh_request_data()
        if data is None: return self._fetch_token()
        
        try:
            return self._request_token(data)
        except Exception as e:
            logger.warning(f"Token refresh failed, will fetch new token: {e}")
            return self._fetch_token()

    async def _refresh_token_async(self) -> TokenInfo:
        """Refresh access token using refresh token (async)."""
        data = self._get_refresh_request_data()
        if data is None: return await self._fetch_token_async()
        
        try:
            return await self._request_token_async(data)
        except Exception as e:
            logger.warning(f"Token refresh failed, will fetch new token: {e}")
            return await self._fetch_token_async()

    def _get_refresh_request_data(self) -> Optional[Dict]:
        """Helper to build refresh request data."""
        if not self._token or not self._token.refresh_token:
            return None
        
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._token.refresh_token,
            "client_id": self._client_id,
        }
        
        if self._client_secret:
            data["client_secret"] = self._client_secret
        return data

    def _request_token(self, data: Dict) -> TokenInfo:
        """Perform request to token endpoint."""
        try:
            response = requests.post(
                self._token_url,
                data=data,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
            response.raise_for_status()
            return self._process_token_response(response.json())
        except requests.RequestException as e:
            logger.error(f"Failed to request OAuth2 token: {e}")
            raise AuthenticationError(f"OAuth2 token request failed: {e}") from e

    async def _request_token_async(self, data: Dict) -> TokenInfo:
        """Perform request to token endpoint (async)."""
        try:
            async with httpx.AsyncClient(verify=self._verify_ssl) as client:
                response = await client.post(
                    self._token_url,
                    data=data,
                    timeout=self._timeout,
                )
                response.raise_for_status()
                return self._process_token_response(response.json())
        except httpx.HTTPError as e:
            logger.error(f"Failed to request OAuth2 token (async): {e}")
            raise AuthenticationError(f"OAuth2 token request failed: {e}") from e

    def _process_token_response(self, token_data: Dict) -> TokenInfo:
        """Shared logic to process token JSON."""
        # Keep old refresh token if new one not provided and we had one
        if self._token and self._token.refresh_token and "refresh_token" not in token_data:
            token_data["refresh_token"] = self._token.refresh_token
        
        self._token = TokenInfo.from_response(token_data)
        logger.debug(f"Fetched/Refreshed OAuth2 token, expires at {self._token.expires_at}")
        
        if self._on_token_refresh:
            self._on_token_refresh(self._token)
        
        return self._token


class JWTAuthManager(BaseAuthManager):
    """
    JWT/Bearer Token Authentication.
    
    For static JWT tokens (not auto-refreshed).
    """
    
    def __init__(self, token: str):
        self._token = token
    
    def get_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}
    
    @property
    def auth_type(self) -> str:
        return "jwt"
    
    def update_token(self, token: str):
        """Update the JWT token."""
        self._token = token


# Unified AuthManager class for backward compatibility
class AuthManager(BaseAuthManager):
    """
    Unified Authentication Manager (facade).
    
    Automatically selects the appropriate auth strategy based on provided credentials.
    """
    
    def __init__(
        self,
        # Basic Auth
        username: str = None,
        password: str = None,
        # API Key
        api_key: str = None,
        api_key_header: str = "X-API-Key",
        # OAuth2
        oauth_token: str = None,
        oauth_refresh_token: str = None,
        oauth_token_url: str = None,
        oauth_client_id: str = None,
        oauth_client_secret: str = None,
        oauth_grant_type: str = "client_credentials",
        oauth_scope: str = None,
        # JWT
        jwt_token: str = None,
        **kwargs
    ):
        self._delegate: Optional[BaseAuthManager] = None
        
        # Determine auth type and create appropriate manager
        if oauth_token or (oauth_client_id and oauth_token_url):
            # OAuth2
            self._delegate = OAuth2Manager(
                token_url=oauth_token_url,
                client_id=oauth_client_id,
                client_secret=oauth_client_secret,
                grant_type=oauth_grant_type,
                scope=oauth_scope,
                access_token=oauth_token,
                refresh_token=oauth_refresh_token,
                username=username,  # For password grant
                password=password,
            )
        elif api_key:
            # API Key
            self._delegate = APIKeyAuthManager(
                api_key=api_key,
                header_name=api_key_header,
            )
        elif username and password:
            # Basic Auth
            self._delegate = BasicAuthManager(
                username=username,
                password=password,
            )
        elif jwt_token:
            # JWT
            self._delegate = JWTAuthManager(token=jwt_token)
    
    def get_headers(self) -> Dict[str, str]:
        if self._delegate:
            return self._delegate.get_headers()
        return {}

    async def get_headers_async(self) -> Dict[str, str]:
        if self._delegate:
            return await self._delegate.get_headers_async()
        return {}
    
    @property
    def auth_type(self) -> Optional[str]:
        if self._delegate:
            return self._delegate.auth_type
        return None
    
    @property
    def is_authenticated(self) -> bool:
        return self._delegate is not None
    
    def set_token_refresh_callback(self, callback: Callable):
        """Set callback for OAuth2 token refresh (passthrough)."""
        if isinstance(self._delegate, OAuth2Manager):
            self._delegate.set_token_refresh_callback(callback)


def create_auth_manager(
    auth_type: str = None,
    **kwargs
) -> Optional[BaseAuthManager]:
    """
    Factory function to create the appropriate auth manager.
    
    Args:
        auth_type: Explicit auth type ("basic", "api_key", "oauth2", "jwt")
        **kwargs: Auth credentials
        
    Returns:
        Configured auth manager or None
        
    Example:
        # Basic Auth
        auth = create_auth_manager("basic", username="user", password="pass")
        
        # OAuth2 Client Credentials
        auth = create_auth_manager(
            "oauth2",
            oauth_token_url="https://auth.example.com/token",
            oauth_client_id="client-id",
            oauth_client_secret="secret",
        )
    """
    if auth_type == "basic":
        return BasicAuthManager(
            username=kwargs.get("username"),
            password=kwargs.get("password"),
        )
    elif auth_type == "api_key":
        return APIKeyAuthManager(
            api_key=kwargs.get("api_key"),
            header_name=kwargs.get("api_key_header", "X-API-Key"),
            prefix=kwargs.get("api_key_prefix", ""),
        )
    elif auth_type == "oauth2":
        return OAuth2Manager(
            token_url=kwargs.get("oauth_token_url"),
            client_id=kwargs.get("oauth_client_id"),
            client_secret=kwargs.get("oauth_client_secret"),
            grant_type=kwargs.get("oauth_grant_type", "client_credentials"),
            scope=kwargs.get("oauth_scope"),
            access_token=kwargs.get("oauth_token"),
            refresh_token=kwargs.get("oauth_refresh_token"),
            username=kwargs.get("username"),
            password=kwargs.get("password"),
        )
    elif auth_type == "jwt":
        return JWTAuthManager(token=kwargs.get("jwt_token"))
    else:
        # Auto-detect from provided kwargs
        return AuthManager(**kwargs)._delegate
