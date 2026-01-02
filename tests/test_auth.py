"""
Tests for WaveQL Authentication Managers

Tests cover:
- Basic Authentication
- API Key Authentication  
- OAuth2 (client_credentials, password, refresh_token)
- JWT Authentication
- Token refresh and expiration handling
"""

import base64
import time
import pytest
import responses
from unittest.mock import Mock, patch

from waveql.auth import (
    AuthManager,
    BasicAuthManager,
    APIKeyAuthManager,
    OAuth2Manager,
    JWTAuthManager,
    TokenInfo,
    AuthenticationError,
    create_auth_manager,
)


class TestBasicAuthManager:
    """Tests for Basic Authentication."""
    
    def test_basic_auth_headers(self):
        """Test Basic Auth header generation."""
        auth = BasicAuthManager(username="admin", password="secret123")
        headers = auth.get_headers()
        
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")
        
        # Decode and verify
        encoded = headers["Authorization"].split(" ")[1]
        decoded = base64.b64decode(encoded).decode()
        assert decoded == "admin:secret123"
    
    def test_basic_auth_type(self):
        """Test auth type property."""
        auth = BasicAuthManager(username="user", password="pass")
        assert auth.auth_type == "basic"
    
    def test_basic_auth_special_chars(self):
        """Test Basic Auth with special characters."""
        auth = BasicAuthManager(username="user@domain.com", password="p@ss:word!")
        headers = auth.get_headers()
        
        encoded = headers["Authorization"].split(" ")[1]
        decoded = base64.b64decode(encoded).decode()
        assert decoded == "user@domain.com:p@ss:word!"


class TestAPIKeyAuthManager:
    """Tests for API Key Authentication."""
    
    def test_default_header(self):
        """Test API key with default header name."""
        auth = APIKeyAuthManager(api_key="my-api-key-123")
        headers = auth.get_headers()
        
        assert headers == {"X-API-Key": "my-api-key-123"}
    
    def test_custom_header(self):
        """Test API key with custom header name."""
        auth = APIKeyAuthManager(
            api_key="key123",
            header_name="X-Custom-Auth",
        )
        headers = auth.get_headers()
        
        assert headers == {"X-Custom-Auth": "key123"}
    
    def test_bearer_style(self):
        """Test API key as Bearer token."""
        auth = APIKeyAuthManager(
            api_key="token123",
            header_name="Authorization",
            prefix="Bearer ",
        )
        headers = auth.get_headers()
        
        assert headers == {"Authorization": "Bearer token123"}
    
    def test_auth_type(self):
        """Test auth type property."""
        auth = APIKeyAuthManager(api_key="key")
        assert auth.auth_type == "api_key"
    
    def test_query_params(self):
        """Test API key as query parameter."""
        auth = APIKeyAuthManager(api_key="key123")
        params = auth.get_query_params()
        
        assert params == {"api_key": "key123"}


class TestOAuth2Manager:
    """Tests for OAuth2 Authentication."""
    
    @responses.activate
    def test_client_credentials_grant(self):
        """Test OAuth2 client credentials flow."""
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={
                "access_token": "access-token-123",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
            status=200,
        )
        
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="my-secret",
            grant_type="client_credentials",
        )
        
        headers = auth.get_headers()
        
        assert headers == {"Authorization": "Bearer access-token-123"}
        
        # Verify request
        assert len(responses.calls) == 1
        request_body = responses.calls[0].request.body
        assert "grant_type=client_credentials" in request_body
        assert "client_id=my-client" in request_body
    
    @responses.activate
    def test_password_grant(self):
        """Test OAuth2 password grant flow."""
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={
                "access_token": "user-token-456",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "refresh-789",
            },
            status=200,
        )
        
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="my-secret",
            grant_type="password",
            username="user@example.com",
            password="userpass",
        )
        
        headers = auth.get_headers()
        
        assert headers == {"Authorization": "Bearer user-token-456"}
        
        # Verify request includes username/password
        request_body = responses.calls[0].request.body
        assert "grant_type=password" in request_body
        assert "username=user%40example.com" in request_body
    
    @responses.activate
    def test_existing_token(self):
        """Test OAuth2 with pre-existing token (no immediate fetch)."""
        # No responses added - should not make any requests
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            access_token="existing-token-abc",
        )
        
        headers = auth.get_headers()
        
        assert headers == {"Authorization": "Bearer existing-token-abc"}
        assert len(responses.calls) == 0  # No token fetch
    
    @responses.activate
    def test_token_refresh(self):
        """Test automatic token refresh when expired."""
        # Token that's already expired
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={
                "access_token": "new-token-after-refresh",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "new-refresh-token",
            },
            status=200,
        )
        
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="my-secret",
            access_token="old-expired-token",
            refresh_token="refresh-token-123",
            expires_at=time.time() - 100,  # Already expired
        )
        
        headers = auth.get_headers()
        
        assert headers == {"Authorization": "Bearer new-token-after-refresh"}
        
        # Verify refresh was called
        request_body = responses.calls[0].request.body
        assert "grant_type=refresh_token" in request_body
        assert "refresh_token=refresh-token-123" in request_body
    
    @responses.activate
    def test_token_refresh_callback(self):
        """Test token refresh callback is called."""
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={
                "access_token": "callback-token",
                "expires_in": 3600,
            },
            status=200,
        )
        
        callback_called = []
        
        def on_refresh(token_info):
            callback_called.append(token_info)
        
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="my-secret",
            grant_type="client_credentials",
        )
        auth.set_token_refresh_callback(on_refresh)
        auth.force_refresh()
        
        assert len(callback_called) == 1
        assert callback_called[0].access_token == "callback-token"
    
    @responses.activate
    def test_scope_parameter(self):
        """Test OAuth2 with scope parameter."""
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={"access_token": "scoped-token", "expires_in": 3600},
            status=200,
        )
        
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="my-secret",
            grant_type="client_credentials",
            scope="read write admin",
        )
        
        auth.get_headers()
        
        request_body = responses.calls[0].request.body
        assert "scope=read+write+admin" in request_body
    
    @responses.activate
    def test_token_fetch_failure(self):
        """Test error handling when token fetch fails."""
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={"error": "invalid_client"},
            status=401,
        )
        
        with pytest.raises(AuthenticationError, match="OAuth2 token request failed"):
            OAuth2Manager(
                token_url="https://auth.example.com/oauth/token",
                client_id="invalid-client",
                client_secret="invalid-secret",
                grant_type="client_credentials",
            )
    
    def test_auth_type(self):
        """Test auth type property."""
        auth = OAuth2Manager(
            token_url="https://auth.example.com/token",
            client_id="client",
            access_token="token",
        )
        assert auth.auth_type == "oauth2"
    
    def test_is_authenticated(self):
        """Test is_authenticated property."""
        auth = OAuth2Manager(
            token_url="https://auth.example.com/token",
            client_id="client",
            access_token="token",
        )
        assert auth.is_authenticated is True


    @responses.activate
    def test_authorization_code_flow(self):
        """Test Authorization Code flow components."""
        auth = OAuth2Manager(
            token_url="https://auth.example.com/oauth/token",
            client_id="my-client",
            client_secret="my-secret",
            grant_type="authorization_code",
            authorization_url="https://auth.example.com/oauth/authorize",
            redirect_uri="https://app.com/callback",
            scope="read"
        )
        
        # 1. Generate Auth URL
        url = auth.get_authorization_url(state="random-state")
        assert "https://auth.example.com/oauth/authorize?" in url
        assert "response_type=code" in url
        assert "client_id=my-client" in url
        # Check params exist (order might vary)
        assert "redirect_uri=https%3A%2F%2Fapp.com%2Fcallback" in url
        assert "scope=read" in url
        assert "state=random-state" in url
        
        # 2. Exchange Code
        responses.add(
            responses.POST,
            "https://auth.example.com/oauth/token",
            json={
                "access_token": "code-token",
                "expires_in": 3600,
                "refresh_token": "refresh-code",
            },
            status=200
        )
        
        token = auth.exchange_code("auth-code-123")
        
        assert token.access_token == "code-token"
        assert auth.token.access_token == "code-token"
        
        # Verify request params
        request_body = responses.calls[0].request.body
        assert "grant_type=authorization_code" in request_body
        assert "code=auth-code-123" in request_body
        assert "redirect_uri=https%3A%2F%2Fapp.com%2Fcallback" in request_body


class TestJWTAuthManager:
    """Tests for JWT Authentication."""
    
    def test_jwt_headers(self):
        """Test JWT Bearer token headers."""
        auth = JWTAuthManager(token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        headers = auth.get_headers()
        
        assert headers == {"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."}
    
    def test_auth_type(self):
        """Test auth type property."""
        auth = JWTAuthManager(token="token")
        assert auth.auth_type == "jwt"
    
    def test_update_token(self):
        """Test updating JWT token."""
        auth = JWTAuthManager(token="old-token")
        auth.update_token("new-token")
        
        headers = auth.get_headers()
        assert headers == {"Authorization": "Bearer new-token"}


class TestTokenInfo:
    """Tests for TokenInfo dataclass."""
    
    def test_is_expired_not_set(self):
        """Test is_expired when expiry not set."""
        token = TokenInfo(access_token="token")
        assert token.is_expired() is False
    
    def test_is_expired_true(self):
        """Test is_expired when token is expired."""
        token = TokenInfo(
            access_token="token",
            expires_at=time.time() - 100,  # 100 seconds ago
        )
        assert token.is_expired() is True
    
    def test_is_expired_with_buffer(self):
        """Test is_expired respects buffer."""
        token = TokenInfo(
            access_token="token",
            expires_at=time.time() + 30,  # 30 seconds from now
        )
        # With 60 second buffer, should be considered expired
        assert token.is_expired(buffer_seconds=60) is True
        # With 10 second buffer, should not be expired
        assert token.is_expired(buffer_seconds=10) is False
    
    def test_from_response(self):
        """Test creating TokenInfo from OAuth response."""
        response_data = {
            "access_token": "access-123",
            "token_type": "Bearer",
            "expires_in": 7200,
            "refresh_token": "refresh-456",
            "scope": "read write",
        }
        
        token = TokenInfo.from_response(response_data)
        
        assert token.access_token == "access-123"
        assert token.token_type == "Bearer"
        assert token.expires_in == 7200
        assert token.refresh_token == "refresh-456"
        assert token.scope == "read write"
        assert token.expires_at > time.time()


class TestUnifiedAuthManager:
    """Tests for the unified AuthManager facade."""
    
    def test_auto_detect_basic(self):
        """Test auto-detection of Basic Auth."""
        auth = AuthManager(username="user", password="pass")
        
        assert auth.auth_type == "basic"
        assert "Authorization" in auth.get_headers()
        assert auth.get_headers()["Authorization"].startswith("Basic ")
    
    def test_auto_detect_api_key(self):
        """Test auto-detection of API Key."""
        auth = AuthManager(api_key="my-key")
        
        assert auth.auth_type == "api_key"
        assert auth.get_headers() == {"X-API-Key": "my-key"}
    
    def test_auto_detect_jwt(self):
        """Test auto-detection of JWT."""
        auth = AuthManager(jwt_token="jwt-token-123")
        
        assert auth.auth_type == "jwt"
        assert auth.get_headers() == {"Authorization": "Bearer jwt-token-123"}
    
    @responses.activate
    def test_auto_detect_oauth2(self):
        """Test auto-detection of OAuth2."""
        responses.add(
            responses.POST,
            "https://auth.example.com/token",
            json={"access_token": "oauth-token", "expires_in": 3600},
            status=200,
        )
        
        auth = AuthManager(
            oauth_token_url="https://auth.example.com/token",
            oauth_client_id="client-id",
            oauth_client_secret="secret",
        )
        
        assert auth.auth_type == "oauth2"
        assert auth.get_headers() == {"Authorization": "Bearer oauth-token"}
    
    def test_no_credentials(self):
        """Test with no credentials provided."""
        auth = AuthManager()
        
        assert auth.auth_type is None
        assert auth.is_authenticated is False
        assert auth.get_headers() == {}


class TestCreateAuthManager:
    """Tests for create_auth_manager factory function."""
    
    def test_create_basic(self):
        """Test creating Basic Auth manager."""
        auth = create_auth_manager("basic", username="u", password="p")
        assert isinstance(auth, BasicAuthManager)
    
    def test_create_api_key(self):
        """Test creating API Key manager."""
        auth = create_auth_manager("api_key", api_key="key123")
        assert isinstance(auth, APIKeyAuthManager)
    
    def test_create_jwt(self):
        """Test creating JWT manager."""
        auth = create_auth_manager("jwt", jwt_token="token")
        assert isinstance(auth, JWTAuthManager)
    
    @responses.activate
    def test_create_oauth2(self):
        """Test creating OAuth2 manager."""
        responses.add(
            responses.POST,
            "https://auth.example.com/token",
            json={"access_token": "token", "expires_in": 3600},
            status=200,
        )
        
        auth = create_auth_manager(
            "oauth2",
            oauth_token_url="https://auth.example.com/token",
            oauth_client_id="client",
            oauth_client_secret="secret",
        )
        assert isinstance(auth, OAuth2Manager)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
