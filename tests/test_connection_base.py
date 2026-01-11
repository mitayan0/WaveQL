"""
Tests for Connection Base - Common functionality for sync and async connections

Tests cover:
- Connection string parsing (various formats)
- OAuth parameter extraction
- AuthManager creation from parameters
- Edge cases (special characters, missing parts, etc.)
"""

import pytest

from waveql.connection_base import ConnectionMixin


class TestParseConnectionString:
    """Tests for connection string parsing."""
    
    def test_simple_adapter_host(self):
        """Test simple adapter://host format."""
        result = ConnectionMixin.parse_connection_string("servicenow://dev.service-now.com")
        
        assert result["adapter"] == "servicenow"
        assert result["host"] == "dev.service-now.com"
        assert result["username"] is None
        assert result["password"] is None
        assert result["port"] is None
    
    def test_with_credentials(self):
        """Test with username and password."""
        result = ConnectionMixin.parse_connection_string(
            "servicenow://admin:secret@dev.service-now.com"
        )
        
        assert result["adapter"] == "servicenow"
        assert result["host"] == "dev.service-now.com"
        assert result["username"] == "admin"
        assert result["password"] == "secret"
    
    def test_with_port(self):
        """Test with port number."""
        result = ConnectionMixin.parse_connection_string(
            "postgres://localhost:5432"
        )
        
        assert result["adapter"] == "postgres"
        assert result["host"] == "localhost"
        assert result["port"] == 5432
    
    def test_with_credentials_and_port(self):
        """Test with credentials and port."""
        result = ConnectionMixin.parse_connection_string(
            "mysql://user:pass@db.example.com:3306"
        )
        
        assert result["adapter"] == "mysql"
        assert result["host"] == "db.example.com"
        assert result["username"] == "user"
        assert result["password"] == "pass"
        assert result["port"] == 3306
    
    def test_with_query_params(self):
        """Test with query parameters."""
        result = ConnectionMixin.parse_connection_string(
            "jira://company.atlassian.net?expand=names&project=PROJ"
        )
        
        assert result["adapter"] == "jira"
        assert result["host"] == "company.atlassian.net"
        assert result["params"]["expand"] == "names"
        assert result["params"]["project"] == "PROJ"
    
    def test_file_url(self):
        """Test file:// URL."""
        result = ConnectionMixin.parse_connection_string(
            "file:///path/to/data.csv"
        )
        
        assert result["adapter"] == "file"
        assert result["host"] == "/path/to/data.csv"
    
    def test_google_sheets_with_underscore(self):
        """Test adapter name with underscore (google_sheets)."""
        result = ConnectionMixin.parse_connection_string(
            "google_sheets://1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        )
        
        assert result["adapter"] == "google_sheets"
        # Preserve case for spreadsheet ID
        assert "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms" in result["host"]
    
    def test_salesforce_adapter(self):
        """Test Salesforce connection string."""
        result = ConnectionMixin.parse_connection_string(
            "salesforce://na1.salesforce.com"
        )
        
        assert result["adapter"] == "salesforce"
        assert result["host"] == "na1.salesforce.com"
    
    def test_hubspot_adapter(self):
        """Test HubSpot connection string."""
        result = ConnectionMixin.parse_connection_string("hubspot://api.hubapi.com")
        
        assert result["adapter"] == "hubspot"
        assert "hubapi.com" in result["host"]
    
    def test_stripe_adapter(self):
        """Test Stripe connection string."""
        result = ConnectionMixin.parse_connection_string("stripe://api.stripe.com")
        
        assert result["adapter"] == "stripe"
    
    def test_shopify_adapter(self):
        """Test Shopify connection string."""
        result = ConnectionMixin.parse_connection_string(
            "shopify://mystore.myshopify.com"
        )
        
        assert result["adapter"] == "shopify"
        assert "mystore" in result["host"]
    
    def test_zendesk_adapter(self):
        """Test Zendesk connection string."""
        result = ConnectionMixin.parse_connection_string(
            "zendesk://company.zendesk.com"
        )
        
        assert result["adapter"] == "zendesk"
    
    def test_rest_adapter(self):
        """Test generic REST adapter."""
        result = ConnectionMixin.parse_connection_string(
            "rest://api.example.com/v1"
        )
        
        assert result["adapter"] == "rest"


class TestParseConnectionStringEdgeCases:
    """Tests for edge cases in connection string parsing."""
    
    def test_empty_params(self):
        """Test with no query params returns empty dict."""
        result = ConnectionMixin.parse_connection_string(
            "servicenow://dev.service-now.com"
        )
        
        assert result["params"] == {}
    
    def test_single_query_param(self):
        """Test with single query param."""
        result = ConnectionMixin.parse_connection_string(
            "servicenow://dev.service-now.com?timeout=30"
        )
        
        assert result["params"]["timeout"] == "30"
    
    def test_multiple_values_same_param(self):
        """Test multiple values for same param."""
        result = ConnectionMixin.parse_connection_string(
            "rest://api.example.com?fields=id&fields=name&fields=status"
        )
        
        # Should be a list when multiple values
        assert isinstance(result["params"]["fields"], list)
        assert len(result["params"]["fields"]) == 3
    
    def test_special_characters_in_password(self):
        """Test handling of special characters in password."""
        result = ConnectionMixin.parse_connection_string(
            "postgres://user:p%40ssw0rd%21@localhost"
        )
        
        assert result["username"] == "user"
        # Password should be URL-decoded
        assert result["password"] == "p@ssw0rd!"
    
    def test_ipv4_host(self):
        """Test with IPv4 address as host."""
        result = ConnectionMixin.parse_connection_string(
            "postgres://192.168.1.100:5432"
        )
        
        assert result["host"] == "192.168.1.100"
        assert result["port"] == 5432


class TestExtractOAuthParams:
    """Tests for OAuth parameter extraction."""
    
    def test_extract_oauth_params(self):
        """Test extracting oauth_ prefixed params."""
        params = ConnectionMixin.extract_oauth_params(
            oauth_token="token123",
            oauth_client_id="client_id",
            oauth_client_secret="secret",
            username="admin",
            password="pass",
            other_param="value",
        )
        
        assert "oauth_token" in params
        assert "oauth_client_id" in params
        assert "oauth_client_secret" in params
        assert "username" not in params
        assert "password" not in params
        assert "other_param" not in params
    
    def test_extract_auth_params(self):
        """Test extracting auth_ prefixed params."""
        params = ConnectionMixin.extract_oauth_params(
            auth_mode="oauth2",
            auth_scope="read write",
            api_key="key123",
        )
        
        assert "auth_mode" in params
        assert "auth_scope" in params
        assert "api_key" not in params
    
    def test_extract_empty_when_no_oauth(self):
        """Test returns empty dict when no oauth params."""
        params = ConnectionMixin.extract_oauth_params(
            username="admin",
            password="pass",
            api_key="key",
        )
        
        assert params == {}


class TestCreateAuthManager:
    """Tests for AuthManager creation from parameters."""
    
    def test_basic_auth_manager(self):
        """Test creating AuthManager with basic auth."""
        auth_manager = ConnectionMixin.create_auth_manager_from_params(
            username="admin",
            password="password123",
        )
        
        assert auth_manager is not None
        assert auth_manager._username == "admin"
        assert auth_manager._password == "password123"
    
    def test_api_key_auth_manager(self):
        """Test creating AuthManager with API key."""
        auth_manager = ConnectionMixin.create_auth_manager_from_params(
            api_key="sk-test-12345",
        )
        
        assert auth_manager is not None
        assert auth_manager._api_key == "sk-test-12345"
    
    def test_oauth_token_auth_manager(self):
        """Test creating AuthManager with OAuth token."""
        auth_manager = ConnectionMixin.create_auth_manager_from_params(
            oauth_token="access_token_12345",
        )
        
        assert auth_manager is not None
        assert auth_manager._oauth_token == "access_token_12345"
    
    def test_oauth_params_auth_manager(self):
        """Test creating AuthManager with OAuth params."""
        auth_manager = ConnectionMixin.create_auth_manager_from_params(
            oauth_token_url="https://auth.example.com/token",
            oauth_client_id="client_123",
            oauth_client_secret="secret_456",
            oauth_grant_type="client_credentials",
        )
        
        assert auth_manager is not None
        assert auth_manager._oauth_token_url == "https://auth.example.com/token"
        assert auth_manager._oauth_client_id == "client_123"
        assert auth_manager._oauth_client_secret == "secret_456"
    
    def test_combined_auth_manager(self):
        """Test creating AuthManager with multiple auth methods."""
        auth_manager = ConnectionMixin.create_auth_manager_from_params(
            username="admin",
            password="pass",
            api_key="key123",
            oauth_token="token456",
        )
        
        assert auth_manager is not None
        # All should be set
        assert auth_manager._username == "admin"
        assert auth_manager._api_key == "key123"
        assert auth_manager._oauth_token == "token456"


class TestConnectionMixinIntegration:
    """Integration tests for ConnectionMixin."""
    
    def test_parse_and_create_auth_manager(self):
        """Test parsing connection string and creating auth manager."""
        conn_str = "servicenow://admin:secret@dev.service-now.com"
        
        parsed = ConnectionMixin.parse_connection_string(conn_str)
        auth_manager = ConnectionMixin.create_auth_manager_from_params(
            username=parsed["username"],
            password=parsed["password"],
        )
        
        assert parsed["adapter"] == "servicenow"
        assert parsed["host"] == "dev.service-now.com"
        assert auth_manager._username == "admin"
        assert auth_manager._password == "secret"
    
    def test_full_connection_flow(self):
        """Test complete connection parsing flow."""
        conn_str = "salesforce://user:pass@na1.salesforce.com?timeout=60"
        
        parsed = ConnectionMixin.parse_connection_string(conn_str)
        oauth_params = ConnectionMixin.extract_oauth_params(
            oauth_token_url="https://login.salesforce.com/token",
            timeout=60,
        )
        
        assert parsed["adapter"] == "salesforce"
        assert parsed["params"]["timeout"] == "60"
        assert "oauth_token_url" in oauth_params
        assert "timeout" not in oauth_params
