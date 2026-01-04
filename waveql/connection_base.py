"""
WaveQL Connection Base - Common functionality for sync and async connections
"""

from __future__ import annotations
import logging
from typing import Any, Dict, TYPE_CHECKING
from urllib.parse import urlparse, parse_qs

if TYPE_CHECKING:
    from waveql.auth.manager import AuthManager
    from waveql.schema_cache import SchemaCache
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class ConnectionMixin:
    """
    Common functionality shared between sync and async connections.
    
    Provides:
    - Connection string parsing
    - Adapter initialization helpers
    - Common configuration extraction
    """
    
    @staticmethod
    def parse_connection_string(conn_str: str) -> Dict[str, Any]:
        """
        Parse URI-style connection string.
        
        Supported formats:
        - file:///path/to/data.csv
        - servicenow://instance.service-now.com
        - adapter://user:password@host:port?param=value
        
        Args:
            conn_str: Connection string to parse
            
        Returns:
            Dict with 'adapter', 'host', 'username', 'password', 'port', and 'params' keys
            
        Examples:
            >>> parse_connection_string("servicenow://admin:secret@dev.service-now.com")
            {'adapter': 'servicenow', 'host': 'dev.service-now.com', 'username': 'admin', ...}
            
            >>> parse_connection_string("jira://company.atlassian.net?expand=names")
            {'adapter': 'jira', 'host': 'company.atlassian.net', 'params': {'expand': 'names'}, ...}
        """
        # Handle file:// URLs
        if conn_str.startswith("file://"):
            return {
                "adapter": "file",
                "host": conn_str[7:],  # Remove file://
                "username": None,
                "password": None,
                "port": None,
                "params": {}
            }
        
        # Parse adapter://[user:pass@]host[:port][?params] format
        parsed = urlparse(conn_str)
        params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}
        
        result = {
            "adapter": parsed.scheme,
            "host": parsed.hostname or parsed.path,  # hostname excludes credentials
            "username": parsed.username,              # Extracted from user:pass@host
            "password": parsed.password,              # Extracted from user:pass@host
            "port": parsed.port,                      # Extracted from host:port
            "params": params
        }
        
        logger.debug(
            "Parsed connection string: adapter=%s, host=%s, user=%s",
            result["adapter"], result["host"], result["username"] or "(none)"
        )
        return result
    
    @staticmethod
    def extract_oauth_params(**kwargs) -> Dict[str, Any]:
        """
        Extract OAuth-related parameters from kwargs.
        
        Args:
            **kwargs: All connection parameters
            
        Returns:
            Dict containing only oauth_ and auth_ prefixed params
        """
        return {k: v for k, v in kwargs.items() 
                if k.startswith("oauth_") or k.startswith("auth_")}
    
    @staticmethod
    def create_auth_manager_from_params(
        username: str = None,
        password: str = None,
        api_key: str = None,
        oauth_token: str = None,
        **oauth_params
    ) -> "AuthManager":
        """
        Create an AuthManager from connection parameters.
        
        Args:
            username: Username for Basic Auth
            password: Password for Basic Auth
            api_key: API key
            oauth_token: OAuth2 access token
            **oauth_params: Additional OAuth parameters
            
        Returns:
            Configured AuthManager instance
        """
        from waveql.auth.manager import AuthManager
        
        return AuthManager(
            username=username,
            password=password,
            api_key=api_key,
            oauth_token=oauth_token,
            oauth_token_url=oauth_params.get("oauth_token_url"),
            oauth_client_id=oauth_params.get("oauth_client_id"),
            oauth_client_secret=oauth_params.get("oauth_client_secret"),
            oauth_grant_type=oauth_params.get("oauth_grant_type", "client_credentials"),
            oauth_refresh_token=oauth_params.get("oauth_refresh_token"),
            oauth_scope=oauth_params.get("oauth_scope"),
            **oauth_params
        )
