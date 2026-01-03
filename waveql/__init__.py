"""
WaveQL - Universal Python Connector

Query any API with SQL.

Usage:
    import waveql
    
    conn = waveql.connect("servicenow://instance.service-now.com",
                          username="admin", password="secret")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM incident WHERE priority = 1")
    print(cursor.fetchall())
"""

from waveql.connection import WaveQLConnection
from waveql.cursor import WaveQLCursor
from waveql.exceptions import (
    WaveQLError,
    ConnectionError,
    AuthenticationError,
    QueryError,
    AdapterError,
    SchemaError,
    RateLimitError,
    PredicatePushdownError,
    ConfigurationError,
    TimeoutError,
)
from waveql.adapters import BaseAdapter, register_adapter, get_adapter
from waveql.auth import (
    AuthManager,
    OAuth2Manager,
    BasicAuthManager,
    APIKeyAuthManager,
    JWTAuthManager,
    create_auth_manager,
)

__version__ = "0.1.1"
__all__ = [
    "connect",
    "WaveQLConnection",
    "WaveQLCursor",
    # Exceptions
    "WaveQLError",
    "ConnectionError",
    "AuthenticationError",
    "QueryError",
    "AdapterError",
    "SchemaError",
    "RateLimitError",
    "PredicatePushdownError",
    "ConfigurationError",
    "TimeoutError",
    # Adapters
    "BaseAdapter",
    "register_adapter",
    "get_adapter",
    # Authentication
    "AuthManager",
    "OAuth2Manager",
    "BasicAuthManager",
    "APIKeyAuthManager",
    "JWTAuthManager",
    "create_auth_manager",
    # Async support
    "connect_async",
    "AsyncWaveQLConnection",
    "AsyncWaveQLCursor",
    # DB-API 2.0 globals
    "apilevel",
    "threadsafety",
    "paramstyle",
]

# DB-API 2.0 compliance
apilevel = "2.0"
threadsafety = 1  # Threads may share module but not connections
paramstyle = "qmark"  # Question mark style: WHERE id = ?


def connect(
    connection_string: str = None,
    *,
    adapter: str = None,
    host: str = None,
    username: str = None,
    password: str = None,
    api_key: str = None,
    oauth_token: str = None,
    **kwargs
) -> WaveQLConnection:
    """
    Create a new WaveQL connection.
    
    Args:
        connection_string: URI-style connection (e.g., "servicenow://instance.service-now.com")
        adapter: Adapter type if not using connection_string
        host: Host/instance URL
        username: Username for Basic Auth
        password: Password for Basic Auth
        api_key: API key for API Key auth
        oauth_token: OAuth2 access token
        **kwargs: Additional adapter-specific options
        
    Returns:
        WaveQLConnection instance
        
    Examples:
        # Using connection string
        conn = waveql.connect("servicenow://myinstance.service-now.com",
                              username="admin", password="secret")
        
        # Using explicit parameters
        conn = waveql.connect(adapter="servicenow", 
                              host="myinstance.service-now.com",
                              username="admin", password="secret")
        
        # CSV/Parquet files
        conn = waveql.connect("file:///path/to/data.csv")
    """
    return WaveQLConnection(
        connection_string=connection_string,
        adapter=adapter,
        host=host,
        username=username,
        password=password,
        api_key=api_key,
        oauth_token=oauth_token,
        **kwargs
    )


async def connect_async(
    connection_string: str = None,
    *,
    adapter: str = None,
    host: str = None,
    username: str = None,
    password: str = None,
    api_key: str = None,
    oauth_token: str = None,
    **kwargs
) -> "AsyncWaveQLConnection":
    """
    Create a new asynchronous WaveQL connection.
    """
    from waveql.async_connection import AsyncWaveQLConnection
    
    return AsyncWaveQLConnection(
        connection_string=connection_string,
        adapter=adapter,
        host=host,
        username=username,
        password=password,
        api_key=api_key,
        oauth_token=oauth_token,
        **kwargs
    )
