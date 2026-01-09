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
    ContractViolationError,
)
from waveql.contracts import (
    DataContract,
    ColumnContract,
    ContractValidator,
    ContractRegistry,
    ContractValidationResult,
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
from waveql.cache import QueryCache, CacheConfig, CacheStats
from waveql.optimizer import (
    QueryOptimizer,
    CompoundPredicate,
    PredicateType,
    SubqueryInfo,
    SubqueryPushdownOptimizer,
)
from waveql.streaming import (
    RecordBatchStream,
    AsyncRecordBatchStream,
    BufferedAsyncStream,
    StreamConfig,
    StreamStats,
    create_stream,
)

# Semantic Layer
from waveql.semantic import (
    VirtualView,
    VirtualViewRegistry,
    SavedQuery,
    SavedQueryRegistry,
    DbtManifest,
    DbtModel,
)

# AI Functions (Vector Search & Embeddings)
from waveql.ai import (
    register_ai_functions,
    EmbeddingConfig,
    VectorSearchManager,
)

# Configuration
from waveql.config import (
    WaveQLConfig,
    get_config,
    set_config,
)


__version__ = "0.1.7"
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
    # Caching
    "QueryCache",
    "CacheConfig",
    "CacheStats",
    # Optimizer
    "QueryOptimizer",
    "CompoundPredicate",
    "PredicateType",
    "SubqueryInfo",
    "SubqueryPushdownOptimizer",
    # Contracts
    "DataContract",
    "ColumnContract",
    "ContractValidator",
    "ContractRegistry",
    "ContractValidationResult",
    "ContractViolationError",
    # Async support
    "connect_async",
    "AsyncWaveQLConnection",
    "AsyncWaveQLCursor",
    # Streaming
    "RecordBatchStream",
    "AsyncRecordBatchStream",
    "BufferedAsyncStream",
    "StreamConfig",
    "StreamStats",
    "create_stream",
    # Semantic Layer
    "VirtualView",
    "VirtualViewRegistry",
    "SavedQuery",
    "SavedQueryRegistry",
    "DbtManifest",
    "DbtModel",
    # AI Functions
    "register_ai_functions",
    "EmbeddingConfig",
    "VectorSearchManager",
    # Configuration
    "WaveQLConfig",
    "get_config",
    "set_config",
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
    # Cache configuration
    cache_ttl: float = None,
    cache_config: CacheConfig = None,
    enable_cache: bool = True,
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
        cache_ttl: Cache TTL in seconds (default: 300). Set to 0 to disable caching.
        cache_config: Full CacheConfig object for advanced configuration
        enable_cache: Whether to enable query caching (default: True)
        **kwargs: Additional adapter-specific options
        
    Returns:
        WaveQLConnection instance
        
    Examples:
        # Using connection string with default caching (5 min TTL)
        conn = waveql.connect("servicenow://myinstance.service-now.com",
                              username="admin", password="secret")
        
        # Disable caching
        conn = waveql.connect("servicenow://...", enable_cache=False)
        
        # Custom cache TTL (1 minute)
        conn = waveql.connect("servicenow://...", cache_ttl=60)
        
        # Advanced cache configuration
        conn = waveql.connect("servicenow://...",
                              cache_config=CacheConfig(
                                  default_ttl=300,
                                  max_memory_mb=256,
                                  adapter_ttl={"servicenow": 60}
                              ))
    """
    return WaveQLConnection(
        connection_string=connection_string,
        adapter=adapter,
        host=host,
        username=username,
        password=password,
        api_key=api_key,
        oauth_token=oauth_token,
        cache_ttl=cache_ttl,
        cache_config=cache_config,
        enable_cache=enable_cache,
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
    # Cache configuration
    cache_ttl: float = None,
    cache_config: CacheConfig = None,
    enable_cache: bool = True,
    **kwargs
) -> "AsyncWaveQLConnection":
    """
    Create a new asynchronous WaveQL connection.
    
    Same parameters as connect(), with full async/await support.
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
        cache_ttl=cache_ttl,
        cache_config=cache_config,
        enable_cache=enable_cache,
        **kwargs
    )
