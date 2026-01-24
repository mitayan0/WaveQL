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

# Core
from waveql.connection import WaveQLConnection
from waveql.cursor import WaveQLCursor

# Exceptions
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

# Adapters
from waveql.adapters import BaseAdapter, register_adapter, get_adapter

# Authentication
from waveql.auth import (
    AuthManager,
    OAuth2Manager,
    BasicAuthManager,
    APIKeyAuthManager,
    JWTAuthManager,
    create_auth_manager,
)

# Caching
from waveql.cache import QueryCache, CacheConfig, CacheStats

# Contracts
from waveql.contracts import (
    DataContract,
    ColumnContract,
    ContractValidator,
    ContractRegistry,
    ContractValidationResult,
)

# Streaming
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

# AI Functions
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

# Optimizer
from waveql.optimizer import (
    QueryOptimizer,
    CompoundPredicate,
    PredicateType,
    SubqueryInfo,
    SubqueryPushdownOptimizer,
)

# Join Optimizer
from waveql.join_optimizer import (
    JoinOptimizer,
    JoinPlan,
    JoinEdge,
    TableStats,
    get_join_optimizer,
)

# Resource Optimizer (Low-Resource Systems Engineering)
from waveql.resource_optimizer import (
    CardinalityEstimator,
    CardinalityStats,
    AdaptivePagination,
    PaginationState,
    BudgetPlanner,
    QueryBudget,
    BudgetUnit,
    get_cardinality_estimator,
    get_adaptive_pagination,
    get_budget_planner,
    get_resource_executor,
)

# Row-Level Security (Enterprise Features)
from waveql.security import (
    SecurityPolicy,
    PolicyManager,
    PolicyViolationError,
)

# Note: ChunkedExecutor is now internal - chunking happens automatically
# via BaseAdapter.fetch_with_auto_chunking(). No user configuration needed.


__version__ = "0.1.7"

# Everything users might need - just import from waveql
__all__ = [
    # Core
    "connect",
    "connect_async",
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
    "ContractViolationError",
    
    # Adapters
    "BaseAdapter",
    "register_adapter",
    "get_adapter",
    
    # Auth
    "AuthManager",
    "OAuth2Manager",
    "BasicAuthManager",
    "APIKeyAuthManager",
    "JWTAuthManager",
    "create_auth_manager",
    
    # Cache
    "QueryCache",
    "CacheConfig",
    "CacheStats",
    
    # Contracts
    "DataContract",
    "ColumnContract",
    "ContractValidator",
    "ContractRegistry",
    "ContractValidationResult",
    
    # Streaming
    "RecordBatchStream",
    "AsyncRecordBatchStream",
    "BufferedAsyncStream",
    "StreamConfig",
    "StreamStats",
    "create_stream",
    
    # Semantic
    "VirtualView",
    "VirtualViewRegistry",
    "SavedQuery",
    "SavedQueryRegistry",
    "DbtManifest",
    "DbtModel",
    
    # AI
    "register_ai_functions",
    "EmbeddingConfig",
    "VectorSearchManager",
    
    # Config
    "WaveQLConfig",
    "get_config",
    "set_config",
    
    # Optimizer
    "QueryOptimizer",
    "CompoundPredicate",
    "PredicateType",
    "SubqueryInfo",
    "SubqueryPushdownOptimizer",
    
    # Join Optimizer
    "JoinOptimizer",
    "JoinPlan",
    "JoinEdge",
    "TableStats",
    "get_join_optimizer",
    
    # Resource Optimizer (Low-Resource Systems Engineering)
    "CardinalityEstimator",
    "CardinalityStats",
    "AdaptivePagination",
    "PaginationState",
    "BudgetPlanner",
    "QueryBudget",
    "BudgetUnit",
    "get_cardinality_estimator",
    "get_adaptive_pagination",
    "get_budget_planner",
    "get_resource_executor",
    
    # Row-Level Security (Enterprise Features)
    "SecurityPolicy",
    "PolicyManager",
    "PolicyViolationError",
    
    # Async
    "AsyncWaveQLConnection",
    "AsyncWaveQLCursor",
    
    # DB-API 2.0
    "apilevel",
    "threadsafety",
    "paramstyle",
    "__version__",
]

# DB-API 2.0 compliance
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

# DB-API 2.0 Exceptions
class Warning(Exception):
    pass

class Error(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

# Map internal exceptions to DB-API exceptions where possible
# (Ideally, we would inherit from these, but for now we define them for SQLAlchemy compliance)


def connect(
    connection_string: str = None,
    *,
    adapter: str = None,
    host: str = None,
    username: str = None,
    password: str = None,
    api_key: str = None,
    oauth_token: str = None,
    cache_ttl: float = None,
    cache_config: CacheConfig = None,
    enable_cache: bool = True,
    **kwargs
) -> WaveQLConnection:
    """
    Create a new WaveQL connection.
    
    Args:
        connection_string: URI (e.g., "servicenow://instance.service-now.com")
        username: Username for Basic Auth
        password: Password for Basic Auth
        api_key: API key authentication
        oauth_token: OAuth2 access token
        cache_ttl: Cache TTL in seconds (default: 300)
        enable_cache: Enable query caching (default: True)
        
    Returns:
        WaveQLConnection
        
    Examples:
        >>> conn = waveql.connect("servicenow://myinstance.service-now.com",
        ...                       username="admin", password="secret")
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM incident LIMIT 10")
        >>> rows = cursor.fetchall()
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
    cache_ttl: float = None,
    cache_config: CacheConfig = None,
    enable_cache: bool = True,
    **kwargs
) -> "AsyncWaveQLConnection":
    """
    Create an async WaveQL connection.
    
    Example:
        >>> conn = await waveql.connect_async("servicenow://...")
        >>> cursor = conn.cursor()
        >>> await cursor.execute("SELECT * FROM incident")
        >>> rows = await cursor.fetchall()
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
