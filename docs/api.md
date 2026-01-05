# API Reference

## `waveql.connect`
```python
def connect(
    connection_string: str = None, 
    username: str = None, 
    password: str = None,
    cache_ttl: float = None,
    cache_config: CacheConfig = None,
    enable_cache: bool = True,
    **kwargs
) -> WaveQLConnection
```
Creates a synchronous connection to a data source.
*   `connection_string`: URI formatted string (e.g., `servicenow://...`)
*   `username`: (Optional) Username for Auth
*   `password`: (Optional) Password or API Token
*   `cache_ttl`: (Optional) Cache TTL in seconds (default: 300)
*   `cache_config`: (Optional) Full `CacheConfig` for advanced configuration
*   `enable_cache`: (Optional) Enable/disable caching (default: True)

## `waveql.connect_async`
```python
async def connect_async(
    connection_string: str = None, 
    username: str = None, 
    password: str = None,
    cache_ttl: float = None,
    cache_config: CacheConfig = None,
    enable_cache: bool = True,
    **kwargs
) -> AsyncWaveQLConnection
```
Creates an asynchronous connection with the same caching parameters.

## `waveql.WaveQLConnection`
The main synchronous connection object.

### Methods
*   `cursor()`: Returns a new `WaveQLCursor`.
*   `close()`: Closes the underlying HTTP session.

### Cache Properties and Methods
*   `cache`: Access the `QueryCache` instance directly.
*   `cache_stats`: Returns `CacheStats` with hits, misses, hit rate, etc.
*   `invalidate_cache(adapter=None, table=None)`: Clear cache entries.
*   `set_cache_ttl(adapter, ttl)`: Set per-adapter TTL.

## `waveql.AsyncWaveQLConnection`
The main asynchronous connection object.

### Methods
*   `cursor()`: Returns a new `WaveQLCursor` (whose methods are async).
*   `stream_changes(table, config=None)`: Returns an async generator of changes (CDC).
*   `close()`: Closes the underlying HTTP session.

### Cache Properties and Methods
Same as `WaveQLConnection`: `cache`, `cache_stats`, `invalidate_cache()`, `set_cache_ttl()`.

## `waveql.WaveQLCursor`
Standard DB-API 2.0 cursor.

### Methods
*   `execute(query, params=None)`: Prepares and runs a SQL query.
*   `fetchone()`: Returns the next row.
*   `fetchall()`: Returns all remaining rows in a list.
*   `fetchmany(size)`: Returns `size` rows.

### Extensions
*   `fetchall().to_df()`: Converts the result set immediately to a Pandas DataFrame.
*   `fetchall().to_arrow()`: Returns the underlying PyArrow Table.

## `waveql.CacheConfig`
Configuration for the query result cache.
```python
@dataclass
class CacheConfig:
    enabled: bool = True              # Enable/disable caching
    default_ttl: float = 300.0        # Default TTL in seconds
    max_entries: int = 1000           # Maximum cached queries
    max_memory_mb: float = 512.0      # Maximum cache size in MB
    adapter_ttl: Dict[str, float]     # Per-adapter TTL overrides
    exclude_tables: List[str]         # Tables to never cache
```

## `waveql.CacheStats`
Cache statistics snapshot.
```python
@dataclass
class CacheStats:
    hits: int                         # Number of cache hits
    misses: int                       # Number of cache misses
    evictions: int                    # Entries evicted (LRU/memory)
    invalidations: int                # Manual invalidations
    entries: int                      # Current entry count
    size_mb: float                    # Current size in MB
    
    @property
    def hit_rate(self) -> float       # Hit rate as percentage
    
    def to_dict(self) -> Dict         # Serialize to dictionary
```

## `waveql.adapters`

### `register_adapter(name, class_ref)`
Registers a new adapter class to be used with a specific URI scheme.

## Exceptions

*   `waveql.WaveQLError`: Base exception for all errors.
*   `waveql.AuthenticationError`: 401/403 errors from APIs.
*   `waveql.QueryError`: SQL syntax errors or invalid field names.
*   `waveql.ConnectionError`: Network/Timeout issues.
*   `waveql.RateLimitError`: API rate limit exceeded (includes retry_after).

