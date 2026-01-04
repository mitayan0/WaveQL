# Error Handling

WaveQL provides rich, developer-friendly error messages with structured information to help you quickly diagnose and fix issues.

## Error Structure

Every WaveQL exception includes:

| Field | Description |
|-------|-------------|
| `error_code` | Machine-readable identifier (e.g., `E001`) |
| `message` | Human-readable error description |
| `suggestion` | Actionable advice for fixing the issue |
| `context` | Additional details (adapter, URL, table, etc.) |

### Example Error Output

```
[E004] ServiceNow request failed: 401 Unauthorized
  Context: adapter=servicenow, url=https://dev.service-now.com/api/now/table/incident, status_code=401
  Suggestion: Authentication required. Provide valid credentials for servicenow.
```

## Error Codes Reference

| Code | Exception | Description |
|------|-----------|-------------|
| `E000` | `WaveQLError` | Base error (generic) |
| `E001` | `ConnectionError` | Failed to connect to host |
| `E002` | `AuthenticationError` | Invalid or expired credentials |
| `E003` | `QueryError` | SQL syntax error or unsupported operation |
| `E004` | `AdapterError` | API request failed |
| `E005` | `SchemaError` | Schema discovery/validation failed |
| `E006` | `RateLimitError` | API rate limit exceeded |
| `E007` | `PredicatePushdownError` | Filter not supported by API |
| `E008` | `ConfigurationError` | Invalid configuration |
| `E009` | `TimeoutError` | Operation timed out |
| `E010` | `SchemaEvolutionError` | Incompatible schema change detected |

## Handling Errors

### Basic Error Handling

```python
import waveql
from waveql.exceptions import AdapterError, RateLimitError, QueryError

try:
    cursor = conn.execute("SELECT * FROM servicenow.incident")
except RateLimitError as e:
    print(f"Rate limited! Retry in {e.retry_after} seconds")
    time.sleep(e.retry_after)
    # Retry...
except AdapterError as e:
    print(f"API Error: {e.message}")
    print(f"Suggestion: {e.suggestion}")
except QueryError as e:
    print(f"Query failed: {e.message}")
```

### Accessing Error Details

```python
try:
    cursor = conn.execute("SELECT * FROM jira.issues")
except AdapterError as e:
    # Access structured error info
    print(e.error_code)      # "E004"
    print(e.message)         # "Jira request failed: 403 Forbidden"
    print(e.suggestion)      # "Access denied. Your credentials don't..."
    print(e.context)         # {"adapter": "jira", "status_code": 403, "url": "..."}
    
    # Convert to JSON for logging
    import json
    print(json.dumps(e.to_dict(), indent=2))
```

### HTTP Status Code Suggestions

When an API returns an error, WaveQL provides context-aware suggestions:

| Status | Suggestion |
|--------|------------|
| 400 | Check your query syntax. The request was malformed. |
| 401 | Authentication required. Provide valid credentials. |
| 403 | Access denied. Check your permissions. |
| 404 | Resource not found. Verify the table/endpoint exists. |
| 429 | Rate limit exceeded. Wait before retrying. |
| 500 | Internal server error. This is temporary - retry later. |
| 503 | Service unavailable. The API is down for maintenance. |

## Rate Limit Handling

WaveQL automatically retries on rate limits, but if retries are exhausted:

```python
from waveql.exceptions import RateLimitError

try:
    cursor = conn.execute("SELECT * FROM servicenow.incident")
except RateLimitError as e:
    # The retry_after attribute tells you how long to wait
    print(f"Rate limited by {e.context.get('adapter', 'API')}")
    print(f"Wait {e.retry_after} seconds before retrying")
    
    # Automatic backoff
    import time
    time.sleep(e.retry_after)
```

## Schema Evolution Errors

When an API's schema changes:

```python
from waveql.exceptions import SchemaEvolutionError

try:
    cursor = conn.execute("SELECT * FROM api.users")
except SchemaEvolutionError as e:
    print(f"Schema changed for table: {e.context.get('table')}")
    print(f"Changes detected: {e.context.get('changes')}")
    
    # Clear cache and retry with new schema
    conn.clear_schema_cache("api", "users")
```

## Custom Error Handling

For application-level error handling:

```python
from waveql.exceptions import WaveQLError

def execute_query_safely(conn, sql):
    """Execute a query with comprehensive error handling."""
    try:
        return conn.execute(sql).fetchall()
    except WaveQLError as e:
        # Log the structured error
        logger.error(
            "Query failed",
            error_code=e.error_code,
            message=e.message,
            context=e.context,
        )
        
        # Return the suggestion to the user
        return {"error": e.message, "suggestion": e.suggestion}
```

## Programmatic Error Codes

Use error codes for programmatic handling:

```python
from waveql.exceptions import WaveQLError

try:
    cursor = conn.execute(sql)
except WaveQLError as e:
    if e.error_code == "E006":  # Rate limit
        handle_rate_limit(e)
    elif e.error_code == "E002":  # Auth
        refresh_credentials()
    elif e.error_code in ("E004", "E001"):  # Network issues
        retry_with_backoff()
    else:
        raise  # Unknown error, propagate
```

## Best Practices

1. **Always catch specific exceptions** - Catch `RateLimitError` before `AdapterError`
2. **Use the suggestion field** - It provides actionable advice
3. **Log the context** - The context dict has valuable debugging info
4. **Implement retry logic** - Use `retry_after` for rate limits
5. **Clear caches on schema errors** - Schema evolution requires cache refresh
