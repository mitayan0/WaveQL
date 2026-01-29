# Error Codes

| Code | Exception | Description |
| :--- | :--- | :--- |
| `E001` | `ConnectionError` | Network failure / Host unreachable |
| `E002` | `AuthenticationError` | 401/403 credentials invalid |
| `E003` | `QueryError` | SQL syntax bad or unsupported |
| `E004` | `AdapterError` | Generic API failure (400/500) |
| `E005` | `SchemaError` | Invalid schema / discovery failed |
| `E006` | `RateLimitError` | 429 Too Many Requests |
| `E007` | `PredicatePushdownError` | Filter cannot be pushed to API |
| `E010` | `SchemaEvolutionError` | API schema drift detected |

## Details

Errors contain structured metadata in `e.context`:

```python
try:
    cursor.execute("SELECT * FROM incident")
except AdapterError as e:
    print(f"[{e.error_code}] {e.message}")
    print(e.context) 
    # {'adapter': 'servicenow', 'status': 401, 'url': '...'}
    print(e.suggestion)
    # "Check your password..."
```

## Best Practices
1.  **Catch Specifics**: Capture `RateLimitError` to implement backoff.
2.  **Schema Drift**: On `SchemaEvolutionError`, clear cache & reload.
3.  **Logs**: Log `e.context` for debugging.
4.  **Transient Errors**: Stripe, Shopify, and HubSpot adapters automatically retry on:
    - `ConnectError` - DNS/connection failures
    - `NetworkError` - Network-level failures
    - `ReadError` - Read operation failures
    - `TimeoutException` - Request timeouts
    
    All use exponential backoff (base 1s, max 5 retries).

## Retry Behavior by Adapter

| Adapter | Connect | Network | Read | Timeout | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Stripe** | ✅ | ✅ | ✅ | ✅ | Full retry coverage |
| **Shopify** | ✅ | ✅ | ✅ | ✅ | Full retry coverage |
| **HubSpot** | ✅ | ✅ | ✅ | ✅ | Native async with retry |
| **REST** | ✅ | ❌ | ❌ | ❌ | Via `_rate_limiter` |
