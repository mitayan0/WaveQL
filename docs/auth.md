# Authentication Guide

WaveQL delegates authentication to the `AuthManager` class hierarchy. This separates the "how we connect" logic from the "how we query" logic.

## Supported Methods

### 1. Basic Authentication
The simplest method, used by ServiceNow and many older APIs.

```python
from waveql.auth import AuthManager

# Implicit usage via connect params
conn = waveql.connect(..., username="user", password="pwd")

# Explicit usage
auth = AuthManager(username="user", password="pwd")
conn = waveql.connect(..., auth_manager=auth)
```
*   **Header**: Adds `Authorization: Basic <base64>`

### 2. API Key / Bearer Token
Common for Jira (Email + API Token) or simple REST services.

```python
# API Key as custom header
auth = AuthManager(
    api_key="12345-abcde", 
    api_key_header="X-API-Key"
)

# API Key as Bearer token
auth = AuthManager(
    api_key="my-jwt-token",
    api_key_header="Authorization",
    api_key_prefix="Bearer"
)
```

### 3. OAuth 2.0 (Client Credentials Flow)
For machine-to-machine communication (e.g., Salesforce Connected App).

WaveQL will automatically:
1.  Request an access token using `client_id` and `client_secret`.
2.  Cache the token.
3.  Refresh the token automatically if a request fails with `401 Unauthorized`.

```python
auth = AuthManager(
    oauth2_token_url="https://login.salesforce.com/services/oauth2/token",
    client_id="consumer-key",
    client_secret="consumer-secret",
    scope="full"
)
```

## Security Best Practices
*   **Never hardcode credentials** in your SQL scripts or Python files.
*   Use Environment Variables (`os.getenv`) or a secrets manager.
*   WaveQL cleans credentials from logs, but you should ensure your application logs filter connection strings.

```python
import os
conn = waveql.connect(
    "servicenow://instance.service-now.com",
    username=os.getenv("SN_USER"),
    password=os.getenv("SN_PASS")
)
```
