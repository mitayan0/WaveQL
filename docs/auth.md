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

# API Key as Bearer token (Use specific manager for prefixes)
from waveql.auth import APIKeyAuthManager
auth = APIKeyAuthManager(
    api_key="my-jwt-token",
    header_name="Authorization",
    prefix="Bearer "
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
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="consumer-key",
    oauth_client_secret="consumer-secret",
    oauth_scope="full"
)
```

### 4. OAuth 2.0 (Password Grant Flow)
For Salesforce Production and Sandbox orgs.

```python
auth = AuthManager(
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="consumer-key",
    oauth_client_secret="consumer-secret",
    username="user@example.com",
    password="password+security_token",  # Append security token to password
    oauth_grant_type="password",
)
```

### 5. OAuth 2.0 (Access Token + Refresh)
When you already have an access token (e.g., from browser-based OAuth).

```python
conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    oauth_token="your_access_token",
    oauth_refresh_token="your_refresh_token",  # Optional, for auto-refresh
    oauth_token_url="https://your-instance.my.salesforce.com/services/oauth2/token",
    oauth_client_id="consumer-key",
)
```

This method works with **all Salesforce org types**, including Developer Edition and Trailhead Playgrounds.

📖 **See [Salesforce Guide](salesforce.md) for detailed Salesforce authentication setup.**

## 6. Unified Credential Provider Chain ✨ NEW

For Cloud Storage (S3/GCS/Azure) and Google Sheets, WaveQL supports a unified resolution chain. This allows you to omit credentials in code and rely on environment or system configuration.

**Order of Resolution:**
1.  **Explicit Parameters**: Passed directly to `connect()` or the Adapter.
2.  **Environment Variables**: `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.
3.  **Config File**: `~/.waveql/credentials.yaml`.
4.  **System Auth**: IAM Roles (AWS), Workload Identity (GCP), or default token paths.

### `credentials.yaml` Example
```yaml
aws:
  access_key_id: "AKIA..."
  secret_access_key: "secret..."
  region: "us-east-1"
gcs:
  project_id: "my-project"
  service_account_json: "/path/to/creds.json"
azure:
  storage_account: "myaccount"
  storage_key: "key..."
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
