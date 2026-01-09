# Authentication Drivers

## Credential Chain
WaveQL resolves credentials in this order:
1.  **Explicit**: Passed to `connect()` or `base_adapter.fetch()`.
2.  **Env Vars**: `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.
3.  **Config**: `~/.waveql/credentials.yaml`.
4.  **System**: IAM Roles, Workload Identity.

## Drivers

### Basic / API Key
```python
# Keys
auth = APIKeyAuthManager(api_key="123", header_name="X-API-Key")

# Basic
auth = BasicAuthManager("user", "pass")
```

### OAuth 2.0 (Client Credentials)
Auto-refreshes tokens.
```python
auth = AuthManager(
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="key",
    oauth_client_secret="secret"
)
```

### OAuth 2.0 (Password Flow)
Legacy flow for Salesforce.
```python
auth = AuthManager(
    ...,
    username="user",
    password="password+token",
    oauth_grant_type="password"
)
```

### OAuth 2.0 (Bearer Token)
When you have a pre-existing token.
```python
conn = waveql.connect(..., oauth_token="access_token")
```
