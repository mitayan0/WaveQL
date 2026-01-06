# Salesforce Adapter Guide

The Salesforce adapter provides full SQL access to your Salesforce data, translating queries into native SOQL for optimal performance.

## Quick Start

```python
import waveql

# Connect to Salesforce
conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    oauth_token="your_access_token",
    oauth_client_id="your_consumer_key"
)

# Query with standard SQL
cursor = conn.cursor()
cursor.execute("SELECT Id, Name, Industry FROM Account LIMIT 10")

for row in cursor:
    print(f"{row.Name}: {row.Industry}")
```

## Authentication Methods

WaveQL supports multiple authentication methods for Salesforce:

### Method 1: Username/Password Flow (Production/Sandbox Orgs)

This is the simplest method and works with **Production** and **Sandbox** orgs.

```python
import waveql

conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    username="user@example.com",
    password="password" + "security_token",  # Concatenate password and security token
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="your_consumer_key",
    oauth_client_secret="your_consumer_secret",
    oauth_grant_type="password",
)
```

**Requirements:**
- Connected App with OAuth enabled
- API Access enabled for the user
- Security Token (from User Settings → Reset My Security Token)

**When to use:**
- Production orgs
- Sandbox orgs
- Automation scripts
- Backend services

### Method 2: Access Token Flow (All Org Types)

Use this method when you have an access token from browser-based OAuth. **This works with ALL org types**, including Trailhead Playgrounds and Developer Edition orgs.

```python
import waveql

conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    oauth_token="your_access_token",
    oauth_refresh_token="your_refresh_token",  # Optional, for auto-refresh
    oauth_token_url="https://your-instance.my.salesforce.com/services/oauth2/token",
    oauth_client_id="your_consumer_key",
    oauth_client_secret="your_consumer_secret",  # Optional for public clients
)
```

**Getting tokens via browser OAuth:**

WaveQL includes a helper script for one-time browser authentication:

```bash
python playground/salesforce_oauth_setup.py
```

This will:
1. Open your browser to Salesforce login
2. Capture the authorization code
3. Exchange it for access and refresh tokens
4. Save tokens to your `.env` file

### Method 3: Using AuthManager Directly

For advanced use cases:

```python
from waveql.auth import AuthManager

auth = AuthManager(
    oauth_token_url="https://login.salesforce.com/services/oauth2/token",
    oauth_client_id="your_consumer_key",
    oauth_client_secret="your_consumer_secret",
    username="user@example.com",
    password="password+token",
    oauth_grant_type="password",
)

conn = waveql.connect(
    "salesforce://your-instance.my.salesforce.com",
    auth_manager=auth
)
```

## Setting Up a Connected App

### Step 1: Create Connected App in Salesforce

1. Log into Salesforce Setup
2. Navigate to **App Manager** (search in Quick Find)
3. Click **New Connected App**
4. Configure:
   - **Connected App Name**: Your App Name
   - **API Name**: YourAppName
   - **Contact Email**: your-email@example.com
   - **Enable OAuth Settings**: ✅ Checked
   - **Callback URL**: `https://login.salesforce.com/services/oauth2/success`
   - **Selected OAuth Scopes**:
     - `Access and manage your data (api)`
     - `Perform requests at any time (refresh_token, offline_access)`
5. Save and wait 2-10 minutes for activation

### Step 2: Get Consumer Key and Secret

1. Go to **App Manager**
2. Find your app → Click dropdown → **View**
3. Copy **Consumer Key** (this is `oauth_client_id`)
4. Click "Click to reveal" for **Consumer Secret** (this is `oauth_client_secret`)

### Step 3: Get Security Token (for Password Flow)

1. Click your profile → **Settings**
2. Search for **"Reset My Security Token"**
3. Click **Reset Security Token**
4. Check your email for the new token

## Supported SQL Features

### Basic Queries
```sql
-- Select all columns
SELECT * FROM Account LIMIT 10

-- Select specific columns
SELECT Id, Name, Industry FROM Account

-- With predicates
SELECT Name, Type FROM Account 
WHERE Industry = 'Technology' AND Type = 'Customer'
```

### WHERE Clause Operators

| SQL Operator | SOQL Translation | Example |
|--------------|------------------|---------|
| `=` | `=` | `WHERE Status = 'Active'` |
| `!=` | `!=` | `WHERE Status != 'Closed'` |
| `>`, `<`, `>=`, `<=` | Same | `WHERE Amount > 1000` |
| `LIKE` | `LIKE` | `WHERE Name LIKE '%Corp%'` |
| `IN` | `IN` | `WHERE Type IN ('A', 'B')` |
| `IS NULL` | `= null` | `WHERE Phone IS NULL` |
| `IS NOT NULL` | `!= null` | `WHERE Email IS NOT NULL` |

### ORDER BY
```sql
SELECT Name, CreatedDate FROM Account 
ORDER BY CreatedDate DESC 
LIMIT 20
```

### LIMIT and OFFSET
```sql
-- Pagination
SELECT Name FROM Account ORDER BY Name LIMIT 10 OFFSET 20
```

### Aggregations
```sql
SELECT COUNT(Id) FROM Account

SELECT Industry, COUNT(Id) cnt 
FROM Account 
GROUP BY Industry
```

### GROUP BY
```sql
SELECT Type, COUNT(Id) cnt 
FROM Account 
GROUP BY Type
```

## CRUD Operations

### INSERT
```sql
INSERT INTO Account (Name, Industry, Type) 
VALUES ('New Company', 'Technology', 'Prospect')
```

Or via cursor:
```python
cursor.execute("""
    INSERT INTO Account (Name, Industry) 
    VALUES ('ACME Corp', 'Technology')
""")
```

### UPDATE
```sql
UPDATE Account 
SET Industry = 'Finance' 
WHERE Id = '001XXXXXXXXXXXXXXX'
```

Note: Salesforce UPDATE requires the record `Id` in the WHERE clause.

### DELETE
```sql
DELETE FROM Account WHERE Id = '001XXXXXXXXXXXXXXX'
```

Note: Salesforce DELETE requires the record `Id` in the WHERE clause.

## Bulk Operations

For inserting many records efficiently, use the Bulk API:

```python
adapter = conn._get_adapter()
records = [
    {"Name": "Bulk Account 1", "Industry": "Technology"},
    {"Name": "Bulk Account 2", "Industry": "Finance"},
    {"Name": "Bulk Account 3", "Industry": "Healthcare"},
]

result = adapter.insert_bulk("Account", records)
print(f"Status: {result['state']}")
print(f"Records Processed: {result['numberRecordsProcessed']}")
```

## Schema Discovery

WaveQL automatically discovers table schemas:

```python
# Get schema for a table
schema = conn.get_table_columns("Account")
for col in schema:
    print(f"{col['name']}: {col['data_type']}")
```

## Configuration Options

### Environment Variables

```env
SF_HOST=https://your-instance.my.salesforce.com
SF_USERNAME=user@example.com
SF_PASSWORD=yourpassword
SF_SECURITY_TOKEN=yourtoken
SF_CLIENT_ID=consumer_key
SF_CLIENT_SECRET=consumer_secret
SF_ACCESS_TOKEN=access_token  # If using token auth
SF_REFRESH_TOKEN=refresh_token  # For auto-refresh
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `api_version` | Salesforce API version | `v57.0` |
| `timeout` | Request timeout in seconds | `30` |
| `cache_ttl` | Cache TTL in seconds | None (no caching) |

## Troubleshooting

### `invalid_grant: authentication failure`

**Cause:** Username/password authentication failed.

**Solutions:**
1. Verify password is correct
2. Ensure security token is appended to password
3. Check if user has API access enabled
4. For Dev/Trailhead orgs, use the Access Token method instead

### `invalid_client_id: client identifier invalid`

**Cause:** Connected App Consumer Key is wrong or app not activated.

**Solutions:**
1. Wait 2-10 minutes after creating Connected App
2. Verify Consumer Key is copied correctly
3. Check if Connected App is in the correct org

### `missing required code challenge`

**Cause:** Your Connected App requires PKCE (common for newer orgs).

**Solution:** Use the OAuth setup script which includes PKCE:
```bash
python playground/salesforce_oauth_setup.py
```

### Rate Limiting

Salesforce has API limits. WaveQL handles rate limit errors gracefully with automatic retries.

To check your usage:
```python
# After queries, check response headers for limit info
# Or use Salesforce Setup → Company Information → API Requests
```

## Best Practices

1. **Use Caching** for repeated queries:
   ```python
   conn = waveql.connect("salesforce://...", cache_ttl=300)
   ```

2. **Select only needed columns** to reduce payload:
   ```sql
   SELECT Id, Name FROM Account  -- Better than SELECT *
   ```

3. **Use LIMIT** for large tables:
   ```sql
   SELECT * FROM Account LIMIT 1000
   ```

4. **Use Bulk API** for inserting many records:
   ```python
   adapter.insert_bulk("Account", records)
   ```

5. **Store refresh tokens** for long-running processes:
   - Access tokens expire (typically 2 hours)
   - Refresh tokens + `oauth_token_url` enable auto-refresh

## Example: Complete Workflow

```python
import waveql
import os

# Connect using access token
conn = waveql.connect(
    f"salesforce://{os.getenv('SF_HOST')}",
    oauth_token=os.getenv('SF_ACCESS_TOKEN'),
    oauth_refresh_token=os.getenv('SF_REFRESH_TOKEN'),
    oauth_token_url=f"{os.getenv('SF_HOST')}/services/oauth2/token",
    oauth_client_id=os.getenv('SF_CLIENT_ID'),
    cache_ttl=60,
)

cursor = conn.cursor()

# Query accounts
cursor.execute("""
    SELECT Id, Name, Industry, Type 
    FROM Account 
    WHERE Industry = 'Technology' 
    ORDER BY Name 
    LIMIT 10
""")

# Process results
for row in cursor:
    print(f"{row.Name} ({row.Type}): {row.Industry}")

# Get as DataFrame
df = cursor.fetchall().to_df()
print(df.describe())

# Clean up
conn.close()
```
