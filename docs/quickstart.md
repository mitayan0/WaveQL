# Quick Start

Get up and running with WaveQL in 5 minutes.

## 1. Installation

```bash
pip install waveql
# Optional: Install extra dependencies for specific adapters
pip install waveql[snowflake,postgres]
```

## 2. Configuration (`.env`)

WaveQL looks for environment variables or a `waveql.config` file. The easiest way to start is a `.env` file in your working directory.

```bash
# Data Source Credentials
ZENDESK_EMAIL="user@company.com"
ZENDESK_TOKEN="your_api_token"
ZENDESK_SUBDOMAIN="company"

# Cache Settings
WAVEQL_CACHE_DIR="./.cache"
```

## 3. Your First Query

WaveQL maps API endpoints to tables.

```python
import waveql

# Connect (automatically loads env vars)
conn = waveql.connect()

# Run a query
# Note: 'zendesk.tickets' maps to the /tickets.json endpoint
df = conn.query("""
    SELECT 
        id, 
        subject, 
        priority, 
        created_at 
    FROM zendesk.tickets 
    WHERE status = 'open' 
    LIMIT 5
""").to_df()

print(df)
```

## 4. Federated Join (The "Magic")

Combine data from two different APIs as if they were in the same database.

```python
query = """
    SELECT 
        t.id as ticket_id,
        t.subject,
        u.email as customer_email,
        s.mrr as customer_value
    FROM zendesk.tickets t
    JOIN hubspot.contacts c ON t.requester_id = c.custom_zendesk_id
    JOIN stripe.subscriptions s ON c.email = s.customer_email
    WHERE t.priority = 'urgent'
      AND s.status = 'active'
"""

df = conn.query(query).to_df()
```

## 5. Next Steps

*   **[Adapters List](adapters.md)**: See all supported APIs and their table schemas.
*   **[Authentication](auth.md)**: Learn about OAuth2 and secure credential management.
*   **[Internals](internals/README.md)**: Understand how the query optimizer pushes filters down to the API.
