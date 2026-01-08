# Testing & Developer Credentials

To test WaveQL against real APIs, follow these steps to obtain **free developer instances** from each provider.

## 1. Getting Credentials

### 🔗 ServiceNow
ServiceNow offers full "Personal Developer Instances" (PDIs) that are perfect for testing.
1.  Sign up at the [ServiceNow Developer Program](https://developer.servicenow.com/).
2.  Request a **Personal Developer Instance** (PDI).
3.  You will receive a URL (e.g., `https://dev12345.service-now.com`) and admin credentials.
4.  **Cost**: Free (hibernates if unused).

### ☁️ Salesforce
Get a free "Developer Edition" org, which includes API access.
1.  Sign up at [developer.salesforce.com/signup](https://developer.salesforce.com/signup).
2.  Login to your new org.
3.  **To get API Credentials (OAuth)**:
    *   Go to **Setup** > **Apps** > **App Manager**.
    *   Click **New Connected App**.
    *   Enable OAuth Settings.
    *   Select scopes (`api`, `refresh_token`, `offline_access`).
    *   Save to get your **Consumer Key** (Client ID) and **Consumer Secret**.

### 🥋 Jira (Atlassian)
Use a free Jira Cloud development site.
1.  Sign up for a [Free Jira Cloud account](https://www.atlassian.com/software/jira/free).
2.  **To get an API Token**:
    *   Go to [id.atlassian.com/manage-profile/security/api-tokens](https://id.atlassian.com/manage-profile/security/api-tokens).
    *   Create a new token.
    *   Use your email as the username and this token as the password.

### 🟧 HubSpot
1.  Create a [HubSpot Developer Account](https://developers.hubspot.com/get-started).
2.  Create a **Test Portal** (Account).
3.  Go to **Settings** > **Integrations** > **Private Apps**.
4.  Create an app and select scopes (e.g., `crm.objects.contacts.read`).
5.  Copy the **Access Token**.

### 🛍️ Shopify
1.  Join the [Shopify Partner Program](https://partners.shopify.com/).
2.  Create a **Development Store**.
3.  Go to **Apps** > **App and sales channel settings**.
4.  Create a custom app to get an `Admin API access token`.

### 💳 Stripe
1.  Sign up at [dashboard.stripe.com/register](https://dashboard.stripe.com/register).
2.  Go to **Developers** > **API keys**.
3.  Toggle **Test Mode** on.
4.  Use `pk_test_...` (Publishable) or `sk_test_...` (Secret) keys.

### 🎧 Zendesk
1.  Start a [14-day Free Trial](https://www.zendesk.com/register/).
2.  Enable **API Token Access**:
    *   **Admin Center** > **Apps and integrations** > **APIs** > **Zendesk API**.
    *   Toggle **Token Access** to Enabled.
    *   Create a new API Token.
3.  **Connection**:
    *   Username: `email@example.com/token`
    *   Password: `your_api_token`

---

## 2. Setting Up Integration Tests

WaveQL includes integration tests in the `playground/` directory. To run them, create a `.env` file in the root directory with your credentials:

```ini
# .env file

# ServiceNow
SERVICENOW_HOST=dev12345.service-now.com
SERVICENOW_USER=admin
SERVICENOW_PASSWORD=your_password

# Salesforce
SALESFORCE_HOST=your-dev-ed.my.salesforce.com
SALESFORCE_USER=email@example.com
SALESFORCE_PASSWORD=password
SALESFORCE_TOKEN=security_token
SALESFORCE_CONSUMER_KEY=your_connected_app_key
SALESFORCE_CONSUMER_SECRET=your_connected_app_secret

# Jira
JIRA_HOST=your-domain.atlassian.net
JIRA_USER=email@example.com
JIRA_TOKEN=your_api_token

# HubSpot
HUBSPOT_ACCESS_TOKEN=your_private_app_token

# Shopify
SHOPIFY_SHOP_URL=your-shop.myshopify.com
SHOPIFY_ACCESS_TOKEN=your_admin_api_token

# Zendesk
ZENDESK_SUBDOMAIN=your-subdomain
ZENDESK_EMAIL=email@example.com/token
ZENDESK_TOKEN=your_api_token

# Stripe
STRIPE_API_KEY=sk_test_...

# Google Sheets
GOOGLE_APPLICATION_CREDENTIALS=path/to/service_account.json

# AWS S3 (Cloud Storage)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
# AWS_ENDPOINT_URL=http://localhost:9000  # For MinIO
```

## 3. Public REST APIs (No Credentials)

For testing the **Generic REST Adapter** without signup, you can use these public APIs:

*   **JSONPlaceholder**: `https://jsonplaceholder.typicode.com`
    *   `rest://jsonplaceholder.typicode.com`
    *   Tables: `users`, `posts`, `comments`
*   **PokeAPI**: `https://pokeapi.co/api/v2`
    *   `rest://pokeapi.co/api/v2`
    *   Tables: `pokemon`, `type`, `ability`
*   **SWAPI (Star Wars)**: `https://swapi.dev/api`
    *   `rest://swapi.dev/api`
    *   Tables: `people`, `planets`, `starships`

---

## 4. Cloud Storage & Data Lakes

### 🪣 Amazon S3 / Cloud Storage
For testing S3, GCS, or Azure Blob without spending money:
*   **MinIO (Recommended)**: Run a local S3-compatible server.
    ```bash
    docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
    ```
    *   User: `minioadmin`, Password: `minioadmin`
    *   URI: `s3://bucket/` with `endpoint_url=http://localhost:9000`
*   **Public Datasets**: Use open datasets (ReadOnly).
    *   `s3://coiled-datasets/` (AWS Open Data)
*   **AWS Free Tier**: Standard S3 has a 5GB free tier for 12 months.

### 📊 Google Sheets
1.  **Service Account (Recommended)**:
    *   Go to [Google Cloud Console](https://console.cloud.google.com/).
    *   Create a project > **IAM & Admin** > **Service Accounts**.
    *   Create account, download keys as JSON.
    *   **Crucial**: Share your target sheet with the service account's email address (`...iam.gserviceaccount.com`).
2.  **Public Sheets**:
    *   File > Share > Publish to web.
    *   Use the CSV URL as a "File" adapter source: `waveql.connect('file://url...')`.

### 🧊 Iceberg & Delta Lake
1.  **Tabular.io**: Offers a free tier for hosted Iceberg catalogs.
2.  **Local Testing**:
    *   Use **MinIO** as the storage layer.
    *   Create tables using `spark` or `duckdb` locally.

### 🛢️ SQL Databases (Postgres, MySQL)
1.  **Local Docker (Recommended)**:
    ```bash
    # PostgreSQL
    docker run --name waveql-pg -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres:latest
    
    # MySQL
    docker run --name waveql-mysql -e MYSQL_ROOT_PASSWORD=password -p 3306:3306 -d mysql:latest
    ```
    *   URI: `postgresql://postgres:password@localhost:5432/postgres`
2.  **Free Cloud Tiers**:
    *   **Supabase / Neon** (Free Postgres)
    *   **PlanetScale** (MySQL-compatible)
