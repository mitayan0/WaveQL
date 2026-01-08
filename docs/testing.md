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
SN_HOST=dev12345.service-now.com
SN_USERNAME=admin
SN_PASSWORD=your_password

# Salesforce
SF_HOST=https://your-domain.my.salesforce.com
SF_USERNAME=email@example.com
SF_PASSWORD=password
SF_SECURITY_TOKEN=security_token
SF_CLIENT_ID=your_connected_app_key
SF_CLIENT_SECRET=your_connected_app_secret
# OR use OAuth tokens:
# SF_ACCESS_TOKEN=your_access_token
# SF_REFRESH_TOKEN=your_refresh_token

# Jira
JIRA_HOST=your-domain.atlassian.net
JIRA_USERNAME=email@example.com
JIRA_API_TOKEN=your_api_token

# HubSpot
HUBSPOT_API_KEY=your_private_app_token

# Shopify
SHOPIFY_STORE=your-shop.myshopify.com
SHOPIFY_ACCESS_TOKEN=your_admin_api_token

# Zendesk
ZENDESK_SUBDOMAIN=your-subdomain
ZENDESK_EMAIL=email@example.com
ZENDESK_API_TOKEN=your_api_token

# Stripe (use test mode key!)
STRIPE_API_KEY=sk_test_...

# Google Sheets
GOOGLE_SHEETS_SPREADSHEET_ID=1PT73EFLcCy5tbpKQf1vYpfUGBvQJg2h0jsN7PdT9zno
GOOGLE_SHEETS_CREDENTIALS_FILE=path/to/service_account.json

# AWS S3 (Cloud Storage)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
# AWS_ENDPOINT_URL=http://localhost:9000  # For MinIO
```

## 3. Running Integration Tests

Integration tests are located in the `playground/` directory. Each test file validates a specific adapter against a live API.

### Available Test Suites

| Test File | Adapter | Tests Covered |
|-----------|---------|---------------|
| `test_sn_full.py` | ServiceNow | SELECT, WHERE, LIMIT/OFFSET, Schema, CRUD, CDC, Materialized Views |
| `test_salesforce_full.py` | Salesforce | SOQL pushdown, OAuth, Bulk API, Aggregations, CRUD |
| `test_jira_full.py` | Jira | JQL pushdown, Issue CRUD, Pagination, Attachments |
| `test_hubspot_full.py` | HubSpot | CRM Search API, Smart COUNT, Contacts/Companies/Deals |
| `test_shopify_full.py` | Shopify | Orders/Products/Customers, Smart COUNT, CRUD |
| `test_zendesk_full.py` | Zendesk | Tickets/Users/Orgs, Search API, CRUD |
| `test_stripe_full.py` | Stripe | Charges/Customers/Invoices, Search vs List API |
| `test_google_sheets_full.py` | Google Sheets | Sheet as Table, Client-Side Filtering, INSERT |
| `test_cloud_storage_full.py` | Cloud Storage | S3 (MinIO), GCS, Azure Blob, Local Files |
| `test_files_full.py` | File Adapter | CSV, Parquet, Excel (Local) |
| `test_public_api_full.py` | Generic REST | Public APIs (no auth needed) |

### Running Tests

```bash
# Activate virtual environment
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # macOS/Linux

# Run a specific adapter test
python playground/test_hubspot_full.py
python playground/test_salesforce_full.py
python playground/test_cloud_storage_full.py

# Run unit tests (no credentials needed)
pytest tests/ -v

# Run all tests with coverage
pytest tests/ --cov=waveql --cov-report=html
```

### Test Output

Each test suite produces a summary showing pass/fail status:

```
============================================================
  TEST SUMMARY
============================================================
  [PASS]  Basic SELECT
  [PASS]  Column Selection
  [PASS]  Predicate Pushdown
  [PASS]  Schema Discovery
  [PASS]  CRUD Operations
  ...

  Result: 14/14 tests passed

  ** ALL HUBSPOT TESTS PASSED! **
```

---

## 4. Public REST APIs (No Credentials)

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

## 5. Cloud Storage & Data Lakes

### 🪣 Local Emulator Suite (Recommended)

WaveQL provides a Docker Compose setup to run local emulators for AWS S3, Google Cloud Storage, and Azure Blob Storage. This allows you to run the full cloud storage validation suite (`test_cloud_storage_full.py`) without real cloud credentials.

1.  **Start the Emulators**:
    ```bash
    docker-compose -f docker-compose.test.yml up -d
    ```
    This starts:
    *   **MinIO** (S3 compatible) at `http://localhost:9000`
    *   **Fake GCS Server** at `http://localhost:4443`
    *   **Azurite** (Azure Blob) at `http://localhost:10000`

2.  **Run the Tests**:
    The test script automatically configures the environment to use these local emulators.
    ```bash
    python playground/test_cloud_storage_full.py
    ```

3.  **Stop Emulators**:
    ```bash
    docker-compose -f docker-compose.test.yml down
    ```

### ☁️ Real Cloud Testing
You can also test against real cloud providers by setting credentials in your `.env` file. Referece `playground/test_cloud_storage_full.py` for required variables.

*   **AWS S3**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
*   **GCS**: `GOOGLE_APPLICATION_CREDENTIALS` (path to JSON)
*   **Azure**: `AZURE_STORAGE_CONNECTION_STRING`

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

### 🎤 Singer Taps
1.  **Install a Tap**:
    ```bash
    pip install tap-github
    ```
2.  **Configuration**:
    Create a `config_github.json` with your access token.
3.  **Connection**:
    ```python
    conn = waveql.connect(
        "singer://tap-github",
        config_path="config_github.json"
    )
    ```

