
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load .env from project root
def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    line = line.strip()
                    if not line:
                        continue
                    key, value = line.split("=", 1)
                    os.environ[key] = value

load_env()

import waveql

# Configuration
SF_HOST = os.getenv("SF_HOST")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")

# Full password is required for Salesforce password grant
SF_FULL_PASSWORD = f"{SF_PASSWORD}{SF_SECURITY_TOKEN}" if SF_PASSWORD and SF_SECURITY_TOKEN else None

def main():
    if not all([SF_HOST, SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_CLIENT_ID, SF_CLIENT_SECRET]):
        print("Error: Missing Salesforce credentials in .env file")
        return

    sf_host = SF_HOST.replace('https://', '').replace('http://', '')
    # For some orgs might need test.salesforce.com, but usually login works
    token_url = f"https://login.salesforce.com/services/oauth2/token"
    
    print(f"Connecting to {SF_HOST}...")
    try:
        conn = waveql.connect(
            f"salesforce://{sf_host}",
            username=SF_USERNAME,
            password=SF_FULL_PASSWORD,
            oauth_token_url=token_url,
            oauth_client_id=SF_CLIENT_ID,
            oauth_client_secret=SF_CLIENT_SECRET,
            oauth_grant_type="password",
        )
        cursor = conn.cursor()
        
        print("Executing simple query: SELECT Name FROM Account LIMIT 3")
        cursor.execute("SELECT Name FROM Account LIMIT 3")
        
        rows = cursor.fetchall()
        print(f"Connection Successful! Found {len(rows)} accounts:")
        for row in rows:
            print(f" - {row[0]}")
            
        conn.close()
    except Exception as e:
        print(f"Connection Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
