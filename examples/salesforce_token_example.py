"""
Test Salesforce Connection with Access Token
=============================================
Uses the access token from the OAuth flow instead of password grant.
"""

import os
import sys
from pathlib import Path

# Add project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Fix Windows encoding  
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Load .env
def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#") and line:
                    key, value = line.split("=", 1)
                    os.environ[key] = value

load_env()

import waveql

# Configuration from .env
SF_HOST = os.getenv("SF_HOST")
SF_ACCESS_TOKEN = os.getenv("SF_ACCESS_TOKEN")
SF_REFRESH_TOKEN = os.getenv("SF_REFRESH_TOKEN")
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")


def main():
    if not SF_HOST or not SF_ACCESS_TOKEN:
        print("Error: Missing SF_HOST or SF_ACCESS_TOKEN in .env")
        print("Run salesforce_oauth_setup.py first to get tokens.")
        return
    
    sf_host = SF_HOST.replace("https://", "").replace("http://", "")
    token_url = f"{SF_HOST}/services/oauth2/token"
    
    print(f"Connecting to {SF_HOST}...")
    print("Using existing access token")
    
    try:
        # Connect using existing access token (and refresh token for auto-refresh)
        conn = waveql.connect(
            f"salesforce://{sf_host}",
            oauth_token=SF_ACCESS_TOKEN,
            oauth_refresh_token=SF_REFRESH_TOKEN,
            oauth_token_url=token_url,
            oauth_client_id=SF_CLIENT_ID,
            oauth_client_secret=SF_CLIENT_SECRET,
        )
        cursor = conn.cursor()
        
        print("Executing: SELECT Id, Name FROM Account LIMIT 5")
        cursor.execute("SELECT Id, Name FROM Account LIMIT 5")
        
        rows = cursor.fetchall()
        print(f"\n[SUCCESS] Connected! Found {len(rows)} accounts:")
        for row in rows:
            print(f"  - {row['Id']}: {row['Name']}")
        
        conn.close()
        print("\nConnection test PASSED!")
        
    except Exception as e:
        print(f"\nConnection Failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
