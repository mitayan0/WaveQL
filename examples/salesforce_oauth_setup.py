"""
Salesforce OAuth Setup with PKCE - One-Time Browser Login
==========================================================
This script will:
1. Open your browser to Salesforce login
2. Capture the authorization code  
3. Exchange it for access + refresh tokens
4. Save the refresh token to your .env file
"""

import os
import sys
import webbrowser
import urllib.parse
import hashlib
import base64
import secrets
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
import requests

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

# Configuration
SF_HOST = os.getenv("SF_HOST", "").rstrip("/")
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
CALLBACK_PORT = 8080
CALLBACK_URL = f"http://localhost:{CALLBACK_PORT}/callback"

# Use instance URL for auth
if SF_HOST:
    AUTH_URL = f"{SF_HOST}/services/oauth2/authorize"
    TOKEN_URL = f"{SF_HOST}/services/oauth2/token"
else:
    AUTH_URL = "https://login.salesforce.com/services/oauth2/authorize"
    TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"


def generate_pkce():
    """Generate PKCE code verifier and challenge."""
    # Generate a random code verifier (43-128 characters)
    # Using 32 bytes gives us ~43 chars which is the minimum for PKCE
    code_verifier = secrets.token_urlsafe(32)
    
    # Ensure it's within bounds (43-128 chars)
    if len(code_verifier) < 43:
        code_verifier = code_verifier + secrets.token_urlsafe(8)
    code_verifier = code_verifier[:128]
    
    # Create SHA256 hash of verifier (use ascii encoding explicitly)
    sha256_hash = hashlib.sha256(code_verifier.encode('ascii')).digest()
    
    # Base64 URL encode (no padding)
    code_challenge = base64.urlsafe_b64encode(sha256_hash).decode('ascii').rstrip("=")
    
    return code_verifier, code_challenge


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handle the OAuth callback"""
    
    def log_message(self, format, *args):
        pass  # Suppress HTTP logs
    
    def do_GET(self):
        if self.path.startswith("/callback"):
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)
            
            if "code" in params:
                self.server.auth_code = params["code"][0]
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"""
                <html>
                <head><title>Success!</title></head>
                <body style="font-family: Arial; text-align: center; padding-top: 50px;">
                    <h1 style="color: green;">Authorization Successful!</h1>
                    <p>You can close this window and return to the terminal.</p>
                </body>
                </html>
                """)
            else:
                error = params.get("error", ["Unknown error"])[0]
                error_desc = params.get("error_description", [""])[0]
                self.server.auth_code = None
                self.server.error = f"{error}: {error_desc}"
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(f"""
                <html>
                <head><title>Error</title></head>
                <body style="font-family: Arial; text-align: center; padding-top: 50px;">
                    <h1 style="color: red;">Authorization Failed</h1>
                    <p>{error}: {error_desc}</p>
                </body>
                </html>
                """.encode())


def update_env_file(refresh_token, access_token):
    """Update .env file with the tokens"""
    env_path = Path(__file__).parent.parent / ".env"
    
    lines = []
    if env_path.exists():
        with open(env_path, "r") as f:
            lines = f.readlines()
    
    has_refresh = False
    has_access = False
    
    new_lines = []
    for line in lines:
        if line.startswith("SF_REFRESH_TOKEN="):
            new_lines.append(f"SF_REFRESH_TOKEN={refresh_token}\n")
            has_refresh = True
        elif line.startswith("SF_ACCESS_TOKEN="):
            new_lines.append(f"SF_ACCESS_TOKEN={access_token}\n")
            has_access = True
        else:
            new_lines.append(line)
    
    if not has_refresh:
        for i, line in enumerate(new_lines):
            if line.startswith("SF_CLIENT_SECRET="):
                new_lines.insert(i + 1, f"SF_REFRESH_TOKEN={refresh_token}\n")
                break
        else:
            new_lines.append(f"SF_REFRESH_TOKEN={refresh_token}\n")
    
    if not has_access:
        for i, line in enumerate(new_lines):
            if line.startswith("SF_REFRESH_TOKEN="):
                new_lines.insert(i + 1, f"SF_ACCESS_TOKEN={access_token}\n")
                break
    
    with open(env_path, "w") as f:
        f.writelines(new_lines)


def main():
    print("=" * 60)
    print("  Salesforce OAuth Setup (with PKCE)")
    print("=" * 60)
    print(f"  Client ID: {SF_CLIENT_ID[:20] if SF_CLIENT_ID else 'NOT SET'}...")
    print(f"  Auth URL: {AUTH_URL}")
    print(f"  Callback: {CALLBACK_URL}")
    print("=" * 60)
    
    if not SF_CLIENT_ID:
        print("\n  [ERROR] Missing SF_CLIENT_ID in .env")
        sys.exit(1)
    
    # Generate PKCE
    code_verifier, code_challenge = generate_pkce()
    print("\n  Generated PKCE code challenge")
    
    # Build authorization URL with PKCE
    auth_params = {
        "response_type": "code",
        "client_id": SF_CLIENT_ID,
        "redirect_uri": CALLBACK_URL,
        "scope": "api refresh_token offline_access",
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(auth_params)}"
    
    print("\n  Step 1: Opening browser for Salesforce login...")
    print(f"  URL: {auth_url[:80]}...")
    
    # Start local server with timeout
    server = HTTPServer(("localhost", CALLBACK_PORT), OAuthCallbackHandler)
    server.timeout = 120  # 2 minute timeout
    server.auth_code = None
    server.error = None
    
    # Open browser
    webbrowser.open(auth_url)
    
    print("\n  Step 2: Waiting for you to log in...")
    print("  (A browser window should have opened)")
    print("  Log in with your Salesforce credentials and click Allow.")
    print("  (Timeout: 2 minutes)")
    
    # Wait for callback with timeout
    server.handle_request()
    
    if server.auth_code:
        print("\n  Step 3: Got authorization code, exchanging for tokens...")
        
        # Exchange code for tokens (with PKCE verifier)
        token_data = {
            "grant_type": "authorization_code",
            "client_id": SF_CLIENT_ID,
            "redirect_uri": CALLBACK_URL,
            "code": server.auth_code,
            "code_verifier": code_verifier,
        }
        
        # Add client_secret if available
        if SF_CLIENT_SECRET:
            token_data["client_secret"] = SF_CLIENT_SECRET
        
        response = requests.post(TOKEN_URL, data=token_data)
        
        if response.ok:
            tokens = response.json()
            access_token = tokens.get("access_token", "")
            refresh_token = tokens.get("refresh_token", "")
            instance_url = tokens.get("instance_url", "")
            
            print("\n  [SUCCESS] Got tokens!")
            print(f"  Instance URL: {instance_url}")
            print(f"  Access Token: {access_token[:30]}...")
            if refresh_token:
                print(f"  Refresh Token: {refresh_token[:30]}...")
                
                update_env_file(refresh_token, access_token)
                print("\n  Step 4: Updated .env file with tokens!")
                print("\n  You can now run the Salesforce tests.")
                print("  The access token will be refreshed automatically.")
            else:
                print("\n  [WARNING] No refresh token received.")
                print("  Make sure 'refresh_token' scope is in your Connected App.")
        else:
            print("\n  [ERROR] Token exchange failed:")
            print(f"  {response.text}")
    else:
        print(f"\n  [ERROR] Authorization failed: {server.error}")
    
    print()


if __name__ == "__main__":
    main()
