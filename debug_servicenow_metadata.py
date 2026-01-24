import asyncio
import os
import sys
import httpx
from waveql.adapters.servicenow import ServiceNowAdapter

# Usage: python debug_servicenow_metadata.py <instance_url> <username> <password> <table>
# Example: python debug_servicenow_metadata.py https://dev12345.service-now.com admin password incident

async def debug_metadata(host, username, password, table_name="incident"):
    print(f"--- Debugging Metadata for table: {table_name} ---")
    print(f"Host: {host}")
    
    # Manually constructing the request to mirror the adapter's logic
    url = f"{host}/api/now/table/sys_dictionary"
    params = {
        "sysparm_query": f"name={table_name}",
        "sysparm_fields": "element,internal_type,mandatory,primary,attributes,default_value,read_only",
        "sysparm_limit": "10" 
    }
    
    auth = (username, password)
    
    print(f"Requesting: {url}")
    print(f"Params: {params}")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params, auth=auth)
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                result = data.get("result", [])
                print(f"Record Count: {len(result)}")
                if not result:
                    print("WARNING: No records returned. Check if table name is correct or if ACLs prevent access.")
                
                for i, row in enumerate(result):
                    print(f"\n[Row {i}]")
                    print(f"  Element: {row.get('element')}")
                    print(f"  Mandatory: {row.get('mandatory')}")
                    print(f"  Primary: {row.get('primary')}")
                    print(f"  Default Value: {row.get('default_value')}")
                    print(f"  Read Only: {row.get('read_only')}")
                    
            elif response.status_code == 403:
                print("ERROR: 403 Forbidden. User does not have permission to read sys_dictionary.")
                print("Suggestion: Grant 'read' access to sys_dictionary for this user.")
            else:
                print(f"ERROR: request failed: {response.text}")

        except Exception as e:
            print(f"EXCEPTION: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python debug_servicenow_metadata.py <host> <user> <pass> [table]")
        sys.exit(1)
    
    host = sys.argv[1]
    user = sys.argv[2]
    pwd = sys.argv[3]
    table = sys.argv[4] if len(sys.argv) > 4 else "incident"
    
    asyncio.run(debug_metadata(host, user, pwd, table))
