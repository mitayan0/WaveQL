import asyncio
import sys
import httpx

# Usage: python debug_servicenow_hierarchy.py <instance_url> <username> <password> <table>

async def debug_hierarchy(host, username, password, start_table="sc_req_item"):
    print(f"--- Debugging Hierarchy Crawl for: {start_table} ---")
    auth = (username, password)
    
    # 1. Resolve Hierarchy
    hierarchy = [start_table]
    current_table = start_table
    print("\n[Step 1] Crawling sys_db_object...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(5):
            url = f"{host}/api/now/table/sys_db_object"
            params = {
                "sysparm_query": f"name={current_table}",
                "sysparm_fields": "super_class.name,name",
                "sysparm_limit": "1"
            }
            try:
                resp = await client.get(url, params=params, auth=auth)
                print(f"  Query {current_table}: Status {resp.status_code}")
                if resp.status_code != 200:
                    print(f"  Error: {resp.text}")
                    break
                
                data = resp.json().get("result", [])
                if not data:
                    print("  No result found (End of chain or permission/ACL issue).")
                    break
                    
                record = data[0]
                parent = record.get("super_class.name")
                print(f"  -> Found record: {record}")
                print(f"  -> Parent: {parent}")
                
                if not parent:
                    break
                    
                hierarchy.append(parent)
                current_table = parent
            except Exception as e:
                print(f"  Exception: {e}")
                break
                
    print(f"\nFinal Hierarchy: {hierarchy}")
    
    # 2. Fetch Metadata
    tables_str = ",".join(hierarchy)
    print(f"\n[Step 2] Querying sys_dictionary for: {tables_str}")
    
    url = f"{host}/api/now/table/sys_dictionary"
    params = {
        "sysparm_query": f"nameIN{tables_str}",
        "sysparm_fields": "element,internal_type,mandatory,primary,default_value,read_only,name",
        "sysparm_limit": "2000"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(url, params=params, auth=auth)
            print(f"Status: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json().get("result", [])
                print(f"Found {len(data)} fields total.")
                
                # Check critical fields
                found_number = False
                found_sys_id = False
                
                print("\n[Inspecting Key Fields]")
                for row in data:
                    element = row.get("element")
                    table = row.get("name")
                    
                    if element == "number":
                        found_number = True
                        print(f" - number (from {table}):")
                        print(f"   Mandatory: {row.get('mandatory')}")
                        print(f"   ReadOnly: {row.get('read_only')}")
                        print(f"   Default: {row.get('default_value')}")
                        
                    if element == "sys_id":
                         found_sys_id = True
                         print(f" - sys_id (from {table}):")
                         print(f"   Primary: {row.get('primary')}")
                         
                if not found_number:
                    print("WARNING: 'number' field NOT found in response!")
                if not found_sys_id:
                     print("WARNING: 'sys_id' field NOT found in response!")
                     
            else:
                print(f"Error: {resp.text}")
                
        except Exception as e:
             print(f"Exception: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python debug_servicenow_hierarchy.py <host> <user> <pass> <table>")
        sys.exit(1)
        
    host = sys.argv[1]
    user = sys.argv[2]
    pwd = sys.argv[3]
    table = sys.argv[4] if len(sys.argv) > 4 else "sc_req_item"
    asyncio.run(debug_hierarchy(host, user, pwd, table))
