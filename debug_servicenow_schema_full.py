import asyncio
import sys
import httpx

# Usage: python debug_servicenow_schema_full.py <instance_url> <username> <password> <table>

async def debug_full_schema(host, username, password, table_name="incident"):
    print(f"--- Debugging Full Schema for table: {table_name} ---")
    auth = (username, password)
    
    # Test 1: Undocumented Schema API
    print("\n[Test 1] Checking /api/now/doc/table/schema/...")
    url_doc = f"{host}/api/now/doc/table/schema/{table_name}"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url_doc, auth=auth)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                print("SUCCESS: Endpoint available.")
                data = resp.json()
                result = data.get("result")
                print(f"Result Type: {type(result)}")
                
                cols_list = []
                if isinstance(result, dict):
                     cols_list = result.get("columns", [])
                     if isinstance(cols_list, dict):
                         cols_list = cols_list.values()
                elif isinstance(result, list):
                     cols_list = result
                
                print(f"Found {len(cols_list)} columns.")
                
                # Inspect specific columns of interest
                target_cols = ["number", "sys_id"]
                for col in cols_list:
                    name = col.get("name") or col.get("element")
                    if name == "number":
                         print(f"\n[RAW DUMP for 'number']:")
                         print(f"{col}")
                    
                    if name in target_cols:
                        print(f"\n[Field: {name}]")
                        # Try alternative keys
                        print(f"  Type: {col.get('type') or col.get('internal_type')}")
                        print(f"  Mandatory: {col.get('mandatory')}")
                        print(f"  Read Only: {col.get('read_only')}")
                        print(f"  Default: {col.get('default_value')}")
            else:
                print("FAILURE: Endpoint not available or forbidden.")
        except Exception as e:
            print(f"Exception: {e}")

    # Test 2: Checking sys_db_object for super_class
    print("\n[Test 2] Checking sys_db_object for inheritance...")
    url_db = f"{host}/api/now/table/sys_db_object"
    params = {
        "sysparm_query": f"name={table_name}",
        "sysparm_fields": "name,super_class,super_class.name"
    }
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url_db, params=params, auth=auth)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                print("SUCCESS: sys_db_object accessible.")
                res = resp.json().get("result", [])
                for r in res:
                    print(f"Table: {r.get('name')}")
                    super_c = r.get("super_class")
                    if isinstance(super_c, dict):
                         # If sysparm_display_value is not used, it might be a link object
                         print(f"Super Class keys: {super_c.keys()}")
                         # With dot-walking, we might need sysparm_display_value=true? 
                         # Actually sysparm_fields=super_class.name should return it if supported
                         print(f"Super Class Name (dot-walk): {r.get('super_class.name')}")
                    else:
                         print(f"Super Class: {super_c}")
            else:
                print("FAILURE: sys_db_object forbidden.")
        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    host = sys.argv[1]
    user = sys.argv[2]
    pwd = sys.argv[3]
    table = sys.argv[4] if len(sys.argv) > 4 else "incident"
    asyncio.run(debug_full_schema(host, user, pwd, table))
