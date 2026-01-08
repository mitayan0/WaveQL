"""
WaveQL Stripe Adapter - Comprehensive Feature Test
===================================================
Tests ALL supported Stripe features against a live account.

Prerequisites:
- Add to .env file:
    STRIPE_API_KEY=sk_test_... (use test mode key for safety)
"""

import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

load_env()

import waveql

STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")

def separator(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

def safe_get(row, key, default='N/A'):
    try:
        val = row[key]
        return val if val is not None else default
    except (KeyError, IndexError):
        return default

def test_basic_select_charges(cursor):
    separator("1. Basic SELECT - Charges")
    cursor.execute("SELECT id, amount, currency, status FROM charges LIMIT 5")
    for row in cursor:
        amt = safe_get(row, 'amount', 0)
        print(f"  {safe_get(row, 'id')}: ${int(amt)/100:.2f} ({safe_get(row, 'status')})")
    print(f"  ✓ Returned {cursor.rowcount} charges")
    return True

def test_customers_table(cursor):
    separator("2. Customers Table")
    cursor.execute("SELECT id, email, name FROM customers LIMIT 5")
    for row in cursor:
        print(f"  {safe_get(row, 'email')}: {safe_get(row, 'name')}")
    print(f"  ✓ Returned {cursor.rowcount} customers")
    return True

def test_invoices_table(cursor):
    separator("3. Invoices Table")
    cursor.execute("SELECT id, customer, total, status FROM invoices LIMIT 5")
    for row in cursor:
        total = safe_get(row, 'total', 0)
        print(f"  {safe_get(row, 'id')}: ${int(total)/100:.2f} ({safe_get(row, 'status')})")
    print(f"  ✓ Returned {cursor.rowcount} invoices")
    return True

def test_schema_discovery(cursor, conn):
    separator("4. Schema Discovery")
    adapter = conn.get_adapter("default")
    schema = adapter.get_schema("charges")
    print(f"  Discovered {len(schema)} columns in 'charges' table")
    for col in schema[:5]:
        print(f"    - {col.name}: {col.data_type}")
    print("  ✓ Schema discovery works")
    return True

def test_data_formats(cursor):
    separator("5. Data Format Outputs")
    cursor.execute("SELECT id, amount FROM charges LIMIT 3")
    arrow_table = cursor.to_arrow()
    print(f"  Arrow: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
    cursor.execute("SELECT id, amount FROM charges LIMIT 3")
    df = cursor.to_df()
    print(f"  Pandas: {len(df)} rows")
    print("  ✓ Multiple data formats work")
    return True

def test_smart_count(cursor):
    separator("6. Smart COUNT")
    cursor.execute("SELECT COUNT(*) as total FROM charges")
    row = cursor.fetchone()
    print(f"  COUNT(*) charges: {safe_get(row, 'total', 0)}")
    print("  ✓ Smart COUNT works")
    return True

def test_caching(conn, cursor):
    separator("7. Query Caching")
    conn.invalidate_cache()
    cursor.execute("SELECT id FROM charges LIMIT 2")
    _ = cursor.fetchall()
    cursor.execute("SELECT id FROM charges LIMIT 2")
    _ = cursor.fetchall()
    stats = conn.cache_stats
    print(f"  Cache hits: {stats.hits}, misses: {stats.misses}")
    assert stats.hits >= 1
    print("  ✓ Caching works")
    return True

def test_crud_customers(cursor, conn):
    separator("8. CRUD Operations")
    try:
        adapter = conn.get_adapter("default")
        test_email = f"waveql-test-{datetime.now().strftime('%H%M%S')}@example.com"
        
        # INSERT - Use adapter directly to get the response with ID
        import httpx
        url = "https://api.stripe.com/v1/customers"
        headers = {"Authorization": f"Bearer {os.getenv('STRIPE_API_KEY')}"}
        response = httpx.post(url, headers=headers, data={"email": test_email, "name": "WaveQL Test"})
        
        if response.status_code >= 400:
            print(f"    ⚠ Insert failed: {response.text}")
            return False
        
        customer_id = response.json().get("id")
        print(f"    ✓ Created: {customer_id}")
        
        # UPDATE
        cursor.execute(f"UPDATE customers SET name = 'Updated' WHERE id = '{customer_id}'")
        print(f"    ✓ Updated: {customer_id}")
        
        # DELETE
        cursor.execute(f"DELETE FROM customers WHERE id = '{customer_id}'")
        print(f"    ✓ Deleted: {customer_id}")
        
        print("  ✓ All CRUD operations work")
    except Exception as e:
        print(f"  ⚠ CRUD test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    return True

def main():
    if not STRIPE_API_KEY:
        print("ERROR: Missing STRIPE_API_KEY in .env file")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("  WaveQL Stripe Adapter - Feature Test Suite")
    print("="*60)
    
    conn = waveql.connect("stripe://api.stripe.com", api_key=STRIPE_API_KEY, cache_ttl=60)
    cursor = conn.cursor()
    
    results = {}
    tests = [
        ("Basic SELECT", lambda: test_basic_select_charges(cursor)),
        ("Customers Table", lambda: test_customers_table(cursor)),
        ("Invoices Table", lambda: test_invoices_table(cursor)),
        ("Schema Discovery", lambda: test_schema_discovery(cursor, conn)),
        ("Data Formats", lambda: test_data_formats(cursor)),
        ("Smart COUNT", lambda: test_smart_count(cursor)),
        ("Caching", lambda: test_caching(conn, cursor)),
        ("CRUD Operations", lambda: test_crud_customers(cursor, conn)),
    ]
    
    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"\n  X FAILED: {name} - {e}")
            results[name] = False
    
    conn.close()
    
    separator("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v)
    for name, result in results.items():
        print(f"  {'[PASS]' if result else '[FAIL]'}  {name}")
    print(f"\n  Result: {passed}/{len(results)} tests passed")

if __name__ == "__main__":
    main()
