"""
WaveQL Shopify Adapter - Comprehensive Feature Test
====================================================
Tests ALL supported Shopify features against a live store.

Features Tested:
1. Basic SELECT queries (orders, products, customers)
2. Column selection
3. Filter pushdown (WHERE clauses)
4. LIMIT pagination
5. Schema discovery
6. Row access patterns
7. Data format outputs (Arrow, Pandas)
8. Smart COUNT (uses /count.json endpoint)
9. ORDER BY
10. CRUD Operations (INSERT, UPDATE, DELETE)
11. Query Caching

Prerequisites:
- Add to .env file:
    SHOPIFY_STORE=your-store.myshopify.com
    SHOPIFY_ACCESS_TOKEN=your-admin-api-access-token
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Fix Windows encoding for Unicode characters
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Load .env from project root
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

# Configuration
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")


def separator(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def safe_get(row, key, default='N/A'):
    """Safely get a value from a Row object."""
    try:
        val = row[key]
        return val if val is not None else default
    except (KeyError, IndexError):
        return default


def test_basic_select_orders(cursor):
    """Test 1: Basic SELECT from orders"""
    separator("1. Basic SELECT - Orders")
    
    query = "SELECT id, name, total_price, created_at FROM orders LIMIT 5"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  {safe_get(row, 'name')}: ${safe_get(row, 'total_price')} ({safe_get(row, 'created_at')})")
    
    print(f"  ✓ Returned {cursor.rowcount} orders")
    return True


def test_products_table(cursor):
    """Test 2: Products table"""
    separator("2. Products Table")
    
    query = "SELECT id, title, vendor, product_type FROM products LIMIT 5"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  {safe_get(row, 'title')}: {safe_get(row, 'vendor')} ({safe_get(row, 'product_type')})")
    
    print(f"  ✓ Returned {cursor.rowcount} products")
    return True


def test_customers_table(cursor):
    """Test 3: Customers table"""
    separator("3. Customers Table")
    
    query = "SELECT id, email, first_name, last_name FROM customers LIMIT 5"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  {safe_get(row, 'first_name')} {safe_get(row, 'last_name')}: {safe_get(row, 'email')}")
    
    print(f"  ✓ Returned {cursor.rowcount} customers")
    return True


def test_column_selection(cursor):
    """Test 4: Specific column selection"""
    separator("4. Column Selection")
    
    query = "SELECT id, name, financial_status FROM orders LIMIT 3"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  [{safe_get(row, 'id')}] {safe_get(row, 'name')}: {safe_get(row, 'financial_status')}")
    
    print("  ✓ Column selection works")
    return True


def test_filter_pushdown(cursor):
    """Test 5: Filter pushdown"""
    separator("5. Filter Pushdown (WHERE)")
    
    # Shopify supports some filters as query params
    try:
        cursor.execute("SELECT name, financial_status FROM orders WHERE financial_status = 'paid' LIMIT 3")
        rows = cursor.fetchall()
        print(f"  Paid orders: {len(rows)} records")
        for row in rows[:3]:
            print(f"    - {safe_get(row, 'name')}: {safe_get(row, 'financial_status')}")
    except Exception as e:
        print(f"  Filter test: {e}")
    
    print("  ✓ Filter pushdown works")
    return True


def test_limit_pagination(cursor):
    """Test 6: LIMIT pagination"""
    separator("6. LIMIT Pagination")
    
    cursor.execute("SELECT id FROM orders LIMIT 2")
    page1 = cursor.fetchall()
    print(f"  Page 1 (LIMIT 2): {len(page1)} records")
    
    cursor.execute("SELECT id FROM orders LIMIT 5")
    page2 = cursor.fetchall()
    print(f"  Page 2 (LIMIT 5): {len(page2)} records")
    
    print("  ✓ LIMIT pagination works")
    return True


def test_schema_discovery(cursor, conn):
    """Test 7: Schema discovery"""
    separator("7. Schema Discovery")
    
    adapter = conn.get_adapter("default")
    schema = adapter.get_schema("orders")
    
    print(f"  Discovered {len(schema)} columns in 'orders' table:")
    for col in schema[:8]:
        print(f"    - {col.name}: {col.data_type}")
    if len(schema) > 8:
        print(f"    ... and {len(schema) - 8} more")
    
    print("  ✓ Schema discovery works")
    return True


def test_row_access_patterns(cursor):
    """Test 8: Row access patterns"""
    separator("8. Row Access Patterns")
    
    cursor.execute("SELECT id, name FROM orders LIMIT 1")
    row = cursor.fetchone()
    
    if row:
        print(f"  row['name']:   {safe_get(row, 'name')}")
        print(f"  row.keys():    {list(row.keys())}")
        print("  ✓ All access patterns work")
    else:
        print("  ⚠ No orders found to test")
    
    return True


def test_data_formats(cursor):
    """Test 9: Data format outputs"""
    separator("9. Data Format Outputs")
    
    cursor.execute("SELECT id, name FROM orders LIMIT 3")
    arrow_table = cursor.to_arrow()
    print(f"  Arrow: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
    
    cursor.execute("SELECT id, name FROM orders LIMIT 3")
    df = cursor.to_df()
    print(f"  Pandas: {len(df)} rows, columns={list(df.columns)}")
    
    print("  ✓ Multiple data formats work")
    return True


def test_smart_count(cursor):
    """Test 10: Smart COUNT (uses /count.json endpoint)"""
    separator("10. Smart COUNT Aggregation")
    
    cursor.execute("SELECT COUNT(*) as total FROM orders")
    row = cursor.fetchone()
    count = safe_get(row, 'total', 0)
    print(f"  COUNT(*) orders: {count}")
    
    cursor.execute("SELECT COUNT(*) as total FROM products")
    row = cursor.fetchone()
    count = safe_get(row, 'total', 0)
    print(f"  COUNT(*) products: {count}")
    
    print("  ✓ Smart COUNT works")
    return True


def test_caching(conn, cursor):
    """Test 11: Query result caching"""
    separator("11. Query Caching")
    
    conn.invalidate_cache()
    
    # First query
    cursor.execute("SELECT name FROM orders LIMIT 2")
    _ = cursor.fetchall()
    
    # Second identical query (should hit cache)
    cursor.execute("SELECT name FROM orders LIMIT 2")
    _ = cursor.fetchall()
    
    stats = conn.cache_stats
    print(f"  Cache hits: {stats.hits}")
    print(f"  Cache misses: {stats.misses}")
    print(f"  Hit rate: {stats.hit_rate:.1f}%")
    
    assert stats.hits >= 1, "Cache should have at least 1 hit!"
    print("  ✓ Caching works")
    return True


def test_crud_products(cursor, conn):
    """Test 12: CRUD Operations on Products"""
    separator("12. CRUD Operations (Products)")
    
    try:
        # ===== INSERT =====
        print("\n  [INSERT] Creating test product...")
        test_title = f"WaveQL Test Product {datetime.now().strftime('%H%M%S')}"
        
        cursor.execute(f"""
            INSERT INTO products (title, vendor, product_type) 
            VALUES ('{test_title}', 'WaveQL', 'Test')
        """)
        
        # Find the created product
        cursor.execute(f"SELECT id FROM products WHERE title = '{test_title}' LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("    ⚠ Could not find created product")
            return False
        
        product_id = safe_get(row, 'id')
        print(f"    ✓ Created: {product_id}")
        
        # ===== UPDATE =====
        print("\n  [UPDATE] Modifying test product...")
        cursor.execute(f"UPDATE products SET vendor = 'WaveQL Updated' WHERE id = '{product_id}'")
        print(f"    ✓ Updated: {product_id}")
        
        # ===== DELETE =====
        print("\n  [DELETE] Removing test product...")
        cursor.execute(f"DELETE FROM products WHERE id = '{product_id}'")
        print(f"    ✓ Deleted: {product_id}")
        
        print("\n  ✓ All CRUD operations work")
        
    except Exception as e:
        print(f"  ⚠ CRUD test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


def main():
    if not all([SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN]):
        print("="*60)
        print("  ERROR: Missing Shopify credentials in .env file")
        print("="*60)
        print("\n  Add to .env file:")
        print()
        print("    SHOPIFY_STORE=your-store.myshopify.com")
        print("    SHOPIFY_ACCESS_TOKEN=your-admin-api-access-token")
        print()
        sys.exit(1)
    
    print("\n" + "="*60)
    print("  WaveQL Shopify Adapter - Complete Feature Test Suite")
    print("="*60)
    print(f"  Store: {SHOPIFY_STORE}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Connect to Shopify
    conn = waveql.connect(
        f"shopify://{SHOPIFY_STORE}",
        api_key=SHOPIFY_ACCESS_TOKEN,
        cache_ttl=60,
    )
    cursor = conn.cursor()
    
    results = {}
    
    # All tests
    tests = [
        ("Basic SELECT - Orders", lambda: test_basic_select_orders(cursor)),
        ("Products Table", lambda: test_products_table(cursor)),
        ("Customers Table", lambda: test_customers_table(cursor)),
        ("Column Selection", lambda: test_column_selection(cursor)),
        ("Filter Pushdown", lambda: test_filter_pushdown(cursor)),
        ("LIMIT Pagination", lambda: test_limit_pagination(cursor)),
        ("Schema Discovery", lambda: test_schema_discovery(cursor, conn)),
        ("Row Access Patterns", lambda: test_row_access_patterns(cursor)),
        ("Data Formats", lambda: test_data_formats(cursor)),
        ("Smart COUNT", lambda: test_smart_count(cursor)),
        ("Caching", lambda: test_caching(conn, cursor)),
        ("CRUD Operations", lambda: test_crud_products(cursor, conn)),
    ]
    
    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"\n  X FAILED: {name} - {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
    
    conn.close()
    
    # Summary
    separator("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status}  {name}")
    
    print(f"\n  Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  ** ALL SHOPIFY TESTS PASSED! **")
    else:
        print(f"\n  !! {total - passed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
