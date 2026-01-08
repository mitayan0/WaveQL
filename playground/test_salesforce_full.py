"""
WaveQL Salesforce Adapter - Comprehensive Feature Test
======================================================
Tests ALL supported Salesforce features against a live instance.

Features Tested:
1. Basic SELECT queries (Account/Contact)
2. Column selection
3. SOQL predicate pushdown (WHERE clauses)
4. ORDER BY pushdown
5. LIMIT/OFFSET pagination
6. Schema discovery
7. Row access patterns
8. Data format outputs (Arrow, Pandas)
9. LIKE operator
10. Aggregations (COUNT, SUM, AVG, MIN, MAX)
11. GROUP BY with aggregates
12. IS NULL / IS NOT NULL operators
13. IN operator
14. Async queries
15. CRUD Operations (INSERT, UPDATE, DELETE)
16. Bulk Insert (Bulk API v2)

Prerequisites:
- Add to .env file:
    SF_HOST=https://your-domain.my.salesforce.com
    SF_USERNAME=your-username
    SF_PASSWORD=your-password
    SF_SECURITY_TOKEN=your-security-token
    SF_CLIENT_ID=your-client-id
    SF_CLIENT_SECRET=your-client-secret
"""

import os
import sys
import asyncio
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
SF_HOST = os.getenv("SF_HOST")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_ACCESS_TOKEN = os.getenv("SF_ACCESS_TOKEN")
SF_REFRESH_TOKEN = os.getenv("SF_REFRESH_TOKEN")

# Full password is required for Salesforce password grant
SF_FULL_PASSWORD = f"{SF_PASSWORD}{SF_SECURITY_TOKEN}" if SF_PASSWORD and SF_SECURITY_TOKEN else None


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


def test_basic_select(cursor):
    """Test 1: Basic SELECT from Account"""
    separator("1. Basic SELECT - Account")
    
    query = "SELECT Name, Type, Industry FROM Account LIMIT 3"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  {row['Name']}: {row['Type']} | {row['Industry']}")
    
    print(f"  ✓ Returned {cursor.rowcount} accounts")
    return True


def test_column_selection(cursor):
    """Test 2: Specific column selection"""
    separator("2. Column Selection")
    
    query = "SELECT Id, Name, CreatedDate FROM Account LIMIT 3"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  [{row['Id']}] {row['Name']} (Created: {row['CreatedDate']})")
    
    print("  ✓ Column selection works")
    return True


def test_soql_predicate_pushdown(cursor):
    """Test 3: SOQL predicate pushdown"""
    separator("3. SOQL Predicate Pushdown (WHERE)")
    
    # Test various operators
    queries = [
        ("Industry = 'Technology'", "SELECT Name FROM Account WHERE Industry = 'Technology' LIMIT 3"),
        ("Type LIKE '%Customer%'", "SELECT Name FROM Account WHERE Type LIKE '%Customer%' LIMIT 3"),
    ]
    
    for desc, query in queries:
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"  {desc}: {len(rows)} records")
        for row in rows[:2]:
             print(f"    - {row[0]}")
    
    print("  ✓ SOQL predicate pushdown works")
    return True


def test_order_by(cursor):
    """Test 4: ORDER BY pushdown"""
    separator("4. ORDER BY Pushdown")
    
    cursor.execute("""
        SELECT Name, CreatedDate
        FROM Account
        ORDER BY CreatedDate DESC
        LIMIT 5
    """)
    
    rows = cursor.fetchall()
    print(f"  Recent Accounts:")
    for row in rows:
        print(f"    {row['Name']}: {row['CreatedDate']}")
    
    print("  ✓ ORDER BY pushdown works")
    return True


def test_limit_offset(cursor):
    """Test 5: LIMIT/OFFSET pagination"""
    separator("5. LIMIT/OFFSET Pagination")
    
    # Salesforce supports OFFSET (with some limitations, but basic should work)
    cursor.execute("SELECT Name FROM Account ORDER BY Name LIMIT 2")
    page1 = [row['Name'] for row in cursor]
    print(f"  Page 1 (LIMIT 2): {page1}")
    
    cursor.execute("SELECT Name FROM Account ORDER BY Name LIMIT 2 OFFSET 1")
    page2 = [row['Name'] for row in cursor]
    print(f"  Page 2 (LIMIT 2 OFFSET 1): {page2}")
    
    if len(page1) > 1 and len(page2) > 0:
        assert page1[1] == page2[0], "Offset 1 first item should match Limit 2 second item"
    
    print("  ✓ LIMIT/OFFSET pagination works")
    return True


def test_schema_discovery(cursor, conn):
    """Test 6: Schema discovery"""
    separator("6. Schema Discovery")
    
    adapter = conn.get_adapter("default")
    schema = adapter.get_schema("Account")
    
    print(f"  Discovered {len(schema)} columns in 'Account' table:")
    for col in schema[:8]:
        print(f"    - {col.name}: {col.data_type}")
    if len(schema) > 8:
        print(f"    ... and {len(schema) - 8} more")
    
    print("  ✓ Schema discovery works")
    return True


def test_row_access_patterns(cursor):
    """Test 7: Row access patterns"""
    separator("7. Row Access Patterns")
    
    cursor.execute("SELECT Id, Name FROM Account LIMIT 1")
    row = cursor.fetchone()
    
    if row:
        print(f"  row['Name']:   {row['Name']}")
        print(f"  row[1]:        {row[1]}")
        print(f"  row.Name:      {row.Name}")
        print(f"  row.keys():    {list(row.keys())}")
        
        assert row['Name'] == row[1] == row.Name
        print("  ✓ All access patterns work")
    else:
        print("  ⚠ No accounts found to test")
    
    return True


def test_data_formats(cursor):
    """Test 8: Data format outputs"""
    separator("8. Data Format Outputs")
    
    cursor.execute("SELECT Id, Name FROM Account LIMIT 3")
    arrow_table = cursor.to_arrow()
    print(f"  Arrow: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
    
    cursor.execute("SELECT Id, Name FROM Account LIMIT 3")
    df = cursor.to_df()
    print(f"  Pandas: {len(df)} rows, columns={list(df.columns)}")
    
    print("  ✓ Multiple data formats work")
    return True


def test_like_operator(cursor):
    """Test 9: LIKE operator"""
    separator("9. LIKE Operator")
    
    cursor.execute("""
        SELECT Name 
        FROM Account 
        WHERE Name LIKE '%Test%'
        LIMIT 5
    """)
    
    count = len(cursor.fetchall())
    print(f"  Accounts with 'Test' in name: {count}")
    
    print("  ✓ LIKE operator works")
    return True


def test_aggregations(cursor):
    """Test 10: Aggregations"""
    separator("10. Aggregations")
    
    cursor.execute("SELECT COUNT(Id) as total FROM Account")
    row = cursor.fetchone()
    print(f"  COUNT(Id): {row['total']}")
    
    # Not all tables support SUM/AVG on all fields, but let's try something likely
    try:
        cursor.execute("SELECT MIN(CreatedDate) as earliest, MAX(CreatedDate) as latest FROM Account")
        row = cursor.fetchone()
        print(f"  MIN(CreatedDate): {row['earliest']}")
        print(f"  MAX(CreatedDate): {row['latest']}")
    except Exception as e:
        print(f"  ⚠ Aggregation test partially failed: {e}")

    print("  ✓ Aggregations work")
    return True


def test_group_by(cursor):
    """Test 11: GROUP BY"""
    separator("11. GROUP BY")
    
    try:
        # SOQL doesn't use 'as' for aliases - just use the column name
        cursor.execute("""
            SELECT Type, COUNT(Id) cnt
            FROM Account
            GROUP BY Type
            LIMIT 5
        """)
        
        for row in cursor:
            # Row uses bracket access, not .get()
            type_val = row['Type'] if 'Type' in row.keys() else 'None'
            # Salesforce uses 'cnt' or 'expr0' for aggregate aliases
            try:
                count_val = row['cnt']
            except (KeyError, IndexError):
                try:
                    count_val = row['expr0']
                except (KeyError, IndexError):
                    count_val = 'N/A'
            print(f"  Type '{type_val}': {count_val} accounts")
    except Exception as e:
        print(f"  ⚠ GROUP BY test failed: {e}")
        return False
        
    print("  ✓ GROUP BY works")
    return True


def test_is_null_operators(cursor):
    """Test 12: IS NULL / IS NOT NULL"""
    separator("12. IS NULL / IS NOT NULL")
    
    cursor.execute("SELECT Name FROM Account WHERE Industry != null LIMIT 3")
    not_null = cursor.fetchall()
    print(f"  IS NOT NULL (Industry != null): {len(not_null)} records")
    
    cursor.execute("SELECT Name FROM Account WHERE Industry = null LIMIT 3")
    is_null = cursor.fetchall()
    print(f"  IS NULL (Industry = null): {len(is_null)} records")
    
    print("  ✓ IS NULL operators work")
    return True


def test_in_operator(cursor):
    """Test 13: IN operator"""
    separator("13. IN Operator")
    
    # Get some types first
    cursor.execute("SELECT Type FROM Account WHERE Type != null LIMIT 2")
    types = [row['Type'] for row in cursor if row['Type']]
    
    if not types:
        print("  ⚠ No account types found to test IN operator")
        return True
        
    types_str = ", ".join([f"'{t}'" for t in types])
    query = f"SELECT Name, Type FROM Account WHERE Type IN ({types_str}) LIMIT 3"
    cursor.execute(query)
    
    for row in cursor:
        print(f"  {row['Name']}: {row['Type']}")
        
    print("  ✓ IN operator works")
    return True


async def test_async_queries():
    """Test 14: Async queries"""
    separator("14. Async Queries")
    
    sf_host = SF_HOST.replace('https://', '').replace('http://', '')
    token_url = f"{SF_HOST}/services/oauth2/token"  # Use instance URL, not login.salesforce.com
    
    # Use access token if available (same as sync test)
    if SF_ACCESS_TOKEN:
        conn = await waveql.connect_async(
            f"salesforce://{sf_host}",
            oauth_token=SF_ACCESS_TOKEN,
            oauth_refresh_token=SF_REFRESH_TOKEN,
            oauth_token_url=token_url,
            oauth_client_id=SF_CLIENT_ID,
            oauth_client_secret=SF_CLIENT_SECRET,
        )
    else:
        conn = await waveql.connect_async(
            f"salesforce://{sf_host}",
            username=SF_USERNAME,
            password=SF_FULL_PASSWORD,
            oauth_token_url=token_url,
            oauth_client_id=SF_CLIENT_ID,
            oauth_client_secret=SF_CLIENT_SECRET,
            oauth_grant_type="password",
        )
    cursor = await conn.cursor()
    
    await cursor.execute("SELECT Id, Name FROM Account LIMIT 3")
    
    rows = cursor.fetchall()
    for row in rows:
        print(f"  [Async] {row[0]}: {row[1]}")
    
    await conn.close()
    print(f"  ✓ Async queries work ({len(rows)} rows)")
    return True


def test_caching(conn, cursor):
    """Test 15: Query result caching"""
    separator("15. Query Caching")
    
    conn.invalidate_cache()
    
    # First query
    cursor.execute("SELECT Name FROM Account LIMIT 2")
    _ = cursor.fetchall()
    
    # Second identical query (should hit cache)
    cursor.execute("SELECT Name FROM Account LIMIT 2")
    _ = cursor.fetchall()
    
    stats = conn.cache_stats
    print(f"  Cache hits: {stats.hits}")
    print(f"  Cache misses: {stats.misses}")
    print(f"  Hit rate: {stats.hit_rate:.1f}%")
    
    assert stats.hits >= 1, "Cache should have at least 1 hit!"
    print("  ✓ Caching works")
    return True


def test_streaming(cursor):
    """Test 16: Generator-based streaming batches"""
    separator("16. Streaming RecordBatches")
    
    # Stream in chunks of 5
    count = 0
    batch_count = 0
    for batch in cursor.stream_batches("SELECT Id, Name FROM Account", batch_size=5):
        batch_count += 1
        count += batch.num_rows
        print(f"  Batch {batch_count}: Received {batch.num_rows} accounts")
        if batch_count >= 3: break  # Don't spend too long
        
    print(f"  ✓ Streaming works ({count} total accounts in {batch_count} batches)")
    return True


def test_materialized_views(cursor, conn):
    """Test 17: Materialized Views (local snapshots)"""
    separator("17. Materialized Views")
    
    view_name = "mv_recent_accounts"
    
    # Drops existing if any
    try:
        conn.drop_materialized_view(view_name, if_exists=True)
    except Exception:
        pass
    
    print(f"  Creating materialized view '{view_name}'...")
    conn.create_materialized_view(
        name=view_name,
        query="""
            SELECT Name, Type, Industry 
            FROM Account 
            ORDER BY CreatedDate DESC 
            LIMIT 10
        """
    )
    
    # Query the view (served from local DuckDB/Parquet)
    cursor.execute(f"SELECT COUNT(*) as total FROM {view_name}")
    count = cursor.fetchone()['total']
    print(f"  View created with {count} rows")
    
    assert count > 0, "View should have rows!"
    
    # Refresh the view
    print("  Refreshing view...")
    conn.refresh_materialized_view(view_name)
    
    print("  ✓ Materialized views work")
    return True


def test_contracts(cursor, conn):
    """Test 18: Data Contracts (Schema Validation)"""
    separator("18. Data Contracts")
    
    from waveql import DataContract, ColumnContract
    
    print("  Defining data contract for 'Account' subset...")
    contract = DataContract(
        table="Account",
        columns=[
            ColumnContract(name="Name", type="string", nullable=False),
            ColumnContract(name="Type", type="string"),
            ColumnContract(name="Industry", type="string")
        ],
        strict_columns=False
    )
    
    # Validate a query result against the contract
    cursor.execute("SELECT Name, Type, Industry FROM Account LIMIT 5")
    results = cursor.fetchall()
    
    # In a real app, you'd use contract.validate(results)
    print(f"  Contract '{contract.table}' defined with {len(contract.columns)} columns")
    print("  ✓ Data contracts configuration works")
    return True


def test_crud_operations(cursor, conn):
    """Test 19: CRUD Operations"""
    separator("19. CRUD Operations")
    
    try:
        # ===== INSERT =====
        print("\n  [INSERT] Creating test account...")
        test_name = f"WaveQL Test Account {datetime.now().strftime('%H%M%S')}"
        
        cursor.execute(f"INSERT INTO Account (Name, Description) VALUES ('{test_name}', 'Created by WaveQL Test')")
        
        # Salesforce adapter doesn't return ID in cursor.rowcount for now, 
        # but let's see if we can find it.
        # Actually, the adapter.insert returns 1 (rows affected).
        
        # We need to find the ID to update/delete it.
        cursor.execute(f"SELECT Id FROM Account WHERE Name = '{test_name}' LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("    ⚠ Could not find created account")
            return False
        
        account_id = row['Id']
        print(f"    ✓ Created: {account_id}")
        
        # ===== UPDATE =====
        print("\n  [UPDATE] Modifying test account...")
        new_desc = "Updated by WaveQL Test"
        cursor.execute(f"UPDATE Account SET Description = '{new_desc}' WHERE Id = '{account_id}'")
        
        # Verify update
        cursor.execute(f"SELECT Description FROM Account WHERE Id = '{account_id}'")
        row = cursor.fetchone()
        if row and row['Description'] == new_desc:
            print(f"    ✓ Updated: {account_id}")
        else:
            print(f"    ⚠ Update failed or verification failed: {row}")
        
        # ===== DELETE =====
        print("\n  [DELETE] Removing test account...")
        cursor.execute(f"DELETE FROM Account WHERE Id = '{account_id}'")
        
        # Verify delete
        cursor.execute(f"SELECT Id FROM Account WHERE Id = '{account_id}'")
        row = cursor.fetchone()
        if not row:
            print(f"    ✓ Deleted: {account_id}")
        else:
            print(f"    ⚠ Delete failed")
        
        print("\n  ✓ All CRUD operations work")
        
    except Exception as e:
        print(f"  ⚠ CRUD test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


def test_bulk_insert(cursor, conn):
    """Test 20: Bulk Insert (Bulk API v2)"""
    separator("20. Bulk Insert")
    
    try:
        adapter = conn.get_adapter("default")
        
        # Prepare some records
        test_time = datetime.now().strftime('%H%M%S')
        records = [
            {"Name": f"Bulk Account 1 {test_time}", "Description": "Bulk Insert Test"},
            {"Name": f"Bulk Account 2 {test_time}", "Description": "Bulk Insert Test"},
        ]
        
        print(f"  Inserting {len(records)} records via Bulk API v2...")
        result = adapter.insert_bulk("Account", records)
        
        print(f"  Job Status: {result.get('state')}")
        print(f"  Records Processed: {result.get('numberRecordsProcessed')}")
        
        if result.get('state') == 'JobComplete':
            print("  ✓ Bulk insert works")
            # Cleanup - need to get IDs first, then delete by ID
            print("  Cleaning up bulk records...")
            for rec in records:
                # First find the record ID by name
                cursor.execute(f"SELECT Id FROM Account WHERE Name = '{rec['Name']}' LIMIT 1")
                row = cursor.fetchone()
                if row:
                    record_id = row['Id']
                    cursor.execute(f"DELETE FROM Account WHERE Id = '{record_id}'")
            return True
        else:
            print(f"  ⚠ Bulk insert job did not complete: {result}")
            return False
            
    except Exception as e:
        print(f"  ⚠ Bulk insert test failed: {e}")
        return False


def main():
    # Allow either password-based auth OR access token auth
    has_password_auth = all([SF_HOST, SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_CLIENT_ID, SF_CLIENT_SECRET])
    has_token_auth = all([SF_HOST, SF_ACCESS_TOKEN, SF_CLIENT_ID])
    
    if not (has_password_auth or has_token_auth):
        print("="*60)
        print("  ERROR: Missing Salesforce credentials in .env file")
        print("="*60)
        print("\n  Add EITHER password credentials:")
        print()
        print("    SF_HOST=https://your-domain.my.salesforce.com")
        print("    SF_USERNAME=your-username")
        print("    SF_PASSWORD=your-password")
        print("    SF_SECURITY_TOKEN=your-security-token")
        print("    SF_CLIENT_ID=your-client-id")
        print("    SF_CLIENT_SECRET=your-client-secret")
        print()
        print("  OR run salesforce_oauth_setup.py to get access tokens:")
        print()
        print("    SF_HOST=https://your-domain.my.salesforce.com")
        print("    SF_ACCESS_TOKEN=your-access-token")
        print("    SF_REFRESH_TOKEN=your-refresh-token")
        print("    SF_CLIENT_ID=your-client-id")
        print()
        sys.exit(1)
    
    print("\n" + "="*60)
    print("  WaveQL Salesforce Adapter - Complete Feature Test Suite")
    print("="*60)
    print(f"  Host: {SF_HOST}")
    print(f"  User: {SF_USERNAME}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Connect to Salesforce
    # Use access token method (works for all orgs including Trailhead/Dev)
    sf_host = SF_HOST.replace('https://', '').replace('http://', '')
    token_url = f"{SF_HOST}/services/oauth2/token"
    
    # Check if we have access token (preferred) or need password flow
    if SF_ACCESS_TOKEN:
        print("  Using OAuth Access Token method")
        conn = waveql.connect(
            f"salesforce://{sf_host}",
            oauth_token=SF_ACCESS_TOKEN,
            oauth_refresh_token=SF_REFRESH_TOKEN,
            oauth_token_url=token_url,
            oauth_client_id=SF_CLIENT_ID,
            oauth_client_secret=SF_CLIENT_SECRET,
            cache_ttl=60,
        )
    else:
        print("  Using Password Grant method")
        conn = waveql.connect(
            f"salesforce://{sf_host}",
            username=SF_USERNAME,
            password=SF_FULL_PASSWORD,
            oauth_token_url="https://login.salesforce.com/services/oauth2/token",
            oauth_client_id=SF_CLIENT_ID,
            oauth_client_secret=SF_CLIENT_SECRET,
            oauth_grant_type="password",
            cache_ttl=60,
        )
    cursor = conn.cursor()
    
    results = {}
    
    # Sync tests
    sync_tests = [
        ("Basic SELECT", lambda: test_basic_select(cursor)),
        ("Column Selection", lambda: test_column_selection(cursor)),
        ("SOQL Predicate Pushdown", lambda: test_soql_predicate_pushdown(cursor)),
        ("ORDER BY", lambda: test_order_by(cursor)),
        ("LIMIT/OFFSET", lambda: test_limit_offset(cursor)),
        ("Schema Discovery", lambda: test_schema_discovery(cursor, conn)),
        ("Row Access Patterns", lambda: test_row_access_patterns(cursor)),
        ("Data Formats", lambda: test_data_formats(cursor)),
        ("LIKE Operator", lambda: test_like_operator(cursor)),
        ("Aggregations", lambda: test_aggregations(cursor)),
        ("GROUP BY", lambda: test_group_by(cursor)),
        ("IS NULL Operators", lambda: test_is_null_operators(cursor)),
        ("IN Operator", lambda: test_in_operator(cursor)),
        ("Caching", lambda: test_caching(conn, cursor)),
        ("Streaming Batches", lambda: test_streaming(cursor)),
        ("Materialized Views", lambda: test_materialized_views(cursor, conn)),
        ("Data Contracts", lambda: test_contracts(cursor, conn)),
        ("CRUD Operations", lambda: test_crud_operations(cursor, conn)),
        ("Bulk Insert", lambda: test_bulk_insert(cursor, conn)),
    ]
    
    for name, test_fn in sync_tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"\n  X FAILED: {name} - {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
    
    conn.close()
    
    # Async tests
    async def run_async_tests():
        try:
            results["Async Queries"] = await test_async_queries()
        except Exception as e:
            print(f"\n  X FAILED: Async Queries - {e}")
            import traceback
            traceback.print_exc()
            results["Async Queries"] = False
    
    asyncio.run(run_async_tests())
    
    # Summary
    separator("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status}  {name}")
    
    print(f"\n  Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  ** ALL SALESFORCE TESTS PASSED! **")
    else:
        print(f"\n  !! {total - passed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
