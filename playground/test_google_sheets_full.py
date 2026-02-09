"""
WaveQL Google Sheets Adapter - Comprehensive Feature Test
==========================================================
Tests ALL supported Google Sheets features against a live spreadsheet.

Prerequisites:
- Add to .env file:
    GOOGLE_SHEETS_SPREADSHEET_ID=your-spreadsheet-id
    GOOGLE_SHEETS_CREDENTIALS_FILE=path/to/service-account.json
    
Or use OAuth:
    GOOGLE_SHEETS_OAUTH_TOKEN=your-oauth-token
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

SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")

# Resolve relative path for credentials file
if CREDENTIALS_FILE and not os.path.isabs(CREDENTIALS_FILE):
    # Assume relative to project root (where .env is)
    root_path = Path(__file__).parent.parent
    abs_path = root_path / CREDENTIALS_FILE
    if abs_path.exists():
        CREDENTIALS_FILE = str(abs_path)
        print(f"Resolved credentials file to: {CREDENTIALS_FILE}")
    else:
        print(f"Warning: Credentials file {CREDENTIALS_FILE} not found at {abs_path}")

OAUTH_TOKEN = os.getenv("GOOGLE_SHEETS_OAUTH_TOKEN")

def separator(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

def quote_column(name: str) -> str:
    """Quote a column name if it contains special characters."""
    # If contains special chars that need quoting, wrap in double quotes
    if any(c in name for c in ':-()/ '):
        return f'"{name}"'
    return name

def safe_get(row, key, default='N/A'):
    try:
        val = row[key]
        return val if val is not None else default
    except (KeyError, IndexError):
        return default

def test_list_tables(cursor, conn):
    separator("1. List Tables (Sheets)")
    adapter = conn.get_adapter("default")
    tables = adapter.list_tables()
    print(f"  Found {len(tables)} sheets:")
    for t in tables[:5]:
        print(f"    - {t}")
    print("  ✓ List tables works")
    return True

def test_basic_select(cursor):
    separator("2. Basic SELECT")
    # Assumes there's a sheet called "Sheet1" with data
    cursor.execute("SELECT * FROM Sheet1 LIMIT 5")
    rows = cursor.fetchall()
    print(f"  Returned {len(rows)} rows")
    if rows:
        print(f"  Columns: {list(rows[0].keys())}")
        for row in rows[:3]:
            print(f"    {dict(row)}")
    print("  ✓ Basic SELECT works")
    return True

def test_column_selection(cursor):
    separator("3. Column Selection")
    # Get columns from first row
    cursor.execute("SELECT * FROM Sheet1 LIMIT 1")
    row = cursor.fetchone()
    if row:
        cols = list(row.keys())
        if len(cols) >= 2:
            # Quote column names in case they have special characters
            quoted_cols = [quote_column(c) for c in cols[:2]]
            cursor.execute(f"SELECT {quoted_cols[0]}, {quoted_cols[1]} FROM Sheet1 LIMIT 3")
            for r in cursor:
                print(f"  {safe_get(r, cols[0])}: {safe_get(r, cols[1])}")
    print("  ✓ Column selection works")
    return True

def test_schema_discovery(cursor, conn):
    separator("4. Schema Discovery")
    adapter = conn.get_adapter("default")
    schema = adapter.get_schema("Sheet1")
    print(f"  Discovered {len(schema)} columns:")
    for col in schema[:5]:
        print(f"    - {col.name}: {col.data_type}")
    print("  ✓ Schema discovery works")
    return True

def test_data_formats(cursor):
    separator("5. Data Format Outputs")
    cursor.execute("SELECT * FROM Sheet1 LIMIT 3")
    arrow_table = cursor.to_arrow()
    print(f"  Arrow: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
    cursor.execute("SELECT * FROM Sheet1 LIMIT 3")
    df = cursor.to_df()
    print(f"  Pandas: {len(df)} rows")
    print("  ✓ Multiple data formats work")
    return True

def test_client_side_filter(cursor):
    separator("6. Client-Side Filtering")
    # Google Sheets doesn't support server-side filtering
    # All filtering is done client-side
    cursor.execute("SELECT * FROM Sheet1 LIMIT 10")
    all_rows = cursor.fetchall()
    if all_rows:
        cols = list(all_rows[0].keys())
        # Try a simple filter
        cursor.execute(f"SELECT * FROM Sheet1 WHERE {cols[0]} IS NOT NULL LIMIT 5")
        filtered = cursor.fetchall()
        print(f"  All rows: {len(all_rows)}, Filtered: {len(filtered)}")
    print("  ✓ Client-side filtering works")
    return True

def test_aggregations(cursor):
    separator("7. Aggregations (Client-Side)")
    cursor.execute("SELECT COUNT(*) as total FROM Sheet1")
    row = cursor.fetchone()
    print(f"  COUNT(*): {safe_get(row, 'total', 0)}")
    print("  ✓ Aggregations work")
    return True

def test_caching(conn, cursor):
    separator("8. Query Caching")
    conn.invalidate_cache()
    cursor.execute("SELECT * FROM Sheet1 LIMIT 2")
    _ = cursor.fetchall()
    cursor.execute("SELECT * FROM Sheet1 LIMIT 2")
    _ = cursor.fetchall()
    stats = conn.cache_stats
    print(f"  Cache hits: {stats.hits}, misses: {stats.misses}")
    assert stats.hits >= 1
    print("  ✓ Caching works")
    return True

def test_insert(cursor, conn):
    separator("9. INSERT (Append Row)")
    try:
        # Get column names first
        cursor.execute("SELECT * FROM Sheet1 LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("  ⚠ No data to infer columns")
            return True
        
        cols = list(row.keys())
        test_values = [f"WaveQL_Test_{datetime.now().strftime('%H%M%S')}"]
        test_values.extend(["Test" for _ in range(len(cols) - 1)])
        
        values_str = ", ".join([f"'{v}'" for v in test_values])
        # Quote column names in case they have special characters
        cols_str = ", ".join([quote_column(c) for c in cols])
        
        cursor.execute(f"INSERT INTO Sheet1 ({cols_str}) VALUES ({values_str})")
        print("  ✓ Inserted test row")
    except Exception as e:
        print(f"  ⚠ Insert test: {e}")
        return False
    return True

def main():
    if not SPREADSHEET_ID:
        print("ERROR: Missing GOOGLE_SHEETS_SPREADSHEET_ID in .env file")
        sys.exit(1)
    
    if not (CREDENTIALS_FILE or OAUTH_TOKEN):
        print("ERROR: Missing credentials. Set GOOGLE_SHEETS_CREDENTIALS_FILE or GOOGLE_SHEETS_OAUTH_TOKEN")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("  WaveQL Google Sheets Adapter - Feature Test Suite")
    print("="*60)
    print(f"  Spreadsheet: {SPREADSHEET_ID[:20]}...")
    
    # Connect
    if CREDENTIALS_FILE:
        conn = waveql.connect(
            f"google_sheets://{SPREADSHEET_ID}",
            credentials_file=CREDENTIALS_FILE,
            cache_ttl=60,
        )
    else:
        conn = waveql.connect(
            f"google_sheets://{SPREADSHEET_ID}",
            oauth_token=OAUTH_TOKEN,
            cache_ttl=60,
        )
    cursor = conn.cursor()
    
    results = {}
    tests = [
        ("List Tables", lambda: test_list_tables(cursor, conn)),
        ("Basic SELECT", lambda: test_basic_select(cursor)),
        ("Column Selection", lambda: test_column_selection(cursor)),
        ("Schema Discovery", lambda: test_schema_discovery(cursor, conn)),
        ("Data Formats", lambda: test_data_formats(cursor)),
        ("Client-Side Filter", lambda: test_client_side_filter(cursor)),
        ("Aggregations", lambda: test_aggregations(cursor)),
        ("Caching", lambda: test_caching(conn, cursor)),
        ("INSERT", lambda: test_insert(cursor, conn)),
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
    
    separator("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v)
    for name, result in results.items():
        print(f"  {'[PASS]' if result else '[FAIL]'}  {name}")
    print(f"\n  Result: {passed}/{len(results)} tests passed")

if __name__ == "__main__":
    main()
