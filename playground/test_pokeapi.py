import waveql
import time
import pyarrow as pa

def test_pokeapi_complex():
    print("--- Connecting to PokeAPI (Complex Nested Data) ---")
    
    # Configure endpoints specifically for PokeAPI structure
    endpoints = {
        "pokemon": {
            "path": "/pokemon",
            "data_path": "results",  # List is inside "results"
            "supports_filter": False, # PokeAPI list endpoint doesn't support complex filters
            "limit_param": "limit",
            "offset_param": "offset"
        },
        "ability": {
            "path": "/ability",
            "data_path": "results"
        }
    }

    # Enable caching to speed up subsequent runs
    conn = waveql.connect(
        adapter="rest",
        host="https://pokeapi.co/api/v2",
        endpoints=endpoints,
        enable_cache=True, 
        cache_ttl=300 # 5 minutes
    )
    cursor = conn.cursor()

    # --- 1. Basic Pagination Test ---
    print("\n[Pagination] Fetching 5 Pokemon (Offset 0)...")
    start = time.time()
    cursor.execute("SELECT name, url FROM pokemon LIMIT 5")
    rows = cursor.fetchall()
    print(f"Fetched in {time.time() - start:.2f}s")
    for r in rows:
        print(f" - {r.name}")

    print("\n[Pagination] Fetching next 5 Pokemon (Offset 5)...")
    start = time.time()
    cursor.execute("SELECT name FROM pokemon LIMIT 5 OFFSET 5")
    rows = cursor.fetchall()
    print(f"Fetched in {time.time() - start:.2f}s")
    for r in rows:
        print(f" - {r.name}")

    # --- 2. Client-Side Filtering (Pushdown Disabled) ---
    # PokeAPI's list endpoint ONLY returns name/url.
    # To filter by name, we fetch the list and filter locally.
    print("\n[Filtering] Finding Pokemon with 'char' in name...")
    # NOTE: Since supports_filter=False, adapter fetches a page (default size) and filters
    # For robust search we might need to fetch MORE, but let's see default behavior.
    
    start = time.time()
    cursor.execute("SELECT name FROM pokemon WHERE name LIKE '%char%' LIMIT 5")
    rows = cursor.fetchall()
    print(f"Fetched in {time.time() - start:.2f}s")
    if rows:
        for r in rows:
            print(f" Match: {r.name}")
    else:
        print("No matches found in default page (expected if page size is small)")


    # --- 3. Join / Cross-Reference (Manual) ---
    # Since API doesn't support joins, we do it in 2 steps or via virtual join logic
    # But PokeAPI is unique: the "url" in the list points to the details.
    
    # Let's try to fetch details using a generic endpoint trick?
    # No, Generic REST adapter is table-based.
    
    # Let's test Aggregation on the list
    print("\n[Aggregation] Counting cached Pokemon...")
    start = time.time()
    cursor.execute("SELECT COUNT(*) as count FROM pokemon")
    rows = cursor.fetchall()
    print(f"Count: {rows[0].count} (Time: {time.time() - start:.2f}s)")
    
    # --- 4. Caching Verification ---
    print("\n[Cache Stats]")
    stats = conn.cache_stats
    print(f"Hits: {stats.hits}, Misses: {stats.misses}, Size: {stats.size_mb:.2f}MB")

if __name__ == "__main__":
    test_pokeapi_complex()
