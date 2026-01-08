import waveql
import time

def test_json_placeholder_advanced():
    print("--- Connecting to JSONPlaceholder API (Advanced Features) ---")
    
    endpoints = {
        "posts": {
            "path": "/posts",
            "limit_param": "_limit",
            "offset_param": "_start",
            "supports_filter": True
        },
        "users": {"path": "/users"}
    }

    # Enable caching
    conn = waveql.connect(
        adapter="rest",
        host="https://jsonplaceholder.typicode.com",
        endpoints=endpoints,
        enable_cache=True, 
        cache_ttl=60
    )
    cursor = conn.cursor()

    # --- 1. Pagination Test ---
    # JSONPlaceholder has 100 posts. Let's fetch 15, starting from offset 10.
    print("\n[Pagination] Fetching 5 posts at offset 10...")
    cursor.execute("SELECT id, title FROM posts LIMIT 5 OFFSET 10")
    rows = cursor.fetchall()
    for r in rows:
        print(f"Post {r.id}")
    
    # --- 2. Complex Filtering (IN, LIKE) ---
    print("\n[Complex Filter] Fetching posts where ID IN (1, 3, 5)...")
    # Note: JSONPlaceholder supports ?id=1&id=3... if "filter_format" is query (default)
    # The Generic REST adapter should interpret IN as multiple params if supported, 
    # or client-side filter if not.
    cursor.execute("SELECT id, title FROM posts WHERE id IN (1, 3, 5)")
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} posts: {[r.id for r in rows]}")

    print("\n[Complex Filter] Fetching posts with title LIKE '%optio%'...")
    # This will likely be client-side filtering unless the API supports 'like'
    cursor.execute("SELECT id, title FROM posts WHERE title LIKE '%optio%' LIMIT 3")
    rows = cursor.fetchall()
    for r in rows:
        print(f"Post {r.id}: {r.title}")

    # --- 3. Aggregation (Client-Side) ---
    print("\n[Aggregation] Counting posts per user (GROUP BY)...")
    # This forces fetching ALL posts (100) then grouping locally
    start = time.time()
    cursor.execute("SELECT userId, COUNT(*) as count FROM posts GROUP BY userId LIMIT 5")
    rows = cursor.fetchall()
    print(f"Aggregation took {time.time() - start:.4f}s")
    for r in rows:
        print(f"User {r.userId}: {r.count} posts")

    # --- 4. Caching Test ---
    print("\n[Caching] Running aggregation again...")
    start = time.time()
    # Should be instant
    cursor.execute("SELECT userId, COUNT(*) as count FROM posts GROUP BY userId LIMIT 5")
    rows = cursor.fetchall()
    print(f"Cached aggregation took {time.time() - start:.4f}s")
    
    # Check stats
    stats = conn.cache_stats
    print(f"Cache Stats: Hits={stats.hits}, Misses={stats.misses}")

if __name__ == "__main__":
    test_json_placeholder_advanced()
