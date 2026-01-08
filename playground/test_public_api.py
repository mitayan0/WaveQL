import waveql
import json

def test_json_placeholder():
    print("--- Connecting to JSONPlaceholder API ---")
    
    # Configure endpoints to match JSONPlaceholder's pagination style
    # They use _limit and _start instead of limit/offset
    endpoints = {
        "posts": {
            "path": "/posts",
            "limit_param": "_limit",
            "offset_param": "_start",
            "supports_filter": True,
            "filter_format": "query"
        },
        "users": {
            "path": "/users",
            # Users endpoint returns all users (only 10), so standard pagination might not apply perfectly 
            # but we can still configure it just in case.
            "limit_param": "_limit", 
            "offset_param": "_start"
        },
        "comments": {
            "path": "/comments",
            "limit_param": "_limit",
            "offset_param": "_start",
            "supports_filter": True
        }
    }

    # Initialize connection
    # Note: No auth needed for this public API
    conn = waveql.connect(
        adapter="rest",
        host="https://jsonplaceholder.typicode.com",
        endpoints=endpoints
    )
    
    cursor = conn.cursor()
    
    # 1. Test Listing Tables (virtual, based on registered endpoints if we had a way to list them, 
    # but RESTAdapter.list_tables isn't implemented to crawl the API. 
    # The default base implementation might return empty or error if not overridden.
    # Let's check base usage first)
    
    # 2. Test Fetching Users (Schema Inference)
    print("\n[Query 1] Fetching Users (Schema Inference)...")
    try:
        cursor.execute("SELECT id, name, email, company FROM users LIMIT 3")
        users = cursor.fetchall()
        print("Columns:", [d[0] for d in cursor.description])
        for row in users:
            print(f"- {row.id}: {row.name} ({row.email}) - {row.company}")
            # Note: 'company' is a struct, WaveQL should handle it
    except Exception as e:
        print(f"Error fetching users: {e}")

    # 3. Test Filtering (Predicate Pushdown)
    print("\n[Query 2] Fetching Posts for User ID 1 (Predicate Pushdown)...")
    try:
        # JSONPlaceholder supports ?userId=1
        cursor.execute("SELECT id, title FROM posts WHERE userId = 1 LIMIT 5")
        posts = cursor.fetchall()
        for row in posts:
            print(f"- Post {row.id}: {row.title[:40]}...")
            
        # Verify it actually filtered
        print(f"Fetched {len(posts)} posts (Limit 5)")
    except Exception as e:
        print(f"Error fetching posts: {e}")

    # 4. Test Cross-Endpoint Join (Virtual Join)
    print("\n[Query 3] Joining Users and Posts (Virtual Join)...")
    try:
        # Join users and posts on userId
        query = """
        SELECT 
            u.name as user_name, 
            p.title as post_title 
        FROM users u
        JOIN posts p ON u.id = p.userId
        WHERE u.id = 2
        LIMIT 3
        """
        cursor.execute(query)
        joined = cursor.fetchall()
        for row in joined:
            print(f"- {row.user_name} wrote: {row.post_title[:40]}...")
    except Exception as e:
        print(f"Error executing join: {e}")

    print("\n--- Test Complete ---")

if __name__ == "__main__":
    test_json_placeholder()
