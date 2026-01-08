import waveql
import json

def test_json_placeholder_full():
    print("--- Connecting to JSONPlaceholder API (Full Feature Test) ---")
    
    # Configure endpoints
    endpoints = {
        "posts": {
            "path": "/posts",
            "limit_param": "_limit",
            "offset_param": "_start",
            "supports_filter": True,
            "filter_format": "query"
        },
        # Used for client-side filtering test
        "comments": {
            "path": "/comments",
            "supports_filter": False # Disable pushdown explicitly to test local filtering
        }
    }

    conn = waveql.connect(
        adapter="rest",
        host="https://jsonplaceholder.typicode.com",
        endpoints=endpoints
    )
    
    cursor = conn.cursor()

    # --- 1. Fetch & Pushdown (READ) ---
    print("\n[READ] Fetching Posts for User 1...")
    cursor.execute("SELECT id, title FROM posts WHERE userId = 1 LIMIT 3")
    for row in cursor.fetchall():
        print(f"Post {row.id}: {row.title[:20]}...")

    # --- 2. Client-Side Filtering ---
    # We disabled pushdown for 'comments', so this WHERE clause runs in Python
    print("\n[READ CS] Client-Side Filtering (Comments)...")
    # Fetch comments where id=5 (should be 1 record). 
    # The API fetches all (or default page), WaveQL filters locally.
    cursor.execute("SELECT id, name, email FROM comments WHERE id = 5")
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} comments (Expected 1)")
    if rows:
        print(f"Comment: {rows[0].name} ({rows[0].email})")

    # --- 3. CREATE (INSERT) ---
    print("\n[CREATE] Inserting new post...")
    try:
        cursor.execute(
            "INSERT INTO posts (title, body, userId) VALUES (?, ?, ?)", 
            ("My New Post", "Content here", 1)
        )
        print("Insert successful (JSONPlaceholder fakes this, returns 201)")
    except Exception as e:
        print(f"INSERT failed: {e}")

    # --- 4. UPDATE ---
    print("\n[UPDATE] Updating post 1...")
    try:
        # UPDATE requires WHERE id = X for REST adapter
        cursor.execute(
            "UPDATE posts SET title = ? WHERE id = 1", 
            ("Updated Title",)
        )
        print("Update successful (JSONPlaceholder fakes this, returns 200)")
    except Exception as e:
        print(f"UPDATE failed: {e}")

    # --- 5. DELETE ---
    print("\n[DELETE] Deleting post 1...")
    try:
        cursor.execute("DELETE FROM posts WHERE id = 1")
        print("Delete successful (JSONPlaceholder fakes this, returns 200)")
    except Exception as e:
        print(f"DELETE failed: {e}")

    print("\n--- Test Complete ---")

if __name__ == "__main__":
    test_json_placeholder_full()
