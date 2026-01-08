#!/usr/bin/env python
"""
WaveQL AI Functions Demo

Demonstrates Vector Search and Embeddings capabilities:
1. Generate embeddings with EMBED() (mock provider for demo)
2. Store vectors in DuckDB
3. Perform similarity search with vector_search()

This example uses a mock embedding provider so it runs without API credentials.

To use with real embeddings:
    ai = register_ai_functions(conn, provider="openai", api_key="sk-...")
"""

import waveql
from waveql import register_ai_functions


def main():
    print("=" * 60)
    print("WaveQL AI Functions Demo (Vector Search & Embeddings)")
    print("=" * 60)
    print()
    
    # Create connection
    conn = waveql.connect()
    cursor = conn.cursor()
    
    # Register AI functions with mock provider (no API key needed)
    ai = register_ai_functions(conn, provider="mock", dimensions=8)
    
    print("1. Creating document table with embeddings...")
    print("-" * 40)
    
    # Create a simple documents table
    cursor.execute("""
        CREATE OR REPLACE TABLE documents (
            id INTEGER PRIMARY KEY,
            title VARCHAR,
            content VARCHAR,
            embedding FLOAT[8]
        )
    """)
    
    # Sample documents
    documents = [
        (1, "Python Tutorial", "Learn Python programming basics"),
        (2, "Machine Learning Guide", "Introduction to ML algorithms"),
        (3, "Database Design", "SQL and relational database concepts"),
        (4, "Web Development", "Building modern web applications"),
        (5, "Data Science", "Statistical analysis and visualization"),
        (6, "AI and Neural Networks", "Deep learning fundamentals"),
        (7, "Cloud Computing", "AWS and Azure infrastructure"),
        (8, "DevOps Practices", "CI/CD and deployment automation"),
    ]
    
    # Generate embeddings and insert
    for doc_id, title, content in documents:
        # Generate embedding from content
        embedding = ai.embed(content)
        
        # Format as DuckDB array literal
        vec_str = "[" + ",".join(str(v) for v in embedding) + "]"
        
        cursor.execute(f"""
            INSERT INTO documents (id, title, content, embedding)
            VALUES ({doc_id}, '{title}', '{content}', {vec_str}::FLOAT[8])
        """)
    
    print(f"  Inserted {len(documents)} documents with embeddings")
    print()
    
    # Show the documents
    print("2. Documents in table:")
    print("-" * 40)
    cursor.execute("SELECT id, title FROM documents")
    for row in cursor:
        print(f"  {row['id']}: {row['title']}")
    print()
    
    # Perform vector search
    print("3. Vector Similarity Search")
    print("-" * 40)
    
    query = "machine learning and AI"
    print(f"  Query: '{query}'")
    print()
    
    # Generate query embedding
    query_embedding = ai.embed(query)
    
    # Perform search
    results = ai.vector_search(
        table="documents",
        query_vector=query_embedding,
        k=3,
        vector_column="embedding",
        distance_metric="l2",
    )
    
    print("  Top 3 similar documents:")
    for i, row in enumerate(results.to_pydict()['title']):
        distance = results.to_pydict()['_distance'][i]
        print(f"    {i+1}. {row} (distance: {distance:.4f})")
    print()
    
    # Demo: Direct SQL with array_distance
    print("4. Direct SQL Vector Query")
    print("-" * 40)
    
    # We can also query directly using DuckDB's array_distance
    vec_sql = "[" + ",".join(str(v) for v in query_embedding) + "]"
    cursor.execute(f"""
        SELECT title, 
               array_distance(embedding, {vec_sql}::FLOAT[8]) as distance
        FROM documents
        ORDER BY distance ASC
        LIMIT 5
    """)
    
    print("  Top 5 by SQL query:")
    for row in cursor:
        print(f"    - {row['title']}: {row['distance']:.4f}")
    print()
    
    # Clean up
    cursor.execute("DROP TABLE documents")
    conn.close()
    
    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)
    print()
    print("To use with real embeddings (OpenAI):")
    print("  ai = register_ai_functions(conn, provider='openai', api_key='sk-...')")
    print()
    print("To use with Ollama (local):")
    print("  ai = register_ai_functions(conn, provider='ollama', model='nomic-embed-text')")


if __name__ == "__main__":
    main()
