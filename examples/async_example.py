#!/usr/bin/env python
"""
WaveQL Async Example

Demonstrates async/await support for non-blocking queries.
Also shows caching works identically with async connections.
"""

import asyncio
from waveql import connect_async


async def query_servicenow():
    """Query ServiceNow asynchronously with caching."""
    conn = await connect_async(
        "servicenow://your-instance.service-now.com",
        username="admin",
        password="your-password",
        cache_ttl=60,  # 60 second cache TTL
    )
    
    cursor = await conn.cursor()
    
    # First query - fetches from API
    await cursor.execute("""
        SELECT number, short_description 
        FROM incident 
        LIMIT 5
    """)
    
    results = cursor.fetchall()
    print("ServiceNow Results (first query - API call):")
    for row in results:
        print(f"  {row}")
    
    # Second query - served from cache
    await cursor.execute("""
        SELECT number, short_description 
        FROM incident 
        LIMIT 5
    """)
    
    # Show cache stats
    stats = conn.cache_stats
    print(f"Cache: {stats.hits} hits, {stats.misses} misses ({stats.hit_rate:.0f}% hit rate)")
    
    await conn.close()


async def query_jira():
    """Query Jira asynchronously with caching."""
    conn = await connect_async(
        "jira://your-domain.atlassian.net",
        username="email@example.com",
        password="api-token",
        cache_ttl=120,  # 2 minute cache for Jira
    )
    
    cursor = await conn.cursor()
    await cursor.execute("""
        SELECT key, summary 
        FROM issues 
        WHERE project = 'PROJ'
        LIMIT 5
    """)
    
    results = cursor.fetchall()
    print("Jira Results:")
    for row in results:
        print(f"  {row}")
    
    await conn.close()


async def concurrent_queries():
    """Run multiple queries concurrently."""
    print("Running queries concurrently...")
    print("=" * 50)
    
    # Run both queries at the same time
    await asyncio.gather(
        query_servicenow(),
        query_jira(),
    )
    
    print("\nAll done!")


if __name__ == "__main__":
    asyncio.run(concurrent_queries())

