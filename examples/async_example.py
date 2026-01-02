#!/usr/bin/env python
"""
WaveQL Async Example

Demonstrates async/await support for non-blocking queries.
"""

import asyncio
from waveql import connect_async


async def query_servicenow():
    """Query ServiceNow asynchronously."""
    conn = await connect_async(
        "servicenow://your-instance.service-now.com",
        username="admin",
        password="your-password",
    )
    
    cursor = conn.cursor()
    await cursor.execute("""
        SELECT number, short_description 
        FROM incident 
        LIMIT 5
    """)
    
    results = await cursor.fetchall()
    print("ServiceNow Results:")
    for row in results.to_pylist():
        print(f"  {row}")
    
    await conn.close()


async def query_jira():
    """Query Jira asynchronously."""
    conn = await connect_async(
        "jira://your-domain.atlassian.net",
        username="email@example.com",
        password="api-token",
    )
    
    cursor = conn.cursor()
    await cursor.execute("""
        SELECT key, summary 
        FROM issues 
        WHERE project = 'PROJ'
        LIMIT 5
    """)
    
    results = await cursor.fetchall()
    print("Jira Results:")
    for row in results.to_pylist():
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
