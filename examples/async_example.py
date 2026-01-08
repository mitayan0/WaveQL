#!/usr/bin/env python
"""
WaveQL Async Example

Demonstrates async/await support for non-blocking queries.
Also shows caching works identically with async connections.

This example uses mock adapters so it runs without credentials.
"""

import asyncio
import pyarrow as pa
from waveql import connect
from waveql.adapters import BaseAdapter, register_adapter


class MockServiceNowAdapter(BaseAdapter):
    """Mock ServiceNow adapter for async demo."""
    
    adapter_name = "mock_sn_async"
    
    def _get_table_schema(self, table_name):
        return {}
    
    def get_schema(self, table):
        return []
    
    def fetch(self, table, columns=None, predicates=None, limit=None, offset=None,
              order_by=None, group_by=None, aggregates=None):
        if table == "incident":
            data = {
                "number": ["INC0001", "INC0002", "INC0003", "INC0004", "INC0005"],
                "short_description": [
                    "Server outage in datacenter",
                    "Email delivery delayed",
                    "VPN connection issues",
                    "Application error on login",
                    "Network latency problems",
                ],
            }
            if limit:
                data = {k: v[:limit] for k, v in data.items()}
            return pa.Table.from_pydict(data)
        return pa.Table.from_pydict({})


class MockJiraAdapter(BaseAdapter):
    """Mock Jira adapter for async demo."""
    
    adapter_name = "mock_jira_async"
    
    def _get_table_schema(self, table_name):
        return {}
    
    def get_schema(self, table):
        return []
    
    def fetch(self, table, columns=None, predicates=None, limit=None, offset=None,
              order_by=None, group_by=None, aggregates=None):
        if table == "issues":
            data = {
                "key": ["PROJ-101", "PROJ-102", "PROJ-103", "PROJ-104", "PROJ-105"],
                "summary": [
                    "Implement OAuth flow",
                    "Fix memory leak",
                    "Add dark mode",
                    "Upgrade dependencies",
                    "Performance tuning",
                ],
            }
            if limit:
                data = {k: v[:limit] for k, v in data.items()}
            return pa.Table.from_pydict(data)
        return pa.Table.from_pydict({})


def query_servicenow():
    """Query ServiceNow with caching (sync version for demo)."""
    conn = connect(
        adapter="mock_sn_async",
        cache_ttl=60,  # 60 second cache TTL
    )
    
    cursor = conn.cursor()
    
    # First query - fetches from "API"
    cursor.execute("""
        SELECT number, short_description 
        FROM incident 
        LIMIT 5
    """)
    
    results = cursor.fetchall()
    print("ServiceNow Results (first query):")
    for row in results:
        print(f"  {row}")
    
    # Second query - served from cache
    cursor.execute("""
        SELECT number, short_description 
        FROM incident 
        LIMIT 5
    """)
    
    # Show cache stats
    stats = conn.cache_stats
    print(f"Cache: {stats.hits} hits, {stats.misses} misses ({stats.hit_rate:.0f}% hit rate)")
    
    conn.close()


def query_jira():
    """Query Jira with caching (sync version for demo)."""
    conn = connect(
        adapter="mock_jira_async",
        cache_ttl=120,  # 2 minute cache for Jira
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT key, summary 
        FROM issues 
        LIMIT 5
    """)
    
    results = cursor.fetchall()
    print("Jira Results:")
    for row in results:
        print(f"  {row}")
    
    conn.close()


async def concurrent_queries():
    """
    Simulate concurrent queries.
    
    Note: In a real async scenario with actual adapters that support
    async I/O, you would use connect_async() and await cursor.execute().
    This demo uses sync adapters wrapped in asyncio.to_thread().
    """
    print("Simulating concurrent queries...")
    print("=" * 50)
    
    # Run both queries "concurrently" using thread pool
    loop = asyncio.get_event_loop()
    await asyncio.gather(
        loop.run_in_executor(None, query_servicenow),
        loop.run_in_executor(None, query_jira),
    )
    
    print("\nAll done!")


def main():
    print("WaveQL - Async Example (Mock Data)")
    print("=" * 50)
    print()
    
    # Register mock adapters
    register_adapter("mock_sn_async", MockServiceNowAdapter)
    register_adapter("mock_jira_async", MockJiraAdapter)
    
    # Run the async demo
    asyncio.run(concurrent_queries())
    
    print()
    print("=" * 50)
    print("Demo complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
