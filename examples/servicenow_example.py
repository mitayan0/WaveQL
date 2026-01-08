#!/usr/bin/env python
"""
WaveQL ServiceNow Example

Demonstrates querying ServiceNow using WaveQL.
This example uses a mock adapter so it runs without credentials.

To use with real ServiceNow:
    Replace MockServiceNowAdapter with actual credentials:
    conn = connect(
        "servicenow://your-instance.service-now.com",
        username="admin",
        password="your-password",
    )
"""

import pyarrow as pa
from waveql import connect
from waveql.adapters import BaseAdapter, register_adapter


class MockServiceNowAdapter(BaseAdapter):
    """Mock ServiceNow adapter for demo purposes."""
    
    adapter_name = "mock_servicenow"
    
    def _get_table_schema(self, table_name):
        return {}
    
    def get_schema(self, table):
        return []
    
    def fetch(self, table, columns=None, predicates=None, limit=None, offset=None,
              order_by=None, group_by=None, aggregates=None):
        """Return mock ServiceNow data."""
        
        if table == "incident":
            # Mock incident data
            data = {
                "number": ["INC0001234", "INC0001235", "INC0001236", "INC0001237", "INC0001238",
                          "INC0001239", "INC0001240", "INC0001241", "INC0001242", "INC0001243"],
                "short_description": [
                    "Server not responding to ping requests",
                    "Email service intermittent failures",
                    "Database connection timeout errors",
                    "VPN disconnects randomly for users",
                    "Application crashes on login attempt",
                    "Network latency issues in building B",
                    "Printer queue stuck for department",
                    "Password reset not working properly",
                    "Software license expiration warning",
                    "Backup job failed last night",
                ],
                "priority": [1, 2, 1, 3, 2, 4, 5, 3, 4, 2],
                "state": [1, 1, 2, 1, 3, 1, 1, 2, 1, 1],
                "sys_created_on": [
                    "2024-01-15 10:30:00", "2024-01-15 09:15:00", "2024-01-14 16:45:00",
                    "2024-01-14 14:20:00", "2024-01-14 11:00:00", "2024-01-13 17:30:00",
                    "2024-01-13 15:00:00", "2024-01-13 12:00:00", "2024-01-12 09:00:00",
                    "2024-01-12 08:00:00",
                ],
            }
            
            # Handle aggregation
            if group_by and aggregates:
                # Mock GROUP BY priority with COUNT(*)
                return pa.Table.from_pydict({
                    "priority": [1, 2, 3, 4, 5],
                    "count": [2, 3, 2, 2, 1],
                })
            
            # Apply state filter if present
            if predicates:
                for p in predicates:
                    if p.column == "state" and p.operator == "=" and p.value == 1:
                        # Filter to state=1 only
                        indices = [i for i, s in enumerate(data["state"]) if s == 1]
                        data = {k: [v[i] for i in indices] for k, v in data.items()}
                        break
            
            # Apply limit
            if limit:
                data = {k: v[:limit] for k, v in data.items()}
            
            return pa.Table.from_pydict(data)
        
        return pa.Table.from_pydict({})


def main():
    print("WaveQL - ServiceNow Example (Mock Data)")
    print("=" * 50)
    
    # Register mock adapter
    register_adapter("mock_servicenow", MockServiceNowAdapter)
    
    # Connect using mock adapter (no real credentials needed!)
    conn = connect(
        adapter="mock_servicenow",
        cache_ttl=60,  # Cache results for 60 seconds
    )
    cursor = conn.cursor()
    
    # Example 1: Query incidents
    print("\n1. Recent Incidents (state=1):")
    print("-" * 40)
    cursor.execute("""
        SELECT number, short_description, priority, state
        FROM incident
        WHERE state = 1
        ORDER BY sys_created_on DESC
        LIMIT 5
    """)
    
    for row in cursor:
        desc = row['short_description'][:45] + "..." if len(row['short_description']) > 45 else row['short_description']
        print(f"  {row['number']}: {desc}")
    
    # Example 2: Aggregation (uses Stats API in real ServiceNow)
    print("\n2. Incident Count by Priority:")
    print("-" * 40)
    cursor.execute("""
        SELECT priority, COUNT(*) as count
        FROM incident
        GROUP BY priority
    """)
    
    for row in cursor:
        print(f"  Priority {row['priority']}: {row['count']} incidents")
    
    # Example 3: Convert to Pandas
    print("\n3. Export to Pandas DataFrame:")
    print("-" * 40)
    cursor.execute("SELECT number, short_description FROM incident LIMIT 5")
    df = cursor.to_df()
    print(df.to_string(index=False))
    
    # Example 4: Demonstrate caching
    print("\n4. Cache Statistics:")
    print("-" * 40)
    # Repeat a query - will be served from cache
    cursor.execute("""
        SELECT number, short_description, priority, state
        FROM incident
        WHERE state = 1
        LIMIT 5
    """)
    
    stats = conn.cache_stats
    print(f"  Cache hits: {stats.hits}")
    print(f"  Cache misses: {stats.misses}")
    print(f"  Hit rate: {stats.hit_rate:.1f}%")
    print(f"  Cached entries: {stats.entries}")
    
    conn.close()
    print("\n" + "=" * 50)
    print("Demo complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
