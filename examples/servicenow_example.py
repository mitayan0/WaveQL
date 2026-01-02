#!/usr/bin/env python
"""
WaveQL ServiceNow Example

Demonstrates querying ServiceNow using WaveQL.

Prerequisites:
    - ServiceNow instance with REST API access
    - User credentials or API token
"""

from waveql import connect

# Replace with your ServiceNow instance details
INSTANCE = "your-instance.service-now.com"
USERNAME = "admin"
PASSWORD = "your-password"  # Or API token


def main():
    print("WaveQL - ServiceNow Example")
    print("=" * 50)
    
    # Connect to ServiceNow
    conn = connect(
        f"servicenow://{INSTANCE}",
        username=USERNAME,
        password=PASSWORD,
    )
    cursor = conn.cursor()
    
    # Example 1: Query incidents
    print("\n1. Recent Incidents:")
    print("-" * 40)
    cursor.execute("""
        SELECT number, short_description, priority, state
        FROM incident
        WHERE state = 1
        ORDER BY sys_created_on DESC
        LIMIT 10
    """)
    
    for row in cursor:
        print(f"  {row['number']}: {row['short_description'][:50]}...")
    
    # Example 2: Aggregation (uses Stats API)
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
    print(df)
    
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
