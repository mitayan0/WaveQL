#!/usr/bin/env python
"""
WaveQL Jira Example

Demonstrates querying Jira using WaveQL with JQL predicate pushdown.

Prerequisites:
    - Jira Cloud account
    - API token from https://id.atlassian.com/manage/api-tokens
"""

from waveql import connect

# Replace with your Jira details
JIRA_DOMAIN = "your-domain.atlassian.net"
EMAIL = "your-email@example.com"
API_TOKEN = "your-api-token"


def main():
    print("WaveQL - Jira Example")
    print("=" * 50)
    
    # Connect to Jira
    conn = connect(
        f"jira://{JIRA_DOMAIN}",
        username=EMAIL,
        password=API_TOKEN,
    )
    cursor = conn.cursor()
    
    # Example 1: Query open issues in a project
    print("\n1. Open Issues in Project:")
    print("-" * 40)
    cursor.execute("""
        SELECT key, summary, status, priority, assignee
        FROM issues
        WHERE project = 'YOUR_PROJECT' AND status = 'Open'
        ORDER BY priority DESC
        LIMIT 10
    """)
    
    for row in cursor:
        assignee = row.get('assignee', 'Unassigned') or 'Unassigned'
        print(f"  {row['key']}: {row['summary'][:40]}... ({assignee})")
    
    # Example 2: Search for bugs
    print("\n2. Bug Issues:")
    print("-" * 40)
    cursor.execute("""
        SELECT key, summary, created
        FROM issues
        WHERE issuetype = 'Bug' AND status != 'Done'
        LIMIT 5
    """)
    
    for row in cursor:
        print(f"  {row['key']}: {row['summary'][:50]}...")
    
    # Example 3: List all projects
    print("\n3. Available Projects:")
    print("-" * 40)
    cursor.execute("SELECT key, name FROM projects LIMIT 10")
    
    for row in cursor:
        print(f"  {row['key']}: {row['name']}")
    
    # Example 4: Export to Pandas
    print("\n4. Export to Pandas:")
    print("-" * 40)
    cursor.execute("SELECT key, summary, status FROM issues LIMIT 5")
    df = cursor.to_df()
    print(df)
    
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
