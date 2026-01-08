#!/usr/bin/env python
"""
WaveQL Jira Example

Demonstrates querying Jira using WaveQL with JQL predicate pushdown.
This example uses a mock adapter so it runs without credentials.

To use with real Jira:
    Replace MockJiraAdapter with actual credentials:
    conn = connect(
        "jira://your-domain.atlassian.net",
        username="your-email@example.com",
        password="your-api-token",
    )
"""

import pyarrow as pa
from waveql import connect
from waveql.adapters import BaseAdapter, register_adapter


class MockJiraAdapter(BaseAdapter):
    """Mock Jira adapter for demo purposes."""
    
    adapter_name = "mock_jira"
    
    def _get_table_schema(self, table_name):
        return {}
    
    def get_schema(self, table):
        return []
    
    def fetch(self, table, columns=None, predicates=None, limit=None, offset=None,
              order_by=None, group_by=None, aggregates=None):
        """Return mock Jira data."""
        
        if table == "issues":
            data = {
                "key": ["PROJ-101", "PROJ-102", "PROJ-103", "PROJ-104", "PROJ-105",
                       "PROJ-106", "PROJ-107", "PROJ-108", "PROJ-109", "PROJ-110"],
                "summary": [
                    "Implement user authentication flow",
                    "Fix database connection pooling issue",
                    "Add export to CSV functionality",
                    "Upgrade React to version 18",
                    "Performance optimization for search",
                    "Mobile responsive design fixes",
                    "API rate limiting implementation",
                    "Add dark mode support",
                    "Fix memory leak in worker process",
                    "Integrate with Slack notifications",
                ],
                "status": ["Open", "In Progress", "Open", "Done", "Open",
                          "In Progress", "Open", "Done", "Open", "In Progress"],
                "priority": ["High", "Critical", "Medium", "Low", "High",
                            "Medium", "High", "Low", "Critical", "Medium"],
                "assignee": ["alice@example.com", "bob@example.com", None, 
                            "charlie@example.com", "alice@example.com",
                            "bob@example.com", None, "diana@example.com",
                            "eve@example.com", "alice@example.com"],
                "issuetype": ["Story", "Bug", "Story", "Task", "Story",
                             "Bug", "Story", "Task", "Bug", "Story"],
                "created": ["2024-01-15", "2024-01-14", "2024-01-13", "2024-01-12",
                           "2024-01-11", "2024-01-10", "2024-01-09", "2024-01-08",
                           "2024-01-07", "2024-01-06"],
            }
            
            # Apply filters
            if predicates:
                indices = list(range(len(data["key"])))
                for p in predicates:
                    if p.column == "status" and p.operator == "=":
                        indices = [i for i in indices if data["status"][i] == p.value]
                    elif p.column == "status" and p.operator == "!=":
                        indices = [i for i in indices if data["status"][i] != p.value]
                    elif p.column == "issuetype" and p.operator == "=":
                        indices = [i for i in indices if data["issuetype"][i] == p.value]
                
                data = {k: [v[i] for i in indices] for k, v in data.items()}
            
            # Apply limit
            if limit:
                data = {k: v[:limit] for k, v in data.items()}
            
            return pa.Table.from_pydict(data)
        
        elif table == "projects":
            return pa.Table.from_pydict({
                "key": ["PROJ", "CORE", "INFRA", "UI", "API"],
                "name": ["Main Project", "Core Services", "Infrastructure", 
                        "User Interface", "API Platform"],
            })
        
        return pa.Table.from_pydict({})


def main():
    print("WaveQL - Jira Example (Mock Data)")
    print("=" * 50)
    
    # Register mock adapter
    register_adapter("mock_jira", MockJiraAdapter)
    
    # Connect using mock adapter
    conn = connect(adapter="mock_jira")
    cursor = conn.cursor()
    
    # Example 1: Query open issues in a project
    print("\n1. Open Issues:")
    print("-" * 40)
    cursor.execute("""
        SELECT key, summary, status, priority, assignee
        FROM issues
        WHERE status = 'Open'
        ORDER BY priority DESC
        LIMIT 5
    """)
    
    for row in cursor:
        assignee = row['assignee'] or 'Unassigned'
        summary = row['summary'][:35] + "..." if len(row['summary']) > 35 else row['summary']
        print(f"  {row['key']}: {summary} ({assignee})")
    
    # Example 2: Search for bugs
    print("\n2. Bug Issues (not Done):")
    print("-" * 40)
    cursor.execute("""
        SELECT key, summary, created
        FROM issues
        WHERE issuetype = 'Bug' AND status != 'Done'
        LIMIT 5
    """)
    
    for row in cursor:
        summary = row['summary'][:45] + "..." if len(row['summary']) > 45 else row['summary']
        print(f"  {row['key']}: {summary}")
    
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
    print(df.to_string(index=False))
    
    conn.close()
    print("\n" + "=" * 50)
    print("Demo complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
