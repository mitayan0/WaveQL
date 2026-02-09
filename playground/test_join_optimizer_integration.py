"""
Integration Test: Join Optimizer with Real APIs

This test validates the JoinOptimizer using real API data from:
- ServiceNow
- HubSpot
- Stripe
- Zendesk
- Salesforce
- Jira

Run:
    python playground/test_join_optimizer_integration.py
"""

import os
import sys
import time
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

import waveql
from waveql.join_optimizer import get_join_optimizer
from waveql.optimizer import QueryOptimizer


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print('='*70)


def print_stats():
    """Print current optimizer statistics in a table."""
    optimizer = get_join_optimizer()
    stats = optimizer.get_all_stats()
    
    if not stats:
        print("  No statistics collected yet.")
        return
    
    print(f"\n  {'Table':<40} {'Latency/Row':<15} {'Avg Rows':<12} {'Execs':<8}")
    print(f"  {'-'*40} {'-'*15} {'-'*12} {'-'*8}")
    
    # Sort by latency (slowest first)
    sorted_stats = sorted(stats.items(), key=lambda x: x[1]['avg_latency_per_row'], reverse=True)
    
    for table, s in sorted_stats:
        latency_ms = s['avg_latency_per_row'] * 1000
        print(f"  {table:<40} {latency_ms:>10.2f} ms   {s['avg_row_count']:>10.0f}   {s['execution_count']:>6}")


def test_adapter(name: str, conn, queries: list) -> bool:
    """Execute queries against an adapter and track statistics."""
    print(f"\n  Testing {name}...")
    cursor = conn.cursor()
    success = True
    
    for sql in queries:
        try:
            start = time.perf_counter()
            cursor.execute(sql)
            rows = cursor.fetchall()
            duration = time.perf_counter() - start
            print(f"    [{duration*1000:>6.0f}ms] {len(rows):>4} rows <- {sql[:60]}...")
        except Exception as e:
            print(f"    [ERROR] {sql[:50]}... -> {e}")
            success = False
    
    return success


# =============================================================================
# ServiceNow Tests
# =============================================================================

def test_servicenow():
    """Test ServiceNow adapter with real data."""
    print_section("ServiceNow Integration")
    
    instance = os.environ.get("SN_INSTANCE")
    username = os.environ.get("SN_USERNAME")
    password = os.environ.get("SN_PASSWORD")
    
    if not all([instance, username, password]):
        print("  [SKIP] ServiceNow credentials not configured")
        return None
    
    conn = waveql.connect(
        f"servicenow://{instance}",
        username=username,
        password=password
    )
    
    queries = [
        "SELECT sys_id, number, short_description, priority FROM incident LIMIT 20",
        "SELECT sys_id, user_name, email, active FROM sys_user LIMIT 20",
        "SELECT sys_id, name, sys_class_name FROM cmdb_ci LIMIT 20",
        "SELECT sys_id, number, short_description FROM change_request LIMIT 20",
    ]
    
    test_adapter("ServiceNow", conn, queries)
    return conn


# =============================================================================
# HubSpot Tests
# =============================================================================

def test_hubspot():
    """Test HubSpot adapter with real data."""
    print_section("HubSpot Integration")
    
    api_key = os.environ.get("HUBSPOT_API_KEY")
    
    if not api_key:
        print("  [SKIP] HubSpot API key not configured")
        return None
    
    conn = waveql.connect(
        "hubspot://api.hubapi.com",
        api_key=api_key
    )
    
    queries = [
        "SELECT id, properties FROM contacts LIMIT 20",
        "SELECT id, properties FROM companies LIMIT 20",
        "SELECT id, properties FROM deals LIMIT 20",
    ]
    
    test_adapter("HubSpot", conn, queries)
    return conn


# =============================================================================
# Stripe Tests
# =============================================================================

def test_stripe():
    """Test Stripe adapter with real data."""
    print_section("Stripe Integration")
    
    api_key = os.environ.get("STRIPE_API_KEY")
    
    if not api_key:
        print("  [SKIP] Stripe API key not configured")
        return None
    
    conn = waveql.connect(
        "stripe://api.stripe.com",
        api_key=api_key
    )
    
    queries = [
        "SELECT id, email, name FROM customers LIMIT 20",
        "SELECT id, amount, currency, status FROM charges LIMIT 20",
        "SELECT id, amount, currency FROM payment_intents LIMIT 20",
    ]
    
    test_adapter("Stripe", conn, queries)
    return conn


# =============================================================================
# Zendesk Tests
# =============================================================================

def test_zendesk():
    """Test Zendesk adapter with real data."""
    print_section("Zendesk Integration")
    
    subdomain = os.environ.get("ZENDESK_SUBDOMAIN")
    email = os.environ.get("ZENDESK_EMAIL")
    token = os.environ.get("ZENDESK_API_TOKEN")
    
    if not all([subdomain, email, token]):
        print("  [SKIP] Zendesk credentials not configured")
        return None
    
    conn = waveql.connect(
        f"zendesk://{subdomain}.zendesk.com",
        username=email,
        api_key=token
    )
    
    queries = [
        "SELECT id, subject, status, priority FROM tickets LIMIT 20",
        "SELECT id, name, email FROM users LIMIT 20",
        "SELECT id, name FROM groups LIMIT 20",
    ]
    
    test_adapter("Zendesk", conn, queries)
    return conn


# =============================================================================
# Salesforce Tests
# =============================================================================

def test_salesforce():
    """Test Salesforce adapter with real data."""
    print_section("Salesforce Integration")
    
    host = os.environ.get("SF_HOST")
    username = os.environ.get("SF_USERNAME")
    password = os.environ.get("SF_PASSWORD")
    token = os.environ.get("SF_SECURITY_TOKEN")
    
    if not all([host, username, password]):
        print("  [SKIP] Salesforce credentials not configured")
        return None
    
    conn = waveql.connect(
        f"salesforce://{host.replace('https://', '')}",
        username=username,
        password=password + (token or "")
    )
    
    queries = [
        "SELECT Id, Name, Email FROM Contact LIMIT 20",
        "SELECT Id, Name, Industry FROM Account LIMIT 20",
        "SELECT Id, Name, StageName FROM Opportunity LIMIT 20",
    ]
    
    test_adapter("Salesforce", conn, queries)
    return conn


# =============================================================================
# Jira Tests
# =============================================================================

def test_jira():
    """Test Jira adapter with real data."""
    print_section("Jira Integration")
    
    host = os.environ.get("JIRA_HOST")
    email = os.environ.get("JIRA_EMAIL")
    token = os.environ.get("JIRA_API_TOKEN")
    
    if not all([host, email, token]):
        print("  [SKIP] Jira credentials not configured")
        return None
    
    conn = waveql.connect(
        f"jira://{host}",
        username=email,
        api_key=token
    )
    
    queries = [
        "SELECT key, summary, status FROM issues LIMIT 20",
        "SELECT key, name FROM projects LIMIT 20",
    ]
    
    test_adapter("Jira", conn, queries)
    return conn


# =============================================================================
# Join Reordering Analysis
# =============================================================================

def analyze_join_ordering():
    """Analyze the collected statistics and show recommended join orders."""
    print_section("Join Reordering Analysis")
    
    optimizer = get_join_optimizer()
    stats = optimizer.get_all_stats()
    
    if len(stats) < 2:
        print("  Need at least 2 tables with statistics to analyze join ordering.")
        return
    
    # Get all table names
    tables = list(stats.keys())
    
    print(f"\n  Collected stats for {len(tables)} tables:")
    print_stats()
    
    # Simulate a cross-adapter join reordering
    print("\n  Simulated join reordering:")
    
    # Create a mock connection that returns None for adapters
    # (the optimizer will use cached stats)
    from unittest.mock import MagicMock
    mock_conn = MagicMock()
    mock_conn.get_adapter.return_value = None
    
    query_optimizer = QueryOptimizer()
    
    # Test with all tables
    if len(tables) >= 2:
        test_tables = tables[:4]  # Test with up to 4 tables
        ordered = query_optimizer.reorder_joins(test_tables, {}, mock_conn)
        
        print(f"\n    Input:  {' -> '.join(test_tables)}")
        print(f"    Output: {' -> '.join(ordered)}")
        
        # Show reasoning
        print("\n  Reasoning (sorted by cost, cheapest first):")
        for i, t in enumerate(ordered):
            if t in stats:
                s = stats[t]
                cost = s['avg_latency_per_row'] * s['avg_row_count']
                print(f"    {i+1}. {t}: {s['avg_latency_per_row']*1000:.2f}ms/row x {s['avg_row_count']:.0f} rows = {cost*1000:.1f}ms est.")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("""
+------------------------------------------------------------------------------+
|       Join Optimizer Integration Test - Real API Data                        |
+------------------------------------------------------------------------------+
|  This test executes real queries against multiple APIs to validate           |
|  the JoinOptimizer's latency tracking and reordering decisions.              |
+------------------------------------------------------------------------------+
    """)
    
    # Clear any existing stats
    get_join_optimizer().clear_stats()
    
    # Test each adapter
    connections = []
    
    try:
        # Run tests for all configured adapters
        if conn := test_servicenow():
            connections.append(("ServiceNow", conn))
        
        if conn := test_hubspot():
            connections.append(("HubSpot", conn))
        
        if conn := test_stripe():
            connections.append(("Stripe", conn))
        
        if conn := test_zendesk():
            connections.append(("Zendesk", conn))
        
        if conn := test_salesforce():
            connections.append(("Salesforce", conn))
        
        if conn := test_jira():
            connections.append(("Jira", conn))
        
        # Show collected statistics
        print_section("Collected Statistics (All Adapters)")
        print_stats()
        
        # Analyze join ordering
        analyze_join_ordering()
        
        # Summary
        print_section("Summary")
        print(f"\n  Tested {len(connections)} adapters with real API data")
        print(f"  Collected latency stats for {len(get_join_optimizer().get_all_stats())} tables")
        print("\n  The JoinOptimizer now knows the real performance characteristics")
        print("  of each table and will use this for optimal join ordering!")
        
    finally:
        # Close all connections
        for name, conn in connections:
            try:
                conn.close()
            except Exception:
                pass
    
    print("\n" + "="*70)
    print("  Test Complete!")
    print("="*70 + "\n")
