"""
Live Test: Phase 2 - Low-Resource Systems Engineering

Tests the three new features with real API connections:
1. Statistical Cardinality Estimator
2. Adaptive Pagination (AIMD)
3. Budget-Constrained Planning (WITH BUDGET)

Usage:
    python examples/test_resource_optimizer_live.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add waveql to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import waveql
from waveql import (
    get_cardinality_estimator,
    get_adaptive_pagination,
    get_budget_planner,
)

load_dotenv()


def test_servicenow_with_budget():
    """Test budget-constrained queries against ServiceNow."""
    print("\n" + "="*60)
    print("TEST 1: Budget-Constrained Query (ServiceNow)")
    print("="*60)
    
    instance = os.getenv("SN_INSTANCE")
    username = os.getenv("SN_USERNAME")
    password = os.getenv("SN_PASSWORD")
    
    if not all([instance, username, password]):
        print("[!] ServiceNow credentials not found. Skipping test.")
        return
    
    print(f"Connecting to: {instance}")
    
    conn = waveql.connect(
        f"servicenow://{instance}",
        username=username,
        password=password
    )
    cursor = conn.cursor()
    
    # Test 1a: Query WITH BUDGET (should limit execution time)
    print("\n[Query] SELECT * FROM incident WITH BUDGET 2s LIMIT 50")
    
    cursor.execute("SELECT * FROM incident WITH BUDGET 2s LIMIT 50")
    rows = cursor.fetchall()
    
    print(f"   Rows returned: {len(rows)}")
    if cursor.last_budget:
        print(f"   Budget: {cursor.last_budget.value} {cursor.last_budget.unit.value}")
        print(f"   Elapsed: {cursor.last_budget.get_elapsed_ms():.2f}ms")
        print(f"   Exhausted: {cursor.last_budget.is_exhausted}")
    
    # Test 1b: Check cardinality estimator learned from the query
    estimator = get_cardinality_estimator()
    stats = estimator.get_stats("servicenow", "incident")
    
    if stats:
        print("\n[Stats] Learned Statistics for servicenow.incident:")
        print(f"   Sample count: {stats.sample_count}")
        print(f"   Avg rows: {stats.avg_rows:.0f}")
        print(f"   Min/Max: {stats.min_rows} / {stats.max_rows}")
    
    # Test 1c: Estimate future query cardinality
    est_rows, lower, upper = estimator.estimate_cardinality("servicenow", "incident")
    print("\n[Estimate] Cardinality Estimate (no predicates):")
    print(f"   Estimated rows: {est_rows:.0f}")
    print(f"   Range: [{lower:.0f}, {upper:.0f}]")
    
    # Test 1d: Run another query to see learning
    print("\n[Query] SELECT * FROM incident WHERE priority = 1 LIMIT 10")
    cursor.execute("SELECT * FROM incident WHERE priority = 1 LIMIT 10")
    rows2 = cursor.fetchall()
    print(f"   Rows returned: {len(rows2)}")
    
    conn.close()
    print("\n[OK] ServiceNow budget test complete!")


def test_salesforce_adaptive_pagination():
    """Test adaptive pagination with Salesforce."""
    print("\n" + "="*60)
    print("TEST 2: Cardinality Estimation (Salesforce)")
    print("="*60)
    
    host = os.getenv("SF_HOST")
    username = os.getenv("SF_USERNAME")
    password = os.getenv("SF_PASSWORD")
    token = os.getenv("SF_SECURITY_TOKEN")
    
    if not all([host, username, password, token]):
        print("[!] Salesforce credentials not found. Skipping test.")
        return
    
    print(f"Connecting to: {host}")
    
    conn = waveql.connect(
        host,
        adapter="salesforce",
        username=username,
        password=password + token  # SF requires password+token
    )
    cursor = conn.cursor()
    
    # Run a few queries to build statistics
    print("\n[Query] Running queries to build statistics...")
    
    for i in range(3):
        cursor.execute("SELECT Id, Name FROM Account LIMIT 20")
        rows = cursor.fetchall()
        print(f"   Query {i+1}: {len(rows)} rows")
    
    # Check learned statistics
    estimator = get_cardinality_estimator()
    stats = estimator.get_stats("salesforce", "Account")
    
    if stats:
        print("\n[Stats] Learned Statistics for salesforce.Account:")
        print(f"   Sample count: {stats.sample_count}")
        print(f"   Avg rows: {stats.avg_rows:.0f}")
    
    # Check pagination state
    pagination = get_adaptive_pagination()
    state = pagination.get_state("salesforce", "Account")
    
    print("\n[Pagination] Adaptive Pagination State:")
    print(f"   Current page size: {state.page_size}")
    print(f"   State: {state.state.value}")
    print(f"   Avg throughput: {state.avg_throughput:.1f} rows/s")
    
    conn.close()
    print("\n[OK] Salesforce cardinality test complete!")


def test_budget_feasibility():
    """Test budget feasibility estimation."""
    print("\n" + "="*60)
    print("TEST 3: Budget Feasibility Estimation")
    print("="*60)
    
    # First, let's populate the estimator with some data
    estimator = get_cardinality_estimator()
    planner = get_budget_planner()
    
    # Simulate some executions to have data
    for i in range(5):
        estimator.record_execution("demo", "large_table", 10000)
    
    print("\n[Demo] large_table with ~10000 rows average")
    
    # Check feasibility of a tight budget
    from waveql.resource_optimizer import QueryBudget, BudgetUnit
    
    budget_tight = QueryBudget(value=100, unit=BudgetUnit.MILLISECONDS)
    result_tight = planner.estimate_feasibility(budget_tight, "demo", "large_table")
    
    print("\n[Budget] 100ms")
    print(f"   Is feasible: {result_tight['is_feasible']}")
    print(f"   Estimated cost: {result_tight['estimated_cost']*1000:.1f}ms")
    if result_tight.get('suggested_limit'):
        print(f"   Suggested LIMIT: {result_tight['suggested_limit']}")
    
    budget_loose = QueryBudget(value=5000, unit=BudgetUnit.MILLISECONDS)
    result_loose = planner.estimate_feasibility(budget_loose, "demo", "large_table")
    
    print("\n[Budget] 5000ms")
    print(f"   Is feasible: {result_loose['is_feasible']}")
    print(f"   Estimated cost: {result_loose['estimated_cost']*1000:.1f}ms")
    
    print("\n[OK] Feasibility estimation test complete!")


def test_jira_full_workflow():
    """Test full workflow with Jira."""
    print("\n" + "="*60)
    print("TEST 4: Full Workflow (Jira)")
    print("="*60)
    
    host = os.getenv("JIRA_HOST")
    email = os.getenv("JIRA_EMAIL")
    token = os.getenv("JIRA_API_TOKEN")
    
    if not all([host, email, token]):
        print("[!] Jira credentials not found. Skipping test.")
        return
    
    print(f"Connecting to: {host}")
    
    conn = waveql.connect(
        f"jira://{host}",
        username=email,
        api_key=token
    )
    cursor = conn.cursor()
    
    # Query with budget
    print("\n[Query] SELECT * FROM issues WITH BUDGET 3s LIMIT 20")
    
    cursor.execute("SELECT * FROM issues WITH BUDGET 3s LIMIT 20")
    rows = cursor.fetchall()
    
    print(f"   Rows returned: {len(rows)}")
    if cursor.last_budget:
        print(f"   Elapsed: {cursor.last_budget.get_elapsed_ms():.2f}ms")
    
    # Check all diagnostics
    from waveql import get_resource_executor
    executor = get_resource_executor()
    diagnostics = executor.get_diagnostics()
    
    print("\n[Diagnostics] Global Diagnostics:")
    print(f"   Tables with cardinality stats: {len(diagnostics['cardinality_stats'])}")
    print(f"   Tables with pagination state: {len(diagnostics['pagination_states'])}")
    
    for table, stats in diagnostics['cardinality_stats'].items():
        print(f"   - {table}: {stats['avg_rows']:.0f} avg rows, {stats['sample_count']} samples")
    
    conn.close()
    print("\n[OK] Jira workflow test complete!")


def show_summary():
    """Show summary of all collected statistics."""
    print("\n" + "="*60)
    print("SUMMARY: Global Statistics")
    print("="*60)
    
    estimator = get_cardinality_estimator()
    pagination = get_adaptive_pagination()
    
    card_stats = estimator.get_all_stats()
    page_stats = pagination.get_all_states()
    
    print("\n[Cardinality] Statistics:")
    if card_stats:
        for table, stats in card_stats.items():
            print(f"   {table}:")
            print(f"      Samples: {stats['sample_count']}, Avg: {stats['avg_rows']:.0f}")
            print(f"      Range: [{stats['min_rows']}, {stats['max_rows']}]")
    else:
        print("   No statistics collected yet.")
    
    print("\n[Pagination] States:")
    if page_stats:
        for key, state in page_stats.items():
            print(f"   {key}:")
            print(f"      Page size: {state['page_size']}, State: {state['state']}")
            print(f"      Throughput: {state['avg_throughput']:.1f} rows/s")
    else:
        print("   No pagination state yet.")


if __name__ == "__main__":
    print("[*] WaveQL Phase 2: Low-Resource Systems Engineering - Live Test")
    print("="*60)
    
    # Run tests
    try:
        test_servicenow_with_budget()
    except Exception as e:
        print(f"[X] ServiceNow test failed: {e}")
    
    try:
        test_salesforce_adaptive_pagination()
    except Exception as e:
        print(f"[X] Salesforce test failed: {e}")
    
    try:
        test_budget_feasibility()
    except Exception as e:
        print(f"[X] Feasibility test failed: {e}")
    
    try:
        test_jira_full_workflow()
    except Exception as e:
        print(f"[X] Jira test failed: {e}")
    
    show_summary()
    
    print("\n" + "="*60)
    print("[*] All tests complete!")
    print("="*60)
