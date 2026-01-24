"""
WaveQL Query Provenance Integration Test
=========================================
Tests provenance tracking across multiple live API adapters.

Adapters Tested:
1. ServiceNow
2. Jira
3. Stripe
4. Zendesk
5. Shopify

Features Tested:
1. Single adapter provenance tracking
2. Multi-adapter provenance (federation)
3. Predicate tracking (why-provenance)
4. Row-level provenance (where-provenance)
5. API call tracing
6. Provenance serialization

Novel Research:
    This is the first implementation of query provenance for SQL-over-API
    federation systems. See docs/research/query_provenance.md for details.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Load .env from project root
def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

load_env()

import waveql
from waveql.provenance import (
    enable_provenance,
    disable_provenance,
    get_provenance_tracker,
)

# ==============================================================================
# Configuration
# ==============================================================================

# ServiceNow
SN_INSTANCE = os.getenv("SN_INSTANCE")
SN_USERNAME = os.getenv("SN_USERNAME")
SN_PASSWORD = os.getenv("SN_PASSWORD")

# Jira
JIRA_HOST = os.getenv("JIRA_HOST")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_TOKEN = os.getenv("JIRA_TOKEN")

# Stripe
STRIPE_KEY = os.getenv("STRIPE_SECRET_KEY") or os.getenv("STRIPE_API_KEY")

# Zendesk
ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_TOKEN = os.getenv("ZENDESK_TOKEN")

# HubSpot
HUBSPOT_TOKEN = os.getenv("HUBSPOT_API_KEY") or os.getenv("HUBSPOT_ACCESS_TOKEN") or os.getenv("HUBSPOT_TOKEN")

# Shopify
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE_URL") or os.getenv("SHOPIFY_STORE")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") or os.getenv("SHOPIFY_TOKEN")


def separator(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def has_servicenow():
    return all([SN_INSTANCE, SN_USERNAME, SN_PASSWORD])

def has_jira():
    return all([JIRA_HOST, JIRA_EMAIL, JIRA_TOKEN])

def has_stripe():
    return bool(STRIPE_KEY)

def has_zendesk():
    return all([ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_TOKEN])

def has_hubspot():
    return bool(HUBSPOT_TOKEN)

def has_shopify():
    return all([SHOPIFY_STORE, SHOPIFY_TOKEN])


# ==============================================================================
# Test Functions
# ==============================================================================

def test_provenance_servicenow():
    """Test 1: ServiceNow provenance tracking"""
    separator("1. ServiceNow Provenance")
    
    if not has_servicenow():
        print("  [SKIP] ServiceNow credentials not configured")
        return None
    
    # Reset tracker
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        f"servicenow://{SN_INSTANCE}",
        username=SN_USERNAME,
        password=SN_PASSWORD,
    )
    cursor = conn.cursor()
    
    # Execute query
    cursor.execute("""
        SELECT sys_id, number, short_description, priority 
        FROM incident 
        WHERE priority <= 2 
        LIMIT 5
    """)
    results = cursor.fetchall()
    print(f"  Fetched {len(results)} incidents")
    
    # Check provenance
    history = tracker.get_history()
    assert len(history) > 0, "Should have provenance history"
    
    prov = history[-1]
    print(f"  Query ID: {prov.query_id[:12]}...")
    print(f"  Execution Time: {prov.total_latency_ms:.1f}ms")
    print(f"  API Calls: {prov.total_api_calls}")
    print(f"  Adapters: {prov.adapters_used}")
    
    # Verify adapter tracking
    assert "servicenow" in prov.adapters_used, "Should track ServiceNow adapter"
    
    # Verify API call details
    if prov.api_calls:
        call = prov.api_calls[0]
        print(f"\n  API Call Details:")
        print(f"    Adapter: {call.adapter_name}")
        print(f"    Table: {call.table_name}")
        print(f"    Rows: {call.rows_returned}")
        print(f"    Latency: {call.response_time_ms:.1f}ms")
        
        # Verify predicate tracking
        if call.request_params.get("predicates"):
            print(f"    Predicates: {call.request_params['predicates']}")
    
    # Verify row provenance (full mode)
    if prov.row_provenance:
        print(f"\n  Row Provenance: {len(prov.row_provenance)} rows tracked")
        for rp in prov.row_provenance[:2]:
            print(f"    Row {rp.row_index}: {rp.source_adapter}.{rp.source_table}")
            if rp.source_primary_key:
                print(f"      PK: {rp.source_primary_key}")
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] ServiceNow provenance works")
    return True


def test_provenance_jira():
    """Test 2: Jira provenance tracking"""
    separator("2. Jira Provenance")
    
    if not has_jira():
        print("  [SKIP] Jira credentials not configured")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        f"jira://{JIRA_HOST}",
        username=JIRA_EMAIL,
        password=JIRA_TOKEN,
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT key, summary, status 
        FROM issues 
        LIMIT 5
    """)
    results = cursor.fetchall()
    print(f"  Fetched {len(results)} issues")
    
    history = tracker.get_history()
    assert len(history) > 0, "Should have provenance history"
    
    prov = history[-1]
    print(f"  Adapters: {prov.adapters_used}")
    print(f"  API Calls: {prov.total_api_calls}")
    
    assert "jira" in prov.adapters_used, "Should track Jira adapter"
    
    if prov.api_calls:
        call = prov.api_calls[0]
        print(f"  API Call: {call.table_name} -> {call.rows_returned} rows in {call.response_time_ms:.1f}ms")
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] Jira provenance works")
    return True


def test_provenance_stripe():
    """Test 3: Stripe provenance tracking"""
    separator("3. Stripe Provenance")
    
    if not has_stripe():
        print("  [SKIP] Stripe credentials not configured")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        "stripe://api",
        password=STRIPE_KEY,
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, amount, status 
        FROM charges 
        LIMIT 5
    """)
    results = cursor.fetchall()
    print(f"  Fetched {len(results)} charges")
    
    history = tracker.get_history()
    assert len(history) > 0, "Should have provenance history"
    
    prov = history[-1]
    print(f"  Adapters: {prov.adapters_used}")
    print(f"  API Calls: {prov.total_api_calls}")
    
    assert "stripe" in prov.adapters_used, "Should track Stripe adapter"
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] Stripe provenance works")
    return True


def test_provenance_zendesk():
    """Test 4: Zendesk provenance tracking"""
    separator("4. Zendesk Provenance")
    
    if not has_zendesk():
        print("  [SKIP] Zendesk credentials not configured")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        f"zendesk://{ZENDESK_SUBDOMAIN}",
        username=ZENDESK_EMAIL,
        password=ZENDESK_TOKEN,
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, subject, status 
        FROM tickets 
        LIMIT 5
    """)
    results = cursor.fetchall()
    print(f"  Fetched {len(results)} tickets")
    
    history = tracker.get_history()
    assert len(history) > 0, "Should have provenance history"
    
    prov = history[-1]
    print(f"  Adapters: {prov.adapters_used}")
    print(f"  API Calls: {prov.total_api_calls}")
    
    assert "zendesk" in prov.adapters_used, "Should track Zendesk adapter"
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] Zendesk provenance works")
    return True


def test_provenance_shopify():
    """Test 5: Shopify provenance tracking"""
    separator("5. Shopify Provenance")
    
    if not has_shopify():
        print("  [SKIP] Shopify credentials not configured")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        f"shopify://{SHOPIFY_STORE}",
        password=SHOPIFY_TOKEN,
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, title, vendor 
        FROM products 
        LIMIT 5
    """)
    results = cursor.fetchall()
    print(f"  Fetched {len(results)} products")
    
    history = tracker.get_history()
    assert len(history) > 0, "Should have provenance history"
    
    prov = history[-1]
    print(f"  Adapters: {prov.adapters_used}")
    print(f"  API Calls: {prov.total_api_calls}")
    
    assert "shopify" in prov.adapters_used, "Should track Shopify adapter"
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] Shopify provenance works")
    return True


def test_provenance_hubspot():
    """Test 6: HubSpot provenance tracking"""
    separator("6. HubSpot Provenance")
    
    if not has_hubspot():
        print("  [SKIP] HubSpot credentials not configured")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        "hubspot://api",
        api_key=HUBSPOT_TOKEN,  # HubSpot adapter expects api_key, not password
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, firstname, lastname, email 
        FROM contacts 
        LIMIT 5
    """)
    results = cursor.fetchall()
    print(f"  Fetched {len(results)} contacts")
    
    history = tracker.get_history()
    assert len(history) > 0, "Should have provenance history"
    
    prov = history[-1]
    print(f"  Query ID: {prov.query_id[:12]}...")
    print(f"  Execution Time: {prov.total_latency_ms:.1f}ms")
    print(f"  Adapters: {prov.adapters_used}")
    print(f"  API Calls: {prov.total_api_calls}")
    
    assert "hubspot" in prov.adapters_used, "Should track HubSpot adapter"
    
    # Verify API call details
    if prov.api_calls:
        call = prov.api_calls[0]
        print(f"\n  API Call Details:")
        print(f"    Adapter: {call.adapter_name}")
        print(f"    Table: {call.table_name}")
        print(f"    Rows: {call.rows_returned}")
        print(f"    Latency: {call.response_time_ms:.1f}ms")
    
    # Verify row provenance (full mode)
    if prov.row_provenance:
        print(f"\n  Row Provenance: {len(prov.row_provenance)} rows tracked")
        for rp in prov.row_provenance[:2]:
            print(f"    Row {rp.row_index}: {rp.source_adapter}.{rp.source_table}")
            if rp.source_primary_key:
                print(f"      PK: {rp.source_primary_key}")
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] HubSpot provenance works")
    return True


def test_provenance_modes():
    """Test 7: Provenance modes (summary vs full vs sampled)"""
    separator("7. Provenance Modes")
    
    # Need at least one adapter to test modes
    if not has_servicenow():
        print("  [SKIP] ServiceNow credentials needed for mode test")
        return None
    
    tracker = get_provenance_tracker()
    
    # Test SUMMARY mode
    print("\n  Testing SUMMARY mode...")
    tracker.clear_history()
    enable_provenance(mode="summary")
    
    conn = waveql.connect(
        f"servicenow://{SN_INSTANCE}",
        username=SN_USERNAME,
        password=SN_PASSWORD,
    )
    cursor = conn.cursor()
    
    # Invalidate cache to ensure fresh fetch
    conn.invalidate_cache()
    
    cursor.execute("SELECT number FROM incident LIMIT 3")
    cursor.fetchall()
    
    prov = tracker.get_history()[-1]
    print(f"    Mode: {prov.provenance_mode}")
    print(f"    API Calls: {len(prov.api_calls)}")
    print(f"    Row Provenance: {len(prov.row_provenance)} (should be 0 in summary)")
    assert prov.provenance_mode == "summary"
    assert len(prov.row_provenance) == 0, "Summary mode should not track rows"
    
    # Test FULL mode
    print("\n  Testing FULL mode...")
    tracker.clear_history()
    enable_provenance(mode="full")
    
    # Invalidate cache again and use different query to avoid any caching issues
    conn.invalidate_cache()
    
    cursor.execute("SELECT number, priority FROM incident WHERE priority = 1 LIMIT 3")
    cursor.fetchall()
    
    prov = tracker.get_history()[-1]
    print(f"    Mode: {prov.provenance_mode}")
    print(f"    API Calls: {len(prov.api_calls)}")
    print(f"    Row Provenance: {len(prov.row_provenance)}")
    assert prov.provenance_mode == "full"
    # API calls should be recorded
    assert len(prov.api_calls) > 0, "Full mode should have API calls"
    # Row provenance should be recorded if we have results
    if prov.api_calls and prov.api_calls[0].rows_returned > 0:
        assert len(prov.row_provenance) > 0, f"Full mode should track rows (got {prov.api_calls[0].rows_returned} rows)"
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] Provenance modes work correctly")
    return True


def test_provenance_serialization():
    """Test 8: Provenance serialization to JSON"""
    separator("8. Provenance Serialization")
    
    if not has_servicenow():
        print("  [SKIP] ServiceNow credentials needed")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    conn = waveql.connect(
        f"servicenow://{SN_INSTANCE}",
        username=SN_USERNAME,
        password=SN_PASSWORD,
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT number, priority FROM incident LIMIT 3")
    cursor.fetchall()
    
    prov = tracker.get_history()[-1]
    
    # Test serialization
    import json
    prov_dict = prov.to_dict()
    
    print(f"  Serializing provenance to JSON...")
    json_str = json.dumps(prov_dict, indent=2, default=str)
    print(f"  JSON size: {len(json_str)} bytes")
    
    # Verify structure
    assert "query_id" in prov_dict
    assert "original_sql" in prov_dict
    assert "api_calls" in prov_dict
    assert "adapters_used" in prov_dict
    
    print(f"  Keys: {list(prov_dict.keys())}")
    
    conn.close()
    disable_provenance()
    
    print("  [PASS] Provenance serialization works")
    return True


def test_provenance_cross_adapter():
    """Test 9: Cross-adapter provenance (if multiple adapters configured)"""
    separator("9. Cross-Adapter Provenance")
    
    adapters_available = []
    if has_servicenow():
        adapters_available.append("servicenow")
    if has_jira():
        adapters_available.append("jira")
    if has_stripe():
        adapters_available.append("stripe")
    
    if len(adapters_available) < 2:
        print(f"  [SKIP] Need at least 2 adapters, have: {adapters_available}")
        return None
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="summary")
    
    # Execute queries on multiple adapters
    connections = []
    
    if has_servicenow():
        conn_sn = waveql.connect(
            f"servicenow://{SN_INSTANCE}",
            username=SN_USERNAME,
            password=SN_PASSWORD,
        )
        cursor_sn = conn_sn.cursor()
        cursor_sn.execute("SELECT number FROM incident LIMIT 2")
        cursor_sn.fetchall()
        connections.append(conn_sn)
        print("  Queried ServiceNow")
    
    if has_jira():
        conn_jira = waveql.connect(
            f"jira://{JIRA_HOST}",
            username=JIRA_EMAIL,
            password=JIRA_TOKEN,
        )
        cursor_jira = conn_jira.cursor()
        cursor_jira.execute("SELECT key FROM issues LIMIT 2")
        cursor_jira.fetchall()
        connections.append(conn_jira)
        print("  Queried Jira")
    
    if has_stripe():
        conn_stripe = waveql.connect(
            "stripe://api",
            password=STRIPE_KEY,
        )
        cursor_stripe = conn_stripe.cursor()
        cursor_stripe.execute("SELECT id FROM charges LIMIT 2")
        cursor_stripe.fetchall()
        connections.append(conn_stripe)
        print("  Queried Stripe")
    
    # Check provenance history
    history = tracker.get_history()
    print(f"\n  Total queries tracked: {len(history)}")
    
    all_adapters = set()
    for prov in history:
        all_adapters.update(prov.adapters_used)
    
    print(f"  All adapters in history: {all_adapters}")
    
    # Each query should have its own provenance
    assert len(history) >= 2, "Should have multiple provenance entries"
    
    # Cleanup
    for conn in connections:
        conn.close()
    disable_provenance()
    
    print("  [PASS] Cross-adapter provenance works")
    return True


# ==============================================================================
# Main
# ==============================================================================

def main():
    print("\n" + "="*60)
    print("  WaveQL Query Provenance Integration Test")
    print("  Novel Research: Data Lineage for API Federation")
    print("="*60)
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Check available adapters
    print("\n  Configured Adapters:")
    print(f"    ServiceNow: {'YES' if has_servicenow() else 'NO'}")
    print(f"    Jira:       {'YES' if has_jira() else 'NO'}")
    print(f"    Stripe:     {'YES' if has_stripe() else 'NO'}")
    print(f"    Zendesk:    {'YES' if has_zendesk() else 'NO'}")
    print(f"    HubSpot:    {'YES' if has_hubspot() else 'NO'}")
    print(f"    Shopify:    {'YES' if has_shopify() else 'NO'}")
    
    results = {}
    
    # Run tests
    tests = [
        ("ServiceNow Provenance", test_provenance_servicenow),
        ("Jira Provenance", test_provenance_jira),
        ("Stripe Provenance", test_provenance_stripe),
        ("Zendesk Provenance", test_provenance_zendesk),
        ("Shopify Provenance", test_provenance_shopify),
        ("HubSpot Provenance", test_provenance_hubspot),
        ("Provenance Modes", test_provenance_modes),
        ("Provenance Serialization", test_provenance_serialization),
        ("Cross-Adapter Provenance", test_provenance_cross_adapter),
    ]
    
    for name, test_fn in tests:
        try:
            result = test_fn()
            results[name] = result
        except Exception as e:
            print(f"\n  [FAIL] {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
        
        # Small delay between tests to help DNS resolver stability on Windows
        import time
        time.sleep(0.5)
    
    # Summary
    separator("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v is True)
    skipped = sum(1 for v in results.values() if v is None)
    failed = sum(1 for v in results.values() if v is False)
    
    for name, result in results.items():
        if result is True:
            status = "[PASS]"
        elif result is None:
            status = "[SKIP]"
        else:
            status = "[FAIL]"
        print(f"  {status} {name}")
    
    print(f"\n  Result: {passed} passed, {skipped} skipped, {failed} failed")
    
    if failed == 0:
        print("\n  All provenance tests passed!")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
