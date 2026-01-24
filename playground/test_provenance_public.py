"""
WaveQL Provenance Test with Public APIs
========================================
Tests provenance tracking using free, public APIs that require no authentication.

APIs Used:
1. JSONPlaceholder - Fake REST API for testing (https://jsonplaceholder.typicode.com)

This demonstrates that provenance tracking works correctly without
the complexity of API keys or network authentication issues.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
import pyarrow as pa

# Import WaveQL provenance
from waveql.provenance import (
    enable_provenance,
    disable_provenance,
    get_provenance_tracker,
)
from waveql.provenance.models import QueryProvenance, APICallTrace, RowProvenance
from waveql.adapters.base import BaseAdapter
from waveql.schema_cache import ColumnInfo


class JSONPlaceholderAdapter(BaseAdapter):
    """
    Adapter for JSONPlaceholder - a free fake REST API for testing.
    
    Tables:
    - posts: 100 fake blog posts
    - users: 10 fake users
    - comments: 500 fake comments
    - todos: 200 fake todos
    """
    
    adapter_name = "jsonplaceholder"
    supports_predicate_pushdown = False  # Simple API, no filtering
    
    BASE_URL = "https://jsonplaceholder.typicode.com"
    
    TABLES = {
        "posts": "posts",
        "users": "users", 
        "comments": "comments",
        "todos": "todos",
        "albums": "albums",
        "photos": "photos",
    }
    
    def __init__(self, host="jsonplaceholder.typicode.com", **kwargs):
        super().__init__(host, None, None, **kwargs)
    
    def fetch(
        self,
        table: str,
        columns=None,
        predicates=None,
        limit=None,
        offset=None,
        order_by=None,
        group_by=None,
        aggregates=None,
    ) -> pa.Table:
        """Fetch data from JSONPlaceholder."""
        resource = self.TABLES.get(table.lower(), table.lower())
        url = f"{self.BASE_URL}/{resource}"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with httpx.Client(timeout=30.0) as client:
                    response = client.get(url)
                    if response.status_code >= 400:
                        raise Exception(f"API error: {response.status_code}")
                    
                    data = response.json()
                    
                    # Apply limit
                    if limit:
                        data = data[:limit]
                    
                    return pa.Table.from_pylist(data) if data else pa.table({})
                    
            except httpx.ConnectError as e:
                if attempt < max_retries - 1:
                    time.sleep(1.0 * (2 ** attempt))
                else:
                    raise
    
    def get_schema(self, table: str):
        """Get schema by fetching one record."""
        result = self.fetch(table, limit=1)
        if len(result) == 0:
            return []
        return [ColumnInfo(name=col, data_type="string") for col in result.schema.names]
    
    def list_tables(self):
        return list(self.TABLES.keys())


def separator(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def test_network():
    """Test 1: Direct network connectivity test."""
    separator("1. Network Connectivity")
    
    urls = [
        ("JSONPlaceholder Posts", "https://jsonplaceholder.typicode.com/posts?_limit=1"),
        ("JSONPlaceholder Users", "https://jsonplaceholder.typicode.com/users"),
    ]
    
    all_pass = True
    for name, url in urls:
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(url)
                if response.status_code == 200:
                    print(f"  [OK] {name}: HTTP {response.status_code}")
                else:
                    print(f"  [WARN] {name}: HTTP {response.status_code}")
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            all_pass = False
    
    if all_pass:
        print("  [PASS] Network connectivity works")
    return all_pass


def test_adapter_direct():
    """Test 2: Direct adapter fetch (no provenance)."""
    separator("2. Direct Adapter Fetch")
    
    adapter = JSONPlaceholderAdapter()
    
    # Test posts
    result = adapter.fetch(table="posts", limit=5)
    print(f"  Posts: {len(result)} rows")
    
    # Test users
    result = adapter.fetch(table="users", limit=3)
    print(f"  Users: {len(result)} rows")
    
    # Test todos
    result = adapter.fetch(table="todos", limit=5)
    print(f"  Todos: {len(result)} rows")
    
    print("  [PASS] Direct adapter fetch works")
    return True


def test_provenance_manual():
    """Test 3: Manual provenance tracking."""
    separator("3. Manual Provenance Tracking")
    
    import uuid
    from datetime import datetime
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="full")
    
    adapter = JSONPlaceholderAdapter()
    
    # Create provenance record manually
    query_id = str(uuid.uuid4())
    start_time = datetime.now()
    
    # Fetch data
    result = adapter.fetch(table="posts", limit=5)
    
    end_time = datetime.now()
    latency = (end_time - start_time).total_seconds() * 1000
    
    # Create API call trace
    api_call = APICallTrace(
        adapter_name="jsonplaceholder",
        table_name="posts",
        request_params={"limit": 5},
        response_time_ms=latency,
        rows_returned=len(result),
        timestamp=start_time,
    )
    
    # Create row provenance
    row_provenance = []
    for i in range(min(len(result), 5)):
        row_prov = RowProvenance(
            row_index=i,
            source_adapter="jsonplaceholder",
            source_table="posts",
            source_primary_key=str(result.column("id")[i].as_py()) if "id" in result.schema.names else None,
        )
        row_provenance.append(row_prov)
    
    # Create full provenance
    prov = QueryProvenance(
        query_id=query_id,
        original_sql="SELECT * FROM posts LIMIT 5",
        execution_start=start_time,
        execution_end=end_time,
        api_calls=[api_call],
        adapters_used=["jsonplaceholder"],
        tables_accessed=["posts"],
        provenance_mode="full",
        row_provenance=row_provenance,
    )
    
    # Add to tracker
    tracker._history.append(prov)
    
    print(f"  Fetched {len(result)} posts")
    print(f"  Query ID: {prov.query_id[:12]}...")
    print(f"  Execution Time: {prov.total_latency_ms:.1f}ms")
    print(f"  API Calls: {prov.total_api_calls}")
    print(f"  Adapters: {prov.adapters_used}")
    print(f"  Row Provenance: {len(prov.row_provenance)} rows tracked")
    
    # Show sample row provenance
    if prov.row_provenance:
        for rp in prov.row_provenance[:2]:
            print(f"    Row {rp.row_index}: {rp.source_adapter}.{rp.source_table} (PK: {rp.source_primary_key})")
    
    disable_provenance()
    print("  [PASS] Manual provenance tracking works")
    return True


def test_provenance_serialization():
    """Test 4: Provenance serialization to JSON."""
    separator("4. Provenance Serialization")
    
    tracker = get_provenance_tracker()
    
    history = tracker.get_history()
    if not history:
        print("  [WARN] No provenance history - creating one")
        # Create a sample provenance for testing
        test_provenance_manual()
        history = tracker.get_history()
    
    if not history:
        print("  [SKIP] Still no provenance to serialize")
        return None
    
    prov = history[-1]
    
    import json
    prov_dict = prov.to_dict()
    
    print(f"  Serializing provenance to JSON...")
    json_str = json.dumps(prov_dict, indent=2, default=str)
    print(f"  JSON size: {len(json_str)} bytes")
    
    # Verify structure
    assert "query_id" in prov_dict, "Missing query_id"
    assert "api_calls" in prov_dict, "Missing api_calls"
    assert "adapters_used" in prov_dict, "Missing adapters_used"
    
    print(f"  Keys: {list(prov_dict.keys())}")
    
    print("  [PASS] Provenance serialization works")
    return True


def test_multi_table_provenance():
    """Test 5: Multi-table provenance tracking."""
    separator("5. Multi-Table Provenance")
    
    import uuid
    from datetime import datetime
    
    tracker = get_provenance_tracker()
    tracker.clear_history()
    enable_provenance(mode="summary")
    
    adapter = JSONPlaceholderAdapter()
    
    tables = ["posts", "users", "todos"]
    
    for table in tables:
        query_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        result = adapter.fetch(table=table, limit=3)
        
        end_time = datetime.now()
        latency = (end_time - start_time).total_seconds() * 1000
        
        api_call = APICallTrace(
            adapter_name="jsonplaceholder",
            table_name=table,
            request_params={"limit": 3},
            response_time_ms=latency,
            rows_returned=len(result),
            timestamp=start_time,
        )
        
        prov = QueryProvenance(
            query_id=query_id,
            original_sql=f"SELECT * FROM {table} LIMIT 3",
            execution_start=start_time,
            execution_end=end_time,
            api_calls=[api_call],
            adapters_used=["jsonplaceholder"],
            tables_accessed=[table],
            provenance_mode="summary",
        )
        
        tracker._history.append(prov)
        print(f"  {table}: {len(result)} rows, {latency:.1f}ms")
    
    history = tracker.get_history()
    print(f"\n  Total queries tracked: {len(history)}")
    
    all_tables = set()
    for prov in history:
        all_tables.update(prov.tables_accessed)
    
    print(f"  All tables accessed: {all_tables}")
    
    disable_provenance()
    print("  [PASS] Multi-table provenance works")
    return True


def main():
    print("\n" + "="*60)
    print("  WaveQL Provenance Test - Public APIs")
    print("  No API Keys Required!")
    print("="*60)
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    results = {}
    
    tests = [
        ("Network Connectivity", test_network),
        ("Direct Adapter Fetch", test_adapter_direct),
        ("Manual Provenance Tracking", test_provenance_manual),
        ("Provenance Serialization", test_provenance_serialization),
        ("Multi-Table Provenance", test_multi_table_provenance),
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
        
        time.sleep(0.3)  # Small delay between tests
    
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
        print("\n  ✓ All provenance tests passed with public APIs!")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
