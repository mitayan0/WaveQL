import re
from waveql.query_planner import Predicate
from waveql.adapters.rest_adapter import RESTAdapter

def debug_filtering():
    print("--- Debugging Client-Side Filtering ---")
    
    # Mock data
    records = [
        {"id": 1, "title": "sunt aut facere repellat provident occaecati excepturi optio reprehenderit"},
        {"id": 2, "title": "qui est esse"},
        {"id": 3, "title": "ea molestias quasi exercitationem repellat qui ipsa sit aut"},
    ]
    
    # 1. LIKE Filter
    pred = Predicate(column="title", operator="LIKE", value="%optio%")
    print(f"Predicate: {pred}")
    
    # Manually run logic from _apply_filters
    pattern = pred.value.replace("%", ".*").replace("_", ".")
    print(f"Regex Pattern: {pattern}")
    
    for r in records:
        val = r["title"]
        match = bool(re.search(pattern, str(val or ""), re.IGNORECASE))
        print(f"ID {r['id']}: Match={match}")
        
    # 2. RUN Adapter Method
    # We need to instantiate adapter just to access the method, or mock it
    # We can just call the method if we strip 'self' dependency (it's static-ish)
    # But let's verify if the class actually filters.
    
    adapter = RESTAdapter(host="http://mock", endpoints={})
    filtered = adapter._apply_filters(records, [pred])
    print(f"\nAdapter Filtered Result Count: {len(filtered)}")
    for r in filtered:
        print(f"Kept: {r['id']}")

def debug_cache_logic():
    print("\n--- Debugging Cache Logic ---")
    import waveql
    
    # 1. Setup Connection with Cache
    conn = waveql.connect(adapter="rest", host="http://mock", enable_cache=True, cache_ttl=60)
    print(f"Connection Cache Enabled: {conn.cache_enabled}")
    
    if not conn.cache_enabled:
        print("ERROR: Cache should be enabled!")
        
    cache = conn._cache
    print(f"Cache Instance: {cache}")
    
    # 2. Simulate Put/Get
    key = "test_key"
    data = {"foo": "bar"}
    import pyarrow as pa
    table = pa.Table.from_pydict({"a": [1, 2, 3]})
    
    cache.put(key, table, adapter_name="rest", table_name="test_table")
    print("Put item in cache.")
    
    cached = cache.get(key)
    print(f"Get item: {cached is not None}")
    
    # 3. Stats
    print(f"Stats: Hits={conn.cache_stats.hits}, Misses={conn.cache_stats.misses}")
    # Note: Manual get() might not update high-level stats if stats are tracked in cursor wrapper
    
if __name__ == "__main__":
    debug_filtering()
    debug_cache_logic()
