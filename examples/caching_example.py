#!/usr/bin/env python
"""
WaveQL Caching Example

Demonstrates query result caching to improve performance and reduce API calls.

Caching is enabled by default with a 5-minute TTL. This example shows:
- Basic caching behavior
- Custom TTL configuration
- Cache statistics
- Cache invalidation
- Per-adapter TTL settings
"""

import time
from waveql import connect, CacheConfig


def demo_basic_caching():
    """Demonstrate basic caching behavior."""
    print("\n1. Basic Caching")
    print("-" * 50)
    
    # Caching is enabled by default
    conn = connect("file://employees.csv")
    cursor = conn.cursor()
    
    # First query - fetches from source
    start = time.time()
    cursor.execute("SELECT * FROM employees")
    first_query_time = time.time() - start
    print(f"First query:  {first_query_time:.4f}s (from source)")
    
    # Second query - served from cache (instant!)
    start = time.time()
    cursor.execute("SELECT * FROM employees")
    second_query_time = time.time() - start
    print(f"Second query: {second_query_time:.4f}s (from cache)")
    
    # Show cache stats
    stats = conn.cache_stats
    print(f"Cache hits: {stats.hits}, misses: {stats.misses}")
    
    conn.close()


def demo_cache_statistics():
    """Demonstrate cache statistics monitoring."""
    print("\n2. Cache Statistics")
    print("-" * 50)
    
    conn = connect("file://employees.csv")
    cursor = conn.cursor()
    
    # Run several queries
    cursor.execute("SELECT * FROM employees")
    cursor.execute("SELECT * FROM employees")  # cache hit
    cursor.execute("SELECT * FROM employees WHERE department = 'Engineering'")
    cursor.execute("SELECT * FROM employees")  # cache hit
    cursor.execute("SELECT * FROM employees WHERE department = 'Engineering'")  # cache hit
    
    # Get detailed statistics
    stats = conn.cache_stats
    print(f"Hits:        {stats.hits}")
    print(f"Misses:      {stats.misses}")
    print(f"Hit Rate:    {stats.hit_rate:.1f}%")
    print(f"Entries:     {stats.entries}")
    print(f"Size:        {stats.size_mb:.3f} MB")
    print(f"Evictions:   {stats.evictions}")
    
    # As dictionary (useful for logging/metrics)
    print(f"\nAs dict: {stats.to_dict()}")
    
    conn.close()


def demo_custom_ttl():
    """Demonstrate custom TTL configuration."""
    print("\n3. Custom TTL Configuration")
    print("-" * 50)
    
    # Short TTL for rapidly changing data (5 seconds)
    conn = connect("file://employees.csv", cache_ttl=5)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM employees")
    print("Query executed - cached with 5s TTL")
    
    cursor.execute("SELECT * FROM employees")
    print(f"Immediate re-query: cache hit = {conn.cache_stats.hits}")
    
    print("Waiting 6 seconds for TTL to expire...")
    time.sleep(6)
    
    cursor.execute("SELECT * FROM employees")
    print("After TTL expired: cache miss (re-fetched from source)")
    print(f"Stats: {conn.cache_stats.to_dict()}")
    
    conn.close()


def demo_cache_invalidation():
    """Demonstrate manual cache invalidation."""
    print("\n4. Cache Invalidation")
    print("-" * 50)
    
    conn = connect("file://employees.csv")
    cursor = conn.cursor()
    
    # Populate cache
    cursor.execute("SELECT * FROM employees")
    cursor.execute("SELECT * FROM employees WHERE salary > 80000")
    print(f"Cache entries: {conn.cache_stats.entries}")
    
    # Invalidate specific table
    cleared = conn.invalidate_cache(table="employees")
    print(f"Invalidated {cleared} entries for 'employees' table")
    print(f"Cache entries after: {conn.cache_stats.entries}")
    
    # Re-populate
    cursor.execute("SELECT * FROM employees")
    cursor.execute("SELECT * FROM employees WHERE salary > 80000")
    
    # Clear all cache
    cleared = conn.invalidate_cache()
    print(f"Cleared all {cleared} entries")
    
    conn.close()


def demo_advanced_config():
    """Demonstrate advanced cache configuration."""
    print("\n5. Advanced Configuration")
    print("-" * 50)
    
    # Full configuration with CacheConfig
    config = CacheConfig(
        enabled=True,
        default_ttl=300,           # 5 minutes default
        max_entries=1000,          # Max 1000 cached queries
        max_memory_mb=256,         # Max 256 MB memory
        adapter_ttl={              # Per-adapter overrides
            "servicenow": 60,      # ServiceNow: 1 minute
            "jira": 120,           # Jira: 2 minutes
            "file": 600,           # Files: 10 minutes
        },
        exclude_tables=[           # Never cache these tables
            "audit_log",
            "sys_journal",
        ],
    )
    
    conn = connect("file://employees.csv", cache_config=config)
    
    print("CacheConfig settings:")
    print(f"  enabled:       {config.enabled}")
    print(f"  default_ttl:   {config.default_ttl}s")
    print(f"  max_entries:   {config.max_entries}")
    print(f"  max_memory_mb: {config.max_memory_mb}")
    print(f"  adapter_ttl:   {config.adapter_ttl}")
    print(f"  exclude:       {config.exclude_tables}")
    
    conn.close()


def demo_disable_caching():
    """Demonstrate disabling caching."""
    print("\n6. Disable Caching")
    print("-" * 50)
    
    # Completely disable caching
    conn = connect("file://employees.csv", enable_cache=False)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM employees")
    cursor.execute("SELECT * FROM employees")
    
    stats = conn.cache_stats
    print(f"Cache enabled: {conn.cache.config.enabled}")
    print(f"Hits: {stats.hits}, Misses: {stats.misses}")
    print("(Both queries hit the source)")
    
    conn.close()


def demo_servicenow_caching():
    """Example: ServiceNow with caching (template)."""
    print("\n7. ServiceNow Caching Example (Template)")
    print("-" * 50)
    
    example_code = '''
# Production example: ServiceNow with caching
from waveql import connect, CacheConfig

config = CacheConfig(
    default_ttl=60,              # Default: 1 minute
    adapter_ttl={
        "servicenow": 30,        # Tickets: 30 seconds (fast changing)
    },
    exclude_tables=[
        "sys_audit",             # Never cache audit tables
    ],
)

conn = connect(
    "servicenow://instance.service-now.com",
    username="admin",
    password="password",
    cache_config=config,
)

cursor = conn.cursor()

# First query: ~500ms (API call)
cursor.execute("SELECT number, priority FROM incident WHERE active=true")

# Repeated query: ~1ms (from cache!)
cursor.execute("SELECT number, priority FROM incident WHERE active=true")

# Check performance
print(conn.cache_stats.to_dict())
# {'hits': 1, 'misses': 1, 'hit_rate': '50.0%', 'size_mb': 0.5}

# After data update, invalidate cache
conn.invalidate_cache(table="incident")
'''
    
    print(example_code)


def main():
    """Run all caching demos."""
    print("=" * 60)
    print("WaveQL - Query Result Caching Demo")
    print("=" * 60)
    
    # Create a simple CSV for demos
    import csv
    import tempfile
    from pathlib import Path
    
    temp_dir = Path(tempfile.mkdtemp())
    csv_path = temp_dir / "employees.csv"
    
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "department", "salary"])
        writer.writerow([1, "Alice", "Engineering", 95000])
        writer.writerow([2, "Bob", "Marketing", 75000])
        writer.writerow([3, "Charlie", "Engineering", 85000])
    
    # Change to temp dir for file:// URIs
    import os
    original_dir = os.getcwd()
    os.chdir(temp_dir)
    
    try:
        demo_basic_caching()
        demo_cache_statistics()
        demo_custom_ttl()
        demo_cache_invalidation()
        demo_advanced_config()
        demo_disable_caching()
        demo_servicenow_caching()
        
        print("\n" + "=" * 60)
        print("Demo complete!")
        print("=" * 60)
        
    finally:
        os.chdir(original_dir)
        csv_path.unlink()
        temp_dir.rmdir()


if __name__ == "__main__":
    main()
