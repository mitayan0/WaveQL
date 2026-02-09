#!/usr/bin/env python
"""
Demo Script: Query Provenance with Live Data

This script demonstrates the Query Provenance feature with live adapters.
It shows how to:
1. Enable provenance tracking
2. Execute queries 
3. Inspect the captured provenance data

Usage:
    python examples/demo_provenance.py
"""

import sys
import os
import io
import tempfile

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from waveql import connect
from waveql.provenance import (
    enable_provenance, 
    disable_provenance, 
    get_provenance_tracker,
)


def demo_csv_adapter():
    """Demo with CSV file adapter (no external credentials needed)."""
    print("\n" + "="*60)
    print("[DEMO] WaveQL Query Provenance Demo - CSV Adapter")
    print("="*60)
    
    # Create a temporary CSV file with test data
    import csv
    
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "incidents.csv")
    
    print(f"\n[SETUP] Creating test CSV file: {csv_path}")
    
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'number', 'priority', 'state', 'description'])
        for i in range(1, 21):
            writer.writerow([
                i, 
                f'INC{i:05d}', 
                (i % 4) + 1, 
                'open' if i % 3 else 'closed',
                f'Test incident {i}'
            ])
    
    print("[OK] Created test CSV with 20 incidents\n")
    
    # Connect using file adapter
    conn = connect(adapter="file", host=csv_path)
    cursor = conn.cursor()
    
    # Enable provenance tracking
    enable_provenance(mode="full")
    print("[INFO] Provenance tracking ENABLED (full mode)\n")
    
    # Execute a query
    print("-"*60)
    print("Executing: SELECT * FROM incidents WHERE priority <= 2 LIMIT 5")
    print("-"*60)
    cursor.execute("SELECT * FROM incidents WHERE priority <= 2 LIMIT 5")
    results = cursor.fetchall()
    print(f"-> Returned {len(results)} rows\n")
    
    # Show results
    if results:
        print("   Results:")
        for row in results[:3]:
            print(f"      {dict(row)}")
        if len(results) > 3:
            print(f"      ... and {len(results) - 3} more rows")
    
    # Check provenance
    tracker = get_provenance_tracker()
    history = tracker.get_history()
    
    if history:
        prov = history[-1]
        print("\n" + "="*60)
        print("[PROVENANCE] CAPTURED:")
        print("="*60)
        print(f"   Query ID:        {prov.query_id}")
        print(f"   SQL:             {prov.original_sql}")
        print(f"   Execution Time:  {prov.total_latency_ms:.1f}ms")
        print(f"   Total Rows:      {prov.total_rows}")
        print(f"   Total API Calls: {prov.total_api_calls}")
        print(f"   Adapters Used:   {prov.adapters_used}")
        print(f"   Tables Accessed: {prov.tables_accessed}")
        print(f"   Mode:            {prov.provenance_mode}")
        
        if prov.api_calls:
            print("\n   [API CALLS]:")
            for i, call in enumerate(prov.api_calls, 1):
                print(f"\n   [{i}] {call.adapter_name}.{call.table_name}")
                print(f"       Trace ID:  {call.trace_id[:12]}...")
                print(f"       Endpoint:  {call.endpoint_url or 'N/A'}")
                print(f"       Rows:      {call.rows_returned}")
                print(f"       Latency:   {call.response_time_ms:.1f}ms")
                if call.request_params:
                    print(f"       Predicates: {call.request_params.get('predicates', [])}")
        
        if prov.row_provenance:
            print(f"\n   [ROW PROVENANCE] ({len(prov.row_provenance)} rows tracked):")
            for rp in prov.row_provenance[:3]:
                print(f"\n   Row {rp.row_index}:")
                print(f"       Source: {rp.source_adapter}.{rp.source_table}")
                if rp.source_primary_key:
                    print(f"       PK:     {rp.source_primary_key}")
            if len(prov.row_provenance) > 3:
                print(f"\n   ... and {len(prov.row_provenance) - 3} more rows")
        
        # Export to dict
        print("\n   [SERIALIZED PROVENANCE (partial)]:")
        prov_dict = prov.to_dict()
        import json
        print(f"   {json.dumps(prov_dict, indent=2, default=str)[:500]}...")
        
    else:
        print("[WARN] No provenance was captured")
    
    # Cleanup
    disable_provenance()
    conn.close()
    
    # Try to cleanup temp file
    try:
        os.remove(csv_path)
        os.rmdir(temp_dir)
    except Exception:
        pass
    
    print("\n[OK] Demo complete!")
    return True


def demo_with_api_adapter(adapter_name: str, connection_params: dict):
    """Demo with a real API adapter."""
    print("\n" + "="*60)
    print(f"[DEMO] WaveQL Query Provenance Demo - {adapter_name.upper()}")
    print("="*60)
    
    try:
        conn = connect(**connection_params)
        cursor = conn.cursor()
    except Exception as e:
        print(f"[ERROR] Could not connect: {e}")
        return False
    
    # Enable FULL provenance tracking
    enable_provenance(mode="full")
    print("\n[INFO] Provenance tracking ENABLED (full mode)\n")
    
    # Get table to query
    table = connection_params.get("test_table", "incident")
    
    print("-"*60)
    print(f"Executing: SELECT * FROM {table} LIMIT 5")
    print("-"*60)
    
    try:
        cursor.execute(f"SELECT * FROM {table} LIMIT 5")
        results = cursor.fetchall()
        print(f"-> Returned {len(results)} rows\n")
        
        if results:
            print("   Columns:", list(results[0].keys()) if hasattr(results[0], 'keys') else "N/A")
    except Exception as e:
        print(f"[ERROR] Query failed: {e}")
        disable_provenance()
        return False
    
    # Check provenance
    tracker = get_provenance_tracker()
    history = tracker.get_history()
    
    if history:
        prov = history[-1]
        print("\n[PROVENANCE] CAPTURED:")
        print("-"*60)
        print(f"   Query ID:        {prov.query_id}")
        print(f"   SQL:             {prov.original_sql}")
        print(f"   Total Latency:   {prov.total_latency_ms:.1f}ms")
        print(f"   Total Rows:      {prov.total_rows}")
        print(f"   Total API Calls: {prov.total_api_calls}")
        print(f"   Adapters Used:   {prov.adapters_used}")
        print(f"   Tables Accessed: {prov.tables_accessed}")
        
        if prov.api_calls:
            print("\n   [API CALLS]:")
            for i, call in enumerate(prov.api_calls, 1):
                print(f"\n   [{i}] {call.adapter_name}.{call.table_name}")
                print(f"       Trace ID: {call.trace_id[:12]}...")
                print(f"       Endpoint: {call.endpoint_url or 'N/A'}")
                print(f"       Status:   {call.response_status}")
                print(f"       Rows:     {call.rows_returned}")
                print(f"       Latency:  {call.response_time_ms:.1f}ms")
        
        if prov.row_provenance:
            print(f"\n   [ROW PROVENANCE] ({len(prov.row_provenance)} rows tracked):")
            for rp in prov.row_provenance[:3]:
                print(f"      Row {rp.row_index}: {rp.source_adapter}.{rp.source_table}")
                if rp.source_primary_key:
                    print(f"                  PK: {rp.source_primary_key}")
        
    else:
        print("[WARN] No provenance was captured")
    
    disable_provenance()
    conn.close()
    
    print("\n[OK] Demo complete!")
    return True


def main():
    """Main entry point."""
    
    print("\n" + "="*60)
    print("       WAVEQL QUERY PROVENANCE DEMO")
    print("       Novel Research: Data Lineage for API Federation")
    print("="*60)
    
    # Run CSV adapter demo (no credentials needed)
    demo_csv_adapter()
    
    # Check for environment variables to test with real adapters
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    # Try ServiceNow if configured
    if os.getenv("SERVICENOW_INSTANCE"):
        print("\n\n" + "-"*60)
        print("Found ServiceNow credentials, running live demo...")
        demo_with_api_adapter("servicenow", {
            "adapter": "servicenow",
            "host": os.getenv("SERVICENOW_INSTANCE"),
            "user": os.getenv("SERVICENOW_USER"),
            "password": os.getenv("SERVICENOW_PASSWORD"),
            "test_table": "incident",
        })
    
    print("\n\n" + "="*60)
    print("[USAGE] To use provenance in your code:")
    print("""
   from waveql import connect
   from waveql.provenance import enable_provenance, get_provenance_tracker
   
   # Enable tracking
   enable_provenance(mode="full")  # or "summary", "sampled"
   
   # Execute queries
   conn = connect(adapter="servicenow", host="...", ...)
   cursor = conn.cursor()
   cursor.execute("SELECT * FROM incident LIMIT 10")
   
   # Inspect provenance
   tracker = get_provenance_tracker()
   for prov in tracker.get_history():
       print(f"Query: {prov.original_sql}")
       print(f"API Calls: {prov.total_api_calls}")
       for call in prov.api_calls:
           print(f"  - {call.adapter_name}.{call.table_name}: {call.rows_returned} rows")
""")
    print("="*60)


if __name__ == "__main__":
    main()
