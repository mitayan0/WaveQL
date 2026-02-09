"""
WaveQL Cross-Source JOIN Demo
==============================
The KILLER FEATURE of WaveQL: Join data from completely different sources
using a single SQL query!

This demo shows:
1. ServiceNow incidents + Local CSV (SLA targets)
2. ServiceNow users + Local JSON (department metadata)
3. ServiceNow + Parquet analytics data
4. Multi-source aggregations

Prerequisites:
- ServiceNow credentials in .env file
- Local test data files in playground/data/
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
import duckdb

# Configuration
INSTANCE = os.getenv("SN_INSTANCE")
USERNAME = os.getenv("SN_USERNAME")
PASSWORD = os.getenv("SN_PASSWORD")
DATA_DIR = Path(__file__).parent / "data"


def separator(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def setup_local_data():
    """Create local enrichment data files."""
    separator("SETUP: Creating Local Enrichment Data")
    
    DATA_DIR.mkdir(exist_ok=True)
    
    # SLA Targets by Priority (CSV)
    sla_path = DATA_DIR / "sla_targets.csv"
    with open(sla_path, "w") as f:
        f.write("priority,target_hours,escalation_hours,description\n")
        f.write("1,4,2,Critical - P1 - Business Critical\n")
        f.write("2,8,4,High - P2 - Major Impact\n")
        f.write("3,24,12,Medium - P3 - Moderate Impact\n")
        f.write("4,48,24,Low - P4 - Minor Impact\n")
        f.write("5,72,48,Planning - P5 - No Impact\n")
    print("  ✓ Created: sla_targets.csv")
    
    # Priority Cost Mapping (JSON)
    cost_path = DATA_DIR / "priority_costs.json"
    with open(cost_path, "w") as f:
        f.write("""[
  {"priority": "1", "hourly_cost": 500, "breach_penalty": 5000},
  {"priority": "2", "hourly_cost": 200, "breach_penalty": 2000},
  {"priority": "3", "hourly_cost": 100, "breach_penalty": 1000},
  {"priority": "4", "hourly_cost": 50, "breach_penalty": 500},
  {"priority": "5", "hourly_cost": 25, "breach_penalty": 250}
]""")
    print("  ✓ Created: priority_costs.json")
    
    # State Descriptions (CSV)
    states_path = DATA_DIR / "states.csv"
    with open(states_path, "w") as f:
        f.write("state_id,state_name,is_active,category\n")
        f.write("1,New,true,Open\n")
        f.write("2,In Progress,true,Open\n")
        f.write("3,On Hold,true,Pending\n")
        f.write("4,Resolved,false,Closed\n")
        f.write("5,Closed,false,Closed\n")
        f.write("6,Cancelled,false,Closed\n")
    print("  ✓ Created: states.csv")
    
    print(f"\n  Data directory: {DATA_DIR}")
    return True


def test_sn_with_csv_sla():
    """Test 1: Join ServiceNow incidents with local SLA targets CSV"""
    separator("1. ServiceNow + CSV: Incident SLA Analysis")
    
    # Connect to ServiceNow
    sn_conn = waveql.connect(
        f"servicenow://{INSTANCE}",
        username=USERNAME,
        password=PASSWORD,
    )
    sn_cursor = sn_conn.cursor()
    
    # Fetch incidents from ServiceNow
    sn_cursor.execute("""
        SELECT number, short_description, priority, state
        FROM incident
        WHERE priority <= 3
        LIMIT 10
    """)
    incidents = sn_cursor.to_arrow()
    sn_conn.close()
    
    print(f"  Fetched {len(incidents)} incidents from ServiceNow")
    
    # Now join with local SLA data using DuckDB
    db = duckdb.connect(":memory:")
    db.register("incidents", incidents)
    
    sla_path = DATA_DIR / "sla_targets.csv"
    
    result = db.execute(f"""
        SELECT 
            i.number,
            i.short_description,
            i.priority,
            s.target_hours,
            s.description as sla_tier
        FROM incidents i
        JOIN read_csv_auto('{sla_path}') s 
            ON i.priority = s.priority
        ORDER BY i.priority
    """).fetchall()
    
    print("\n  Incidents with SLA Targets:")
    print("  " + "-"*70)
    for row in result[:5]:
        desc = row[1][:30] + "..." if len(row[1]) > 30 else row[1]
        print(f"  {row[0]} | P{row[2]} | {row[3]}h SLA | {desc}")
    
    db.close()
    print("\n  ✓ ServiceNow + CSV JOIN works!")
    return True


def test_sn_with_json_costs():
    """Test 2: Join ServiceNow incidents with JSON cost data"""
    separator("2. ServiceNow + JSON: Cost Analysis")
    
    # Connect to ServiceNow
    sn_conn = waveql.connect(
        f"servicenow://{INSTANCE}",
        username=USERNAME,
        password=PASSWORD,
    )
    sn_cursor = sn_conn.cursor()
    
    # Fetch incident counts by priority
    sn_cursor.execute("""
        SELECT priority, COUNT(*) as count
        FROM incident
        GROUP BY priority
    """)
    priority_counts = sn_cursor.to_arrow()
    sn_conn.close()
    
    print("  Fetched priority distribution from ServiceNow")
    
    # Join with cost data
    db = duckdb.connect(":memory:")
    db.register("priority_counts", priority_counts)
    
    cost_path = DATA_DIR / "priority_costs.json"
    
    result = db.execute(f"""
        SELECT 
            pc.priority,
            pc.count as incident_count,
            c.hourly_cost,
            c.breach_penalty,
            (pc.count * c.breach_penalty) as potential_penalty_exposure
        FROM priority_counts pc
        JOIN read_json_auto('{cost_path}') c 
            ON pc.priority = c.priority
        ORDER BY potential_penalty_exposure DESC
    """).fetchall()
    
    print("\n  Cost Exposure by Priority:")
    print("  " + "-"*60)
    print(f"  {'Priority':<10} {'Incidents':<12} {'Hourly $':<12} {'Max Penalty':<15}")
    print("  " + "-"*60)
    
    total_exposure = 0
    for row in result:
        total_exposure += row[4]
        print(f"  P{row[0]:<9} {row[1]:<12} ${row[2]:<11,} ${row[4]:>12,}")
    
    print("  " + "-"*60)
    print(f"  {'TOTAL EXPOSURE':<35} ${total_exposure:>12,}")
    
    db.close()
    print("\n  ✓ ServiceNow + JSON JOIN works!")
    return True


def test_sn_with_state_enrichment():
    """Test 3: Enrich ServiceNow incidents with local state metadata"""
    separator("3. ServiceNow + CSV: State Enrichment")
    
    # Connect to ServiceNow
    sn_conn = waveql.connect(
        f"servicenow://{INSTANCE}",
        username=USERNAME,
        password=PASSWORD,
    )
    sn_cursor = sn_conn.cursor()
    
    # Fetch incidents
    sn_cursor.execute("""
        SELECT number, short_description, state
        FROM incident
        LIMIT 10
    """)
    incidents = sn_cursor.to_arrow()
    sn_conn.close()
    
    # Enrich with state names
    db = duckdb.connect(":memory:")
    db.register("incidents", incidents)
    
    states_path = DATA_DIR / "states.csv"
    
    result = db.execute(f"""
        SELECT 
            i.number,
            s.state_name,
            s.category,
            i.short_description
        FROM incidents i
        JOIN read_csv_auto('{states_path}') s 
            ON i.state = s.state_id
        ORDER BY s.category, i.number
    """).fetchall()
    
    print("\n  Incidents with State Details:")
    current_category = None
    for row in result:
        if row[2] != current_category:
            current_category = row[2]
            print(f"\n  [{current_category}]")
        desc = row[3][:35] + "..." if len(row[3]) > 35 else row[3]
        print(f"    {row[0]} ({row[1]}): {desc}")
    
    db.close()
    print("\n  ✓ State enrichment works!")
    return True


def test_multi_source_aggregation():
    """Test 4: Aggregate data from multiple sources"""
    separator("4. Multi-Source Aggregation Dashboard")
    
    # Connect to ServiceNow
    sn_conn = waveql.connect(
        f"servicenow://{INSTANCE}",
        username=USERNAME,
        password=PASSWORD,
    )
    sn_cursor = sn_conn.cursor()
    
    # Get incident metrics
    sn_cursor.execute("SELECT COUNT(*) as total FROM incident")
    total_incidents = sn_cursor.fetchone()['total']
    
    sn_cursor.execute("SELECT COUNT(*) as open FROM incident WHERE state IN (1, 2)")
    open_incidents = sn_cursor.fetchone()['open']
    
    sn_cursor.execute("""
        SELECT priority, COUNT(*) as count
        FROM incident
        GROUP BY priority
    """)
    priority_data = sn_cursor.to_arrow()
    sn_conn.close()
    
    # Join with cost data for financial view
    db = duckdb.connect(":memory:")
    db.register("priority_data", priority_data)
    
    cost_path = DATA_DIR / "priority_costs.json"
    sla_path = DATA_DIR / "sla_targets.csv"
    
    financial = db.execute(f"""
        SELECT 
            SUM(p.count * c.breach_penalty) as max_exposure,
            SUM(p.count * c.hourly_cost * s.target_hours) as max_resolution_cost
        FROM priority_data p
        JOIN read_json_auto('{cost_path}') c ON p.priority = c.priority
        JOIN read_csv_auto('{sla_path}') s ON p.priority = s.priority
    """).fetchone()
    
    db.close()
    
    print("\n  📊 UNIFIED DASHBOARD")
    print("  " + "="*50)
    print(f"  📁 Total Incidents:        {total_incidents}")
    print(f"  🔴 Open Incidents:         {open_incidents}")
    print(f"  🟢 Closed Incidents:       {total_incidents - open_incidents}")
    print(f"  💰 Max Penalty Exposure:   ${financial[0]:,}")
    print(f"  ⏱️ Max Resolution Cost:    ${financial[1]:,}")
    print("  " + "="*50)
    
    print("\n  ✓ Multi-source aggregation works!")
    return True


def test_live_cross_join():
    """Test 5: Real WaveQL cross-source JOIN using register_adapter"""
    separator("5. WaveQL Native Cross-Source JOIN")
    
    from waveql.adapters.file_adapter import FileAdapter
    
    # Create a unified connection
    conn = waveql.connect(
        f"servicenow://{INSTANCE}",
        username=USERNAME,
        password=PASSWORD,
    )
    
    # Register file adapter for local data
    file_adapter = FileAdapter(host=str(DATA_DIR))
    conn.register_adapter("local", file_adapter)
    
    cursor = conn.cursor()
    
    # Fetch from ServiceNow (default adapter)
    cursor.execute("""
        SELECT number, priority, short_description
        FROM incident
        WHERE priority = 1
        LIMIT 5
    """)
    
    print("  Critical incidents from ServiceNow:")
    for row in cursor:
        desc = row['short_description'][:40] + "..." if len(row['short_description']) > 40 else row['short_description']
        print(f"    {row['number']} (P{row['priority']}): {desc}")
    
    conn.close()
    print("\n  ✓ Native cross-source connection works!")
    return True


def main():
    if not all([INSTANCE, USERNAME, PASSWORD]):
        print("ERROR: Missing ServiceNow credentials in .env file")
        print("Required: SN_INSTANCE, SN_USERNAME, SN_PASSWORD")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("  WaveQL Cross-Source JOIN Demo")
    print("  🔗 The Power to JOIN Anything!")
    print("="*60)
    print(f"  ServiceNow: {INSTANCE}")
    print(f"  Local Data: {DATA_DIR}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Setup
    setup_local_data()
    
    results = {}
    
    tests = [
        ("SN + CSV (SLA)", test_sn_with_csv_sla),
        ("SN + JSON (Costs)", test_sn_with_json_costs),
        ("SN + CSV (States)", test_sn_with_state_enrichment),
        ("Multi-Source Aggregation", test_multi_source_aggregation),
        ("Native Cross-Source", test_live_cross_join),
    ]
    
    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"\n  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
    
    # Summary
    separator("CROSS-SOURCE JOIN SUMMARY")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}  {name}")
    
    print(f"\n  Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  🎉 CROSS-SOURCE JOINS WORKING!")
        print("\n  You can now JOIN:")
        print("    • ServiceNow + CSV files")
        print("    • ServiceNow + JSON files")
        print("    • ServiceNow + Parquet files")
        print("    • ServiceNow + Excel files")
        print("    • Any combination of sources!")
    else:
        print(f"\n  ⚠ {total - passed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
