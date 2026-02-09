"""
WaveQL Semantic Layer Demo

This script demonstrates how to use:
1. Virtual Views to organized complex logic
2. Saved Queries for parameterized tools
3. dbt Integration to reuse business logic
"""

import os
import json
import logging
from waveql import connect
from waveql.semantic import (
    VirtualView, 
    VirtualViewRegistry, 
    SavedQuery, 
    DbtManifest
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("demo")

def demo_virtual_views(conn):
    logger.info("--- 1. Virtual Views Demo ---")
    
    # Define a view for "Closed Won" opportunities
    # Notice we can assume a 'salesforce' schema exists or mock it
    # We use the MockAdapter to provide the 'opportunities' table data

    registry = VirtualViewRegistry()

    # 1. Base View: Filter raw data
    registry.register(VirtualView(
        name="won_opportunities",
        sql="SELECT * FROM opportunities WHERE stage = 'Closed Won'",
        description="Only opportunities that we won"
    ))

    # 2. Derived View: Build on top of the previous view
    # This is the power of views: Chaining logic!
    registry.register(VirtualView(
        name="high_value_wins",
        sql="SELECT * FROM won_opportunities WHERE amount > 1000",
        dependencies=["won_opportunities"],
        description="Big wins only"
    ))

    # Register them
    conn.register_views(registry)
    
    cursor = conn.cursor()

    # debug: check raw table
    raw_count = cursor.execute("SELECT COUNT(*) FROM opportunities").fetchone()
    logger.info(f"Raw opportunities count: {raw_count[0] if raw_count else 'None'}")

    # Query the final derived view
    cursor.execute("SELECT * FROM high_value_wins")
    result = cursor.fetchall()
    logger.info(f"High Value Wins: {result}")
    
    # Verify dependency handling
    views = conn.list_views()
    logger.info(f"Registered Views: {views}")


def demo_saved_queries(conn):
    logger.info("\n--- 2. Saved Queries Demo ---")

    # Imagine a chatbot or internal tool needs this query
    # We define strict types for safety
    find_opp_query = SavedQuery(
        name="find_opp_by_amount",
        sql="SELECT * FROM opportunities WHERE amount >= :min_amount AND stage = :stage",
        parameters={
            "min_amount": {
                "type": "int", 
                "default": 0,
                "description": "Minimum deal size"
            },
            "stage": {
                "type": "str", 
                "choices": ["Closed Won", "Negotiation", "Prospecting"],
                "required": True
            }
        },
        description="Find opportunities matching criteria"
    )

    # Execute efficiently without string concatenation risks
    cursor = conn.execute_saved(
        find_opp_query, 
        min_amount=100, 
        stage="Negotiation"
    )
    
    logger.info(f"Found Negotiation Opps: {cursor.fetchall()}")

    # Demonstrate validation error
    try:
        conn.execute_saved(find_opp_query, stage="Invalid Stage")
    except ValueError as e:
        logger.info(f"Caught expected validation error: {e}")


def demo_dbt_integration(conn):
    logger.info("\n--- 3. dbt Integration Demo ---")

    # Mock a dbt manifest.json structure
    # In reality, you would load this from: target/manifest.json
    mock_manifest = {
        "metadata": {"dbt_version": "1.7.0", "project_name": "my_analytics"},
        "nodes": {
            "model.my_analytics.fct_sales": {
                "name": "fct_sales",
                "resource_type": "model",
                "compiled_sql": "SELECT id, amount FROM opportunities WHERE stage = 'Closed Won'",
                "depends_on": {"nodes": []}, 
                "tags": ["finance"],
                "config": {"materialized": "table"}
            }
        },
        "sources": {}
    }

    # Create a temporary manifest file
    with open("mock_manifest.json", "w") as f:
        json.dump(mock_manifest, f)

    try:
        # Load the manifest
        # WaveQL converts the dbt model 'fct_sales' into a queryable view!
        manifest = DbtManifest.from_file("mock_manifest.json")
        conn.register_dbt_models(manifest)

        # Now we can query the dbt model as if it were a table
        cursor = conn.cursor()
        cursor.execute("SELECT AVG(amount) FROM fct_sales")
        result = cursor.fetchone()
        
        avg_val = result[0] if result else 0.0
        logger.info(f"Average Sales (from dbt logic): {avg_val}")
        
    finally:
        if os.path.exists("mock_manifest.json"):
            os.remove("mock_manifest.json")

def main():
    # Register a mock adapter for the demo
    from waveql.adapters import BaseAdapter, register_adapter
    
    class MockAdapter(BaseAdapter):
        def _get_table_schema(self, table_name):
            return {}
            
        def get_schema(self, table):
            return []
            
        def fetch(self, table, columns=None, predicates=None, limit=None, offset=None, 
                 order_by=None, group_by=None, aggregates=None):
            import pyarrow as pa
            
            # Return sample data for opportunities table
            if table == "opportunities":
                return pa.Table.from_pydict({
                    "id": [1, 2, 3],
                    "amount": [5000, 200, 10000],
                    "stage": ["Closed Won", "Negotiation", "Closed Won"]
                })
                
            return pa.Table.from_pydict({})

    register_adapter("mock", MockAdapter)
    
    # Connect using the mock adapter
    conn = connect(adapter="mock")
    
    demo_virtual_views(conn)
    demo_saved_queries(conn)
    demo_dbt_integration(conn)

if __name__ == "__main__":
    main()
