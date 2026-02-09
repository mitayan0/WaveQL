#!/usr/bin/env python
"""
WaveQL Complex Predicate Demo

Demonstrates the new v0.1.5 features:
1. Complex OR predicate extraction and optimization
2. Automatic OR-to-IN conversion for pushdown
3. Subquery analysis

This example shows how WaveQL now handles complex WHERE clauses
that were previously not pushable to APIs.
"""

from waveql.query_planner import QueryPlanner
from waveql.optimizer import QueryOptimizer, SubqueryPushdownOptimizer


def demo_or_to_in_conversion():
    """Demonstrate automatic OR to IN conversion."""
    print("=" * 60)
    print("Demo 1: OR to IN Conversion")
    print("=" * 60)
    print()
    
    planner = QueryPlanner()
    
    # Example 1: Simple OR on same column
    sql1 = "SELECT * FROM incident WHERE status = 'open' OR status = 'closed'"
    info1 = planner.parse(sql1)
    
    print("SQL:", sql1)
    print("Predicates extracted:")
    for pred in info1.predicates:
        print(f"  - {pred.column} {pred.operator} {pred.value}")
    print()
    
    # Example 2: Triple OR
    sql2 = "SELECT * FROM incident WHERE priority = 1 OR priority = 2 OR priority = 3"
    info2 = planner.parse(sql2)
    
    print("SQL:", sql2)
    print("Predicates extracted:")
    for pred in info2.predicates:
        print(f"  - {pred.column} {pred.operator} {pred.value}")
    print()
    
    # Example 3: OR combined with AND
    sql3 = """
        SELECT * FROM incident 
        WHERE (status = 'open' OR status = 'pending') 
        AND priority > 2
    """
    info3 = planner.parse(sql3)
    
    print("SQL:", sql3.strip())
    print("Predicates extracted:")
    for pred in info3.predicates:
        print(f"  - {pred.column} {pred.operator} {pred.value}")
    print()


def demo_servicenow_filter_generation():
    """Demonstrate API-specific filter generation."""
    print("=" * 60)
    print("Demo 2: ServiceNow Filter Generation")
    print("=" * 60)
    print()
    
    from waveql.optimizer import CompoundPredicate, PredicateType
    
    # Create an IN predicate
    in_pred = CompoundPredicate(
        type=PredicateType.IN_LIST,
        column="state",
        values=[1, 2, 3, 6]  # New, In Progress, On Hold, Resolved
    )
    
    print("Predicate: state IN (1, 2, 3, 6)")
    print(f"ServiceNow filter: {in_pred.to_api_filter('servicenow')}")
    print(f"Salesforce filter: {in_pred.to_api_filter('salesforce')}")
    print(f"Jira filter: {in_pred.to_api_filter('jira')}")
    print()


def demo_subquery_analysis():
    """Demonstrate subquery pushdown analysis."""
    print("=" * 60)
    print("Demo 3: Subquery Pushdown Analysis")
    print("=" * 60)
    print()
    
    from waveql.optimizer import SubqueryInfo
    from waveql.query_planner import QueryInfo, Predicate
    
    optimizer = SubqueryPushdownOptimizer()
    
    # Scenario 1: Same adapter subquery (can push)
    outer_query = QueryInfo(operation="SELECT", table="servicenow.incident")
    inner_subquery = SubqueryInfo(
        sql="SELECT sys_id FROM servicenow.sys_user WHERE active = true",
        column="assigned_to",
        operator="IN",
        inner_table="servicenow.sys_user",
        inner_columns=["sys_id"],
        inner_predicates=[Predicate("active", "=", True)]
    )
    
    adapters = {"servicenow": "servicenow"}
    result1 = optimizer.analyze_subquery(outer_query, inner_subquery, adapters)
    
    print("Scenario: Both tables on ServiceNow")
    print(f"  - Can push down: {result1['can_push']}")
    print(f"  - Strategy: {result1['strategy']}")
    print(f"  - Reason: {result1['reason']}")
    print()
    
    # Scenario 2: Cross-adapter subquery (cannot push)
    outer_query2 = QueryInfo(operation="SELECT", table="servicenow.incident")
    inner_subquery2 = SubqueryInfo(
        sql="SELECT Id FROM salesforce.Contact WHERE Active__c = true",
        column="customer_id",
        operator="IN",
        inner_table="salesforce.Contact",
        inner_columns=["Id"],
        inner_predicates=[Predicate("Active__c", "=", True)]
    )
    
    adapters2 = {"servicenow": "servicenow", "salesforce": "salesforce"}
    result2 = optimizer.analyze_subquery(outer_query2, inner_subquery2, adapters2)
    
    print("Scenario: Outer on ServiceNow, Inner on Salesforce")
    print(f"  - Can push down: {result2['can_push']}")
    print(f"  - Strategy: {result2['strategy']}")
    print(f"  - Reason: {result2['reason']}")
    print()


def demo_adapter_capabilities():
    """Demonstrate adapter capability detection."""
    print("=" * 60)
    print("Demo 4: Adapter Capabilities")
    print("=" * 60)
    print()
    
    optimizer = QueryOptimizer()
    
    adapters = ["servicenow", "salesforce", "jira", "rest"]
    
    for adapter in adapters:
        caps = optimizer.get_adapter_capabilities(adapter)
        print(f"{adapter.upper()}:")
        print(f"  - Supports OR: {caps.get('supports_or', False)}")
        print(f"  - Supports IN: {caps.get('supports_in', True)}")
        print(f"  - Max IN values: {caps.get('max_in_values', 'unlimited')}")
        print()


def demo_complex_query():
    """Demonstrate a complex real-world query."""
    print("=" * 60)
    print("Demo 5: Real-World Complex Query")
    print("=" * 60)
    print()
    
    planner = QueryPlanner()
    
    sql = """
        SELECT 
            number, short_description, priority, state, assigned_to
        FROM servicenow.incident 
        WHERE (state = 1 OR state = 2 OR state = 3)
        AND (category = 'software' OR category = 'hardware' OR category = 'network')
        AND priority IN (1, 2)
        AND active = true
        ORDER BY sys_created_on DESC
        LIMIT 100
    """
    
    info = planner.parse(sql)
    
    print("SQL:")
    print(sql.strip())
    print()
    print("Parsed Query Info:")
    print(f"  - Operation: {info.operation}")
    print(f"  - Table: {info.table}")
    print(f"  - Columns: {info.columns}")
    print(f"  - Limit: {info.limit}")
    print()
    print("Predicates (all pushable!):")
    for pred in info.predicates:
        print(f"  - {pred.column} {pred.operator} {pred.value}")
    print()
    
    # Show ServiceNow query construction
    print("ServiceNow API Query:")
    sn_parts = []
    for pred in info.predicates:
        sn_parts.append(pred.to_api_filter("servicenow"))
    print(f"  sysparm_query={'^'.join(sn_parts)}")
    print()


def main():
    print("\n" + "=" * 60)
    print("WaveQL v0.1.5 - Complex Predicate Demo")
    print("=" * 60 + "\n")
    
    demo_or_to_in_conversion()
    demo_servicenow_filter_generation()
    demo_subquery_analysis()
    demo_adapter_capabilities()
    demo_complex_query()
    
    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
