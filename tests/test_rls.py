"""
Tests for Row-Level Security (RLS) functionality.

Tests cover:
- Policy creation and management
- Predicate injection during query execution
- Multiple policies (AND/OR combinations)
- Wildcard table policies
- Operation-specific policies
- Dynamic predicates
"""

import pytest

from waveql.security.policy import (
    SecurityPolicy,
    PolicyManager,
    PolicyMode,
)


class TestSecurityPolicy:
    """Tests for the SecurityPolicy dataclass."""
    
    def test_create_simple_policy(self):
        """Test creating a basic security policy."""
        policy = SecurityPolicy(
            name="sales_only",
            table="incident",
            predicate="department = 'sales'"
        )
        
        assert policy.name == "sales_only"
        assert policy.table == "incident"
        assert policy.predicate == "department = 'sales'"
        assert policy.mode == PolicyMode.RESTRICTIVE
        assert policy.enabled is True
        assert "SELECT" in policy.operations
        assert "UPDATE" in policy.operations
        assert "DELETE" in policy.operations
    
    def test_policy_applies_to_exact_match(self):
        """Test policy applies_to with exact table match."""
        policy = SecurityPolicy(
            name="test",
            table="incident",
            predicate="x = 1"
        )
        
        assert policy.applies_to("incident", "SELECT") is True
        assert policy.applies_to("INCIDENT", "SELECT") is True  # Case insensitive
        assert policy.applies_to("other_table", "SELECT") is False
    
    def test_policy_applies_to_schema_qualified(self):
        """Test policy applies to schema-qualified table names."""
        policy = SecurityPolicy(
            name="test",
            table="incident",
            predicate="x = 1"
        )
        
        # Policy on "incident" should match "servicenow.incident"
        assert policy.applies_to("servicenow.incident", "SELECT") is True
        assert policy.applies_to("salesforce.incident", "SELECT") is True
        assert policy.applies_to("servicenow.case", "SELECT") is False
    
    def test_policy_wildcard_table(self):
        """Test wildcard policy applies to all tables."""
        policy = SecurityPolicy(
            name="tenant_isolation",
            table="*",
            predicate="org_id = 123"
        )
        
        assert policy.applies_to("incident", "SELECT") is True
        assert policy.applies_to("users", "SELECT") is True
        assert policy.applies_to("any.table", "SELECT") is True
    
    def test_policy_operation_filter(self):
        """Test policy only applies to specified operations."""
        policy = SecurityPolicy(
            name="read_only",
            table="incident",
            predicate="x = 1",
            operations={"SELECT"}
        )
        
        assert policy.applies_to("incident", "SELECT") is True
        assert policy.applies_to("incident", "UPDATE") is False
        assert policy.applies_to("incident", "DELETE") is False
        assert policy.applies_to("incident", "INSERT") is False
    
    def test_policy_disabled(self):
        """Test disabled policy doesn't apply."""
        policy = SecurityPolicy(
            name="test",
            table="incident",
            predicate="x = 1",
            enabled=False
        )
        
        assert policy.applies_to("incident", "SELECT") is False
    
    def test_invalid_predicate_raises(self):
        """Test that invalid SQL predicate raises ValueError."""
        with pytest.raises(ValueError, match="Invalid predicate syntax"):
            SecurityPolicy(
                name="bad",
                table="incident",
                predicate="THIS IS NOT SQL $%^&"
            )
    
    def test_dynamic_predicate_function(self):
        """Test dynamic predicate using a function."""
        current_user_id = 42
        
        policy = SecurityPolicy(
            name="user_data",
            table="records",
            predicate="",
            predicate_fn=lambda: f"owner_id = {current_user_id}"
        )
        
        assert policy.get_predicate() == "owner_id = 42"
        
        # Change the variable
        current_user_id = 99
        # Note: closure captures variable by reference
        # So this would still be 42 unless we use a mutable container
    
    def test_policy_hashable(self):
        """Test policies can be used in sets/dicts."""
        p1 = SecurityPolicy(name="a", table="t", predicate="x=1")
        p2 = SecurityPolicy(name="b", table="t", predicate="x=1")
        p3 = SecurityPolicy(name="a", table="t", predicate="x=2")  # Same name as p1
        
        policies = {p1, p2, p3}
        assert len(policies) == 2  # p1 and p3 have same name, so deduplicated


class TestPolicyManager:
    """Tests for PolicyManager."""
    
    def test_add_and_list_policies(self):
        """Test adding and listing policies."""
        manager = PolicyManager()
        
        manager.add_policy("incident", "status = 'active'", name="active_only")
        manager.add_policy("users", "role = 'admin'", name="admin_users")
        
        policies = manager.list_policies()
        assert len(policies) == 2
        
        names = [p.name for p in policies]
        assert "active_only" in names
        assert "admin_users" in names
    
    def test_auto_generated_name(self):
        """Test policies get auto-generated names if not provided."""
        manager = PolicyManager()
        
        p1 = manager.add_policy("t1", "x = 1")
        p2 = manager.add_policy("t2", "x = 2")
        
        assert p1.name.startswith("policy_")
        assert p2.name.startswith("policy_")
        assert p1.name != p2.name
    
    def test_remove_policy(self):
        """Test removing a policy by name."""
        manager = PolicyManager()
        
        manager.add_policy("t", "x = 1", name="test")
        assert len(manager) == 1
        
        result = manager.remove_policy("test")
        assert result is True
        assert len(manager) == 0
        
        # Removing non-existent returns False
        result = manager.remove_policy("nonexistent")
        assert result is False
    
    def test_clear_policies(self):
        """Test clearing all policies."""
        manager = PolicyManager()
        
        manager.add_policy("t1", "x = 1")
        manager.add_policy("t2", "x = 2")
        manager.add_policy("t3", "x = 3")
        
        count = manager.clear_policies()
        assert count == 3
        assert len(manager) == 0
    
    def test_list_policies_by_table(self):
        """Test filtering policies by table."""
        manager = PolicyManager()
        
        manager.add_policy("incident", "x = 1", name="p1")
        manager.add_policy("users", "x = 2", name="p2")
        manager.add_policy("*", "org = 1", name="global")
        
        incident_policies = manager.list_policies("incident")
        assert len(incident_policies) == 2  # p1 + global wildcard
        
        users_policies = manager.list_policies("users")
        assert len(users_policies) == 2  # p2 + global wildcard
    
    def test_build_combined_predicate_restrictive(self):
        """Test combining restrictive policies with AND."""
        manager = PolicyManager()
        
        manager.add_policy("t", "a = 1", mode="restrictive")
        manager.add_policy("t", "b = 2", mode="restrictive")
        
        combined = manager.build_combined_predicate("t", "SELECT")
        
        # Both predicates should be ANDed
        assert "(a = 1)" in combined
        assert "(b = 2)" in combined
        assert " AND " in combined
    
    def test_build_combined_predicate_permissive(self):
        """Test combining permissive policies with OR."""
        manager = PolicyManager()
        
        manager.add_policy("t", "role = 'admin'", mode="permissive")
        manager.add_policy("t", "role = 'superuser'", mode="permissive")
        
        combined = manager.build_combined_predicate("t", "SELECT")
        
        # Permissive policies should be ORed
        assert "(role = 'admin')" in combined
        assert "(role = 'superuser')" in combined
        assert " OR " in combined
    
    def test_build_combined_predicate_mixed(self):
        """Test combining permissive and restrictive policies."""
        manager = PolicyManager()
        
        # Permissive: user is admin OR superuser
        manager.add_policy("t", "role = 'admin'", mode="permissive")
        manager.add_policy("t", "role = 'super'", mode="permissive")
        
        # Restrictive: must be active
        manager.add_policy("t", "active = true", mode="restrictive")
        
        combined = manager.build_combined_predicate("t", "SELECT")
        
        # Result should be: (permissive OR permissive) AND restrictive
        assert "(active = true)" in combined
        assert "OR" in combined
        assert "AND" in combined
    
    def test_no_policies_returns_none(self):
        """Test that no applicable policies returns None."""
        manager = PolicyManager()
        
        manager.add_policy("other_table", "x = 1")
        
        combined = manager.build_combined_predicate("incident", "SELECT")
        assert combined is None
    
    def test_apply_policies_to_sql_no_where(self):
        """Test applying policies to SQL without existing WHERE."""
        manager = PolicyManager()
        manager.add_policy("incident", "department = 'sales'")
        
        original = "SELECT * FROM incident"
        rewritten = manager.apply_policies(original, "incident", "SELECT")
        
        assert "WHERE" in rewritten.upper()
        assert "department" in rewritten
    
    def test_apply_policies_to_sql_with_where(self):
        """Test applying policies to SQL with existing WHERE."""
        manager = PolicyManager()
        manager.add_policy("incident", "department = 'sales'")
        
        original = "SELECT * FROM incident WHERE status = 'open'"
        rewritten = manager.apply_policies(original, "incident", "SELECT")
        
        # Should have both conditions
        assert "status" in rewritten
        assert "department" in rewritten
        assert "AND" in rewritten.upper()


class TestRLSIntegration:
    """Integration tests for RLS with WaveQLConnection."""
    
    def test_connection_has_policy_manager(self):
        """Test that connection has a policy manager."""
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        
        assert hasattr(conn, '_policy_manager')
        assert hasattr(conn, 'add_policy')
        assert hasattr(conn, 'remove_policy')
        assert hasattr(conn, 'list_policies')
        assert hasattr(conn, 'clear_policies')
        
        conn.close()
    
    def test_add_policy_via_connection(self):
        """Test adding policies through connection API."""
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        
        policy = conn.add_policy(
            table="incident",
            predicate="department = 'sales'",
            name="sales_restriction"
        )
        
        assert policy.name == "sales_restriction"
        assert len(conn.list_policies()) == 1
        
        conn.clear_policies()
        assert len(conn.list_policies()) == 0
        
        conn.close()
    
    def test_rls_predicate_injection(self):
        """Test that RLS predicates are injected into query_info."""
        from waveql.connection import WaveQLConnection
        from waveql.cursor import WaveQLCursor
        from waveql.query_planner import QueryPlanner
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        # Add RLS policy
        conn.add_policy("data", "org_id = 42", name="tenant_isolation")
        
        # Parse a query and apply RLS
        planner = QueryPlanner()
        query_info = planner.parse("SELECT * FROM test.data WHERE status = 'active'")
        
        # Apply RLS policies
        query_info = cursor._apply_rls_policies(query_info, "SELECT * FROM test.data WHERE status = 'active'")
        
        # Check that policy predicate was injected
        predicate_columns = [p.column for p in query_info.predicates]
        assert "org_id" in predicate_columns, f"Expected org_id in {predicate_columns}"
        # Original predicate should also be there
        assert "status" in predicate_columns
        
        conn.close()
    
    def test_rls_multiple_policies(self):
        """Test that multiple policies are all applied."""
        from waveql.connection import WaveQLConnection
        from waveql.cursor import WaveQLCursor
        from waveql.query_planner import QueryPlanner
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        # Add multiple policies
        conn.add_policy("data", "org_id = 1")
        conn.add_policy("data", "active = true")
        conn.add_policy("data", "deleted = false")
        
        # Parse and apply RLS
        planner = QueryPlanner()
        query_info = planner.parse("SELECT * FROM test.data")
        query_info = cursor._apply_rls_policies(query_info, "SELECT * FROM test.data")
        
        # All three policy predicates should be injected
        predicate_columns = [p.column for p in query_info.predicates]
        assert "org_id" in predicate_columns
        assert "active" in predicate_columns
        assert "deleted" in predicate_columns
        
        conn.close()
    
    def test_rls_wildcard_policy(self):
        """Test that wildcard policies apply to all tables."""
        from waveql.connection import WaveQLConnection
        from waveql.cursor import WaveQLCursor
        from waveql.query_planner import QueryPlanner
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        # Add global wildcard policy
        conn.add_policy("*", "tenant_id = 999", name="global_tenant")
        
        planner = QueryPlanner()
        
        # Test on different tables
        for table in ["users", "orders", "products"]:
            query_info = planner.parse(f"SELECT * FROM test.{table}")
            query_info = cursor._apply_rls_policies(query_info, f"SELECT * FROM test.{table}")
            
            predicate_columns = [p.column for p in query_info.predicates]
            assert "tenant_id" in predicate_columns, f"Expected tenant_id for {table}, got {predicate_columns}"
        
        conn.close()
    
    def test_rls_operation_specific(self):
        """Test that operation-specific policies only apply to those operations."""
        from waveql.connection import WaveQLConnection
        from waveql.cursor import WaveQLCursor
        from waveql.query_planner import QueryPlanner
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        # Add SELECT-only policy
        conn.add_policy(
            "data",
            "visible = true",
            operations={"SELECT"},
            name="visibility_filter"
        )
        
        planner = QueryPlanner()
        
        # SELECT should have the policy
        query_info = planner.parse("SELECT * FROM test.data")
        query_info = cursor._apply_rls_policies(query_info, "SELECT * FROM test.data")
        columns = [p.column for p in query_info.predicates]
        assert "visible" in columns
        
        # UPDATE should NOT have the policy (operation not in policy)
        query_info2 = planner.parse("UPDATE test.data SET x = 1 WHERE id = 5")
        query_info2 = cursor._apply_rls_policies(query_info2, "UPDATE test.data SET x = 1 WHERE id = 5")
        columns2 = [p.column for p in query_info2.predicates]
        assert "visible" not in columns2
        
        conn.close()
    
    def test_rls_no_policy_for_different_table(self):
        """Test that policies don't apply to unrelated tables."""
        from waveql.connection import WaveQLConnection
        from waveql.cursor import WaveQLCursor
        from waveql.query_planner import QueryPlanner
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        # Add policy only for 'users' table
        conn.add_policy("users", "role = 'admin'")
        
        planner = QueryPlanner()
        
        # Query to 'orders' should not have the policy
        query_info = planner.parse("SELECT * FROM test.orders")
        query_info = cursor._apply_rls_policies(query_info, "SELECT * FROM test.orders")
        
        columns = [p.column for p in query_info.predicates]
        assert "role" not in columns
        
        conn.close()


class TestPredicateParsing:
    """Tests for predicate parsing from policy SQL."""
    
    def test_parse_equality(self):
        """Test parsing simple equality predicates."""
        from waveql.cursor import WaveQLCursor
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        predicates = cursor._parse_policy_predicate("status = 'active'")
        
        assert len(predicates) == 1
        assert predicates[0].column == "status"
        assert predicates[0].operator == "="
        assert predicates[0].value == "active"
        
        conn.close()
    
    def test_parse_numeric(self):
        """Test parsing numeric predicates."""
        from waveql.cursor import WaveQLCursor
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        predicates = cursor._parse_policy_predicate("priority > 5")
        
        assert len(predicates) == 1
        assert predicates[0].column == "priority"
        assert predicates[0].operator == ">"
        assert predicates[0].value == 5
        
        conn.close()
    
    def test_parse_in_clause(self):
        """Test parsing IN predicates."""
        from waveql.cursor import WaveQLCursor
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        predicates = cursor._parse_policy_predicate("status IN ('a', 'b', 'c')")
        
        assert len(predicates) == 1
        assert predicates[0].column == "status"
        assert predicates[0].operator == "IN"
        assert predicates[0].value == ["a", "b", "c"]
        
        conn.close()
    
    def test_parse_and_condition(self):
        """Test parsing AND conditions."""
        from waveql.cursor import WaveQLCursor
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        predicates = cursor._parse_policy_predicate("a = 1 AND b = 2")
        
        assert len(predicates) == 2
        columns = [p.column for p in predicates]
        assert "a" in columns
        assert "b" in columns
        
        conn.close()
    
    def test_parse_complex_fallback_to_raw(self):
        """Test that complex predicates fall back to RAW."""
        from waveql.cursor import WaveQLCursor
        from waveql.connection import WaveQLConnection
        
        conn = WaveQLConnection()
        cursor = WaveQLCursor(conn)
        
        # OR conditions should fall back to RAW
        predicates = cursor._parse_policy_predicate("a = 1 OR b = 2")
        
        # Should have a RAW predicate
        raw_preds = [p for p in predicates if p.operator == "RAW"]
        assert len(raw_preds) >= 1
        
        conn.close()
