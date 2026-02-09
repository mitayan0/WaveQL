"""
Tests for Query Optimizer - Complex Predicate Extraction and Subquery Pushdown
"""

from waveql.query_planner import QueryPlanner, Predicate
from waveql.optimizer import (
    QueryOptimizer,
    CompoundPredicate,
    PredicateType,
    SubqueryInfo,
    SubqueryPushdownOptimizer,
)


class TestComplexPredicateExtraction:
    """Tests for complex OR predicate extraction and optimization."""
    
    def setup_method(self):
        self.planner = QueryPlanner()
        self.optimizer = QueryOptimizer()
    
    def test_simple_or_to_in_conversion(self):
        """Test converting OR with same column equality to IN."""
        sql = "SELECT * FROM incident WHERE status = 'open' OR status = 'closed'"
        info = self.planner.parse(sql)
        
        # Should have one predicate with IN operator
        assert len(info.predicates) == 1
        pred = info.predicates[0]
        assert pred.column == "status"
        assert pred.operator == "IN"
        assert set(pred.value) == {"open", "closed"}
    
    def test_triple_or_to_in_conversion(self):
        """Test converting three OR conditions to IN."""
        sql = "SELECT * FROM incident WHERE priority = 1 OR priority = 2 OR priority = 3"
        info = self.planner.parse(sql)
        
        assert len(info.predicates) == 1
        pred = info.predicates[0]
        assert pred.column == "priority"
        assert pred.operator == "IN"
        assert set(pred.value) == {1, 2, 3}
    
    def test_or_with_and(self):
        """Test OR groups combined with AND."""
        sql = """
        SELECT * FROM incident 
        WHERE (status = 'open' OR status = 'pending') 
        AND priority > 3
        """
        info = self.planner.parse(sql)
        
        # Should have two predicates: IN for status, > for priority
        assert len(info.predicates) == 2
        
        status_pred = next((p for p in info.predicates if p.column == "status"), None)
        priority_pred = next((p for p in info.predicates if p.column == "priority"), None)
        
        assert status_pred is not None
        assert status_pred.operator == "IN"
        assert set(status_pred.value) == {"open", "pending"}
        
        assert priority_pred is not None
        assert priority_pred.operator == ">"
        assert priority_pred.value == 3
    
    def test_mixed_operators_in_or_not_converted(self):
        """Test that mixed operators in OR are not converted."""
        sql = "SELECT * FROM incident WHERE status = 'open' OR priority > 3"
        info = self.planner.parse(sql)
        
        # Mixed operators can't be converted - predicates should be empty or partial
        # The OR with different columns cannot be converted to IN
        assert len(info.predicates) == 0  # Can't push down mixed OR
    
    def test_different_columns_in_or_not_converted(self):
        """Test that OR on different columns is not converted."""
        sql = "SELECT * FROM incident WHERE status = 'open' OR category = 'bug'"
        info = self.planner.parse(sql)
        
        # Different columns can't be converted to IN
        assert len(info.predicates) == 0
    
    def test_nested_or_flattening(self):
        """Test flattening of nested OR expressions."""
        sql = """
        SELECT * FROM incident 
        WHERE status = 'a' OR status = 'b' OR status = 'c' OR status = 'd'
        """
        info = self.planner.parse(sql)
        
        assert len(info.predicates) == 1
        pred = info.predicates[0]
        assert pred.operator == "IN"
        assert set(pred.value) == {"a", "b", "c", "d"}
    
    def test_in_predicate_passthrough(self):
        """Test that explicit IN predicates work correctly."""
        sql = "SELECT * FROM incident WHERE status IN ('open', 'closed', 'pending')"
        info = self.planner.parse(sql)
        
        assert len(info.predicates) == 1
        pred = info.predicates[0]
        assert pred.column == "status"
        assert pred.operator == "IN"
        assert set(pred.value) == {"open", "closed", "pending"}
    
    def test_parenthesized_conditions(self):
        """Test handling of parenthesized conditions."""
        sql = """
        SELECT * FROM incident 
        WHERE (category = 'bug') AND (status = 'open' OR status = 'pending')
        """
        info = self.planner.parse(sql)
        
        assert len(info.predicates) == 2
        
        category_pred = next((p for p in info.predicates if p.column == "category"), None)
        status_pred = next((p for p in info.predicates if p.column == "status"), None)
        
        assert category_pred is not None
        assert category_pred.operator == "="
        
        assert status_pred is not None
        assert status_pred.operator == "IN"


class TestCompoundPredicate:
    """Tests for CompoundPredicate class."""
    
    def test_simple_predicate_creation(self):
        """Test creating a simple compound predicate."""
        pred = Predicate(column="status", operator="=", value="open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        assert compound.type == PredicateType.SIMPLE
        assert len(compound.predicates) == 1
    
    def test_or_group_creation(self):
        """Test creating an OR group compound predicate."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        assert compound.type == PredicateType.OR_GROUP
        assert compound.column == "status"
        assert len(compound.predicates) == 2
    
    def test_or_to_in_conversion(self):
        """Test converting OR group to IN predicate."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        simple_preds = compound.to_simple_predicates()
        
        assert len(simple_preds) == 1
        assert simple_preds[0].column == "status"
        assert simple_preds[0].operator == "IN"
        assert set(simple_preds[0].value) == {"open", "closed"}
    
    def test_can_push_down_simple(self):
        """Test pushdown capability for simple predicates."""
        pred = Predicate(column="status", operator="=", value="open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        assert compound.can_push_down() is True
    
    def test_can_push_down_or_with_supports_in(self):
        """Test pushdown capability for OR groups with IN support."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        capabilities = {"supports_in": True, "supports_or": False}
        assert compound.can_push_down(capabilities) is True
    
    def test_servicenow_filter_generation(self):
        """Test ServiceNow filter string generation."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        filter_str = compound.to_api_filter("servicenow")
        assert filter_str is not None
        # ServiceNow uses ^OR for OR conditions
        assert "^OR" in filter_str or "IN" in filter_str
    
    def test_in_list_filter_generation(self):
        """Test IN list filter generation."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="status",
            values=["open", "closed", "pending"]
        )
        
        # ServiceNow format
        sn_filter = compound.to_api_filter("servicenow")
        assert sn_filter is not None
        assert "status" in sn_filter
        assert "IN" in sn_filter
        
        # Salesforce format
        sf_filter = compound.to_api_filter("salesforce")
        assert sf_filter is not None
        assert "IN" in sf_filter


class TestQueryOptimizer:
    """Tests for the QueryOptimizer class."""
    
    def setup_method(self):
        self.optimizer = QueryOptimizer()
    
    def test_get_servicenow_capabilities(self):
        """Test getting ServiceNow adapter capabilities."""
        caps = self.optimizer.get_adapter_capabilities("servicenow")
        
        assert caps["supports_or"] is True
        assert caps["supports_in"] is True
        assert caps["max_in_values"] == 500
    
    def test_get_jira_capabilities(self):
        """Test getting Jira adapter capabilities."""
        caps = self.optimizer.get_adapter_capabilities("jira")
        
        assert caps["supports_or"] is True
        assert caps["supports_in"] is True
        assert caps["max_in_values"] == 100
    
    def test_get_unknown_adapter_capabilities(self):
        """Test getting capabilities for unknown adapter."""
        caps = self.optimizer.get_adapter_capabilities("unknown_adapter")
        
        # Should return defaults
        assert caps["supports_or"] is False
        assert caps["supports_in"] is True
    
    def test_convert_or_to_in(self):
        """Test batch conversion of OR groups to IN predicates."""
        pred1 = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="status", operator="=", value="open")]
        )
        pred2 = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="status", operator="=", value="closed")]
        )
        
        or_group = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        result = self.optimizer.convert_or_to_in([or_group])
        
        assert len(result) == 1
        assert result[0].operator == "IN"
        assert set(result[0].value) == {"open", "closed"}


class TestSubqueryPushdown:
    """Tests for subquery pushdown optimization."""
    
    def setup_method(self):
        self.optimizer = SubqueryPushdownOptimizer()
        self.planner = QueryPlanner()
    
    def test_subquery_info_creation(self):
        """Test SubqueryInfo dataclass creation."""
        sq = SubqueryInfo(
            sql="SELECT id FROM users WHERE active = true",
            column="user_id",
            operator="IN",
            inner_table="users",
            inner_columns=["id"],
            inner_predicates=[Predicate("active", "=", True)]
        )
        
        assert sq.column == "user_id"
        assert sq.operator == "IN"
        assert sq.inner_table == "users"
        assert len(sq.inner_predicates) == 1
    
    def test_same_adapter_subquery_analysis(self):
        """Test subquery analysis when both tables are on same adapter."""
        from waveql.query_planner import QueryInfo
        
        outer_query = QueryInfo(
            operation="SELECT",
            table="servicenow.incident"
        )
        
        subquery = SubqueryInfo(
            sql="SELECT sys_id FROM servicenow.users WHERE active = true",
            column="assigned_to",
            operator="IN",
            inner_table="servicenow.users",
            inner_columns=["sys_id"],
            inner_predicates=[Predicate("active", "=", True)]
        )
        
        adapters = {"servicenow": "servicenow"}
        result = self.optimizer.analyze_subquery(outer_query, subquery, adapters)
        
        assert result["can_push"] is True
        assert result["strategy"] == "push_entire"
    
    def test_cross_adapter_subquery_analysis(self):
        """Test subquery analysis when tables are on different adapters."""
        from waveql.query_planner import QueryInfo
        
        outer_query = QueryInfo(
            operation="SELECT",
            table="servicenow.incident"
        )
        
        subquery = SubqueryInfo(
            sql="SELECT id FROM salesforce.users WHERE active = true",
            column="assigned_to",
            operator="IN",
            inner_table="salesforce.users",
            inner_columns=["id"],
            inner_predicates=[Predicate("active", "=", True)]
        )
        
        adapters = {"servicenow": "servicenow", "salesforce": "salesforce"}
        result = self.optimizer.analyze_subquery(outer_query, subquery, adapters)
        
        assert result["can_push"] is False
        assert result["strategy"] == "materialize_inner"


class TestIntegration:
    """Integration tests for optimizer with query planner."""
    
    def setup_method(self):
        self.planner = QueryPlanner()
        self.optimizer = QueryOptimizer()
    
    def test_complex_query_optimization(self):
        """Test optimization of a complex query with multiple OR groups."""
        sql = """
        SELECT * FROM servicenow.incident 
        WHERE (status = 'open' OR status = 'pending' OR status = 'active')
        AND priority IN (1, 2, 3)
        AND (category = 'bug' OR category = 'feature')
        """
        info = self.planner.parse(sql)
        
        # All predicates should be pushable
        assert len(info.predicates) == 3
        
        # Check conversions
        status_pred = next((p for p in info.predicates if p.column == "status"), None)
        priority_pred = next((p for p in info.predicates if p.column == "priority"), None)
        category_pred = next((p for p in info.predicates if p.column == "category"), None)
        
        assert status_pred is not None
        assert status_pred.operator == "IN"
        assert len(status_pred.value) == 3
        
        assert priority_pred is not None
        assert priority_pred.operator == "IN"
        
        assert category_pred is not None
        assert category_pred.operator == "IN"
    
    def test_predicate_api_filter_generation(self):
        """Test generating API filters from predicates."""
        sql = "SELECT * FROM incident WHERE status = 'open' OR status = 'closed'"
        info = self.planner.parse(sql)
        
        pred = info.predicates[0]
        
        # Test ServiceNow format
        sn_filter = pred.to_api_filter("servicenow")
        assert "status" in sn_filter
        assert "IN" in sn_filter or "open" in sn_filter
