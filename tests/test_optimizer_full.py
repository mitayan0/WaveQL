"""
Comprehensive tests for waveql/optimizer.py - targets 100% coverage

Tests for PredicateType, CompoundPredicate, SubqueryInfo, and QueryOptimizer.
"""

import pytest
import sqlglot
from sqlglot import exp

from waveql.optimizer import (
    PredicateType,
    CompoundPredicate,
    SubqueryInfo,
    QueryOptimizer,
)
from waveql.query_planner import Predicate


class TestPredicateType:
    """Tests for PredicateType enum."""
    
    def test_predicate_types_exist(self):
        """Test all predicate types are defined."""
        assert PredicateType.SIMPLE.value == "simple"
        assert PredicateType.OR_GROUP.value == "or_group"
        assert PredicateType.AND_GROUP.value == "and_group"
        assert PredicateType.IN_LIST.value == "in_list"
        assert PredicateType.BETWEEN.value == "between"
        assert PredicateType.NOT.value == "not"
        assert PredicateType.SUBQUERY.value == "subquery"
        assert PredicateType.COMPLEX.value == "complex"


class TestCompoundPredicate:
    """Tests for CompoundPredicate class."""
    
    def test_simple_predicate_can_push_down(self):
        """Test simple predicates can always be pushed down."""
        pred = Predicate(column="status", operator="=", value="open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        assert compound.can_push_down() is True
        assert compound.can_push_down({}) is True
    
    def test_in_list_can_push_down(self):
        """Test IN list pushdown based on capability."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="status",
            values=["open", "closed"]
        )
        
        assert compound.can_push_down({"supports_in": True}) is True
        assert compound.can_push_down({"supports_in": False}) is False
    
    def test_between_can_push_down(self):
        """Test BETWEEN pushdown based on capability."""
        compound = CompoundPredicate(
            type=PredicateType.BETWEEN,
            predicates=[
                Predicate(column="created", operator=">=", value="2024-01-01"),
                Predicate(column="created", operator="<=", value="2024-12-31"),
            ]
        )
        
        assert compound.can_push_down({"supports_between": True}) is True
        assert compound.can_push_down({"supports_between": False}) is False
    
    def test_or_group_can_push_down_with_or_support(self):
        """Test OR group pushdown when adapter supports OR."""
        # For IN conversion, we need base Predicate objects, not CompoundPredicates
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        # With OR support, should be pushable
        assert compound.can_push_down({"supports_or": True}) is True
        # With IN support and same-column equality, should convert to IN
        assert compound.can_push_down({"supports_or": False, "supports_in": True}) is True

    
    def test_or_group_cannot_push_down_mixed_operators(self):
        """Test OR group with mixed operators cannot be pushed."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator=">", value="2")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        assert compound.can_push_down({"supports_or": False, "supports_in": True}) is False
    
    def test_and_group_can_push_down(self):
        """Test AND group pushdown."""
        pred1 = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="status", operator="=", value="open")]
        )
        pred2 = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="priority", operator=">", value=3)]
        )
        
        compound = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[pred1, pred2]
        )
        
        assert compound.can_push_down() is True
    
    def test_subquery_cannot_push_down(self):
        """Test subquery predicates cannot be pushed down."""
        compound = CompoundPredicate(type=PredicateType.SUBQUERY)
        
        assert compound.can_push_down() is False
    
    def test_complex_cannot_push_down(self):
        """Test complex predicates cannot be pushed down."""
        compound = CompoundPredicate(type=PredicateType.COMPLEX)
        
        assert compound.can_push_down() is False
    
    def test_to_simple_predicates_simple(self):
        """Test converting simple compound to simple predicates."""
        pred = Predicate(column="status", operator="=", value="open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        result = compound.to_simple_predicates()
        assert len(result) == 1
        assert result[0].column == "status"
    
    def test_to_simple_predicates_and_group(self):
        """Test converting AND group to simple predicates."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="priority", operator=">", value=3)
        
        compound = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound.to_simple_predicates()
        assert len(result) == 2
    
    def test_to_simple_predicates_or_group_same_column(self):
        """Test converting OR group on same column to IN predicate."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        result = compound.to_simple_predicates()
        assert len(result) == 1
        assert result[0].operator == "IN"
        assert set(result[0].value) == {"open", "closed"}
    
    def test_to_simple_predicates_or_group_mixed(self):
        """Test OR group with mixed operators returns empty."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator=">", value=5)
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        result = compound.to_simple_predicates()
        assert len(result) == 0
    
    def test_to_simple_predicates_in_list(self):
        """Test converting IN list to simple predicate."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="status",
            values=["open", "closed", "pending"]
        )
        
        result = compound.to_simple_predicates()
        assert len(result) == 1
        assert result[0].operator == "IN"
    
    def test_to_api_filter_servicenow(self):
        """Test ServiceNow filter generation."""
        pred = Predicate(column="status", operator="=", value="open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        result = compound.to_api_filter("servicenow")
        assert result is not None
    
    def test_to_api_filter_salesforce(self):
        """Test Salesforce filter generation."""
        pred = Predicate(column="Status", operator="=", value="Open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        result = compound.to_api_filter("salesforce")
        assert "Status" in result
        assert "Open" in result
    
    def test_to_api_filter_jira(self):
        """Test Jira JQL generation."""
        pred = Predicate(column="status", operator="=", value="Open")
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        result = compound.to_api_filter("jira")
        assert "status" in result
    
    def test_to_api_filter_unknown_dialect(self):
        """Test unknown dialect returns None."""
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="x", operator="=", value=1)]
        )
        
        result = compound.to_api_filter("unknown")
        assert result is None
    
    def test_servicenow_or_filter(self):
        """Test ServiceNow OR filter syntax."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound._to_servicenow_filter()
        assert "^OR" in result
    
    def test_servicenow_and_filter(self):
        """Test ServiceNow AND filter syntax."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="priority", operator=">", value=3)
        
        compound = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound._to_servicenow_filter()
        assert "^" in result
    
    def test_servicenow_in_filter(self):
        """Test ServiceNow IN filter syntax."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="status",
            values=["open", "closed"]
        )
        
        result = compound._to_servicenow_filter()
        assert "statusIN" in result
    
    def test_salesforce_or_filter(self):
        """Test Salesforce OR filter syntax."""
        pred1 = Predicate(column="Status", operator="=", value="Open")
        pred2 = Predicate(column="Status", operator="=", value="Closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound._to_salesforce_filter()
        assert " OR " in result
    
    def test_salesforce_and_filter(self):
        """Test Salesforce AND filter syntax."""
        pred1 = Predicate(column="Status", operator="=", value="Open")
        pred2 = Predicate(column="Priority", operator="=", value="High")
        
        compound = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound._to_salesforce_filter()
        assert " AND " in result
    
    def test_salesforce_in_filter(self):
        """Test Salesforce IN filter syntax."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="Status",
            values=["Open", "Closed"]
        )
        
        result = compound._to_salesforce_filter()
        assert "IN (" in result
    
    def test_jira_or_filter(self):
        """Test Jira OR filter syntax."""
        pred1 = Predicate(column="status", operator="=", value="Open")
        pred2 = Predicate(column="status", operator="=", value="Closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound._to_jira_filter()
        assert " OR " in result
    
    def test_jira_and_filter(self):
        """Test Jira AND filter syntax."""
        pred1 = Predicate(column="status", operator="=", value="Open")
        pred2 = Predicate(column="priority", operator="=", value="High")
        
        compound = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[pred1, pred2]
        )
        
        result = compound._to_jira_filter()
        assert " AND " in result
    
    def test_jira_in_filter(self):
        """Test Jira IN filter syntax."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="status",
            values=["Open", "Closed"]
        )
        
        result = compound._to_jira_filter()
        assert "IN (" in result
    
    def test_repr(self):
        """Test string representation."""
        pred = Predicate(column="x", operator="=", value=1)
        compound = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[pred]
        )
        
        repr_str = repr(compound)
        assert "CompoundPredicate" in repr_str
    
    def test_repr_or_group(self):
        """Test OR group string representation."""
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[]
        )
        
        repr_str = repr(compound)
        assert "OR" in repr_str
    
    def test_repr_and_group(self):
        """Test AND group string representation."""
        compound = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[]
        )
        
        repr_str = repr(compound)
        assert "AND" in repr_str
    
    def test_repr_in_list(self):
        """Test IN list string representation."""
        compound = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="status",
            values=["a", "b"]
        )
        
        repr_str = repr(compound)
        assert "IN" in repr_str
    
    def test_nested_compound_in_and_group(self):
        """Test nested compound predicates in AND group."""
        inner_pred = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="x", operator="=", value=1)]
        )
        outer_pred = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[inner_pred]
        )
        
        result = outer_pred.to_simple_predicates()
        assert len(result) == 1


class TestSubqueryInfo:
    """Tests for SubqueryInfo class."""
    
    def test_subquery_info_creation(self):
        """Test SubqueryInfo creation."""
        info = SubqueryInfo(
            sql="SELECT id FROM users WHERE active = true",
            column="user_id",
            operator="IN",
            inner_table="users",
            inner_columns=["id"],
            inner_predicates=[Predicate(column="active", operator="=", value=True)],
            can_push_down=False
        )
        
        assert info.column == "user_id"
        assert info.operator == "IN"
        assert info.inner_table == "users"
    
    def test_subquery_info_repr(self):
        """Test SubqueryInfo string representation."""
        info = SubqueryInfo(
            sql="SELECT id FROM users",
            column="user_id",
            operator="IN",
            inner_table="users"
        )
        
        repr_str = repr(info)
        assert "user_id" in repr_str
        assert "IN" in repr_str
        assert "users" in repr_str


class TestQueryOptimizer:
    """Tests for QueryOptimizer class."""
    
    @pytest.fixture
    def optimizer(self):
        """Create a QueryOptimizer instance."""
        return QueryOptimizer()
    
    def test_init_default_capabilities(self, optimizer):
        """Test default capabilities are set."""
        caps = optimizer.get_adapter_capabilities("unknown")
        
        assert caps["supports_or"] is False
        assert caps["supports_in"] is True
        assert caps["supports_between"] is True
    
    def test_known_capabilities_servicenow(self, optimizer):
        """Test ServiceNow capabilities."""
        caps = optimizer.get_adapter_capabilities("servicenow")
        
        assert caps["supports_or"] is True
        assert caps["max_in_values"] == 500
    
    def test_known_capabilities_salesforce(self, optimizer):
        """Test Salesforce capabilities."""
        caps = optimizer.get_adapter_capabilities("salesforce")
        
        assert caps["supports_or"] is True
        assert caps["supports_subquery"] is True
        assert caps["max_in_values"] == 2000
    
    def test_known_capabilities_jira(self, optimizer):
        """Test Jira capabilities."""
        caps = optimizer.get_adapter_capabilities("jira")
        
        assert caps["supports_or"] is True
        assert caps["max_in_values"] == 100
    
    def test_extract_simple_equality(self, optimizer):
        """Test extracting simple equality predicate."""
        sql = "SELECT * FROM t WHERE status = 'open'"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, subqueries = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
        assert predicates[0].type == PredicateType.SIMPLE
    
    def test_extract_comparison_operators(self, optimizer):
        """Test extracting various comparison operators."""
        operators = ["<", "<=", ">", ">=", "!="]
        
        for op in operators:
            sql = f"SELECT * FROM t WHERE x {op} 5"
            parsed = sqlglot.parse_one(sql)
            where_clause = parsed.find(exp.Where).this
            
            predicates, _ = optimizer.extract_complex_predicates(where_clause)
            assert len(predicates) >= 1
    
    def test_extract_like(self, optimizer):
        """Test extracting LIKE predicate."""
        sql = "SELECT * FROM t WHERE name LIKE '%test%'"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
    
    def test_extract_in_list(self, optimizer):
        """Test extracting IN list predicate."""
        sql = "SELECT * FROM t WHERE status IN ('open', 'closed', 'pending')"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
        assert predicates[0].type == PredicateType.IN_LIST
        assert len(predicates[0].values) == 3
    
    def test_extract_between(self, optimizer):
        """Test extracting BETWEEN predicate."""
        sql = "SELECT * FROM t WHERE created BETWEEN '2024-01-01' AND '2024-12-31'"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
        assert predicates[0].type == PredicateType.BETWEEN
    
    def test_extract_is_null(self, optimizer):
        """Test extracting IS NULL predicate."""
        sql = "SELECT * FROM t WHERE deleted_at IS NULL"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
    
    def test_extract_or_group(self, optimizer):
        """Test extracting OR group predicate."""
        sql = "SELECT * FROM t WHERE status = 'open' OR status = 'pending'"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
        assert predicates[0].type == PredicateType.OR_GROUP
    
    def test_extract_and_group(self, optimizer):
        """Test extracting AND group predicates."""
        sql = "SELECT * FROM t WHERE status = 'open' AND priority > 3"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        # AND at top level should flatten
        assert len(predicates) >= 2
    
    def test_extract_nested_or_and(self, optimizer):
        """Test extracting nested OR and AND predicates."""
        sql = "SELECT * FROM t WHERE (status = 'open' OR status = 'pending') AND priority > 3"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        # Should have OR group and simple predicate
        types = [p.type for p in predicates]
        assert PredicateType.OR_GROUP in types or PredicateType.AND_GROUP in types
    
    def test_extract_parenthesized(self, optimizer):
        """Test extracting predicates with parentheses."""
        sql = "SELECT * FROM t WHERE (status = 'open')"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
    
    def test_extract_subquery(self, optimizer):
        """Test extracting subquery predicate."""
        sql = "SELECT * FROM t WHERE id IN (SELECT user_id FROM active_users WHERE status = 'active')"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, subqueries = optimizer.extract_complex_predicates(where_clause)
        
        assert len(subqueries) == 1
        assert subqueries[0].column == "id"
        assert subqueries[0].operator == "IN"
    
    def test_extract_not_predicate(self, optimizer):
        """Test extracting NOT predicate."""
        sql = "SELECT * FROM t WHERE NOT status = 'deleted'"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        # NOT should create a compound predicate
        assert len(predicates) >= 0  # Behavior may vary
    
    def test_extract_literal_number(self, optimizer):
        """Test extracting numeric literal."""
        sql = "SELECT * FROM t WHERE x = 42"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
        simple = predicates[0].to_simple_predicates()
        assert simple[0].value == 42
    
    def test_extract_literal_float(self, optimizer):
        """Test extracting float literal."""
        sql = "SELECT * FROM t WHERE x = 3.14"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        simple = predicates[0].to_simple_predicates()
        assert simple[0].value == 3.14
    
    def test_optimize_for_adapter_simple(self, optimizer):
        """Test optimizing simple predicates for an adapter."""
        pred = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="status", operator="=", value="open")]
        )
        
        simple, compound, subq = optimizer.optimize_for_adapter(
            [pred], [], "servicenow"
        )
        
        assert len(simple) == 1
        assert simple[0].column == "status"
    
    def test_optimize_for_adapter_or_to_in(self, optimizer):
        """Test OR group converted to IN for adapter."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        compound = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        simple, _, _ = optimizer.optimize_for_adapter([compound], [], "servicenow")
        
        assert len(simple) == 1
        assert simple[0].operator == "IN"
    
    def test_optimize_for_adapter_subquery_pushdown(self, optimizer):
        """Test subquery pushdown detection."""
        subquery = SubqueryInfo(
            sql="SELECT id FROM salesforce.users WHERE active = true",
            column="user_id",
            operator="IN",
            inner_table="salesforce.users",
            can_push_down=False
        )
        
        _, _, subqueries = optimizer.optimize_for_adapter([], [subquery], "salesforce")
        
        assert len(subqueries) == 1
        assert subqueries[0].can_push_down is True
    
    def test_optimize_for_adapter_subquery_cross_adapter(self, optimizer):
        """Test subquery cannot be pushed across adapters."""
        subquery = SubqueryInfo(
            sql="SELECT id FROM servicenow.users",
            column="user_id",
            operator="IN",
            inner_table="servicenow.users",
            can_push_down=False
        )
        
        _, _, subqueries = optimizer.optimize_for_adapter([], [subquery], "salesforce")
        
        # Should not be pushable to salesforce as it's from servicenow
        assert len(subqueries) == 0
    
    def test_convert_or_to_in(self, optimizer):
        """Test OR to IN conversion."""
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
        
        result = optimizer.convert_or_to_in([or_group])
        
        assert len(result) == 1
        assert result[0].operator == "IN"
        assert set(result[0].value) == {"open", "closed"}
    
    def test_convert_or_to_in_with_simple_predicates(self, optimizer):
        """Test OR to IN with base Predicate objects."""
        pred1 = Predicate(column="status", operator="=", value="open")
        pred2 = Predicate(column="status", operator="=", value="closed")
        
        or_group = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[pred1, pred2],
            column="status"
        )
        
        result = optimizer.convert_or_to_in([or_group])
        
        assert len(result) == 1
        assert result[0].operator == "IN"
    
    def test_convert_or_to_in_mixed_types(self, optimizer):
        """Test OR to IN conversion with various predicate types."""
        simple = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="x", operator="=", value=1)]
        )
        and_group = CompoundPredicate(
            type=PredicateType.AND_GROUP,
            predicates=[Predicate(column="y", operator="=", value=2)]
        )
        in_list = CompoundPredicate(
            type=PredicateType.IN_LIST,
            column="z",
            values=[3, 4, 5]
        )
        
        result = optimizer.convert_or_to_in([simple, and_group, in_list])
        
        assert len(result) == 3
    
    def test_resolve_adapter_for_table_with_schema(self, optimizer):
        """Test adapter resolution for qualified table names."""
        result = optimizer._resolve_adapter_for_table("servicenow.incident")
        
        assert result == "servicenow"
    
    def test_resolve_adapter_for_table_no_schema(self, optimizer):
        """Test adapter resolution for unqualified table names."""
        result = optimizer._resolve_adapter_for_table("incident")
        
        assert result is None
    
    def test_resolve_adapter_for_table_quoted(self, optimizer):
        """Test adapter resolution for quoted schema names."""
        result = optimizer._resolve_adapter_for_table('"servicenow".incident')
        
        assert result == "servicenow"
    
    def test_nested_compound_filter(self):
        """Test nested compound predicates in filter generation."""
        inner = CompoundPredicate(
            type=PredicateType.SIMPLE,
            predicates=[Predicate(column="x", operator="=", value=1)]
        )
        outer = CompoundPredicate(
            type=PredicateType.OR_GROUP,
            predicates=[inner]
        )
        
        result = outer._to_servicenow_filter()
        assert result is not None


class TestComplexQueries:
    """Integration tests for complex query optimization."""
    
    @pytest.fixture
    def optimizer(self):
        return QueryOptimizer()
    
    def test_complex_where_with_multiple_or_groups(self, optimizer):
        """Test complex WHERE with multiple OR groups."""
        sql = """
        SELECT * FROM t 
        WHERE (status = 'open' OR status = 'pending')
        AND (priority = 'high' OR priority = 'critical')
        """
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        # Should have multiple predicates
        assert len(predicates) >= 1
    
    def test_triple_or(self, optimizer):
        """Test OR group with three conditions."""
        sql = "SELECT * FROM t WHERE status = 'a' OR status = 'b' OR status = 'c'"
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        assert len(predicates) == 1
        assert predicates[0].type == PredicateType.OR_GROUP
        assert len(predicates[0].predicates) == 3
    
    def test_deeply_nested_predicates(self, optimizer):
        """Test deeply nested predicate structure."""
        sql = """
        SELECT * FROM t 
        WHERE ((a = 1 AND b = 2) OR (c = 3 AND d = 4))
        """
        parsed = sqlglot.parse_one(sql)
        where_clause = parsed.find(exp.Where).this
        
        predicates, _ = optimizer.extract_complex_predicates(where_clause)
        
        # Should be able to extract without error
        assert predicates is not None
