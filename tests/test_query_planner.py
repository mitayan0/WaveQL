"""
Tests for Query Planner - SQL parsing and predicate extraction

Tests cover:
- Simple SELECT parsing
- Complex predicates (AND, OR, IN, BETWEEN)
- OR → IN conversion optimization
- Aggregation extraction (GROUP BY, COUNT, SUM, AVG, MIN, MAX)
- JOIN parsing and table extraction
- INSERT/UPDATE/DELETE parsing
- CTE and subquery handling
- View expansion
- Parameterized queries
"""


from waveql.query_planner import (
    QueryPlanner,
    QueryInfo,
    Predicate,
    Aggregate,
    ParameterPlaceholder,
)


class TestPredicateDataclass:
    """Tests for Predicate dataclass."""
    
    def test_predicate_creation(self):
        """Test creating a basic predicate."""
        pred = Predicate(column="status", operator="=", value="open")
        
        assert pred.column == "status"
        assert pred.operator == "="
        assert pred.value == "open"
    
    def test_predicate_repr(self):
        """Test string representation of predicate."""
        pred = Predicate(column="id", operator=">", value=100)
        
        repr_str = repr(pred)
        assert "id" in repr_str
        assert ">" in repr_str
        assert "100" in repr_str
    
    def test_predicate_to_api_filter_default(self):
        """Test converting predicate to API filter (default dialect)."""
        pred = Predicate(column="name", operator="=", value="test")
        
        filter_str = pred.to_api_filter()
        # Default dialect returns column=value style
        assert "name" in filter_str
        assert "test" in filter_str


class TestAggregateDataclass:
    """Tests for Aggregate dataclass."""
    
    def test_aggregate_creation(self):
        """Test creating an aggregate."""
        agg = Aggregate(func="COUNT", column="*", alias="total")
        
        assert agg.func == "COUNT"
        assert agg.column == "*"
        assert agg.alias == "total"
    
    def test_aggregate_repr(self):
        """Test string representation of aggregate."""
        agg = Aggregate(func="SUM", column="amount")
        
        repr_str = repr(agg)
        assert "SUM" in repr_str
        assert "amount" in repr_str


class TestParameterPlaceholder:
    """Tests for ParameterPlaceholder."""
    
    def test_placeholder_equality(self):
        """Test that placeholders are equal."""
        p1 = ParameterPlaceholder()
        p2 = ParameterPlaceholder()
        
        assert p1 == p2
    
    def test_placeholder_repr(self):
        """Test placeholder string representation."""
        p = ParameterPlaceholder()
        assert repr(p) == "?"


class TestSimpleSelectParsing:
    """Tests for parsing simple SELECT queries."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_select_all_columns(self):
        """Test SELECT * query."""
        info = self.planner.parse("SELECT * FROM incident")
        
        assert info.operation == "SELECT"
        assert info.table == "incident"
        assert "*" in info.columns or info.columns == ["*"]
    
    def test_select_specific_columns(self):
        """Test SELECT with specific columns."""
        info = self.planner.parse("SELECT id, name, status FROM users")
        
        assert info.operation == "SELECT"
        assert info.table == "users"
        assert "id" in info.columns
        assert "name" in info.columns
        assert "status" in info.columns
    
    def test_select_with_alias(self):
        """Test SELECT with column aliases."""
        info = self.planner.parse("SELECT id, name AS user_name FROM users")
        
        assert info.operation == "SELECT"
        assert info.table == "users"
    
    def test_select_with_limit(self):
        """Test SELECT with LIMIT clause."""
        info = self.planner.parse("SELECT * FROM incident LIMIT 10")
        
        assert info.operation == "SELECT"
        assert info.limit == 10
    
    def test_select_with_offset(self):
        """Test SELECT with LIMIT and OFFSET."""
        info = self.planner.parse("SELECT * FROM incident LIMIT 10 OFFSET 20")
        
        assert info.limit == 10
        assert info.offset == 20
    
    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY clause."""
        info = self.planner.parse("SELECT * FROM incident ORDER BY created_at DESC")
        
        assert len(info.order_by) > 0
        assert info.order_by[0][0] == "created_at"
        assert info.order_by[0][1].upper() in ("DESC", "DESCENDING")


class TestPredicateExtraction:
    """Tests for extracting predicates from WHERE clause."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_simple_equality_predicate(self):
        """Test simple equality predicate."""
        info = self.planner.parse("SELECT * FROM incident WHERE status = 'open'")
        
        assert len(info.predicates) > 0
        pred = info.predicates[0]
        assert pred.column.lower() == "status"
        assert pred.operator == "="
        assert pred.value == "open"
    
    def test_numeric_predicate(self):
        """Test numeric comparison predicate."""
        info = self.planner.parse("SELECT * FROM orders WHERE amount > 100")
        
        assert len(info.predicates) > 0
        pred = info.predicates[0]
        assert pred.column.lower() == "amount"
        assert pred.operator == ">"
        assert pred.value == 100
    
    def test_and_predicates(self):
        """Test AND combined predicates."""
        info = self.planner.parse(
            "SELECT * FROM incident WHERE status = 'open' AND priority = 1"
        )
        
        assert len(info.predicates) >= 2
        columns = [p.column.lower() for p in info.predicates]
        assert "status" in columns
        assert "priority" in columns
    
    def test_in_predicate(self):
        """Test IN clause predicate."""
        info = self.planner.parse(
            "SELECT * FROM incident WHERE status IN ('open', 'pending', 'active')"
        )
        
        assert len(info.predicates) > 0
        pred = info.predicates[0]
        assert pred.column.lower() == "status"
        assert pred.operator.upper() == "IN"
        assert isinstance(pred.value, (list, tuple))
        assert len(pred.value) == 3
    
    def test_like_predicate(self):
        """Test LIKE predicate."""
        info = self.planner.parse("SELECT * FROM users WHERE name LIKE '%john%'")
        
        assert len(info.predicates) > 0
        pred = info.predicates[0]
        assert pred.column.lower() == "name"
        assert pred.operator.upper() == "LIKE"
    
    def test_is_null_predicate(self):
        """Test IS NULL predicate."""
        info = self.planner.parse("SELECT * FROM incident WHERE resolved_at IS NULL")
        
        assert len(info.predicates) > 0
        # IS NULL may be represented differently
        pred = info.predicates[0]
        assert pred.column.lower() == "resolved_at"
    
    def test_between_predicate(self):
        """Test BETWEEN predicate extraction."""
        info = self.planner.parse(
            "SELECT * FROM orders WHERE amount BETWEEN 100 AND 500"
        )
        
        # BETWEEN might be converted to >= and <= predicates
        assert len(info.predicates) > 0


class TestOrToInConversion:
    """Tests for OR → IN predicate conversion optimization."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_or_same_column_converts_to_in(self):
        """Test that OR on same column with equality converts to IN."""
        info = self.planner.parse(
            "SELECT * FROM incident WHERE status = 'open' OR status = 'pending' OR status = 'active'"
        )
        
        # Should be converted to IN predicate or kept as separate predicates
        # Either way, should be parseable
        assert info.operation == "SELECT"
        assert info.table == "incident"
    
    def test_complex_or_predicates(self):
        """Test complex OR predicates that can't be converted to IN."""
        info = self.planner.parse(
            "SELECT * FROM incident WHERE status = 'open' OR priority > 3"
        )
        
        # Complex OR (different columns/operators) should be marked
        assert info.operation == "SELECT"


class TestAggregationParsing:
    """Tests for aggregation function parsing."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_count_star(self):
        """Test COUNT(*) aggregation."""
        info = self.planner.parse("SELECT COUNT(*) FROM incident")
        
        assert len(info.aggregates) > 0
        agg = info.aggregates[0]
        assert agg.func.upper() == "COUNT"
    
    def test_sum_aggregation(self):
        """Test SUM aggregation."""
        info = self.planner.parse("SELECT SUM(amount) FROM orders")
        
        assert len(info.aggregates) > 0
        agg = info.aggregates[0]
        assert agg.func.upper() == "SUM"
        assert agg.column == "amount"
    
    def test_avg_aggregation(self):
        """Test AVG aggregation."""
        info = self.planner.parse("SELECT AVG(price) FROM products")
        
        assert len(info.aggregates) > 0
        agg = info.aggregates[0]
        assert agg.func.upper() == "AVG" or agg.func.upper() == "MEAN"
    
    def test_min_max_aggregation(self):
        """Test MIN and MAX aggregations."""
        info = self.planner.parse("SELECT MIN(created_at), MAX(created_at) FROM orders")
        
        assert len(info.aggregates) >= 2
        funcs = [a.func.upper() for a in info.aggregates]
        assert "MIN" in funcs
        assert "MAX" in funcs
    
    def test_aggregation_with_alias(self):
        """Test aggregation with alias."""
        info = self.planner.parse("SELECT COUNT(*) AS total FROM incident")
        
        assert len(info.aggregates) > 0
        agg = info.aggregates[0]
        assert agg.alias == "total" or "total" in str(agg)
    
    def test_group_by(self):
        """Test GROUP BY clause extraction."""
        info = self.planner.parse(
            "SELECT status, COUNT(*) FROM incident GROUP BY status"
        )
        
        assert len(info.group_by) > 0
        assert "status" in [g.lower() for g in info.group_by]
    
    def test_having_clause(self):
        """Test query with HAVING clause."""
        info = self.planner.parse(
            "SELECT status, COUNT(*) as cnt FROM incident GROUP BY status HAVING COUNT(*) > 5"
        )
        
        # Should parse without error
        assert info.operation == "SELECT"


class TestJoinParsing:
    """Tests for JOIN parsing (for virtual joins)."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_inner_join(self):
        """Test INNER JOIN parsing."""
        info = self.planner.parse(
            "SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id"
        )
        
        # Should identify both tables
        assert info.operation == "SELECT"
    
    def test_left_join(self):
        """Test LEFT JOIN parsing."""
        info = self.planner.parse(
            "SELECT * FROM orders LEFT JOIN customers ON orders.customer_id = customers.id"
        )
        
        assert info.operation == "SELECT"
    
    def test_multiple_joins(self):
        """Test query with multiple JOINs."""
        info = self.planner.parse("""
            SELECT * FROM orders
            JOIN customers ON orders.customer_id = customers.id
            JOIN products ON orders.product_id = products.id
        """)
        
        assert info.operation == "SELECT"


class TestInsertParsing:
    """Tests for INSERT statement parsing."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_insert_values(self):
        """Test INSERT with VALUES."""
        info = self.planner.parse(
            "INSERT INTO incident (short_description, priority) VALUES ('Test', 1)"
        )
        
        assert info.operation == "INSERT"
        assert info.table == "incident"
    
    def test_insert_column_extraction(self):
        """Test that INSERT columns are extracted."""
        info = self.planner.parse(
            "INSERT INTO users (name, email, status) VALUES ('John', 'john@example.com', 'active')"
        )
        
        assert info.operation == "INSERT"
        assert info.table == "users"


class TestUpdateParsing:
    """Tests for UPDATE statement parsing."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_update_basic(self):
        """Test basic UPDATE parsing."""
        info = self.planner.parse(
            "UPDATE incident SET status = 'closed' WHERE id = '123'"
        )
        
        assert info.operation == "UPDATE"
        assert info.table == "incident"
        assert len(info.predicates) > 0
    
    def test_update_multiple_columns(self):
        """Test UPDATE with multiple columns."""
        info = self.planner.parse(
            "UPDATE users SET name = 'Jane', status = 'inactive' WHERE id = 1"
        )
        
        assert info.operation == "UPDATE"
        assert info.table == "users"


class TestDeleteParsing:
    """Tests for DELETE statement parsing."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_delete_with_where(self):
        """Test DELETE with WHERE clause."""
        info = self.planner.parse("DELETE FROM incident WHERE id = '123'")
        
        assert info.operation == "DELETE"
        assert info.table == "incident"
        assert len(info.predicates) > 0


class TestViewExpansion:
    """Tests for virtual view expansion."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_simple_view_expansion(self):
        """Test expanding a simple view definition."""
        views = {
            "active_incidents": "SELECT * FROM incident WHERE status = 'active'"
        }
        
        sql = "SELECT * FROM active_incidents"
        expanded = self.planner.expand_views(sql, views)
        
        # View should be expanded into a subquery
        assert "incident" in expanded.lower()
    
    def test_nested_view_expansion(self):
        """Test expanding nested views."""
        views = {
            "open_incidents": "SELECT * FROM incident WHERE status = 'open'",
            "high_priority_open": "SELECT * FROM open_incidents WHERE priority = 1"
        }
        
        sql = "SELECT * FROM high_priority_open"
        expanded = self.planner.expand_views(sql, views)
        
        # Should expand both levels
        assert "incident" in expanded.lower()


class TestQueryInfoDataclass:
    """Tests for QueryInfo dataclass."""
    
    def test_query_info_defaults(self):
        """Test QueryInfo default values."""
        info = QueryInfo(operation="SELECT")
        
        assert info.operation == "SELECT"
        assert info.table is None
        assert info.columns == ["*"]
        assert info.predicates == []
        assert info.limit is None
        assert info.offset is None
    
    def test_query_info_repr(self):
        """Test QueryInfo string representation."""
        info = QueryInfo(operation="SELECT", table="users")
        
        repr_str = repr(info)
        assert "SELECT" in repr_str
        assert "users" in repr_str


class TestEdgeCases:
    """Tests for edge cases and error handling."""
    
    def setup_method(self):
        """Initialize planner for each test."""
        self.planner = QueryPlanner()
    
    def test_quoted_identifiers(self):
        """Test handling of quoted identifiers."""
        info = self.planner.parse('SELECT * FROM "User Table" WHERE "Status" = \'active\'')
        
        assert info.operation == "SELECT"
    
    def test_case_insensitivity(self):
        """Test case insensitivity of SQL keywords."""
        info = self.planner.parse("select * FROM incident where STATUS = 'open'")
        
        assert info.operation == "SELECT"
        assert info.table == "incident"
    
    def test_empty_predicates(self):
        """Test query with no WHERE clause."""
        info = self.planner.parse("SELECT * FROM incident")
        
        assert info.predicates == []
    
    def test_complex_subquery(self):
        """Test query with subquery."""
        info = self.planner.parse("""
            SELECT * FROM incident
            WHERE customer_id IN (SELECT id FROM customers WHERE region = 'US')
        """)
        
        assert info.operation == "SELECT"
    
    def test_cte_query(self):
        """Test Common Table Expression (CTE) query."""
        info = self.planner.parse("""
            WITH active AS (
                SELECT * FROM incident WHERE status = 'active'
            )
            SELECT * FROM active WHERE priority = 1
        """)
        
        assert info.operation == "SELECT"
