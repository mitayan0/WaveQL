"""
Query Planner - SQL parsing and predicate extraction for pushdown using sqlglot
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class ParameterPlaceholder:
    """Represents a SQL parameter placeholder (?)"""
    def __eq__(self, other):
        return isinstance(other, ParameterPlaceholder)
    
    def __repr__(self):
        return "?"


@dataclass
class Predicate:
    """Represents a WHERE clause predicate."""
    column: str
    operator: str  # =, !=, <, >, <=, >=, LIKE, IN, IS NULL, IS NOT NULL
    value: Any
    
    def to_api_filter(self, dialect: str = "default") -> str:
        """Convert predicate to API-specific filter format."""
        if dialect == "servicenow":
            op_map = {"=": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<=", 
                      "LIKE": "LIKE", "IN": "IN"}
            return f"{self.column}{op_map.get(self.operator, '=')}{self.value}"
        return f"{self.column} {self.operator} {self.value}"


@dataclass
class Aggregate:
    """Represents an aggregation function."""
    func: str  # COUNT, SUM, AVG, MIN, MAX
    column: str
    alias: Optional[str] = None


@dataclass
class QueryInfo:
    """Parsed query information."""
    operation: str  # SELECT, INSERT, UPDATE, DELETE
    table: Optional[str] = None
    columns: List[str] = field(default_factory=lambda: ["*"])
    predicates: List[Predicate] = field(default_factory=list)
    values: Dict[str, Any] = field(default_factory=dict)
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: List[Tuple[str, str]] = field(default_factory=list)  # [(col, ASC/DESC), ...]
    joins: List[Dict] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    aggregates: List[Aggregate] = field(default_factory=list)
    raw_sql: str = ""
    is_explain: bool = False


class QueryPlanner:
    """
    Parses SQL and extracts components for predicate pushdown using sqlglot.
    
    Supports:
    - Complex SELECT queries (CTEs, Subqueries, Joins)
    - Predicate extraction for pushdown
    - Aggregation discovery
    - INSERT, UPDATE, DELETE parsing
    """
    
    def parse(self, sql: str) -> QueryInfo:
        """Parse SQL query and extract components."""
        sql = sql.strip()
        try:
            # We use DuckDB dialect by default as it's our engine
            expression = sqlglot.parse_one(sql, read="duckdb")
        except Exception as e:
            logger.debug(f"sqlglot failed to parse query, falling back to RAW: {e}")
            return QueryInfo(operation="RAW", raw_sql=sql)

        # 1. Handle EXPLAIN
        # Some versions of sqlglot use exp.Explain, others use exp.Command
        is_explain = False
        inner_expression = None

        explain_class = getattr(exp, "Explain", None)
        if explain_class and isinstance(expression, explain_class):
            is_explain = True
            inner_expression = expression.this
        elif isinstance(expression, exp.Command) and expression.this.upper() == "EXPLAIN":
            is_explain = True
            inner_expression = expression.expression

        if is_explain and inner_expression:
            # Recursively parse the inner statement
            # If inner_expression is a parser Literal (from Command), use .this to get the raw string
            inner_sql = inner_expression.this if isinstance(inner_expression, exp.Literal) else inner_expression.sql() 
            info = self.parse(inner_sql)
            info.is_explain = True
            return info

        if isinstance(expression, exp.Select):
            return self._parse_select(expression, sql)
        elif isinstance(expression, exp.Insert):
            return self._parse_insert(expression, sql)
        elif isinstance(expression, exp.Update):
            return self._parse_update(expression, sql)
        elif isinstance(expression, exp.Delete):
            return self._parse_delete(expression, sql)
        
        return QueryInfo(operation="RAW", raw_sql=sql)

    def _parse_select(self, expression: exp.Select, raw_sql: str) -> QueryInfo:
        """Extract information from a SELECT statement."""
        info = QueryInfo(operation="SELECT", raw_sql=raw_sql)
        
        # 1. Primary Table Detection
        # We need the table name to resolve the correct adapter.
        # We prioritize the first physical table found that isn't a CTE alias.
        ctes = {step.alias for step in expression.find_all(exp.CTE)}
        for table in expression.find_all(exp.Table):
            t_name = table.sql()
            if t_name not in ctes:
                info.table = t_name
                break
        
        # Fallback to first table if all are CTEs or none found
        all_tables = list(expression.find_all(exp.Table))
        if not info.table and all_tables:
            info.table = all_tables[0].sql()

        # 2. Joins
        for join in expression.find_all(exp.Join):
            info.joins.append({
                "type": join.args.get("kind", "INNER").upper(),
                "table": join.this.sql()
            })

        # 3. Columns & Aggregates
        info.columns = []
        for e in expression.expressions:
            if isinstance(e, exp.Star):
                info.columns.append("*")
            elif isinstance(e, exp.Alias):
                alias = e.alias
                if isinstance(e.this, exp.AggFunc):
                    func = e.this.key.upper()
                    col = e.this.this.sql() if e.this.this else "*"
                    info.aggregates.append(Aggregate(func, col, alias))
                info.columns.append(alias)
            elif isinstance(e, exp.AggFunc):
                func = e.key.upper()
                col = e.this.sql() if e.this else "*"
                info.aggregates.append(Aggregate(func, col))
                info.columns.append(f"{func}({col})")
            else:
                info.columns.append(e.sql())

        # 4. Where Clause (Predicates)
        where = expression.args.get("where")
        if where:
            info.predicates = self._parse_condition(where.this)

        # 5. Group By
        group = expression.args.get("group")
        if group:
            info.group_by = [g.sql() for g in group.expressions]

        # 6. Order By
        order = expression.args.get("order")
        if order:
            for o in order.expressions:
                # Resolve column name (stripping direction)
                col = o.this.sql()
                direction = "DESC" if isinstance(o, exp.Ordered) and o.args.get("desc") else "ASC"
                info.order_by.append((col, direction))

        # 7. Limit & Offset
        limit = expression.args.get("limit")
        if limit:
             try:
                 info.limit = int(limit.expression.this)
             except (ValueError, AttributeError):
                 pass

        offset = expression.args.get("offset")
        if offset:
            try:
                info.offset = int(offset.expression.this)
            except (ValueError, AttributeError):
                pass

        return info

    def _parse_condition(self, expression: exp.Expression) -> List[Predicate]:
        """Recursively parse WHERE clause conditions for pushdown."""
        predicates = []
        
        # Handle top-level ANDs (common for pushdown)
        if isinstance(expression, exp.And):
            predicates.extend(self._parse_condition(expression.left))
            predicates.extend(self._parse_condition(expression.right))
        
        # Handle Binary Operations
        elif isinstance(expression, (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Like, exp.In)):
            col = expression.left.sql()
            
            # Map sqlglot node types to SQL operators
            op_map = {
                exp.EQ: "=", 
                exp.NEQ: "!=", 
                exp.LT: "<", 
                exp.LTE: "<=", 
                exp.GT: ">", 
                exp.GTE: ">=", 
                exp.Like: "LIKE", 
                exp.In: "IN"
            }
            operator = op_map.get(type(expression), "=")
            
            # Handle list for IN
            if isinstance(expression, exp.In):
                if isinstance(expression.args.get("field"), exp.Tuple):
                    val = [self._extract_literal(v) for v in expression.args["field"].expressions]
                else:
                    val = self._extract_literal(expression.args.get("field"))
            else:
                val = self._extract_literal(expression.right)
            
            predicates.append(Predicate(column=col, operator=operator, value=val))
            
        # Handle IS NULL / IS NOT NULL
        elif isinstance(expression, exp.Is):
            col = expression.left.sql()
            if isinstance(expression.right, exp.Null):
                predicates.append(Predicate(column=col, operator="IS NULL", value=None))
        elif isinstance(expression, exp.Not):
            if isinstance(expression.this, exp.Is) and isinstance(expression.this.right, exp.Null):
                col = expression.this.left.sql()
                predicates.append(Predicate(column=col, operator="IS NOT NULL", value=None))
        
        return predicates

    def _extract_literal(self, expression: exp.Expression) -> Any:
        """Extract a Python value from a sqlglot expression."""
        if isinstance(expression, exp.Literal):
            if expression.is_number:
                return float(expression.this) if "." in expression.this else int(expression.this)
            return expression.this
        elif isinstance(expression, exp.Boolean):
            return expression.this
        elif isinstance(expression, exp.Null):
            return None
        elif isinstance(expression, exp.Placeholder):
            return ParameterPlaceholder()
        return expression.sql()

    def _parse_insert(self, expression: exp.Insert, raw_sql: str) -> QueryInfo:
        """Parse INSERT statement."""
        info = QueryInfo(operation="INSERT", raw_sql=raw_sql)
        # Handle Schema object (table details with columns)
        if isinstance(expression.this, exp.Schema):
            info.table = expression.this.this.sql()
            # If explicit columns are provided in the Schema, use them if not already found
            schema_cols = [e.sql() for e in expression.this.expressions]
        else:
            info.table = expression.this.sql()
            schema_cols = []
        
        # Get columns from args if present (typical in some dialects) or fallback to Schema columns
        cols = [c.sql() for c in expression.args.get("columns", [])]
        if not cols and schema_cols:
            cols = schema_cols
        
        # Check for VALUES clause
        values_expr = expression.expression
        if isinstance(values_expr, exp.Values):
            # Only handle first row for simple insertion
            first_row = next(values_expr.find_all(exp.Tuple), None)
            if first_row:
                vals = [self._extract_literal(v) for v in first_row.expressions]
                if cols:
                    info.values = dict(zip(cols, vals))
                else:
                    info.values = {"_values": vals}
        
        return info

    def _parse_update(self, expression: exp.Update, raw_sql: str) -> QueryInfo:
        """Parse UPDATE statement."""
        info = QueryInfo(operation="UPDATE", raw_sql=raw_sql)
        info.table = expression.this.sql()
        
        # Extract SET expressions
        expressions = expression.args.get("expressions", [])
        for eq in expressions:
            if isinstance(eq, exp.EQ):
                info.values[eq.left.sql()] = self._extract_literal(eq.right)
        
        # WHERE
        where = expression.args.get("where")
        if where:
            info.predicates = self._parse_condition(where.this)
            
        return info

    def _parse_delete(self, expression: exp.Delete, raw_sql: str) -> QueryInfo:
        """Parse DELETE statement."""
        info = QueryInfo(operation="DELETE", raw_sql=raw_sql)
        info.table = expression.this.sql()
        
        # WHERE
        where = expression.args.get("where")
        if where:
            info.predicates = self._parse_condition(where.this)
            
        return info
