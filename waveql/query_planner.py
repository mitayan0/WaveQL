"""
Query Planner - SQL parsing and predicate extraction for pushdown using sqlglot
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

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
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"Predicate({self.column} {self.operator} {self.value!r})"


@dataclass
class Aggregate:
    """Represents an aggregation function."""
    func: str  # COUNT, SUM, AVG, MIN, MAX
    column: str
    alias: Optional[str] = None

    def __repr__(self) -> str:
        """String representation for debugging."""
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"Aggregate({self.func}({self.column}){alias_str})"


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
    aliases: Dict[str, str] = field(default_factory=dict)  # Alias -> Table Name
    group_by: List[str] = field(default_factory=list)
    aggregates: List[Aggregate] = field(default_factory=list)
    raw_sql: str = ""
    is_explain: bool = False
    # Advanced optimization fields
    compound_predicates: List[Any] = field(default_factory=list)  # CompoundPredicate objects
    subqueries: List[Any] = field(default_factory=list)  # SubqueryInfo objects
    has_complex_or: bool = False  # True if query has OR predicates
    is_hybrid: bool = False  # True if /*+ HYBRID */ hint is present

    def __repr__(self) -> str:
        """String representation for debugging."""
        parts = [f"QueryInfo({self.operation}"]
        if self.table:
            parts.append(f" FROM {self.table}")
        if self.predicates:
            parts.append(f" WHERE [{len(self.predicates)} predicates]")
        if self.joins:
            parts.append(f" [{len(self.joins)} JOINs]")
        if self.limit:
            parts.append(f" LIMIT {self.limit}")
        parts.append(")")
        return "".join(parts)


class QueryPlanner:
    """
    Parses SQL and extracts components for predicate pushdown using sqlglot.
    
    Supports:
    - Complex SELECT queries (CTEs, Subqueries, Joins)
    - Predicate extraction for pushdown
    - Aggregation discovery
    - INSERT, UPDATE, DELETE parsing
    """
    
    def expand_views(self, sql: str, views: Dict[str, str]) -> str:
        """
        Recursively expand virtual views in SQL by replacing table references
        with the view definition as a subquery.
        
        Args:
            sql: The SQL query to expand
            views: Dictionary mapping view names to their SQL definitions
            
        Returns:
            SQL string with views expanded
        """
        if not views:
            return sql
            
        try:
            # Parse
            expression = sqlglot.parse_one(sql, read="duckdb")
            
            # Track expanded views to prevent infinite loops (registry should handle cycles, but safety first)
            iteration = 0
            max_iterations = 20 
            
            while iteration < max_iterations:
                found_view = False
                
                # Find all tables currently in the expression
                # We collect them first to avoid modification issues during iteration
                tables_to_replace = []
                for table in expression.find_all(exp.Table):
                    # Check if table name matches a view
                    clean_name = table.name
                    if clean_name in views:
                        tables_to_replace.append((table, clean_name))
                
                if not tables_to_replace:
                    break
                    
                for table, view_name in tables_to_replace:
                    view_sql = views[view_name]
                    try:
                        view_expr = sqlglot.parse_one(view_sql, read="duckdb")
                        
                        # Replace with subquery: (VIEW_SQL) AS view_name
                        subquery = exp.Subquery(
                            this=view_expr,
                            alias=exp.TableAlias(this=exp.Identifier(this=view_name, quoted=False))
                        )
                        
                        table.replace(subquery)
                        found_view = True
                    except Exception as e:
                        logger.warning(f"Failed to expand view {view_name}: {e}")
                
                if not found_view:
                    break
                    
                iteration += 1
                
            return expression.sql(dialect="duckdb")
            
        except Exception as e:
            logger.debug("Failed to expand views logic: %s", e)
            return sql

    def rewrite_table_ref(self, sql: str, old_table: str, new_table: str) -> str:
        """
        Safely rewrite table references in SQL.
        
        Replaces all occurrences of `old_table` with `new_table`.
        Handles aliases, quoting, and formatting transparently using sqlglot.
        
        Args:
            sql: Original SQL query
            old_table: Normalized name of table to replace
            new_table: New table name to use
            
        Returns:
            Rewritten SQL string
        """
        try:
            expression = sqlglot.parse_one(sql, read="duckdb")
            
            # Normalize old_table for comparison (handle schema.table vs table)
            # We assume old_table is passed in normalized form logic-wise
            
            for table in expression.find_all(exp.Table):
                # Check if this table matches old_table
                # We compare standard SQL representation
                if table.sql() == old_table or table.name == old_table:
                    # Update table name
                    # We create a new Identifier/Table node
                    new_node = sqlglot.to_table(new_table)
                    # Preserve alias if exists
                    if table.alias:
                        new_node.set("alias", table.args.get("alias"))
                    table.replace(new_node)
            
            return expression.sql(dialect="duckdb")
            
        except Exception as e:
            logger.warning(f"Failed to rewrite table ref '{old_table}' -> '{new_table}': {e}. Falling back to string replacement.")
            # Fallback to simple replace if parser fails (though risky)
            import re
            pattern = re.compile(f"\\b{re.escape(old_table)}\\b", re.IGNORECASE)
            return pattern.sub(new_table, sql)

    def parse(self, sql: str) -> QueryInfo:
        """Parse SQL query and extract components."""
        sql = sql.strip()
        try:
            # We use DuckDB dialect by default as it's our engine
            expression = sqlglot.parse_one(sql, read="duckdb")
        except Exception as e:
            logger.debug(f"sqlglot failed to parse query, falling back to RAW: {e}")
            return QueryInfo(operation="RAW", raw_sql=sql)

        # 0. Detect Hints
        is_hybrid = "/*+ HYBRID */" in sql.upper()

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
            info.is_hybrid = is_hybrid or info.is_hybrid
            return info

        if isinstance(expression, exp.Select):
            info = self._parse_select(expression, sql)
            info.is_hybrid = is_hybrid
            return info
        elif isinstance(expression, exp.Insert):
            info = self._parse_insert(expression, sql)
            info.is_hybrid = is_hybrid
            return info
        elif isinstance(expression, exp.Update):
            info = self._parse_update(expression, sql)
            info.is_hybrid = is_hybrid
            return info
        elif isinstance(expression, exp.Delete):
            info = self._parse_delete(expression, sql)
            info.is_hybrid = is_hybrid
            return info
        
        return QueryInfo(operation="RAW", raw_sql=sql, is_hybrid=is_hybrid)

    def _parse_select(self, expression: exp.Select, raw_sql: str) -> QueryInfo:
        """Extract information from a SELECT statement."""
        info = QueryInfo(operation="SELECT", raw_sql=raw_sql)
        
        # 1. Primary Table Detection
        # We need the table name to resolve the correct adapter.
        # We prioritize the first physical table found that isn't a CTE alias.
        ctes = {step.alias for step in expression.find_all(exp.CTE)}
        for table in expression.find_all(exp.Table):
            t_name = table.sql()
            t_alias = table.alias or t_name
            if t_name not in ctes:
                if not info.table:
                    info.table = t_name
                info.aliases[t_alias] = t_name
        
        # Fallback to first table if all are CTEs or none found
        all_tables = list(expression.find_all(exp.Table))
        if not info.table and all_tables:
            info.table = all_tables[0].sql()

        # 2. Joins
        for join in expression.find_all(exp.Join):
            t_node = join.this
            t_name = t_node.sql()
            # If join.this is a Table, it might have an alias
            if isinstance(t_node, exp.Table):
                t_alias = t_node.alias or t_name
                info.aliases[t_alias] = t_name
            
            join_info = {
                # Use 'or' pattern to handle None values safely
                "type": (join.args.get("kind") or "INNER").upper(),
                "table": t_name,
            }
            on_condition = join.args.get("on")
            if on_condition:
                join_info["on"] = self._parse_condition(on_condition)
            info.joins.append(join_info)

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
        """Recursively parse WHERE clause conditions for pushdown.
        
        Enhanced to support:
        - Simple AND conditions (always pushed down)
        - OR conditions on same column with equality (converted to IN)
        - Nested OR conditions with optimization
        """
        predicates = []
        
        # Handle top-level ANDs (common for pushdown)
        if isinstance(expression, exp.And):
            predicates.extend(self._parse_condition(expression.left))
            predicates.extend(self._parse_condition(expression.right))
        
        # Handle OR conditions - try to optimize to IN predicate
        elif isinstance(expression, exp.Or):
            or_result = self._parse_or_condition(expression)
            if or_result:
                predicates.append(or_result)
            else:
                logger.debug(
                    "Complex OR condition detected - cannot push down, will filter client-side: %s",
                    expression.sql()
                )
        
        # Handle Binary Operations (excluding In which has different structure)
        elif isinstance(expression, (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Like)):
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
            }
            operator = op_map.get(type(expression), "=")
            val = self._extract_literal(expression.right)
            
            predicates.append(Predicate(column=col, operator=operator, value=val))
        
        # Handle IN separately (uses 'this' for column, not 'left')
        elif isinstance(expression, exp.In):
            col = expression.this.sql()
            
            # Check for expressions (value list)
            expressions = expression.args.get("expressions")
            if expressions:
                val = [self._extract_literal(v) for v in expressions]
            elif isinstance(expression.args.get("field"), exp.Tuple):
                val = [self._extract_literal(v) for v in expression.args["field"].expressions]
            else:
                # Single value or other form
                field = expression.args.get("field")
                if field:
                    val = [self._extract_literal(field)]
                else:
                    val = []
            
            predicates.append(Predicate(column=col, operator="IN", value=val))
            
        # Handle IS NULL / IS NOT NULL
        elif isinstance(expression, exp.Is):
            col = expression.left.sql()
            if isinstance(expression.right, exp.Null):
                predicates.append(Predicate(column=col, operator="IS NULL", value=None))
        elif isinstance(expression, exp.Not):
            if isinstance(expression.this, exp.Is) and isinstance(expression.this.right, exp.Null):
                col = expression.this.left.sql()
                predicates.append(Predicate(column=col, operator="IS NOT NULL", value=None))
        
        # Handle parentheses
        elif isinstance(expression, exp.Paren):
            predicates.extend(self._parse_condition(expression.this))
        
        return predicates
    
    def _parse_or_condition(self, expression: exp.Or) -> Optional[Predicate]:
        """
        Parse OR condition and try to convert to IN predicate.
        
        Converts:
            col = 'a' OR col = 'b' OR col = 'c'
        To:
            col IN ('a', 'b', 'c')
        
        Returns None if conversion is not possible.
        """
        # Collect all OR branches
        branches = self._flatten_or(expression)
        
        if not branches:
            return None
        
        # Check if all branches are equality conditions on same column
        column = None
        values = []
        
        for branch in branches:
            if isinstance(branch, exp.EQ):
                col = branch.left.sql()
                val = self._extract_literal(branch.right)
                
                if column is None:
                    column = col
                elif column != col:
                    # Different columns - can't convert to IN
                    return None
                
                values.append(val)
            elif isinstance(branch, exp.Paren):
                # Unwrap and check inner
                inner = branch.this
                if isinstance(inner, exp.EQ):
                    col = inner.left.sql()
                    val = self._extract_literal(inner.right)
                    
                    if column is None:
                        column = col
                    elif column != col:
                        return None
                    
                    values.append(val)
                else:
                    return None
            else:
                # Non-equality condition in OR - can't optimize
                return None
        
        if column and values:
            logger.debug(
                "Converted OR to IN: %s IN %s",
                column, values
            )
            return Predicate(column=column, operator="IN", value=values)
        
        return None
    
    def _flatten_or(self, expression: exp.Or) -> List[exp.Expression]:
        """Flatten nested OR expressions into a list of conditions."""
        result = []
        
        # Process left side
        if isinstance(expression.left, exp.Or):
            result.extend(self._flatten_or(expression.left))
        else:
            result.append(expression.left)
        
        # Process right side
        if isinstance(expression.right, exp.Or):
            result.extend(self._flatten_or(expression.right))
        else:
            result.append(expression.right)
        
        return result

    def _extract_literal(self, expression: exp.Expression) -> Union[str, int, float, bool, None, ParameterPlaceholder]:
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
