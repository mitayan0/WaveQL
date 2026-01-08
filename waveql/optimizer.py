"""
Query Optimizer - Advanced predicate extraction and query optimization

Features:
- Complex predicate extraction with nested OR support
- Subquery pushdown to single-adapter sources
- Conjunctive Normal Form (CNF) conversion for pushdown
- Predicate analysis and optimization
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from enum import Enum

import sqlglot
from sqlglot import exp

from waveql.query_planner import Predicate, QueryInfo, Aggregate

logger = logging.getLogger(__name__)


class PredicateType(Enum):
    """Type of predicate for optimization decisions."""
    SIMPLE = "simple"           # col op value
    OR_GROUP = "or_group"       # (a OR b OR c)
    AND_GROUP = "and_group"     # (a AND b AND c)  
    IN_LIST = "in_list"         # col IN (v1, v2, v3)
    BETWEEN = "between"         # col BETWEEN v1 AND v2
    NOT = "not"                 # NOT condition
    SUBQUERY = "subquery"       # col IN (SELECT ...)
    COMPLEX = "complex"         # Cannot be pushed down


@dataclass
class CompoundPredicate:
    """
    Represents a compound predicate that may contain OR conditions.
    
    This allows us to represent predicates like:
    - (status = 'open' OR status = 'pending')
    - priority > 3 AND (category = 'bug' OR category = 'feature')
    
    These can be converted to pushdown-friendly formats when supported
    by the target adapter (e.g., ServiceNow's ^OR operator).
    """
    type: PredicateType
    predicates: List[Union[Predicate, "CompoundPredicate"]] = field(default_factory=list)
    column: Optional[str] = None  # For single-column OR groups
    operator: Optional[str] = None  # For simple predicates within groups
    values: List[Any] = field(default_factory=list)  # For IN-like conversions
    
    def can_push_down(self, adapter_capabilities: Dict[str, bool] = None) -> bool:
        """
        Check if this predicate can be pushed down to the adapter.
        
        Args:
            adapter_capabilities: Dict of adapter capabilities
                - supports_or: Adapter supports OR predicates
                - supports_in: Adapter supports IN predicates
                - supports_between: Adapter supports BETWEEN
        """
        capabilities = adapter_capabilities or {}
        
        if self.type == PredicateType.SIMPLE:
            return True
        
        if self.type == PredicateType.IN_LIST:
            return capabilities.get("supports_in", True)
        
        if self.type == PredicateType.BETWEEN:
            return capabilities.get("supports_between", True)
        
        if self.type == PredicateType.OR_GROUP:
            # Can push down OR if:
            # 1. Adapter supports OR, or
            # 2. All conditions are on same column (convert to IN)
            if capabilities.get("supports_or", False):
                return all(
                    p.can_push_down(capabilities) if isinstance(p, CompoundPredicate)
                    else True
                    for p in self.predicates
                )
            # Try to convert to IN if single column
            if self.column and all(
                (isinstance(p, Predicate) and p.operator == "=")
                for p in self.predicates
            ):
                return capabilities.get("supports_in", True)
            return False
        
        if self.type == PredicateType.AND_GROUP:
            return all(
                p.can_push_down(capabilities) if isinstance(p, CompoundPredicate)
                else True
                for p in self.predicates
            )
        
        if self.type == PredicateType.SUBQUERY:
            # Subqueries handled separately
            return False
        
        return False
    
    def to_simple_predicates(self) -> List[Predicate]:
        """
        Convert to list of simple predicates for adapters without OR support.
        
        For OR groups on single column with equality, converts to IN predicate.
        For other cases, returns empty list (must filter client-side).
        """
        if self.type == PredicateType.SIMPLE and len(self.predicates) == 1:
            p = self.predicates[0]
            if isinstance(p, Predicate):
                return [p]
        
        if self.type == PredicateType.AND_GROUP:
            result = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    result.extend(p.to_simple_predicates())
                elif isinstance(p, Predicate):
                    result.append(p)
            return result
        
        if self.type == PredicateType.OR_GROUP and self.column:
            # Check if all are equality on same column -> convert to IN
            eq_values = []
            for p in self.predicates:
                if isinstance(p, Predicate) and p.operator == "=" and p.column == self.column:
                    eq_values.append(p.value)
                else:
                    # Mixed operators, can't convert
                    return []
            if eq_values:
                return [Predicate(column=self.column, operator="IN", value=eq_values)]
        
        if self.type == PredicateType.IN_LIST and self.column:
            return [Predicate(column=self.column, operator="IN", value=self.values)]
        
        return []
    
    def to_api_filter(self, dialect: str = "default") -> Optional[str]:
        """
        Convert to API-specific filter string.
        
        Args:
            dialect: API dialect (servicenow, salesforce, jira, etc.)
        
        Returns:
            Filter string or None if not pushable
        """
        if dialect == "servicenow":
            return self._to_servicenow_filter()
        elif dialect == "salesforce":
            return self._to_salesforce_filter()
        elif dialect == "jira":
            return self._to_jira_filter()
        return None
    
    def _to_servicenow_filter(self) -> Optional[str]:
        """Convert to ServiceNow query syntax with ^OR support."""
        if self.type == PredicateType.SIMPLE and self.predicates:
            p = self.predicates[0]
            if isinstance(p, Predicate):
                return p.to_api_filter("servicenow")
        
        if self.type == PredicateType.OR_GROUP:
            # ServiceNow uses ^OR between conditions
            parts = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    part = p._to_servicenow_filter()
                    if part:
                        parts.append(part)
                elif isinstance(p, Predicate):
                    parts.append(p.to_api_filter("servicenow"))
            if parts:
                return "^OR".join(parts)
        
        if self.type == PredicateType.AND_GROUP:
            # ServiceNow uses ^ between conditions
            parts = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    part = p._to_servicenow_filter()
                    if part:
                        parts.append(part)
                elif isinstance(p, Predicate):
                    parts.append(p.to_api_filter("servicenow"))
            if parts:
                return "^".join(parts)
        
        if self.type == PredicateType.IN_LIST and self.column:
            # ServiceNow IN syntax
            values_str = ",".join(str(v) for v in self.values)
            return f"{self.column}IN{values_str}"
        
        return None
    
    def _to_salesforce_filter(self) -> Optional[str]:
        """Convert to Salesforce SOQL WHERE clause."""
        if self.type == PredicateType.SIMPLE and self.predicates:
            p = self.predicates[0]
            if isinstance(p, Predicate):
                val = f"'{p.value}'" if isinstance(p.value, str) else p.value
                return f"{p.column} {p.operator} {val}"
        
        if self.type == PredicateType.OR_GROUP:
            parts = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    part = p._to_salesforce_filter()
                    if part:
                        parts.append(f"({part})")
                elif isinstance(p, Predicate):
                    val = f"'{p.value}'" if isinstance(p.value, str) else p.value
                    parts.append(f"{p.column} {p.operator} {val}")
            if parts:
                return " OR ".join(parts)
        
        if self.type == PredicateType.AND_GROUP:
            parts = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    part = p._to_salesforce_filter()
                    if part:
                        parts.append(f"({part})")
                elif isinstance(p, Predicate):
                    val = f"'{p.value}'" if isinstance(p.value, str) else p.value
                    parts.append(f"{p.column} {p.operator} {val}")
            if parts:
                return " AND ".join(parts)
        
        if self.type == PredicateType.IN_LIST and self.column:
            values_str = ", ".join(
                f"'{v}'" if isinstance(v, str) else str(v) 
                for v in self.values
            )
            return f"{self.column} IN ({values_str})"
        
        return None
    
    def _to_jira_filter(self) -> Optional[str]:
        """Convert to Jira JQL."""
        if self.type == PredicateType.SIMPLE and self.predicates:
            p = self.predicates[0]
            if isinstance(p, Predicate):
                val = f'"{p.value}"' if isinstance(p.value, str) else p.value
                op = "~" if p.operator == "LIKE" else p.operator
                return f"{p.column} {op} {val}"
        
        if self.type == PredicateType.OR_GROUP:
            parts = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    part = p._to_jira_filter()
                    if part:
                        parts.append(f"({part})")
                elif isinstance(p, Predicate):
                    val = f'"{p.value}"' if isinstance(p.value, str) else p.value
                    parts.append(f"{p.column} = {val}")
            if parts:
                return " OR ".join(parts)
        
        if self.type == PredicateType.AND_GROUP:
            parts = []
            for p in self.predicates:
                if isinstance(p, CompoundPredicate):
                    part = p._to_jira_filter()
                    if part:
                        parts.append(f"({part})")
                elif isinstance(p, Predicate):
                    val = f'"{p.value}"' if isinstance(p.value, str) else p.value
                    parts.append(f"{p.column} = {val}")
            if parts:
                return " AND ".join(parts)
        
        if self.type == PredicateType.IN_LIST and self.column:
            values_str = ", ".join(
                f'"{v}"' if isinstance(v, str) else str(v)
                for v in self.values
            )
            return f"{self.column} IN ({values_str})"
        
        return None
    
    def __repr__(self) -> str:
        if self.type == PredicateType.SIMPLE and self.predicates:
            return f"CompoundPredicate({self.predicates[0]})"
        elif self.type == PredicateType.OR_GROUP:
            return f"CompoundPredicate(OR: {self.predicates})"
        elif self.type == PredicateType.AND_GROUP:
            return f"CompoundPredicate(AND: {self.predicates})"
        elif self.type == PredicateType.IN_LIST:
            return f"CompoundPredicate({self.column} IN {self.values})"
        return f"CompoundPredicate({self.type})"


@dataclass
class SubqueryInfo:
    """Information about a subquery for pushdown optimization."""
    sql: str
    column: str  # Column being compared
    operator: str  # IN, NOT IN, EXISTS, etc.
    inner_table: Optional[str] = None
    inner_columns: List[str] = field(default_factory=list)
    inner_predicates: List[Predicate] = field(default_factory=list)
    can_push_down: bool = False
    
    def __repr__(self) -> str:
        return f"SubqueryInfo({self.column} {self.operator} SELECT FROM {self.inner_table})"


class QueryOptimizer:
    """
    Advanced query optimizer with support for:
    - Complex OR predicate extraction
    - Subquery pushdown detection
    - CNF conversion for maximum pushdown
    """
    
    def __init__(self, adapter_registry: Dict[str, Any] = None):
        """
        Initialize optimizer.
        
        Args:
            adapter_registry: Dict mapping adapter names to their capabilities
        """
        self._adapter_registry = adapter_registry or {}
        self._adapter_capabilities = {}
        
        # Default capabilities
        self._default_capabilities = {
            "supports_or": False,
            "supports_in": True,
            "supports_between": True,
            "supports_like": True,
            "supports_subquery": False,
            "max_in_values": 1000,
        }
        
        # Known adapter capabilities
        self._known_capabilities = {
            "servicenow": {
                "supports_or": True,  # ^OR operator
                "supports_in": True,
                "supports_between": True,
                "supports_like": True,
                "supports_subquery": False,
                "max_in_values": 500,
            },
            "salesforce": {
                "supports_or": True,
                "supports_in": True,
                "supports_between": False,
                "supports_like": True,
                "supports_subquery": True,  # SOQL supports some subqueries
                "max_in_values": 2000,
            },
            "jira": {
                "supports_or": True,
                "supports_in": True,
                "supports_between": False,
                "supports_like": True,  # Uses ~
                "supports_subquery": False,
                "max_in_values": 100,
            },
        }
    
    def get_adapter_capabilities(self, adapter_name: str) -> Dict[str, Any]:
        """Get capabilities for an adapter."""
        if adapter_name in self._known_capabilities:
            return self._known_capabilities[adapter_name]
        return self._default_capabilities.copy()
    
    def extract_complex_predicates(
        self, 
        expression: exp.Expression
    ) -> Tuple[List[CompoundPredicate], List[SubqueryInfo]]:
        """
        Extract complex predicates including OR groups from WHERE clause.
        
        Args:
            expression: sqlglot WHERE expression
            
        Returns:
            Tuple of (compound_predicates, subqueries)
        """
        compound_predicates = []
        subqueries = []
        
        self._extract_recursive(expression, compound_predicates, subqueries)
        
        return compound_predicates, subqueries
    
    def _extract_recursive(
        self,
        expression: exp.Expression,
        predicates: List[CompoundPredicate],
        subqueries: List[SubqueryInfo],
        parent_type: PredicateType = None
    ) -> Optional[CompoundPredicate]:
        """Recursively extract predicates from expression tree."""
        
        if isinstance(expression, exp.Paren):
            # Unwrap parentheses
            return self._extract_recursive(expression.this, predicates, subqueries, parent_type)
        
        if isinstance(expression, exp.And):
            # AND group
            left_result = self._extract_recursive(expression.left, predicates, subqueries, PredicateType.AND_GROUP)
            right_result = self._extract_recursive(expression.right, predicates, subqueries, PredicateType.AND_GROUP)
            
            children = []
            if left_result:
                children.append(left_result)
            if right_result:
                children.append(right_result)
            
            if children:
                compound = CompoundPredicate(type=PredicateType.AND_GROUP, predicates=children)
                if parent_type is None:
                    # Top level AND - flatten to list
                    predicates.extend(children)
                return compound
            return None
        
        if isinstance(expression, exp.Or):
            # OR group - this is the key enhancement
            left_result = self._extract_recursive(expression.left, [], subqueries, PredicateType.OR_GROUP)
            right_result = self._extract_recursive(expression.right, [], subqueries, PredicateType.OR_GROUP)
            
            children = []
            if left_result:
                # Flatten nested OR groups
                if left_result.type == PredicateType.OR_GROUP:
                    children.extend(left_result.predicates)
                else:
                    children.append(left_result)
            if right_result:
                if right_result.type == PredicateType.OR_GROUP:
                    children.extend(right_result.predicates)
                else:
                    children.append(right_result)
            
            if children:
                # Check if all children are on same column (for IN optimization)
                columns = set()
                for child in children:
                    if isinstance(child, CompoundPredicate) and child.predicates:
                        p = child.predicates[0]
                        if isinstance(p, Predicate):
                            columns.add(p.column)
                    elif isinstance(child, Predicate):
                        columns.add(child.column)
                
                single_column = columns.pop() if len(columns) == 1 else None
                
                compound = CompoundPredicate(
                    type=PredicateType.OR_GROUP,
                    predicates=children,
                    column=single_column
                )
                
                if parent_type is None or parent_type == PredicateType.AND_GROUP:
                    predicates.append(compound)
                
                return compound
            return None
        
        # Handle comparison operators
        if isinstance(expression, (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Like)):
            pred = self._extract_simple_predicate(expression)
            if pred:
                compound = CompoundPredicate(
                    type=PredicateType.SIMPLE,
                    predicates=[pred]
                )
                if parent_type is None:
                    predicates.append(compound)
                return compound
        
        # Handle IN
        if isinstance(expression, exp.In):
            # Check for subquery
            query = expression.args.get("query")
            if query:
                subquery_info = self._extract_subquery(expression)
                if subquery_info:
                    subqueries.append(subquery_info)
                    return CompoundPredicate(type=PredicateType.SUBQUERY)
            
            # Regular IN list
            pred = self._extract_in_predicate(expression)
            if pred:
                compound = CompoundPredicate(
                    type=PredicateType.IN_LIST,
                    column=pred.column,
                    values=pred.value if isinstance(pred.value, list) else [pred.value],
                    predicates=[pred]
                )
                if parent_type is None:
                    predicates.append(compound)
                return compound
        
        # Handle BETWEEN
        if isinstance(expression, exp.Between):
            pred = self._extract_between_predicate(expression)
            if pred:
                compound = CompoundPredicate(
                    type=PredicateType.BETWEEN,
                    predicates=pred  # Returns two predicates
                )
                if parent_type is None:
                    predicates.append(compound)
                return compound
        
        # Handle IS NULL / IS NOT NULL
        if isinstance(expression, exp.Is):
            pred = self._extract_is_predicate(expression)
            if pred:
                compound = CompoundPredicate(
                    type=PredicateType.SIMPLE,
                    predicates=[pred]
                )
                if parent_type is None:
                    predicates.append(compound)
                return compound
        
        # Handle NOT
        if isinstance(expression, exp.Not):
            inner = self._extract_recursive(expression.this, [], subqueries, PredicateType.NOT)
            if inner:
                return CompoundPredicate(type=PredicateType.NOT, predicates=[inner])
        
        return None
    
    def _extract_simple_predicate(self, expression: exp.Expression) -> Optional[Predicate]:
        """Extract a simple comparison predicate."""
        op_map = {
            exp.EQ: "=",
            exp.NEQ: "!=",
            exp.LT: "<",
            exp.LTE: "<=",
            exp.GT: ">",
            exp.GTE: ">=",
            exp.Like: "LIKE",
        }
        
        operator = op_map.get(type(expression))
        if not operator:
            return None
        
        column = expression.left.sql()
        value = self._extract_literal(expression.right)
        
        return Predicate(column=column, operator=operator, value=value)
    
    def _extract_in_predicate(self, expression: exp.In) -> Optional[Predicate]:
        """Extract IN predicate."""
        column = expression.this.sql()
        
        # Get values
        field_expr = expression.args.get("expressions") or []
        if not field_expr:
            # Try to get from query or other forms
            query = expression.args.get("query")
            if query:
                return None  # Subquery, handled separately
            
            # Try tuple form
            unnest = expression.args.get("unnest")
            if unnest:
                return None
        
        values = [self._extract_literal(v) for v in field_expr]
        
        return Predicate(column=column, operator="IN", value=values)
    
    def _extract_between_predicate(self, expression: exp.Between) -> List[Predicate]:
        """Extract BETWEEN as two predicates."""
        column = expression.this.sql()
        low = self._extract_literal(expression.args.get("low"))
        high = self._extract_literal(expression.args.get("high"))
        
        return [
            Predicate(column=column, operator=">=", value=low),
            Predicate(column=column, operator="<=", value=high),
        ]
    
    def _extract_is_predicate(self, expression: exp.Is) -> Optional[Predicate]:
        """Extract IS NULL / IS NOT NULL predicate."""
        column = expression.left.sql()
        
        if isinstance(expression.right, exp.Null):
            return Predicate(column=column, operator="IS NULL", value=None)
        
        return None
    
    def _extract_subquery(self, expression: exp.In) -> Optional[SubqueryInfo]:
        """Extract subquery information for pushdown analysis."""
        column = expression.this.sql()
        query = expression.args.get("query")
        
        if not query:
            return None
        
        # Parse the subquery
        select = query.this if isinstance(query, exp.Subquery) else query
        
        if not isinstance(select, exp.Select):
            return None
        
        # Extract subquery components
        inner_table = None
        for table in select.find_all(exp.Table):
            inner_table = table.sql()
            break
        
        inner_columns = [col.sql() for col in select.expressions]
        
        # Extract inner predicates
        inner_predicates = []
        where = select.args.get("where")
        if where:
            from waveql.query_planner import QueryPlanner
            planner = QueryPlanner()
            inner_predicates = planner._parse_condition(where.this)
        
        return SubqueryInfo(
            sql=select.sql(),
            column=column,
            operator="IN",
            inner_table=inner_table,
            inner_columns=inner_columns,
            inner_predicates=inner_predicates,
            can_push_down=False  # Determined later based on adapter
        )
    
    def _extract_literal(self, expression: exp.Expression) -> Any:
        """Extract Python value from sqlglot expression."""
        if isinstance(expression, exp.Literal):
            if expression.is_number:
                text = expression.this
                return float(text) if "." in text else int(text)
            return expression.this
        elif isinstance(expression, exp.Boolean):
            return expression.this
        elif isinstance(expression, exp.Null):
            return None
        return expression.sql()
    
    def optimize_for_adapter(
        self,
        predicates: List[CompoundPredicate],
        subqueries: List[SubqueryInfo],
        adapter_name: str
    ) -> Tuple[List[Predicate], List[CompoundPredicate], List[SubqueryInfo]]:
        """
        Optimize predicates for a specific adapter.
        
        Args:
            predicates: Compound predicates from extraction
            subqueries: Subquery information
            adapter_name: Target adapter name
            
        Returns:
            Tuple of:
            - pushable_simple: Simple predicates that can be pushed
            - pushable_compound: Compound predicates (OR groups) that can be pushed
            - pushable_subqueries: Subqueries that can be pushed
        """
        capabilities = self.get_adapter_capabilities(adapter_name)
        
        pushable_simple = []
        pushable_compound = []
        
        for pred in predicates:
            if pred.can_push_down(capabilities):
                if pred.type == PredicateType.SIMPLE:
                    pushable_simple.extend(pred.to_simple_predicates())
                elif pred.type == PredicateType.OR_GROUP:
                    # Try to convert to IN first
                    simple_version = pred.to_simple_predicates()
                    if simple_version:
                        pushable_simple.extend(simple_version)
                    elif capabilities.get("supports_or"):
                        pushable_compound.append(pred)
                elif pred.type == PredicateType.AND_GROUP:
                    pushable_simple.extend(pred.to_simple_predicates())
                elif pred.type == PredicateType.IN_LIST:
                    pushable_simple.extend(pred.to_simple_predicates())
        
        # Check subqueries
        pushable_subqueries = []
        for sq in subqueries:
            if self._can_push_subquery(sq, adapter_name):
                sq.can_push_down = True
                pushable_subqueries.append(sq)
        
        return pushable_simple, pushable_compound, pushable_subqueries
    
    def _can_push_subquery(self, subquery: SubqueryInfo, adapter_name: str) -> bool:
        """
        Check if a subquery can be pushed to the adapter.
        
        A subquery can be pushed if:
        1. The adapter supports subqueries
        2. The inner table is on the same adapter
        3. The subquery is a simple SELECT with pushable predicates
        """
        capabilities = self.get_adapter_capabilities(adapter_name)
        
        if not capabilities.get("supports_subquery"):
            return False
        
        # Check if inner table is on same adapter
        if subquery.inner_table:
            inner_adapter = self._resolve_adapter_for_table(subquery.inner_table)
            if inner_adapter != adapter_name:
                return False
        
        return True
    
    def _resolve_adapter_for_table(self, table_name: str) -> Optional[str]:
        """Resolve which adapter handles a table."""
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            schema = schema.strip('"')
            return schema
        return None
    
    def convert_or_to_in(
        self,
        predicates: List[CompoundPredicate]
    ) -> List[Predicate]:
        """
        Convert OR groups on same column to IN predicates.
        
        Example:
            (status = 'open' OR status = 'closed' OR status = 'pending')
            -> status IN ('open', 'closed', 'pending')
        """
        result = []
        
        for pred in predicates:
            if pred.type == PredicateType.OR_GROUP and pred.column:
                # All conditions on same column
                values = []
                can_convert = True
                
                for p in pred.predicates:
                    if isinstance(p, CompoundPredicate) and p.type == PredicateType.SIMPLE:
                        inner = p.predicates[0]
                        if isinstance(inner, Predicate) and inner.operator == "=":
                            values.append(inner.value)
                        else:
                            can_convert = False
                            break
                    elif isinstance(p, Predicate) and p.operator == "=":
                        values.append(p.value)
                    else:
                        can_convert = False
                        break
                
                if can_convert and values:
                    result.append(Predicate(
                        column=pred.column,
                        operator="IN",
                        value=values
                    ))
            elif pred.type == PredicateType.SIMPLE:
                result.extend(pred.to_simple_predicates())
            elif pred.type == PredicateType.AND_GROUP:
                result.extend(pred.to_simple_predicates())
            elif pred.type == PredicateType.IN_LIST:
                result.extend(pred.to_simple_predicates())
        
        return result
    
    def reorder_joins(
        self,
        tables: List[str],
        predicates: Dict[str, List["Predicate"]],
        connection: Any,
    ) -> List[str]:
        """
        Reorder join tables based on CBO (Cost-Based Optimization).
        
        Prioritizes tables that are likely to be smaller (lower cardinality)
        and faster to fetch, to serve as the driver for semi-join pushdown.
        
        Cost Formula:
            Cost = EstimatedRows * AdapterLatency * SelectivityFactor
            
        Args:
            tables: List of normalized table names (e.g. "servicenow.incident")
            predicates: Dict mapping table name to list of predicates
            connection: Connection object to access adapters and their history
            
        Returns:
            Sorted list of table names
        """
        table_costs = []
        
        for table_name in tables:
            # 1. Resolve Adapter
            adapter_name = "default"
            if "." in table_name:
                adapter_name, _ = table_name.split(".", 1)
                adapter_name = adapter_name.strip('"')
            
            adapter = connection.get_adapter(adapter_name)
            
            # Default metrics if adapter/history not found
            # Default rows: 1000 (modest assumption)
            # Default latency: 0.001 (1ms - optimistic)
            latency_per_row = 0.001
            avg_rows = 1000.0
            
            if adapter:
                latency_per_row = getattr(adapter, "avg_latency_per_row", 0.001)
                history = getattr(adapter, "_execution_history", [])
                if history:
                    # simplistic: average of last executions
                    # If history is empty, use default
                    total_rows = sum(h.get("rows", 0) for h in history)
                    avg_rows = total_rows / len(history)
            
            # 2. Calculate Selectivity
            # Start with 1.0 (100%)
            selectivity = 1.0
            table_preds = predicates.get(table_name, [])
            
            for pred in table_preds:
                # Naive selectivity estimation
                op = str(pred.operator).upper()
                if op == "=":
                    selectivity *= 0.1  # High selectivity (10% remain)
                elif op in ("IN", "BETWEEN", "<", ">", "<=", ">="):
                    selectivity *= 0.5  # Medium selectivity (50% remain)
                else:
                    selectivity *= 0.9  # Low selectivity (90% remain)
            
            # 3. Calculate Score (Estimated Duration)
            estimated_rows = max(1, avg_rows * selectivity)
            estimated_duration = estimated_rows * latency_per_row
            
            table_costs.append({
                "table": table_name,
                "score": estimated_duration,
                "rows": estimated_rows,
                "latency": latency_per_row
            })
            
        # Sort by score ascending (cheaper/faster first)
        sorted_costs = sorted(table_costs, key=lambda x: x["score"])
        
        return [item["table"] for item in sorted_costs]


class SubqueryPushdownOptimizer:
    """
    Optimizer for pushing down subqueries to single-adapter sources.
    
    When both outer and inner queries target the same adapter,
    we can potentially push the entire subquery to avoid round-trips.
    """
    
    def __init__(self, adapter_registry: Dict[str, Any] = None):
        self._adapter_registry = adapter_registry or {}
    
    def analyze_subquery(
        self,
        outer_query: QueryInfo,
        subquery: SubqueryInfo,
        registered_adapters: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Analyze if a subquery can be pushed down.
        
        Args:
            outer_query: The main query info
            subquery: The subquery to analyze
            registered_adapters: Mapping of schemas to adapter names
            
        Returns:
            Analysis result with pushdown recommendation
        """
        result = {
            "can_push": False,
            "reason": None,
            "strategy": None,
            "optimized_predicates": [],
        }
        
        # Determine adapter for outer query
        outer_adapter = self._get_adapter_for_table(
            outer_query.table, registered_adapters
        )
        
        # Determine adapter for inner query
        inner_adapter = self._get_adapter_for_table(
            subquery.inner_table, registered_adapters
        )
        
        if not outer_adapter or not inner_adapter:
            result["reason"] = "Could not determine adapter"
            return result
        
        if outer_adapter != inner_adapter:
            result["reason"] = "Cross-adapter subquery - cannot push down"
            result["strategy"] = "materialize_inner"
            return result
        
        # Same adapter - can potentially push down
        result["can_push"] = True
        result["strategy"] = "push_entire"
        result["reason"] = f"Both queries on {outer_adapter} adapter"
        
        # Generate optimized predicates for the inner query
        # This avoids fetching inner results first
        if subquery.inner_predicates:
            result["optimized_predicates"] = subquery.inner_predicates
        
        return result
    
    def _get_adapter_for_table(
        self,
        table_name: str,
        registered_adapters: Dict[str, str]
    ) -> Optional[str]:
        """Get adapter name for a table."""
        if not table_name:
            return None
        
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            schema = schema.strip('"')
            return registered_adapters.get(schema)
        
        return registered_adapters.get("default")
    
    def optimize_subquery_execution(
        self,
        subquery: SubqueryInfo,
        adapter
    ) -> Optional[List[Any]]:
        """
        Execute subquery and return result for IN predicate.
        
        This is used when we can't push the subquery but need its results
        for the outer query's IN predicate.
        
        Args:
            subquery: The subquery information
            adapter: The adapter to execute on
            
        Returns:
            List of values for IN predicate, or None if failed
        """
        try:
            # Extract table name from inner table
            table = subquery.inner_table
            if "." in table:
                _, table = table.rsplit(".", 1)
            table = table.strip('"')
            
            # Fetch with inner predicates
            result = adapter.fetch(
                table=table,
                columns=subquery.inner_columns,
                predicates=subquery.inner_predicates,
            )
            
            if result and len(result) > 0:
                # Extract values from first column
                col_name = subquery.inner_columns[0] if subquery.inner_columns else result.column_names[0]
                # Clean column name
                if "." in col_name:
                    _, col_name = col_name.rsplit(".", 1)
                col_name = col_name.strip('"')
                
                values = result.column(col_name).to_pylist()
                return [v for v in values if v is not None]
        except Exception as e:
            logger.warning(f"Failed to optimize subquery: {e}")
        
        return None
