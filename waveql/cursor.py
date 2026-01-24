"""
WaveQL Cursor - DB-API 2.0 compliant cursor with predicate pushdown
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING
import re
import uuid
import logging
import time
import pyarrow as pa

from waveql.exceptions import QueryError
from waveql.query_planner import QueryPlanner
from waveql.optimizer import QueryOptimizer, CompoundPredicate, PredicateType
from waveql.observability import QueryPlan
from waveql.resource_optimizer import (
    get_budget_planner,
    get_cardinality_estimator,
    get_adaptive_pagination,
    get_resource_executor,
)
from waveql.provenance.tracker import get_provenance_tracker
from waveql.provenance.traced_adapter import traced_fetch

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


class Row:
    """
    A row object that supports both tuple-like indexing and dict-like key access.
    """
    def __init__(self, data: Dict[str, Any], schema: List[Tuple]):
        self._data = data
        self._schema = schema
        self._keys = [col[0] for col in schema]
        self._values = tuple(data[k] for k in self._keys)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._data[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        return f"Row({self._data})"

    def keys(self):
        return self._keys

    def values(self):
        return self._values

    def items(self):
        return self._data.items()

    def as_dict(self):
        return self._data

    def __getattr__(self, name):
        """Allow attribute-style access: row.column_name"""
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"'Row' object has no attribute '{name}'")


class WaveQLCursor:
    """
    DB-API 2.0 compliant cursor with intelligent query routing.
    
    Features:
    - Predicate pushdown to adapters
    - Automatic schema discovery
    - Arrow-native data handling
    - Virtual table registration in DuckDB
    """
    
    def __init__(self, connection: "WaveQLConnection"):
        self._connection = connection
        self._description: Optional[List[Tuple]] = None
        self._rowcount = -1
        self._arraysize = 100
        self._closed = False
        
        # Current result set
        self._result: Optional[pa.Table] = None
        self._result_index = 0
        
        
        # Query planner for predicate extraction
        self._planner = QueryPlanner()
        
        # Query optimizer for predicate classification (pushable vs residual)
        self._optimizer = QueryOptimizer()
        
        # Budget planner for WITH BUDGET queries
        self._budget_planner = get_budget_planner()
        
        # Resource executor for unified optimization
        self._resource_executor = get_resource_executor(connection)
        
        # Last execution plan
        self.last_plan: Optional[QueryPlan] = None
        
        # Budget from last query (if specified)
        self.last_budget = None
    
    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        DB-API 2.0 description attribute.
        Returns sequence of 7-item tuples describing columns.
        """
        return self._description
    
    @property
    def rowcount(self) -> int:
        """Number of rows affected by last operation."""
        return self._rowcount
    
    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize
    
    @arraysize.setter
    def arraysize(self, value: int):
        self._arraysize = value
    
    def execute(self, operation: str, parameters: Sequence = None) -> "WaveQLCursor":
        """
        Execute a SQL query.
        
        Args:
            operation: SQL query string
            parameters: Query parameters (for parameterized queries)
            
        Returns:
            Self for method chaining
            
        Budget-Constrained Queries:
            Supports `WITH BUDGET <time>` syntax for resource-limited execution.
            Example: `SELECT * FROM incidents WITH BUDGET 500ms`
            
            Supported units: ms, s/seconds, rows
            
            When budget is exhausted, partial results are returned.
            Check `cursor.last_budget.is_exhausted` for status.
        """
        if self._closed:
            raise QueryError("Cursor is closed")
        
        # Parse budget constraint if present (e.g., "WITH BUDGET 500ms")
        cleaned_operation, budget = self._budget_planner.parse_budget(operation)
        self.last_budget = budget
        
        if budget:
            budget.start()  # Start budget tracking
            
        # Expand virtual views if any (Macro expansion)
        if self._connection._virtual_views:
            cleaned_operation = self._planner.expand_views(
                cleaned_operation, self._connection._virtual_views
            )
        
        # Parse query to extract table, predicates, etc.
        query_info = self._planner.parse(cleaned_operation)
        
        # Apply Row-Level Security policies (inject predicates)
        if self._connection._policy_manager:
            query_info = self._apply_rls_policies(query_info, cleaned_operation)
        
        # Initialize execution plan
        self.last_plan = QueryPlan(sql=operation, is_explain=query_info.is_explain)
        
        if budget:
            self.last_plan.add_step(
                name=f"Budget: {budget.value} {budget.unit.value}",
                type="budget",
                details={"value": budget.value, "unit": budget.unit.value}
            ).finish()
        
        # Determine which adapter to use
        adapter = self._resolve_adapter(query_info)
        
        # Wrap execution with provenance tracking
        tracker = get_provenance_tracker()
        with tracker.trace_query(operation) as _prov:
            try:
                if query_info.is_hybrid:
                    # Handle hybrid query (Historical + Live)
                    self._result = self._execute_hybrid(query_info, operation, parameters)
                elif query_info.joins:
                    # Handle virtual join across adapters
                    self._result = self._execute_virtual_join(query_info, operation, parameters)
                elif adapter:
                    # Route to adapter with predicate pushdown
                    self._result = self._execute_via_adapter(query_info, adapter, parameters)
                else:
                    # Fall back to direct DuckDB execution
                    self._result = self._execute_direct(operation, parameters)
            finally:
                self.last_plan.finish()
                
                # Update provenance with row count
                if _prov and self._result is not None:
                    _prov.total_rows = len(self._result)
        
        if query_info.is_explain:
            # For EXPLAIN, return the plan as a single-column table
            plan_text = self.last_plan.format_text()
            self._result = pa.Table.from_pydict({"Execution Plan": [plan_text]})
            self._rowcount = 1
        
        # Update description from result schema
        self._update_description()
        self._result_index = 0
        
        return self
    
    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence]) -> "WaveQLCursor":
        """Execute operation for each parameter set (for batch INSERT/UPDATE)."""
        if self._closed:
            raise QueryError("Cursor is closed")
        
        query_info = self._planner.parse(operation)
        adapter = self._resolve_adapter(query_info)
        
        if adapter and query_info.operation in ("INSERT", "UPDATE", "DELETE"):
            # Batch operation via adapter
            self._rowcount = adapter.execute_batch(query_info, seq_of_parameters)
        else:
            # Execute one by one
            total = 0
            for params in seq_of_parameters:
                self.execute(operation, params)
                if self._rowcount > 0:
                    total += self._rowcount
            self._rowcount = total
        
        return self
    
    def _clean_table_name(self, table_name: str) -> str:
        """
        Clean a table name by stripping quotes and extracting just the table portion.
        
        Examples:
            '"servicenow"."incident"' -> 'incident'
            'servicenow.incident'      -> 'incident'
            '"incident"'               -> 'incident'
            'incident'                 -> 'incident'
        """
        if not table_name:
            return table_name
        
        # Normalize: Remove alias if any and strip quotes
        # Table name might be '"schema"."table" AS "alias"'
        name = table_name.split()[0]
        
        # If there's a schema prefix, extract just the table name
        if "." in name:
            _, table_part = name.rsplit(".", 1)
        else:
            table_part = name
        
        # Strip surrounding quotes
        return table_part.strip('"')

    def _normalize_table_name(self, table_name: str) -> str:
        """
        Normalize a table name to include schema and name without quotes or aliases.
        Example: '"servicenow"."incident" AS i' -> 'servicenow.incident'
        """
        if not table_name:
            return table_name
        
        # Remove alias
        name = table_name.split()[0]
        
        # Split into parts and strip quotes
        parts = [p.strip('"') for p in name.split(".")]
        return ".".join(parts)
    
    def _apply_rls_policies(self, query_info, sql: str):
        """
        Apply Row-Level Security policies to the query.
        
        This method injects policy predicates into the query_info's predicate list,
        enabling predicate pushdown to adapters while maintaining security.
        
        Args:
            query_info: Parsed query information
            sql: Original SQL string (for SQL-level rewriting if needed)
            
        Returns:
            Modified query_info with policy predicates injected
        """
        from waveql.query_planner import Predicate
        
        if not query_info.table:
            return query_info
        
        # Normalize table name for policy lookup
        table = self._normalize_table_name(query_info.table)
        operation = query_info.operation
        
        # Get applicable policies
        policies = self._connection._policy_manager.get_applicable_policies(table, operation)
        if not policies:
            return query_info
        
        # Log policy application for audit
        policy_names = [p.name for p in policies]
        logger.debug("RLS: Applying policies %s to %s.%s", policy_names, table, operation)
        
        # Parse policy predicates and inject into query_info
        # Note: We inject at the predicate level for pushdown compatibility
        for policy in policies:
            predicate_sql = policy.get_predicate()
            
            # Parse the policy predicate to extract structured Predicate objects
            policy_predicates = self._parse_policy_predicate(predicate_sql)
            
            # Append policy predicates to existing predicates
            # All predicates are ANDed together during execution
            query_info.predicates.extend(policy_predicates)
        
        # Also store raw combined predicate for DuckDB fallback execution
        combined = self._connection._policy_manager.build_combined_predicate(table, operation)
        if combined:
            # Store for use in _execute_direct if needed
            query_info.rls_predicate = combined
        
        return query_info
    
    def _parse_policy_predicate(self, predicate_sql: str) -> list:
        """
        Parse a policy predicate SQL fragment into Predicate objects.
        
        Handles simple predicates like:
        - column = 'value'
        - column IN (1, 2, 3)
        - column > 10
        
        For complex predicates (OR, nested), returns a single Predicate
        with the full SQL as a 'RAW' operator for later injection.
        
        Args:
            predicate_sql: SQL WHERE clause fragment
            
        Returns:
            List of Predicate objects
        """
        from waveql.query_planner import Predicate
        import sqlglot
        from sqlglot import exp
        
        predicates = []
        
        try:
            # Parse the predicate as part of a SELECT statement
            parsed = sqlglot.parse_one(f"SELECT * FROM t WHERE {predicate_sql}")
            where = parsed.find(exp.Where)
            
            if where:
                predicates.extend(self._extract_predicates_from_expression(where.this))
        except Exception as e:
            # If parsing fails, create a RAW predicate for SQL-level injection
            logger.debug("RLS: Complex predicate, using RAW: %s", predicate_sql)
            predicates.append(Predicate(
                column="__RLS_RAW__",
                operator="RAW",
                value=predicate_sql
            ))
        
        return predicates
    
    def _extract_predicates_from_expression(self, expr) -> list:
        """
        Recursively extract Predicate objects from a sqlglot expression.
        
        Args:
            expr: sqlglot expression node
            
        Returns:
            List of Predicate objects
        """
        from waveql.query_planner import Predicate
        from sqlglot import exp
        
        predicates = []
        
        if isinstance(expr, exp.And):
            # Recursively handle AND
            predicates.extend(self._extract_predicates_from_expression(expr.this))
            predicates.extend(self._extract_predicates_from_expression(expr.expression))
        
        elif isinstance(expr, exp.EQ):
            # column = value
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator="=", value=value))
        
        elif isinstance(expr, exp.NEQ):
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator="!=", value=value))
        
        elif isinstance(expr, exp.GT):
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator=">", value=value))
        
        elif isinstance(expr, exp.GTE):
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator=">=", value=value))
        
        elif isinstance(expr, exp.LT):
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator="<", value=value))
        
        elif isinstance(expr, exp.LTE):
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator="<=", value=value))
        
        elif isinstance(expr, exp.In):
            column = self._get_column_name(expr.this)
            values = []
            for item in expr.expressions:
                values.append(self._get_literal_value(item))
            if column:
                predicates.append(Predicate(column=column, operator="IN", value=values))
        
        elif isinstance(expr, exp.Like):
            column = self._get_column_name(expr.this)
            value = self._get_literal_value(expr.expression)
            if column:
                predicates.append(Predicate(column=column, operator="LIKE", value=value))
        
        elif isinstance(expr, exp.Is):
            column = self._get_column_name(expr.this)
            if isinstance(expr.expression, exp.Null):
                predicates.append(Predicate(column=column, operator="IS", value=None))
        
        else:
            # Fallback: Complex predicate, use RAW
            predicates.append(Predicate(
                column="__RLS_RAW__",
                operator="RAW",
                value=expr.sql()
            ))
        
        return predicates
    
    def _get_column_name(self, expr) -> Optional[str]:
        """Extract column name from a sqlglot expression."""
        from sqlglot import exp
        
        if isinstance(expr, exp.Column):
            return expr.name
        if isinstance(expr, exp.Identifier):
            return expr.name
        if hasattr(expr, 'name'):
            return expr.name
        return None
    
    def _get_literal_value(self, expr):
        """Extract literal value from a sqlglot expression."""
        from sqlglot import exp
        
        if isinstance(expr, exp.Literal):
            if expr.is_string:
                return expr.this
            elif expr.is_number:
                # Try to return int or float
                try:
                    if '.' in str(expr.this):
                        return float(expr.this)
                    return int(expr.this)
                except ValueError:
                    return expr.this
        if isinstance(expr, exp.Null):
            return None
        if isinstance(expr, exp.Boolean):
            return expr.this
        # Fallback: return string representation
        return str(expr)
    
    def _resolve_adapter(self, query_info):
        """Determine which adapter handles this query based on table name."""
        table_name = query_info.table
        if not table_name:
            return None
        
        # 1. Check if it's a materialized view
        # Use normalized name for view lookup
        clean_table = self._normalize_table_name(query_info.table)
        
        try:
             # Lazy check to avoid cyclic imports or init issues
             if hasattr(self._connection, 'view_manager') and self._connection.view_manager.exists(clean_table):
                 # If it's a hybrid query, we want the adapter to fetch the fresh part
                 if query_info.is_hybrid:
                      view_info = self._connection.view_manager.get(clean_table)
                      return self._connection.get_adapter(view_info.definition.source_adapter)
                 return None # Execute locally in DuckDB
        except Exception:
             pass
        
        # Check for schema prefix (e.g., "sales.Account" or "servicenow"."incident")
        # Use the original table_name for schema extraction as it might contain quotes
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            # Strip quotes from schema name for lookup
            schema = schema.strip('"')
            adapter = self._connection.get_adapter(schema)
            if adapter:
                return adapter
        
        # Use default adapter
        return self._connection.get_adapter("default")
    
    def _classify_predicates(self, query_info, adapter) -> tuple:
        """
        Classify predicates into pushable and residual categories.
        
        This is the key integration point with QueryOptimizer. It ensures:
        1. Complex OR conditions are properly handled
        2. Predicates that can't be pushed are filtered client-side
        3. No silent dropping of unsupported predicate logic
        
        Args:
            query_info: Parsed query information with predicates
            adapter: Target adapter
            
        Returns:
            Tuple of (pushable_predicates, residual_predicates, has_residual)
            - pushable_predicates: List[Predicate] to send to adapter
            - residual_predicates: List[CompoundPredicate] to filter client-side
            - has_residual: bool indicating if client-side filtering is needed
        """
        from waveql.query_planner import Predicate
        import sqlglot
        from sqlglot import exp
        
        pushable = []
        residual = []
        
        # Get adapter capabilities
        adapter_name = getattr(adapter, 'adapter_name', 'default')
        capabilities = self._optimizer.get_adapter_capabilities(adapter_name)
        
        # If no predicates, return early
        if not query_info.predicates:
            return pushable, residual, False
        
        # We need to re-examine the original WHERE clause for complex OR conditions
        # that the simple QueryPlanner might have dropped or logged as warnings
        try:
            parsed = sqlglot.parse_one(query_info.raw_sql, read="duckdb")
            where = parsed.find(exp.Where)
            
            if where:
                # Use QueryOptimizer's advanced extraction
                compound_preds, subqueries = self._optimizer.extract_complex_predicates(where.this)
                
                # Now classify each compound predicate
                for cp in compound_preds:
                    if cp.can_push_down(capabilities):
                        # Convert to simple predicates for pushdown
                        simple = cp.to_simple_predicates()
                        pushable.extend(simple)
                    else:
                        # Cannot push - add to residual for client-side filtering
                        residual.append(cp)
                        logger.info(
                            "Predicate cannot be pushed to %s, will filter client-side: %s",
                            adapter_name, cp
                        )
                
                has_residual = len(residual) > 0
                
                # If we successfully processed with optimizer, return
                if compound_preds:
                    return pushable, residual, has_residual
                    
        except Exception as e:
            logger.debug("Failed to use QueryOptimizer for predicate classification: %s", e)
        
        # Fallback: Use predicates from QueryPlanner directly
        # All predicates from the simple planner are considered pushable
        pushable = query_info.predicates[:]
        return pushable, [], False
    
    def _apply_residual_filter(self, data: pa.Table, residual_predicates: list, query_info) -> pa.Table:
        """
        Apply residual predicates as client-side filtering via DuckDB.
        
        This is the "Safety Net" that ensures correctness for complex predicates
        that could not be pushed to the adapter.
        
        Args:
            data: Arrow table with fetched data
            residual_predicates: List of CompoundPredicate objects to filter
            query_info: Original query info for context
            
        Returns:
            Filtered Arrow table
        """
        if not residual_predicates or data is None or len(data) == 0:
            return data
        
        import uuid
        
        # Register data in DuckDB
        temp_name = f"residual_{uuid.uuid4().hex}"
        self._connection._duckdb.register(temp_name, data)
        
        try:
            # Build WHERE clause from residual predicates
            where_parts = []
            for cp in residual_predicates:
                sql_part = self._compound_predicate_to_sql(cp)
                if sql_part:
                    where_parts.append(f"({sql_part})")
            
            if not where_parts:
                return data
            
            where_clause = " AND ".join(where_parts)
            filter_sql = f'SELECT * FROM "{temp_name}" WHERE {where_clause}'
            
            # Add to execution plan
            step = self.last_plan.add_step(
                name="Client-side residual filter",
                type="filter",
                details={
                    "predicates": [str(p) for p in residual_predicates],
                    "sql": filter_sql,
                    "input_rows": len(data),
                }
            )
            
            result = self._connection._duckdb.execute(filter_sql).fetch_arrow_table()
            
            step.details["output_rows"] = len(result)
            step.finish()
            
            logger.info(
                "Residual filter: %d rows -> %d rows",
                len(data), len(result)
            )
            
            return result
            
        finally:
            self._connection._duckdb.unregister(temp_name)
    
    def _compound_predicate_to_sql(self, cp) -> str:
        """
        Convert a CompoundPredicate to SQL WHERE clause fragment.
        
        Args:
            cp: CompoundPredicate object
            
        Returns:
            SQL string for WHERE clause
        """
        from waveql.query_planner import Predicate
        
        if cp.type == PredicateType.SIMPLE and cp.predicates:
            p = cp.predicates[0]
            if isinstance(p, Predicate):
                return self._predicate_to_sql(p)
        
        if cp.type == PredicateType.OR_GROUP:
            parts = []
            for p in cp.predicates:
                if isinstance(p, CompoundPredicate):
                    part = self._compound_predicate_to_sql(p)
                    if part:
                        parts.append(f"({part})")
                elif isinstance(p, Predicate):
                    parts.append(self._predicate_to_sql(p))
            if parts:
                return " OR ".join(parts)
        
        if cp.type == PredicateType.AND_GROUP:
            parts = []
            for p in cp.predicates:
                if isinstance(p, CompoundPredicate):
                    part = self._compound_predicate_to_sql(p)
                    if part:
                        parts.append(f"({part})")
                elif isinstance(p, Predicate):
                    parts.append(self._predicate_to_sql(p))
            if parts:
                return " AND ".join(parts)
        
        if cp.type == PredicateType.IN_LIST and cp.column:
            values_str = ", ".join(
                f"'{v}'" if isinstance(v, str) else str(v)
                for v in cp.values
            )
            return f"{cp.column} IN ({values_str})"
        
        return ""
    
    def _predicate_to_sql(self, p) -> str:
        """Convert a simple Predicate to SQL."""
        if p.operator == "IN":
            if isinstance(p.value, (list, tuple)):
                values_str = ", ".join(
                    f"'{v}'" if isinstance(v, str) else str(v)
                    for v in p.value
                )
                return f"{p.column} IN ({values_str})"
            return f"{p.column} = {repr(p.value)}"
        
        if p.operator in ("IS NULL", "IS NOT NULL"):
            return f"{p.column} {p.operator}"
        
        if isinstance(p.value, str):
            return f"{p.column} {p.operator} '{p.value}'"
        elif p.value is None:
            return f"{p.column} IS NULL"
        else:
            return f"{p.column} {p.operator} {p.value}"

    
    def _execute_via_adapter(self, query_info, adapter, parameters) -> pa.Table:
        """Execute query via adapter with predicate pushdown and caching."""
        # Clean the table name to remove schema prefix and quotes for the adapter
        clean_table = self._clean_table_name(query_info.table)
        
        # OPTIMIZER INTEGRATION: Classify predicates into pushable vs residual
        pushable_predicates, residual_predicates, has_residual = self._classify_predicates(
            query_info, adapter
        )
        
        # Let adapter fetch data with pushed-down predicates
        if query_info.operation == "SELECT":
            # Check cache first
            cache = self._connection._cache
            cache_key = None
            
            if cache.config.enabled and cache.config.should_cache_table(query_info.table):
                # Generate cache key from query components
                cache_key = cache.generate_key(
                    adapter_name=adapter.adapter_name,
                    table=clean_table,
                    columns=tuple(query_info.columns) if query_info.columns else ("*",),
                    predicates=tuple(
                        (p.column, p.operator, p.value) for p in query_info.predicates
                    ) if query_info.predicates else (),
                    limit=query_info.limit,
                    offset=query_info.offset,
                    order_by=tuple(query_info.order_by) if query_info.order_by else None,
                    group_by=tuple(query_info.group_by) if query_info.group_by else None,
                )
                
                # Try to get from cache
                cached_result = cache.get(cache_key)
                if cached_result is not None:
                    # Cache hit - add to execution plan and return
                    step = self.last_plan.add_step(
                        name=f"Cache hit for {clean_table}",
                        type="cache",
                        details={
                            "table": clean_table,
                            "adapter": adapter.adapter_name,
                            "cache_key": cache_key,
                            "rows": len(cached_result),
                        }
                    )
                    step.finish()
                    self._rowcount = len(cached_result)
                    logger.debug(
                        "Cache hit: adapter=%s, table=%s, rows=%d",
                        adapter.adapter_name, clean_table, len(cached_result)
                    )
                    return cached_result
            
            # Cache miss or caching disabled - fetch from adapter
            step = self.last_plan.add_step(
                name=f"Fetch from {adapter.adapter_name}",
                type="fetch",
                details={
                    "table": clean_table,
                    "adapter": adapter.adapter_name,
                    "pushdown_predicates": [str(p) for p in pushable_predicates],
                    "residual_predicates": [str(p) for p in residual_predicates] if has_residual else [],
                    "has_client_side_filter": has_residual,
                    "cache_miss": cache_key is not None,
                }
            )
            start_time = time.perf_counter()
            try:
                # Use traced_fetch for automatic provenance capture
                # OPTIMIZER INTEGRATION: Use pushable_predicates instead of raw predicates
                data = traced_fetch(
                    adapter=adapter,
                    table=clean_table,
                    columns=query_info.columns,
                    predicates=pushable_predicates,  # Use classified pushable predicates
                    limit=query_info.limit if not has_residual else None,  # Don't limit if we need to filter
                    offset=query_info.offset if not has_residual else None,  # Don't offset if we need to filter
                    order_by=query_info.order_by,
                    group_by=query_info.group_by,
                    aggregates=query_info.aggregates,
                )
                duration = time.perf_counter() - start_time
                
                # Update performance metrics for CBO
                if data is not None:
                    adapter._update_performance_metrics(len(data), duration)
                    
                    # Unified Resource Update: Update all optimizers (CBO, Cardinality, Pagination)
                    self._resource_executor.record_execution(
                        adapter_name=adapter.adapter_name,
                        table_name=clean_table,
                        rows_fetched=len(data),
                        duration=duration,
                        predicates=query_info.predicates
                    )
                
                # Check for source query in metadata
                if data is not None and data.schema.metadata:
                    source_query = data.schema.metadata.get(b"waveql_source_query")
                    if source_query:
                        step.details["source_query"] = source_query.decode("utf-8")
                
                step.finish()
                
                # SAFETY NET: Apply residual predicates that couldn't be pushed to the adapter
                if has_residual and data is not None and len(data) > 0:
                    data = self._apply_residual_filter(data, residual_predicates, query_info)
                    
                    # Apply LIMIT and OFFSET after client-side filtering
                    if query_info.offset and query_info.offset > 0:
                        data = data.slice(query_info.offset)
                    if query_info.limit and query_info.limit > 0:
                        data = data.slice(0, query_info.limit)
                
                self._rowcount = len(data) if data else 0
                
                # Store in cache if enabled
                if cache_key is not None and data is not None:
                    cache.put(
                        key=cache_key,
                        data=data,
                        adapter_name=adapter.adapter_name,
                        table_name=clean_table,
                    )
                    logger.debug(
                        "Cache store: adapter=%s, table=%s, rows=%d, size=%.2fMB",
                        adapter.adapter_name, clean_table, len(data),
                        data.nbytes / (1024 * 1024)
                    )
                
                return data
            except NotImplementedError:
                step.finish()
                # Adapter does not support aggregation pushdown.
                # Fallback: Fetch raw data (filtered) and execute SQL locally in DuckDB.
                
                # Check cache for RAW data first
                cache = self._connection._cache
                raw_cache_key = None
                raw_data = None
                
                if cache.config.enabled and cache.config.should_cache_table(query_info.table):
                    raw_cache_key = cache.generate_key(
                        adapter_name=adapter.adapter_name,
                        table=clean_table,
                        columns=("*",),
                        predicates=tuple(
                            (p.column, p.operator, p.value) for p in query_info.predicates
                        ) if query_info.predicates else (),
                    )
                    raw_data = cache.get(raw_cache_key)
                    if raw_data is not None:
                        logger.debug(
                            "Cache hit (fallback): adapter=%s, table=%s, rows=%d",
                            adapter.adapter_name, clean_table, len(raw_data)
                        )
                
                if raw_data is None:
                    step_raw = self.last_plan.add_step(
                        name=f"Fetch raw data from {adapter.adapter_name} (Fallback)",
                        type="fetch",
                        details={"table": clean_table, "adapter": adapter.adapter_name, "cache_miss": True}
                    )
                    # Fetch raw data with predicates pushed down
                    raw_data = adapter.fetch(
                        table=clean_table,
                        columns=None, 
                        predicates=query_info.predicates
                    )
                    step_raw.finish()
                    
                    # Store in cache
                    if raw_cache_key and raw_data is not None:
                        cache.put(
                            key=raw_cache_key, 
                            data=raw_data, 
                            adapter_name=adapter.adapter_name, 
                            table_name=clean_table
                        )
                else:
                    self.last_plan.add_step(
                        name=f"Cache hit for {clean_table} (Fallback)",
                        type="cache",
                        details={"table": clean_table, "cache_key": raw_cache_key, "rows": len(raw_data)}
                    ).finish()
                
                if not raw_data or len(raw_data) == 0:
                     self._rowcount = 0
                     return raw_data
 
                # Register temp table
                temp_name = f"t_{uuid.uuid4().hex}"
                self._connection._duckdb.register(temp_name, raw_data)
                
                try:
                    step_local = self.last_plan.add_step(
                        name="Local DuckDB execution (Fallback)",
                        type="duckdb",
                        details={"engine": "duckdb"}
                    )
                    # Rewrite SQL: Replace table name with temp table name
                    # Utilizes safe sqlglot-based rewriting
                    rewritten_sql = self._planner.rewrite_table_ref(query_info.raw_sql, query_info.table, temp_name)
                    
                    # Execute
                    result = self._connection._duckdb.execute(rewritten_sql).fetch_arrow_table()
                    step_local.finish()
                    self._rowcount = len(result)
                    return result
                finally:
                    self._connection._duckdb.unregister(temp_name)
        
        elif query_info.operation == "INSERT":
            resolved_values, _ = self._resolve_combined_parameters(query_info, parameters)
            self._rowcount = adapter.insert(
                table=clean_table,
                values=resolved_values,
                parameters=parameters,
            )
            # Invalidate cache for this table after write
            self._connection._cache.invalidate(table=clean_table)
            return None
        
        elif query_info.operation == "UPDATE":
            resolved_values, resolved_predicates = self._resolve_combined_parameters(query_info, parameters)
            self._rowcount = adapter.update(
                table=clean_table,
                values=resolved_values,
                predicates=resolved_predicates,
                parameters=parameters,
            )
            # Invalidate cache for this table after write
            self._connection._cache.invalidate(table=clean_table)
            return None
        
        elif query_info.operation == "DELETE":
            _, resolved_predicates = self._resolve_combined_parameters(query_info, parameters)
            self._rowcount = adapter.delete(
                table=clean_table,
                predicates=resolved_predicates,
                parameters=parameters,
            )
            # Invalidate cache for this table after write
            self._connection._cache.invalidate(table=clean_table)
            return None
        
        else:
            raise QueryError(f"Unsupported operation: {query_info.operation}")
 
    def _resolve_combined_parameters(self, query_info, parameters: Sequence):
        """
        Resolves parameters for both values (INSERT/UPDATE) and predicates, 
        respecting the order they appear in the SQL.
        
        Note: Simple extraction assumes params align with dict iteration order for values,
        which works for Python 3.7+ dicts if inserted in order.
        """
        if not parameters:
             return query_info.values, query_info.predicates
        
        params_iter = iter(parameters)
        from waveql.query_planner import ParameterPlaceholder, Predicate

        # 1. Resolve Values (SET/VALUES clause)
        # Note: We create a copy to avoid mutating the original QueryInfo in-place if reused
        resolved_values = {}
        for k, v in query_info.values.items():
            if isinstance(v, ParameterPlaceholder):
                try:
                    resolved_values[k] = next(params_iter)
                except StopIteration:
                     resolved_values[k] = None
            else:
                 resolved_values[k] = v
                 
        # 2. Resolve Predicates (WHERE clause)
        # Usually WHERE comes AFTER SET/VALUES in SQL execution flow logic
        resolved_predicates = []
        for p in query_info.predicates:
            new_val = p.value
            if isinstance(p.value, ParameterPlaceholder):
                try:
                    new_val = next(params_iter)
                except StopIteration:
                    new_val = None
            
            resolved_predicates.append(Predicate(
                column=p.column,
                operator=p.operator,
                value=new_val
            ))
            
        return resolved_values, resolved_predicates

    def _execute_virtual_join(self, query_info, sql: str, parameters: Sequence = None) -> pa.Table:
        """
        Execute a virtual join with semi-join pushdown optimization.
        """
        from collections import defaultdict
        from waveql.query_planner import Predicate
        
        registered_tables = []
        created_views = []
        dataset_cache = {} # clean_table_name -> Arrow Table
 
        try:
            # 1. Map Tables & Aliases
            # aliases: alias -> normalized_table_name
            clean_aliases = {}
            all_tables = set()
            
            for alias, t_full in query_info.aliases.items():
                normalized_table = self._normalize_table_name(t_full)
                # Map both raw and normalized alias to normalized table name
                clean_aliases[alias] = normalized_table
                clean_aliases[alias.strip('"')] = normalized_table
                all_tables.add(normalized_table)
            
            # Ensure primary table is included
            if query_info.table:
                all_tables.add(self._normalize_table_name(query_info.table))
            
            # Ensure join tables are included (if not already via aliases)
            for join in query_info.joins:
                all_tables.add(self._normalize_table_name(join["table"]))
            
            # 2. Group initial predicates by table
            table_predicates = defaultdict(list)
            for pred in query_info.predicates:
                # Find which table this predicate belongs to via alias or direct name
                # Simple logic: if column has dot, split it.
                if "." in pred.column:
                    alias_part, col_part = pred.column.split(".", 1)
                    # Resolve alias to normalized table name - strip quotes from alias part
                    clean_alias_key = alias_part.strip('"')
                    table_name = clean_aliases.get(clean_alias_key, clean_alias_key)
                    table_name = self._normalize_table_name(table_name)
                    
                    # Strip quotes from column name for pushdown
                    col_part = col_part.strip('"')
                    
                    # Strip the alias from the predicate column for the pushdown
                    p_copy = Predicate(column=col_part, operator=pred.operator, value=pred.value)
                    table_predicates[table_name].append(p_copy)
                else:
                    # Ambiguous or Main Table? Assume main table if not aliased? 
                    main_table = self._normalize_table_name(query_info.table)
                    table_predicates[main_table].append(pred)
 
            # 3. Execution Plan (Cost-Based Optimization)
            # Use optimizer to reorder tables based on latency and selectivity
            # Note: While we use the CBO's optimal table order, the execution strategy below
            # is "Greedy Semi-Join Pushdown". We iterate the ordered tables and opportunistically
            # push predicates to future tables based on results fetched so far.
            # This aligns with the CBO's plan but handles data movement dynamically.
            from waveql.optimizer import QueryOptimizer
            optimizer = QueryOptimizer()
            sorted_tables = optimizer.reorder_joins(list(all_tables), table_predicates, self._connection)
            
            # Fallback heuristic if optimizer returns empty or error (though it shouldn't)
            if not sorted_tables:
                sorted_tables = sorted(all_tables, key=lambda t: len(table_predicates[t]), reverse=True)
            
            # 4. Fetch Loop with Pushdown
            pushed_filters = defaultdict(list) # table -> list[Predicate]
            
            fetched_tables = set()
            
            for table_name in sorted_tables:
                # Resolve Adapter
                temp_info = type(query_info)(operation="SELECT", table=table_name)
                adapter = self._resolve_adapter(temp_info)
                
                if adapter:
                    clean_table = self._clean_table_name(table_name)
                    
                    # Combine Base Predicates + Pushed Filters
                    current_preds = table_predicates[table_name] + pushed_filters[table_name]
                    
                    # Fetch with automatic chunking for large IN predicates
                    # This transparently handles 414 errors by splitting large IN clauses
                    fetch_start = time.perf_counter()
                    data = adapter.fetch_with_auto_chunking(
                        table=clean_table, 
                        columns=["*"], 
                        predicates=current_preds
                    )
                    fetch_duration = time.perf_counter() - fetch_start
                    
                    # Updates stats via unified executor
                    if data is not None:
                        self._resource_executor.record_execution(
                            adapter_name=adapter.adapter_name,
                            table_name=clean_table,
                            rows_fetched=len(data),
                            duration=fetch_duration,
                            predicates=current_preds
                        )
                    
                    dataset_cache[table_name] = data
                    fetched_tables.add(table_name)
                    
                    # 5. Analyze Joins for Pushdown Opportunities
                    # Check if this table is joined with any table NOT yet fetched
                    # And if we can generate a filter.
                    
                    # We need to look at all joins
                    # We look for: ON T1.c1 = T2.c2
                    # If T1 is current table, and T2 is not fetched, push condition to T2.
                    if data and len(data) > 0 and len(data) < 100000: # Limit pushdown for massive results
                        for join in query_info.joins:
                            if not join.get("on"): continue
                            
                            # Join involves which tables?
                            # We need to parse the predicates in 'on'
                            for on_pred in join["on"]:
                                if on_pred.operator == "=":
                                    # Check left and right operands
                                    # We expect format like "alias1.col"
                                    # Simple parsing:
                                    left, right = on_pred.column, on_pred.value
                                    if not isinstance(right, str): continue # Value must be a column reference string
                                    
                                    # Resolve tables for left and right
                                    t1_alias, t1_col = left.split(".", 1) if "." in left else (None, left)
                                    t2_alias, t2_col = right.split(".", 1) if "." in right else (None, right)
                                    
                                    t1_name = clean_aliases.get(t1_alias)
                                    t2_name = clean_aliases.get(t2_alias)
                                    
                                    target = None
                                    source_col = None
                                    target_col = None
                                    
                                    # If current table is T1, target is T2
                                    if t1_name == table_name and t2_name and t2_name not in fetched_tables:
                                        target = t2_name
                                        source_col = t1_col
                                        target_col = t2_col
                                    elif t2_name == table_name and t1_name and t1_name not in fetched_tables:
                                        target = t1_name
                                        source_col = t2_col
                                        target_col = t1_col
                                    
                                    if target:
                                        # Extract unique values from current data
                                        try:
                                            # DuckDB/Arrow extraction
                                            # source column in data might be 'c1' not 'alias.c1'
                                            # Adapter fetch creates columns based on schema.
                                            # Usually standard names.
                                            unique_vals = data.column(source_col).unique().to_pylist()
                                            # Remove None
                                            unique_vals = [v for v in unique_vals if v is not None]
                                            
                                            if unique_vals and len(unique_vals) < 50000: # IN clause limit (handled by auto-chunking)
                                                # Create IN predicate
                                                pushed_filters[target].append(
                                                    Predicate(column=target_col, operator="IN", value=unique_vals)
                                                )
                                        except KeyError:
                                            # Column not found in result, maybe aliasing mismatch
                                            pass
            
            # 6. Register all fetched data
            for table_name, data in dataset_cache.items():
                if data is not None:
                     temp_name = f"t_{uuid.uuid4().hex}"
                     self._connection.duckdb.register(temp_name, data)
                     registered_tables.append(temp_name)
                     if "." in table_name:
                         parts = table_name.split(".")
                         # Create intermediate schemas if needed
                         for i in range(1, len(parts)):
                             schema = ".".join([f'"{p}"' for p in parts[:i]])
                             try:
                                 self._connection.duckdb.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
                             except Exception:
                                 pass # DuckDB might be sensitive to nested schema creation
                         
                         view_name = ".".join([f'"{p}"' for p in parts])
                         self._connection.duckdb.execute(
                            f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM "{temp_name}"'
                         )
                         created_views.append(view_name)
                     else:
                         self._connection.duckdb.execute(f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM "{temp_name}"')
                         created_views.append(f'"{table_name}"')
 
            # 7. Execute JOIN
            step_join = self.last_plan.add_step(name="Virtual Join (DuckDB)", type="join")
            if parameters:
                result = self._connection.duckdb.execute(sql, parameters)
            else:
                result = self._connection.duckdb.execute(sql)
            
            table = result.fetch_arrow_table()
            step_join.finish()
            
            self._rowcount = -1
            return table
 
        except Exception as e:
            raise QueryError(f"Virtual join failed: {e}") from e
        finally:
            for view_name in created_views:
                try:
                    self._connection.duckdb.execute(f'DROP VIEW IF EXISTS {view_name}')
                except Exception:
                    pass
            for temp_name in registered_tables:
                try:
                    self._connection.duckdb.unregister(temp_name)
                except Exception:
                    pass
    
    def _execute_hybrid(self, query_info: Any, operation: str, parameters: Sequence = None) -> pa.Table:
        """
        Execute a hybrid query: Materialized View (Historical) + Live API (Fresh).
        
        This enables sub-second query speeds on historical data while 
        guaranteeing 100% freshness by merging in live changes from the adapter.
        """
        step = self.last_plan.add_step(name="Hybrid Execution", type="hybrid")
        
        # 1. Resolve view
        clean_name = self._normalize_table_name(query_info.table)
        view_info = self._connection.view_manager.get(clean_name)
        if not view_info:
            logger.warning(f"HYBRID hint used on non-materialized table {clean_name}. Falling back to live fetch.")
            adapter = self._resolve_adapter(query_info)
            if adapter:
                return self._execute_via_adapter(query_info, adapter, parameters)
            return self._execute_direct(operation, parameters)
            
        # 2. Setup adapter for live fetching
        adapter_name = view_info.definition.source_adapter
        adapter = self._connection.get_adapter(adapter_name)
        if not adapter:
             raise QueryError(f"Source adapter '{adapter_name}' for view '{clean_name}' not found.")
             
        # 3. Determine 'freshness' boundary
        sync_state = view_info.sync_state
        last_sync = sync_state.last_sync_value if sync_state else None
        sync_col = view_info.definition.sync_column
        
        # 4. Fetch live data
        from waveql.query_planner import Predicate
        live_preds = list(query_info.predicates)
        if last_sync and sync_col:
             live_preds.append(Predicate(column=sync_col, operator=">", value=last_sync))
             
        logger.info("HYBRID: Fetching fresh records from %s since %s", adapter_name, last_sync)
        step_live = self.last_plan.add_step(name="Fetch Live Records", type="fetch")
        
        try:
            live_data = adapter.fetch(
                table=view_info.definition.source_table,
                columns=query_info.columns,
                predicates=live_preds
            )
            step_live.finish(rows=len(live_data) if live_data else 0)
        except Exception as e:
            logger.error(f"Hybrid live fetch failed: {e}. Falling back to historical data only.")
            step_live.finish(error=str(e))
            return self._execute_direct(operation, parameters)
        
        # 5. Hybrid Combination in DuckDB
        live_tmp = f"live_{uuid.uuid4().hex}"
        self._connection.duckdb.register(live_tmp, live_data)
        
        # Combined logic (UNION + Deduplication if PKs exist)
        pks = view_info.definition.primary_keys
        if pks:
             pk_cols = ", ".join([f'"{p}"' for p in pks])
             union_sql = f"""
                SELECT * FROM "{clean_name}"
                WHERE {pk_cols} NOT IN (SELECT {pk_cols} FROM "{live_tmp}")
                UNION ALL
                SELECT * FROM "{live_tmp}"
             """
        else:
             union_sql = f'SELECT * FROM "{clean_name}" UNION ALL SELECT * FROM "{live_tmp}"'
             
        # 6. Execute final query shadowed by the combined union
        shadow_view = f"shadow_{uuid.uuid4().hex}"
        self._connection.duckdb.execute(f'CREATE TEMP VIEW "{shadow_view}" AS {union_sql}')
        
        # Rewrite query to use shadow view instead of the official one
        # Use regex to replace table reference safely
        import re
        rewrite_regex = r'\b' + re.escape(clean_name) + r'\b'
        shadow_sql = re.sub(rewrite_regex, f'"{shadow_view}"', operation, flags=re.IGNORECASE)
        
        try:
            result = self._connection.duckdb.execute(shadow_sql).fetch_arrow_table()
            step.finish(rows=len(result))
            return result
        finally:
            try:
                self._connection.duckdb.execute(f'DROP VIEW "{shadow_view}"')
                self._connection.duckdb.unregister(live_tmp)
            except Exception:
                pass

    def _execute_direct(self, operation: str, parameters: Sequence = None) -> pa.Table:
        """Execute a query directly on DuckDB."""
        step = self.last_plan.add_step(name="Direct DuckDB execution", type="duckdb")
        try:
            if parameters:
                result = self._connection.duckdb.execute(operation, parameters)
            else:
                result = self._connection.duckdb.execute(operation)
            
            table = result.fetch_arrow_table()
            step.finish()
            return table
        except Exception as e:
            step.finish()
            raise QueryError(f"Query execution failed: {e}")
    
    def _update_description(self):
        """Update cursor description from Arrow schema."""
        if self._result is None:
            self._description = None
            return
        
        schema = self._result.schema
        self._description = [
            (
                field.name,           # name
                field.type,           # type_code
                None,                 # display_size
                None,                 # internal_size
                None,                 # precision
                None,                 # scale
                field.nullable,       # null_ok
            )
            for field in schema
        ]
    
    def fetchone(self) -> Optional[Row]:
        """Fetch next row of result set."""
        if self._result is None or self._result_index >= len(self._result):
            return None
        
        row_dict = self._result.slice(self._result_index, 1).to_pylist()[0]
        self._result_index += 1
        
        return Row(row_dict, self._description)
    
    def fetchmany(self, size: int = None) -> List[Row]:
        """Fetch next set of rows."""
        if size is None:
            size = self._arraysize
        
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        
        return rows
    
    def fetchall(self) -> List[Row]:
        """Fetch all remaining rows."""
        if self._result is None:
            return []
        
        results = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        
        return results
    
    def to_arrow(self) -> Optional[pa.Table]:
        """Return result as Arrow Table (extension method)."""
        return self._result
    
    def to_df(self):
        """Return result as Pandas DataFrame (extension method)."""
        if self._result is None:
            return None
        return self._result.to_pandas()
    
    def stream_batches(
        self,
        operation: str,
        batch_size: int = 1000,
        max_records: int = None,
        progress_callback = None,
    ):
        """
        Stream query results as RecordBatches for memory-efficient processing.
        
        This method yields Arrow RecordBatches one at a time, enabling:
        - Processing of million-row exports without loading into memory
        - Progress tracking for long-running queries
        - Early termination (just stop iterating)
        
        Args:
            operation: SQL SELECT query
            batch_size: Number of records per batch (default 1000)
            max_records: Maximum total records to fetch (None = unlimited)
            progress_callback: Function(records_fetched, total_estimate) for progress
            
        Yields:
            pa.RecordBatch objects
            
        Example:
            for batch in cursor.stream_batches("SELECT * FROM large_table"):
                for row in batch.to_pylist():
                    process(row)
        """
        from waveql.streaming import RecordBatchStream, StreamConfig
        
        if self._closed:
            raise QueryError("Cursor is closed")
        
        # Parse query to get table and predicates
        query_info = self._planner.parse(operation)
        
        if query_info.operation != "SELECT":
            raise QueryError("stream_batches() only supports SELECT queries")
        
        # Resolve adapter
        adapter = self._resolve_adapter(query_info)
        if not adapter:
            raise QueryError("stream_batches() requires an adapter-backed table")
        
        clean_table = self._clean_table_name(query_info.table)
        
        config = StreamConfig(
            batch_size=batch_size,
            max_records=max_records,
            progress_callback=progress_callback,
        )
        
        stream = RecordBatchStream(
            adapter=adapter,
            table=clean_table,
            columns=query_info.columns if query_info.columns != ["*"] else None,
            predicates=query_info.predicates,
            order_by=query_info.order_by,
            config=config,
        )
        
        return stream
    
    def stream_to_file(
        self,
        operation: str,
        output_path: str,
        batch_size: int = 1000,
        compression: str = "snappy",
        progress_callback = None,
    ):
        """
        Stream query results directly to a Parquet file without loading into memory.
        
        This is the most memory-efficient way to export large datasets.
        
        Args:
            operation: SQL SELECT query
            output_path: Path to output Parquet file
            batch_size: Number of records per batch (default 1000)
            compression: Parquet compression ('snappy', 'gzip', 'zstd', 'none')
            progress_callback: Function(records_fetched, total_estimate) for progress
            
        Returns:
            StreamStats with operation statistics
            
        Example:
            stats = cursor.stream_to_file(
                "SELECT * FROM large_table",
                "export.parquet",
                progress_callback=lambda n, t: print(f"Exported {n:,} records")
            )
            print(f"Total: {stats.records_fetched:,} records")
        """
        from waveql.streaming import StreamConfig
        
        config = StreamConfig(
            batch_size=batch_size,
            compression=compression,
            progress_callback=progress_callback,
        )
        
        stream = self.stream_batches(
            operation,
            batch_size=batch_size,
            progress_callback=progress_callback,
        )
        stream._config.compression = compression
        
        return stream.to_parquet(output_path)
    
    def close(self):
        """Close the cursor."""
        self._closed = True
        self._result = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "closed" if self._closed else "open"
        result_len = len(self._result) if self._result is not None else 0
        return f"<WaveQLCursor status={status} rows={result_len} position={self._result_index}>"
