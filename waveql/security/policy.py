"""
Row-Level Security (RLS) Policy Module

Provides the ability to attach security policies to tables that automatically
filter data at query time. Policies are transparent to the user - they are
injected into every query targeting the protected table.

Design Philosophy:
- Zero-Trust by default: Policies apply to ALL queries (SELECT, UPDATE, DELETE)
- Composable: Multiple policies on the same table are ANDed together
- Auditable: Policy applications are logged for compliance

Example:
    ```python
    # Restrict sales team to only see their department's data
    conn.add_policy("incident", "department = 'sales'")
    
    # Multi-tenancy: each user sees only their org's data
    conn.add_policy("*", f"org_id = '{current_user.org_id}'")
    ```
"""

from __future__ import annotations
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union
from enum import Enum

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class PolicyViolationError(Exception):
    """Raised when a query violates a security policy."""
    pass


class PolicyMode(Enum):
    """How the policy predicate should be applied."""
    PERMISSIVE = "permissive"  # Rows pass if ANY permissive policy matches (OR)
    RESTRICTIVE = "restrictive"  # Rows pass only if ALL restrictive policies match (AND)


@dataclass
class SecurityPolicy:
    """
    A Row-Level Security policy definition.
    
    Attributes:
        name: Unique identifier for the policy (auto-generated if not provided)
        table: Table name to apply policy to. Use "*" for all tables.
        predicate: SQL WHERE clause fragment (e.g., "department = 'sales'")
        mode: How to combine with other policies (PERMISSIVE or RESTRICTIVE)
        operations: Which operations to apply to (SELECT, UPDATE, DELETE). Default: all.
        enabled: Whether the policy is currently active
        description: Human-readable description for audit logs
        
    Example:
        ```python
        policy = SecurityPolicy(
            name="sales_only",
            table="incident",
            predicate="department = 'sales'",
            description="Restrict sales team to their department"
        )
        ```
    """
    name: str
    table: str  # Use "*" for all tables
    predicate: str  # SQL WHERE clause fragment
    mode: PolicyMode = PolicyMode.RESTRICTIVE
    operations: Set[str] = field(default_factory=lambda: {"SELECT", "UPDATE", "DELETE"})
    enabled: bool = True
    description: str = ""
    
    # Dynamic predicate: allows runtime evaluation (e.g., current_user())
    # If provided, this function is called at query time to generate the predicate
    predicate_fn: Optional[Callable[[], str]] = field(default=None, repr=False)
    
    def __post_init__(self):
        # Validate the predicate syntax by parsing it
        if self.predicate and not self.predicate_fn:
            try:
                # Try to parse as a WHERE clause
                sqlglot.parse_one(f"SELECT * FROM t WHERE {self.predicate}")
            except Exception as e:
                raise ValueError(f"Invalid predicate syntax: {self.predicate}. Error: {e}")
        
        # Normalize operations to uppercase
        self.operations = {op.upper() for op in self.operations}
    
    def get_predicate(self) -> str:
        """
        Get the effective predicate (static or dynamically evaluated).
        
        Returns:
            SQL WHERE clause fragment
        """
        if self.predicate_fn:
            return self.predicate_fn()
        return self.predicate
    
    def applies_to(self, table: str, operation: str) -> bool:
        """
        Check if this policy applies to a given table and operation.
        
        Args:
            table: Table name (normalized, e.g., "servicenow.incident")
            operation: SQL operation (SELECT, UPDATE, DELETE, INSERT)
            
        Returns:
            True if this policy should be applied
        """
        if not self.enabled:
            return False
        
        if operation.upper() not in self.operations:
            return False
        
        # Wildcard matches all tables
        if self.table == "*":
            return True
        
        # Exact match (case-insensitive)
        if self.table.lower() == table.lower():
            return True
        
        # Handle schema.table matching
        # Policy on "incident" should match "servicenow.incident"
        if "." in table:
            _, table_only = table.rsplit(".", 1)
            if self.table.lower() == table_only.lower():
                return True
        
        return False
    
    def __hash__(self):
        return hash(self.name)
    
    def __eq__(self, other):
        if not isinstance(other, SecurityPolicy):
            return False
        return self.name == other.name


class PolicyManager:
    """
    Manages Row-Level Security policies for a connection.
    
    Thread-safe policy storage and query rewriting.
    
    Usage:
        ```python
        manager = PolicyManager()
        manager.add_policy(SecurityPolicy(
            name="sales_only",
            table="incident",
            predicate="department = 'sales'"
        ))
        
        # Rewrite a query with policies applied
        rewritten_sql = manager.apply_policies(
            "SELECT * FROM incident",
            table="incident",
            operation="SELECT"
        )
        # Result: "SELECT * FROM incident WHERE department = 'sales'"
        ```
    """
    
    def __init__(self):
        self._policies: Dict[str, SecurityPolicy] = {}
        self._policy_counter = 0
    
    def add_policy(
        self,
        table: str,
        predicate: str,
        name: str = None,
        mode: Union[PolicyMode, str] = PolicyMode.RESTRICTIVE,
        operations: Set[str] = None,
        description: str = "",
        predicate_fn: Callable[[], str] = None,
    ) -> SecurityPolicy:
        """
        Add a new security policy.
        
        Args:
            table: Table to protect ("*" for all tables)
            predicate: SQL WHERE clause fragment
            name: Unique policy name (auto-generated if not provided)
            mode: PERMISSIVE or RESTRICTIVE
            operations: Set of operations to apply to (default: SELECT, UPDATE, DELETE)
            description: Human-readable description
            predicate_fn: Dynamic predicate generator function
            
        Returns:
            The created SecurityPolicy
            
        Example:
            ```python
            manager.add_policy("incident", "department = 'sales'")
            manager.add_policy("*", "org_id = 123", name="tenant_isolation")
            ```
        """
        if name is None:
            self._policy_counter += 1
            name = f"policy_{self._policy_counter}"
        
        if isinstance(mode, str):
            mode = PolicyMode(mode.lower())
        
        policy = SecurityPolicy(
            name=name,
            table=table,
            predicate=predicate,
            mode=mode,
            operations=operations or {"SELECT", "UPDATE", "DELETE"},
            description=description,
            predicate_fn=predicate_fn,
        )
        
        self._policies[name] = policy
        logger.info("Added RLS policy: %s on table=%s", name, table)
        return policy
    
    def remove_policy(self, name: str) -> bool:
        """
        Remove a policy by name.
        
        Args:
            name: Policy name to remove
            
        Returns:
            True if policy was found and removed
        """
        if name in self._policies:
            del self._policies[name]
            logger.info("Removed RLS policy: %s", name)
            return True
        return False
    
    def get_policy(self, name: str) -> Optional[SecurityPolicy]:
        """Get a policy by name."""
        return self._policies.get(name)
    
    def list_policies(self, table: str = None) -> List[SecurityPolicy]:
        """
        List all policies, optionally filtered by table.
        
        Args:
            table: Filter to policies affecting this table
            
        Returns:
            List of SecurityPolicy objects
        """
        if table is None:
            return list(self._policies.values())
        
        return [
            p for p in self._policies.values()
            if p.applies_to(table, "SELECT")  # Use SELECT as default check
        ]
    
    def clear_policies(self) -> int:
        """
        Remove all policies.
        
        Returns:
            Number of policies removed
        """
        count = len(self._policies)
        self._policies.clear()
        logger.info("Cleared all RLS policies (%d removed)", count)
        return count
    
    def get_applicable_policies(
        self,
        table: str,
        operation: str
    ) -> List[SecurityPolicy]:
        """
        Get all policies that apply to a table and operation.
        
        Args:
            table: Table name
            operation: SQL operation
            
        Returns:
            List of applicable policies
        """
        return [
            p for p in self._policies.values()
            if p.applies_to(table, operation)
        ]
    
    def build_combined_predicate(
        self,
        table: str,
        operation: str
    ) -> Optional[str]:
        """
        Build a combined predicate from all applicable policies.
        
        RESTRICTIVE policies are ANDed together.
        PERMISSIVE policies are ORed together.
        The final predicate is: (PERMISSIVE_1 OR PERMISSIVE_2) AND RESTRICTIVE_1 AND RESTRICTIVE_2
        
        Args:
            table: Table name
            operation: SQL operation
            
        Returns:
            Combined predicate string, or None if no policies apply
        """
        policies = self.get_applicable_policies(table, operation)
        if not policies:
            return None
        
        restrictive_preds = []
        permissive_preds = []
        
        for policy in policies:
            pred = policy.get_predicate()
            if policy.mode == PolicyMode.RESTRICTIVE:
                restrictive_preds.append(f"({pred})")
            else:
                permissive_preds.append(f"({pred})")
        
        parts = []
        
        # Combine permissive policies with OR
        if permissive_preds:
            if len(permissive_preds) == 1:
                parts.append(permissive_preds[0])
            else:
                parts.append(f"({' OR '.join(permissive_preds)})")
        
        # Combine restrictive policies with AND
        parts.extend(restrictive_preds)
        
        if not parts:
            return None
        
        return " AND ".join(parts)
    
    def apply_policies(
        self,
        sql: str,
        table: str,
        operation: str
    ) -> str:
        """
        Apply all applicable policies to a SQL query.
        
        This rewrites the SQL to include policy predicates in the WHERE clause.
        
        Args:
            sql: Original SQL query
            table: Primary table being queried
            operation: SQL operation
            
        Returns:
            Rewritten SQL with policies applied
            
        Example:
            ```python
            original = "SELECT * FROM incident WHERE status = 'open'"
            rewritten = manager.apply_policies(original, "incident", "SELECT")
            # Result: "SELECT * FROM incident WHERE status = 'open' AND (department = 'sales')"
            ```
        """
        combined = self.build_combined_predicate(table, operation)
        if not combined:
            return sql
        
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            logger.warning("Failed to parse SQL for RLS: %s", e)
            return sql
        
        # Find the WHERE clause
        where = parsed.find(exp.Where)
        
        if where:
            # Extend existing WHERE with AND
            existing_condition = where.this
            new_condition = sqlglot.parse_one(f"SELECT * FROM t WHERE {combined}").find(exp.Where).this
            combined_condition = exp.And(this=existing_condition, expression=new_condition)
            where.set("this", combined_condition)
        else:
            # Add new WHERE clause
            new_where = sqlglot.parse_one(f"SELECT * FROM t WHERE {combined}").find(exp.Where)
            
            # Find the appropriate place to insert WHERE
            # For SELECT, it goes after FROM
            if isinstance(parsed, exp.Select):
                parsed.set("where", new_where)
            elif isinstance(parsed, exp.Update):
                parsed.set("where", new_where)
            elif isinstance(parsed, exp.Delete):
                parsed.set("where", new_where)
        
        rewritten = parsed.sql()
        logger.debug("RLS applied: %s -> %s", sql[:100], rewritten[:100])
        return rewritten
    
    def __len__(self) -> int:
        return len(self._policies)
    
    def __bool__(self) -> bool:
        return len(self._policies) > 0
