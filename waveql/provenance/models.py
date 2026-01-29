"""
Provenance Data Models - Core data structures for query lineage tracking.

This module defines the immutable data structures that represent provenance
information captured during query execution.

Research Context:
    These models implement the three types of provenance from database literature:
    - Where-Provenance (Buneman et al., 2001): Source of each data item
    - Why-Provenance: Predicates that caused inclusion
    - How-Provenance: Transformations applied
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class JoinTransformation:
    """
    Records a join operation for How-Provenance.
    
    This captures how data from multiple tables was combined during query
    execution, including the join type and the columns used.
    
    Attributes:
        left_table: Fully qualified name of the left table
        right_table: Fully qualified name of the right table
        left_column: Column from the left table used in join condition
        right_column: Column from the right table used in join condition
        join_type: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
        timestamp: When the join was executed
    """
    left_table: str
    right_table: str
    left_column: str
    right_column: str
    join_type: str = "INNER"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def __repr__(self) -> str:
        return (
            f"JoinTransformation({self.left_table}.{self.left_column} "
            f"{self.join_type} JOIN {self.right_table}.{self.right_column})"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "left_table": self.left_table,
            "right_table": self.right_table,
            "left_column": self.left_column,
            "right_column": self.right_column,
            "join_type": self.join_type,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }


@dataclass
class APICallTrace:
    """
    Represents a single API call made during query execution.
    
    This captures the "physical" level of where-provenance - the actual
    HTTP request made to fetch data from an external API.
    
    Attributes:
        trace_id: Unique identifier for this API call
        adapter_name: Name of the adapter (e.g., "servicenow", "salesforce")
        table_name: Logical table name being queried
        endpoint_url: Full URL of the API endpoint called
        http_method: HTTP method used (GET, POST, etc.)
        request_params: Query parameters and predicates sent
        response_status: HTTP status code returned
        response_time_ms: Time taken for the API call in milliseconds
        rows_returned: Number of rows returned by this call
        timestamp: When the call was made
        is_paginated: Whether this is part of a paginated request
        page_number: Page number if paginated
        total_pages: Total pages if known
    """
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    adapter_name: str = ""
    table_name: str = ""
    endpoint_url: str = ""
    http_method: str = "GET"
    request_params: Dict[str, Any] = field(default_factory=dict)
    response_status: int = 200
    response_time_ms: float = 0.0
    rows_returned: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Pagination tracking
    is_paginated: bool = False
    page_number: int = 0
    total_pages: int = 1
    
    def __repr__(self) -> str:
        return (
            f"APICallTrace({self.adapter_name}.{self.table_name}, "
            f"{self.rows_returned} rows, {self.response_time_ms:.1f}ms)"
        )


@dataclass
class PredicateMatch:
    """
    Explains why a row was included in the result (why-provenance).
    
    Each PredicateMatch represents a condition from the WHERE clause
    that matched the row.
    
    Attributes:
        column: Column name the predicate operates on
        operator: Comparison operator (=, <, >, IN, LIKE, etc.)
        value: Value being compared against
        source: Origin of the predicate
            - "user": From the user's WHERE clause
            - "rls_policy": Injected by Row-Level Security
            - "join_condition": From a JOIN ON clause
    """
    column: str
    operator: str
    value: Any
    source: str = "user"
    
    def __repr__(self) -> str:
        return f"PredicateMatch({self.column} {self.operator} {self.value!r})"


@dataclass
class RowProvenance:
    """
    Complete provenance for a single row in the result set.
    
    This is the most granular level of provenance tracking, recording
    exactly where each row came from and why it was included.
    
    Note:
        Per-row provenance is only captured in "full" or "sampled" mode
        due to the memory overhead. In "summary" mode, only QueryProvenance
        and APICallTrace are recorded.
    
    Attributes:
        row_index: Position of this row in the result set
        source_adapter: Adapter that provided this row
        source_table: Table within the adapter
        source_primary_key: Primary key value if available
        api_call_trace_id: Links to the APICallTrace that fetched this row
        matched_predicates: List of predicates that matched this row
        join_path: For multi-table queries, the path of tables joined
        join_conditions: Conditions used in joins
        projections_applied: Columns selected (for column-level provenance)
        aggregations_applied: Any aggregations applied to this data
    """
    row_index: int
    source_adapter: str
    source_table: str
    source_primary_key: Optional[str] = None
    api_call_trace_id: str = ""
    matched_predicates: List[PredicateMatch] = field(default_factory=list)
    join_path: List[str] = field(default_factory=list)
    join_conditions: Dict[str, Any] = field(default_factory=dict)
    projections_applied: List[str] = field(default_factory=list)
    aggregations_applied: List[str] = field(default_factory=list)
    
    def __repr__(self) -> str:
        pk = f", pk={self.source_primary_key}" if self.source_primary_key else ""
        return f"RowProvenance(row {self.row_index}: {self.source_adapter}.{self.source_table}{pk})"


@dataclass
class QueryProvenance:
    """
    Complete provenance for an entire query execution.
    
    This is the top-level provenance object that aggregates all information
    about a single query execution, including API calls made, row-level
    provenance (if enabled), and summary statistics.
    
    Attributes:
        query_id: Unique identifier for this query execution
        original_sql: The SQL query as written by the user
        execution_start: When query execution began
        execution_end: When query execution completed
        api_calls: All API calls made during execution
        row_provenance: Per-row provenance (only in full/sampled mode)
        join_transformations: How-Provenance - join operations performed
        total_rows: Number of rows in the final result
        adapters_used: List of adapter names accessed
        tables_accessed: List of fully-qualified table names accessed
        total_api_calls: Count of API calls made
        total_latency_ms: Total query execution time in milliseconds
        provenance_mode: Mode used for this query (summary/full/sampled)
        sample_rate: Sample rate if using sampled mode
    
    Example:
        >>> with tracker.trace_query("SELECT * FROM incident") as prov:
        ...     # execute query
        ...     pass
        >>> print(prov.adapters_used)
        ['servicenow']
        >>> print(prov.total_latency_ms)
        245.3
    """
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    original_sql: str = ""
    execution_start: datetime = field(default_factory=datetime.utcnow)
    execution_end: Optional[datetime] = None
    
    # API-level provenance (always captured when enabled)
    api_calls: List[APICallTrace] = field(default_factory=list)
    
    # Row-level provenance (only in full/sampled mode)
    row_provenance: List[RowProvenance] = field(default_factory=list)
    
    # How-Provenance: Join transformations performed
    join_transformations: List[JoinTransformation] = field(default_factory=list)
    
    # Summary statistics
    total_rows: int = 0
    adapters_used: List[str] = field(default_factory=list)
    tables_accessed: List[str] = field(default_factory=list)
    
    # Computed metrics
    total_api_calls: int = 0
    total_latency_ms: float = 0.0
    
    # Configuration
    provenance_mode: str = "summary"
    sample_rate: float = 1.0
    
    def __repr__(self) -> str:
        return (
            f"QueryProvenance(id={self.query_id[:8]}..., "
            f"adapters={self.adapters_used}, "
            f"api_calls={self.total_api_calls}, "
            f"joins={len(self.join_transformations)}, "
            f"latency={self.total_latency_ms:.1f}ms)"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "query_id": self.query_id,
            "original_sql": self.original_sql,
            "execution_start": self.execution_start.isoformat() if self.execution_start else None,
            "execution_end": self.execution_end.isoformat() if self.execution_end else None,
            "api_calls": [
                {
                    "trace_id": call.trace_id,
                    "adapter": call.adapter_name,
                    "table": call.table_name,
                    "endpoint": call.endpoint_url,
                    "latency_ms": call.response_time_ms,
                    "rows": call.rows_returned,
                    "status": call.response_status,
                }
                for call in self.api_calls
            ],
            "join_transformations": [jt.to_dict() for jt in self.join_transformations],
            "adapters_used": self.adapters_used,
            "tables_accessed": self.tables_accessed,
            "total_api_calls": self.total_api_calls,
            "total_latency_ms": self.total_latency_ms,
            "total_rows": self.total_rows,
            "provenance_mode": self.provenance_mode,
        }
