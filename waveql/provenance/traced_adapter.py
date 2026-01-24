"""
Traced Adapter - Decorator for automatic provenance tracking on adapter calls.

This module provides decorators and utilities to automatically capture
provenance information when adapters make API calls.

Usage:
    The integration happens automatically when provenance is enabled.
    Adapters don't need to be modified - the cursor wraps calls.
"""

from __future__ import annotations
import time
import functools
import logging
from typing import List, Any, Callable, TYPE_CHECKING

from waveql.provenance.tracker import get_provenance_tracker
from waveql.provenance.models import PredicateMatch

if TYPE_CHECKING:
    import pyarrow as pa
    from waveql.adapters.base import BaseAdapter
    from waveql.query_planner import Predicate

logger = logging.getLogger(__name__)


# Primary key column names for common adapters
ADAPTER_PRIMARY_KEYS = {
    "servicenow": "sys_id",
    "salesforce": "Id",
    "jira": "key",
    "hubspot": "id",
    "zendesk": "id",
    "shopify": "id",
    "stripe": "id",
    "rest": "id",
    "csv": None,
    "excel": None,
    "sqlite": None,
    "postgres": None,
    "mysql": None,
}


def find_primary_key_column(table: "pa.Table", adapter_name: str) -> str:
    """
    Heuristic to find the primary key column for an adapter.
    
    Args:
        table: PyArrow table to search
        adapter_name: Name of the adapter
        
    Returns:
        Column name of the primary key, or None if not found
    """
    # Check adapter-specific PK
    pk = ADAPTER_PRIMARY_KEYS.get(adapter_name)
    if pk and pk in table.column_names:
        return pk
    
    # Fallback: look for common ID columns
    for candidate in ["id", "ID", "Id", "_id", "pk", "key", "sys_id"]:
        if candidate in table.column_names:
            return candidate
    
    return None


def traced_fetch(
    adapter: "BaseAdapter",
    table: str,
    columns: List[str] = None,
    predicates: List["Predicate"] = None,
    limit: int = None,
    offset: int = None,
    order_by: List[tuple] = None,
    group_by: List[str] = None,
    aggregates: List[Any] = None,
) -> "pa.Table":
    """
    Wrapper that traces an adapter fetch call for provenance.
    
    This function wraps the adapter's fetch() method to automatically
    record API call details in the provenance tracker.
    
    Args:
        adapter: The adapter instance
        table: Table name to fetch from
        columns, predicates, etc.: Standard fetch parameters
        
    Returns:
        PyArrow Table with results
    """
    tracker = get_provenance_tracker()
    
    # If tracking is disabled, just call fetch directly
    if not tracker.enabled or not tracker.current_query:
        return adapter.fetch(
            table=table,
            columns=columns,
            predicates=predicates,
            limit=limit,
            offset=offset,
            order_by=order_by,
            group_by=group_by,
            aggregates=aggregates,
        )
    
    # Capture timing
    start_time = time.perf_counter()
    
    try:
        result = adapter.fetch(
            table=table,
            columns=columns,
            predicates=predicates,
            limit=limit,
            offset=offset,
            order_by=order_by,
            group_by=group_by,
            aggregates=aggregates,
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        # Build request params for provenance
        request_params = {
            "columns": columns,
            "limit": limit,
            "offset": offset,
        }
        if predicates:
            request_params["predicates"] = [
                {"col": p.column, "op": p.operator, "val": _safe_str(p.value)}
                for p in predicates
            ]
        if order_by:
            request_params["order_by"] = order_by
        if group_by:
            request_params["group_by"] = group_by
        
        # Record the API call in provenance
        trace_id = tracker.record_api_call(
            adapter_name=adapter.adapter_name,
            table_name=table,
            endpoint_url=getattr(adapter, '_host', '') or '',
            http_method="GET",
            request_params=request_params,
            response_status=200,
            response_time_ms=elapsed_ms,
            rows_returned=len(result) if result is not None else 0,
        )
        
        # Record per-row provenance if in full mode
        if tracker.mode == "full" and result is not None and len(result) > 0:
            _record_row_provenance(
                tracker=tracker,
                result=result,
                adapter_name=adapter.adapter_name,
                table_name=table,
                trace_id=trace_id,
                predicates=predicates,
            )
        
        logger.debug(
            "Provenance: %s.%s fetched %d rows in %.1fms",
            adapter.adapter_name, table, 
            len(result) if result else 0, elapsed_ms
        )
        
        return result
        
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        # Record failed API call
        tracker.record_api_call(
            adapter_name=adapter.adapter_name,
            table_name=table,
            endpoint_url=getattr(adapter, '_host', '') or '',
            http_method="GET",
            response_status=500,
            response_time_ms=elapsed_ms,
            rows_returned=0,
        )
        
        raise


def _record_row_provenance(
    tracker,
    result: "pa.Table",
    adapter_name: str,
    table_name: str,
    trace_id: str,
    predicates: List["Predicate"],
):
    """Record per-row provenance for full mode."""
    
    # Find primary key column
    pk_col = find_primary_key_column(result, adapter_name)
    
    # Build matched predicates list
    matched = []
    for pred in (predicates or []):
        matched.append(PredicateMatch(
            column=pred.column,
            operator=pred.operator,
            value=pred.value,
            source="user",
        ))
    
    # Record provenance for each row (capped by tracker.max_row_provenance)
    for i in range(len(result)):
        pk_value = None
        if pk_col:
            try:
                pk_value = str(result.column(pk_col)[i].as_py())
            except Exception:
                pass
        
        tracker.record_row_provenance(
            row_index=i,
            source_adapter=adapter_name,
            source_table=table_name,
            source_primary_key=pk_value,
            api_call_trace_id=trace_id,
            matched_predicates=matched,
        )


def _safe_str(value: Any) -> str:
    """Safely convert a value to string for provenance logging."""
    if value is None:
        return "NULL"
    if isinstance(value, (list, tuple)):
        if len(value) > 5:
            return f"[{len(value)} items]"
        return str(value)
    return str(value)


class ProvenanceAdapterProxy:
    """
    Proxy that wraps an adapter to add provenance tracking.
    
    This proxy intercepts fetch() calls and records them in the
    provenance tracker while delegating all other operations
    to the underlying adapter.
    
    Usage:
        adapter = ServiceNowAdapter(...)
        traced_adapter = ProvenanceAdapterProxy(adapter)
        result = traced_adapter.fetch("incident")  # Automatically traced
    """
    
    def __init__(self, adapter: "BaseAdapter"):
        self._adapter = adapter
    
    def __getattr__(self, name):
        """Delegate all attribute access to the underlying adapter."""
        return getattr(self._adapter, name)
    
    def fetch(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> "pa.Table":
        """Traced fetch that records provenance."""
        return traced_fetch(
            adapter=self._adapter,
            table=table,
            columns=columns,
            predicates=predicates,
            limit=limit,
            offset=offset,
            order_by=order_by,
            group_by=group_by,
            aggregates=aggregates,
        )
