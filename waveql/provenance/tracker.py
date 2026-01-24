"""
Provenance Tracker - Thread-safe capture of query provenance.

This module implements the core tracking infrastructure for query provenance.
It uses a singleton pattern with thread-local storage to safely track
provenance across concurrent query executions.

Research Context:
    This tracker implements the provenance capture strategy described in
    docs/research/query_provenance.md. It is designed to have minimal
    overhead when disabled (<1%) and configurable overhead when enabled
    (2-20% depending on mode).
"""

from __future__ import annotations
import threading
import logging
import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from waveql.provenance.models import (
    APICallTrace,
    PredicateMatch,
    RowProvenance,
    QueryProvenance,
)

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


class ProvenanceTracker:
    """
    Thread-safe tracker for query provenance information.
    
    This is a singleton class that manages provenance tracking across
    all query executions. It uses thread-local storage to safely track
    concurrent queries.
    
    Usage:
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        
        with tracker.trace_query(sql) as query_prov:
            # Execute query...
            tracker.record_api_call(adapter, table, endpoint, ...)
        
        # Provenance now available in query_prov
        print(query_prov.adapters_used)
    
    Modes:
        - "summary": Only track API calls (low overhead)
        - "full": Track per-row provenance (high overhead)
        - "sampled": Sample a percentage of rows (medium overhead)
    """
    
    _instance: Optional["ProvenanceTracker"] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern for global provenance tracking."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        
        # Thread-local storage for active traces
        self._local = threading.local()
        
        # Configuration
        self.enabled: bool = False
        self.mode: str = "summary"  # "full", "summary", "sampled"
        self.sample_rate: float = 0.1  # For sampled mode
        self.max_row_provenance: int = 1000  # Cap per-row tracking
        
        # History (for debugging and analysis)
        self._history: List[QueryProvenance] = []
        self._max_history: int = 100
        self._history_lock = threading.Lock()
        
        logger.debug("ProvenanceTracker initialized")
    
    def enable(self, mode: str = "summary"):
        """
        Enable provenance tracking.
        
        Args:
            mode: Tracking mode
                - "summary": Track API calls only (recommended for production)
                - "full": Track per-row provenance (use for auditing)
                - "sampled": Sample rows for provenance (balance of both)
        """
        if mode not in ("summary", "full", "sampled"):
            raise ValueError(f"Invalid mode: {mode}. Use 'summary', 'full', or 'sampled'")
        
        self.enabled = True
        self.mode = mode
        logger.info(f"Provenance tracking enabled in '{mode}' mode")
    
    def disable(self):
        """Disable provenance tracking."""
        self.enabled = False
        logger.info("Provenance tracking disabled")
    
    @property
    def current_query(self) -> Optional[QueryProvenance]:
        """Get the current query's provenance (thread-local)."""
        return getattr(self._local, "current_query", None)
    
    @contextmanager
    def trace_query(self, sql: str):
        """
        Context manager to trace a query execution.
        
        This is the primary interface for tracking query provenance.
        All API calls made within the context are automatically associated
        with the query.
        
        Args:
            sql: The SQL query being executed
            
        Yields:
            QueryProvenance object that will be populated during execution
            
        Example:
            with tracker.trace_query("SELECT * FROM incident") as prov:
                # Execute the query
                result = adapter.fetch("incident")
            
            # prov now contains complete provenance
            print(prov.api_calls)
        """
        if not self.enabled:
            yield None
            return
        
        query_prov = QueryProvenance(
            original_sql=sql,
            execution_start=datetime.utcnow(),
            provenance_mode=self.mode,
            sample_rate=self.sample_rate if self.mode == "sampled" else 1.0,
        )
        
        # Set as current query for this thread
        self._local.current_query = query_prov
        
        try:
            yield query_prov
        finally:
            # Finalize provenance
            query_prov.execution_end = datetime.utcnow()
            query_prov.total_latency_ms = (
                query_prov.execution_end - query_prov.execution_start
            ).total_seconds() * 1000
            query_prov.total_api_calls = len(query_prov.api_calls)
            
            # Add to history
            with self._history_lock:
                self._history.append(query_prov)
                if len(self._history) > self._max_history:
                    self._history.pop(0)
            
            # Clear thread-local
            self._local.current_query = None
            
            logger.debug(
                f"Query provenance finalized: {query_prov.total_api_calls} API calls, "
                f"{query_prov.total_latency_ms:.1f}ms"
            )
    
    def record_api_call(
        self,
        adapter_name: str,
        table_name: str,
        endpoint_url: str,
        http_method: str = "GET",
        request_params: Dict[str, Any] = None,
        response_status: int = 200,
        response_time_ms: float = 0.0,
        rows_returned: int = 0,
        page_number: int = 0,
        total_pages: int = 1,
    ) -> Optional[str]:
        """
        Record an API call in the current query's provenance.
        
        This method is called by adapters during query execution to record
        the API calls they make.
        
        Args:
            adapter_name: Name of the adapter (e.g., "servicenow")
            table_name: Logical table being queried
            endpoint_url: Full URL of the API endpoint
            http_method: HTTP method (GET, POST, etc.)
            request_params: Query parameters sent to the API
            response_status: HTTP status code returned
            response_time_ms: Response time in milliseconds
            rows_returned: Number of rows returned
            page_number: Page number if paginated (0-indexed)
            total_pages: Total pages if known
            
        Returns:
            trace_id of the recorded call, or None if tracking disabled
        """
        if not self.enabled or not self.current_query:
            return None
        
        trace = APICallTrace(
            adapter_name=adapter_name,
            table_name=table_name,
            endpoint_url=endpoint_url,
            http_method=http_method,
            request_params=request_params or {},
            response_status=response_status,
            response_time_ms=response_time_ms,
            rows_returned=rows_returned,
            is_paginated=total_pages > 1,
            page_number=page_number,
            total_pages=total_pages,
        )
        
        self.current_query.api_calls.append(trace)
        
        # Update summary info
        self.current_query.adapters_used = list(set(
            self.current_query.adapters_used + [adapter_name]
        ))
        self.current_query.tables_accessed = list(set(
            self.current_query.tables_accessed + [f"{adapter_name}.{table_name}"]
        ))
        
        return trace.trace_id
    
    def record_row_provenance(
        self,
        row_index: int,
        source_adapter: str,
        source_table: str,
        source_primary_key: str = None,
        api_call_trace_id: str = None,
        matched_predicates: List[PredicateMatch] = None,
        join_path: List[str] = None,
    ):
        """
        Record provenance for a specific row.
        
        This is only called in "full" or "sampled" mode.
        
        Args:
            row_index: Index of the row in the result set
            source_adapter: Adapter that provided this row
            source_table: Table within the adapter
            source_primary_key: Primary key value if available
            api_call_trace_id: Links to the APICallTrace
            matched_predicates: Predicates that matched this row
            join_path: Path of tables if from a join
        """
        if not self.enabled or not self.current_query:
            return
        
        if self.mode == "summary":
            # In summary mode, skip per-row tracking
            return
        
        if len(self.current_query.row_provenance) >= self.max_row_provenance:
            return  # Cap reached
        
        if self.mode == "sampled":
            import random
            if random.random() > self.sample_rate:
                return  # Skip based on sampling
        
        row_prov = RowProvenance(
            row_index=row_index,
            source_adapter=source_adapter,
            source_table=source_table,
            source_primary_key=source_primary_key,
            api_call_trace_id=api_call_trace_id or "",
            matched_predicates=matched_predicates or [],
            join_path=join_path or [],
        )
        
        self.current_query.row_provenance.append(row_prov)
    
    def get_history(self) -> List[QueryProvenance]:
        """
        Get recent query provenance history.
        
        Returns:
            List of QueryProvenance objects for recent queries
        """
        with self._history_lock:
            return list(self._history)
    
    def clear_history(self):
        """Clear the provenance history."""
        with self._history_lock:
            self._history.clear()


# Module-level singleton accessor
_tracker_instance: Optional[ProvenanceTracker] = None


def get_provenance_tracker() -> ProvenanceTracker:
    """
    Get the global provenance tracker instance.
    
    Returns:
        The singleton ProvenanceTracker instance
    """
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = ProvenanceTracker()
    return _tracker_instance


# Utility functions for PyArrow integration

def attach_provenance(table: "pa.Table", provenance: QueryProvenance) -> "pa.Table":
    """
    Attach provenance information to a PyArrow table as metadata.
    
    This allows provenance to travel with the data through transformations
    and exports.
    
    Args:
        table: PyArrow table to attach provenance to
        provenance: QueryProvenance object to attach
        
    Returns:
        New PyArrow table with provenance in metadata
    """
    import pyarrow as pa
    
    # Serialize provenance to JSON
    provenance_json = json.dumps(provenance.to_dict())
    
    # Merge with existing metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {
        **existing_meta,
        b"waveql:provenance": provenance_json.encode("utf-8"),
    }
    
    return table.replace_schema_metadata(new_meta)


def extract_provenance(table: "pa.Table") -> Optional[Dict[str, Any]]:
    """
    Extract provenance from a PyArrow table's metadata.
    
    Args:
        table: PyArrow table that may contain provenance metadata
        
    Returns:
        Dictionary containing provenance data, or None if not present
    """
    if not table.schema.metadata:
        return None
    
    prov_bytes = table.schema.metadata.get(b"waveql:provenance")
    if prov_bytes:
        return json.loads(prov_bytes.decode("utf-8"))
    return None
