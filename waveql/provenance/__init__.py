"""
WaveQL Query Provenance - Data Lineage for API Federation

This module provides provenance tracking capabilities:
- Where-Provenance: Track which API/table each row originated from
- Why-Provenance: Record predicates that caused row inclusion
- How-Provenance: Document join paths and transformations

Usage:
    from waveql.provenance import enable_provenance, get_provenance_tracker
    
    # Enable tracking
    enable_provenance(mode="summary")  # or "full", "sampled"
    
    # Execute queries - provenance is automatically captured
    cursor.execute("SELECT * FROM servicenow.incident JOIN salesforce.contact ...")
    
    # Access provenance
    tracker = get_provenance_tracker()
    for query_prov in tracker.get_history():
        print(f"Query used adapters: {query_prov.adapters_used}")
        print(f"Total API calls: {query_prov.total_api_calls}")
        print(f"Total latency: {query_prov.total_latency_ms}ms")

Novel Research Area:
    This is the first implementation of query provenance for SQL-over-API
    federation systems. See docs/research/query_provenance.md for the
    full research plan and academic background.
"""

from waveql.provenance.models import (
    APICallTrace,
    PredicateMatch,
    RowProvenance,
    QueryProvenance,
)
from waveql.provenance.tracker import (
    ProvenanceTracker,
    get_provenance_tracker,
    attach_provenance,
    extract_provenance,
)
from waveql.provenance.traced_adapter import (
    traced_fetch,
    ProvenanceAdapterProxy,
)

__all__ = [
    # Data models
    "APICallTrace",
    "PredicateMatch",
    "RowProvenance",
    "QueryProvenance",
    # Tracker
    "ProvenanceTracker",
    "get_provenance_tracker",
    "attach_provenance",
    "extract_provenance",
    # Adapter integration
    "traced_fetch",
    "ProvenanceAdapterProxy",
    # Convenience functions
    "enable_provenance",
    "disable_provenance",
]


def enable_provenance(mode: str = "summary"):
    """
    Enable provenance tracking for all subsequent queries.
    
    Args:
        mode: Tracking mode
            - "summary": Track API calls only (low overhead, ~2-5%)
            - "full": Track per-row provenance (higher overhead, ~10-20%)
            - "sampled": Sample 10% of rows for provenance (medium overhead, ~3-7%)
    
    Example:
        >>> enable_provenance(mode="summary")
        >>> cursor.execute("SELECT * FROM incident")
        >>> tracker = get_provenance_tracker()
        >>> print(tracker.get_history()[-1].adapters_used)
        ['servicenow']
    """
    tracker = get_provenance_tracker()
    tracker.enable(mode=mode)


def disable_provenance():
    """
    Disable provenance tracking.
    
    This restores normal query execution without any tracking overhead.
    """
    tracker = get_provenance_tracker()
    tracker.disable()
