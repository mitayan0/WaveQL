# Query Provenance for API Federation

> **Research Area**: Data Lineage & Provenance in Federated Query Systems  
> **Status**: Novel Research - No existing implementation for SQL-over-API systems  
> **Authors**: WaveQL Team  
> **Date**: January 2026

---

## Executive Summary

This document outlines a research and implementation plan for **Query Provenance** in WaveQL - a novel contribution to the field of federated query systems. While provenance tracking exists for traditional databases (Snowflake, BigQuery) and SPARQL federations, **no system currently provides provenance tracking across heterogeneous API backends** (ServiceNow, Salesforce, Jira, etc.).

WaveQL is uniquely positioned to pioneer this research due to its:
1. Multi-adapter architecture with unified SQL interface
2. Existing predicate pushdown infrastructure
3. PyArrow-native data handling (metadata-friendly)

---

## 1. Background & Motivation

### 1.1 What is Query Provenance?

Query provenance tracks the **origin and transformation history** of data returned by a query. Three types of provenance are recognized in the literature:

| Type | Question Answered | Example |
|------|-------------------|---------|
| **Where-Provenance** | Where did this data come from? | "Row 5 came from ServiceNow incident INC0012345" |
| **Why-Provenance** | Why was this data included? | "Included because `priority < 3` matched" |
| **How-Provenance** | How was this data derived? | "Joined via `user_id = assignee.sys_id`" |

### 1.2 Why This Matters for WaveQL

**Regulatory Compliance**:
- GDPR Article 15: Right to know data sources
- SOC 2: Audit trail requirements
- HIPAA: Data origin tracking for PHI

**Debugging & Trust**:
- "Why is this ticket appearing in my dashboard?" → Show provenance
- "Is this data stale?" → Show API call timestamp
- "Which API contributed to this row?" → Multi-source attribution

**Optimization**:
- Identify slow API sources in join chains
- Cost attribution across adapters

### 1.3 Gap in Existing Research

| System | Provenance Type | Scope | Limitations |
|--------|-----------------|-------|-------------|
| Snowflake Lineage (2024) | Table→View | Internal objects only | No external API support |
| FedQuery (SPARQL, 2024) | How-Provenance | SPARQL federations | Not SQL, not APIs |
| Google Research DP Engine | Sensitivity tracking | Privacy focused | No lineage |
| **WaveQL (Proposed)** | Full provenance | SQL + heterogeneous APIs | **Novel** |

---

## 2. Research Questions

### Primary Questions

1. **RQ1**: How can provenance be efficiently captured during federated API query execution without significant performance overhead?

2. **RQ2**: What is the optimal data structure for representing provenance across heterogeneous API sources with different identity schemes?

3. **RQ3**: How does provenance information scale when queries span multiple adapters with large result sets?

### Secondary Questions

4. **RQ4**: Can provenance be leveraged for automated query optimization (e.g., preferring faster sources)?

5. **RQ5**: How does provenance interact with caching and materialized views?

6. **RQ6**: What are the privacy implications of storing detailed provenance data?

---

## 3. Proposed Architecture

### 3.1 Provenance Data Model

```python
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

@dataclass
class APICallTrace:
    """Represents a single API call made during query execution."""
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    adapter_name: str = ""              # e.g., "servicenow"
    table_name: str = ""                # e.g., "incident"
    endpoint_url: str = ""              # e.g., "https://dev123.service-now.com/api/now/table/incident"
    http_method: str = "GET"
    request_params: Dict[str, Any] = field(default_factory=dict)
    response_status: int = 200
    response_time_ms: float = 0.0
    rows_returned: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # For pagination tracking
    is_paginated: bool = False
    page_number: int = 0
    total_pages: int = 1


@dataclass
class PredicateMatch:
    """Explains why a row was included in the result."""
    column: str
    operator: str
    value: Any
    source: str = "user"  # or "rls_policy", "join_condition"


@dataclass
class RowProvenance:
    """Complete provenance for a single row in the result set."""
    row_index: int
    
    # Where-Provenance: Origin of this row
    source_adapter: str
    source_table: str
    source_primary_key: Optional[str] = None  # e.g., sys_id for ServiceNow
    api_call_trace_id: str = ""
    
    # Why-Provenance: Predicates that matched
    matched_predicates: List[PredicateMatch] = field(default_factory=list)
    
    # How-Provenance: Join chain if from multi-table query
    join_path: List[str] = field(default_factory=list)  # e.g., ["incident", "user"]
    join_conditions: Dict[str, Any] = field(default_factory=dict)
    
    # Transformation tracking
    projections_applied: List[str] = field(default_factory=list)  # Columns selected
    aggregations_applied: List[str] = field(default_factory=list)  # Any aggregates


@dataclass
class QueryProvenance:
    """Complete provenance for an entire query execution."""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    original_sql: str = ""
    execution_start: datetime = field(default_factory=datetime.utcnow)
    execution_end: Optional[datetime] = None
    
    # All API calls made
    api_calls: List[APICallTrace] = field(default_factory=list)
    
    # Per-row provenance (can be sampled for large result sets)
    row_provenance: List[RowProvenance] = field(default_factory=list)
    
    # Summary statistics
    total_rows: int = 0
    adapters_used: List[str] = field(default_factory=list)
    tables_accessed: List[str] = field(default_factory=list)
    
    # Cost attribution
    total_api_calls: int = 0
    total_latency_ms: float = 0.0
    
    # Metadata
    provenance_mode: str = "full"  # "full", "summary", "sampled"
    sample_rate: float = 1.0  # For sampled mode
```

### 3.2 Integration Points

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WaveQL Query Execution                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐       ┌──────────────┐       ┌──────────────────────┐    │
│   │   Cursor    │──────▶│ QueryPlanner │──────▶│   QueryOptimizer     │    │
│   │  execute()  │       │              │       │ (join reordering)    │    │
│   └─────────────┘       └──────────────┘       └──────────────────────┘    │
│         │                                               │                   │
│         │ ◀──── [1] Capture original SQL               │                   │
│         │                                               ▼                   │
│         │                                    ┌──────────────────────┐       │
│         │                                    │   ProvenanceTracker  │       │
│         │                                    │  (NEW COMPONENT)     │       │
│         │                                    └──────────────────────┘       │
│         │                                               │                   │
│         ▼                                               ▼                   │
│   ┌─────────────┐                            ┌──────────────────────┐       │
│   │  Adapter    │ ◀──── [2] Wrap fetch() ───│   TracedAdapter      │       │
│   │   fetch()   │           calls           │   (Decorator/Proxy)  │       │
│   └─────────────┘                            └──────────────────────┘       │
│         │                                               │                   │
│         │                                               │ [3] Record        │
│         ▼                                               │     API calls     │
│   ┌─────────────┐                                       ▼                   │
│   │  PyArrow    │ ◀──── [4] Attach provenance ─────────────────────────    │
│   │   Table     │           as metadata                                     │
│   └─────────────┘                                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 PyArrow Metadata Extension

PyArrow tables support custom metadata that persists through transformations:

```python
import pyarrow as pa
import json

def attach_provenance(table: pa.Table, provenance: QueryProvenance) -> pa.Table:
    """Attach provenance information to a PyArrow table as metadata."""
    
    # Serialize provenance to JSON
    provenance_json = json.dumps({
        "query_id": provenance.query_id,
        "original_sql": provenance.original_sql,
        "adapters_used": provenance.adapters_used,
        "api_calls": [
            {
                "trace_id": call.trace_id,
                "adapter": call.adapter_name,
                "table": call.table_name,
                "endpoint": call.endpoint_url,
                "latency_ms": call.response_time_ms,
                "rows": call.rows_returned,
            }
            for call in provenance.api_calls
        ],
        "total_latency_ms": provenance.total_latency_ms,
    })
    
    # Merge with existing metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {
        **existing_meta,
        b"waveql:provenance": provenance_json.encode("utf-8"),
    }
    
    return table.replace_schema_metadata(new_meta)


def extract_provenance(table: pa.Table) -> Optional[Dict]:
    """Extract provenance from a PyArrow table's metadata."""
    if not table.schema.metadata:
        return None
    
    prov_bytes = table.schema.metadata.get(b"waveql:provenance")
    if prov_bytes:
        return json.loads(prov_bytes.decode("utf-8"))
    return None
```

---

## 4. Implementation Plan

### Phase 1: Core Infrastructure (Week 1-2)

#### 4.1 ProvenanceTracker Module

**File**: `waveql/provenance/__init__.py`, `waveql/provenance/tracker.py`

```python
# waveql/provenance/tracker.py

from __future__ import annotations
import threading
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class ProvenanceTracker:
    """
    Thread-safe tracker for query provenance information.
    
    Usage:
        tracker = ProvenanceTracker()
        with tracker.trace_query(sql) as query_prov:
            # Execute query...
            tracker.record_api_call(adapter, table, endpoint, ...)
        
        # Provenance now available in query_prov
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
        
        # History (optional, for debugging)
        self._history: List[QueryProvenance] = []
        self._max_history: int = 100
    
    def enable(self, mode: str = "summary"):
        """Enable provenance tracking."""
        self.enabled = True
        self.mode = mode
        logger.info(f"Provenance tracking enabled in '{mode}' mode")
    
    def disable(self):
        """Disable provenance tracking."""
        self.enabled = False
    
    @property
    def current_query(self) -> Optional[QueryProvenance]:
        """Get the current query's provenance (thread-local)."""
        return getattr(self._local, "current_query", None)
    
    @contextmanager
    def trace_query(self, sql: str):
        """
        Context manager to trace a query execution.
        
        Usage:
            with tracker.trace_query("SELECT * FROM incident") as prov:
                # Execute query
                pass
            # prov now contains complete provenance
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
        
        self._local.current_query = query_prov
        
        try:
            yield query_prov
        finally:
            query_prov.execution_end = datetime.utcnow()
            query_prov.total_latency_ms = (
                query_prov.execution_end - query_prov.execution_start
            ).total_seconds() * 1000
            query_prov.total_api_calls = len(query_prov.api_calls)
            
            # Add to history
            self._history.append(query_prov)
            if len(self._history) > self._max_history:
                self._history.pop(0)
            
            self._local.current_query = None
    
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
        """Record provenance for a specific row."""
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
                return  # Skip this row based on sampling
        
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
        """Get recent query provenance history."""
        return list(self._history)


# Module-level singleton accessor
def get_provenance_tracker() -> ProvenanceTracker:
    """Get the global provenance tracker instance."""
    return ProvenanceTracker()
```

### Phase 2: Adapter Integration (Week 3-4)

#### 4.2 TracedAdapter Decorator

**File**: `waveql/provenance/traced_adapter.py`

```python
# waveql/provenance/traced_adapter.py

from __future__ import annotations
import time
import functools
from typing import List, Any, TYPE_CHECKING
import pyarrow as pa

from waveql.provenance.tracker import get_provenance_tracker

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter
    from waveql.query_planner import Predicate


def traced_fetch(fetch_method):
    """
    Decorator to automatically trace adapter fetch() calls.
    
    Wraps BaseAdapter.fetch() to record API call details in provenance.
    """
    @functools.wraps(fetch_method)
    def wrapper(
        self: "BaseAdapter",
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        tracker = get_provenance_tracker()
        
        if not tracker.enabled:
            return fetch_method(
                self, table, columns, predicates, limit, 
                offset, order_by, group_by, aggregates
            )
        
        # Capture timing
        start_time = time.perf_counter()
        
        try:
            result = fetch_method(
                self, table, columns, predicates, limit,
                offset, order_by, group_by, aggregates
            )
            
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            
            # Record the API call
            trace_id = tracker.record_api_call(
                adapter_name=self.adapter_name,
                table_name=table,
                endpoint_url=getattr(self, '_host', '') or '',
                http_method="GET",
                request_params={
                    "columns": columns,
                    "predicates": [
                        {"col": p.column, "op": p.operator, "val": str(p.value)}
                        for p in (predicates or [])
                    ],
                    "limit": limit,
                    "offset": offset,
                },
                response_status=200,
                response_time_ms=elapsed_ms,
                rows_returned=len(result) if result else 0,
            )
            
            # Record per-row provenance if in full mode
            if tracker.mode == "full" and result and len(result) > 0:
                # Try to find primary key column
                pk_col = _find_primary_key_column(result, self.adapter_name)
                
                for i in range(len(result)):
                    pk_value = None
                    if pk_col and pk_col in result.column_names:
                        pk_value = str(result.column(pk_col)[i].as_py())
                    
                    # Build matched predicates
                    matched = []
                    for pred in (predicates or []):
                        from waveql.provenance.tracker import PredicateMatch
                        matched.append(PredicateMatch(
                            column=pred.column,
                            operator=pred.operator,
                            value=pred.value,
                            source="user",
                        ))
                    
                    tracker.record_row_provenance(
                        row_index=i,
                        source_adapter=self.adapter_name,
                        source_table=table,
                        source_primary_key=pk_value,
                        api_call_trace_id=trace_id,
                        matched_predicates=matched,
                    )
            
            return result
            
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            tracker.record_api_call(
                adapter_name=self.adapter_name,
                table_name=table,
                endpoint_url=getattr(self, '_host', '') or '',
                http_method="GET",
                response_status=500,
                response_time_ms=elapsed_ms,
                rows_returned=0,
            )
            raise
    
    return wrapper


def _find_primary_key_column(table: pa.Table, adapter_name: str) -> str:
    """Heuristic to find the primary key column for an adapter."""
    
    # Adapter-specific primary keys
    pk_map = {
        "servicenow": "sys_id",
        "salesforce": "Id",
        "jira": "key",
        "hubspot": "id",
        "zendesk": "id",
        "shopify": "id",
        "stripe": "id",
    }
    
    # Check adapter-specific PK
    if adapter_name in pk_map:
        pk = pk_map[adapter_name]
        if pk in table.column_names:
            return pk
    
    # Fallback: look for common ID columns
    for candidate in ["id", "ID", "Id", "_id", "pk", "key"]:
        if candidate in table.column_names:
            return candidate
    
    return None
```

### Phase 3: SQL Extension (Week 5-6)

#### 4.3 PROVENANCE() SQL Function

**Syntax**:
```sql
-- Enable provenance for a query
SELECT *, PROVENANCE() AS _prov FROM servicenow.incident WHERE priority < 3

-- Get provenance summary
SELECT PROVENANCE_SUMMARY() FROM (
    SELECT * FROM jira.issues JOIN servicenow.incident ON ...
)

-- Explain data lineage
EXPLAIN PROVENANCE SELECT * FROM salesforce.contact
```

**Implementation in Cursor**:

```python
# In waveql/cursor.py - add to execute()

def _handle_provenance_function(self, query_info, sql: str):
    """Handle PROVENANCE() function in queries."""
    
    # Check if query contains PROVENANCE()
    if "PROVENANCE()" not in sql.upper():
        return query_info, False
    
    # Enable tracking for this query
    from waveql.provenance.tracker import get_provenance_tracker
    tracker = get_provenance_tracker()
    tracker.enable(mode="full")
    
    # Strip PROVENANCE() from SQL for execution, re-add after
    cleaned_sql = re.sub(r',?\s*PROVENANCE\(\)\s*AS\s*\w+', '', sql, flags=re.IGNORECASE)
    
    return query_info, True  # has_provenance flag
```

### Phase 4: Visualization & Export (Week 7-8)

#### 4.4 Lineage Graph Generation

```python
# waveql/provenance/visualize.py

from typing import Dict, Any, List
import json


def generate_lineage_graph(provenance: QueryProvenance) -> Dict[str, Any]:
    """
    Generate a D3.js-compatible lineage graph from provenance data.
    
    Returns:
        Graph structure with nodes and edges for visualization
    """
    nodes = []
    edges = []
    node_ids = {}
    
    # Add query node
    query_node_id = f"query_{provenance.query_id[:8]}"
    nodes.append({
        "id": query_node_id,
        "type": "query",
        "label": provenance.original_sql[:50] + "...",
        "metadata": {
            "full_sql": provenance.original_sql,
            "execution_time_ms": provenance.total_latency_ms,
        }
    })
    
    # Add nodes for each API call
    for call in provenance.api_calls:
        call_node_id = f"api_{call.trace_id[:8]}"
        nodes.append({
            "id": call_node_id,
            "type": "api_call",
            "label": f"{call.adapter_name}.{call.table_name}",
            "metadata": {
                "adapter": call.adapter_name,
                "table": call.table_name,
                "endpoint": call.endpoint_url,
                "latency_ms": call.response_time_ms,
                "rows": call.rows_returned,
            }
        })
        node_ids[(call.adapter_name, call.table_name)] = call_node_id
        
        # Edge from API call to query result
        edges.append({
            "source": call_node_id,
            "target": query_node_id,
            "type": "data_flow",
            "metadata": {
                "rows_contributed": call.rows_returned,
            }
        })
    
    # Add adapter source nodes
    for adapter in provenance.adapters_used:
        adapter_id = f"adapter_{adapter}"
        nodes.append({
            "id": adapter_id,
            "type": "adapter",
            "label": adapter,
        })
        
        # Connect adapter to its API calls
        for call in provenance.api_calls:
            if call.adapter_name == adapter:
                call_id = f"api_{call.trace_id[:8]}"
                edges.append({
                    "source": adapter_id,
                    "target": call_id,
                    "type": "adapter_call",
                })
    
    return {
        "nodes": nodes,
        "edges": edges,
        "metadata": {
            "query_id": provenance.query_id,
            "total_adapters": len(provenance.adapters_used),
            "total_api_calls": provenance.total_api_calls,
            "total_latency_ms": provenance.total_latency_ms,
        }
    }


def export_lineage_html(provenance: QueryProvenance, output_path: str):
    """Generate an interactive HTML visualization."""
    graph = generate_lineage_graph(provenance)
    
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>WaveQL Query Lineage</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; }
        .node { cursor: pointer; }
        .node.query { fill: #4CAF50; }
        .node.api_call { fill: #2196F3; }
        .node.adapter { fill: #FF9800; }
        .link { stroke: #999; stroke-opacity: 0.6; }
        #tooltip { 
            position: absolute; background: #333; color: white;
            padding: 10px; border-radius: 4px; font-size: 12px;
            pointer-events: none; opacity: 0;
        }
    </style>
</head>
<body>
    <div id="tooltip"></div>
    <svg width="100%" height="600"></svg>
    <script>
        const graph = GRAPH_DATA_PLACEHOLDER;
        // D3 force-directed graph implementation...
    </script>
</body>
</html>
    """
    
    html = html_template.replace(
        "GRAPH_DATA_PLACEHOLDER", 
        json.dumps(graph)
    )
    
    with open(output_path, 'w') as f:
        f.write(html)
```

---

## 5. Testing Strategy

### 5.1 Unit Tests

```python
# tests/test_provenance.py

import pytest
from waveql.provenance.tracker import ProvenanceTracker, get_provenance_tracker


class TestProvenanceTracker:
    
    def setup_method(self):
        """Reset tracker state before each test."""
        tracker = get_provenance_tracker()
        tracker.disable()
        tracker._history.clear()
    
    def test_singleton_pattern(self):
        """Tracker should be a singleton."""
        t1 = get_provenance_tracker()
        t2 = get_provenance_tracker()
        assert t1 is t2
    
    def test_trace_query_context_manager(self):
        """trace_query should capture SQL and timing."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            assert prov.original_sql == "SELECT * FROM incident"
            assert prov.execution_start is not None
        
        assert prov.execution_end is not None
        assert prov in tracker.get_history()
    
    def test_record_api_call(self):
        """API calls should be recorded in active trace."""
        tracker = get_provenance_tracker()
        tracker.enable()
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            trace_id = tracker.record_api_call(
                adapter_name="servicenow",
                table_name="incident",
                endpoint_url="https://dev.service-now.com",
                response_time_ms=150.0,
                rows_returned=42,
            )
            assert trace_id is not None
        
        assert len(prov.api_calls) == 1
        assert prov.api_calls[0].rows_returned == 42
    
    def test_disabled_tracking(self):
        """No data should be recorded when disabled."""
        tracker = get_provenance_tracker()
        tracker.disable()
        
        result = tracker.record_api_call(
            adapter_name="test",
            table_name="test",
            endpoint_url="http://test",
        )
        
        assert result is None
    
    def test_mode_summary_skips_row_provenance(self):
        """Summary mode should not record per-row provenance."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="summary")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            tracker.record_row_provenance(
                row_index=0,
                source_adapter="servicenow",
                source_table="incident",
            )
        
        assert len(prov.row_provenance) == 0
    
    def test_mode_full_records_row_provenance(self):
        """Full mode should record per-row provenance."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="full")
        
        with tracker.trace_query("SELECT * FROM incident") as prov:
            tracker.record_row_provenance(
                row_index=0,
                source_adapter="servicenow",
                source_table="incident",
                source_primary_key="abc123",
            )
        
        assert len(prov.row_provenance) == 1
        assert prov.row_provenance[0].source_primary_key == "abc123"
```

### 5.2 Integration Tests

```python
# tests/test_provenance_integration.py

import pytest
from waveql import connect
from waveql.provenance.tracker import get_provenance_tracker


@pytest.fixture
def mock_servicenow_connection(respx_mock):
    """Create a connection with mocked ServiceNow responses."""
    respx_mock.get("https://dev.service-now.com/api/now/table/incident").respond(
        json={"result": [
            {"sys_id": "abc", "number": "INC001", "priority": "1"},
            {"sys_id": "def", "number": "INC002", "priority": "2"},
        ]}
    )
    
    conn = connect(
        adapter="servicenow",
        host="https://dev.service-now.com",
        user="test",
        password="test",
    )
    return conn


class TestProvenanceIntegration:
    
    def test_provenance_recorded_during_query(self, mock_servicenow_connection):
        """Executing a query should record provenance."""
        tracker = get_provenance_tracker()
        tracker.enable(mode="full")
        
        cursor = mock_servicenow_connection.cursor()
        cursor.execute("SELECT sys_id, number FROM incident WHERE priority = '1'")
        results = cursor.fetchall()
        
        history = tracker.get_history()
        assert len(history) > 0
        
        last_query = history[-1]
        assert "incident" in last_query.original_sql.lower()
        assert "servicenow" in last_query.adapters_used
    
    def test_provenance_attached_to_arrow_table(self, mock_servicenow_connection):
        """Arrow result should have provenance metadata."""
        from waveql.provenance.tracker import attach_provenance, extract_provenance
        
        # This would be called internally by the cursor
        # Test the attachment/extraction functions
        import pyarrow as pa
        
        table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        
        from waveql.provenance.tracker import QueryProvenance
        prov = QueryProvenance(original_sql="SELECT * FROM test")
        
        table_with_prov = attach_provenance(table, prov)
        extracted = extract_provenance(table_with_prov)
        
        assert extracted is not None
        assert extracted["original_sql"] == "SELECT * FROM test"
```

---

## 6. Performance Considerations

### 6.1 Overhead Analysis

| Mode | Description | Overhead | Use Case |
|------|-------------|----------|----------|
| `disabled` | No tracking | 0% | Production default |
| `summary` | API calls only | ~2-5% | Debugging, cost analysis |
| `full` | Per-row provenance | ~10-20% | Audit, compliance |
| `sampled` | 10% row sampling | ~3-7% | Large datasets |

### 6.2 Memory Management

```python
# Configure limits
tracker = get_provenance_tracker()
tracker.max_row_provenance = 1000  # Cap at 1000 rows
tracker.max_history = 50           # Keep last 50 queries

# For very large queries, switch to sampled mode
tracker.enable(mode="sampled")
tracker.sample_rate = 0.01  # 1% sampling
```

### 6.3 Storage Options

For persistent provenance:

```python
# Option 1: SQLite storage (default)
from waveql.provenance.storage import SQLiteProvenanceStore
store = SQLiteProvenanceStore("~/.waveql/provenance.db")

# Option 2: Export to Parquet for analysis
from waveql.provenance.storage import export_to_parquet
export_to_parquet(tracker.get_history(), "provenance_data.parquet")

# Option 3: OpenTelemetry integration
from waveql.provenance.otel import ProvenanceSpanExporter
exporter = ProvenanceSpanExporter(endpoint="http://jaeger:14268")
```

---

## 7. Future Extensions

### 7.1 Provenance-Aware Caching

```python
# Invalidate cache when source provenance changes
cache.invalidate_by_provenance(
    adapter="servicenow",
    table="incident",
    since=datetime(2025, 1, 1)
)
```

### 7.2 Cross-Query Lineage

Track how data flows between queries:

```sql
-- Find all queries that used data from incident table
SELECT * FROM waveql.query_lineage 
WHERE source_table = 'servicenow.incident'
  AND query_time > datetime('now', '-1 hour')
```

### 7.3 Anomaly Detection

```python
# Detect unusual provenance patterns
detector = ProvenanceAnomalyDetector()
detector.train(tracker.get_history())

# Alert if query accesses unexpected APIs
anomalies = detector.check(current_query_provenance)
```

---

## 8. Timeline & Milestones

| Week | Milestone | Deliverables |
|------|-----------|--------------|
| 1 | Core Data Structures | `RowProvenance`, `QueryProvenance`, `APICallTrace` |
| 2 | ProvenanceTracker | Singleton, thread-safety, context manager |
| 3 | Adapter Integration | `traced_fetch` decorator, BaseAdapter changes |
| 4 | Testing | Unit tests, mock integration tests |
| 5 | SQL Functions | `PROVENANCE()`, `EXPLAIN PROVENANCE` |
| 6 | PyArrow Integration | Metadata attachment/extraction |
| 7 | Visualization | D3.js graph, HTML export |
| 8 | Documentation | User guide, API reference |

---

## 9. Success Metrics

1. **Performance**: < 5% overhead in summary mode
2. **Correctness**: 100% of API calls captured
3. **Usability**: Provenance accessible via SQL function
4. **Adoption**: Documented and tested

---

## 10. References

1. Cheney, J., Chiticariu, L., & Tan, W. C. (2009). "Provenance in databases: Why, how, and where." *Foundations and Trends in Databases*.

2. Grislain, N., et al. (2024). "Qrlew: Rewriting SQL into Differentially Private SQL." *AAAI PPAI 2024*.

3. COST Action FedQuery Workshop (2024). "Explaining Federated SPARQL Queries with How-Provenance."

4. Snowflake Documentation (2024). "Data Lineage."

5. Buneman, P., Khanna, S., & Tan, W. C. (2001). "Why and where: A characterization of data provenance." *ICDT*.

---

## Appendix A: Public API Summary

```python
# Enable provenance
from waveql.provenance import enable_provenance
enable_provenance(mode="summary")  # or "full", "sampled"

# Query with provenance
conn = waveql.connect(...)
cursor = conn.cursor()
cursor.execute("SELECT *, PROVENANCE() AS _prov FROM incident")

# Access provenance programmatically
from waveql.provenance import get_provenance_tracker
tracker = get_provenance_tracker()
for query_prov in tracker.get_history():
    print(query_prov.adapters_used)
    print(query_prov.total_latency_ms)
    for call in query_prov.api_calls:
        print(f"  {call.adapter_name}.{call.table_name}: {call.rows_returned} rows")

# Visualize
from waveql.provenance.visualize import export_lineage_html
export_lineage_html(query_prov, "lineage.html")
```
