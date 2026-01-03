"""
Materialized Views - Local caching for remote data sources

This module provides materialized view functionality for WaveQL,
enabling local Parquet-based caching of remote API data for
sub-millisecond query performance.
"""

from waveql.materialized_view.models import (
    ViewDefinition,
    ViewStats,
    SyncState,
    RefreshStrategy,
)
from waveql.materialized_view.manager import MaterializedViewManager
from waveql.materialized_view.registry import ViewRegistry
from waveql.materialized_view.storage import ViewStorage

__all__ = [
    "ViewDefinition",
    "ViewStats",
    "SyncState",
    "RefreshStrategy",
    "MaterializedViewManager",
    "ViewRegistry",
    "ViewStorage",
]
