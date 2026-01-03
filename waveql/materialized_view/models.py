"""
Data models for Materialized Views
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class RefreshStrategy(str, Enum):
    """Strategy for refreshing materialized views."""
    FULL = "full"           # Re-fetch all data
    INCREMENTAL = "incremental"  # Fetch only new/updated records


@dataclass
class ColumnInfo:
    """Column metadata for a materialized view."""
    name: str
    data_type: str
    nullable: bool = True


@dataclass
class ViewDefinition:
    """
    Complete definition of a materialized view.
    
    Attributes:
        name: Unique name for the view
        query: Original SQL query that defines the view
        source_adapter: Name of the adapter (e.g., 'servicenow')
        source_table: Name of the source table
        refresh_strategy: How to refresh (full or incremental)
        sync_column: Column used for incremental sync (e.g., 'sys_updated_on')
        storage_path: Path to the Parquet file
        created_at: When the view was created
        columns: Schema of the view
    """
    name: str
    query: str
    source_adapter: Optional[str] = None
    source_table: Optional[str] = None
    refresh_strategy: RefreshStrategy = RefreshStrategy.FULL
    sync_column: Optional[str] = None
    storage_path: Optional[Path] = None
    created_at: datetime = field(default_factory=datetime.now)
    columns: List[ColumnInfo] = field(default_factory=list)
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"ViewDefinition({self.name} FROM {self.source_adapter}.{self.source_table})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "query": self.query,
            "source_adapter": self.source_adapter,
            "source_table": self.source_table,
            "refresh_strategy": self.refresh_strategy.value,
            "sync_column": self.sync_column,
            "storage_path": str(self.storage_path) if self.storage_path else None,
            "created_at": self.created_at.isoformat(),
            "columns": [
                {"name": c.name, "data_type": c.data_type, "nullable": c.nullable}
                for c in self.columns
            ],
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ViewDefinition":
        """Create from dictionary."""
        columns = [
            ColumnInfo(name=c["name"], data_type=c["data_type"], nullable=c.get("nullable", True))
            for c in data.get("columns", [])
        ]
        return cls(
            name=data["name"],
            query=data["query"],
            source_adapter=data.get("source_adapter"),
            source_table=data.get("source_table"),
            refresh_strategy=RefreshStrategy(data.get("refresh_strategy", "full")),
            sync_column=data.get("sync_column"),
            storage_path=Path(data["storage_path"]) if data.get("storage_path") else None,
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(),
            columns=columns,
        )


@dataclass
class ViewStats:
    """
    Statistics for a materialized view.
    
    Attributes:
        row_count: Number of rows in the view
        size_bytes: Size of the Parquet file in bytes
        last_refresh: When the view was last refreshed
        refresh_duration_ms: How long the last refresh took
    """
    row_count: int = 0
    size_bytes: int = 0
    last_refresh: Optional[datetime] = None
    refresh_duration_ms: Optional[float] = None
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        size_mb = round(self.size_bytes / (1024 * 1024), 2)
        return f"ViewStats({self.row_count} rows, {size_mb} MB)"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
            "refresh_duration_ms": self.refresh_duration_ms,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ViewStats":
        """Create from dictionary."""
        return cls(
            row_count=data.get("row_count", 0),
            size_bytes=data.get("size_bytes", 0),
            last_refresh=datetime.fromisoformat(data["last_refresh"]) if data.get("last_refresh") else None,
            refresh_duration_ms=data.get("refresh_duration_ms"),
        )


@dataclass
class SyncState:
    """
    State tracking for incremental sync.
    
    Attributes:
        last_sync_value: Last value of the sync column (for incremental)
        last_sync_row_count: Row count after last sync
        sync_history: History of sync operations
    """
    last_sync_value: Any = None
    last_sync_row_count: int = 0
    sync_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "last_sync_value": self.last_sync_value,
            "last_sync_row_count": self.last_sync_row_count,
            "sync_history": self.sync_history,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyncState":
        """Create from dictionary."""
        return cls(
            last_sync_value=data.get("last_sync_value"),
            last_sync_row_count=data.get("last_sync_row_count", 0),
            sync_history=data.get("sync_history", []),
        )


@dataclass
class ViewInfo:
    """Combined view information for listing."""
    definition: ViewDefinition
    stats: ViewStats
    sync_state: Optional[SyncState] = None
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"ViewInfo({self.definition.name}: {self.stats.row_count} rows)"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "name": self.definition.name,
            "query": self.definition.query,
            "source_adapter": self.definition.source_adapter,
            "source_table": self.definition.source_table,
            "refresh_strategy": self.definition.refresh_strategy.value,
            "sync_column": self.definition.sync_column,
            "row_count": self.stats.row_count,
            "size_mb": round(self.stats.size_bytes / (1024 * 1024), 2),
            "last_refresh": self.stats.last_refresh.isoformat() if self.stats.last_refresh else None,
            "created_at": self.definition.created_at.isoformat(),
        }
