"""
CDC Data Models - Change records and streaming structures
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class ChangeType(str, Enum):
    """Type of change detected."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UNKNOWN = "unknown"


@dataclass
class Change:
    """
    Represents a single change event from a data source.
    
    Attributes:
        table: Source table name
        operation: Type of change (insert/update/delete)
        key: Primary key of the changed record
        data: Full record data (for insert/update)
        old_data: Previous record data (for update, if available)
        timestamp: When the change occurred
        source_adapter: Name of the adapter that detected the change
        metadata: Additional provider-specific metadata
    """
    table: str
    operation: ChangeType
    key: Any
    data: Optional[Dict[str, Any]] = None
    old_data: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.now)
    source_adapter: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"Change({self.operation.value} {self.table} key={self.key})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "table": self.table,
            "operation": self.operation.value,
            "key": self.key,
            "data": self.data,
            "old_data": self.old_data,
            "timestamp": self.timestamp.isoformat(),
            "source_adapter": self.source_adapter,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Change":
        """Create from dictionary."""
        return cls(
            table=data["table"],
            operation=ChangeType(data["operation"]),
            key=data["key"],
            data=data.get("data"),
            old_data=data.get("old_data"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.now(),
            source_adapter=data.get("source_adapter", ""),
            metadata=data.get("metadata", {}),
        )
    
    @property
    def is_insert(self) -> bool:
        """Check if this is an insert operation."""
        return self.operation == ChangeType.INSERT
    
    @property
    def is_update(self) -> bool:
        """Check if this is an update operation."""
        return self.operation == ChangeType.UPDATE
    
    @property
    def is_delete(self) -> bool:
        """Check if this is a delete operation."""
        return self.operation == ChangeType.DELETE


@dataclass
class ChangeStream:
    """
    Represents a stream of changes for tracking state.
    
    Attributes:
        table: Table being watched
        adapter: Adapter name
        last_sync: Last sync timestamp
        last_key: Last processed key (for cursor-based sync)
        changes_processed: Total changes processed
        errors: List of errors encountered
    """
    table: str
    adapter: str
    last_sync: Optional[datetime] = None
    last_key: Optional[Any] = None
    changes_processed: int = 0
    errors: List[str] = field(default_factory=list)
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"ChangeStream({self.table}: {self.changes_processed} changes)"
    
    def update(self, change: Change) -> None:
        """Update stream state with a processed change."""
        self.last_sync = change.timestamp
        self.last_key = change.key
        self.changes_processed += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return {
            "table": self.table,
            "adapter": self.adapter,
            "last_sync": self.last_sync.isoformat() if self.last_sync else None,
            "last_key": self.last_key,
            "changes_processed": self.changes_processed,
            "errors": self.errors,
        }


@dataclass
class CDCConfig:
    """
    Configuration for CDC streaming.
    
    Attributes:
        poll_interval: Seconds between polling (for poll-based CDC)
        batch_size: Max changes to fetch per batch
        include_data: Whether to include full record data
        since: Only get changes after this timestamp
        key_column: Primary key column name
        sync_column: Column used for ordering changes (e.g., sys_updated_on)
        filters: Additional filters to apply
    """
    poll_interval: float = 5.0
    batch_size: int = 100
    include_data: bool = True
    since: Optional[datetime] = None
    key_column: str = "sys_id"
    sync_column: str = "sys_updated_on"
    filters: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.batch_size > 10000:
            raise ValueError("batch_size cannot exceed 10000")
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"CDCConfig(poll={self.poll_interval}s, batch={self.batch_size})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "poll_interval": self.poll_interval,
            "batch_size": self.batch_size,
            "include_data": self.include_data,
            "since": self.since.isoformat() if self.since else None,
            "key_column": self.key_column,
            "sync_column": self.sync_column,
            "filters": self.filters,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CDCConfig":
        """Create from dictionary."""
        return cls(
            poll_interval=data.get("poll_interval", 5.0),
            batch_size=data.get("batch_size", 100),
            include_data=data.get("include_data", True),
            since=datetime.fromisoformat(data["since"]) if data.get("since") else None,
            key_column=data.get("key_column", "sys_id"),
            sync_column=data.get("sync_column", "sys_updated_on"),
            filters=data.get("filters", {}),
        )
