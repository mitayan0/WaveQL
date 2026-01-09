"""
CDC State Backend - Persistent state storage for CDC streams

This module provides persistence backends for CDC stream positions,
allowing streams to survive restarts and resume from where they left off.

Supported Backends:
- SQLite (default, local file-based)
- Redis (distributed, requires redis-py)
- Memory (ephemeral, for testing)

Usage:
    from waveql.cdc.state import StateBackend, SQLiteStateBackend
    
    backend = SQLiteStateBackend(".waveql_state.db")
    backend.save_position("incident", "servicenow", lsn="12345", offset=100)
    
    pos = backend.get_position("incident", "servicenow")
    # {"lsn": "12345", "offset": 100, "last_sync": "2024-01-15T10:00:00"}
"""

from __future__ import annotations
import json
import logging
import sqlite3
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class StreamPosition:
    """
    Represents the current position/offset in a CDC stream.
    
    Attributes:
        table: Table name being streamed
        adapter: Adapter name (e.g., "servicenow", "postgres")
        lsn: Log Sequence Number (for WAL-based CDC)
        offset: Numeric offset or page number
        last_key: Last processed record key
        last_sync: Timestamp of last successful sync
        metadata: Additional position metadata
    """
    table: str
    adapter: str
    lsn: Optional[str] = None
    offset: Optional[int] = None
    last_key: Optional[str] = None
    last_sync: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "table": self.table,
            "adapter": self.adapter,
            "lsn": self.lsn,
            "offset": self.offset,
            "last_key": self.last_key,
            "last_sync": self.last_sync.isoformat() if self.last_sync else None,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamPosition":
        """Create from dictionary."""
        return cls(
            table=data["table"],
            adapter=data["adapter"],
            lsn=data.get("lsn"),
            offset=data.get("offset"),
            last_key=data.get("last_key"),
            last_sync=datetime.fromisoformat(data["last_sync"]) if data.get("last_sync") else None,
            metadata=data.get("metadata", {}),
        )
    
    def __repr__(self) -> str:
        pos = self.lsn or self.offset or self.last_key or "unknown"
        return f"StreamPosition({self.adapter}.{self.table} @ {pos})"


class StateBackend(ABC):
    """
    Abstract base class for CDC state persistence backends.
    
    State backends store stream positions so CDC can resume after restarts.
    """
    
    @abstractmethod
    def save_position(
        self,
        table: str,
        adapter: str,
        lsn: Optional[str] = None,
        offset: Optional[int] = None,
        last_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Save the current stream position.
        
        Args:
            table: Table name
            adapter: Adapter name
            lsn: Log Sequence Number (for WAL-based)
            offset: Numeric offset
            last_key: Last processed key
            metadata: Additional metadata
        """
        pass
    
    @abstractmethod
    def get_position(self, table: str, adapter: str) -> Optional[StreamPosition]:
        """
        Get the saved position for a stream.
        
        Args:
            table: Table name
            adapter: Adapter name
            
        Returns:
            StreamPosition if found, None otherwise
        """
        pass
    
    @abstractmethod
    def delete_position(self, table: str, adapter: str) -> bool:
        """
        Delete a saved position.
        
        Args:
            table: Table name
            adapter: Adapter name
            
        Returns:
            True if deleted, False if not found
        """
        pass
    
    @abstractmethod
    def list_positions(self, adapter: Optional[str] = None) -> list[StreamPosition]:
        """
        List all saved positions.
        
        Args:
            adapter: Optional filter by adapter
            
        Returns:
            List of StreamPosition objects
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the backend and release resources."""
        pass


class MemoryStateBackend(StateBackend):
    """
    In-memory state backend for testing.
    
    State is lost when the process exits.
    """
    
    def __init__(self):
        self._positions: Dict[str, StreamPosition] = {}
        self._lock = threading.Lock()
    
    def _key(self, table: str, adapter: str) -> str:
        return f"{adapter}:{table}"
    
    def save_position(
        self,
        table: str,
        adapter: str,
        lsn: Optional[str] = None,
        offset: Optional[int] = None,
        last_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            key = self._key(table, adapter)
            self._positions[key] = StreamPosition(
                table=table,
                adapter=adapter,
                lsn=lsn,
                offset=offset,
                last_key=last_key,
                last_sync=datetime.now(),
                metadata=metadata or {},
            )
    
    def get_position(self, table: str, adapter: str) -> Optional[StreamPosition]:
        with self._lock:
            return self._positions.get(self._key(table, adapter))
    
    def delete_position(self, table: str, adapter: str) -> bool:
        with self._lock:
            key = self._key(table, adapter)
            if key in self._positions:
                del self._positions[key]
                return True
            return False
    
    def list_positions(self, adapter: Optional[str] = None) -> list[StreamPosition]:
        with self._lock:
            positions = list(self._positions.values())
            if adapter:
                positions = [p for p in positions if p.adapter == adapter]
            return positions
    
    def close(self) -> None:
        self._positions.clear()


class SQLiteStateBackend(StateBackend):
    """
    SQLite-based state backend for local persistence.
    
    State is stored in a local SQLite database file.
    Thread-safe for concurrent access.
    """
    
    DEFAULT_PATH = ".waveql_state.db"
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize SQLite backend.
        
        Args:
            db_path: Path to SQLite database file. If None, uses centralized config.
        """
        if db_path is None:
            try:
                from waveql.config import get_config
                db_path = str(get_config().cdc_state_db)
            except ImportError:
                db_path = self.DEFAULT_PATH
        
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cdc_positions (
                        adapter TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        lsn TEXT,
                        offset_value INTEGER,
                        last_key TEXT,
                        last_sync TEXT,
                        metadata TEXT,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (adapter, table_name)
                    )
                """)
                conn.commit()
                logger.debug(f"Initialized CDC state backend at {self.db_path}")
            finally:
                conn.close()
    
    def save_position(
        self,
        table: str,
        adapter: str,
        lsn: Optional[str] = None,
        offset: Optional[int] = None,
        last_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                now = datetime.now().isoformat()
                metadata_json = json.dumps(metadata or {})
                
                conn.execute("""
                    INSERT OR REPLACE INTO cdc_positions 
                    (adapter, table_name, lsn, offset_value, last_key, last_sync, metadata, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (adapter, table, lsn, offset, last_key, now, metadata_json, now))
                
                conn.commit()
                logger.debug(f"Saved CDC position for {adapter}.{table}")
            finally:
                conn.close()
    
    def get_position(self, table: str, adapter: str) -> Optional[StreamPosition]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute("""
                    SELECT lsn, offset_value, last_key, last_sync, metadata
                    FROM cdc_positions
                    WHERE adapter = ? AND table_name = ?
                """, (adapter, table))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                lsn, offset, last_key, last_sync, metadata_json = row
                
                return StreamPosition(
                    table=table,
                    adapter=adapter,
                    lsn=lsn,
                    offset=offset,
                    last_key=last_key,
                    last_sync=datetime.fromisoformat(last_sync) if last_sync else None,
                    metadata=json.loads(metadata_json) if metadata_json else {},
                )
            finally:
                conn.close()
    
    def delete_position(self, table: str, adapter: str) -> bool:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute("""
                    DELETE FROM cdc_positions
                    WHERE adapter = ? AND table_name = ?
                """, (adapter, table))
                
                conn.commit()
                return cursor.rowcount > 0
            finally:
                conn.close()
    
    def list_positions(self, adapter: Optional[str] = None) -> list[StreamPosition]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                if adapter:
                    cursor = conn.execute("""
                        SELECT adapter, table_name, lsn, offset_value, last_key, last_sync, metadata
                        FROM cdc_positions
                        WHERE adapter = ?
                    """, (adapter,))
                else:
                    cursor = conn.execute("""
                        SELECT adapter, table_name, lsn, offset_value, last_key, last_sync, metadata
                        FROM cdc_positions
                    """)
                
                positions = []
                for row in cursor:
                    adapter_name, table, lsn, offset, last_key, last_sync, metadata_json = row
                    positions.append(StreamPosition(
                        table=table,
                        adapter=adapter_name,
                        lsn=lsn,
                        offset=offset,
                        last_key=last_key,
                        last_sync=datetime.fromisoformat(last_sync) if last_sync else None,
                        metadata=json.loads(metadata_json) if metadata_json else {},
                    ))
                
                return positions
            finally:
                conn.close()
    
    def close(self) -> None:
        """No persistent connection to close for SQLite."""
        pass


class RedisStateBackend(StateBackend):
    """
    Redis-based state backend for distributed persistence.
    
    Requires redis-py: pip install redis
    """
    
    KEY_PREFIX = "waveql:cdc:position:"
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize Redis backend.
        
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Redis password
            **kwargs: Additional redis-py connection args
        """
        try:
            import redis
        except ImportError:
            raise ImportError("redis package required: pip install redis")
        
        self._client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            **kwargs,
        )
        logger.debug(f"Initialized Redis CDC state backend at {host}:{port}")
    
    def _key(self, table: str, adapter: str) -> str:
        return f"{self.KEY_PREFIX}{adapter}:{table}"
    
    def save_position(
        self,
        table: str,
        adapter: str,
        lsn: Optional[str] = None,
        offset: Optional[int] = None,
        last_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        key = self._key(table, adapter)
        position = StreamPosition(
            table=table,
            adapter=adapter,
            lsn=lsn,
            offset=offset,
            last_key=last_key,
            last_sync=datetime.now(),
            metadata=metadata or {},
        )
        
        self._client.set(key, json.dumps(position.to_dict()))
        logger.debug(f"Saved CDC position for {adapter}.{table} to Redis")
    
    def get_position(self, table: str, adapter: str) -> Optional[StreamPosition]:
        key = self._key(table, adapter)
        data = self._client.get(key)
        
        if not data:
            return None
        
        return StreamPosition.from_dict(json.loads(data))
    
    def delete_position(self, table: str, adapter: str) -> bool:
        key = self._key(table, adapter)
        return self._client.delete(key) > 0
    
    def list_positions(self, adapter: Optional[str] = None) -> list[StreamPosition]:
        pattern = f"{self.KEY_PREFIX}{adapter or '*'}:*"
        keys = self._client.keys(pattern)
        
        positions = []
        for key in keys:
            data = self._client.get(key)
            if data:
                positions.append(StreamPosition.from_dict(json.loads(data)))
        
        return positions
    
    def close(self) -> None:
        self._client.close()


def create_state_backend(
    backend_type: str = "sqlite",
    **kwargs,
) -> StateBackend:
    """
    Factory function to create a state backend.
    
    Args:
        backend_type: Type of backend ("sqlite", "redis", "memory")
        **kwargs: Backend-specific configuration
        
    Returns:
        StateBackend instance
        
    Examples:
        # SQLite (default)
        backend = create_state_backend("sqlite", db_path=".waveql_state.db")
        
        # Redis
        backend = create_state_backend("redis", host="localhost", port=6379)
        
        # Memory (testing)
        backend = create_state_backend("memory")
    """
    backends = {
        "sqlite": SQLiteStateBackend,
        "redis": RedisStateBackend,
        "memory": MemoryStateBackend,
    }
    
    backend_class = backends.get(backend_type.lower())
    if not backend_class:
        raise ValueError(f"Unknown backend type: {backend_type}. Choose from: {list(backends.keys())}")
    
    return backend_class(**kwargs)
