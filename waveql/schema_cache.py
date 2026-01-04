"""
Schema Cache - SQLite-based metadata caching for dynamic schema discovery
"""

from __future__ import annotations
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Column metadata."""
    name: str
    data_type: str
    nullable: bool = True
    primary_key: bool = False
    description: str = ""
    arrow_type: Any = None  # Optional PyArrow DataType for struct/list support



@dataclass
class TableSchema:
    """Table schema information."""
    name: str
    columns: List[ColumnInfo]
    adapter: str
    discovered_at: float
    ttl: int = 3600  # Cache TTL in seconds
    
    def is_expired(self) -> bool:
        return time.time() - self.discovered_at > self.ttl
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "columns": [{"name": c.name, "data_type": c.data_type, "nullable": c.nullable,
                         "primary_key": c.primary_key, "description": c.description}
                        for c in self.columns],
            "adapter": self.adapter,
            "discovered_at": self.discovered_at,
            "ttl": self.ttl,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "TableSchema":
        return cls(
            name=data["name"],
            columns=[ColumnInfo(**c) for c in data["columns"]],
            adapter=data["adapter"],
            discovered_at=data["discovered_at"],
            ttl=data.get("ttl", 3600),
        )


class SchemaCache:
    """
    SQLite-based schema cache for dynamic table discovery.
    
    Features:
    - Persistent schema storage
    - TTL-based expiration
    - Adapter-specific schemas
    - SHOW TABLES / DESCRIBE support
    """
    
    def __init__(self, cache_path: str = None):
        """
        Initialize schema cache.
        
        Args:
            cache_path: Path to SQLite cache file. None for in-memory.
        """
        # Thread-safety lock for all database operations
        self._lock = threading.Lock()
        
        if cache_path:
            self._db_path = Path(cache_path)
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            # check_same_thread=False allows safe multi-threaded access with our lock
            self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        else:
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        
        logger.debug("SchemaCache initialized with path: %s", cache_path or ":memory:")
        self._init_tables()
    
    def _init_tables(self):
        """Create cache tables if they don't exist."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS schemas (
                adapter TEXT NOT NULL,
                table_name TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                discovered_at REAL NOT NULL,
                ttl INTEGER DEFAULT 3600,
                PRIMARY KEY (adapter, table_name)
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_schemas_adapter ON schemas(adapter)
        """)
        self._conn.commit()
    
    def get(self, adapter: str, table_name: str) -> Optional[TableSchema]:
        """
        Get cached schema for a table.
        
        Args:
            adapter: Adapter name
            table_name: Table name
            
        Returns:
            TableSchema if found and not expired, None otherwise
        """
        with self._lock:
            cursor = self._conn.execute(
                "SELECT schema_json, discovered_at, ttl FROM schemas WHERE adapter = ? AND table_name = ?",
                (adapter, table_name)
            )
            row = cursor.fetchone()
        
        if not row:
            return None
        
        schema_json, discovered_at, ttl = row
        schema = TableSchema.from_dict(json.loads(schema_json))
        
        if schema.is_expired():
            self.invalidate(adapter, table_name)
            return None
        
        return schema
    
    def set(self, adapter: str, table_name: str, columns: List[ColumnInfo], ttl: int = 3600):
        """
        Cache a table schema.
        
        Args:
            adapter: Adapter name
            table_name: Table name
            columns: List of column metadata
            ttl: Time-to-live in seconds
        """
        schema = TableSchema(
            name=table_name,
            columns=columns,
            adapter=adapter,
            discovered_at=time.time(),
            ttl=ttl,
        )
        
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO schemas (adapter, table_name, schema_json, discovered_at, ttl)
                   VALUES (?, ?, ?, ?, ?)""",
                (adapter, table_name, json.dumps(schema.to_dict()), schema.discovered_at, ttl)
            )
            self._conn.commit()
        logger.debug("Cached schema for %s.%s (TTL: %ds)", adapter, table_name, ttl)
    
    def invalidate(self, adapter: str, table_name: str = None):
        """
        Invalidate cached schemas.
        
        Args:
            adapter: Adapter name
            table_name: Optional specific table. None to invalidate all for adapter.
        """
        with self._lock:
            if table_name:
                self._conn.execute(
                    "DELETE FROM schemas WHERE adapter = ? AND table_name = ?",
                    (adapter, table_name)
                )
                logger.debug("Invalidated schema cache for %s.%s", adapter, table_name)
            else:
                self._conn.execute(
                    "DELETE FROM schemas WHERE adapter = ?",
                    (adapter,)
                )
                logger.debug("Invalidated all schema cache for adapter: %s", adapter)
            self._conn.commit()
    
    def list_tables(self, adapter: str = None) -> List[str]:
        """
        List all cached tables.
        
        Args:
            adapter: Optional filter by adapter
            
        Returns:
            List of table names
        """
        with self._lock:
            if adapter:
                cursor = self._conn.execute(
                    "SELECT table_name FROM schemas WHERE adapter = ?",
                    (adapter,)
                )
            else:
                cursor = self._conn.execute("SELECT DISTINCT table_name FROM schemas")
            
            return [row[0] for row in cursor.fetchall()]
    
    def describe_table(self, adapter: str, table_name: str) -> Optional[List[Dict]]:
        """
        Get table column descriptions (for DESCRIBE command).
        
        Returns:
            List of column info dicts, or None if not cached
        """
        schema = self.get(adapter, table_name)
        if not schema:
            return None
        
        return [
            {
                "Field": c.name,
                "Type": c.data_type,
                "Null": "YES" if c.nullable else "NO",
                "Key": "PRI" if c.primary_key else "",
                "Description": c.description,
            }
            for c in schema.columns
        ]
    
    def close(self):
        """Close the cache connection."""
        with self._lock:
            self._conn.close()
        logger.debug("SchemaCache closed")
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        path = getattr(self, '_db_path', ':memory:')
        return f"<SchemaCache path={path}>"
