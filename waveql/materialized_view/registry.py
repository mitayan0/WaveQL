"""
View Registry - SQLite-backed catalog for materialized views
"""

from __future__ import annotations
import json
import logging
import sqlite3
from pathlib import Path
from typing import List, Optional

from waveql.materialized_view.models import (
    ViewDefinition,
    ViewStats,
    SyncState,
    ViewInfo,
)

logger = logging.getLogger(__name__)


class ViewRegistry:
    """
    SQLite-backed registry for materialized view metadata.
    
    Stores view definitions, statistics, and sync state in a local SQLite database.
    """
    
    def __init__(self, db_path: Path = None):
        """
        Initialize the registry.
        
        Args:
            db_path: Path to SQLite database. Defaults to ~/.waveql/registry.db
        """
        if db_path is None:
            db_path = Path.home() / ".waveql" / "registry.db"
        
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS materialized_views (
                    name TEXT PRIMARY KEY,
                    definition TEXT NOT NULL,
                    stats TEXT,
                    sync_state TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.commit()
        logger.debug("Initialized view registry at %s", self.db_path)
    
    def register(self, view: ViewDefinition, stats: ViewStats = None) -> None:
        """
        Register a new materialized view.
        
        Args:
            view: View definition
            stats: Optional initial statistics
        """
        from datetime import datetime
        
        now = datetime.now().isoformat()
        stats = stats or ViewStats()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO materialized_views 
                (name, definition, stats, sync_state, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                view.name,
                json.dumps(view.to_dict()),
                json.dumps(stats.to_dict()),
                json.dumps(SyncState().to_dict()),
                view.created_at.isoformat(),
                now,
            ))
            conn.commit()
        logger.info("Registered materialized view: %s", view.name)
    
    def get(self, name: str) -> Optional[ViewInfo]:
        """
        Get a materialized view by name.
        
        Args:
            name: View name
            
        Returns:
            ViewInfo or None if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT definition, stats, sync_state FROM materialized_views WHERE name = ?",
                (name,)
            )
            row = cursor.fetchone()
            
        if not row:
            return None
        
        definition = ViewDefinition.from_dict(json.loads(row[0]))
        stats = ViewStats.from_dict(json.loads(row[1])) if row[1] else ViewStats()
        sync_state = SyncState.from_dict(json.loads(row[2])) if row[2] else None
        
        return ViewInfo(definition=definition, stats=stats, sync_state=sync_state)
    
    def get_definition(self, name: str) -> Optional[ViewDefinition]:
        """Get just the view definition."""
        info = self.get(name)
        return info.definition if info else None
    
    def exists(self, name: str) -> bool:
        """Check if a view exists."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT 1 FROM materialized_views WHERE name = ?",
                (name,)
            )
            return cursor.fetchone() is not None
    
    def list_all(self) -> List[ViewInfo]:
        """
        List all registered materialized views.
        
        Returns:
            List of ViewInfo objects
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT definition, stats, sync_state FROM materialized_views ORDER BY name"
            )
            rows = cursor.fetchall()
        
        views = []
        for row in rows:
            definition = ViewDefinition.from_dict(json.loads(row[0]))
            stats = ViewStats.from_dict(json.loads(row[1])) if row[1] else ViewStats()
            sync_state = SyncState.from_dict(json.loads(row[2])) if row[2] else None
            views.append(ViewInfo(definition=definition, stats=stats, sync_state=sync_state))
        
        return views
    
    def update_stats(self, name: str, stats: ViewStats) -> None:
        """
        Update statistics for a view.
        
        Args:
            name: View name
            stats: Updated statistics
        """
        from datetime import datetime
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE materialized_views 
                SET stats = ?, updated_at = ?
                WHERE name = ?
            """, (
                json.dumps(stats.to_dict()),
                datetime.now().isoformat(),
                name,
            ))
            conn.commit()
    
    def update_sync_state(self, name: str, sync_state: SyncState) -> None:
        """
        Update sync state for a view.
        
        Args:
            name: View name
            sync_state: Updated sync state
        """
        from datetime import datetime
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE materialized_views 
                SET sync_state = ?, updated_at = ?
                WHERE name = ?
            """, (
                json.dumps(sync_state.to_dict()),
                datetime.now().isoformat(),
                name,
            ))
            conn.commit()
    
    def delete(self, name: str) -> bool:
        """
        Delete a materialized view from the registry.
        
        Args:
            name: View name
            
        Returns:
            True if deleted, False if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM materialized_views WHERE name = ?",
                (name,)
            )
            conn.commit()
            deleted = cursor.rowcount > 0
        
        if deleted:
            logger.info("Deleted materialized view from registry: %s", name)
        return deleted
    
    def close(self) -> None:
        """Close the registry (no-op for SQLite, kept for interface consistency)."""
        pass
