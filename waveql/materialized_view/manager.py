"""
Materialized View Manager - Main orchestrator for view operations
"""

from __future__ import annotations
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa

from waveql.materialized_view.models import (
    ColumnInfo,
    RefreshStrategy,
    SyncState,
    ViewDefinition,
    ViewInfo,
    ViewStats,
)
from waveql.materialized_view.registry import ViewRegistry
from waveql.materialized_view.storage import ViewStorage
from waveql.materialized_view.sync import IncrementalSyncer, get_default_sync_column
from waveql.exceptions import WaveQLError

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


class MaterializedViewError(WaveQLError):
    """Error related to materialized view operations."""
    pass


class MaterializedViewManager:
    """
    Orchestrates all materialized view operations.
    
    Provides high-level API for creating, refreshing, querying, and dropping
    materialized views.
    """
    
    def __init__(
        self,
        connection: "WaveQLConnection",
        storage_path: Path = None,
        registry_path: Path = None,
    ):
        """
        Initialize the manager.
        
        Args:
            connection: WaveQL connection instance
            storage_path: Path for Parquet files (defaults to ~/.waveql/views/)
            registry_path: Path for SQLite registry (defaults to ~/.waveql/registry.db)
        """
        self.connection = connection
        self.registry = ViewRegistry(registry_path)
        self.storage = ViewStorage(storage_path)
        self.syncer = IncrementalSyncer()
        
        # Register existing views with DuckDB on init
        self._register_existing_views()
    
    def _register_existing_views(self) -> None:
        """Register all existing materialized views with DuckDB."""
        for view_info in self.registry.list_all():
            view = view_info.definition
            if self.storage.exists(view.name):
                self._register_with_duckdb(view.name)
    
    def _register_with_duckdb(self, name: str) -> None:
        """
        Register a materialized view's Parquet file with DuckDB.
        
        Creates a view in DuckDB that points to the Parquet file.
        """
        data_path = self.storage.get_data_path(name)
        if not data_path.exists():
            return
        
        # Use DuckDB's ability to query Parquet directly
        # Create a view that references the Parquet file
        sql = f"""
            CREATE OR REPLACE VIEW "{name}" AS 
            SELECT * FROM read_parquet('{data_path.as_posix()}')
        """
        try:
            self.connection.duckdb.execute(sql)
            logger.debug("Registered view '%s' with DuckDB", name)
        except Exception as e:
            logger.warning("Failed to register view '%s' with DuckDB: %s", name, e)
    
    def _unregister_from_duckdb(self, name: str) -> None:
        """Unregister a view from DuckDB."""
        try:
            self.connection.duckdb.execute(f'DROP VIEW IF EXISTS "{name}"')
        except Exception as e:
            logger.warning("Failed to unregister view '%s' from DuckDB: %s", name, e)
    
    def _parse_query(self, query: str) -> Dict[str, Any]:
        """
        Parse a query to extract source information.
        
        Returns:
            Dict with adapter, table, and other query info
        """
        from waveql.query_planner import QueryPlanner
        
        planner = QueryPlanner()
        query_info = planner.parse(query)
        
        result = {
            "table": query_info.table,
            "columns": query_info.columns,
            "predicates": query_info.predicates,
        }
        
        # Extract adapter from schema-qualified name
        if query_info.table and "." in query_info.table:
            parts = query_info.table.split(".", 1)
            result["adapter"] = parts[0].strip('"')
            result["table"] = parts[1].strip('"')
        else:
            result["adapter"] = None
        
        return result
    
    def create(
        self,
        name: str,
        query: str,
        refresh_strategy: str = "full",
        sync_column: str = None,
        if_not_exists: bool = False,
    ) -> ViewDefinition:
        """
        Create a new materialized view.
        
        Args:
            name: Unique name for the view
            query: SQL query defining the view
            refresh_strategy: 'full' or 'incremental'
            sync_column: Column for incremental sync (auto-detected if not provided)
            if_not_exists: If True, don't error if view already exists
            
        Returns:
            ViewDefinition of the created view
        """
        # Check if already exists
        if self.registry.exists(name):
            if if_not_exists:
                logger.info("View '%s' already exists, skipping creation", name)
                return self.registry.get_definition(name)
            raise MaterializedViewError(f"Materialized view '{name}' already exists")
        
        # Validate name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise MaterializedViewError(
                f"Invalid view name '{name}'. Must start with letter/underscore "
                "and contain only alphanumeric characters and underscores."
            )
        
        # Parse query to get source info
        parsed = self._parse_query(query)
        
        # Determine refresh strategy
        strategy = RefreshStrategy(refresh_strategy)
        
        # Auto-detect sync column for incremental
        if strategy == RefreshStrategy.INCREMENTAL and not sync_column:
            if parsed.get("adapter"):
                sync_column = get_default_sync_column(parsed["adapter"], parsed["table"])
            if not sync_column:
                logger.warning(
                    "No sync_column specified for incremental refresh. "
                    "Will use full refresh until configured."
                )
                strategy = RefreshStrategy.FULL
        
        # Create view definition
        view = ViewDefinition(
            name=name,
            query=query,
            source_adapter=parsed.get("adapter") or "default",
            source_table=parsed.get("table"),
            refresh_strategy=strategy,
            sync_column=sync_column,
            storage_path=self.storage.get_view_dir(name),
            created_at=datetime.now(),
        )
        
        logger.info(
            "Creating materialized view '%s' from %s.%s",
            name, view.source_adapter, view.source_table
        )
        
        # Execute query and get initial data
        start_time = time.time()
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        
        # Convert to Arrow Table
        if hasattr(result, 'to_pyarrow'):
            data = result.to_pyarrow()
        elif isinstance(result, pa.Table):
            data = result
        else:
            # Convert via DuckDB
            data = self.connection.duckdb.execute(query).fetch_arrow_table()
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Extract column info from data
        view.columns = [
            ColumnInfo(
                name=field.name,
                data_type=str(field.type),
                nullable=field.nullable,
            )
            for field in data.schema
        ]
        
        # Write to storage
        stats = self.storage.write(name, data)
        stats.refresh_duration_ms = duration_ms
        
        # Register in catalog
        self.registry.register(view, stats)
        
        # Initialize sync state for incremental
        if strategy == RefreshStrategy.INCREMENTAL and sync_column:
            initial_state = SyncState(
                last_sync_value=self.syncer._get_max_value(data, sync_column),
                last_sync_row_count=len(data),
            )
            self.registry.update_sync_state(name, initial_state)
        
        # Register with DuckDB
        self._register_with_duckdb(name)
        
        logger.info(
            "Created materialized view '%s': %d rows, %.2f MB in %.2fms",
            name, stats.row_count, stats.size_bytes / (1024 * 1024), duration_ms
        )
        
        return view
    
    def refresh(
        self,
        name: str,
        mode: str = None,
        force_full: bool = False,
    ) -> ViewStats:
        """
        Refresh a materialized view.
        
        Args:
            name: View name
            mode: Override refresh mode ('full' or 'incremental')
            force_full: If True, always do full refresh
            
        Returns:
            Updated ViewStats
        """
        view_info = self.registry.get(name)
        if not view_info:
            raise MaterializedViewError(f"Materialized view '{name}' not found")
        
        view = view_info.definition
        
        # Determine refresh mode
        if force_full or mode == "full":
            strategy = RefreshStrategy.FULL
        elif mode == "incremental":
            strategy = RefreshStrategy.INCREMENTAL
        else:
            strategy = view.refresh_strategy
        
        start_time = time.time()
        
        if strategy == RefreshStrategy.INCREMENTAL and view.sync_column:
            # Incremental refresh
            stats = self._refresh_incremental(view, view_info.sync_state)
        else:
            # Full refresh
            stats = self._refresh_full(view)
        
        stats.refresh_duration_ms = (time.time() - start_time) * 1000
        
        # Update stats in registry
        self.registry.update_stats(name, stats)
        
        # Re-register with DuckDB (in case schema changed)
        self._register_with_duckdb(name)
        
        logger.info(
            "Refreshed materialized view '%s': %d rows, %.2f MB in %.2fms",
            name, stats.row_count, stats.size_bytes / (1024 * 1024),
            stats.refresh_duration_ms
        )
        
        return stats
    
    def _refresh_full(self, view: ViewDefinition) -> ViewStats:
        """Perform a full refresh of the view."""
        logger.info("Performing full refresh of '%s'", view.name)
        
        # Re-execute the query
        data = self.connection.duckdb.execute(view.query).fetch_arrow_table()
        
        # Write to storage (replaces existing)
        stats = self.storage.write(view.name, data)
        stats.last_refresh = datetime.now()
        
        # Reset sync state if incremental is configured
        if view.sync_column:
            new_state = SyncState(
                last_sync_value=self.syncer._get_max_value(data, view.sync_column),
                last_sync_row_count=len(data),
            )
            self.registry.update_sync_state(view.name, new_state)
        
        return stats
    
    def _refresh_incremental(
        self,
        view: ViewDefinition,
        sync_state: SyncState,
    ) -> ViewStats:
        """Perform an incremental refresh of the view."""
        logger.info("Performing incremental refresh of '%s'", view.name)
        
        if sync_state is None:
            sync_state = SyncState()
        
        # Get the adapter
        adapter = self.connection.get_adapter(view.source_adapter)
        if not adapter:
            logger.warning(
                "Adapter '%s' not found, falling back to full refresh",
                view.source_adapter
            )
            return self._refresh_full(view)
        
        # Perform incremental sync
        new_data, new_state, sync_mode = self.syncer.sync(
            view=view,
            adapter=adapter,
            current_state=sync_state,
        )
        
        if new_data is None or len(new_data) == 0:
            logger.info("No new data to sync for '%s'", view.name)
            # Return current stats
            return self.storage.get_stats(view.name) or ViewStats()
        
        # Apply changes to storage
        if sync_mode == "upsert":
            # For now, just append (upsert requires primary key knowledge)
            stats = self.storage.append(view.name, new_data)
        else:
            stats = self.storage.append(view.name, new_data)
        
        stats.last_refresh = datetime.now()
        
        # Update sync state
        self.registry.update_sync_state(view.name, new_state)
        
        return stats
    
    def drop(self, name: str, if_exists: bool = False) -> bool:
        """
        Drop a materialized view.
        
        Args:
            name: View name
            if_exists: If True, don't error if view doesn't exist
            
        Returns:
            True if dropped, False if not found (when if_exists=True)
        """
        if not self.registry.exists(name):
            if if_exists:
                return False
            raise MaterializedViewError(f"Materialized view '{name}' not found")
        
        # Unregister from DuckDB
        self._unregister_from_duckdb(name)
        
        # Delete from storage
        self.storage.delete(name)
        
        # Delete from registry
        self.registry.delete(name)
        
        logger.info("Dropped materialized view '%s'", name)
        return True
    
    def get(self, name: str) -> Optional[ViewInfo]:
        """Get information about a materialized view."""
        return self.registry.get(name)
    
    def list_all(self) -> List[Dict[str, Any]]:
        """
        List all materialized views.
        
        Returns:
            List of view info dictionaries
        """
        views = self.registry.list_all()
        return [v.to_dict() for v in views]
    
    def exists(self, name: str) -> bool:
        """Check if a materialized view exists."""
        return self.registry.exists(name)
    
    def resolve(self, table_name: str) -> Optional[Path]:
        """
        Check if a table name is a materialized view.
        
        Args:
            table_name: Table name to check
            
        Returns:
            Path to Parquet file if it's a materialized view, None otherwise
        """
        # Strip quotes if present
        clean_name = table_name.strip('"')
        
        if self.registry.exists(clean_name) and self.storage.exists(clean_name):
            return self.storage.get_data_path(clean_name)
        
        return None
    
    def close(self) -> None:
        """Clean up resources."""
        self.registry.close()
