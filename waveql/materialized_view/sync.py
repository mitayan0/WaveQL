"""
Incremental Sync - Logic for incremental refresh of materialized views
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional, Tuple

import pyarrow as pa

from waveql.materialized_view.models import SyncState, ViewDefinition, RefreshStrategy
from waveql.query_planner import Predicate

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class IncrementalSyncer:
    """
    Handles incremental synchronization logic for materialized views.
    
    Incremental sync fetches only records that have changed since the last sync,
    based on a timestamp or version column.
    """
    
    def sync(
        self,
        view: ViewDefinition,
        adapter: "BaseAdapter",
        current_state: SyncState,
        key_column: str = None,
    ) -> Tuple[pa.Table, SyncState, str]:
        """
        Perform incremental sync.
        
        Args:
            view: View definition
            adapter: Source adapter
            current_state: Current sync state
            key_column: Optional key column for upsert logic
            
        Returns:
            Tuple of (new_data, updated_state, sync_mode)
            sync_mode is 'append' or 'upsert'
        """
        if not view.sync_column:
            raise ValueError(
                f"View '{view.name}' does not have a sync_column configured. "
                "Cannot perform incremental sync."
            )
        
        # Build predicate for incremental fetch
        predicates = []
        
        if current_state.last_sync_value is not None:
            predicates.append(
                Predicate(
                    column=view.sync_column,
                    operator=">",
                    value=current_state.last_sync_value,
                )
            )
            logger.info(
                "Incremental sync for '%s': fetching records where %s > %s",
                view.name, view.sync_column, current_state.last_sync_value
            )
        else:
            logger.info(
                "First sync for '%s': fetching all records",
                view.name
            )
        
        # Fetch from adapter
        start_time = datetime.now()
        new_data = adapter.fetch(
            table=view.source_table,
            predicates=predicates if predicates else None,
        )
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        rows_fetched = len(new_data) if new_data else 0
        logger.info(
            "Fetched %d new/updated records in %.2fms",
            rows_fetched, duration_ms
        )
        
        # Determine new sync value
        new_sync_value = self._get_max_value(new_data, view.sync_column)
        if new_sync_value is None and current_state.last_sync_value is not None:
            new_sync_value = current_state.last_sync_value
        
        # Update sync state
        new_state = SyncState(
            last_sync_value=new_sync_value,
            last_sync_row_count=current_state.last_sync_row_count + rows_fetched,
            sync_history=current_state.sync_history + [
                {
                    "timestamp": datetime.now().isoformat(),
                    "rows_fetched": rows_fetched,
                    "duration_ms": duration_ms,
                }
            ],
        )
        
        # Determine sync mode
        # If there's a key column, we should upsert (in case of updates)
        # Otherwise, just append
        sync_mode = "upsert" if key_column else "append"
        
        return new_data, new_state, sync_mode
    
    def _get_max_value(self, data: pa.Table, column: str) -> Optional[Any]:
        """
        Get the maximum value of a column from the data.
        
        Args:
            data: PyArrow Table
            column: Column name
            
        Returns:
            Maximum value or None
        """
        if data is None or len(data) == 0:
            return None
        
        if column not in data.column_names:
            logger.warning("Sync column '%s' not found in data", column)
            return None
        
        # Use PyArrow compute for efficiency
        import pyarrow.compute as pc
        
        column_data = data.column(column)
        max_value = pc.max(column_data).as_py()
        
        return max_value
    
    def estimate_changes(
        self,
        view: ViewDefinition,
        adapter: "BaseAdapter",
        current_state: SyncState,
    ) -> dict:
        """
        Estimate the number of changes without fetching full data.
        
        This is adapter-dependent and may not be supported by all adapters.
        
        Returns:
            Dict with estimation info
        """
        # This would require adapter support for COUNT queries
        # For now, return a placeholder
        return {
            "supported": False,
            "estimated_changes": None,
            "last_sync_value": current_state.last_sync_value,
        }


def get_default_sync_column(adapter_name: str, table: str) -> Optional[str]:
    """
    Get the default sync column for a given adapter and table.
    
    Args:
        adapter_name: Name of the adapter (e.g., 'servicenow', 'jira')
        table: Table name
        
    Returns:
        Default sync column name or None
    """
    # Common timestamp columns by adapter
    defaults = {
        "servicenow": "sys_updated_on",
        "salesforce": "LastModifiedDate",
        "jira": "updated",
        "sql": "updated_at",
    }
    
    return defaults.get(adapter_name.lower())
