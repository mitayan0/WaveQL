"""
CDC Providers - Adapter-specific CDC implementations

Each provider knows how to detect changes for a specific data source.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional

from waveql.cdc.models import Change, ChangeType, CDCConfig

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter


class BaseCDCProvider(ABC):
    """
    Base class for CDC providers.
    
    Each adapter can have a corresponding CDC provider that knows
    how to efficiently detect changes for that data source.
    """
    
    # Provider metadata
    provider_name: str = "base"
    supports_delete_detection: bool = False
    supports_old_data: bool = False
    
    # Retry configuration
    max_retries: int = 3
    retry_delay: float = 1.0
    
    def __init__(self, adapter: "BaseAdapter"):
        """
        Initialize the provider.
        
        Args:
            adapter: The adapter instance to use for fetching
        """
        self.adapter = adapter
        self._consecutive_errors = 0
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"<{self.__class__.__name__} adapter={getattr(self.adapter, 'adapter_name', 'unknown')}>"
    
    @abstractmethod
    async def get_changes(
        self,
        table: str,
        since: datetime = None,
        config: CDCConfig = None,
    ) -> List[Change]:
        """
        Get changes since a given timestamp.
        
        Args:
            table: Table to get changes for
            since: Only get changes after this timestamp
            config: CDC configuration
            
        Returns:
            List of Change objects
        """
        pass
    
    @abstractmethod
    async def stream_changes(
        self,
        table: str,
        config: CDCConfig = None,
    ) -> AsyncIterator[Change]:
        """
        Stream changes as they occur.
        
        Args:
            table: Table to watch
            config: CDC configuration
            
        Yields:
            Change objects as they are detected
        """
        pass
    
    def _detect_operation(
        self,
        record: Dict[str, Any],
        created_column: str = None,
        updated_column: str = None,
    ) -> ChangeType:
        """
        Detect the type of operation from a record.
        
        Heuristic: If created == updated, it's likely an insert.
        Otherwise, it's an update.
        """
        if created_column and updated_column:
            created = record.get(created_column)
            updated = record.get(updated_column)
            if created and updated and created == updated:
                return ChangeType.INSERT
            return ChangeType.UPDATE
        
        return ChangeType.UNKNOWN


class ServiceNowCDCProvider(BaseCDCProvider):
    """
    CDC provider for ServiceNow.
    
    Uses the Table API with sysparm_query to filter by sys_updated_on.
    ServiceNow doesn't have native CDC, so we poll for changes.
    
    For deletes, we would need to query sys_audit or maintain a local
    cache of all keys to detect missing records.
    """
    
    provider_name = "servicenow"
    supports_delete_detection = False  # Would require sys_audit access
    supports_old_data = False
    
    async def get_changes(
        self,
        table: str,
        since: datetime = None,
        config: CDCConfig = None,
    ) -> List[Change]:
        """Get changes from ServiceNow since a timestamp."""
        import anyio
        
        config = config or CDCConfig()
        
        # Build query for changes
        query_parts = []
        if since:
            # ServiceNow datetime format
            since_str = since.strftime("%Y-%m-%d %H:%M:%S")
            query_parts.append(f"sys_updated_on>{since_str}")
        
        # Add any custom filters
        for key, value in config.filters.items():
            query_parts.append(f"{key}={value}")
        
        sysparm_query = "^".join(query_parts) if query_parts else ""
        
        # Fetch changes via adapter
        from waveql.query_planner import Predicate
        
        predicates = []
        if since:
            predicates.append(Predicate(
                column=config.sync_column,
                operator=">",
                value=since.isoformat()
            ))
        
        # Run in thread since adapter.fetch is sync
        def fetch():
            return self.adapter.fetch(
                table=table,
                predicates=predicates if predicates else None,
                limit=config.batch_size,
                order_by=[(config.sync_column, "ASC")],
            )
        
        result = await anyio.to_thread.run_sync(fetch)
        
        # Convert to Change objects
        changes = []
        for i in range(len(result)):
            row = {name: result.column(name)[i].as_py() for name in result.column_names}
            
            change = Change(
                table=table,
                operation=self._detect_operation(row, "sys_created_on", "sys_updated_on"),
                key=row.get(config.key_column),
                data=row if config.include_data else None,
                timestamp=self._parse_timestamp(row.get(config.sync_column)),
                source_adapter="servicenow",
            )
            changes.append(change)
        
        return changes
    
    async def stream_changes(
        self,
        table: str,
        config: CDCConfig = None,
    ) -> AsyncIterator[Change]:
        """Stream changes by polling ServiceNow with retry logic."""
        import anyio
        import logging
        
        logger = logging.getLogger(__name__)
        config = config or CDCConfig()
        last_sync = config.since or datetime.now()
        
        while True:
            try:
                # Get changes since last sync
                changes = await self.get_changes(table, since=last_sync, config=config)
                self._consecutive_errors = 0  # Reset on success
                
                for change in changes:
                    yield change
                    # Update last sync to this change's timestamp
                    if change.timestamp and change.timestamp > last_sync:
                        last_sync = change.timestamp
                
                # Wait before next poll
                await anyio.sleep(config.poll_interval)
                
            except Exception as e:
                self._consecutive_errors += 1
                if self._consecutive_errors > self.max_retries:
                    logger.error("CDC stream failed after %d retries: %s", self.max_retries, e)
                    raise
                
                # Exponential backoff
                delay = self.retry_delay * (2 ** (self._consecutive_errors - 1))
                logger.warning(
                    "CDC stream error (attempt %d/%d), retrying in %.1fs: %s",
                    self._consecutive_errors, self.max_retries, delay, e
                )
                await anyio.sleep(delay)
    
    def _parse_timestamp(self, value: Any) -> datetime:
        """Parse ServiceNow timestamp."""
        if value is None:
            return datetime.now()
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            # ServiceNow format: "2026-01-04 12:00:00"
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    return datetime.now()
        return datetime.now()


class SalesforceCDCProvider(BaseCDCProvider):
    """
    CDC provider for Salesforce.
    
    Salesforce has native CDC via Platform Events (Change Data Capture events).
    This requires the CDC feature to be enabled and subscribed.
    
    For simplicity, this implementation uses polling with LastModifiedDate,
    but could be extended to use the Streaming API or Pub/Sub API.
    """
    
    provider_name = "salesforce"
    supports_delete_detection = True  # Via IsDeleted field
    supports_old_data = False
    
    async def get_changes(
        self,
        table: str,
        since: datetime = None,
        config: CDCConfig = None,
    ) -> List[Change]:
        """Get changes from Salesforce since a timestamp."""
        import anyio
        
        config = config or CDCConfig(
            key_column="Id",
            sync_column="LastModifiedDate"
        )
        
        from waveql.query_planner import Predicate
        
        predicates = []
        if since:
            predicates.append(Predicate(
                column=config.sync_column,
                operator=">",
                value=since.isoformat()
            ))
        
        def fetch():
            return self.adapter.fetch(
                table=table,
                predicates=predicates if predicates else None,
                limit=config.batch_size,
                order_by=[(config.sync_column, "ASC")],
            )
        
        result = await anyio.to_thread.run_sync(fetch)
        
        changes = []
        for i in range(len(result)):
            row = {name: result.column(name)[i].as_py() for name in result.column_names}
            
            # Detect operation
            if row.get("IsDeleted"):
                operation = ChangeType.DELETE
            else:
                operation = self._detect_operation(row, "CreatedDate", "LastModifiedDate")
            
            change = Change(
                table=table,
                operation=operation,
                key=row.get(config.key_column),
                data=row if config.include_data else None,
                timestamp=self._parse_timestamp(row.get(config.sync_column)),
                source_adapter="salesforce",
            )
            changes.append(change)
        
        return changes
    
    async def stream_changes(
        self,
        table: str,
        config: CDCConfig = None,
    ) -> AsyncIterator[Change]:
        """Stream changes by polling Salesforce."""
        import anyio
        
        config = config or CDCConfig(
            key_column="Id",
            sync_column="LastModifiedDate"
        )
        last_sync = config.since or datetime.now()
        
        while True:
            changes = await self.get_changes(table, since=last_sync, config=config)
            
            for change in changes:
                yield change
                if change.timestamp and change.timestamp > last_sync:
                    last_sync = change.timestamp
            
            await anyio.sleep(config.poll_interval)
    
    def _parse_timestamp(self, value: Any) -> datetime:
        """Parse Salesforce timestamp (ISO format)."""
        if value is None:
            return datetime.now()
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return datetime.now()
        return datetime.now()


class JiraCDCProvider(BaseCDCProvider):
    """
    CDC provider for Jira.
    
    Uses the Jira REST API with JQL to filter by updated date.
    """
    
    provider_name = "jira"
    supports_delete_detection = False
    supports_old_data = False
    
    async def get_changes(
        self,
        table: str,
        since: datetime = None,
        config: CDCConfig = None,
    ) -> List[Change]:
        """Get changes from Jira since a timestamp."""
        import anyio
        
        config = config or CDCConfig(
            key_column="key",
            sync_column="updated"
        )
        
        from waveql.query_planner import Predicate
        
        predicates = []
        if since:
            # Jira uses "updated" field with format "2026-01-04 12:00"
            jira_date = since.strftime("%Y-%m-%d %H:%M")
            predicates.append(Predicate(
                column="updated",
                operator=">",
                value=jira_date
            ))
        
        def fetch():
            return self.adapter.fetch(
                table=table,
                predicates=predicates if predicates else None,
                limit=config.batch_size,
                order_by=[("updated", "ASC")],
            )
        
        result = await anyio.to_thread.run_sync(fetch)
        
        changes = []
        for i in range(len(result)):
            row = {name: result.column(name)[i].as_py() for name in result.column_names}
            
            change = Change(
                table=table,
                operation=self._detect_operation(row, "created", "updated"),
                key=row.get(config.key_column),
                data=row if config.include_data else None,
                timestamp=self._parse_timestamp(row.get(config.sync_column)),
                source_adapter="jira",
            )
            changes.append(change)
        
        return changes
    
    async def stream_changes(
        self,
        table: str,
        config: CDCConfig = None,
    ) -> AsyncIterator[Change]:
        """Stream changes by polling Jira."""
        import anyio
        
        config = config or CDCConfig(
            key_column="key",
            sync_column="updated"
        )
        last_sync = config.since or datetime.now()
        
        while True:
            changes = await self.get_changes(table, since=last_sync, config=config)
            
            for change in changes:
                yield change
                if change.timestamp and change.timestamp > last_sync:
                    last_sync = change.timestamp
            
            await anyio.sleep(config.poll_interval)
    
    def _parse_timestamp(self, value: Any) -> datetime:
        """Parse Jira timestamp."""
        if value is None:
            return datetime.now()
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return datetime.now()
        return datetime.now()


# Registry of CDC providers by adapter name
CDC_PROVIDERS = {
    "servicenow": ServiceNowCDCProvider,
    "salesforce": SalesforceCDCProvider,
    "jira": JiraCDCProvider,
}


def get_cdc_provider(adapter_name: str, adapter: "BaseAdapter") -> Optional[BaseCDCProvider]:
    """
    Get the appropriate CDC provider for an adapter.
    
    Args:
        adapter_name: Name of the adapter
        adapter: Adapter instance
        
    Returns:
        CDC provider instance or None if not supported
    """
    provider_class = CDC_PROVIDERS.get(adapter_name.lower())
    if provider_class:
        return provider_class(adapter)
    return None
