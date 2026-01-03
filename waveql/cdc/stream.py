"""
CDC Stream - Main streaming interface for Change Data Capture
"""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Dict, List, Optional

from waveql.cdc.models import Change, ChangeStream, ChangeType, CDCConfig
from waveql.cdc.providers import get_cdc_provider, BaseCDCProvider

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection
    from waveql.async_connection import AsyncWaveQLConnection

logger = logging.getLogger(__name__)


class CDCStream:
    """
    High-level CDC streaming interface.
    
    Provides an easy way to stream changes from data sources:
    
    ```python
    async for change in conn.stream_changes("incident"):
        print(f"{change.operation}: {change.key}")
    ```
    """
    
    def __init__(
        self,
        connection: "WaveQLConnection",
        table: str,
        config: CDCConfig = None,
    ):
        """
        Initialize a CDC stream.
        
        Args:
            connection: WaveQL connection
            table: Table to watch for changes
            config: CDC configuration
        """
        self.connection = connection
        self.table = table
        self.config = config or CDCConfig()
        self._running = False
        self._stream_state = ChangeStream(table=table, adapter="")
        
        # Parse table to get adapter
        self._adapter_name, self._table_name = self._parse_table(table)
        
        # Get the appropriate adapter (may be None if not registered)
        adapter = self.connection.get_adapter(self._adapter_name)
        if not adapter:
            # Try default adapter, but keep the parsed adapter name for provider lookup
            adapter = self.connection.get_adapter("default")
        
        self._adapter = adapter
        # Use parsed adapter name for provider lookup (even if adapter is from default)
        self._provider = get_cdc_provider(self._adapter_name, adapter) if adapter else None
        self._stream_state.adapter = self._adapter_name
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "running" if self._running else "stopped"
        return f"CDCStream({self.table} [{status}])"
    
    def _parse_table(self, table: str) -> tuple:
        """Parse schema.table into (adapter, table)."""
        if "." in table:
            parts = table.split(".", 1)
            return parts[0].strip('"'), parts[1].strip('"')
        return "default", table.strip('"')
    
    async def get_changes(self, since: datetime = None) -> List[Change]:
        """
        Get all changes since a timestamp.
        
        Args:
            since: Only get changes after this time. If None, uses config.since
            
        Returns:
            List of Change objects
        """
        if not self._provider:
            logger.warning("No CDC provider for adapter '%s'", self._adapter_name)
            return []
        
        since = since or self.config.since
        changes = await self._provider.get_changes(
            table=self._table_name,
            since=since,
            config=self.config,
        )
        
        # Update stream state
        for change in changes:
            self._stream_state.update(change)
        
        return changes
    
    async def stream(self) -> AsyncIterator[Change]:
        """
        Stream changes as they occur.
        
        Yields:
            Change objects as they are detected
            
        Example:
            ```python
            stream = CDCStream(conn, "incident")
            async for change in stream.stream():
                print(change)
            ```
        """
        if not self._provider:
            logger.warning("No CDC provider for adapter '%s'", self._adapter_name)
            return
        
        self._running = True
        
        try:
            async for change in self._provider.stream_changes(
                table=self._table_name,
                config=self.config,
            ):
                if not self._running:
                    break
                
                self._stream_state.update(change)
                yield change
                
        except asyncio.CancelledError:
            logger.info("CDC stream cancelled for %s", self.table)
            raise
        except Exception as e:
            logger.error("Error in CDC stream for %s: %s", self.table, e)
            self._stream_state.errors.append(str(e))
            raise
    
    def stop(self) -> None:
        """Stop the stream."""
        self._running = False
        logger.info("Stopping CDC stream for %s", self.table)
    
    @property
    def state(self) -> ChangeStream:
        """Get current stream state."""
        return self._stream_state
    
    @property
    def is_running(self) -> bool:
        """Check if stream is running."""
        return self._running
    
    def __aiter__(self):
        """Allow using 'async for' directly."""
        return self.stream()


async def watch_changes(
    connection: "WaveQLConnection",
    table: str,
    callback: Callable[[Change], None],
    config: CDCConfig = None,
    stop_after: int = None,
) -> None:
    """
    Watch for changes and call a callback for each.
    
    Args:
        connection: WaveQL connection
        table: Table to watch
        callback: Function to call for each change
        config: CDC configuration
        stop_after: Stop after this many changes (None = run forever)
        
    Example:
        ```python
        def handle_change(change):
            print(f"Got change: {change}")
        
        await watch_changes(conn, "incident", handle_change, stop_after=100)
        ```
    """
    stream = CDCStream(connection, table, config)
    count = 0
    
    async for change in stream.stream():
        callback(change)
        count += 1
        
        if stop_after and count >= stop_after:
            stream.stop()
            break


async def collect_changes(
    connection: "WaveQLConnection",
    table: str,
    duration_seconds: float = 60.0,
    config: CDCConfig = None,
) -> List[Change]:
    """
    Collect changes for a specified duration.
    
    Args:
        connection: WaveQL connection
        table: Table to watch
        duration_seconds: How long to collect changes
        config: CDC configuration
        
    Returns:
        List of all changes collected
        
    Example:
        ```python
        changes = await collect_changes(conn, "incident", duration_seconds=30)
        print(f"Got {len(changes)} changes in 30 seconds")
        ```
    """
    import anyio
    
    changes = []
    stream = CDCStream(connection, table, config)
    
    async def collector():
        async for change in stream.stream():
            changes.append(change)
    
    with anyio.move_on_after(duration_seconds):
        await collector()
    
    stream.stop()
    return changes
