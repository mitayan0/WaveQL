"""
PostgreSQL CDC Provider - Real-time Change Data Capture via Logical Replication

This provider uses PostgreSQL's Logical Decoding feature to stream changes
directly from the Write-Ahead Log (WAL), providing:
- Zero-latency event streaming (milliseconds after COMMIT)
- No polling overhead (pure async socket-based)
- Guaranteed delivery via replication slots
- Full before/after data for UPDATE operations

Requirements:
- PostgreSQL 9.4+ with wal_level = logical
- User with REPLICATION privilege
- wal2json or test_decoding output plugin

Usage:
    ```python
    async for change in conn.stream_changes("public.users"):
        print(f"{change.operation}: {change.data}")
    ```
"""

from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional

from waveql.cdc.models import Change, ChangeType, CDCConfig

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class PostgresCDCProvider:
    """
    PostgreSQL CDC provider using Logical Replication.
    
    This provider connects to PostgreSQL using the replication protocol
    and streams WAL changes in real-time without polling.
    
    Supported output plugins:
    - wal2json: JSON-formatted output (recommended)
    - test_decoding: Built-in simple text format
    
    Limitations:
    - Requires PostgreSQL 9.4+ with wal_level=logical
    - Requires user with REPLICATION privilege
    - DDL changes are not captured
    - Large transactions may cause memory pressure
    
    Configuration:
    - slot_name: Replication slot name (default: waveql_cdc)
    - output_plugin: wal2json or test_decoding (default: wal2json)
    - create_slot: Auto-create slot if missing (default: True)
    - include_transaction: Include BEGIN/COMMIT events (default: False)
    """
    
    provider_name = "postgres"
    supports_delete_detection = True
    supports_old_data = True  # WAL provides before-image for UPDATE/DELETE
    
    # Retry configuration
    max_retries: int = 5
    retry_delay: float = 1.0
    
    # Default slot configuration
    DEFAULT_SLOT_NAME = "waveql_cdc"
    DEFAULT_OUTPUT_PLUGIN = "wal2json"
    
    def __init__(
        self,
        adapter: "BaseAdapter",
        connection_string: str = None,
        slot_name: str = None,
        output_plugin: str = None,
        create_slot: bool = True,
        include_transaction: bool = False,
    ):
        """
        Initialize the PostgreSQL CDC provider.
        
        Args:
            adapter: The SQL adapter instance (for connection details)
            connection_string: Override connection string for replication
            slot_name: Logical replication slot name
            output_plugin: wal2json or test_decoding
            create_slot: Create slot if it doesn't exist
            include_transaction: Include BEGIN/COMMIT changes
        """
        self.adapter = adapter
        self._connection_string = connection_string
        self._slot_name = slot_name or self.DEFAULT_SLOT_NAME
        self._output_plugin = output_plugin or self.DEFAULT_OUTPUT_PLUGIN
        self._create_slot = create_slot
        self._include_transaction = include_transaction
        self._consecutive_errors = 0
        self._replication_conn = None
        self._cursor = None
        self._running = False
        
    def __repr__(self) -> str:
        return f"<PostgresCDCProvider slot={self._slot_name} plugin={self._output_plugin}>"
    
    def _get_connection_string(self) -> str:
        """Get the connection string for replication."""
        if self._connection_string:
            return self._connection_string
        
        # Try to extract from adapter
        conn_str = getattr(self.adapter, '_connection_string', None)
        if conn_str:
            return conn_str
            
        host = getattr(self.adapter, '_host', None)
        if host:
            return host
        
        raise ValueError("No connection string available for PostgreSQL CDC")
    
    async def _ensure_slot_exists(self) -> None:
        """Create the replication slot if it doesn't exist."""
        import psycopg2
        
        conn_str = self._get_connection_string()
        
        # Use a regular connection to check/create slot
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        
        try:
            with conn.cursor() as cur:
                # Check if slot exists
                cur.execute(
                    "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
                    (self._slot_name,)
                )
                
                if cur.fetchone() is None:
                    if self._create_slot:
                        logger.info(
                            "Creating replication slot: %s with plugin: %s",
                            self._slot_name, self._output_plugin
                        )
                        cur.execute(
                            "SELECT pg_create_logical_replication_slot(%s, %s)",
                            (self._slot_name, self._output_plugin)
                        )
                        logger.info("Replication slot created successfully")
                    else:
                        raise ValueError(
                            f"Replication slot '{self._slot_name}' does not exist. "
                            "Set create_slot=True to auto-create."
                        )
                else:
                    logger.debug("Replication slot '%s' already exists", self._slot_name)
        finally:
            conn.close()
    
    async def _connect_replication(self):
        """Establish a replication connection."""
        import psycopg2
        from psycopg2.extras import LogicalReplicationConnection
        
        conn_str = self._get_connection_string()
        
        # Ensure slot exists before connecting
        await self._ensure_slot_exists()
        
        # Create replication connection
        self._replication_conn = psycopg2.connect(
            conn_str,
            connection_factory=LogicalReplicationConnection
        )
        self._cursor = self._replication_conn.cursor()
        
        logger.info(
            "Connected to PostgreSQL for logical replication, slot: %s",
            self._slot_name
        )
        
        return self._cursor
    
    async def get_changes(
        self,
        table: str,
        since: datetime = None,
        config: CDCConfig = None,
    ) -> List[Change]:
        """
        Get accumulated changes from the replication slot.
        
        This is a one-shot method that peeks at changes without consuming them.
        For continuous streaming, use stream_changes().
        
        Args:
            table: Table to filter changes for (schema.table format)
            since: Not used for WAL (slot tracks position)
            config: CDC configuration
            
        Returns:
            List of Change objects
        """
        import psycopg2
        
        config = config or CDCConfig()
        conn_str = self._get_connection_string()
        
        # Use pg_logical_slot_peek_changes to view without consuming
        conn = psycopg2.connect(conn_str)
        changes = []
        
        try:
            with conn.cursor() as cur:
                # Peek at changes (does not advance slot position)
                cur.execute(
                    """
                    SELECT lsn, xid, data 
                    FROM pg_logical_slot_peek_changes(%s, NULL, %s)
                    """,
                    (self._slot_name, config.batch_size)
                )
                
                for lsn, xid, data in cur.fetchall():
                    parsed_changes = self._parse_wal_message(data, table)
                    changes.extend(parsed_changes)
                    
                    if len(changes) >= config.batch_size:
                        break
                        
        finally:
            conn.close()
        
        return changes
    
    async def stream_changes(
        self,
        table: str,
        config: CDCConfig = None,
    ) -> AsyncIterator[Change]:
        """
        Stream changes in real-time from the PostgreSQL WAL.
        
        This method establishes a replication connection and yields
        changes as they occur. Unlike polling-based CDC, this has
        near-zero latency.
        
        Args:
            table: Table to watch (schema.table format, or just table name)
            config: CDC configuration
            
        Yields:
            Change objects as they are detected
            
        Example:
            ```python
            provider = PostgresCDCProvider(adapter)
            async for change in provider.stream_changes("users"):
                print(f"{change.operation}: {change.key}")
            ```
        """
        config = config or CDCConfig()
        self._running = True
        
        # Parse table name for filtering
        schema, table_name = self._parse_table_name(table)
        
        try:
            cursor = await self._connect_replication()
            
            # Build plugin options based on output plugin
            options = self._build_plugin_options(schema, table_name)
            
            # Start replication
            cursor.start_replication(
                slot_name=self._slot_name,
                decode=True,
                options=options
            )
            
            logger.info(
                "Started streaming changes for %s.%s",
                schema or "public", table_name
            )
            
            # Enter the streaming loop
            while self._running:
                try:
                    # Use consume_stream with a message handler
                    # We wrap this in asyncio to make it non-blocking
                    msg = await asyncio.get_event_loop().run_in_executor(
                        None, self._read_message_sync, cursor, config.poll_interval
                    )
                    
                    if msg is None:
                        # Timeout, send keepalive and continue
                        continue
                    
                    # Reset error counter on successful read
                    self._consecutive_errors = 0
                    
                    # Parse the message
                    changes = self._parse_wal_message(msg.payload, table)
                    
                    for change in changes:
                        yield change
                    
                    # Send feedback to advance the replication position
                    msg.cursor.send_feedback(flush_lsn=msg.data_start)
                    
                except Exception as e:
                    self._consecutive_errors += 1
                    
                    if self._consecutive_errors > self.max_retries:
                        logger.error(
                            "PostgreSQL CDC stream failed after %d retries: %s",
                            self.max_retries, e
                        )
                        raise
                    
                    # Exponential backoff
                    delay = self.retry_delay * (2 ** (self._consecutive_errors - 1))
                    logger.warning(
                        "PostgreSQL CDC error (attempt %d/%d), retrying in %.1fs: %s",
                        self._consecutive_errors, self.max_retries, delay, e
                    )
                    await asyncio.sleep(delay)
                    
                    # Reconnect
                    await self._connect_replication()
                    
        finally:
            self._running = False
            await self._close_connection()
    
    def _read_message_sync(self, cursor, timeout: float):
        """
        Read a single message from the replication stream (sync wrapper).
        
        This is called via run_in_executor to make it async-compatible.
        """
        import select
        
        # Use select for timeout support
        if select.select([cursor.connection], [], [], timeout)[0]:
            cursor.connection.poll()
            return cursor.read_message()
        
        return None
    
    def _parse_table_name(self, table: str) -> tuple:
        """Parse schema.table into (schema, table)."""
        if "." in table:
            parts = table.split(".", 1)
            return parts[0].strip('"'), parts[1].strip('"')
        return None, table.strip('"')
    
    def _build_plugin_options(self, schema: str, table_name: str) -> dict:
        """Build options for the output plugin."""
        options = {}
        
        if self._output_plugin == "wal2json":
            # wal2json specific options
            options = {
                'include-xids': '1',
                'include-timestamp': '1',
                'include-schemas': '1',
                'include-types': '1',
                'include-not-null': '1',
                'write-in-chunks': '0',
                'format-version': '2',
            }
            
            # Add table filter if specified
            if table_name:
                if schema:
                    options['add-tables'] = f'{schema}.{table_name}'
                else:
                    options['add-tables'] = f'*.{table_name}'
                    
        elif self._output_plugin == "test_decoding":
            # test_decoding has minimal options
            pass
        
        return options
    
    def _parse_wal_message(self, payload: str, table_filter: str = None) -> List[Change]:
        """
        Parse a WAL message into Change objects.
        
        Supports both wal2json and test_decoding formats.
        """
        if self._output_plugin == "wal2json":
            return self._parse_wal2json(payload, table_filter)
        else:
            return self._parse_test_decoding(payload, table_filter)
    
    def _parse_wal2json(self, payload: str, table_filter: str = None) -> List[Change]:
        """Parse wal2json format v2."""
        changes = []
        
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("Failed to parse wal2json payload: %s", payload[:100])
            return []
        
        # Handle v2 format (action-based)
        if 'action' in data:
            # Single change message
            messages = [data]
        elif 'change' in data:
            # Batch of changes
            messages = data.get('change', [])
        else:
            messages = []
        
        # Parse table filter
        filter_schema, filter_table = self._parse_table_name(table_filter) if table_filter else (None, None)
        
        for msg in messages:
            action = msg.get('action', '').upper()
            
            # Skip transaction messages unless requested
            if action in ('B', 'C') and not self._include_transaction:
                continue
            
            # Map action to ChangeType
            if action == 'I':
                operation = ChangeType.INSERT
            elif action == 'U':
                operation = ChangeType.UPDATE
            elif action == 'D':
                operation = ChangeType.DELETE
            elif action == 'B':
                # BEGIN transaction
                continue
            elif action == 'C':
                # COMMIT transaction
                continue
            else:
                operation = ChangeType.UNKNOWN
            
            # Extract table info
            schema = msg.get('schema', 'public')
            table = msg.get('table', '')
            
            # Apply table filter
            if filter_table:
                if filter_schema and schema != filter_schema:
                    continue
                if table != filter_table:
                    continue
            
            # Extract primary key
            pk_columns = msg.get('pk', [])
            pk_values = {}
            for pk in pk_columns:
                pk_name = pk.get('name')
                pk_value = pk.get('value')
                if pk_name:
                    pk_values[pk_name] = pk_value
            
            # Build key (use first PK value or composite)
            if len(pk_values) == 1:
                key = list(pk_values.values())[0]
            elif pk_values:
                key = pk_values
            else:
                key = None
            
            # Extract columns data
            columns = msg.get('columns', [])
            data_dict = {}
            for col in columns:
                col_name = col.get('name')
                col_value = col.get('value')
                if col_name:
                    data_dict[col_name] = col_value
            
            # Extract old data for UPDATE/DELETE
            old_columns = msg.get('identity', [])  # v2 uses 'identity' for old values
            old_data = {}
            for col in old_columns:
                col_name = col.get('name')
                col_value = col.get('value')
                if col_name:
                    old_data[col_name] = col_value
            
            # Get timestamp
            timestamp = datetime.now()
            if 'timestamp' in msg:
                try:
                    timestamp = datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    pass
            
            change = Change(
                table=f"{schema}.{table}",
                operation=operation,
                key=key,
                data=data_dict if data_dict else None,
                old_data=old_data if old_data else None,
                timestamp=timestamp,
                source_adapter="postgres",
                metadata={
                    "xid": msg.get('xid'),
                    "lsn": msg.get('lsn'),
                }
            )
            changes.append(change)
        
        return changes
    
    def _parse_test_decoding(self, payload: str, table_filter: str = None) -> List[Change]:
        """
        Parse test_decoding format.
        
        Format examples:
        - table public.users: INSERT: id[integer]:1 name[text]:'John'
        - table public.users: UPDATE: id[integer]:1 name[text]:'Jane'
        - table public.users: DELETE: id[integer]:1
        """
        changes = []
        
        # Skip BEGIN/COMMIT messages
        if payload.startswith('BEGIN') or payload.startswith('COMMIT'):
            return []
        
        # Parse the message
        # Format: "table {schema}.{table}: {ACTION}: {columns}"
        if not payload.startswith('table '):
            return []
        
        try:
            # Split into parts
            parts = payload.split(': ', 2)
            if len(parts) < 2:
                return []
            
            # Parse table name
            table_part = parts[0].replace('table ', '')
            action = parts[1].upper()
            
            # Map action
            if action == 'INSERT':
                operation = ChangeType.INSERT
            elif action == 'UPDATE':
                operation = ChangeType.UPDATE
            elif action == 'DELETE':
                operation = ChangeType.DELETE
            else:
                return []
            
            # Parse table filter
            filter_schema, filter_table = self._parse_table_name(table_filter) if table_filter else (None, None)
            msg_schema, msg_table = self._parse_table_name(table_part)
            
            # Apply filter
            if filter_table:
                if filter_schema and msg_schema != filter_schema:
                    return []
                if msg_table != filter_table:
                    return []
            
            # Parse columns (simplified - would need more robust parsing for production)
            data_dict = {}
            if len(parts) > 2:
                col_str = parts[2]
                # Parse: col[type]:value pairs
                # This is simplified - actual test_decoding parsing is more complex
                for item in col_str.split(' '):
                    if ':' in item and '[' in item:
                        name_type, value = item.rsplit(':', 1)
                        name = name_type.split('[')[0]
                        # Strip quotes from strings
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        data_dict[name] = value
            
            # Use first column as key (simplified)
            key = list(data_dict.values())[0] if data_dict else None
            
            change = Change(
                table=table_part,
                operation=operation,
                key=key,
                data=data_dict if data_dict else None,
                old_data=None,  # test_decoding doesn't provide old values easily
                timestamp=datetime.now(),
                source_adapter="postgres",
            )
            changes.append(change)
            
        except Exception as e:
            logger.warning("Failed to parse test_decoding message: %s - %s", payload[:100], e)
        
        return changes
    
    async def _close_connection(self):
        """Close the replication connection."""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._replication_conn:
                self._replication_conn.close()
                self._replication_conn = None
            logger.debug("Closed PostgreSQL replication connection")
        except Exception as e:
            logger.warning("Error closing replication connection: %s", e)
    
    async def stop(self):
        """Stop the streaming loop."""
        self._running = False
        await self._close_connection()
    
    async def drop_slot(self, force: bool = False) -> bool:
        """
        Drop the replication slot.
        
        Call this when you no longer need CDC for this table.
        WARNING: This will lose any unconsumed changes!
        
        Args:
            force: If True, terminate any active connections using the slot first
        
        Returns:
            True if slot was dropped, False if it didn't exist
        """
        import psycopg2
        
        # First close our own connection
        await self._close_connection()
        
        conn_str = self._get_connection_string()
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        
        try:
            with conn.cursor() as cur:
                if force:
                    # Terminate any backend using this slot
                    cur.execute(
                        """
                        SELECT pg_terminate_backend(active_pid) 
                        FROM pg_replication_slots 
                        WHERE slot_name = %s AND active_pid IS NOT NULL
                        """,
                        (self._slot_name,)
                    )
                    # Brief pause to let the backend terminate
                    import time
                    time.sleep(0.1)
                
                cur.execute(
                    "SELECT pg_drop_replication_slot(%s)",
                    (self._slot_name,)
                )
                logger.info("Dropped replication slot: %s", self._slot_name)
                return True
        except psycopg2.Error as e:
            if "does not exist" in str(e):
                return False
            raise
        finally:
            conn.close()
    
    async def get_slot_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the replication slot.
        
        Returns:
            Dict with slot information or None if slot doesn't exist
        """
        import psycopg2
        
        conn_str = self._get_connection_string()
        conn = psycopg2.connect(conn_str)
        
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        slot_name,
                        plugin,
                        slot_type,
                        active,
                        restart_lsn,
                        confirmed_flush_lsn,
                        pg_current_wal_lsn() as current_lsn,
                        pg_size_pretty(
                            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                        ) as lag
                    FROM pg_replication_slots 
                    WHERE slot_name = %s
                    """,
                    (self._slot_name,)
                )
                
                row = cur.fetchone()
                if row:
                    return {
                        "slot_name": row[0],
                        "plugin": row[1],
                        "slot_type": row[2],
                        "active": row[3],
                        "restart_lsn": str(row[4]) if row[4] else None,
                        "confirmed_flush_lsn": str(row[5]) if row[5] else None,
                        "current_lsn": str(row[6]) if row[6] else None,
                        "lag": row[7],
                    }
                return None
        finally:
            conn.close()
