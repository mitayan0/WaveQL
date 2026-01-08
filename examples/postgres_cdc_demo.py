#!/usr/bin/env python
"""
PostgreSQL CDC Demo (Simulated WAL-Based Streaming)

This script demonstrates the concept of WaveQL's CDC (Change Data Capture)
capabilities using a simulated in-memory approach that runs without a database.

For real PostgreSQL CDC streaming, you need:
- PostgreSQL 9.4+ with wal_level=logical
- User with REPLICATION privilege
- wal2json extension (recommended) or test_decoding

This demo shows the event flow without requiring PostgreSQL.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class Operation(Enum):
    """CDC operation types."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class Change:
    """Represents a database change event."""
    operation: Operation
    table: str
    key: Dict[str, Any]
    data: Optional[Dict[str, Any]] = None
    old_data: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class MockCDCStream:
    """Simulated CDC stream for demo purposes."""
    
    def __init__(self, table: str):
        self.table = table
        self._changes = []
    
    def add_change(self, operation: Operation, key: dict, data: dict = None, old_data: dict = None):
        """Add a change to the stream."""
        self._changes.append(Change(
            operation=operation,
            table=self.table,
            key=key,
            data=data,
            old_data=old_data,
        ))
    
    async def __aiter__(self):
        """Async iterator over changes."""
        for change in self._changes:
            yield change
            await asyncio.sleep(0.5)  # Simulate delay between events


async def simulate_database_activity(stream: MockCDCStream):
    """Simulate database INSERT, UPDATE, DELETE operations."""
    await asyncio.sleep(1)  # Wait for consumer to start
    
    print("\n[Producer] Inserting record 1...")
    stream.add_change(
        Operation.INSERT,
        key={"id": 1},
        data={"id": 1, "message": "Hello CDC", "updated_at": "2024-01-15 10:00:00"}
    )
    await asyncio.sleep(1)
    
    print("[Producer] Updating record 1...")
    stream.add_change(
        Operation.UPDATE,
        key={"id": 1},
        data={"id": 1, "message": "Updated CDC", "updated_at": "2024-01-15 10:01:00"},
        old_data={"id": 1, "message": "Hello CDC", "updated_at": "2024-01-15 10:00:00"}
    )
    await asyncio.sleep(1)
    
    print("[Producer] Deleting record 1...")
    stream.add_change(
        Operation.DELETE,
        key={"id": 1},
        old_data={"id": 1, "message": "Updated CDC", "updated_at": "2024-01-15 10:01:00"}
    )
    
    print("[Producer] All changes submitted!")


async def consume_changes(stream: MockCDCStream):
    """Stream changes and display them."""
    print("Starting CDC stream on table 'waveql_cdc_demo'...")
    print("Waiting for changes...\n")
    
    await asyncio.sleep(2)  # Wait for producer to populate
    
    count = 0
    async for change in stream:
        print(f"\n[Consumer] Captured change:")
        print(f"  Operation: {change.operation.value}")
        print(f"  Key: {change.key}")
        if change.data:
            print(f"  Data: {change.data}")
        if change.old_data:
            print(f"  Old Data: {change.old_data}")
        print(f"  Timestamp: {change.timestamp}")
        
        count += 1
        if count >= 3:
            print("\nCaptured all expected changes!")
            break


async def main():
    print("=" * 60)
    print("WaveQL - PostgreSQL CDC Demo (Simulated)")
    print("=" * 60)
    print()
    print("This demo simulates CDC streaming without a real database.")
    print("In production, WaveQL connects to PostgreSQL's logical")
    print("replication slot to stream changes in real-time.")
    print()
    
    # Create shared stream
    stream = MockCDCStream("waveql_cdc_demo")
    
    # Run producer and consumer concurrently
    await asyncio.gather(
        simulate_database_activity(stream),
        consume_changes(stream),
    )
    
    print()
    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)
    print()
    print("To use real PostgreSQL CDC:")
    print("  1. Set wal_level=logical in postgresql.conf")
    print("  2. Install wal2json extension")
    print("  3. Use: conn.stream_changes_wal('table_name')")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
