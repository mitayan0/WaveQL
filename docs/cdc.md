# Change Data Capture (CDC)

WaveQL provides a robust Change Data Capture (CDC) system that allows you to stream data changes (Inserts, Updates, Deletes) from your supported data sources in real-time.

## Concepts

The CDC system is built on an async stream processing model. 
*   **Provider**: Each adapter has a CDC Provider that knows how to poll or listen for changes.
*   **Stream**: A continuous flow of `Change` objects.
*   **State**: The stream maintains state (last timestamp/key) to ensure no data is lost during restarts (persistence logic to be implemented by user).

## CDC Methods

WaveQL supports two CDC methods:

| Method | Latency | Overhead | Use Case |
|:-------|:--------|:---------|:---------|
| **Polling** | Seconds to minutes | Queries the database repeatedly | SaaS APIs (ServiceNow, Jira, Salesforce) |
| **WAL Streaming** | Milliseconds | Zero (push-based) | PostgreSQL databases |

## Basic Usage (Polling-Based)

The easiest way to use CDC is via the `stream_changes` method on an async connection.

```python
async for change in conn.stream_changes("table_name"):
    print(change)
```

## PostgreSQL WAL-Based CDC (NEW!)

For PostgreSQL databases, WaveQL supports true Change Data Capture using Logical Replication. This provides:

- **Millisecond latency**: Changes arrive within milliseconds of `COMMIT`
- **Zero polling overhead**: Push-based streaming, no database queries
- **Guaranteed delivery**: Replication slots ensure no events are lost
- **Before/after data**: Full access to old values on UPDATE/DELETE

### Prerequisites

1. **PostgreSQL 9.4+** with `wal_level = logical`
2. **User with REPLICATION privilege**
3. **Output plugin**: `wal2json` (recommended) or `test_decoding` (built-in)

```sql
-- Check your configuration
SHOW wal_level;  -- Should be 'logical'

-- Grant replication privilege
ALTER USER your_user WITH REPLICATION;
```

### Usage

```python
import waveql
import asyncio

async def main():
    # Connect to PostgreSQL
    conn_str = "postgresql://user:pass@localhost:5432/mydb"
    
    # Option 1: Direct provider usage (recommended)
    from waveql.cdc.postgres import PostgresCDCProvider
    from waveql.adapters.sql import SQLAdapter
    
    adapter = SQLAdapter(host=conn_str)
    provider = PostgresCDCProvider(
        adapter=adapter,
        connection_string=conn_str,
        slot_name="my_cdc_slot",
        output_plugin="test_decoding",  # or "wal2json"
    )
    
    # Stream changes in real-time
    async for change in provider.stream_changes("users"):
        print(f"{change.operation}: {change.key}")
        print(f"  Data: {change.data}")
        if change.old_data:
            print(f"  Old:  {change.old_data}")

asyncio.run(main())
```

### One-Shot Change Retrieval

```python
# Peek at accumulated changes without consuming them
changes = await provider.get_changes("users")
for change in changes:
    print(f"{change.operation}: {change.data}")
```

### Slot Management

```python
# Get slot info
info = await provider.get_slot_info()
print(f"Lag: {info['lag']}")

# Drop slot when done (WARNING: loses unconsumed changes!)
# Use force=True to kill active connections holding the slot
await provider.drop_slot(force=True)
```

### Enabling Old Data (Before-Image)

To capture the old values on UPDATE and DELETE, set `REPLICA IDENTITY FULL`:

```sql
ALTER TABLE your_table REPLICA IDENTITY FULL;
```

## Configuration

You can customize the polling interval, batch size, and initial sync point using `CDCConfig`.

```python
from waveql.cdc import CDCConfig
from datetime import datetime, timedelta

config = CDCConfig(
    poll_interval=10.0,       # Poll every 10 seconds
    batch_size=500,           # Fetch up to 500 changes per poll
    include_data=True,        # Include full record data in the event
    since=datetime.now() - timedelta(hours=1) # Start from 1 hour ago
)

async for change in conn.stream_changes("incident", config=config):
    # Process change
    pass
```

## The `Change` Object

Each event yielded by the stream is a `Change` object with the following attributes:

*   `operation`: `insert`, `update`, or `delete`
*   `table`: The table name
*   `key`: The primary key (e.g., sys_id)
*   `data`: The new data (for inserts/updates)
*   `old_data`: The previous data (if available)
*   `timestamp`: When the change happened
*   `source_adapter`: The adapter name

## Helper Functions

WaveQL includes helpers for common patterns.

### `watch_changes`
Invokes a callback for every change.

```python
from waveql.cdc import watch_changes

def on_change(change):
    print(f"Detected {change.operation}")

await watch_changes(conn, "incident", callback=on_change)
```

### `collect_changes`
Collects changes for a fixed duration.

```python
from waveql.cdc import collect_changes

# Gather all changes that happen in the next minute
changes = await collect_changes(conn, "incident", duration_seconds=60)
```

## Adapter Support

| Adapter | Mechanism | Latency | Delete Detection | Old Data |
|:--------|:----------|:--------|:-----------------|:---------|
| **PostgreSQL** | WAL Logical Replication | Milliseconds | ✅ Yes | ✅ Yes (with REPLICA IDENTITY FULL) |
| **ServiceNow** | Polling (`sys_updated_on`) | Seconds | ⚠️ Limited | ❌ No |
| **Jira** | Polling (`updated`) | Seconds | ⚠️ Limited | ❌ No |
| **Salesforce** | Polling (`SystemModstamp`) | Seconds | ⚠️ Limited | ❌ No |

## PostgreSQL CDC Best Practices

1. **Use `wal2json`** if available - it provides JSON output that's easier to parse
2. **Set `REPLICA IDENTITY FULL`** on tables where you need old values
3. **Monitor slot lag** - unconsumed changes accumulate WAL files
4. **Clean up slots** when no longer needed to avoid disk space issues
5. **Use unique slot names** per consumer to avoid conflicts

