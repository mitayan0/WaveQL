# Change Data Capture (CDC)

WaveQL provides a robust Change Data Capture (CDC) system that allows you to stream data changes (Inserts, Updates, Deletes) from your supported data sources in real-time.

## Concepts

The CDC system is built on an async stream processing model. 
*   **Provider**: Each adapter (like ServiceNow) has a CDC Provider that knows how to poll or listen for changes.
*   **Stream**: A continuous flow of `Change` objects.
*   **State**: The stream maintains state (last timestamp/key) to ensure no data is lost during restarts (persistence logic to be implemented by user).

## Basic Usage

The easiest way to use CDC is via the `stream_changes` method on an async connection.

```python
async for change in conn.stream_changes("table_name"):
    print(change)
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

| Adapter | Mechanism | Notes |
|:--------|:----------|:------|
| **ServiceNow** | Polling (`sys_updated_on`) | Requires `sys_updated_on` field. Deletes may not be detected without Audit Log table access. |
| **Jira** | Polling (`updated`) | Uses JQL `updated > last_sync`. |
| **Salesforce** | Polling / Streaming API | Currently uses SOQL polling on `SystemModstamp`. |

