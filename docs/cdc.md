# Change Data Capture (CDC)

## Capabilities

| Engine | Mechanism | Latency | Old Data |
| :--- | :--- | :--- | :--- |
| **PaaS** (ServiceNow/SF) | Polling (`sys_updated_on`) | Seconds | ❌ |
| **Postgres** | WAL (Logical Replication) | ms | ✅ |

## Configuration

```python
from waveql.cdc import CDCConfig

config = CDCConfig(
    poll_interval=10.0,
    batch_size=500,
    since=datetime.now() - timedelta(hours=1),
    include_data=True
)

async for change in conn.stream_changes("incident", config=config):
    print(change.operation, change.key, change.data)
```

## Postgres Specifics

Requires `wal_level=logical`.

```python
from waveql.cdc.postgres import PostgresCDCProvider

provider = PostgresCDCProvider(
    adapter=adapter,
    slot_name="my_slot",
    output_plugin="wal2json"  # or test_decoding
)

# old_data available if REPLICA IDENTITY FULL
async for change in provider.stream_changes("users"):
    print(change.old_data)
```

## State & Persistence
Streams auto-resume using a local cursor DB (`cdc_state.db`).
*   **Polling**: Tracks `last_updated_timestamp`.
*   **WAL**: Tracks `LSN`.
