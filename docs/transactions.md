# Atomic Writes (Saga)

Best-effort distributed transactions using the Saga Pattern (Compensating Transactions).

> ⚠️ **Not ACID**: No 2PC. Failure triggers compensation (rollback) logic.

## Usage

```python
with conn.transaction() as txn:
    # 1. Execute
    txn.insert("servicenow.incident", {"short_description": "Server Down"})
    
    # 2. Execute
    txn.insert("salesforce.Case", {"Subject": "Server Down"})
    
    # If #2 fails, #1 is deleted (compensated).
```

## Retry Policy
Compensations auto-retry with exponential backoff before hitting DLQ.

```python
from waveql.transaction import CompensationRetryPolicy

# Default: 3 retries, 1s base delay
policy = CompensationRetryPolicy(max_retries=5)
```

## Dead Letter Queue (DLQ)
Permanently failed compensations are stored in `transactions.db`.

```python
# Monitor
entries = conn.coordinator.get_dlq_entries()

# Retry
conn.coordinator.retry_dlq_entry(entry_id)

# Resolve
conn.coordinator.resolve_dlq_entry(entry_id, notes="Fixed manually")
```

## Persistence
State is logged to SQLite (`WAVEQL_TRANSACTION_DB`). Crash recovery happens on `connect()`.
```python
conn.recover_pending_transactions()
```
