# The Optimizer: Making Queries Fast

The `Optimizer` is responsible for taking a logical query plan (what the user *asked* for) and turning it into a physical execution plan (the fastest way to *get* it).

## 1. The Strategy: "Pushdown First"
The golden rule of WaveQL optimization is **Pushdown**. We want to move as much work as possible *to* the remote API.

### why?
-   **Network**: Transferring 1MB is faster than 1GB.
-   **Memory**: Processing 10 rows is cheaper than 10,000.
-   **Compute**: Usage APIs often have indexed search which is O(log n), whereas client-side filtering is O(n).

## 2. Optimization Phases

### Phase 1: Predicate Analysis
We scan the `WHERE` clause.
-   `status = 'open'`: **Safe**. Pushed to almost all adapters.
-   `description LIKE '%error%'`: **Unsafe** for some APIs (like simple REST endpoints), Safe for SQL databases.
-   `created_at > NOW() - INTERVAL 1 DAY`: **Complex**. Transformed into absolute timestamps (e.g., `created_at > '2023-10-27'`) before pushing.

### Phase 2: Join Reordering (Star Schema)
If joining `Salesforce` (Network, Slow) with `Local CSV` (Disk, Fast).
-   **Bad Plan**: Fetch Salesforce -> For each row, scan CSV.
-   **Good Plan**: Load CSV into DuckDB -> Fetch filtered Salesforce -> Hash Join in DuckDB.

WaveQL currently defaults to **"Fetch All required, then Join locally"** for cross-adapter joins, relying on DuckDB's internal optimizer for the heavy lifting once data is in memory.

## 3. Cost-Based Estimation (Future Work)
We are building a `stats` interface where adapters can report:
-   `approx_row_count()`
-   `is_indexed(column)`

This will allow us to choose between `NESTED LOOP JOIN` (good for small left-side) and `HASH JOIN` (good for bulk).

## 4. Federated Grouping
`SELECT count(*) FROM table`
-   **Optimized**: Send `?summary=true` or `HEAD` request to API.
-   **Fallback**: Fetch all IDs, count in Python. (We try to avoid this).
