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

### Phase 2: Join Reordering with Real-Time Latency Stats

WaveQL uses implicit **JoinOptimizer** for cost-based join reordering:

```python
# The optimizer learns from each query execution
from waveql.join_optimizer import get_join_optimizer
stats = get_join_optimizer().get_all_stats()
# {'servicenow.incident': {'avg_latency_per_row': 0.002, 'avg_row_count': 500, ...}}
```

**Cost Model:**
```
Cost(table) = EstimatedRows × EffectiveLatency × (1 + log(rows)/10)
```

**Features:**
- **Per-table latency tracking**: Each table's response time is tracked individually using EMA.
- **Selectivity estimation**: Predicates reduce estimated row counts (= → 10%, IN → varies by value count).
- **Rate limit awareness**: Tables that recently hit rate limits get penalized in the cost model.
- **Semi-join pushdown detection**: When the first table is much smaller, we use it to filter the second.

If joining `Salesforce` (Network, Slow) with `Local CSV` (Disk, Fast):
-   **Bad Plan**: Fetch Salesforce → For each row, scan CSV.
-   **Good Plan**: Load CSV into DuckDB → Fetch filtered Salesforce → Hash Join in DuckDB.

## 3. Cost-Based Optimization (Implemented)

The `JoinOptimizer` tracks per-table statistics:
- `avg_latency_per_row`: Running average (EMA α=0.3) of fetch latency
- `p95_latency_per_row`: 95th percentile for outlier detection
- `avg_row_count`: Historical average rows returned
- `rate_limit_hits`: Count of 429 responses

These stats are updated **automatically** after each fetch, enabling the optimizer to adapt to:
- Network condition changes
- API rate limiting patterns
- Data volume fluctuations

## 4. Federated Grouping
`SELECT count(*) FROM table`
-   **Optimized**: Send `?summary=true` or `HEAD` request to API.
-   **Fallback**: Fetch all IDs, count in Python. (We try to avoid this).

