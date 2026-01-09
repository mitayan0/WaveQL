# Query Lifecycle Deep Dive

## 1. The Parsing Stage (SQLGlot)
Incoming SQL strings are not parsed by WaveQL directly. We utilize `sqlglot` to generate an Abstract Syntax Tree (AST).

```python
# Raw
SELECT id, name FROM users WHERE age > 25

# AST (Simplified)
Select(
    expressions=[Column(id), Column(name)],
    from=Table(users),
    where=GT(Column(age), Literal(25))
)
```

**Key Constraint**: We enforce a subset of ANSI SQL. Recursive CTEs and Window Functions are currently delegated to DuckDB *after* initial fetch.

## 2. The Context Analysis
The `QueryPlanner` attaches metadata to the AST:
1.  **Resolver**: Maps `users` -> `PostgresAdapter(tables=['users'])`.
2.  **Validator**: Checks if columns exist in the adapter schema.

## 3. Optimization (The "Pushdown")
This is the most critical phase for performance.

### Phase A: Split
We separate the AST into:
-   **Pushable Logic**: Simple predicates (`=`, `>`, `<`), naming.
-   **Client-Side Logic**: Complex aggregations, Joins across adapters.

### Phase B: Translation
The "Pushable Logic" is transpiled into the native query language.
-   **Postgres**: Remains SQL.
-   **Jira**: Becomes JQL (`project = 'WaveQL' AND type = 'Bug'`).
-   **Salesforce**: Becomes SOQL.
-   **REST**: Becomes query params (`?age_gt=25`).

## 4. Execution (Async Streams)
The `Executor` orchestrates the I/O.
1.  **Parallel Fetch**: If multiple adapters are involved (e.g., UNION ALL), requests are fired in parallel `asyncio.gather`.
2.  **Streaming**: Adapters yield `pyarrow.RecordBatch` chunks to minimize memory pressure.
3.  **Materialization**: If a Join is required, we dump chunks into an in-memory `duckdb` instance.
4.  **Final Pass**: DuckDB executes the "Client-Side Logic" (Joins, Aggregates) on the materialized chunks.

## 5. Result
The final `pyarrow.Table` is wrapped in a WaveQL DataFrame interface.
