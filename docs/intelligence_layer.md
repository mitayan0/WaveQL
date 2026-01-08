# Intelligence Layer (v0.1.7)

WaveQL v0.1.7 introduces the "Intelligence Layer," a suite of features designed to make APIs more accessible to LLMs (Large Language Models) and clearer for human developers. This release bridges the gap between raw data sources and semantic understanding.

This guide covers the core components of the Intelligence Layer:

1.  **Semantic Metadata & LLM Context**: Auto-generating prompt-ready schema descriptions.
2.  **Relationship Discovery**: Automatically finding foreign key links across disparated adapters.
3.  **Cost-Based Optimizer (CBO)**: Intelligent join reordering based on system latency.
4.  **Hybrid Querying**: Merging historical materialized views with live data.

---

## 1. Semantic Metadata & LLM Context

One of the biggest challenges in Text-to-SQL is giving the LLM the right context. Raw `CREATE TABLE` statements are often insufficient for complex SaaS APIs. WaveQL solves this by generating rich, semantic contexts from your `DataContracts`.

### Usage

The `to_llm_context()` method on `DataContract`, `ColumnContract`, and `RelationshipContract` generates concise, informative descriptions optimized for LLM prompts.

```python
from waveql.contracts.models import DataContract, ColumnContract

contract = DataContract(
    table="incidents",
    adapter="servicenow",
    description="ITSM Incident records",
    columns=[
        ColumnContract(
            name="sys_id", 
            type="string", 
            primary_key=True, 
            description="Unique global identifier (GUID)"
        ),
        ColumnContract(
            name="short_description", 
            type="string", 
            description="One-line summary of the issue"
        )
    ]
)

# Generate prompt context
print(contract.to_llm_context())
```

**Output:**
```
### Table: incidents
Source: servicenow
Description: ITSM Incident records

Columns:
  - sys_id (string, required) [Primary Key]: Unique global identifier (GUID)
  - short_description (string, nullable): One-line summary of the issue
```

This output can be directly injected into your system prompt for agents like Cursor, ChatGPT, or custom internal tools.

---

## 2. Relationship Discovery

WaveQL can now automatically discover relationships between tables across different adapters. This allows it to suggest joins even if the underlying APIs don't explicitly define foreign keys.

### How it Works

The engine uses a combination of heuristics:
*   **Exact Name Matching**: e.g., `email` field in `jira.users` matches `email` in `servicenow.sys_user`.
*   **Type Compatibility**: Ensuring columns share the same data type.
*   **Semantic Identifiers**: Prioritizing common join keys like `id`, `uuid`, `user_id`, `email`.
*   *(Future)* LLM-based semantic matching.

### Usage

```python
conn = waveql.connect()

# Discover potential relationships across all registered adapters
relationships = conn.discover_relationships()

for rel in relationships:
    print(f"Found link: {rel.name}")
    # Output: Found link: Auto:jira.users.email->servicenow.sys_user.email
```

These discovered relationships are returned as `RelationshipContract` objects, which can be fed back into the semantic layer or used to augment your Data Contracts.

---

## 3. Cost-Based Optimizer (CBO)

Federated queries often involve joining data from sources with vastly different performance characteristics (e.g., a fast local Parquet file vs. a slow Salesforce API). WaveQL's new Cost-Based Optimizer (CBO) intelligently reorders joins to minimize execution time.

### The Problem
If you join a **fast, huge table** (1M rows) with a **slow, small table** (100 rows), the order matters significantly for "semi-join pushdown". Using the small table as the driver to filter the large one is usually better, but if the small table dominates latency (taking 10s to return 100 rows vs 0.1s for 1M rows), the optimal plan changes.

### The Solution
The CBO uses a cost formula:
$$ \text{Cost} = \text{EstimatedRows} \times \text{AdapterLatency} \times \text{Selectivity} $$

*   **AdapterLatency**: Automatically tracked per-adapter historical average latency (time per row).
*   **EstimatedRows**: Based on historical execution counts.
*   **Selectivity**: Estimates how many rows will pass the `WHERE` clause (e.g., `=` is very selective, `>` is less so).

WaveQL reorders the tables in your virtual join execution plan to fetch the "cheapest" (fastest/most selective) tables first, using their results to push filters down to subsequent tables.

### Monitoring
You can view the active latency metrics via the adapter instance:

```python
adapter = conn.get_adapter("servicenow")
print(f"Avg Latency: {adapter.avg_latency_per_row:.4f}s per row")
```

---

## 4. Hybrid Querying

Hybrid querying allows you to combine the speed of **Materialized Views (Historical Data)** with the freshness of **Live API calls**. This enables sub-second query performance while guaranteeing 100% up-to-date results.

### The Strategy
A Hybrid query executes in two parts:
1.  **Historical**: Fetches the bulk of data from a local Materialized View (Parquet/DuckDB).
2.  **Live**: Fetches *only* records changed since the view's last sync timestamp from the live API.

The engine then merges these results in memory, deduplicating based on Primary Keys, so the user sees a seamless, up-to-date dataset.

### Usage
Use the `/*+ HYBRID */` SQL hint to trigger this mode:

```sql
/*+ HYBRID */ 
SELECT * FROM incident 
WHERE priority = 1
```

**Requirements:**
1.  A Materialized View must exist for the target table.
2.  The view must have a tracked sync column (e.g., `sys_updated_on`).

The engine handles the complexity of fetching the delta, creating a temporary union view, and returning the final result set.
