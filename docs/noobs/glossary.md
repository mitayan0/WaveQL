# WaveQL Glossary for Beginners

### Adapter
A connector that allows WaveQL to talk to a specific data source (like Google Sheets, Zendesk, or a Postgres database). It translates your SQL commands into the specific language or API calls that the source understands.

### Aggregation
Combining multiple rows of data into a single summary value. Common aggregations include `COUNT` (how many?), `SUM` (total?), `AVG` (average?), `MIN` (smallest), and `MAX` (largest).

### Cache
A temporary storage area. WaveQL uses caching to remember the results of expensive queries so that if you ask the same question again, it can answer instantly without doing all the hard work twice.

### DataFrame
A table of data in memory, similar to a spreadsheet with rows and columns. WaveQL typically returns your query results as a Pandas or Polars DataFrame, which are popular tools in Python for specific data analysis.

### JOIN
A SQL command used to combine rows from two or more tables, based on a related column between them.
-   **Inner Join**: Keep only rows that match in *both* tables.
-   **Left Join**: Keep all rows from the first table, and match what you can from the second.

### Predicate Pushdown (or just "Pushdown")
A performance feature. Instead of downloading *all* the data and then filtering it on your computer, WaveQL "pushes" the filter down to the source.
*   **Bad:** Download 1 million tweets -> Filter for "funny".
*   **Good (Pushdown):** Ask Twitter API: "Send me only the funny tweets".

### Query
A question or command you send to a database. In WaveQL, queries are written in SQL.

### Schema
The "blueprint" of a table. It defines what columns exist (Name, Date, Price) and what type of data they hold (Text, Date/Time, Number).

### Semantic Layer
A layer of "business logic" that sits on top of your raw data. It lets you define friendly names and calculations (like `total_revenue`) so you don't have to rewrite the math in every single query.

### SQL (Structured Query Language)
The standard language for managing and querying databases. WaveQL lets you use this single language to control many different types of data sources.

### URI (Uniform Resource Identifier)
A string of characters used to identify a resource. In WaveQL, we use connection strings like `postgres://user:pass@localhost:5432/db` or `gsheets://` to tell the system how to connect to a specific adapter.
