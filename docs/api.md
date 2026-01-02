# API Reference

## `waveql.connect`
```python
def connect(
    connection_string: str = None, 
    username: str = None, 
    password: str = None, 
    **kwargs
) -> WaveQLConnection
```
Creates a synchronous connection to a data source.
*   `connection_string`: URI formatted string (e.g., `servicenow://...`)

## `waveql.WaveQLConnection`
The main connection object.

### Methods
*   `cursor()`: Returns a new `WaveQLCursor`.
*   `commit()`: No-op (APIs are auto-commit usually).
*   `close()`: Closes the underlying HTTP session.

## `waveql.WaveQLCursor`
Standard DB-API 2.0 cursor.

### Methods
*   `execute(query, params=None)`: Prepares and runs a SQL query.
*   `fetchone()`: Returns the next row.
*   `fetchall()`: Returns all remaining rows in a list.
*   `fetchmany(size)`: Returns `size` rows.

### Extensions
*   `fetchall().to_df()`: Converts the result set immediately to a Pandas DataFrame.
*   `fetchall().to_arrow()`: Returns the underlying PyArrow Table.

## `waveql.adapters`

### `register_adapter(name, class_ref)`
Registers a new adapter class to be used with a specific URI scheme.

## Exceptions

*   `waveql.WaveQLError`: Base exception for all errors.
*   `waveql.AuthenticationError`: 401/403 errors from APIs.
*   `waveql.QueryError`: SQL syntax errors or invalid field names.
*   `waveql.ConnectionError`: Network/Timeout issues.
