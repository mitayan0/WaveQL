# Wire Protocol (Draft)

**Status**: Experimental / In-Development.

WaveQL aims to implement the **Postgres Wire Protocol (pgwire)**. This means efficient binary communication that trick standard tools (Tableau, Excel, DBeaver) into thinking WaveQL is just a standard Postgres database.

## 1. Why Postgres?
-   It is the de-facto standard for data tools.
-   Drivers exist for every language (Java JDBC, C# Npgsql, Python psycopg2).
-   Documentation is open and thorough.

## 2. Architecture

```
[ Tableau / DBeaver ]
       |
       |  (TCP/5432)
       v
[ WaveQL PG Server ]
       | Parses 'Q' (Query) messages
       v
[ WaveQL Engine ]
       | Fetches data from APIs
       v
[ DuckDB ]
       | Formats result as Postgres Binary Tuples
       v
[ WaveQL PG Server ]
       | Sends 'D' (DataRow) messages
       v
[ Client ]
```

## 3. Implemented Messages

### Handshake (Startup)
-   `StartupMessage`: Client sends user/database.
-   `AuthenticationOk`: We currently accept cleartext or perform no-op auth (trusted).

### Query Cycle (Simple Query)
1.  **Client**: `Q` "SELECT * FROM zendesk.tickets"
2.  **Server**: `RowDescription` (Column names, types OIDs)
3.  **Server**: `DataRow` (The actual data, row by row)
4.  **Server**: `CommandComplete` ("SELECT 100")
5.  **Server**: `ReadyForQuery` (Idle)

## 4. Challenges
-   **Type Mapping**: Converting weird API types (Zendesk specialized dates) to standard Postgres OIDs (`TIMESTAMPTZ`, `VARCHAR`).
-   **Blocking vs Async**: standard `asyncpg` drivers expect swift responses. API calls take time. We may need to implement "Portal" support to stream rows slowly without timing out the client.
