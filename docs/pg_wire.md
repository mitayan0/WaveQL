# PostgreSQL Wire Protocol Server

WaveQL includes a PostgreSQL wire protocol server that enables BI tools like **Tableau**, **PowerBI**, **DBeaver**, and any PostgreSQL-compatible client to connect directly to your API data sources.

## Quick Start

```bash
# Start the server
waveql-server --port 5432

# Connect with psql
psql -h localhost -p 5432 -U postgres -d waveql

# Run SQL queries against your APIs
waveql=> SELECT * FROM servicenow.incident LIMIT 5;
```

## Architecture

```
┌─────────────┐     PostgreSQL    ┌──────────────┐
│   Tableau   │◄──── Protocol ────►│ PGWireServer │
│   PowerBI   │     (TCP 5432)    │              │
│   DBeaver   │                   │   WaveQL     │
│   psql      │                   │   Engine     │
└─────────────┘                   └──────┬───────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                    ▼
              ┌──────────┐        ┌──────────┐        ┌──────────┐
              │ServiceNow│        │Salesforce│        │  Jira    │
              └──────────┘        └──────────┘        └──────────┘
```

## Features

### Catalog Emulation (pg_catalog)

BI tools query `pg_catalog` tables to discover schemas and tables. WaveQL emulates these:

| Catalog Table | Purpose |
|---------------|---------|
| `pg_namespace` | Lists schemas (one per adapter) |
| `pg_class` | Lists tables from adapters |
| `pg_attribute` | Column metadata |
| `pg_type` | PostgreSQL type definitions |
| `pg_database` | Database info |
| `pg_settings` | Server configuration |

### Type Mapping

WaveQL automatically maps between PyArrow types and PostgreSQL OIDs:

| Arrow Type | PostgreSQL Type |
|------------|-----------------|
| `int32()` | `int4` (OID 23) |
| `int64()` | `int8` (OID 20) |
| `string()` | `text` (OID 25) |
| `bool_()` | `bool` (OID 16) |
| `float64()` | `float8` (OID 701) |
| `timestamp()` | `timestamp` (OID 1114) |
| `struct()` | `jsonb` (OID 3802) |
| `list()` | Array types |

### Protocol Support

- **Simple Query Protocol** (`Q` message) - Used by most clients
- **Extended Query Protocol** (`Parse`, `Bind`, `Execute`) - Used by prepared statements
- **Authentication** - Trust mode (default) or MD5 password

## CLI Reference

```bash
# Basic usage
waveql-server

# Custom port
waveql-server --port 15432

# With pre-configured connection
waveql-server --connection "servicenow://instance.service-now.com"

# MD5 authentication
waveql-server --auth md5

# Verbose logging
waveql-server -v
```

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--host, -H` | Host to bind to | `0.0.0.0` |
| `--port, -p` | Port to listen on | `5432` |
| `--connection, -c` | WaveQL connection string | None |
| `--auth` | Authentication mode (`trust`, `md5`) | `trust` |
| `--verbose, -v` | Enable debug logging | `False` |

## Python API

```python
import waveql
from waveql.pg_wire import PGWireServer
import asyncio

# Create a WaveQL connection with adapters
conn = waveql.connect()
conn.register_adapter("servicenow", ServiceNowAdapter(host="..."))

# Start the PostgreSQL server
server = PGWireServer(conn)
asyncio.run(server.serve(host="0.0.0.0", port=5432))
```

### Async Context

```python
from waveql.pg_wire import run_server

async def main():
    conn = waveql.connect("servicenow://...")
    await run_server(conn, port=5432)

asyncio.run(main())
```

## Connecting from BI Tools

### Tableau

1. Open Tableau Desktop
2. Connect → PostgreSQL
3. Enter:
   - Server: `localhost`
   - Port: `5432`
   - Database: `waveql`
   - Username: `postgres`
4. Click Connect
5. Your adapter schemas appear as database schemas

### PowerBI

1. Get Data → PostgreSQL
2. Server: `localhost:5432`
3. Database: `waveql`
4. Authenticate with username `postgres`

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5432`
3. Database: `waveql`
4. Username: `postgres`
5. Test Connection

### psql

```bash
psql -h localhost -p 5432 -U postgres -d waveql

# List schemas (adapters)
\dn

# List tables in a schema
\dt servicenow.*

# Query data
SELECT number, short_description 
FROM servicenow.incident 
WHERE priority = 1 
LIMIT 10;
```

## Special Commands

The server handles PostgreSQL administrative commands:

```sql
-- Transaction control (no-op, WaveQL is stateless)
BEGIN; COMMIT; ROLLBACK;

-- Session settings (ignored)
SET client_encoding = 'UTF8';

-- Server info
SHOW server_version;
SHOW ALL;

-- Session reset
DISCARD ALL;
```

## Limitations

Current implementation limitations:

1. **No transactions** - WaveQL is stateless; `BEGIN/COMMIT` are no-ops
2. **No indexes** - API sources don't have traditional indexes
3. **No COPY** - Bulk loading not supported yet
4. **Text format only** - Binary format for most types falls back to text
5. **Authentication** - Only `trust` and basic `md5` modes

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 5432
netstat -ano | findstr :5432  # Windows
lsof -i :5432                  # Linux/Mac

# Use a different port
waveql-server --port 15432
```

### Connection Refused

1. Check the server is running
2. Verify firewall allows connections
3. Try connecting to `127.0.0.1` instead of `localhost`

### Schema Not Visible

1. Adapters must be registered before starting the server
2. Check adapter's `list_tables()` method returns tables
3. Enable verbose mode: `waveql-server -v`

## Performance Notes

- The server is fully async and can handle multiple concurrent connections
- Each connection gets its own session with isolated state
- Query results are streamed row-by-row to minimize memory
- Large result sets work because PostgreSQL protocol streams data
