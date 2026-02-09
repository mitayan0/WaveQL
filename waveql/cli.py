
import sys
import argparse
import time
import logging
import pyarrow as pa
from typing import Optional

import waveql
from waveql.exceptions import WaveQLError

# Set up logging to silent by default for CLI
logging.getLogger("waveql").setLevel(logging.WARNING)

def main():
    parser = argparse.ArgumentParser(
        description="WaveQL CLI - The Semantic Layer for LLMs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  waveql "SELECT * FROM servicenow.incident LIMIT 5"
  waveql --explain "SELECT * FROM salesforce.Account"
  waveql --hybrid "SELECT * FROM jira.issue"
"""
    )
    parser.add_argument("query", nargs="?", help="SQL query to execute")
    parser.add_argument("--connection", "-c", help="Connection string")
    parser.add_argument("--explain", action="store_true", help="Show execution plan")
    parser.add_argument("--hybrid", action="store_true", help="Enable hybrid querying (Cache + Live)")
    parser.add_argument("--format", default="table", choices=["table", "json", "csv", "markdown"], help="Output format")
    
    args = parser.parse_args()
    
    if args.query:
        execute_single(args)
    else:
        repl(args.connection)

def execute_single(args):
    try:
        sql = args.query
        if args.explain:
            sql = f"EXPLAIN {sql}"
        if args.hybrid and "/*+ HYBRID */" not in sql.upper():
            sql = f"/*+ HYBRID */ {sql}"
            
        conn = waveql.connect(args.connection)
        cursor = conn.cursor()
        
        start = time.perf_counter()
        cursor.execute(sql)
        duration = time.perf_counter() - start
        
        results = cursor.to_arrow()
        
        if results is None or len(results) == 0:
            print("No results found.")
            return

        print_results(results, args.format)
        
        if not args.explain:
            print(f"\nQuery completed in {duration:.3f}s. Rows: {len(results)}")
            
    except WaveQLError as e:
        print(f"Query Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected Error: {e}", file=sys.stderr)
        sys.exit(1)

def print_results(table: pa.Table, format: str):
    if format == "json":
        import json
        print(json.dumps(table.to_pylist(), indent=2, default=str))
    elif format == "csv":
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=table.column_names)
        writer.writeheader()
        writer.writerows(table.to_pylist())
        print(output.getvalue())
    elif format == "markdown":
        df = table.to_pandas()
        print(df.to_markdown(index=False))
    else:
        # Default: Table
        try:
            from tabulate import tabulate
            print(tabulate(table.to_pylist(), headers="keys", tablefmt="pretty"))
        except ImportError:
            # Fallback to pandas
            print(table.to_pandas().to_string(index=False))

def repl(conn_str: Optional[str]):
    print("\n\x1b[36mWaveQL Interactive Shell\x1b[0m")
    print("Type '.exit' or press Ctrl+D to quit. Type '.help' for commands.\n")
    
    # Try to use prompt_toolkit for better experience
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.lexers import PygmentsLexer
        from pygments.lexers.sql import SqlLexer
        session = PromptSession(lexer=PygmentsLexer(SqlLexer))
    except ImportError:
        session = None

    conn = None
    try:
        conn = waveql.connect(conn_str)
    except Exception as e:
        print(f"Warning: Could not establish initial connection: {e}")

    while True:
        try:
            if session:
                query = session.prompt("waveql> ")
            else:
                query = input("waveql> ")
            
            if not query.strip():
                continue
                
            if query.strip().lower() in (".exit", "exit", "quit"):
                break
                
            if query.strip().lower() == ".help":
                print_help()
                continue

            if query.strip().lower() == ".stats":
                if conn:
                    print_stats(conn)
                else:
                    print("Error: No active connection.")
                continue

            # Check for connection change
            if query.strip().startswith(".connect "):
                new_conn = query.strip()[9:].strip()
                try:
                    conn = waveql.connect(new_conn)
                    print(f"Connected to {new_conn}")
                except Exception as e:
                    print(f"Connection failed: {e}")
                continue

            if not conn:
                print("Error: No active connection. Use '.connect <conn_str>'")
                continue

            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.to_arrow()
            duration = time.perf_counter() - start
            
            if results is not None:
                print_results(results, "table")
                print(f"\n{len(results)} rows in {duration:.3f}s")
            else:
                print(f"Done in {duration:.3f}s")                
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print(f"Error: {e}")

def print_stats(conn):
    """Print connection and adapter statistics."""
    print("\n\x1b[32m--- WaveQL Connection Statistics ---\x1b[0m")
    
    # Cache Stats
    print("\n[Cache Performance]")
    try:
        stats = conn.cache_stats.to_dict()
        print(f"  Hit Rate: {stats.get('hit_rate', '0%')}")
        print(f"  Hits:     {stats.get('hits', 0)}")
        print(f"  Misses:   {stats.get('misses', 0)}")
        print(f"  Size:     {stats.get('size_mb', 0)} MB")
    except Exception:
        print("  Cache metrics unavailable.")

    # Adapter Latency (CBO)
    print("\n[Adapter Performance (CBO)]")
    try:
        # Access through the internal adapter map in connection
        adapters = conn._adapters if hasattr(conn, '_adapters') else {}
        if not adapters:
            print("  No active adapters.")
        else:
            for name, adapter in adapters.items():
                latency = getattr(adapter, 'avg_latency_per_row', 0)
                print(f"  {name: <12} | Avg Latency/Row: {latency*1000:.3f}ms")
    except Exception as e:
        print(f"  Adapter metrics unavailable: {e}")
    print("")

def print_help():
    print("""
Available Commands:
  .connect <str>   Change connection
  .stats           Show cache and performance statistics
  .exit            Exit the shell
  .help            Show this help
  <SQL Query>      Execute SQL query against connected adapters
""")

if __name__ == "__main__":
    main()
