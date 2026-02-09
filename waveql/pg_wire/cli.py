"""
WaveQL PostgreSQL Server CLI

Starts a PostgreSQL wire protocol server, enabling BI tools like
Tableau, PowerBI, and DBeaver to connect to WaveQL via standard
PostgreSQL drivers.

Usage:
    waveql-server --port 5432
    waveql-server --port 5432 --connection "servicenow://instance.service-now.com"
"""

from __future__ import annotations
import argparse
import asyncio
import logging
import signal
import sys

import waveql


def main():
    parser = argparse.ArgumentParser(
        description="WaveQL PostgreSQL Wire Protocol Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  waveql-server
  waveql-server --port 5432
  waveql-server --port 15432 --connection "servicenow://instance.service-now.com"
  waveql-server --auth md5

Connection:
  Once running, connect with any PostgreSQL client:
    psql -h localhost -p 5432 -U postgres -d waveql
    
  Or use BI tools like Tableau or DBeaver with connection string:
    Host: localhost
    Port: 5432
    Database: waveql
    User: postgres
"""
    )
    
    parser.add_argument(
        "--host", "-H",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=5432,
        help="Port to listen on (default: 5432)"
    )
    parser.add_argument(
        "--connection", "-c",
        help="WaveQL connection string for default adapter"
    )
    parser.add_argument(
        "--auth",
        choices=["trust", "md5"],
        default="trust",
        help="Authentication mode (default: trust)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create WaveQL connection
    try:
        conn = waveql.connect(args.connection)
    except Exception as e:
        print(f"Error creating WaveQL connection: {e}", file=sys.stderr)
        print("Starting server without pre-configured adapter.", file=sys.stderr)
        conn = waveql.connect()
    
    # Import and run server
    from waveql.pg_wire import PGWireServer
    
    server = PGWireServer(conn, auth_mode=args.auth)
    
    print(r"""
 __        __             ___  _     
 \ \      / /_ ___   ___ / _ \| |    
  \ \ /\ / / _` \ \ / / | | | | |    
   \ V  V / (_| |\ V /| | |_| | |___ 
    \_/\_/ \__,_| \_/  \ \__\_\_____|
                        \____/       
    """)
    print("🐘 PostgreSQL Wire Protocol Server")
    print(f"   Listening on {args.host}:{args.port}")
    print(f"   Auth mode: {args.auth}")
    print()
    print(f"   Connect with: psql -h {args.host} -p {args.port} -U postgres -d waveql")
    print()
    
    # Handle graceful shutdown
    async def shutdown(sig):
        print(f"\nReceived {sig.name}, shutting down...")
        await server.stop()
    
    async def run():
        # Set up signal handlers
        loop = asyncio.get_event_loop()
        
        if sys.platform != "win32":
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
        
        try:
            await server.serve(args.host, args.port)
        except asyncio.CancelledError:
            pass
    
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nShutdown complete.")


if __name__ == "__main__":
    main()
