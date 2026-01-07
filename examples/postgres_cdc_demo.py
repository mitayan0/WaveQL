"""
PostgreSQL CDC Demo (WAL-Based Streaming)

This script demonstrates WaveQL's PostgreSQL CDC capabilities.
It connects to a local PostgreSQL instance, creates a test table,
and streams changes (inserts, updates, deletes) in real-time.

Prerequisites:
- PostgreSQL 9.4+ with wal_level=logical
- User with REPLICATION privilege
- wal2json extension (recommended) or test_decoding

Usage:
    python examples/postgres_cdc_demo.py
"""

import os
import sys
import asyncio
import time
from dotenv import load_dotenv

# Add parent directory to path to allow running from root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import waveql
from waveql.cdc.postgres import PostgresCDCProvider
from waveql.adapters.sql import SQLAdapter
from waveql.cdc.models import CDCConfig

load_dotenv()

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DATABASE", "postgres")

CONNECTION_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

TEST_TABLE = "waveql_cdc_demo"
SLOT_NAME = "waveql_demo_slot"
OUTPUT_PLUGIN = os.getenv("POSTGRES_CDC_OUTPUT_PLUGIN", "wal2json").strip()


def setup_database():
    """Create test table and ensure prerequisites."""
    import psycopg2
    
    print(f"Connecting to {CONNECTION_STRING}...")
    conn = psycopg2.connect(CONNECTION_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Check wal_level
    cur.execute("SHOW wal_level")
    if cur.fetchone()[0] != "logical":
        print("ERROR: wal_level must be 'logical'. Please configure postgresql.conf.")
        sys.exit(1)
        
    # Create table
    cur.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    cur.execute(f"""
        CREATE TABLE {TEST_TABLE} (
            id SERIAL PRIMARY KEY,
            message TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute(f"ALTER TABLE {TEST_TABLE} REPLICA IDENTITY FULL")
    print(f"Created table {TEST_TABLE}")
    
    # Drop slot if exists from previous run (for clean demo)
    try:
        cur.execute(f"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{SLOT_NAME}'")
        time.sleep(0.2)
        cur.execute(f"SELECT pg_drop_replication_slot('{SLOT_NAME}')")
        print(f"Dropped existing slot {SLOT_NAME}")
    except:
        pass
        
    conn.close()


async def producer():
    """Simulate database activity."""
    import psycopg2
    await asyncio.sleep(2) # Wait for consumer to start
    
    conn = psycopg2.connect(CONNECTION_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    
    print("\n[Producer] Inserting record 1...")
    cur.execute(f"INSERT INTO {TEST_TABLE} (message) VALUES ('Hello CDC')")
    await asyncio.sleep(1)
    
    print("[Producer] Updating record 1...")
    cur.execute(f"UPDATE {TEST_TABLE} SET message = 'Updated CDC'")
    await asyncio.sleep(1)
    
    print("[Producer] Deleting record 1...")
    cur.execute(f"DELETE FROM {TEST_TABLE} WHERE message = 'Updated CDC'")
    
    conn.close()


async def consumer():
    """Stream changes using WaveQL."""
    print(f"Starting CDC stream on table '{TEST_TABLE}'...")
    
    # Create provider directly
    adapter = SQLAdapter(host=CONNECTION_STRING)
    provider = PostgresCDCProvider(
        adapter=adapter,
        connection_string=CONNECTION_STRING,
        slot_name=SLOT_NAME,
        output_plugin=OUTPUT_PLUGIN,
        create_slot=True
    )
    
    count = 0
    try:
        async for change in provider.stream_changes(TEST_TABLE):
            print(f"\n[Consumer] Captured change:")
            print(f"  Operation: {change.operation.value}")
            print(f"  Key: {change.key}")
            print(f"  Data: {change.data}")
            if change.old_data:
                print(f"  Old Data: {change.old_data}")
            
            count += 1
            if count >= 3:
                print("\nCaptured all expected changes!")
                break
    finally:
        await provider.drop_slot(force=True)


async def main():
    setup_database()
    
    # Run producer and consumer concurrently
    await asyncio.gather(
        consumer(),
        producer()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
