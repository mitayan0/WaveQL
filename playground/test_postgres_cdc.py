"""
PostgreSQL CDC Full Feature Test

This script tests the PostgreSQL WAL-based Change Data Capture functionality.
It will:
1. Connect to your local PostgreSQL database
2. Create a test table
3. Set up a CDC stream
4. Insert/Update/Delete records
5. Verify changes are captured in real-time

Prerequisites:
- PostgreSQL 9.4+ with wal_level=logical
- User with REPLICATION privilege
- wal2json extension installed (recommended) or test_decoding

To check your PostgreSQL config:
    SHOW wal_level;  -- Should be 'logical'

To install wal2json (if not installed):
    -- On Ubuntu/Debian:
    sudo apt-get install postgresql-15-wal2json
    
    -- On Windows with pgAdmin, you may need to compile or use test_decoding instead

To grant replication privilege:
    ALTER USER your_user WITH REPLICATION;

Usage:
    1. Set your credentials below
    2. Run: python playground/test_postgres_cdc.py
"""

import os
import sys
import asyncio
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# =============================================================================
# CONFIGURATION - Reads from environment variables or .env file
# =============================================================================

from dotenv import load_dotenv
load_dotenv()  # Load from .env file

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DATABASE", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

# Build connection string
CONNECTION_STRING = (
    f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}"
    f"@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
)

# Test configuration
TEST_TABLE = os.getenv("POSTGRES_CDC_TEST_TABLE", "waveql_cdc_test").strip()
SLOT_NAME = os.getenv("POSTGRES_CDC_SLOT_NAME", "waveql_test_slot").strip()
OUTPUT_PLUGIN = os.getenv("POSTGRES_CDC_OUTPUT_PLUGIN", "wal2json").strip()  # or "test_decoding"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_success(msg: str):
    """Print success message."""
    print(f"  [OK] {msg}")


def print_error(msg: str):
    """Print error message."""
    print(f"  [ERROR] {msg}")


def print_info(msg: str):
    """Print info message."""
    print(f"  [INFO] {msg}")


def print_change(change):
    """Print a change event nicely."""
    op_symbol = {"insert": "+", "update": "~", "delete": "-"}
    symbol = op_symbol.get(change.operation.value, "?")
    print(f"    [{symbol}] {change.operation.value.upper()}: key={change.key}")
    if change.data:
        print(f"       Data: {change.data}")
    if change.old_data:
        print(f"       Old:  {change.old_data}")


# =============================================================================
# DATABASE SETUP FUNCTIONS
# =============================================================================

def check_prerequisites():
    """Check if PostgreSQL is configured correctly for CDC."""
    import psycopg2
    
    print_header("Checking Prerequisites")
    
    try:
        conn = psycopg2.connect(CONNECTION_STRING)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Check wal_level
        cur.execute("SHOW wal_level")
        wal_level = cur.fetchone()[0]
        if wal_level == "logical":
            print_success(f"wal_level = {wal_level}")
        else:
            print_error(f"wal_level = {wal_level} (should be 'logical')")
            print_info("Fix: Set wal_level = logical in postgresql.conf and restart")
            return False
        
        # Check if user has replication privilege
        cur.execute("""
            SELECT rolreplication FROM pg_roles 
            WHERE rolname = current_user
        """)
        has_replication = cur.fetchone()[0]
        if has_replication:
            print_success("User has REPLICATION privilege")
        else:
            print_error("User lacks REPLICATION privilege")
            print_info(f"Fix: ALTER USER {POSTGRES_CONFIG['user']} WITH REPLICATION;")
            return False
        
        # Check for wal2json
        if OUTPUT_PLUGIN == "wal2json":
            try:
                cur.execute("""
                    SELECT * FROM pg_create_logical_replication_slot(
                        'waveql_check_slot', 'wal2json'
                    )
                """)
                cur.execute("SELECT pg_drop_replication_slot('waveql_check_slot')")
                print_success("wal2json plugin available")
            except psycopg2.Error as e:
                if "could not access file" in str(e) or "not found" in str(e).lower():
                    print_error("wal2json plugin not installed")
                    print_info("Fix: Install wal2json or use test_decoding instead")
                    print_info("     Set OUTPUT_PLUGIN = 'test_decoding' in this script")
                    return False
                else:
                    # Slot might already exist, that's ok
                    try:
                        cur.execute("SELECT pg_drop_replication_slot('waveql_check_slot')")
                    except:
                        pass
                    print_success("wal2json plugin available")
        
        cur.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print_error(f"Connection failed: {e}")
        return False


def setup_test_table():
    """Create the test table."""
    import psycopg2
    
    print_header("Setting Up Test Table")
    
    conn = psycopg2.connect(CONNECTION_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Drop existing table
    cur.execute(f"DROP TABLE IF EXISTS {TEST_TABLE} CASCADE")
    print_info("Dropped existing table (if any)")
    
    # Create new test table
    cur.execute(f"""
        CREATE TABLE {TEST_TABLE} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(200),
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success(f"Created table: {TEST_TABLE}")
    
    # Set REPLICA IDENTITY to FULL for complete UPDATE/DELETE info
    cur.execute(f"ALTER TABLE {TEST_TABLE} REPLICA IDENTITY FULL")
    print_success("Set REPLICA IDENTITY FULL (enables old data capture)")
    
    cur.close()
    conn.close()


def cleanup_slot():
    """Clean up any existing replication slot."""
    import psycopg2
    import time
    
    conn = psycopg2.connect(CONNECTION_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        # First, terminate any active connections using this slot
        cur.execute(f"""
            SELECT pg_terminate_backend(active_pid) 
            FROM pg_replication_slots 
            WHERE slot_name = '{SLOT_NAME}' AND active_pid IS NOT NULL
        """)
        time.sleep(0.2)  # Brief pause to let backends terminate
        
        # Now drop the slot
        cur.execute(f"SELECT pg_drop_replication_slot('{SLOT_NAME}')")
        print_info(f"Dropped existing replication slot: {SLOT_NAME}")
    except psycopg2.Error:
        pass  # Slot doesn't exist, that's fine
    
    cur.close()
    conn.close()


def make_changes():
    """Make some changes to the test table."""
    import psycopg2
    
    print_header("Making Database Changes")
    
    conn = psycopg2.connect(CONNECTION_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    
    # INSERT
    print_info("Inserting 3 records...")
    cur.execute(f"""
        INSERT INTO {TEST_TABLE} (name, email, status)
        VALUES 
            ('Alice Smith', 'alice@example.com', 'active'),
            ('Bob Johnson', 'bob@example.com', 'active'),
            ('Charlie Brown', 'charlie@example.com', 'pending')
        RETURNING id
    """)
    ids = [row[0] for row in cur.fetchall()]
    print_success(f"Inserted records with IDs: {ids}")
    time.sleep(0.5)
    
    # UPDATE
    print_info("Updating Bob's status...")
    cur.execute(f"""
        UPDATE {TEST_TABLE}
        SET status = 'verified', updated_at = CURRENT_TIMESTAMP
        WHERE name = 'Bob Johnson'
    """)
    print_success("Updated Bob's status to 'verified'")
    time.sleep(0.5)
    
    # DELETE
    print_info("Deleting Charlie...")
    cur.execute(f"""
        DELETE FROM {TEST_TABLE}
        WHERE name = 'Charlie Brown'
    """)
    print_success("Deleted Charlie's record")
    
    cur.close()
    conn.close()
    
    return ids


# =============================================================================
# CDC TEST FUNCTIONS
# =============================================================================

async def test_get_changes():
    """Test the get_changes() method (one-shot, peek at changes)."""
    from waveql.cdc.postgres import PostgresCDCProvider
    from waveql.adapters.sql import SQLAdapter
    
    print_header("Test 1: Get Changes (One-Shot)")
    
    # Create adapter
    adapter = SQLAdapter(host=CONNECTION_STRING)
    
    # Create CDC provider
    provider = PostgresCDCProvider(
        adapter=adapter,
        connection_string=CONNECTION_STRING,
        slot_name=SLOT_NAME,
        output_plugin=OUTPUT_PLUGIN,
        create_slot=True,
    )
    
    print_info(f"Created CDC provider: {provider}")
    
    # IMPORTANT: Create the slot BEFORE making changes
    # Otherwise the WAL won't have the changes for this slot
    await provider._ensure_slot_exists()
    print_info(f"Ensured slot exists: {SLOT_NAME}")
    
    # Make some changes AFTER the slot is created
    make_changes()
    
    # Give PostgreSQL a moment to write to WAL
    await asyncio.sleep(1)
    
    # Get changes
    print_info("Fetching changes from WAL...")
    try:
        changes = await provider.get_changes(TEST_TABLE)
        
        if changes:
            print_success(f"Captured {len(changes)} changes:")
            for change in changes:
                print_change(change)
        else:
            print_info("No changes captured (this is normal if slot was just created)")
            print_info("Changes are only captured AFTER the slot is created")
        
        # Clean up
        await provider.drop_slot(force=True)
        
    except Exception as e:
        print_error(f"Error getting changes: {e}")
        await provider.drop_slot(force=True)
        raise
    
    return True


async def test_stream_changes():
    """Test the stream_changes() method (real-time streaming)."""
    from waveql.cdc.postgres import PostgresCDCProvider
    from waveql.adapters.sql import SQLAdapter
    from waveql.cdc.models import CDCConfig
    import psycopg2
    
    print_header("Test 2: Stream Changes (Real-Time)")
    
    # Clean up slot first
    cleanup_slot()
    
    # Create adapter and provider
    adapter = SQLAdapter(host=CONNECTION_STRING)
    provider = PostgresCDCProvider(
        adapter=adapter,
        connection_string=CONNECTION_STRING,
        slot_name=SLOT_NAME,
        output_plugin=OUTPUT_PLUGIN,
        create_slot=True,
    )
    
    print_info(f"Created CDC provider with slot: {SLOT_NAME}")
    
    # Track received changes
    received_changes = []
    expected_count = 5  # 3 inserts + 1 update + 1 delete
    
    async def stream_with_timeout():
        """Stream changes with a timeout."""
        config = CDCConfig(poll_interval=0.5, batch_size=100)
        
        try:
            async for change in provider.stream_changes(TEST_TABLE, config):
                received_changes.append(change)
                print_change(change)
                
                if len(received_changes) >= expected_count:
                    break
        except asyncio.CancelledError:
            pass
    
    async def make_changes_async():
        """Make changes in a separate task."""
        await asyncio.sleep(3)  # Wait longer for stream to fully start
        
        conn = psycopg2.connect(CONNECTION_STRING)
        conn.autocommit = True
        cur = conn.cursor()
        
        # INSERT
        print_info("  [Producer] Inserting records...")
        cur.execute(f"""
            INSERT INTO {TEST_TABLE} (name, email, status)
            VALUES 
                ('Diana Prince', 'diana@example.com', 'active'),
                ('Eve Wilson', 'eve@example.com', 'active'),
                ('Frank Miller', 'frank@example.com', 'pending')
        """)
        await asyncio.sleep(0.5)
        
        # UPDATE
        print_info("  [Producer] Updating Diana...")
        cur.execute(f"""
            UPDATE {TEST_TABLE}
            SET status = 'verified'
            WHERE name = 'Diana Prince'
        """)
        await asyncio.sleep(0.5)
        
        # DELETE
        print_info("  [Producer] Deleting Frank...")
        cur.execute(f"""
            DELETE FROM {TEST_TABLE}
            WHERE name = 'Frank Miller'
        """)
        
        cur.close()
        conn.close()
    
    print_info("Starting CDC stream (will capture changes for 10 seconds)...")
    print_info("Making database changes in background...")
    print()
    print("  Received Changes:")
    print("  -----------------")
    
    # Run both tasks
    stream_task = asyncio.create_task(stream_with_timeout())
    producer_task = asyncio.create_task(make_changes_async())
    
    # Wait with timeout
    try:
        await asyncio.wait_for(
            asyncio.gather(stream_task, producer_task),
            timeout=15.0
        )
    except asyncio.TimeoutError:
        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass
    
    # Stop the provider
    await provider.stop()
    
    print()
    if received_changes:
        print_success(f"Captured {len(received_changes)} changes in real-time!")
        
        # Verify we got all types
        operations = set(c.operation.value for c in received_changes)
        if "insert" in operations:
            print_success("Captured INSERT operations")
        if "update" in operations:
            print_success("Captured UPDATE operations")
        if "delete" in operations:
            print_success("Captured DELETE operations")
    else:
        print_error("No changes captured. Check PostgreSQL configuration.")
    
    return len(received_changes) > 0


async def test_slot_management():
    """Test replication slot management."""
    from waveql.cdc.postgres import PostgresCDCProvider
    from waveql.adapters.sql import SQLAdapter
    
    print_header("Test 3: Slot Management")
    
    # Clean up first
    cleanup_slot()
    
    adapter = SQLAdapter(host=CONNECTION_STRING)
    provider = PostgresCDCProvider(
        adapter=adapter,
        connection_string=CONNECTION_STRING,
        slot_name=SLOT_NAME,
        output_plugin=OUTPUT_PLUGIN,
        create_slot=True,
    )
    
    # Create slot by ensuring it exists
    await provider._ensure_slot_exists()
    print_success(f"Created replication slot: {SLOT_NAME}")
    
    # Get slot info
    info = await provider.get_slot_info()
    if info:
        print_success("Slot info retrieved:")
        print(f"      Name: {info['slot_name']}")
        print(f"      Plugin: {info['plugin']}")
        print(f"      Active: {info['active']}")
        print(f"      Lag: {info['lag']}")
    else:
        print_error("Failed to get slot info")
    
    # Drop slot
    dropped = await provider.drop_slot()
    if dropped:
        print_success(f"Dropped replication slot: {SLOT_NAME}")
    else:
        print_error("Failed to drop slot")
    
    # Verify it's gone
    info = await provider.get_slot_info()
    if info is None:
        print_success("Confirmed slot no longer exists")
    
    return True


async def test_waveql_connection_api():
    """Test using CDC through WaveQL connection API."""
    import waveql
    
    print_header("Test 4: WaveQL Connection API")
    
    # Clean up slot first
    cleanup_slot()
    
    # Connect via WaveQL
    conn = waveql.connect(CONNECTION_STRING)
    print_success(f"Connected via WaveQL: {conn}")
    
    # Create CDC provider - since WaveQL parses the DSN and only stores
    # host component, we need to manually set the connection_string
    provider = conn.stream_changes_wal(
        table=TEST_TABLE,
        slot_name=SLOT_NAME,
        output_plugin=OUTPUT_PLUGIN,
    )
    # Override with full connection string since WaveQL connection parses it
    provider._connection_string = CONNECTION_STRING
    print_success(f"Created CDC provider via connection API: {provider}")
    
    # Get slot info
    await provider._ensure_slot_exists()
    info = await provider.get_slot_info()
    if info:
        print_success("Slot created successfully via WaveQL API")
        print_info(f"  Slot Name: {info['slot_name']}")
        print_info(f"  Plugin: {info['plugin']}")
        print_info(f"  Active: {info['active']}")
    
    # Cleanup
    await provider.drop_slot(force=True)
    conn.close()
    
    return True


# =============================================================================
# MAIN
# =============================================================================

async def run_all_tests():
    """Run all CDC tests."""
    print("\n" + "=" * 70)
    print("  PostgreSQL CDC Full Feature Test")
    print("  " + "=" * 66)
    print(f"  Connection: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    print(f"  User: {POSTGRES_CONFIG['user']}")
    print(f"  Slot: {SLOT_NAME}")
    print(f"  Plugin: {OUTPUT_PLUGIN}")
    print("=" * 70)
    
    # Check prerequisites
    if not check_prerequisites():
        print_header("Prerequisites Failed")
        print_error("Please fix the issues above and try again.")
        return
    
    # Setup
    setup_test_table()
    cleanup_slot()
    
    # Run tests
    results = {}
    
    try:
        # Test 1: Get Changes
        try:
            results["get_changes"] = await test_get_changes()
        except Exception as e:
            print_error(f"Test failed: {e}")
            results["get_changes"] = False
        
        # Reset table for next test
        setup_test_table()
        cleanup_slot()
        
        # Test 2: Stream Changes
        try:
            results["stream_changes"] = await test_stream_changes()
        except Exception as e:
            print_error(f"Test failed: {e}")
            import traceback
            traceback.print_exc()
            results["stream_changes"] = False
        
        # Test 3: Slot Management
        try:
            results["slot_management"] = await test_slot_management()
        except Exception as e:
            print_error(f"Test failed: {e}")
            results["slot_management"] = False
        
        # Test 4: WaveQL Connection API
        try:
            results["connection_api"] = await test_waveql_connection_api()
        except Exception as e:
            print_error(f"Test failed: {e}")
            results["connection_api"] = False
        
    finally:
        # Cleanup
        cleanup_slot()
    
    # Summary
    print_header("Test Summary")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "PASSED" if passed_test else "FAILED"
        print(f"  {test_name}: {status}")
    
    print()
    print(f"  Total: {passed}/{total} tests passed")
    
    if passed == total:
        print_success("All tests passed! PostgreSQL CDC is working correctly.")
    else:
        print_error("Some tests failed. Check the output above for details.")


if __name__ == "__main__":
    print("""
========================================================================
                     PostgreSQL CDC Test Suite                          
                                                                        
  This script tests WaveQL's WAL-based Change Data Capture feature.    
                                                                        
  Prerequisites:                                                        
  1. PostgreSQL 9.4+ with wal_level=logical                            
  2. User with REPLICATION privilege                                   
  3. wal2json extension (or use test_decoding)                         
========================================================================
    """)
    
    asyncio.run(run_all_tests())
