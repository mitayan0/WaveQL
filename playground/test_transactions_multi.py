"""
WaveQL Multi-Adapter Transactions - Saga Pattern Test
======================================================
Tests distributed transactions across multiple SaaS adapters using the Saga pattern.

Features Tested:
1. Transaction coordinator initialization
2. Multi-adapter transaction execution
3. Automatic rollback on failure
4. Dead Letter Queue handling (via TransactionLog)
5. Transaction logging and recovery
6. Compensation retry policies

Prerequisites:
- At least two adapters configured (any combination of):
  - ServiceNow: SN_INSTANCE, SN_USERNAME, SN_PASSWORD
  - Salesforce: SF_HOST, SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
  - HubSpot: HUBSPOT_API_KEY
  - Stripe: STRIPE_API_KEY

Usage:
    python playground/test_transactions_multi.py
"""

import os
import sys
import asyncio
from pathlib import Path
from datetime import datetime
import tempfile
import json
import uuid
from unittest.mock import MagicMock 

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Load environment
from dotenv import load_dotenv
load_dotenv()

import waveql
from waveql.transaction.coordinator import (
    TransactionCoordinator,
    TransactionLog,
    CompensationRetryPolicy,
    TransactionState,
    InsertResult,
    Transaction,
    TransactionOperation,
    CompensatingAction,
    OperationType
)


# =============================================================================
# Configuration
# =============================================================================

# ServiceNow
SN_INSTANCE = os.getenv("SN_INSTANCE")
SN_USERNAME = os.getenv("SN_USERNAME")
SN_PASSWORD = os.getenv("SN_PASSWORD")

# Salesforce
SF_HOST = os.getenv("SF_HOST")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

# HubSpot
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

# Stripe
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")


def separator(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def get_available_adapters():
    """Return list of available adapter configurations."""
    adapters = []
    
    if SN_INSTANCE and SN_USERNAME and SN_PASSWORD:
        adapters.append({
            "name": "ServiceNow",
            "conn_str": f"servicenow://{SN_INSTANCE}",
            "auth": {"username": SN_USERNAME, "password": SN_PASSWORD},
            "table": "incident",
            "insert_data": {"short_description": f"WaveQL Transaction Test {datetime.now().isoformat()}", "priority": 4},
        })
    
    if SF_HOST and SF_USERNAME and SF_PASSWORD:
        adapters.append({
            "name": "Salesforce",
            "conn_str": f"salesforce://{SF_HOST}",
            "auth": {
                "username": SF_USERNAME,
                "password": f"{SF_PASSWORD}{SF_SECURITY_TOKEN or ''}"
            },
            "table": "Contact",
            "insert_data": {"LastName": f"WaveQL_Test_{datetime.now().strftime('%H%M%S')}", "Email": "waveql@test.com"},
        })
    
    if HUBSPOT_API_KEY:
        adapters.append({
            "name": "HubSpot",
            "conn_str": "hubspot://api.hubapi.com",
            "auth": {"api_key": HUBSPOT_API_KEY},
            "table": "contacts",
            "insert_data": {"email": f"waveql-{datetime.now().strftime('%H%M%S')}@test.com", "firstname": "WaveQL", "lastname": "Test"},
        })
    
    if STRIPE_API_KEY:
        adapters.append({
            "name": "Stripe",
            "conn_str": "stripe://api.stripe.com",
            "auth": {"api_key": STRIPE_API_KEY},
            "table": "customers",
            "insert_data": {"email": f"waveql-{datetime.now().strftime('%H%M%S')}@test.com", "name": "WaveQL Test"},
        })
    
    return adapters


# =============================================================================
# Transaction Coordinator Tests
# =============================================================================

def test_transaction_log():
    """Test 1: Transaction log persistence."""
    separator("1. Transaction Log Persistence")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "transactions.db"
        log = TransactionLog(str(log_path))
        
        # Create a transaction
        tx_id = "TX-001"
        tx = Transaction(id=tx_id)
        tx.state = TransactionState.PENDING
        
        log.save_transaction(tx)
        print(f"  Created transaction: {tx_id}")
        
        # Load it back
        loaded_tx = log.load_transaction(tx_id)
        print(f"  Loaded transaction: {loaded_tx.id}, state={loaded_tx.state}")
        
        # Update state
        tx.state = TransactionState.COMMITTED
        log.save_transaction(tx)
        
        loaded_tx = log.load_transaction(tx_id)
        print(f"  Updated state: {loaded_tx.state}")
        
        # Test pending transactions list
        tx2_id = "TX-002"
        tx2 = Transaction(id=tx2_id)
        tx2.state = TransactionState.PENDING
        log.save_transaction(tx2)
        
        pending = log.get_pending_transactions()
        print(f"  Pending transactions: {len(pending)}")
        assert any(t.id == tx2_id for t in pending), "TX-002 should be pending"
        
        print("  ✓ Transaction log works")
        return True


def test_dlq_operations():
    """Test 2: Dead Letter Queue operations via TransactionLog."""
    separator("2. Dead Letter Queue Operations")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "transactions.db"
        log = TransactionLog(str(log_path))
        
        # Create a compensation action to store
        comp_action = CompensatingAction(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            original_data={},
            result_data={"sys_id": "123"},
            record_id="123"
        )
        
        # Add failed operation
        dlq_id = log.add_to_dlq(
            transaction_id="TX-FAIL-001",
            operation_id="OP-001",
            compensation=comp_action,
            error="Connection timeout"
        )
        print(f"  Added failed operation to DLQ (ID: {dlq_id})")
        
        # Count entries
        count = log.get_dlq_count()
        print(f"  DLQ entries: {count}")
        assert count == 1
        
        # Resolve one
        log.resolve_dlq_entry(dlq_id, notes="manually_fixed")
        resolved_count = log.get_dlq_count()
        print(f"  After resolving one: {resolved_count}")
        assert resolved_count == 0
        
        print("  ✓ Dead Letter Queue works")
        return True


def test_compensation_retry_policy():
    """Test 3: Compensation retry policy."""
    separator("3. Compensation Retry Policy")
    
    # Default policy
    policy = CompensationRetryPolicy()
    print(f"  Default policy: max_retries={policy.max_retries}, base_delay={policy.base_delay}")
    
    # Test retry decisions
    print(f"  Should retry at attempt 0: {policy.should_retry(0)}")
    print(f"  Should retry at attempt 3: {policy.should_retry(3)}")
    print(f"  Should retry at attempt 5: {policy.should_retry(5)}")
    
    # Test backoff calculation
    for attempt in range(4):
        delay = policy.calculate_delay(attempt)
        print(f"  Backoff delay for attempt {attempt}: {delay:.2f}s")
    
    # Custom policy
    custom_policy = CompensationRetryPolicy(max_retries=10, base_delay=0.5, max_delay=30.0)
    print(f"\n  Custom policy: max_retries={custom_policy.max_retries}")
    
    print("  ✓ Compensation retry policy works")
    return True


def test_insert_result():
    """Test 4: InsertResult handling."""
    separator("4. InsertResult Handling")
    
    # From integer
    r1 = InsertResult.from_adapter_result(1, {})
    # Note: rowcount maps to rows_affected, inserted_id might be None for pure int result
    print(f"  From int(1): rows_affected={r1.rows_affected}")
    
    # From dict with id
    values = {"name": "test"}
    r2 = InsertResult.from_adapter_result({"id": "abc123", "success": True}, values)
    print(f"  From dict: record_id={r2.record_id}")
    
    # From dict with sys_id (ServiceNow style)
    r3 = InsertResult.from_adapter_result({"sys_id": "xyz789"}, values)
    print(f"  From dict (sys_id): record_id={r3.record_id}")
    
    # Serialization
    d = r2.to_dict()
    print(f"  Serialized: {d}")
    
    print("  ✓ InsertResult handling works")
    return True


def test_transaction_coordinator_mock():
    """Test 5: Transaction coordinator with mock adapters."""
    separator("5. Transaction Coordinator (Mock)")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "tx.db"
        log = TransactionLog(str(log_path))
        
        # Create coordinator with temp storage
        coordinator = TransactionCoordinator(
            adapters={},  # Empty for now
            log=log,
        )
        
        print(f"  Coordinator initialized")
        
        # Test begin/commit without operations
        tx = coordinator.begin()
        print(f"  Transaction started: {tx.id}")
        
        # Commit empty transaction
        coordinator.commit()
        print(f"  Transaction committed")
        
        print("  ✓ Transaction coordinator initialization works")
        return True


def test_transaction_rollback_mock():
    """Test 6: Transaction rollback on failure (Mock)."""
    separator("6. Transaction Rollback on Failure (Mock)")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "tx.db"
        log = TransactionLog(str(log_path))
        
        # Create mock adapter
        mock_adapter = MagicMock()
        mock_adapter.insert.return_value = {"id": "123"}
        mock_adapter.delete.return_value = 1
        
        # Create coordinator
        coordinator = TransactionCoordinator(
            adapters={"mock": mock_adapter},
            log=log,
        )
        
        # Begin transaction
        tx = coordinator.begin()
        print(f"  Transaction started: {tx.id}")
        
        # Perform insert (which succeeds)
        try:
            coordinator.insert("mock.table", {"col": "val"})
            print(f"  Insert executed")
            
            # Simulate failure elsewhere by calling rollback manually
            # In real code this would catch an exception
            print(f"  Simulating error, rolling back...")
            coordinator.rollback()
            
            # Verify delete was called (compensation)
            mock_adapter.delete.assert_called()
            print(f"  Compensation delete called verified")
            
        except Exception as e:
            print(f"  Unexpected error: {e}")
            return False
            
        # Verify state in log
        loaded_tx = log.load_transaction(tx.id)
        if loaded_tx:
            print(f"  Transaction final state: {loaded_tx.state}")
            assert loaded_tx.state in (TransactionState.ROLLED_BACK, TransactionState.FAILED)
        else:
            print("  Transaction not found in log (Error!)")
            return False
        
        print("  ✓ Transaction rollback logic works (Mock)")
        return True


def test_dlq_retry():
    """Test 7: DLQ retry mechanism."""
    separator("7. DLQ Retry Mechanism")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "transactions.db"
        log = TransactionLog(str(log_path))
        
        # Create failed compensation
        comp = CompensatingAction(
            adapter_name="sn",
            table="inc",
            operation=OperationType.INSERT,
            original_data={}, 
            result_data={}
        )
        
        # Add to DLQ
        dlq_id = log.add_to_dlq(
            transaction_id="TX-RETRY-001",
            operation_id="OP-001",
            compensation=comp,
            error="Temporary failure"
        )
        
        initial_count = log.get_dlq_count()
        print(f"  Initial DLQ entries: {initial_count}")
        
        # Simulate retry mechanism calling update
        log.update_dlq_attempt(dlq_id, error="Still failing", attempts=2)
        print("  Incremented retry attempt")
        
        # Resolve after successful retry
        log.resolve_dlq_entry(dlq_id, notes="retried_successfully")
        
        final_count = log.get_dlq_count()
        print(f"  Final DLQ entries: {final_count}")
        assert final_count == 0
        
        print("  ✓ DLQ retry mechanism works")
        return True


# =============================================================================
# Live Multi-Adapter Transaction Tests
# =============================================================================

def test_multi_adapter_transaction():
    """Test 8: Live multi-adapter transaction (requires credentials)."""
    separator("8. Multi-Adapter Transaction (Live)")
    
    adapters_conf = get_available_adapters()
    
    if len(adapters_conf) < 2:
        print(f"  ⚠ Skipped: Need at least 2 adapters, found {len(adapters_conf)}")
        print("  Available adapters:", [a["name"] for a in adapters_conf] if adapters_conf else "None")
        return None
    
    print(f"  Available adapters: {[a['name'] for a in adapters_conf]}")
    
    # Use first two adapters
    adapter1_conf, adapter2_conf = adapters_conf[0], adapters_conf[1]
    name1, name2 = adapter1_conf["name"].lower(), adapter2_conf["name"].lower()
    
    print(f"  Testing transaction across: {name1} ↔ {name2}")
    
    try:
        # Connect to both adapters
        # We assume waveql.connect works or we need to setup adapters manually
        # This part depends on WaveQL implementation details
        
        # For this test, we create 'real' adapter instances via connect
        conn1 = waveql.connect(adapter1_conf["conn_str"], **adapter1_conf["auth"])
        conn2 = waveql.connect(adapter2_conf["conn_str"], **adapter2_conf["auth"])
        
        # Assuming we can access the underlying adapter instance
        # Typically connection object has a way to get adapter. 
        # Using private _adapter if get_adapter is not available in public API.
        if hasattr(conn1, "get_adapter"):
            real_adapter1 = conn1.get_adapter("default")
        else:
            real_adapter1 = conn1._adapter 
            
        if hasattr(conn2, "get_adapter"):
            real_adapter2 = conn2.get_adapter("default")
        else:
            real_adapter2 = conn2._adapter

        # Setup coordinator
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "live_tx.db"
            log = TransactionLog(str(log_path))
            
            coordinator = TransactionCoordinator(
                adapters={
                    name1: real_adapter1,
                    name2: real_adapter2,
                },
                log=log,
            )
            
            # 1. Begin
            tx = coordinator.begin()
            print(f"  Transaction started: {tx.id}")
            
            try:
                # 2. Insert into Adapter 1
                res1 = coordinator.insert(f"{name1}.{adapter1_conf['table']}", adapter1_conf["insert_data"])
                print(f"  Insert 1 success: {res1}")
                
                # 3. Insert into Adapter 2
                res2 = coordinator.insert(f"{name2}.{adapter2_conf['table']}", adapter2_conf["insert_data"])
                print(f"  Insert 2 success: {res2}")
                
                # 4. Rollback (to test compensation in live env)
                # We ROLLBACK so we don't spam the actual system permanently
                coordinator.rollback()
                print(f"  Transaction rolled back successfully")
                
            except Exception as e:
                print(f"  Error during operations: {e}")
                if coordinator._current_transaction:
                    coordinator.rollback()
                raise
        
        print("  ✓ Multi-adapter transaction framework works")
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    print("\n" + "=" * 60)
    print("  WaveQL Multi-Adapter Transactions - Feature Test Suite")
    print("=" * 60)
    
    adapters = get_available_adapters()
    print(f"  Available adapters: {[a['name'] for a in adapters] if adapters else 'None'}")
    
    results = {}
    
    tests = [
        ("Transaction Log", test_transaction_log),
        ("DLQ Operations", test_dlq_operations),
        ("Compensation Retry Policy", test_compensation_retry_policy),
        ("InsertResult Handling", test_insert_result),
        ("Coordinator (Mock)", test_transaction_coordinator_mock),
        ("Rollback Logic (Mock)", test_transaction_rollback_mock),
        ("DLQ Retry", test_dlq_retry),
        ("Multi-Adapter Transaction (Live)", test_multi_adapter_transaction),
    ]
    
    for name, test_fn in tests:
        try:
            result = test_fn()
            results[name] = result
        except Exception as e:
            print(f"\n  ✗ FAILED: {name} - {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
    
    # Summary
    separator("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v is True)
    skipped = sum(1 for v in results.values() if v is None)
    failed = sum(1 for v in results.values() if v is False)
    
    for name, result in results.items():
        status = "[PASS]" if result is True else "[SKIP]" if result is None else "[FAIL]"
        print(f"  {status}  {name}")
    
    print(f"\n  Result: {passed} passed, {skipped} skipped, {failed} failed")


if __name__ == "__main__":
    main()
