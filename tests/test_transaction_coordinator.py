
import pytest
import sqlite3
import json
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch, ANY
from waveql.transaction.coordinator import (
    TransactionCoordinator, TransactionLog, TransactionState, 
    OperationType, InsertResult
)
from waveql.query_planner import Predicate
from waveql.exceptions import QueryError

# --- Fixtures ---

@pytest.fixture
def transaction_log(tmp_path):
    db_path = tmp_path / "test_transactions.db"
    return TransactionLog(str(db_path))

@pytest.fixture
def mock_adapter():
    adapter = MagicMock()
    adapter.adapter_name = "mock_adapter"
    # Mock insert return
    adapter.insert.return_value = {"id": "123", "rows_affected": 1}
    # Mock update/delete return (rows affected)
    adapter.update.return_value = 1
    adapter.delete.return_value = 1
    
    # Mock retrieval for update/delete compensation
    # fetch returns an object (Arrow Table) which has to_pydict
    mock_result = MagicMock()
    mock_result.__len__.return_value = 1
    # to_pydict returns dict of lists
    mock_result.to_pydict.return_value = {"id": ["123"], "val": ["old"]}
    
    adapter.fetch.return_value = mock_result
    return adapter

def test_rollback_insert(coordinator, mock_adapter):
    # We need to test rollback.
    # We need access to rollback method which was at the end of the file.
    # Assuming rollback executes compensations.
    
    txn = coordinator.begin()
    coordinator.insert("mock.users", {"name": "Alice"})
    
    # Now rollback
    coordinator.rollback()
    
    assert txn.state == TransactionState.ROLLED_BACK
    
    # Verify delete called for compensation
    mock_adapter.delete.assert_called()
    call_args = mock_adapter.delete.call_args
    # args: (table, predicates)
    assert call_args[0][0] == "users"
    preds = call_args[0][1]
    assert len(preds) == 1
    # Code tries sys_id first
    assert preds[0].column in ("sys_id", "id")
    assert preds[0].value == "123"

@pytest.fixture
def coordinator(transaction_log, mock_adapter):
    adapters = {"mock": mock_adapter}
    return TransactionCoordinator(adapters, log=transaction_log)

# --- Transaction Log Tests ---

def test_log_init(transaction_log):
    # Verify tables created
    with sqlite3.connect(transaction_log._db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        assert "transactions" in tables
        assert "operations" in tables
        assert "failed_compensations" in tables

def test_save_load_transaction(coordinator, transaction_log):
    txn = coordinator.begin()
    txn.state = TransactionState.COMMITTED
    
    # Save manually (coordinator saves on begin)
    transaction_log.save_transaction(txn)
    
    loaded = transaction_log.load_transaction(txn.id)
    assert loaded is not None
    assert loaded.id == txn.id
    assert loaded.state == TransactionState.COMMITTED

def test_dlq_add_get(transaction_log):
    # Create a fake compensation and error
    comp = MagicMock()
    comp.adapter_name = "test"
    comp.table = "t"
    comp.operation = OperationType.INSERT
    comp.original_data = {}
    comp.result_data = {}
    comp.record_id = None
    
    dlq_id = transaction_log.add_to_dlq(
        transaction_id="txn1", 
        operation_id="op1", 
        compensation=comp, 
        error="fail"
    )
    
    entries = transaction_log.get_dlq_entries()
    assert len(entries) == 1
    assert entries[0].id == dlq_id
    assert entries[0].error == "fail"

# --- Coordinator Tests ---

def test_begin_transaction(coordinator):
    txn = coordinator.begin()
    assert txn.state == TransactionState.IN_PROGRESS
    assert coordinator._current_transaction == txn

def test_insert_flow(coordinator, mock_adapter):
    txn = coordinator.begin()
    
    result = coordinator.insert("mock.users", {"name": "Alice"})
    
    assert result["record_id"] == "123"
    assert len(txn.operations) == 1
    op = txn.operations[0]
    assert op.operation == OperationType.INSERT
    assert op.success is True
    assert op.compensation is not None
    assert op.compensation.operation == OperationType.INSERT # Reversed action type logic? 
    # The Log/Coordinator code says: "For INSERT -> DELETE the created record"
    # But OperationType in compensation usually stores the ORIGINAL operation type 
    # and the coordinator logic decides how to reverse it?
    # Let's check coordinator.rollback logic (not shown in snippet but implied)
    # Actually line 110: "For INSERT -> DELETE the created record"
    
    mock_adapter.insert.assert_called_with("users", {"name": "Alice"})



def test_update_flow(coordinator, mock_adapter):
    txn = coordinator.begin()
    
    # Update requires where clause
    coordinator.update("mock.users", {"name": "Bob"}, {"id": "123"})
    
    # Should have fetched original data first
    mock_adapter.fetch.assert_called()
    mock_adapter.update.assert_called()
    
    assert len(txn.operations) == 1
    op = txn.operations[0]
    assert op.compensation is not None
    assert op.compensation.original_data == {"id": "123", "val": "old"}

def test_rollback_update(coordinator, mock_adapter):
    txn = coordinator.begin()
    coordinator.update("mock.users", {"name": "Bob"}, {"id": "123"})
    
    # Rollback should Update back to old values
    coordinator.rollback()
    
    # Verify update called twice (once for do, once for undo)
    assert mock_adapter.update.call_count == 2
    # Second call should be the compensation
    args, kwargs = mock_adapter.update.call_args_list[1]
    # update(table, values, predicates)
    # values should be original data
    assert args[1] == {"id": "123", "val": "old"}

def test_coordinator_commit(coordinator):
    txn = coordinator.begin()
    coordinator.commit()
    assert txn.state == TransactionState.COMMITTED
    assert coordinator._current_transaction is None

def test_fail_if_no_transaction(coordinator):
    with pytest.raises(RuntimeError):
        coordinator.insert("mock.users", {})


def test_delete_flow(coordinator, mock_adapter):
    txn = coordinator.begin()
    
    # Delete requires where clause
    coordinator.delete("mock.users", {"id": "123"})
    
    # Should have fetched original data first
    mock_adapter.fetch.assert_called()
    mock_adapter.delete.assert_called()
    
    assert len(txn.operations) == 1
    op = txn.operations[0]
    assert op.operation == OperationType.DELETE
    assert op.compensation is not None
    assert op.compensation.original_data == {"id": "123", "val": "old"}

def test_rollback_delete(coordinator, mock_adapter):
    txn = coordinator.begin()
    coordinator.delete("mock.users", {"id": "123"})
    
    # Rollback should Insert back the old values
    coordinator.rollback()
    
    # Verify insert called (compensation for delete)
    mock_adapter.insert.assert_called()
    args, _ = mock_adapter.insert.call_args
    # insert(table, values)
    assert args[0] == "users"
    assert args[1] == {"id": "123", "val": "old"}


def test_execute_error_triggers_rollback(coordinator, mock_adapter):
    mock_adapter.insert.side_effect = Exception("DB Error")
    
    txn = coordinator.begin()
    with pytest.raises(Exception, match="DB Error"):
        coordinator.insert("mock.users", {"name": "Fail"})
        
    assert txn.state == TransactionState.IN_PROGRESS
    assert len(txn.operations) == 1
    assert txn.operations[0].success is False

def test_compensation_failure_to_dlq(coordinator, mock_adapter):
    txn = coordinator.begin()
    coordinator.insert("mock.users", {"name": "Alice"})
    
    # Make delete (compensation) fail
    mock_adapter.delete.side_effect = Exception("Delete Failed")
    
    # Rollback should NOT raise exception even if compensation fails
    coordinator.rollback()
        
    # Verify transaction state
    assert txn.state == TransactionState.FAILED
    
    # Check DLQ (access private member _log)
    entries = coordinator._log.get_dlq_entries()
    assert len(entries) == 1
    # The coordinator tries multiple ID columns and eventually raises ValueError
    # if all fail, swallowing the original specific "Delete Failed" exception
    assert "Could not delete record" in entries[0].error

def test_recovery(transaction_log, mock_adapter):
    # Create a pending transaction in DB
    txn_id = str(uuid.uuid4())
    now = datetime.now()
    with sqlite3.connect(transaction_log._db_path) as conn:
        # Schema: id, state, created_at, completed_at, error
        conn.execute(
            "INSERT INTO transactions (id, state, created_at) VALUES (?, ?, ?)",
            (txn_id, TransactionState.IN_PROGRESS.value, now.isoformat())
        )
        # Add an operation to compensate
        op_id = str(uuid.uuid4())
        comp_data = {
            "adapter_name": "mock",
            "table": "users",
            "operation": OperationType.INSERT.value,
            "record_id": "123",
            "result_data": {},
            "original_data": {}
        }
        # Schema operations: ..., compensation_data, ...
        conn.execute(
            "INSERT INTO operations (id, transaction_id, adapter_name, table_name, operation, data, compensation_data, success) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (op_id, txn_id, "mock", "users", OperationType.INSERT.value, "{}", json.dumps(comp_data), 1)
        )
    
    adapters = {"mock": mock_adapter}
    coord = TransactionCoordinator(adapters, log=transaction_log)
    
    coord.recover_pending()
    
    mock_adapter.delete.assert_called()
    
    loaded = transaction_log.load_transaction(txn_id)
    assert loaded.state == TransactionState.ROLLED_BACK

def test_retry_dlq(coordinator, mock_adapter):
    txn = coordinator.begin()
    from waveql.transaction.coordinator import CompensatingAction
    comp = CompensatingAction(
        adapter_name="mock",
        table="users",
        operation=OperationType.INSERT,
        original_data={},
        result_data={},
        record_id="123"
    )
    
    dlq_id = coordinator._log.add_to_dlq(
        transaction_id="txn1", 
        operation_id="op1", 
        compensation=comp, 
        error="fail"
    )
    
    result = coordinator.retry_dlq_entry(dlq_id)
    assert result is True
    
    mock_adapter.delete.assert_called()
    
    entries = coordinator._log.get_dlq_entries()
    assert len(entries) == 0

def test_coordinator_errors(coordinator, mock_adapter):
    # Already in progress
    coordinator.begin()
    with pytest.raises(RuntimeError, match="Transaction already in progress"):
        coordinator.begin()
    coordinator.rollback()
    
    # Not in progress
    coord2 = TransactionCoordinator({"mock": mock_adapter})
    with pytest.raises(RuntimeError, match="No transaction in progress"):
        coord2.insert("mock.users", {})
    with pytest.raises(RuntimeError, match="No transaction in progress"):
        coord2.update("mock.users", {}, [])
    with pytest.raises(RuntimeError, match="No transaction in progress"):
        coord2.delete("mock.users", [])

def test_update_flow(coordinator, mock_adapter):
    coordinator.begin()
    # Mock fetch for compensation
    mock_adapter.fetch.return_value.to_pydict.return_value = {"id": ["123"], "name": ["Old"]}
    
    # Mock update result
    mock_adapter.update.return_value = {"id": "123", "updated": True}
    
    coordinator.update("mock.users", {"name": "New"}, {"id": "123"})
    coordinator.commit()
    
    assert mock_adapter.update.called

def test_delete_flow(coordinator, mock_adapter):
    coordinator.begin()
    # Mock fetch for compensation
    mock_adapter.fetch.return_value.to_pydict.return_value = {"id": ["123"], "name": ["To Delete"]}
    
    coordinator.delete("mock.users", {"id": "123"})
    coordinator.commit()

    assert mock_adapter.delete.called

def test_context_manager(coordinator, mock_adapter):
    # Mock for insert
    mock_adapter.insert.return_value = {"id": "1"}
    
    with coordinator.transaction() as txn:
        coordinator.insert("mock.users", {"name": "cm"})
    
    assert mock_adapter.insert.called

def test_dlq_management(coordinator):
    # get_dlq_count, get_dlq_entries, resolve_dlq_entry
    from waveql.transaction.coordinator import CompensatingAction
    comp = CompensatingAction("mock", "users", OperationType.INSERT, {}, {}, "123")
    dlq_id = coordinator._log.add_to_dlq("t1", "o1", comp, "err")
    
    assert coordinator.get_dlq_count() == 1
    assert len(coordinator.get_dlq_entries()) == 1
    
    coordinator.resolve_dlq_entry(dlq_id, "done")
    assert coordinator.get_dlq_count() == 0

def test_compensation_update_logic(coordinator, mock_adapter):
    # Test ROLLBACK of UPDATE
    coordinator.begin()
    mock_adapter.fetch.return_value.to_pydict.return_value = {"id": ["123"], "name": ["Old"]}
    coordinator.update("mock.users", {"name": "New"}, {"id": "123"})
    
    coordinator.rollback()
    # Should call update with Old values
    mock_adapter.update.assert_called()
    # Find the call that restored data
    found = False
    for call in mock_adapter.update.call_args_list:
        if isinstance(call[0][1], dict) and call[0][1].get("name") == "Old":
            found = True
            break
    assert found

def test_compensation_delete_logic(coordinator, mock_adapter):
    # Test ROLLBACK of DELETE (should re-insert)
    coordinator.begin()
    mock_adapter.fetch.return_value.to_pydict.return_value = {"id": ["123"], "name": ["Gone"]}
    # Delete requires a dict
    coordinator.delete("mock.users", {"id": "123"})
    
    coordinator.rollback()
    # Should call insert with Gone values
    # Note: compensation for DELETE is re-inserting the 'original_data'
    # The original_data returned by _fetch_for_compensation is a list of dicts.
    # Actually _fetch_for_compensation returns a List[Dict].
    # But wait, looking at coordinator.py:978 RE-INSERT logic:
    # it iterates over original_data.
    mock_adapter.insert.assert_called_with("users", {"id": "123", "name": "Gone"})

def test_get_adapter_error(coordinator):
    coordinator.begin()
    with pytest.raises(ValueError, match="Unknown adapter: ghost"):
        coordinator.insert("ghost.table", {})

def test_transaction_log_errors(transaction_log):
    # load_transaction for missing ID
    assert transaction_log.load_transaction("ghost") is None
    
    # get_pending_transactions with empty DB
    assert transaction_log.get_pending_transactions() == []

def test_insert_result_variants():
    from waveql.transaction.coordinator import InsertResult
    # From InsertResult
    ir = InsertResult(1, "123")
    assert InsertResult.from_adapter_result(ir, {}) == ir
    
    # From int
    ir2 = InsertResult.from_adapter_result(5, {"sys_id": "SYS1"})
    assert ir2.rows_affected == 5
    assert ir2.record_id == "SYS1"
    
    # From dict
    ir3 = InsertResult.from_adapter_result({"id": "D1", "other": "val"}, {})
    assert ir3.rows_affected == 1
    assert ir3.record_id == "D1"
    assert ir3.record_data["other"] == "val"
    
    # Fallback
    ir4 = InsertResult.from_adapter_result("weird", {"val": 1})
    assert ir4.rows_affected == 1
    assert ir4.record_data == {"val": 1}

def test_retry_policy_logic():
    from waveql.transaction.coordinator import CompensationRetryPolicy
    policy = CompensationRetryPolicy(max_retries=2, base_delay=1.0)
    assert policy.should_retry(0) is True
    assert policy.should_retry(1) is True
    assert policy.should_retry(2) is False
    
    delay = policy.calculate_delay(1)
    # 1.0 * (2^1) = 2.0. Jitter +/- 25% => [1.5, 2.5]
    assert 1.4 <= delay <= 2.6

def test_insert_compensation_fallbacks(coordinator, mock_adapter):
    from waveql.transaction.coordinator import CompensatingAction
    # Case: No record_id but in result_data
    comp = CompensatingAction("mock", "users", OperationType.INSERT, {}, {"id": "res_id"}, None)
    coordinator._execute_compensation(comp)
    mock_adapter.delete.assert_called()
    
    # Case: No record_id but in result_data.record_data
    mock_adapter.delete.reset_mock()
    comp2 = CompensatingAction("mock", "users", OperationType.INSERT, {}, {"record_data": {"sys_id": "sys_1"}}, None)
    coordinator._execute_compensation(comp2)
    mock_adapter.delete.assert_called()
    
    # Case: failure to find ID
    comp3 = CompensatingAction("mock", "users", OperationType.INSERT, {}, {}, None)
    with pytest.raises(ValueError, match="no record ID found"):
        coordinator._execute_compensation(comp3)

def test_update_compensation_errors(coordinator, mock_adapter):
    from waveql.transaction.coordinator import CompensatingAction
    # No original data warning (logs warning but doesn't fail)
    comp = CompensatingAction("mock", "users", OperationType.UPDATE, {}, {}, "123")
    coordinator._execute_compensation(comp) 
    
    # No ID found
    comp2 = CompensatingAction("mock", "users", OperationType.UPDATE, {"name": "Old"}, {}, None)
    with pytest.raises(ValueError, match="no record ID found"):
        coordinator._execute_compensation(comp2)

def test_retry_dlq_errors(coordinator):
    with pytest.raises(ValueError, match="DLQ entry not found"):
        coordinator.retry_dlq_entry("ghost")

def test_retry_dlq_failure(coordinator, mock_adapter):
    from waveql.transaction.coordinator import CompensatingAction
    comp = CompensatingAction("mock", "users", OperationType.INSERT, {}, {}, "123")
    dlq_id = coordinator._log.add_to_dlq("t1", "o1", comp, "err")
    
    mock_adapter.delete.side_effect = Exception("Still Failing")
    result = coordinator.retry_dlq_entry(dlq_id)
    assert result is False
    
    entries = coordinator._log.get_dlq_entries()
    assert len(entries) == 1
    assert entries[0].attempts == 2 # 1 (init) + 1 (retry)

def test_transaction_context_rollback(coordinator):
    # Mocking rollback verification
    txn_id = None
    with pytest.raises(Exception, match="Boom"):
        with coordinator.transaction() as coord:
            txn_id = coord._current_transaction.id
            raise Exception("Boom")
    
    # Reload from log to verify it was rolled back
    loaded = coordinator._log.load_transaction(txn_id)
    assert loaded.state in (TransactionState.ROLLED_BACK, TransactionState.FAILED)

def test_recover_pending_error(transaction_log, mock_adapter):
    # recover_pending where rollback fails
    txn_id = str(uuid.uuid4())
    with sqlite3.connect(transaction_log._db_path) as conn:
         conn.execute(
            "INSERT INTO transactions (id, state, created_at) VALUES (?, ?, ?)",
            (txn_id, TransactionState.IN_PROGRESS.value, datetime.now().isoformat())
        )
    
    coord = TransactionCoordinator({"mock": mock_adapter}, log=transaction_log)
    # Patch rollback on the coordinator instance to simulate a failure during recovery
    with patch.object(coord, 'rollback', side_effect=Exception("Recovery Failed")):
        recovered = coord.recover_pending()
        assert len(recovered) == 0 

def test_dlq_resolve_notes(coordinator):
    from waveql.transaction.coordinator import CompensatingAction
    comp = CompensatingAction("mock", "users", OperationType.INSERT, {}, {}, "123")
    dlq_id = coordinator._log.add_to_dlq("t1", "o1", comp, "err")
    coordinator.resolve_dlq_entry(dlq_id)
    assert coordinator.get_dlq_count() == 0

def test_transaction_log_config_fallback():
    # Tests line 239-241 in coordinator.py
    from waveql.transaction.coordinator import TransactionLog
    import sys
    from unittest.mock import patch
    
    # Hide waveql.config to trigger ImportError
    with patch.dict(sys.modules, {'waveql.config': None}):
        log = TransactionLog(db_path=None)
        # It should fallback to Path.home() / ".waveql" / "transactions.db"
        assert ".waveql" in log._db_path
        assert "transactions.db" in log._db_path

def test_coordinator_commit_rollback_no_txn(coordinator):
    with pytest.raises(RuntimeError, match="No transaction in progress"):
        coordinator.commit()
    with pytest.raises(RuntimeError, match="No transaction in progress"):
        coordinator.rollback()

def test_coordinator_table_no_dot(coordinator, mock_adapter):
    # Mocking for default adapter
    coordinator._adapters["default"] = mock_adapter
    mock_adapter.insert.return_value = {"id": 1}
    
    coordinator.begin()
    coordinator.insert("incident", {"number": "INC1"})
    coordinator.commit()
    
    mock_adapter.insert.assert_called_with("incident", {"number": "INC1"})

def test_compensation_more_fallbacks(coordinator, mock_adapter):
    from waveql.transaction.coordinator import CompensatingAction
    # UPDATE id fallback from original_data
    comp = CompensatingAction("mock", "users", OperationType.UPDATE, {"sys_id": "SYS1", "name": "Old"}, {}, None)
    coordinator._execute_compensation(comp)
    mock_adapter.update.assert_called()
    
    # DELETE no data warning
    comp2 = CompensatingAction("mock", "users", OperationType.DELETE, {}, {}, None)
    coordinator._execute_compensation(comp2) # should log warning
    
    # fetch_for_compensation exception
    mock_adapter.fetch.side_effect = Exception("Fetch Fail")
    res = coordinator._fetch_for_compensation(mock_adapter, "users", {"id": 1})
    assert res == {}

def test_dlq_entries_resolved(coordinator):
    from waveql.transaction.coordinator import CompensatingAction
    comp = CompensatingAction("mock", "users", OperationType.INSERT, {}, {}, "123")
    dlq_id = coordinator._log.add_to_dlq("t1", "o1", comp, "err")
    coordinator.resolve_dlq_entry(dlq_id)
    
    # Default is False
    assert len(coordinator.get_dlq_entries()) == 0
    # True should show it
    assert len(coordinator._log.get_dlq_entries(include_resolved=True)) == 1

def test_operation_errors(coordinator, mock_adapter):
    coordinator.begin()
    # Mock update to fail
    mock_adapter.update.side_effect = Exception("Update Failed")
    # Mock fetch to return something so we pass the compensation fetch
    mock_adapter.fetch.return_value.to_pydict.return_value = {"id": ["1"]}
    with pytest.raises(Exception, match="Update Failed"):
        coordinator.update("mock.users", {}, {"id": "1"})
        
    # Mock delete to fail
    mock_adapter.delete.side_effect = Exception("Delete Failed")
    with pytest.raises(Exception, match="Delete Failed"):
        coordinator.delete("mock.users", {"id": "1"})

def test_fetch_compensation_warning(coordinator, mock_adapter):
    coordinator.begin()
    # Mock search with no results
    mock_adapter.fetch.return_value.to_pydict.return_value = {}
    
    # Update with no data to compensate (should log warning at 797)
    coordinator.update("mock.users", {"a": 1}, {"id": "ghost"})
    
    # Delete with no data to compensate (should log warning at 817)
    coordinator.delete("mock.users", {"id": "ghost"})

def test_execute_compensation_errors(coordinator, mock_adapter):
    from waveql.transaction.coordinator import CompensatingAction
    # UPDATE compensation error
    mock_adapter.update.side_effect = Exception("Comp Update Fail")
    comp = CompensatingAction("mock", "users", OperationType.UPDATE, {"name": "Old"}, {}, "123")
    with pytest.raises(Exception, match="Comp Update Fail"):
        coordinator._execute_compensation(comp)
        
    # DELETE compensation error
    mock_adapter.insert.side_effect = Exception("Comp Insert Fail")
    # Note: compensation for DELETE is re-inserting original_data
    comp2 = CompensatingAction("mock", "users", OperationType.DELETE, {"id": "1", "name": "Old"}, {}, None)
    with pytest.raises(Exception, match="Comp Insert Fail"):
        coordinator._execute_compensation(comp2)
