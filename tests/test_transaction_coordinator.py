"""
Tests for the Transaction Coordinator (Saga Pattern).

These tests verify the best-effort atomic writes implementation including:
- Basic transaction operations
- Retry policies for compensation
- Dead letter queue functionality
- InsertResult standardization
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call
import tempfile
import time

from waveql.transaction import (
    Transaction,
    TransactionCoordinator,
    TransactionLog,
    TransactionOperation,
    TransactionState,
    OperationType,
    CompensatingAction,
    InsertResult,
    FailedCompensation,
    CompensationRetryPolicy,
)


class TestInsertResult:
    """Tests for standardized INSERT result handling."""
    
    def test_from_int_result(self):
        """Test creating InsertResult from integer (rows affected)."""
        result = InsertResult.from_adapter_result(1, {"name": "Test"})
        
        assert result.rows_affected == 1
        assert result.record_data == {"name": "Test"}
    
    def test_from_int_with_id_in_values(self):
        """Test extracting ID from values when adapter returns int."""
        result = InsertResult.from_adapter_result(1, {"sys_id": "abc123", "name": "Test"})
        
        assert result.rows_affected == 1
        assert result.record_id == "abc123"
    
    def test_from_dict_result(self):
        """Test creating InsertResult from dict response."""
        adapter_response = {
            "id": "xyz789",
            "rows_affected": 1,
            "created": True,
        }
        result = InsertResult.from_adapter_result(adapter_response, {})
        
        assert result.rows_affected == 1
        assert result.record_id == "xyz789"
    
    def test_from_insert_result_passthrough(self):
        """Test that InsertResult passes through unchanged."""
        original = InsertResult(rows_affected=1, record_id="test123")
        result = InsertResult.from_adapter_result(original, {})
        
        assert result is original
    
    def test_to_dict(self):
        """Test serialization to dict."""
        result = InsertResult(
            rows_affected=1,
            record_id="abc123",
            record_data={"name": "Test"}
        )
        
        d = result.to_dict()
        assert d["rows_affected"] == 1
        assert d["record_id"] == "abc123"
        assert d["record_data"] == {"name": "Test"}


class TestCompensationRetryPolicy:
    """Tests for retry policy configuration."""
    
    def test_default_policy(self):
        """Test default retry policy values."""
        policy = CompensationRetryPolicy()
        
        assert policy.max_retries == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 30.0
    
    def test_should_retry(self):
        """Test retry decision logic."""
        policy = CompensationRetryPolicy(max_retries=3)
        
        assert policy.should_retry(0) is True
        assert policy.should_retry(1) is True
        assert policy.should_retry(2) is True
        assert policy.should_retry(3) is False
    
    def test_calculate_delay_exponential(self):
        """Test exponential backoff calculation."""
        policy = CompensationRetryPolicy(base_delay=1.0, exponential_base=2.0)
        
        # Delays should increase exponentially (with jitter)
        delay_0 = policy.calculate_delay(0)
        delay_1 = policy.calculate_delay(1)
        delay_2 = policy.calculate_delay(2)
        
        # Base values: 1, 2, 4 (±25% jitter)
        assert 0.75 <= delay_0 <= 1.25
        assert 1.5 <= delay_1 <= 2.5
        assert 3.0 <= delay_2 <= 5.0
    
    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay."""
        policy = CompensationRetryPolicy(base_delay=1.0, max_delay=5.0)
        
        # Attempt 10 would be 1 * 2^10 = 1024, but capped at 5
        delay = policy.calculate_delay(10)
        assert delay <= 5.0 * 1.25  # max_delay + jitter


class TestTransactionLog:
    """Tests for persistent transaction logging."""
    
    def test_save_and_load_transaction(self, tmp_path):
        """Test transaction persistence."""
        db_path = str(tmp_path / "test_txn.db")
        log = TransactionLog(db_path)
        
        # Create a transaction with operations
        txn = Transaction(id="txn-123")
        txn.state = TransactionState.IN_PROGRESS
        
        op = txn.add_operation(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            data={"short_description": "Test"},
        )
        op.success = True
        op.executed_at = datetime.utcnow()
        op.result = {"rows_affected": 1, "record_id": "abc123"}
        op.compensation = CompensatingAction(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            original_data={},
            result_data={"record_id": "abc123"},
            record_id="abc123",
        )
        
        # Save
        log.save_transaction(txn)
        
        # Load
        loaded = log.load_transaction("txn-123")
        
        assert loaded is not None
        assert loaded.id == "txn-123"
        assert loaded.state == TransactionState.IN_PROGRESS
        assert len(loaded.operations) == 1
        assert loaded.operations[0].adapter_name == "servicenow"
        assert loaded.operations[0].success is True
        assert loaded.operations[0].compensation is not None
        assert loaded.operations[0].compensation.record_id == "abc123"
    
    def test_get_pending_transactions(self, tmp_path):
        """Test recovery of pending transactions."""
        db_path = str(tmp_path / "test_txn.db")
        log = TransactionLog(db_path)
        
        # Create various transactions
        txn1 = Transaction(id="pending-1")
        txn1.state = TransactionState.PENDING
        log.save_transaction(txn1)
        
        txn2 = Transaction(id="in-progress-1")
        txn2.state = TransactionState.IN_PROGRESS
        log.save_transaction(txn2)
        
        txn3 = Transaction(id="committed-1")
        txn3.state = TransactionState.COMMITTED
        log.save_transaction(txn3)
        
        # Get pending
        pending = log.get_pending_transactions()
        
        assert len(pending) == 2
        ids = {t.id for t in pending}
        assert "pending-1" in ids
        assert "in-progress-1" in ids
        assert "committed-1" not in ids


class TestDeadLetterQueue:
    """Tests for the Dead Letter Queue (DLQ)."""
    
    def test_add_to_dlq(self, tmp_path):
        """Test adding failed compensation to DLQ."""
        db_path = str(tmp_path / "test_txn.db")
        log = TransactionLog(db_path)
        
        compensation = CompensatingAction(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            original_data={},
            result_data={"record_id": "abc123"},
            record_id="abc123",
        )
        
        dlq_id = log.add_to_dlq(
            transaction_id="txn-123",
            operation_id="op-456",
            compensation=compensation,
            error="Network timeout",
            attempts=3,
        )
        
        assert dlq_id is not None
        
        # Verify it's in the queue
        entries = log.get_dlq_entries()
        assert len(entries) == 1
        assert entries[0].id == dlq_id
        assert entries[0].error == "Network timeout"
        assert entries[0].attempts == 3
    
    def test_get_dlq_count(self, tmp_path):
        """Test counting DLQ entries."""
        db_path = str(tmp_path / "test_txn.db")
        log = TransactionLog(db_path)
        
        assert log.get_dlq_count() == 0
        
        compensation = CompensatingAction(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            original_data={},
            result_data={},
        )
        
        log.add_to_dlq("txn-1", "op-1", compensation, "Error 1")
        log.add_to_dlq("txn-2", "op-2", compensation, "Error 2")
        
        assert log.get_dlq_count() == 2
    
    def test_resolve_dlq_entry(self, tmp_path):
        """Test resolving a DLQ entry."""
        db_path = str(tmp_path / "test_txn.db")
        log = TransactionLog(db_path)
        
        compensation = CompensatingAction(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            original_data={},
            result_data={},
        )
        
        dlq_id = log.add_to_dlq("txn-1", "op-1", compensation, "Error")
        
        assert log.get_dlq_count() == 1
        
        log.resolve_dlq_entry(dlq_id, "Fixed manually")
        
        assert log.get_dlq_count() == 0
    
    def test_update_dlq_attempt(self, tmp_path):
        """Test updating DLQ entry after retry attempt."""
        db_path = str(tmp_path / "test_txn.db")
        log = TransactionLog(db_path)
        
        compensation = CompensatingAction(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            original_data={},
            result_data={},
        )
        
        dlq_id = log.add_to_dlq("txn-1", "op-1", compensation, "Initial error", attempts=1)
        
        log.update_dlq_attempt(dlq_id, "New error", attempts=2)
        
        entries = log.get_dlq_entries()
        assert len(entries) == 1
        assert entries[0].attempts == 2
        assert entries[0].error == "New error"


class TestTransactionCoordinator:
    """Tests for the transaction coordinator."""
    
    @pytest.fixture
    def mock_adapters(self):
        """Create mock adapters for testing."""
        servicenow = MagicMock()
        servicenow.insert.return_value = 1
        servicenow.update.return_value = 1
        servicenow.delete.return_value = 1
        servicenow.fetch.return_value = MagicMock(
            column_names=["sys_id", "name"],
            to_pydict=lambda: {"sys_id": ["abc123"], "name": ["Original"]}
        )
        
        salesforce = MagicMock()
        salesforce.insert.return_value = {"id": "001xxx", "success": True}
        salesforce.update.return_value = 1
        salesforce.delete.return_value = 1
        salesforce.fetch.return_value = MagicMock(
            column_names=["Id", "Name"],
            to_pydict=lambda: {"Id": ["001xxx"], "Name": ["Original"]}
        )
        
        return {
            "servicenow": servicenow,
            "salesforce": salesforce,
            "default": servicenow,
        }
    
    @pytest.fixture
    def coordinator(self, mock_adapters, tmp_path):
        """Create a coordinator with mock adapters."""
        db_path = str(tmp_path / "txn.db")
        log = TransactionLog(db_path)
        # Use fast retry policy for tests
        retry_policy = CompensationRetryPolicy(max_retries=2, base_delay=0.01, max_delay=0.1)
        return TransactionCoordinator(adapters=mock_adapters, log=log, retry_policy=retry_policy)
    
    def test_begin_transaction(self, coordinator):
        """Test starting a transaction."""
        txn = coordinator.begin()
        
        assert txn is not None
        assert txn.state == TransactionState.IN_PROGRESS
        assert len(txn.id) > 0
    
    def test_insert_operation(self, coordinator, mock_adapters):
        """Test insert within transaction."""
        coordinator.begin()
        
        result = coordinator.insert(
            "servicenow.incident",
            {"short_description": "Test incident"}
        )
        
        assert result["rows_affected"] == 1
        mock_adapters["servicenow"].insert.assert_called_once_with(
            "incident",
            {"short_description": "Test incident"}
        )
    
    def test_insert_captures_record_id(self, coordinator, mock_adapters):
        """Test that INSERT captures record ID from adapter response."""
        coordinator.begin()
        
        # Salesforce returns dict with ID
        result = coordinator.insert(
            "salesforce.Account",
            {"Name": "Test Account"}
        )
        
        assert result["record_id"] == "001xxx"
    
    def test_commit_transaction(self, coordinator):
        """Test committing a transaction."""
        coordinator.begin()
        coordinator.insert("servicenow.incident", {"short_description": "Test"})
        
        txn = coordinator.commit()
        
        assert txn.state == TransactionState.COMMITTED
        assert txn.completed_at is not None
    
    def test_rollback_with_retry(self, coordinator, mock_adapters):
        """Test that rollback retries failed compensations."""
        # Make delete fail twice then succeed
        mock_adapters["servicenow"].delete.side_effect = [
            Exception("Network error"),
            Exception("Timeout"),
            None,  # Success on third try
        ]
        
        coordinator.begin()
        coordinator.insert("servicenow.incident", {"sys_id": "test123", "short_description": "Test"})
        
        txn = coordinator.rollback()
        
        # Should have retried and succeeded
        assert txn.state == TransactionState.ROLLED_BACK
        assert mock_adapters["servicenow"].delete.call_count == 3
    
    def test_rollback_sends_to_dlq_after_retries(self, coordinator, mock_adapters):
        """Test that failed compensations go to DLQ after all retries."""
        # Make delete always fail
        mock_adapters["servicenow"].delete.side_effect = Exception("Permanent failure")
        
        coordinator.begin()
        coordinator.insert("servicenow.incident", {"sys_id": "test123", "short_description": "Test"})
        
        txn = coordinator.rollback()
        
        # Should be FAILED and have DLQ entry
        assert txn.state == TransactionState.FAILED
        assert coordinator.get_dlq_count() == 1
        
        entries = coordinator.get_dlq_entries()
        assert len(entries) == 1
        assert "incident" in entries[0].error  # Error contains table name
    
    def test_context_manager_commits_on_success(self, coordinator, mock_adapters):
        """Test context manager auto-commits."""
        with coordinator.transaction():
            coordinator.insert("servicenow.incident", {"short_description": "Test"})
        
        # Should have committed (no current transaction)
        assert coordinator._current_transaction is None
    
    def test_context_manager_rollbacks_on_error(self, coordinator, mock_adapters):
        """Test context manager auto-rollback on exception."""
        mock_adapters["salesforce"].insert.side_effect = Exception("API Error")
        
        with pytest.raises(Exception, match="API Error"):
            with coordinator.transaction():
                coordinator.insert("servicenow.incident", {"short_description": "Test 1"})
                coordinator.insert("salesforce.Case", {"Subject": "Test 2"})  # This fails
        
        # Should have rolled back
        assert coordinator._current_transaction is None
    
    def test_retry_dlq_entry_success(self, coordinator, mock_adapters):
        """Test retrying a DLQ entry successfully."""
        # Setup: create a failed compensation
        mock_adapters["servicenow"].delete.side_effect = Exception("Temporary failure")
        
        coordinator.begin()
        coordinator.insert("servicenow.incident", {"sys_id": "test123"})
        coordinator.rollback()
        
        # Verify DLQ has entry
        assert coordinator.get_dlq_count() == 1
        entries = coordinator.get_dlq_entries()
        dlq_id = entries[0].id
        
        # Now make delete succeed
        mock_adapters["servicenow"].delete.side_effect = None
        mock_adapters["servicenow"].delete.return_value = 1
        
        # Retry
        success = coordinator.retry_dlq_entry(dlq_id)
        
        assert success is True
        assert coordinator.get_dlq_count() == 0
    
    def test_resolve_dlq_entry_manually(self, coordinator, mock_adapters):
        """Test manually resolving a DLQ entry."""
        mock_adapters["servicenow"].delete.side_effect = Exception("Failure")
        
        coordinator.begin()
        coordinator.insert("servicenow.incident", {"sys_id": "test123"})
        coordinator.rollback()
        
        entries = coordinator.get_dlq_entries()
        dlq_id = entries[0].id
        
        # Manually resolve
        coordinator.resolve_dlq_entry(dlq_id, "Fixed via ServiceNow UI")
        
        assert coordinator.get_dlq_count() == 0


class TestTransactionModels:
    """Tests for transaction data models."""
    
    def test_transaction_add_operation(self):
        """Test adding operations to a transaction."""
        txn = Transaction(id="test-txn")
        
        op = txn.add_operation(
            adapter_name="servicenow",
            table="incident",
            operation=OperationType.INSERT,
            data={"short_description": "Test"},
        )
        
        assert len(txn.operations) == 1
        assert op.transaction_id == "test-txn"
        assert op.adapter_name == "servicenow"
        assert op.table == "incident"
        assert op.operation == OperationType.INSERT
    
    def test_operation_type_values(self):
        """Test operation type enum values."""
        assert OperationType.INSERT.value == "insert"
        assert OperationType.UPDATE.value == "update"
        assert OperationType.DELETE.value == "delete"
    
    def test_transaction_state_values(self):
        """Test transaction state enum values."""
        assert TransactionState.PENDING.value == "pending"
        assert TransactionState.IN_PROGRESS.value == "in_progress"
        assert TransactionState.COMMITTED.value == "committed"
        assert TransactionState.ROLLED_BACK.value == "rolled_back"
        assert TransactionState.FAILED.value == "failed"


class TestWaveQLConnectionTransactions:
    """Tests for transaction integration with WaveQLConnection."""
    
    def test_connection_transaction_method(self):
        """Test that WaveQLConnection.transaction() works."""
        from waveql import connect
        
        # Create a mock connection (no real adapter)
        with patch('waveql.connection.WaveQLConnection._init_default_adapter'):
            conn = connect()
            
            # Mock the adapters
            mock_adapter = MagicMock()
            mock_adapter.insert.return_value = 1
            conn._adapters = {"default": mock_adapter, "servicenow": mock_adapter}
            
            # Use transaction
            with conn.transaction() as txn:
                txn.insert("servicenow.incident", {"short_description": "Test"})
            
            # Should have called insert
            mock_adapter.insert.assert_called()
