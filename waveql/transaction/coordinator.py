"""
WaveQL Transaction Coordinator - Saga Pattern Implementation

Provides best-effort atomic writes across multiple adapters using
the Saga pattern with compensating transactions.

Note: This is NOT true 2PC (which is impossible with REST APIs).
Instead, it provides:
- Transaction tracking with unique IDs
- Operation logging for audit trails
- Compensating transaction support for rollback
- Best-effort atomicity with clear failure semantics
- Retry policies for compensation with exponential backoff
- Dead letter queue for permanently failed compensations
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """Transaction lifecycle states."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"  # Partial failure, some compensations may have failed


class OperationType(Enum):
    """Types of write operations."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class InsertResult:
    """
    Standardized result from INSERT operations.
    
    Adapters should return this to enable proper compensation.
    """
    rows_affected: int
    record_id: Optional[str] = None  # Primary key of created record
    record_data: Optional[Dict[str, Any]] = None  # Full record if available
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "rows_affected": self.rows_affected,
            "record_id": self.record_id,
            "record_data": self.record_data,
        }
    
    @classmethod
    def from_adapter_result(cls, result: Any, values: Dict[str, Any]) -> "InsertResult":
        """
        Create InsertResult from various adapter return formats.
        
        Handles:
        - int (rows affected only)
        - dict with 'id', 'sys_id', 'Id', etc.
        - InsertResult directly
        """
        if isinstance(result, InsertResult):
            return result
        
        if isinstance(result, int):
            # Try to extract ID from values if provided
            record_id = None
            for key in ("id", "sys_id", "Id", "ID", "guid", "uuid"):
                if key in values:
                    record_id = str(values[key])
                    break
            return cls(rows_affected=result, record_id=record_id, record_data=values)
        
        if isinstance(result, dict):
            rows = result.get("rows_affected", 1)
            record_id = None
            for key in ("id", "sys_id", "Id", "ID", "record_id", "guid", "uuid"):
                if key in result:
                    record_id = str(result[key])
                    break
            return cls(rows_affected=rows, record_id=record_id, record_data=result)
        
        return cls(rows_affected=1, record_data=values)


@dataclass
class CompensatingAction:
    """
    A compensating action to undo a write operation.
    
    For INSERT → DELETE the created record
    For UPDATE → UPDATE with original values
    For DELETE → INSERT the deleted record (if we have it)
    """
    adapter_name: str
    table: str
    operation: OperationType
    original_data: Dict[str, Any]  # Data before the operation
    result_data: Dict[str, Any]    # Data after the operation (e.g., new ID)
    record_id: Optional[str] = None  # Primary key for compensation
    compensate_fn: Optional[Callable] = None  # Custom compensation function


@dataclass
class FailedCompensation:
    """
    Represents a compensation that failed and is in the dead letter queue.
    
    These require manual intervention to resolve.
    """
    id: str
    transaction_id: str
    operation_id: str
    adapter_name: str
    table: str
    compensation_data: Dict[str, Any]
    error: str
    attempts: int
    created_at: datetime
    last_attempt_at: Optional[datetime] = None


@dataclass
class TransactionOperation:
    """A single operation within a transaction."""
    id: str
    transaction_id: str
    adapter_name: str
    table: str
    operation: OperationType
    data: Dict[str, Any]
    executed_at: Optional[datetime] = None
    success: bool = False
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    compensation: Optional[CompensatingAction] = None


@dataclass
class Transaction:
    """
    Represents a distributed transaction across adapters.
    
    Uses the Saga pattern for best-effort atomicity.
    """
    id: str
    state: TransactionState = TransactionState.PENDING
    operations: List[TransactionOperation] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    
    def add_operation(
        self,
        adapter_name: str,
        table: str,
        operation: OperationType,
        data: Dict[str, Any],
    ) -> TransactionOperation:
        """Add an operation to this transaction."""
        op = TransactionOperation(
            id=str(uuid.uuid4()),
            transaction_id=self.id,
            adapter_name=adapter_name,
            table=table,
            operation=operation,
            data=data,
        )
        self.operations.append(op)
        return op


class TransactionLog:
    """
    Persistent transaction log using SQLite.
    
    Stores transaction state for durability and recovery.
    Includes dead letter queue for failed compensations.
    
    Database Location Priority:
        1. Explicit `db_path` parameter
        2. WAVEQL_TRANSACTION_DB environment variable
        3. Default: ~/.waveql/transactions.db
    
    Security Considerations:
        - The database contains operation data (INSERT/UPDATE values)
        - May contain sensitive information depending on your data
        - For production, consider:
          - Setting explicit path with restrictive permissions
          - Using `:memory:` for ephemeral transactions (no crash recovery)
          - Encrypting the database with SQLCipher (not built-in)
    
    Examples:
        # Use default location (~/.waveql/transactions.db)
        log = TransactionLog()
        
        # Explicit path
        log = TransactionLog(db_path="/secure/path/transactions.db")
        
        # In-memory (no persistence, no crash recovery)
        log = TransactionLog(db_path=":memory:")
        
        # Via environment variable
        # export WAVEQL_TRANSACTION_DB=/var/lib/waveql/transactions.db
        log = TransactionLog()  # Uses env var
    """
    
    def __init__(self, db_path: str = None):
        import os
        
        if db_path is None:
            # Check environment variable (backwards compatible)
            db_path = os.environ.get("WAVEQL_TRANSACTION_DB")
        
        if db_path is None:
            # Use centralized config
            try:
                from waveql.config import get_config
                db_path = str(get_config().transaction_db)
            except ImportError:
                # Fallback if config module not available
                db_path = str(Path.home() / ".waveql" / "transactions.db")
        
        self._db_path = db_path
        
        # Create parent directory (unless in-memory)
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self._init_db()
    
    def _init_db(self):
        """Initialize the transaction log database."""
        with sqlite3.connect(self._db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    error TEXT
                );
                
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    transaction_id TEXT NOT NULL,
                    adapter_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    data TEXT NOT NULL,
                    executed_at TEXT,
                    success INTEGER DEFAULT 0,
                    result TEXT,
                    error TEXT,
                    compensation_data TEXT,
                    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_ops_txn ON operations(transaction_id);
                
                -- Dead Letter Queue for failed compensations
                CREATE TABLE IF NOT EXISTS failed_compensations (
                    id TEXT PRIMARY KEY,
                    transaction_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    adapter_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    compensation_data TEXT NOT NULL,
                    error TEXT NOT NULL,
                    attempts INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_attempt_at TEXT,
                    resolved INTEGER DEFAULT 0,
                    resolved_at TEXT,
                    resolution_notes TEXT,
                    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_dlq_unresolved 
                    ON failed_compensations(resolved) WHERE resolved = 0;
            """)
    
    def save_transaction(self, txn: Transaction):
        """Persist transaction state."""
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO transactions 
                (id, state, created_at, completed_at, error)
                VALUES (?, ?, ?, ?, ?)
            """, (
                txn.id,
                txn.state.value,
                txn.created_at.isoformat(),
                txn.completed_at.isoformat() if txn.completed_at else None,
                txn.error,
            ))
            
            for op in txn.operations:
                compensation_data = None
                if op.compensation:
                    compensation_data = json.dumps({
                        "adapter_name": op.compensation.adapter_name,
                        "table": op.compensation.table,
                        "operation": op.compensation.operation.value,
                        "original_data": op.compensation.original_data,
                        "result_data": op.compensation.result_data,
                        "record_id": op.compensation.record_id,
                    })
                
                conn.execute("""
                    INSERT OR REPLACE INTO operations
                    (id, transaction_id, adapter_name, table_name, operation,
                     data, executed_at, success, result, error, compensation_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    op.id,
                    op.transaction_id,
                    op.adapter_name,
                    op.table,
                    op.operation.value,
                    json.dumps(op.data),
                    op.executed_at.isoformat() if op.executed_at else None,
                    1 if op.success else 0,
                    json.dumps(op.result) if op.result else None,
                    op.error,
                    compensation_data,
                ))
    
    def load_transaction(self, txn_id: str) -> Optional[Transaction]:
        """Load a transaction from the log."""
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM transactions WHERE id = ?", (txn_id,)
            ).fetchone()
            
            if not row:
                return None
            
            txn = Transaction(
                id=row["id"],
                state=TransactionState(row["state"]),
                created_at=datetime.fromisoformat(row["created_at"]),
                completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
                error=row["error"],
            )
            
            ops = conn.execute(
                "SELECT * FROM operations WHERE transaction_id = ? ORDER BY rowid",
                (txn_id,)
            ).fetchall()
            
            for op_row in ops:
                compensation = None
                if op_row["compensation_data"]:
                    comp_data = json.loads(op_row["compensation_data"])
                    compensation = CompensatingAction(
                        adapter_name=comp_data["adapter_name"],
                        table=comp_data["table"],
                        operation=OperationType(comp_data["operation"]),
                        original_data=comp_data["original_data"],
                        result_data=comp_data["result_data"],
                        record_id=comp_data.get("record_id"),
                    )
                
                txn.operations.append(TransactionOperation(
                    id=op_row["id"],
                    transaction_id=op_row["transaction_id"],
                    adapter_name=op_row["adapter_name"],
                    table=op_row["table_name"],
                    operation=OperationType(op_row["operation"]),
                    data=json.loads(op_row["data"]),
                    executed_at=datetime.fromisoformat(op_row["executed_at"]) if op_row["executed_at"] else None,
                    success=bool(op_row["success"]),
                    result=json.loads(op_row["result"]) if op_row["result"] else None,
                    error=op_row["error"],
                    compensation=compensation,
                ))
            
            return txn
    
    def get_pending_transactions(self) -> List[Transaction]:
        """Get transactions that may need recovery."""
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute("""
                SELECT id FROM transactions 
                WHERE state IN ('pending', 'in_progress')
            """).fetchall()
            
            return [self.load_transaction(row[0]) for row in rows if row]
    
    # =========================================================================
    # Dead Letter Queue (DLQ) Methods
    # =========================================================================
    
    def add_to_dlq(
        self,
        transaction_id: str,
        operation_id: str,
        compensation: CompensatingAction,
        error: str,
        attempts: int = 1,
    ) -> str:
        """Add a failed compensation to the dead letter queue."""
        dlq_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                INSERT INTO failed_compensations
                (id, transaction_id, operation_id, adapter_name, table_name,
                 compensation_data, error, attempts, created_at, last_attempt_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dlq_id,
                transaction_id,
                operation_id,
                compensation.adapter_name,
                compensation.table,
                json.dumps({
                    "operation": compensation.operation.value,
                    "original_data": compensation.original_data,
                    "result_data": compensation.result_data,
                    "record_id": compensation.record_id,
                }),
                error,
                attempts,
                now.isoformat(),
                now.isoformat(),
            ))
        
        logger.warning(f"Added compensation to DLQ: {dlq_id} (txn={transaction_id})")
        return dlq_id
    
    def get_dlq_entries(self, include_resolved: bool = False) -> List[FailedCompensation]:
        """Get all dead letter queue entries."""
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            if include_resolved:
                rows = conn.execute("SELECT * FROM failed_compensations").fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM failed_compensations WHERE resolved = 0"
                ).fetchall()
            
            return [
                FailedCompensation(
                    id=row["id"],
                    transaction_id=row["transaction_id"],
                    operation_id=row["operation_id"],
                    adapter_name=row["adapter_name"],
                    table=row["table_name"],
                    compensation_data=json.loads(row["compensation_data"]),
                    error=row["error"],
                    attempts=row["attempts"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    last_attempt_at=datetime.fromisoformat(row["last_attempt_at"]) if row["last_attempt_at"] else None,
                )
                for row in rows
            ]
    
    def update_dlq_attempt(self, dlq_id: str, error: str, attempts: int):
        """Update a DLQ entry after a retry attempt."""
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                UPDATE failed_compensations 
                SET attempts = ?, error = ?, last_attempt_at = ?
                WHERE id = ?
            """, (attempts, error, datetime.utcnow().isoformat(), dlq_id))
    
    def resolve_dlq_entry(self, dlq_id: str, notes: str = None):
        """Mark a DLQ entry as resolved (manually or automatically)."""
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                UPDATE failed_compensations 
                SET resolved = 1, resolved_at = ?, resolution_notes = ?
                WHERE id = ?
            """, (datetime.utcnow().isoformat(), notes, dlq_id))
        
        logger.info(f"Resolved DLQ entry: {dlq_id}")
    
    def get_dlq_count(self) -> int:
        """Get count of unresolved DLQ entries."""
        with sqlite3.connect(self._db_path) as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM failed_compensations WHERE resolved = 0"
            ).fetchone()
            return result[0] if result else 0


class CompensationRetryPolicy:
    """
    Retry policy for compensation operations.
    
    Uses exponential backoff with configurable parameters.
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        import random
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)
        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        return delay + jitter
    
    def should_retry(self, attempt: int) -> bool:
        """Check if we should retry after the given attempt."""
        return attempt < self.max_retries


class TransactionCoordinator:
    """
    Coordinates distributed transactions across WaveQL adapters.
    
    Implements the Saga pattern with compensating transactions for
    best-effort atomicity across REST APIs.
    
    Features:
    - Transaction tracking with unique IDs
    - Persistent logging for crash recovery
    - Retry policies for compensation with exponential backoff
    - Dead letter queue for permanently failed compensations
    
    Example:
        >>> from waveql import connect
        >>> conn = connect("servicenow://...")
        >>> conn.register_adapter("salesforce", sf_adapter)
        >>>
        >>> # Start a transaction
        >>> with conn.transaction() as txn:
        ...     txn.insert("servicenow.incident", {"short_description": "Test"})
        ...     txn.insert("salesforce.Case", {"Subject": "Test"})
        ...     # Both succeed or both are rolled back
    
    Limitations:
        - Not true ACID (REST APIs don't support it)
        - Compensation may fail (sent to DLQ for manual recovery)
        - No isolation guarantees between transactions
    """
    
    def __init__(
        self,
        adapters: Dict[str, "BaseAdapter"],
        log: TransactionLog = None,
        retry_policy: CompensationRetryPolicy = None,
    ):
        self._adapters = adapters
        self._log = log or TransactionLog()
        self._retry_policy = retry_policy or CompensationRetryPolicy()
        self._current_transaction: Optional[Transaction] = None
    
    def begin(self) -> Transaction:
        """Start a new transaction."""
        if self._current_transaction:
            raise RuntimeError("Transaction already in progress")
        
        txn = Transaction(id=str(uuid.uuid4()))
        txn.state = TransactionState.IN_PROGRESS
        self._current_transaction = txn
        self._log.save_transaction(txn)
        
        logger.info(f"Started transaction {txn.id}")
        return txn
    
    def insert(
        self,
        qualified_table: str,
        values: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Insert a record within the current transaction.
        
        Args:
            qualified_table: Schema-qualified table (e.g., "servicenow.incident")
            values: Column-value pairs
            
        Returns:
            Result including the new record ID (if available)
        """
        if not self._current_transaction:
            raise RuntimeError("No transaction in progress")
        
        adapter_name, table = self._parse_qualified_table(qualified_table)
        adapter = self._get_adapter(adapter_name)
        
        # Add operation to transaction
        op = self._current_transaction.add_operation(
            adapter_name=adapter_name,
            table=table,
            operation=OperationType.INSERT,
            data=values,
        )
        
        try:
            # Execute the insert
            op.executed_at = datetime.utcnow()
            raw_result = adapter.insert(table, values)
            
            # Standardize the result
            insert_result = InsertResult.from_adapter_result(raw_result, values)
            
            # Store result
            op.success = True
            op.result = insert_result.to_dict()
            
            # Create compensation action (DELETE the inserted record)
            op.compensation = CompensatingAction(
                adapter_name=adapter_name,
                table=table,
                operation=OperationType.INSERT,
                original_data={},  # Nothing existed before
                result_data=op.result,
                record_id=insert_result.record_id,
            )
            
            self._log.save_transaction(self._current_transaction)
            logger.debug(f"Transaction {self._current_transaction.id}: INSERT into {qualified_table} succeeded (id={insert_result.record_id})")
            
            return op.result
            
        except Exception as e:
            op.error = str(e)
            logger.error(f"Transaction {self._current_transaction.id}: INSERT into {qualified_table} failed: {e}")
            raise
    
    def update(
        self,
        qualified_table: str,
        values: Dict[str, Any],
        where: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Update records within the current transaction.
        
        Args:
            qualified_table: Schema-qualified table
            values: New values
            where: Filter conditions (simple equality only for now)
            
        Returns:
            Update result
        """
        if not self._current_transaction:
            raise RuntimeError("No transaction in progress")
        
        adapter_name, table = self._parse_qualified_table(qualified_table)
        adapter = self._get_adapter(adapter_name)
        
        # First, fetch the original data for compensation
        original_data = self._fetch_for_compensation(adapter, table, where)
        
        # Extract record ID from where clause
        record_id = None
        for key in ("id", "sys_id", "Id", "ID"):
            if key in where:
                record_id = str(where[key])
                break
        
        op = self._current_transaction.add_operation(
            adapter_name=adapter_name,
            table=table,
            operation=OperationType.UPDATE,
            data={"values": values, "where": where},
        )
        
        try:
            from waveql.query_planner import Predicate
            predicates = [
                Predicate(column=k, operator="=", value=v)
                for k, v in where.items()
            ]
            
            op.executed_at = datetime.utcnow()
            result = adapter.update(table, values, predicates)
            
            op.success = True
            op.result = {"rows_affected": result}
            
            # Compensation: restore original values
            op.compensation = CompensatingAction(
                adapter_name=adapter_name,
                table=table,
                operation=OperationType.UPDATE,
                original_data=original_data,
                result_data=values,
                record_id=record_id,
            )
            
            self._log.save_transaction(self._current_transaction)
            return op.result
            
        except Exception as e:
            op.error = str(e)
            raise
    
    def delete(
        self,
        qualified_table: str,
        where: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Delete records within the current transaction.
        
        Args:
            qualified_table: Schema-qualified table
            where: Filter conditions
            
        Returns:
            Delete result
        """
        if not self._current_transaction:
            raise RuntimeError("No transaction in progress")
        
        adapter_name, table = self._parse_qualified_table(qualified_table)
        adapter = self._get_adapter(adapter_name)
        
        # Fetch data before delete for compensation
        original_data = self._fetch_for_compensation(adapter, table, where)
        
        op = self._current_transaction.add_operation(
            adapter_name=adapter_name,
            table=table,
            operation=OperationType.DELETE,
            data={"where": where},
        )
        
        try:
            from waveql.query_planner import Predicate
            predicates = [
                Predicate(column=k, operator="=", value=v)
                for k, v in where.items()
            ]
            
            op.executed_at = datetime.utcnow()
            result = adapter.delete(table, predicates)
            
            op.success = True
            op.result = {"rows_affected": result}
            
            # Compensation: re-insert the deleted data
            op.compensation = CompensatingAction(
                adapter_name=adapter_name,
                table=table,
                operation=OperationType.DELETE,
                original_data=original_data,
                result_data={},
            )
            
            self._log.save_transaction(self._current_transaction)
            return op.result
            
        except Exception as e:
            op.error = str(e)
            raise
    
    def commit(self) -> Transaction:
        """
        Commit the current transaction.
        
        For the Saga pattern, commit simply marks the transaction as complete.
        All operations have already been executed.
        """
        if not self._current_transaction:
            raise RuntimeError("No transaction in progress")
        
        txn = self._current_transaction
        txn.state = TransactionState.COMMITTED
        txn.completed_at = datetime.utcnow()
        
        self._log.save_transaction(txn)
        self._current_transaction = None
        
        logger.info(f"Committed transaction {txn.id} with {len(txn.operations)} operations")
        return txn
    
    def rollback(self) -> Transaction:
        """
        Rollback the current transaction by executing compensating actions.
        
        Compensations are executed in reverse order with retry policy.
        Failed compensations after all retries are sent to the dead letter queue.
        """
        if not self._current_transaction:
            raise RuntimeError("No transaction in progress")
        
        txn = self._current_transaction
        logger.warning(f"Rolling back transaction {txn.id}")
        
        # Execute compensations in reverse order
        compensation_errors = []
        dlq_entries = []
        
        for op in reversed(txn.operations):
            if op.success and op.compensation:
                success, error = self._execute_compensation_with_retry(
                    op.id, op.compensation
                )
                
                if not success:
                    compensation_errors.append(f"Failed to compensate {op.id}: {error}")
                    dlq_entries.append((op.id, op.compensation, error))
        
        # Add failed compensations to DLQ
        for op_id, compensation, error in dlq_entries:
            self._log.add_to_dlq(
                transaction_id=txn.id,
                operation_id=op_id,
                compensation=compensation,
                error=error,
                attempts=self._retry_policy.max_retries + 1,
            )
        
        if compensation_errors:
            txn.state = TransactionState.FAILED
            txn.error = f"{len(compensation_errors)} compensations failed (sent to DLQ)"
        else:
            txn.state = TransactionState.ROLLED_BACK
        
        txn.completed_at = datetime.utcnow()
        self._log.save_transaction(txn)
        self._current_transaction = None
        
        return txn
    
    def _execute_compensation_with_retry(
        self,
        operation_id: str,
        compensation: CompensatingAction,
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute compensation with retry policy.
        
        Returns:
            Tuple of (success, error_message)
        """
        last_error = None
        
        for attempt in range(self._retry_policy.max_retries + 1):
            try:
                self._execute_compensation(compensation)
                logger.debug(f"Compensated operation {operation_id} (attempt {attempt + 1})")
                return True, None
                
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Compensation attempt {attempt + 1}/{self._retry_policy.max_retries + 1} "
                    f"failed for {operation_id}: {e}"
                )
                
                if self._retry_policy.should_retry(attempt):
                    delay = self._retry_policy.calculate_delay(attempt)
                    logger.debug(f"Retrying in {delay:.2f}s...")
                    time.sleep(delay)
        
        logger.error(f"All compensation attempts failed for {operation_id}: {last_error}")
        return False, last_error
    
    def _execute_compensation(self, compensation: CompensatingAction):
        """Execute a single compensating action."""
        adapter = self._get_adapter(compensation.adapter_name)
        
        if compensation.operation == OperationType.INSERT:
            # Undo INSERT by deleting the created record
            record_id = compensation.record_id
            
            if not record_id:
                # Fallback: try to find ID in result_data
                for key in ("record_id", "sys_id", "id", "Id", "ID"):
                    if key in compensation.result_data:
                        record_id = compensation.result_data[key]
                        break
                    if "record_data" in compensation.result_data:
                        record_data = compensation.result_data["record_data"]
                        if isinstance(record_data, dict) and key in record_data:
                            record_id = record_data[key]
                            break
            
            if not record_id:
                raise ValueError("Cannot compensate INSERT: no record ID found")
            
            from waveql.query_planner import Predicate
            
            # Try common ID column names
            for id_column in ("sys_id", "id", "Id", "ID"):
                try:
                    adapter.delete(compensation.table, [
                        Predicate(column=id_column, operator="=", value=record_id)
                    ])
                    return
                except Exception:
                    continue
            
            raise ValueError(f"Could not delete record {record_id} from {compensation.table}")
        
        elif compensation.operation == OperationType.UPDATE:
            # Undo UPDATE by restoring original values
            if not compensation.original_data:
                logger.warning("No original data for UPDATE compensation")
                return
            
            record_id = compensation.record_id
            if not record_id:
                for key in ("id", "sys_id", "Id", "ID"):
                    if key in compensation.original_data:
                        record_id = compensation.original_data[key]
                        break
            
            if not record_id:
                raise ValueError("Cannot compensate UPDATE: no record ID found")
            
            from waveql.query_planner import Predicate
            
            # Determine ID column
            id_column = "id"
            for key in ("sys_id", "id", "Id", "ID"):
                if key in compensation.original_data:
                    id_column = key
                    break
            
            adapter.update(
                compensation.table,
                compensation.original_data,
                [Predicate(column=id_column, operator="=", value=record_id)]
            )
        
        elif compensation.operation == OperationType.DELETE:
            # Undo DELETE by re-inserting
            if not compensation.original_data:
                logger.warning("No original data for DELETE compensation")
                return
            
            adapter.insert(compensation.table, compensation.original_data)
    
    def _fetch_for_compensation(
        self,
        adapter: "BaseAdapter",
        table: str,
        where: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Fetch current data before modification for compensation."""
        try:
            from waveql.query_planner import Predicate
            predicates = [
                Predicate(column=k, operator="=", value=v)
                for k, v in where.items()
            ]
            result = adapter.fetch(table, predicates=predicates, limit=1)
            if result and len(result) > 0:
                # Convert Arrow table to dict properly
                py_dict = result.to_pydict()
                if py_dict:
                    # Get first row
                    return {k: v[0] if v else None for k, v in py_dict.items()}
        except Exception as e:
            logger.warning(f"Failed to fetch data for compensation: {e}")
        return {}
    
    def _parse_qualified_table(self, qualified_table: str) -> Tuple[str, str]:
        """Parse 'adapter.table' into (adapter_name, table_name)."""
        if "." in qualified_table:
            parts = qualified_table.split(".", 1)
            return parts[0], parts[1]
        return "default", qualified_table
    
    def _get_adapter(self, name: str) -> "BaseAdapter":
        """Get adapter by name."""
        if name not in self._adapters:
            raise ValueError(f"Unknown adapter: {name}")
        return self._adapters[name]
    
    @contextmanager
    def transaction(self):
        """
        Context manager for transactions.
        
        Example:
            with coordinator.transaction() as txn:
                txn.insert("servicenow.incident", {...})
                # Auto-commits on success, auto-rollback on exception
        """
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise
    
    def recover_pending(self) -> List[Transaction]:
        """
        Recover pending transactions after a crash.
        
        This should be called at startup to handle any transactions
        that were in progress when the system crashed.
        
        Returns:
            List of recovered transactions
        """
        pending = self._log.get_pending_transactions()
        recovered = []
        
        for txn in pending:
            logger.warning(f"Recovering transaction {txn.id} in state {txn.state}")
            
            # For pending transactions, we roll back
            # For in-progress, we check what was completed and compensate
            self._current_transaction = txn
            try:
                self.rollback()
                recovered.append(txn)
            except Exception as e:
                logger.error(f"Failed to recover transaction {txn.id}: {e}")
        
        return recovered
    
    # =========================================================================
    # Dead Letter Queue Management
    # =========================================================================
    
    def get_dlq_entries(self) -> List[FailedCompensation]:
        """Get all unresolved dead letter queue entries."""
        return self._log.get_dlq_entries()
    
    def get_dlq_count(self) -> int:
        """Get count of unresolved DLQ entries."""
        return self._log.get_dlq_count()
    
    def retry_dlq_entry(self, dlq_id: str) -> bool:
        """
        Retry a single dead letter queue entry.
        
        Returns:
            True if retry succeeded, False otherwise
        """
        entries = self._log.get_dlq_entries()
        entry = next((e for e in entries if e.id == dlq_id), None)
        
        if not entry:
            raise ValueError(f"DLQ entry not found: {dlq_id}")
        
        # Reconstruct compensation from stored data
        compensation = CompensatingAction(
            adapter_name=entry.adapter_name,
            table=entry.table,
            operation=OperationType(entry.compensation_data["operation"]),
            original_data=entry.compensation_data.get("original_data", {}),
            result_data=entry.compensation_data.get("result_data", {}),
            record_id=entry.compensation_data.get("record_id"),
        )
        
        try:
            self._execute_compensation(compensation)
            self._log.resolve_dlq_entry(dlq_id, "Retried successfully")
            return True
        except Exception as e:
            self._log.update_dlq_attempt(dlq_id, str(e), entry.attempts + 1)
            logger.error(f"DLQ retry failed for {dlq_id}: {e}")
            return False
    
    def resolve_dlq_entry(self, dlq_id: str, notes: str = None):
        """
        Manually resolve a DLQ entry (mark as handled).
        
        Use this when you've manually fixed the issue or determined
        it doesn't need compensation.
        """
        self._log.resolve_dlq_entry(dlq_id, notes or "Manually resolved")
