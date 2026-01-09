"""
WaveQL Transaction Module

Provides best-effort atomic writes across multiple adapters using
the Saga pattern with compensating transactions.

Features:
- Transaction tracking with unique IDs
- Persistent logging for crash recovery
- Retry policies for compensation with exponential backoff
- Dead letter queue for permanently failed compensations
"""

from waveql.transaction.coordinator import (
    # Core classes
    Transaction,
    TransactionCoordinator,
    TransactionLog,
    TransactionOperation,
    # Enums
    TransactionState,
    OperationType,
    # Result types
    InsertResult,
    CompensatingAction,
    FailedCompensation,
    # Configuration
    CompensationRetryPolicy,
)

__all__ = [
    # Core classes
    "Transaction",
    "TransactionCoordinator",
    "TransactionLog",
    "TransactionOperation",
    # Enums
    "TransactionState",
    "OperationType",
    # Result types
    "InsertResult",
    "CompensatingAction",
    "FailedCompensation",
    # Configuration
    "CompensationRetryPolicy",
]
