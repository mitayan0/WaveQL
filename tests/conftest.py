
import sys
from unittest.mock import MagicMock
import pytest

# Aggressively mock duckdb to bypass broken installation in CI/Test env
if "duckdb" not in sys.modules:
    mock_duckdb = MagicMock()
    mock_conn = MagicMock()
    mock_duckdb.connect.return_value = mock_conn
    
    # Mock behavior for execute
    mock_conn.execute.return_value.fetchall.return_value = []
    
    sys.modules["duckdb"] = mock_duckdb
    sys.modules["_duckdb"] = MagicMock()
    
    # Also mock specific internal paths that might be causing issues
    sys.modules["duckdb.sqltypes"] = MagicMock()
    sys.modules["_duckdb._sqltypes"] = MagicMock()

@pytest.fixture(scope="session", autouse=True)
def mock_duckdb_session():
    """Ensure duckdb is mocked throughout the session."""
    # This fixture ensures the mock persists, though sys.modules patch above is the primary mechanism
    pass
