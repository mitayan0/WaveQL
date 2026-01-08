
import sys
import os
import pytest
from waveql.adapters.singer import SingerAdapter

@pytest.fixture
def mock_tap_command():
    # Return the command to run the mock tap
    tap_path = os.path.join(os.path.dirname(__file__), "mock_tap.py")
    return f"{sys.executable} {tap_path}"

def test_discovery(mock_tap_command):
    adapter = SingerAdapter(tap_command=mock_tap_command)
    tables = adapter.list_tables()
    assert "users" in tables
    
    cols = adapter.get_schema("users")
    assert any(c.name == "id" for c in cols)
    assert any(c.name == "name" for c in cols)

def test_fetch(mock_tap_command):
    adapter = SingerAdapter(tap_command=mock_tap_command)
    table = adapter.fetch("users")
    assert len(table) == 2
    
    data = table.to_pylist()
    assert data[0]["name"] == "Alice"
    assert data[1]["name"] == "Bob"

def test_verify_connection(mock_tap_command):
    # Just check if it runs without crashing
    adapter = SingerAdapter(tap_command=mock_tap_command)
    # verify_connection runs tap --version, mock_tap.py ignores unknown args and exits 0 (if explicit in script?)
    # mock_tap.py runs sync() if not --discover.
    # It prints JSONs then exits. returncode 0.
    assert adapter.verify_connection() is True
