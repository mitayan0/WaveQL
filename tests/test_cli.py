
import sys
import pytest
from unittest.mock import patch, MagicMock
import pyarrow as pa
from waveql.cli import main

def test_cli_help(capsys):
    """Test that --help prints usage information."""
    with patch.object(sys, 'argv', ['waveql', '--help']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 0
    
    captured = capsys.readouterr()
    assert "WaveQL CLI" in captured.out
    assert "usage:" in captured.out

@patch("waveql.connect")
def test_cli_select_query(mock_connect, capsys):
    """Test executing a simple SELECT query via CLI."""
    # Mock the connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock query result
    data = pa.Table.from_pydict({"id": [1], "name": ["test"]})
    mock_cursor.to_arrow.return_value = data
    
    with patch.object(sys, 'argv', ['waveql', 'SELECT * FROM test']):
        main()
        
    mock_connect.assert_called_with(None)
    mock_cursor.execute.assert_called_with('SELECT * FROM test')
    
    captured = capsys.readouterr()
    assert "id" in captured.out
    assert "1" in captured.out
    assert "test" in captured.out

@patch("waveql.connect")
def test_cli_connection_string(mock_connect, capsys):
    """Test CLI with a specific connection string."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.to_arrow.return_value = pa.Table.from_pydict({"status": ["ok"]})
    
    conn_str = "servicenow://instance.service-now.com"
    with patch.object(sys, 'argv', ['waveql', '--connection', conn_str, 'SELECT 1']):
        main()
        
    mock_connect.assert_called_with(conn_str)

@patch("waveql.connect")
def test_cli_explain(mock_connect, capsys):
    """Test that --explain modifies the query."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.to_arrow.return_value = pa.Table.from_pydict({"plan": ["SCAN"]})
    
    with patch.object(sys, 'argv', ['waveql', '--explain', 'SELECT * FROM table']):
        main()
        
    mock_cursor.execute.assert_called_with('EXPLAIN SELECT * FROM table')

@patch("waveql.connect")
def test_cli_hybrid_hint(mock_connect, capsys):
    """Test that --hybrid adds the optimizer hint."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.to_arrow.return_value = pa.Table.from_pydict({"count": [1]})
    
    with patch.object(sys, 'argv', ['waveql', '--hybrid', 'SELECT * FROM table']):
        main()
        
    mock_cursor.execute.assert_called_with('/*+ HYBRID */ SELECT * FROM table')
