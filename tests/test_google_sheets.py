"""
Tests for Google Sheets Adapter.
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch
import os

from waveql.adapters.google_sheets import (
    GoogleSheetsAdapter,
    GoogleSheetsCredentials,
)


class TestGoogleSheetsCredentials:
    """Tests for GoogleSheetsCredentials."""
    
    def test_from_env(self, monkeypatch):
        """Test loading credentials from environment."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
        monkeypatch.setenv("GOOGLE_SHEETS_API_KEY", "test_api_key")
        
        creds = GoogleSheetsCredentials.from_env()
        
        assert creds.service_account_json == "/path/to/creds.json"
        assert creds.api_key == "test_api_key"
    
    def test_default_values(self):
        """Test default credential values."""
        creds = GoogleSheetsCredentials()
        
        assert creds.service_account_json is None
        assert creds.oauth_credentials_file is None
        assert creds.api_key is None


class TestSpreadsheetIdExtraction:
    """Tests for spreadsheet ID extraction."""
    
    def test_extract_from_url(self):
        """Test extracting spreadsheet ID from full URL."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0"
        result = adapter._extract_spreadsheet_id(url)
        
        assert result == "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    
    def test_extract_from_uri_scheme(self):
        """Test extracting spreadsheet ID from google_sheets:// URI."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        uri = "google_sheets://1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        result = adapter._extract_spreadsheet_id(uri)
        
        assert result == "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    
    def test_pass_through_id(self):
        """Test passing through a raw spreadsheet ID."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        spreadsheet_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        result = adapter._extract_spreadsheet_id(spreadsheet_id)
        
        assert result == spreadsheet_id


class TestTypeInference:
    """Tests for column type inference."""
    
    def test_infer_integer(self):
        """Test integer type inference."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        values = [1, 2, 3, 4, 5]
        result = adapter._infer_type(values)
        
        assert result == "integer"
    
    def test_infer_float(self):
        """Test float type inference."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        values = [1.5, 2.7, 3.14]
        result = adapter._infer_type(values)
        
        assert result == "double"
    
    def test_infer_boolean(self):
        """Test boolean type inference."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        values = [True, False, True]
        result = adapter._infer_type(values)
        
        assert result == "boolean"
    
    def test_infer_string(self):
        """Test string type inference."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        values = ["hello", "world", "test"]
        result = adapter._infer_type(values)
        
        assert result == "string"
    
    def test_infer_mixed_to_string(self):
        """Test mixed types default to string."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        values = [1, "hello", 3.14]
        result = adapter._infer_type(values)
        
        assert result == "string"
    
    def test_infer_empty_to_string(self):
        """Test empty values default to string."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        values = []
        result = adapter._infer_type(values)
        
        assert result == "string"


class TestClientSideFiltering:
    """Tests for client-side predicate application."""
    
    def test_apply_equals_predicate(self):
        """Test applying equality predicate."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        table = pa.table({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        })
        
        class MockPredicate:
            column = "name"
            operator = "="
            value = "Bob"
        
        result = adapter._apply_predicates(table, [MockPredicate()])
        
        assert len(result) == 1
        assert result.column("name")[0].as_py() == "Bob"
    
    def test_apply_greater_than_predicate(self):
        """Test applying greater than predicate."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        table = pa.table({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        })
        
        class MockPredicate:
            column = "age"
            operator = ">"
            value = 28
        
        result = adapter._apply_predicates(table, [MockPredicate()])
        
        assert len(result) == 2
    
    def test_apply_in_predicate(self):
        """Test applying IN predicate."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        table = pa.table({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        })
        
        class MockPredicate:
            column = "name"
            operator = "IN"
            value = ["Alice", "Charlie"]
        
        result = adapter._apply_predicates(table, [MockPredicate()])
        
        assert len(result) == 2
    
    def test_apply_is_null_predicate(self):
        """Test applying IS NULL predicate."""
        adapter = GoogleSheetsAdapter.__new__(GoogleSheetsAdapter)
        
        table = pa.table({
            "name": ["Alice", None, "Charlie"],
            "age": [25, 30, 35],
        })
        
        class MockPredicate:
            column = "name"
            operator = "IS NULL"
            value = None
        
        result = adapter._apply_predicates(table, [MockPredicate()])
        
        assert len(result) == 1
        assert result.column("age")[0].as_py() == 30


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
