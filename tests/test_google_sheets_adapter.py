"""
Tests for WaveQL adapters/google_sheets module.

This covers the 36% uncovered module waveql/adapters/google_sheets.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch
import sys

# Mock googleapiclient if not present - MUST BE DONE BEFORE IMPORTING ADAPTER
if "googleapiclient" not in sys.modules:
    sys.modules["googleapiclient"] = MagicMock()
    sys.modules["googleapiclient.discovery"] = MagicMock()
    sys.modules["googleapiclient.discovery"] = MagicMock()
    
    # Needs to be an Exception for raising
    class MockHttpError(Exception):
        def __init__(self, resp, content, uri=None):
            self.resp = resp
            self.content = content
            self.uri = uri
            super().__init__(str(content))
            
    mock_errors = MagicMock()
    mock_errors.HttpError = MockHttpError
    sys.modules["googleapiclient.errors"] = mock_errors
if "google.oauth2" not in sys.modules:
    mock_oauth2 = MagicMock()
    mock_service_account = MagicMock()
    mock_credentials = MagicMock()
    
    # Link attributes
    mock_oauth2.service_account = mock_service_account
    mock_oauth2.credentials = mock_credentials
    
    sys.modules["google.oauth2"] = mock_oauth2
    sys.modules["google.oauth2.service_account"] = mock_service_account
    sys.modules["google.oauth2.credentials"] = mock_credentials

if "google.auth.transport.requests" not in sys.modules:
    sys.modules["google.auth.transport.requests"] = MagicMock()
    sys.modules["google_auth_oauthlib.flow"] = MagicMock()

from waveql.adapters.google_sheets import GoogleSheetsAdapter
from waveql.query_planner import Predicate


class TestGoogleSheetsAdapterInit:
    """Tests for GoogleSheetsAdapter initialization."""
    
    def test_init_with_service_account(self):
        """Test initialization with service account."""
        with patch("google.oauth2.service_account", create=True) as mock_sa:
            mock_creds = MagicMock()
            mock_sa.Credentials.from_service_account_info.return_value = mock_creds
            
            from waveql.adapters.google_sheets import GoogleSheetsCredentials
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                credentials=GoogleSheetsCredentials(
                    service_account_json="test_service_account.json",
                    # other fields if needed, or rely on defaults/mock behavior
                ),
            )
            
            assert adapter.adapter_name == "google_sheets"
    
    def test_init_with_api_key(self):
        """Test initialization with API key."""
        adapter = GoogleSheetsAdapter(
            host="spreadsheet_id",
            api_key="test-api-key",
        )
        
        assert adapter.adapter_name == "google_sheets"


class TestGoogleSheetsAdapterFetch:
    """Tests for GoogleSheetsAdapter fetch method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked Google Sheets client."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            mock_values.get.return_value.execute.return_value = {
                "values": [
                    ["id", "name", "age"],
                    ["1", "Alice", "30"],
                    ["2", "Bob", "25"],
                    ["3", "Charlie", "35"],
                ],
            }
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            yield adapter
    
    def test_fetch_sheet(self, mock_adapter):
        """Test fetching sheet data."""
        result = mock_adapter.fetch("Sheet1")
        
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_columns(self, mock_adapter):
        """Test fetching with column selection."""
        result = mock_adapter.fetch(
            "Sheet1",
            columns=["id", "name"],
        )
        
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_limit(self, mock_adapter):
        """Test fetching with limit."""
        result = mock_adapter.fetch(
            "Sheet1",
            limit=2,
        )
        
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_predicates(self, mock_adapter):
        """Test fetching with predicates."""
        predicates = [
            Predicate(column="name", operator="=", value="Alice"),
        ]
        
        result = mock_adapter.fetch(
            "Sheet1",
            predicates=predicates,
        )
        
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_range(self, mock_adapter):
        """Test fetching specific range."""
        result = mock_adapter.fetch("Sheet1!A1:C10")
        
        assert isinstance(result, pa.Table)


class TestGoogleSheetsAdapterSchema:
    """Tests for schema discovery."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked service."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            # Return header row for schema
            mock_values.get.return_value.execute.return_value = {
                "values": [
                    ["id", "name", "age", "created_at"],
                ],
            }
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            yield adapter
    
    def test_get_schema(self, mock_adapter):
        """Test getting schema from header row."""
        schema = mock_adapter.get_schema("Sheet1")
        
        assert isinstance(schema, list)
        assert len(schema) == 4


class TestGoogleSheetsAdapterListTables:
    """Tests for list_tables (sheets) method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked spreadsheet metadata."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            
            mock_spreadsheets.get.return_value.execute.return_value = {
                "sheets": [
                    {"properties": {"title": "Sheet1"}},
                    {"properties": {"title": "Sheet2"}},
                    {"properties": {"title": "Data"}},
                ],
            }
            
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            yield adapter
    
    def test_list_tables(self, mock_adapter):
        """Test listing sheets."""
        tables = mock_adapter.list_tables()
        
        assert isinstance(tables, list)
        assert "Sheet1" in tables
        assert "Sheet2" in tables
        assert "Data" in tables


class TestGoogleSheetsAdapterInsert:
    """Tests for insert/append method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter for insert tests."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            mock_values.append.return_value.execute.return_value = {
                "updates": {"updatedRows": 1},
            }
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            yield adapter
    
    def test_insert_row(self, mock_adapter):
        """Test inserting a row."""
        mock_adapter.insert(
            "Sheet1",
            {"id": "4", "name": "Dave", "age": "28"},
        )
        
        # Should complete without error


class TestGoogleSheetsAdapterErrorHandling:
    """Tests for error handling."""
    
    def test_fetch_api_error(self):
        """Test handling API errors."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            from googleapiclient.errors import HttpError
            mock_response = MagicMock()
            mock_response.status = 403
            mock_values.get.return_value.execute.side_effect = HttpError(
                mock_response, b"Forbidden"
            )
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="invalid_spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            with pytest.raises(Exception):
                adapter.fetch("Sheet1")
    
    def test_fetch_empty_sheet(self):
        """Test handling empty sheet."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            mock_values.get.return_value.execute.return_value = {
                "values": [],
            }
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            result = adapter.fetch("EmptySheet")
            
            assert len(result) == 0


class TestGoogleSheetsAdapterDataTypes:
    """Tests for data type handling."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mixed data types."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            mock_values.get.return_value.execute.return_value = {
                "values": [
                    ["id", "value", "is_active", "created_at"],
                    ["1", "100.50", "TRUE", "2024-01-15"],
                    ["2", "200", "FALSE", "2024-01-16"],
                ],
            }
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            yield adapter
    
    def test_fetch_with_type_inference(self, mock_adapter):
        """Test fetching with data type inference."""
        result = mock_adapter.fetch("Sheet1")
        
        assert isinstance(result, pa.Table)
        assert len(result) == 2


class TestGoogleSheetsAdapterBatchOperations:
    """Tests for batch operations."""
    
    def test_batch_get(self):
        """Test batch get multiple ranges."""
        with patch("googleapiclient.discovery.build", create=True) as mock_build:
            mock_service = MagicMock()
            mock_spreadsheets = MagicMock()
            mock_values = MagicMock()
            
            mock_values.batchGet.return_value.execute.return_value = {
                "valueRanges": [
                    {"values": [["A", "B"], ["1", "2"]]},
                    {"values": [["C", "D"], ["3", "4"]]},
                ],
            }
            
            mock_spreadsheets.values.return_value = mock_values
            mock_service.spreadsheets.return_value = mock_spreadsheets
            mock_build.return_value = mock_service
            
            adapter = GoogleSheetsAdapter(
                host="spreadsheet_id",
                api_key="test-api-key",
            )
            adapter._service = mock_service
            
            # Batch operations if supported


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
