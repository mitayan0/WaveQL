"""
Tests for WaveQL adapters/cloud_storage module.

This covers the 41% uncovered module waveql/adapters/cloud_storage.py
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch

from waveql.adapters.cloud_storage import CloudStorageAdapter
from waveql.query_planner import Predicate


class TestCloudStorageAdapterInit:
    """Tests for CloudStorageAdapter initialization."""
    
    def test_init_s3(self):
        """Test initialization for S3."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(
                host="s3://my-bucket/data",
                credentials=MagicMock(),
            )
            
            assert adapter.adapter_name == "cloud_storage"
            from waveql.adapters.cloud_storage import CloudProvider
            assert adapter._provider == CloudProvider.S3

    def test_init_gcs(self):
        """Test initialization for GCS."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(
                host="gs://my-bucket/data",
            )
            
            from waveql.adapters.cloud_storage import CloudProvider
            assert adapter._provider == CloudProvider.GCS
    
    def test_init_azure(self):
        """Test initialization for Azure Blob."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(
                host="az://my-container/data",
            )
            
            from waveql.adapters.cloud_storage import CloudProvider
            assert adapter._provider == CloudProvider.AZURE



class TestCloudStorageAdapterFetch:
    """Tests for CloudStorageAdapter fetch method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked DuckDB."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(
                host="s3://my-bucket/data",
            )
            
            # Mock duckdb connection
            adapter._duckdb = MagicMock()
            
            # Setup default successful response
            mock_result = MagicMock()
            mock_result.fetch_arrow_table.return_value = pa.table({"id": [1, 2], "name": ["A", "B"]})
            adapter._duckdb.execute.return_value = mock_result
            
            yield adapter
    
    def test_fetch_parquet(self, mock_adapter):
        """Test fetching Parquet files."""
        result = mock_adapter.fetch("data/file1.parquet")
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_columns(self, mock_adapter):
        """Test fetching with column selection."""
        result = mock_adapter.fetch("data/file1.parquet", columns=["id"])
        assert isinstance(result, pa.Table)
    
    def test_fetch_with_predicates(self, mock_adapter):
        """Test fetching with predicates."""
        predicates = [Predicate(column="id", operator="=", value=1)]
        result = mock_adapter.fetch("data/file1.parquet", predicates=predicates)
        assert isinstance(result, pa.Table)


class TestCloudStorageAdapterListTables:
    """Tests for list_tables method."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter with mocked DuckDB."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(host="s3://my-bucket")
            adapter._duckdb = MagicMock()
            
            # Mock list tables query response
            mock_result = MagicMock()
            mock_result.fetchall.return_value = [("file1.parquet",), ("file2.csv",)]
            adapter._duckdb.execute.return_value = mock_result
            
            yield adapter
    
    def test_list_tables(self, mock_adapter):
        """Test listing files as tables."""
        tables = mock_adapter.list_tables()
        assert "file1" in tables or "file2" in tables
        assert isinstance(tables, list)


class TestCloudStorageAdapterSchema:
    """Tests for schema discovery."""
    
    @pytest.fixture
    def mock_adapter(self):
        """Create adapter for schema tests."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(host="s3://my-bucket")
            adapter._duckdb = MagicMock()
            
            # Mock DESCRIBE response
            mock_result = MagicMock()
            mock_result.fetchall.return_value = [
                ("id", "BIGINT", "YES"),
                ("name", "VARCHAR", "YES"),
            ]
            adapter._duckdb.execute.return_value = mock_result
            
            yield adapter
    
    def test_get_schema(self, mock_adapter):
        """Test getting schema."""
        schema = mock_adapter.get_schema("data.parquet")
        assert len(schema) == 2
        assert schema[0].name == "id"


class TestCloudStorageAdapterFileFormats:
    """Tests for different file formats."""
    
    @pytest.fixture
    def mock_adapter(self):
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(host="s3://my-bucket")
            adapter._duckdb = MagicMock()
            
            mock_result = MagicMock()
            mock_result.fetch_arrow_table.return_value = pa.table({"col": [1]})
            adapter._duckdb.execute.return_value = mock_result
            yield adapter
    
    def test_fetch_csv(self, mock_adapter):
        result = mock_adapter.fetch("data.csv")
        assert isinstance(result, pa.Table)
    
    def test_fetch_json(self, mock_adapter):
        result = mock_adapter.fetch("data.json")
        assert isinstance(result, pa.Table)


class TestCloudStorageAdapterGCS:
    """Tests specific to GCS provider."""
    
    def test_fetch_from_gcs(self):
        """Test fetching from GCS."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(host="gs://my-bucket/data")
            adapter._duckdb = MagicMock()
            mock_result = MagicMock()
            mock_result.fetch_arrow_table.return_value = pa.table({"id": [1]})
            adapter._duckdb.execute.return_value = mock_result
            
            result = adapter.fetch("data.parquet")
            assert isinstance(result, pa.Table)


class TestCloudStorageAdapterAzure:
    """Tests specific to Azure Blob provider."""
    
    def test_fetch_from_azure(self):
        """Test fetching from Azure Blob."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(host="az://my-container/data")
            adapter._duckdb = MagicMock()
            mock_result = MagicMock()
            mock_result.fetch_arrow_table.return_value = pa.table({"id": [1]})
            adapter._duckdb.execute.return_value = mock_result
            
            result = adapter.fetch("data.parquet")
            assert isinstance(result, pa.Table)


class TestCloudStorageAdapterErrorHandling:
    """Tests for error handling."""
    
    def test_fetch_error(self):
        """Test handling fetch errors."""
        with patch("waveql.adapters.cloud_storage.duckdb"):
            adapter = CloudStorageAdapter(host="s3://my-bucket")
            adapter._duckdb = MagicMock()
            adapter._duckdb.execute.side_effect = Exception("DuckDB Error")
            
            import waveql
            with pytest.raises(waveql.exceptions.AdapterError):
                adapter.fetch("error.parquet")



if __name__ == "__main__":
    pytest.main([__file__, "-v"])
