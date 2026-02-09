"""
Tests for Cloud Storage Adapter.
"""

import pytest
from unittest.mock import patch

from waveql.adapters.cloud_storage import (
    CloudStorageAdapter,
    CloudCredentials,
    CloudProvider,
    TableFormat,
    s3_adapter,
    gcs_adapter,
    azure_adapter,
    delta_table,
    iceberg_table,
)


class TestCloudCredentials:
    """Tests for CloudCredentials."""
    
    def test_from_env_aws(self, monkeypatch):
        """Test loading AWS credentials from environment."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        
        creds = CloudCredentials.from_env()
        
        assert creds.aws_access_key_id == "test_key"
        assert creds.aws_secret_access_key == "test_secret"
        assert creds.aws_region == "us-east-1"
    
    def test_from_env_gcs(self, monkeypatch):
        """Test loading GCS credentials from environment."""
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "my-project")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
        
        creds = CloudCredentials.from_env()
        
        assert creds.gcs_project_id == "my-project"
        assert creds.gcs_service_account_json == "/path/to/creds.json"
    
    def test_from_env_azure(self, monkeypatch):
        """Test loading Azure credentials from environment."""
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "myaccount")
        monkeypatch.setenv("AZURE_STORAGE_KEY", "mysecretkey")
        
        creds = CloudCredentials.from_env()
        
        assert creds.azure_storage_account == "myaccount"
        assert creds.azure_storage_key == "mysecretkey"
    
    def test_merge_priority(self):
        """Test that merge gives priority to self."""
        explicit = CloudCredentials(aws_access_key_id="explicit_key")
        env_based = CloudCredentials(
            aws_access_key_id="env_key",
            aws_region="us-west-2"
        )
        
        merged = explicit.merge(env_based)
        
        assert merged.aws_access_key_id == "explicit_key"  # From explicit
        assert merged.aws_region == "us-west-2"  # From env_based


class TestCloudProviderDetection:
    """Tests for provider detection."""
    
    def test_detect_s3(self):
        """Test S3 URI detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "s3://my-bucket/path"
        
        assert adapter._detect_provider() == CloudProvider.S3
    
    def test_detect_s3a(self):
        """Test S3A URI detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "s3a://my-bucket/path"
        
        assert adapter._detect_provider() == CloudProvider.S3
    
    def test_detect_gcs(self):
        """Test GCS URI detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "gs://my-bucket/path"
        
        assert adapter._detect_provider() == CloudProvider.GCS
    
    def test_detect_azure(self):
        """Test Azure URI detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "azure://container@account.blob.core.windows.net/path"
        
        assert adapter._detect_provider() == CloudProvider.AZURE
    
    def test_detect_local(self):
        """Test local path detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "/local/path/to/data"
        
        assert adapter._detect_provider() == CloudProvider.LOCAL


class TestTableFormatDetection:
    """Tests for table format detection."""
    
    def test_detect_delta(self):
        """Test Delta Lake detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "s3://bucket/_delta_log"
        
        assert adapter._detect_format() == TableFormat.DELTA
    
    def test_detect_parquet_extension(self):
        """Test Parquet file detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "s3://bucket/data.parquet"
        
        assert adapter._detect_format() == TableFormat.PARQUET
    
    def test_detect_csv_extension(self):
        """Test CSV file detection."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._uri = "s3://bucket/data.csv"
        
        assert adapter._detect_format() == TableFormat.CSV


class TestFactoryFunctions:
    """Tests for convenience factory functions."""
    
    @patch.object(CloudStorageAdapter, '__init__', lambda self, *args, **kwargs: None)
    def test_s3_adapter_factory(self):
        """Test s3_adapter factory function."""
        adapter = s3_adapter(
            bucket="my-bucket",
            prefix="data/",
            format="parquet",
            region="us-east-1"
        )
        
        assert adapter is not None
    
    @patch.object(CloudStorageAdapter, '__init__', lambda self, *args, **kwargs: None)
    def test_gcs_adapter_factory(self):
        """Test gcs_adapter factory function."""
        adapter = gcs_adapter(
            bucket="my-bucket",
            prefix="data/",
            project_id="my-project"
        )
        
        assert adapter is not None
    
    @patch.object(CloudStorageAdapter, '__init__', lambda self, *args, **kwargs: None)
    def test_azure_adapter_factory(self):
        """Test azure_adapter factory function."""
        adapter = azure_adapter(
            container="my-container",
            storage_account="myaccount",
            prefix="data/"
        )
        
        assert adapter is not None
    
    @patch.object(CloudStorageAdapter, '__init__', lambda self, *args, **kwargs: None)
    def test_delta_table_factory(self):
        """Test delta_table factory function."""
        adapter = delta_table("s3://bucket/delta-table/")
        
        assert adapter is not None
    
    @patch.object(CloudStorageAdapter, '__init__', lambda self, *args, **kwargs: None)
    def test_iceberg_table_factory(self):
        """Test iceberg_table factory function."""
        adapter = iceberg_table(
            "s3://bucket/iceberg/",
            catalog="glue"
        )
        
        assert adapter is not None


class TestSourceExpressions:
    """Tests for source expression generation."""
    
    def test_parquet_source(self):
        """Test Parquet source expression."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._format = TableFormat.PARQUET
        adapter._iceberg_catalog = None
        
        source = adapter._get_source_expression("s3://bucket/data.parquet")
        
        assert "read_parquet" in source
        assert "s3://bucket/data.parquet" in source
    
    def test_delta_source(self):
        """Test Delta source expression."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._format = TableFormat.DELTA
        adapter._iceberg_catalog = None
        
        source = adapter._get_source_expression("s3://bucket/delta/")
        
        assert "delta_scan" in source
    
    def test_iceberg_source_with_catalog(self):
        """Test Iceberg source expression with catalog."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._format = TableFormat.ICEBERG
        adapter._iceberg_catalog = "glue"
        
        source = adapter._get_source_expression("s3://bucket/iceberg/")
        
        assert "iceberg_scan" in source
        assert "glue" in source
    
    def test_csv_source(self):
        """Test CSV source expression."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        adapter._format = TableFormat.CSV
        adapter._iceberg_catalog = None
        
        source = adapter._get_source_expression("s3://bucket/data.csv")
        
        assert "read_csv_auto" in source


class TestPredicateConversion:
    """Tests for predicate to SQL conversion."""
    
    def test_equals_string(self):
        """Test string equality predicate."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        
        class MockPredicate:
            column = "name"
            operator = "="
            value = "test"
        
        sql = adapter._predicate_to_sql(MockPredicate())
        
        assert sql == "name = 'test'"
    
    def test_equals_number(self):
        """Test numeric equality predicate."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        
        class MockPredicate:
            column = "count"
            operator = "="
            value = 42
        
        sql = adapter._predicate_to_sql(MockPredicate())
        
        assert sql == "count = 42"
    
    def test_in_list(self):
        """Test IN predicate."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        
        class MockPredicate:
            column = "status"
            operator = "IN"
            value = ["active", "pending"]
        
        sql = adapter._predicate_to_sql(MockPredicate())
        
        assert "status IN" in sql
        assert "'active'" in sql
        assert "'pending'" in sql
    
    def test_is_null(self):
        """Test IS NULL predicate."""
        adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
        
        class MockPredicate:
            column = "deleted_at"
            operator = "IS NULL"
            value = None
        
        sql = adapter._predicate_to_sql(MockPredicate())
        
        assert sql == "deleted_at IS NULL"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
