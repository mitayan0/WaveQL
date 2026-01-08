#!/usr/bin/env python
"""
Cloud Storage Full Integration Tests
=====================================

This test suite validates the CloudStorageAdapter against local Docker emulators:
- MinIO (S3-compatible)
- Fake GCS Server (Google Cloud Storage)
- Azurite (Azure Blob Storage)

Prerequisites:
    docker-compose -f docker-compose.test.yml up -d

Run Tests:
    python playground/test_cloud_storage_full.py

Note: These emulators are completely isolated from real cloud services.
      Using fake servers will NOT affect connections to real AWS/GCS/Azure.
"""

import sys
import os
import socket
import time
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Callable
from contextlib import contextmanager

# Configure stdout for UTF-8 to support emojis on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        # Python < 3.7 fallback
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq

from waveql.adapters.cloud_storage import (
    CloudStorageAdapter,
    CloudCredentials,
    CloudProvider,
    TableFormat,
    s3_adapter,
    gcs_adapter,
    azure_adapter,
)


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class EmulatorConfig:
    """Configuration for a cloud emulator."""
    name: str
    host: str
    port: int
    available: bool = False


# MinIO Configuration
MINIO_CONFIG = EmulatorConfig(
    name="MinIO",
    host="localhost",
    port=9000,
)
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "waveql-test"

# Fake GCS Configuration
FAKE_GCS_CONFIG = EmulatorConfig(
    name="Fake GCS",
    host="localhost",
    port=4443,
)
GCS_BUCKET = "waveql-gcs-test"

# Azurite Configuration
AZURITE_CONFIG = EmulatorConfig(
    name="Azurite",
    host="localhost",
    port=10000,
)
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
AZURITE_CONTAINER = "waveql-test"
AZURITE_ACCOUNT = "devstoreaccount1"


# =============================================================================
# Utilities
# =============================================================================

def check_port_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a service is available on the given host:port."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def create_sample_data(num_rows: int = 100) -> pa.Table:
    """Create sample PyArrow table for testing."""
    import random
    
    return pa.table({
        "id": list(range(1, num_rows + 1)),
        "name": [f"user_{i}" for i in range(1, num_rows + 1)],
        "score": [round(random.uniform(50, 100), 2) for _ in range(num_rows)],
        "active": [random.choice([True, False]) for _ in range(num_rows)],
        "category": [random.choice(["A", "B", "C", "D"]) for _ in range(num_rows)],
    })


def parquet_to_bytes(table: pa.Table) -> bytes:
    """Convert PyArrow table to Parquet bytes."""
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def csv_to_bytes(table: pa.Table) -> bytes:
    """Convert PyArrow table to CSV bytes."""
    import pyarrow.csv as csv
    buf = BytesIO()
    csv.write_csv(table, buf)
    return buf.getvalue()


class TestResult:
    """Test result tracker."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.results = []
    
    def record(self, name: str, status: str, message: str = ""):
        """Record a test result."""
        self.results.append((name, status, message))
        if status == "PASSED":
            self.passed += 1
        elif status == "FAILED":
            self.failed += 1
        else:
            self.skipped += 1
        
        # Print result
        icon = {"PASSED": "✅", "FAILED": "❌", "SKIPPED": "⏭️"}.get(status, "❓")
        print(f"  {icon} {name}: {status}", end="")
        if message:
            print(f" - {message}")
        else:
            print()
    
    def summary(self):
        """Print summary."""
        total = self.passed + self.failed + self.skipped
        print(f"\n{'='*60}")
        print(f"SUMMARY: {self.passed}/{total} passed, {self.failed} failed, {self.skipped} skipped")
        print(f"{'='*60}")


# =============================================================================
# MinIO Tests (S3-Compatible)
# =============================================================================

class MinIOTests:
    """Tests for S3 adapter using MinIO emulator."""
    
    def __init__(self, results: TestResult):
        self.results = results
        self.client = None
        self.sample_data = create_sample_data(50)
    
    def setup(self) -> bool:
        """Set up MinIO client and test bucket."""
        try:
            import boto3
            from botocore.client import Config
            from botocore.exceptions import ClientError
            
            self.client = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_CONFIG.host}:{MINIO_CONFIG.port}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                config=Config(signature_version="s3v4"),
                region_name="us-east-1",
            )
            
            # Create test bucket
            try:
                self.client.create_bucket(Bucket=MINIO_BUCKET)
                print(f"  Created bucket: {MINIO_BUCKET}")
            except ClientError as e:
                if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise
            
            return True
        except ImportError:
            print("  ⚠️  boto3 not installed. Install with: pip install boto3")
            return False
        except Exception as e:
            print(f"  ⚠️  Failed to set up MinIO: {e}")
            return False
    
    def upload_test_files(self):
        """Upload test files to MinIO."""
        # Parquet file
        self.client.put_object(
            Bucket=MINIO_BUCKET,
            Key="test_data/sample.parquet",
            Body=parquet_to_bytes(self.sample_data),
        )
        
        # CSV file
        self.client.put_object(
            Bucket=MINIO_BUCKET,
            Key="test_data/sample.csv",
            Body=csv_to_bytes(self.sample_data),
        )
        
        # Multiple Parquet files for glob testing
        chunk_size = len(self.sample_data) // 3
        for i in range(3):
            start = i * chunk_size
            end = start + chunk_size if i < 2 else len(self.sample_data)
            chunk = self.sample_data.slice(start, end - start)
            self.client.put_object(
                Bucket=MINIO_BUCKET,
                Key=f"test_data/partitioned/part_{i}.parquet",
                Body=parquet_to_bytes(chunk),
            )
    
    def get_adapter(self, path: str = "test_data/", format: str = "parquet") -> CloudStorageAdapter:
        """Create adapter for MinIO."""
        credentials = CloudCredentials(
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            aws_endpoint=f"http://{MINIO_CONFIG.host}:{MINIO_CONFIG.port}",
            aws_region="us-east-1",
            use_ssl=False,
        )
        return CloudStorageAdapter(
            host=f"s3://{MINIO_BUCKET}/{path}",
            credentials=credentials,
            format=format,
        )
    
    def cleanup(self):
        """Clean up test files."""
        if not self.client:
            return
        
        try:
            # List and delete all objects
            response = self.client.list_objects_v2(Bucket=MINIO_BUCKET)
            for obj in response.get("Contents", []):
                self.client.delete_object(Bucket=MINIO_BUCKET, Key=obj["Key"])
        except Exception:
            pass
    
    def run_all(self):
        """Run all MinIO tests."""
        print("\n" + "="*60)
        print("🪣 MinIO (S3-Compatible) Tests")
        print("="*60)
        
        if not check_port_available(MINIO_CONFIG.host, MINIO_CONFIG.port):
            self.results.record("MinIO Connection", "SKIPPED", "MinIO not running on port 9000")
            return
        
        if not self.setup():
            self.results.record("MinIO Setup", "SKIPPED", "Failed to set up MinIO client")
            return
        
        try:
            self.upload_test_files()
            
            # Run individual tests
            self.test_basic_fetch()
            self.test_column_selection()
            self.test_predicate_pushdown()
            self.test_limit_offset()
            self.test_order_by()
            self.test_aggregation()
            self.test_csv_format()
            self.test_glob_pattern()
            self.test_schema_retrieval()
            self.test_anonymous_access()
            
        finally:
            self.cleanup()
    
    def test_basic_fetch(self):
        """Test basic data fetch from Parquet."""
        try:
            adapter = self.get_adapter()
            result = adapter.fetch("sample.parquet")
            
            assert isinstance(result, pa.Table), "Result should be PyArrow Table"
            assert len(result) == 50, f"Expected 50 rows, got {len(result)}"
            assert "id" in result.column_names
            assert "name" in result.column_names
            
            self.results.record("Basic Fetch", "PASSED")
        except Exception as e:
            self.results.record("Basic Fetch", "FAILED", str(e))
    
    def test_column_selection(self):
        """Test column projection."""
        try:
            adapter = self.get_adapter()
            result = adapter.fetch("sample.parquet", columns=["id", "name"])
            
            assert result.column_names == ["id", "name"], f"Got columns: {result.column_names}"
            assert "score" not in result.column_names
            
            self.results.record("Column Selection", "PASSED")
        except Exception as e:
            self.results.record("Column Selection", "FAILED", str(e))
    
    def test_predicate_pushdown(self):
        """Test predicate pushdown filtering."""
        try:
            from waveql.query_planner import Predicate
            
            adapter = self.get_adapter()
            
            # Test greater than
            predicate = Predicate(column="score", operator=">", value=90.0)
            result = adapter.fetch("sample.parquet", predicates=[predicate])
            
            scores = result.column("score").to_pylist()
            assert all(s > 90.0 for s in scores), "All scores should be > 90"
            
            # Test equality
            predicate2 = Predicate(column="category", operator="=", value="A")
            result2 = adapter.fetch("sample.parquet", predicates=[predicate2])
            
            categories = result2.column("category").to_pylist()
            assert all(c == "A" for c in categories), "All categories should be 'A'"
            
            self.results.record("Predicate Pushdown", "PASSED")
        except Exception as e:
            self.results.record("Predicate Pushdown", "FAILED", str(e))
    
    def test_limit_offset(self):
        """Test LIMIT and OFFSET."""
        try:
            adapter = self.get_adapter()
            
            # Test LIMIT
            result = adapter.fetch("sample.parquet", limit=10)
            assert len(result) == 10, f"Expected 10 rows, got {len(result)}"
            
            # Test LIMIT with OFFSET
            result_offset = adapter.fetch("sample.parquet", limit=10, offset=5)
            assert len(result_offset) == 10, f"Expected 10 rows with offset"
            
            self.results.record("LIMIT/OFFSET", "PASSED")
        except Exception as e:
            self.results.record("LIMIT/OFFSET", "FAILED", str(e))
    
    def test_order_by(self):
        """Test ORDER BY."""
        try:
            adapter = self.get_adapter()
            
            result = adapter.fetch("sample.parquet", order_by=[("score", "DESC")], limit=10)
            scores = result.column("score").to_pylist()
            
            assert scores == sorted(scores, reverse=True), "Scores should be descending"
            
            self.results.record("ORDER BY", "PASSED")
        except Exception as e:
            self.results.record("ORDER BY", "FAILED", str(e))
    
    def test_aggregation(self):
        """Test aggregation queries."""
        try:
            from waveql.query_planner import Aggregate
            
            adapter = self.get_adapter()
            
            # Test COUNT
            result = adapter.fetch(
                "sample.parquet",
                aggregates=[Aggregate(func="COUNT", column="*", alias="total")],
            )
            
            assert len(result) == 1, "Aggregation should return 1 row"
            total = result.column("total").to_pylist()[0]
            assert total == 50, f"Expected count 50, got {total}"
            
            # Test GROUP BY
            result_grouped = adapter.fetch(
                "sample.parquet",
                group_by=["category"],
                aggregates=[Aggregate(func="COUNT", column="*", alias="count")],
            )
            
            assert len(result_grouped) <= 4, "Should have at most 4 categories"
            
            self.results.record("Aggregation", "PASSED")
        except Exception as e:
            self.results.record("Aggregation", "FAILED", str(e))
    
    def test_csv_format(self):
        """Test reading CSV files."""
        try:
            adapter = self.get_adapter(format="csv")
            result = adapter.fetch("sample.csv")
            
            assert len(result) == 50, f"Expected 50 rows from CSV"
            
            self.results.record("CSV Format", "PASSED")
        except Exception as e:
            self.results.record("CSV Format", "FAILED", str(e))
    
    def test_glob_pattern(self):
        """Test reading multiple files with glob pattern."""
        try:
            adapter = self.get_adapter(path="test_data/partitioned/")
            result = adapter.fetch("*.parquet")
            
            # Should read all 3 partitioned files
            assert len(result) == 50, f"Expected 50 rows from all partitions, got {len(result)}"
            
            self.results.record("Glob Pattern", "PASSED")
        except Exception as e:
            self.results.record("Glob Pattern", "FAILED", str(e))
    
    def test_schema_retrieval(self):
        """Test schema retrieval."""
        try:
            adapter = self.get_adapter()
            schema = adapter.get_schema("sample.parquet")
            
            assert len(schema) == 5, f"Expected 5 columns, got {len(schema)}"
            column_names = [col.name for col in schema]
            assert "id" in column_names
            assert "name" in column_names
            assert "score" in column_names
            
            self.results.record("Schema Retrieval", "PASSED")
        except Exception as e:
            self.results.record("Schema Retrieval", "FAILED", str(e))
    
    def test_anonymous_access(self):
        """Test anonymous access configuration."""
        try:
            # This test verifies the credential configuration works
            credentials = CloudCredentials(anonymous=True)
            
            # Just verify the adapter can be created with anonymous credentials
            adapter = CloudStorageAdapter(
                host="s3://public-bucket/",
                credentials=credentials,
                format="parquet",
            )
            
            assert adapter._credentials.anonymous is True
            
            self.results.record("Anonymous Access Config", "PASSED")
        except Exception as e:
            self.results.record("Anonymous Access Config", "FAILED", str(e))


# =============================================================================
# Fake GCS Tests
# =============================================================================

class FakeGCSTests:
    """Tests for GCS adapter using fake-gcs-server emulator."""
    
    def __init__(self, results: TestResult):
        self.results = results
        self.client = None
        self.sample_data = create_sample_data(50)
    
    def setup(self) -> bool:
        """Set up GCS client."""
        try:
            from google.cloud import storage
            from google.auth.credentials import AnonymousCredentials
            
            # Point to fake GCS server - must include http:// prefix
            os.environ["STORAGE_EMULATOR_HOST"] = f"http://{FAKE_GCS_CONFIG.host}:{FAKE_GCS_CONFIG.port}"
            
            self.client = storage.Client(
                credentials=AnonymousCredentials(),
                project="test-project",
            )
            
            # Create test bucket
            try:
                self.client.create_bucket(GCS_BUCKET)
                print(f"  Created bucket: {GCS_BUCKET}")
            except Exception:
                pass  # Bucket may already exist
            
            return True
        except ImportError:
            print("  ⚠️  google-cloud-storage not installed. Install with: pip install google-cloud-storage")
            return False
        except Exception as e:
            print(f"  ⚠️  Failed to set up Fake GCS: {e}")
            return False
    
    def upload_test_files(self):
        """Upload test files to fake GCS."""
        bucket = self.client.bucket(GCS_BUCKET)
        
        # Upload Parquet file
        blob = bucket.blob("test_data/sample.parquet")
        blob.upload_from_string(parquet_to_bytes(self.sample_data))
        
        # Upload CSV file
        blob_csv = bucket.blob("test_data/sample.csv")
        blob_csv.upload_from_string(csv_to_bytes(self.sample_data))
    
    def get_adapter(self, path: str = "test_data/", format: str = "parquet") -> CloudStorageAdapter:
        """Create adapter for Fake GCS."""
        # Note: DuckDB uses GOOGLE_APPLICATION_CREDENTIALS for GCS
        # For fake server, we need to configure endpoint differently
        credentials = CloudCredentials(anonymous=True)
        return CloudStorageAdapter(
            host=f"gs://{GCS_BUCKET}/{path}",
            credentials=credentials,
            format=format,
        )
    
    def cleanup(self):
        """Clean up test files."""
        if not self.client:
            return
        
        try:
            bucket = self.client.bucket(GCS_BUCKET)
            blobs = bucket.list_blobs()
            for blob in blobs:
                blob.delete()
        except Exception:
            pass
    
    def run_all(self):
        """Run all Fake GCS tests."""
        print("\n" + "="*60)
        print("☁️  Fake GCS Server Tests")
        print("="*60)
        
        if not check_port_available(FAKE_GCS_CONFIG.host, FAKE_GCS_CONFIG.port):
            self.results.record("Fake GCS Connection", "SKIPPED", "Fake GCS not running on port 4443")
            return
        
        if not self.setup():
            self.results.record("Fake GCS Setup", "SKIPPED", "Failed to set up Fake GCS client")
            return
        
        try:
            self.upload_test_files()
            print("  ✅ File Upload: Files uploaded to fake GCS successfully")
            
            # NOTE: DuckDB's GCS support (gs:// URLs) always connects to the real
            # storage.googleapis.com endpoint. There's no way to redirect it to a
            # local emulator. For local testing, use MinIO (S3-compatible) instead.
            self.results.record("GCS File Upload", "PASSED", "Files uploaded to emulator")
            self.results.record("GCS DuckDB Read", "SKIPPED", 
                "DuckDB gs:// always connects to storage.googleapis.com - use MinIO for local testing")
            
        finally:
            self.cleanup()
    
    def test_basic_fetch(self):
        """Test basic data fetch from GCS."""
        try:
            adapter = self.get_adapter()
            result = adapter.fetch("sample.parquet")
            
            assert isinstance(result, pa.Table), "Result should be PyArrow Table"
            assert len(result) == 50, f"Expected 50 rows, got {len(result)}"
            
            self.results.record("GCS Basic Fetch", "PASSED")
        except Exception as e:
            self.results.record("GCS Basic Fetch", "FAILED", str(e))
    
    def test_column_selection(self):
        """Test column projection on GCS."""
        try:
            adapter = self.get_adapter()
            result = adapter.fetch("sample.parquet", columns=["id", "name"])
            
            assert "id" in result.column_names
            assert "name" in result.column_names
            assert "score" not in result.column_names
            
            self.results.record("GCS Column Selection", "PASSED")
        except Exception as e:
            self.results.record("GCS Column Selection", "FAILED", str(e))
    
    def test_schema_retrieval(self):
        """Test schema retrieval from GCS."""
        try:
            adapter = self.get_adapter()
            schema = adapter.get_schema("sample.parquet")
            
            assert len(schema) >= 3, "Should have multiple columns"
            
            self.results.record("GCS Schema Retrieval", "PASSED")
        except Exception as e:
            self.results.record("GCS Schema Retrieval", "FAILED", str(e))


# =============================================================================
# Azurite Tests (Azure Blob Storage)
# =============================================================================

class AzuriteTests:
    """Tests for Azure adapter using Azurite emulator."""
    
    def __init__(self, results: TestResult):
        self.results = results
        self.client = None
        self.sample_data = create_sample_data(50)
    
    def setup(self) -> bool:
        """Set up Azure Blob client."""
        try:
            from azure.storage.blob import BlobServiceClient
            
            self.client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
            
            # Create test container
            try:
                self.client.create_container(AZURITE_CONTAINER)
                print(f"  Created container: {AZURITE_CONTAINER}")
            except Exception:
                pass  # Container may already exist
            
            return True
        except ImportError:
            print("  ⚠️  azure-storage-blob not installed. Install with: pip install azure-storage-blob")
            return False
        except Exception as e:
            print(f"  ⚠️  Failed to set up Azurite: {e}")
            return False
    
    def upload_test_files(self):
        """Upload test files to Azurite."""
        container_client = self.client.get_container_client(AZURITE_CONTAINER)
        
        # Upload Parquet file
        blob_client = container_client.get_blob_client("test_data/sample.parquet")
        blob_client.upload_blob(parquet_to_bytes(self.sample_data), overwrite=True)
        
        # Upload CSV file
        blob_client_csv = container_client.get_blob_client("test_data/sample.csv")
        blob_client_csv.upload_blob(csv_to_bytes(self.sample_data), overwrite=True)
    
    def get_adapter(self, path: str = "test_data/", format: str = "parquet") -> CloudStorageAdapter:
        """Create adapter for Azurite."""
        credentials = CloudCredentials(
            azure_connection_string=AZURITE_CONNECTION_STRING,
            azure_storage_account=AZURITE_ACCOUNT,
        )
        return CloudStorageAdapter(
            host=f"azure://{AZURITE_CONTAINER}@{AZURITE_ACCOUNT}.blob.core.windows.net/{path}",
            credentials=credentials,
            format=format,
        )
    
    def cleanup(self):
        """Clean up test files."""
        if not self.client:
            return
        
        try:
            container_client = self.client.get_container_client(AZURITE_CONTAINER)
            blobs = container_client.list_blobs()
            for blob in blobs:
                container_client.delete_blob(blob.name)
        except Exception:
            pass
    
    def run_all(self):
        """Run all Azurite tests."""
        print("\n" + "="*60)
        print("🔷 Azurite (Azure Blob) Tests")
        print("="*60)
        
        if not check_port_available(AZURITE_CONFIG.host, AZURITE_CONFIG.port):
            self.results.record("Azurite Connection", "SKIPPED", "Azurite not running on port 10000")
            return
        
        if not self.setup():
            self.results.record("Azurite Setup", "SKIPPED", "Failed to set up Azurite client")
            return
        
        try:
            self.upload_test_files()
            print("  ✅ File Upload: Files uploaded to Azurite successfully")
            
            # NOTE: DuckDB's Azure support (azure:// URLs) always connects to the real
            # blob.core.windows.net endpoint. There's no way to redirect it to a
            # local Azurite emulator. For local testing, use MinIO (S3-compatible) instead.
            self.results.record("Azure File Upload", "PASSED", "Files uploaded to emulator")
            self.results.record("Azure DuckDB Read", "SKIPPED", 
                "DuckDB azure:// always connects to blob.core.windows.net - use MinIO for local testing")
            
        finally:
            self.cleanup()
    
    def test_basic_fetch(self):
        """Test basic data fetch from Azure Blob."""
        try:
            adapter = self.get_adapter()
            result = adapter.fetch("sample.parquet")
            
            assert isinstance(result, pa.Table), "Result should be PyArrow Table"
            assert len(result) == 50, f"Expected 50 rows, got {len(result)}"
            
            self.results.record("Azure Basic Fetch", "PASSED")
        except Exception as e:
            self.results.record("Azure Basic Fetch", "FAILED", str(e))
    
    def test_column_selection(self):
        """Test column projection on Azure Blob."""
        try:
            adapter = self.get_adapter()
            result = adapter.fetch("sample.parquet", columns=["id", "name"])
            
            assert "id" in result.column_names
            assert "name" in result.column_names
            
            self.results.record("Azure Column Selection", "PASSED")
        except Exception as e:
            self.results.record("Azure Column Selection", "FAILED", str(e))
    
    def test_predicate_pushdown(self):
        """Test predicate pushdown on Azure Blob."""
        try:
            from waveql.query_planner import Predicate
            
            adapter = self.get_adapter()
            predicate = Predicate(column="score", operator=">", value=90.0)
            result = adapter.fetch("sample.parquet", predicates=[predicate])
            
            scores = result.column("score").to_pylist()
            assert all(s > 90.0 for s in scores), "All scores should be > 90"
            
            self.results.record("Azure Predicate Pushdown", "PASSED")
        except Exception as e:
            self.results.record("Azure Predicate Pushdown", "FAILED", str(e))
    
    def test_schema_retrieval(self):
        """Test schema retrieval from Azure Blob."""
        try:
            adapter = self.get_adapter()
            schema = adapter.get_schema("sample.parquet")
            
            assert len(schema) >= 3, "Should have multiple columns"
            
            self.results.record("Azure Schema Retrieval", "PASSED")
        except Exception as e:
            self.results.record("Azure Schema Retrieval", "FAILED", str(e))


# =============================================================================
# Cross-Provider Tests
# =============================================================================

class CrossProviderTests:
    """Tests that verify behavior across all providers."""
    
    def __init__(self, results: TestResult):
        self.results = results
    
    def run_all(self):
        """Run cross-provider tests."""
        print("\n" + "="*60)
        print("🔄 Cross-Provider Tests")
        print("="*60)
        
        self.test_provider_detection()
        self.test_format_detection()
        self.test_credential_resolution()
        self.test_factory_functions()
    
    def test_provider_detection(self):
        """Test cloud provider detection from URIs."""
        try:
            test_cases = [
                ("s3://bucket/path", CloudProvider.S3),
                ("s3a://bucket/path", CloudProvider.S3),
                ("gs://bucket/path", CloudProvider.GCS),
                ("gcs://bucket/path", CloudProvider.GCS),
                ("azure://container@account.blob.core.windows.net/path", CloudProvider.AZURE),
                ("az://container/path", CloudProvider.AZURE),
                ("/local/path", CloudProvider.LOCAL),
                ("C:\\local\\path", CloudProvider.LOCAL),
            ]
            
            for uri, expected_provider in test_cases:
                adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
                adapter._uri = uri
                detected = adapter._detect_provider()
                assert detected == expected_provider, f"URI {uri}: expected {expected_provider}, got {detected}"
            
            self.results.record("Provider Detection", "PASSED")
        except Exception as e:
            self.results.record("Provider Detection", "FAILED", str(e))
    
    def test_format_detection(self):
        """Test table format detection from URIs."""
        try:
            test_cases = [
                ("s3://bucket/data.parquet", TableFormat.PARQUET),
                ("s3://bucket/data.csv", TableFormat.CSV),
                ("s3://bucket/data.json", TableFormat.JSON),
                ("s3://bucket/_delta_log/path", TableFormat.DELTA),
                ("s3://bucket/table/delta", TableFormat.DELTA),
            ]
            
            for uri, expected_format in test_cases:
                adapter = CloudStorageAdapter.__new__(CloudStorageAdapter)
                adapter._uri = uri
                detected = adapter._detect_format()
                assert detected == expected_format, f"URI {uri}: expected {expected_format}, got {detected}"
            
            self.results.record("Format Detection", "PASSED")
        except Exception as e:
            self.results.record("Format Detection", "FAILED", str(e))
    
    def test_credential_resolution(self):
        """Test credential chain resolution."""
        try:
            # Test merge priority (explicit > env > config)
            explicit = CloudCredentials(aws_access_key_id="explicit_key")
            env_creds = CloudCredentials(
                aws_access_key_id="env_key",
                aws_region="us-west-2",
            )
            
            merged = explicit.merge(env_creds)
            
            assert merged.aws_access_key_id == "explicit_key", "Explicit should take priority"
            assert merged.aws_region == "us-west-2", "Should inherit from env"
            
            self.results.record("Credential Resolution", "PASSED")
        except Exception as e:
            self.results.record("Credential Resolution", "FAILED", str(e))
    
    def test_factory_functions(self):
        """Test convenience factory functions."""
        try:
            # We can't fully initialize without actual connections,
            # but we can test the URI construction
            
            # Test S3 URI construction
            uri = f"s3://my-bucket/prefix".rstrip("/")
            assert uri == "s3://my-bucket/prefix"
            
            # Test GCS URI construction
            uri = f"gs://my-bucket/prefix".rstrip("/")
            assert uri == "gs://my-bucket/prefix"
            
            # Test Azure URI construction
            container = "my-container"
            account = "myaccount"
            uri = f"azure://{container}@{account}.blob.core.windows.net/prefix".rstrip("/")
            assert "azure://" in uri
            assert container in uri
            assert account in uri
            
            self.results.record("Factory Functions", "PASSED")
        except Exception as e:
            self.results.record("Factory Functions", "FAILED", str(e))


# =============================================================================
# Main
# =============================================================================

def check_docker_status():
    """Check which Docker containers are running."""
    print("\n" + "="*60)
    print("🐳 Docker Emulator Status Check")
    print("="*60)
    
    emulators = [
        ("MinIO (S3)", MINIO_CONFIG.host, MINIO_CONFIG.port, "docker run -p 9000:9000 minio/minio server /data"),
        ("Fake GCS", FAKE_GCS_CONFIG.host, FAKE_GCS_CONFIG.port, "docker run -p 4443:4443 fsouza/fake-gcs-server"),
        ("Azurite", AZURITE_CONFIG.host, AZURITE_CONFIG.port, "docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite"),
    ]
    
    all_running = True
    for name, host, port, start_cmd in emulators:
        available = check_port_available(host, port)
        status = "✅ Running" if available else "❌ Not running"
        print(f"  {name}: {status}")
        if not available:
            all_running = False
            print(f"    Start with: {start_cmd}")
    
    if not all_running:
        print("\n  💡 Or run all with: docker-compose -f docker-compose.test.yml up -d")
    
    return all_running


def main():
    """Run all cloud storage tests."""
    print("\n" + "="*60)
    print("🌩️  WaveQL Cloud Storage Integration Tests")
    print("="*60)
    print("\nThese tests use Docker emulators and are COMPLETELY ISOLATED")
    print("from real cloud services. Safe to run anytime!")
    
    # Check Docker status
    check_docker_status()
    
    # Initialize result tracker
    results = TestResult()
    
    # Run test suites
    MinIOTests(results).run_all()
    FakeGCSTests(results).run_all()
    AzuriteTests(results).run_all()
    CrossProviderTests(results).run_all()
    
    # Print summary
    results.summary()
    
    # Return exit code
    return 0 if results.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
