"""
Cloud Storage Adapter - S3, GCS, Azure Blob, Delta Lake, and Iceberg support via DuckDB.

Features:
- S3, GCS, Azure Blob storage access
- Delta Lake table support
- Apache Iceberg table support
- Credential provider chain (env vars, config, IAM)
- Predicate pushdown for all formats

Uses DuckDB's native extensions for optimal performance.
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass
from enum import Enum

import duckdb
import pyarrow as pa
try:
    import anyio
except ImportError:
    anyio = None

try:
    import boto3
except ImportError:
    boto3 = None

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate

logger = logging.getLogger(__name__)


class CloudProvider(Enum):
    """Supported cloud storage providers."""
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"
    LOCAL = "local"


class TableFormat(Enum):
    """Supported table formats."""
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    DELTA = "delta"
    ICEBERG = "iceberg"


@dataclass
class CloudCredentials:
    """
    Cloud storage credentials with provider chain support.
    
    Resolution order:
    1. Explicit parameters (passed to constructor)
    2. Environment variables
    3. Config file (~/.waveql/credentials.yaml)
    4. IAM role (for AWS) / Workload Identity (for GCP)
    """
    
    # AWS S3
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_region: Optional[str] = None
    aws_endpoint: Optional[str] = None  # For S3-compatible storage (MinIO, etc.)
    
    # GCS
    gcs_project_id: Optional[str] = None
    gcs_service_account_json: Optional[str] = None  # Path to service account JSON
    
    # Azure
    azure_storage_account: Optional[str] = None
    azure_storage_key: Optional[str] = None
    azure_connection_string: Optional[str] = None
    azure_sas_token: Optional[str] = None
    
    # Use anonymous access (for public buckets)
    anonymous: bool = False
    
    # SSL/TLS settings (for S3-compatible storage without SSL)
    use_ssl: bool = True
    
    @classmethod
    def from_env(cls) -> "CloudCredentials":
        """Create credentials from environment variables."""
        return cls(
            # AWS
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
            aws_region=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
            aws_endpoint=os.environ.get("AWS_ENDPOINT_URL"),
            # GCS
            gcs_project_id=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID"),
            gcs_service_account_json=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            # Azure
            azure_storage_account=os.environ.get("AZURE_STORAGE_ACCOUNT"),
            azure_storage_key=os.environ.get("AZURE_STORAGE_KEY"),
            azure_connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
            azure_sas_token=os.environ.get("AZURE_STORAGE_SAS_TOKEN"),
        )
    
    @classmethod
    def from_config_file(cls, path: str = None) -> "CloudCredentials":
        """Load credentials from config file."""
        if path is None:
            try:
                from waveql.config import get_config
                path = str(get_config().credentials_file)
            except ImportError:
                path = os.path.expanduser("~/.waveql/credentials.yaml")
        
        if not os.path.exists(path):
            return cls()
        
        try:
            import yaml
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
            
            return cls(
                aws_access_key_id=config.get("aws", {}).get("access_key_id"),
                aws_secret_access_key=config.get("aws", {}).get("secret_access_key"),
                aws_region=config.get("aws", {}).get("region"),
                gcs_project_id=config.get("gcs", {}).get("project_id"),
                gcs_service_account_json=config.get("gcs", {}).get("service_account_json"),
                azure_storage_account=config.get("azure", {}).get("storage_account"),
                azure_storage_key=config.get("azure", {}).get("storage_key"),
            )
        except Exception as e:
            logger.warning(f"Failed to load credentials from {path}: {e}")
            return cls()
    
    def merge(self, other: "CloudCredentials") -> "CloudCredentials":
        """Merge with another credentials object (self takes priority)."""
        return CloudCredentials(
            aws_access_key_id=self.aws_access_key_id or other.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key or other.aws_secret_access_key,
            aws_session_token=self.aws_session_token or other.aws_session_token,
            aws_region=self.aws_region or other.aws_region,
            aws_endpoint=self.aws_endpoint or other.aws_endpoint,
            gcs_project_id=self.gcs_project_id or other.gcs_project_id,
            gcs_service_account_json=self.gcs_service_account_json or other.gcs_service_account_json,
            azure_storage_account=self.azure_storage_account or other.azure_storage_account,
            azure_storage_key=self.azure_storage_key or other.azure_storage_key,
            azure_connection_string=self.azure_connection_string or other.azure_connection_string,
            azure_sas_token=self.azure_sas_token or other.azure_sas_token,
            anonymous=self.anonymous or other.anonymous,
            use_ssl=self.use_ssl and other.use_ssl,  # Only use SSL if both have it enabled
        )


class CloudStorageAdapter(BaseAdapter):
    """
    Cloud Storage adapter for S3, GCS, Azure Blob, Delta Lake, and Iceberg.
    
    Features:
    - S3, GCS, Azure Blob via DuckDB httpfs extension
    - Delta Lake via DuckDB delta extension
    - Apache Iceberg via DuckDB iceberg extension
    - Automatic credential chain resolution
    - Predicate pushdown for all formats
    
    Examples:
        # S3 Parquet files
        adapter = CloudStorageAdapter("s3://my-bucket/data/")
        
        # Delta Lake table
        adapter = CloudStorageAdapter("s3://my-bucket/delta-table/", format="delta")
        
        # Iceberg table
        adapter = CloudStorageAdapter("s3://my-bucket/iceberg/", format="iceberg",
                                       iceberg_catalog="glue")
        
        # GCS with credentials
        adapter = CloudStorageAdapter("gs://my-bucket/data/",
                                       credentials=CloudCredentials(
                                           gcs_service_account_json="path/to/service-account.json"
                                       ))
        
        # Azure Blob
        adapter = CloudStorageAdapter("azure://container@account.blob.core.windows.net/path/")
    """
    
    adapter_name = "cloud_storage"
    supports_predicate_pushdown = True
    supports_insert = False  # Read-only for cloud storage
    supports_update = False
    supports_delete = False
    
    def __init__(
        self,
        host: str,  # Cloud URI (s3://, gs://, azure://, or file path)
        auth_manager=None,
        schema_cache=None,
        format: str = None,  # parquet, csv, json, delta, iceberg (auto-detected if None)
        credentials: CloudCredentials = None,
        iceberg_catalog: str = None,  # For Iceberg: glue, hive, rest
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        self._uri = host.rstrip("/")
        self._provider = self._detect_provider()
        self._format = TableFormat(format.lower()) if format else self._detect_format()
        self._iceberg_catalog = iceberg_catalog
        
        # Resolve credentials
        self._credentials = self._resolve_credentials(credentials)
        
        # Initialize DuckDB with extensions
        self._duckdb = duckdb.connect(":memory:")
        self._setup_extensions()
        self._configure_credentials()
    
    def _detect_provider(self) -> CloudProvider:
        """Detect cloud provider from URI."""
        uri_lower = self._uri.lower()
        
        if uri_lower.startswith("s3://") or uri_lower.startswith("s3a://"):
            return CloudProvider.S3
        elif uri_lower.startswith("gs://") or uri_lower.startswith("gcs://"):
            return CloudProvider.GCS
        elif uri_lower.startswith("azure://") or uri_lower.startswith("az://") or "blob.core.windows.net" in uri_lower:
            return CloudProvider.AZURE
        else:
            return CloudProvider.LOCAL
    
    def _detect_format(self) -> TableFormat:
        """Detect table format from URI."""
        uri_lower = self._uri.lower()
        
        # Check for Delta Lake markers
        if "_delta_log" in uri_lower or uri_lower.endswith("/delta"):
            return TableFormat.DELTA
        
        # Check for Iceberg markers
        if "metadata" in uri_lower and ".json" in uri_lower:
            return TableFormat.ICEBERG
        
        # Check file extension
        if uri_lower.endswith(".parquet"):
            return TableFormat.PARQUET
        elif uri_lower.endswith(".csv"):
            return TableFormat.CSV
        elif uri_lower.endswith(".json"):
            return TableFormat.JSON
        
        # Default to parquet (most common for data lakes)
        return TableFormat.PARQUET
    
    def _resolve_credentials(self, explicit: CloudCredentials = None) -> CloudCredentials:
        """Resolve credentials from chain: explicit -> env -> config -> IAM."""
        # Start with empty credentials
        creds = CloudCredentials()
        
        # Layer 3: Config file (lowest priority)
        config_creds = CloudCredentials.from_config_file()
        creds = creds.merge(config_creds)
        
        # Layer 2: Environment variables
        env_creds = CloudCredentials.from_env()
        creds = env_creds.merge(creds)
        
        # Layer 1: Explicit (highest priority)
        if explicit:
            creds = explicit.merge(creds)
        
        return creds
    
    def _setup_extensions(self):
        """Install and load required DuckDB extensions."""
        extensions = ["httpfs"]  # Base for cloud storage
        
        if self._format == TableFormat.DELTA:
            extensions.append("delta")
        elif self._format == TableFormat.ICEBERG:
            extensions.append("iceberg")
        
        for ext in extensions:
            try:
                self._duckdb.execute(f"INSTALL {ext}")
                self._duckdb.execute(f"LOAD {ext}")
                logger.debug(f"Loaded DuckDB extension: {ext}")
            except Exception as e:
                logger.warning(f"Failed to load extension {ext}: {e}")
    
    def _configure_credentials(self):
        """Configure DuckDB with cloud credentials."""
        creds = self._credentials
        
        if self._provider == CloudProvider.S3:
            self._configure_s3(creds)
        elif self._provider == CloudProvider.GCS:
            self._configure_gcs(creds)
        elif self._provider == CloudProvider.AZURE:
            self._configure_azure(creds)
    
    def _configure_s3(self, creds: CloudCredentials):
        """Configure S3 credentials in DuckDB."""
        if creds.anonymous:
            self._duckdb.execute("SET s3_access_key_id = ''")
            self._duckdb.execute("SET s3_secret_access_key = ''")
            return
        
        if creds.aws_access_key_id:
            self._duckdb.execute(f"SET s3_access_key_id = '{creds.aws_access_key_id}'")
        if creds.aws_secret_access_key:
            self._duckdb.execute(f"SET s3_secret_access_key = '{creds.aws_secret_access_key}'")
        if creds.aws_session_token:
            self._duckdb.execute(f"SET s3_session_token = '{creds.aws_session_token}'")
        if creds.aws_region:
            self._duckdb.execute(f"SET s3_region = '{creds.aws_region}'")
        if creds.aws_endpoint:
            # Extract host:port from endpoint URL (remove http:// or https:// prefix)
            endpoint = creds.aws_endpoint
            if endpoint.startswith("http://"):
                endpoint = endpoint[7:]  # Remove "http://"
            elif endpoint.startswith("https://"):
                endpoint = endpoint[8:]  # Remove "https://"
            self._duckdb.execute(f"SET s3_endpoint = '{endpoint}'")
            # For S3-compatible storage, often need path-style URLs
            self._duckdb.execute("SET s3_url_style = 'path'")
        
        # Configure SSL (default is true, set to false for local emulators)
        if not creds.use_ssl:
            self._duckdb.execute("SET s3_use_ssl = false")
    
    def _configure_gcs(self, creds: CloudCredentials):
        """Configure GCS credentials in DuckDB."""
        # DuckDB uses GOOGLE_APPLICATION_CREDENTIALS env var for GCS
        if creds.gcs_service_account_json:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds.gcs_service_account_json
    
    def _configure_azure(self, creds: CloudCredentials):
        """Configure Azure credentials in DuckDB."""
        if creds.azure_connection_string:
            self._duckdb.execute(f"SET azure_storage_connection_string = '{creds.azure_connection_string}'")
        else:
            if creds.azure_storage_account:
                self._duckdb.execute(f"SET azure_storage_account_name = '{creds.azure_storage_account}'")
            if creds.azure_storage_key:
                self._duckdb.execute(f"SET azure_storage_account_key = '{creds.azure_storage_key}'")
    
    async def fetch_async(self, *args, **kwargs) -> pa.Table:
        """Fetch data from cloud storage (async)."""
        if anyio is None:
             raise ImportError("anyio is required for async operations")
        return await anyio.to_thread.run_sync(lambda: self.fetch(*args, **kwargs))
    
    def fetch(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        """Fetch data from cloud storage with predicate pushdown."""
        # Resolve the full URI
        full_uri = self._resolve_uri(table)
        
        # Build SQL query
        sql = self._build_query(
            full_uri, columns, predicates, limit, offset, 
            order_by, group_by, aggregates
        )
        
        logger.debug(f"Executing cloud query: {sql}")
        
        try:
            result = self._duckdb.execute(sql)
            return result.fetch_arrow_table()
        except Exception as e:
            raise AdapterError(f"Failed to read from cloud storage: {e}")
    
    def _resolve_uri(self, table: str) -> str:
        """Resolve table name to full cloud URI."""
        # If table looks like a full URI, use it directly
        if table.startswith(("s3://", "gs://", "azure://", "az://", "http")):
            return table
        
        # Otherwise, append to base URI
        base = self._uri.rstrip("/")
        table = table.lstrip("/")
        return f"{base}/{table}"
    
    def _build_query(
        self,
        uri: str,
        columns: List[str],
        predicates: List["Predicate"],
        limit: int,
        offset: int,
        order_by: List[tuple],
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> str:
        """Build DuckDB SQL query for cloud data."""
        # Column selection
        if aggregates or group_by:
            select_parts = []
            if group_by:
                select_parts.extend(group_by)
            if aggregates:
                for agg in aggregates:
                    expr = f"{agg.func}({agg.column})"
                    if agg.alias:
                        expr += f" AS {agg.alias}"
                    select_parts.append(expr)
            if not select_parts:
                select_parts = ["*"]
            cols = ", ".join(select_parts)
        else:
            cols = ", ".join(columns) if columns and columns != ["*"] else "*"
        
        # Source expression based on format
        source = self._get_source_expression(uri)
        
        sql = f"SELECT {cols} FROM {source}"
        
        # WHERE clause
        if predicates:
            where_parts = []
            for pred in predicates:
                where_parts.append(self._predicate_to_sql(pred))
            sql += " WHERE " + " AND ".join(where_parts)
        
        # GROUP BY
        if group_by:
            sql += " GROUP BY " + ", ".join(group_by)
        
        # ORDER BY
        if order_by:
            order_parts = [f"{col} {direction}" for col, direction in order_by]
            sql += " ORDER BY " + ", ".join(order_parts)
        
        # LIMIT/OFFSET
        if limit:
            sql += f" LIMIT {limit}"
        if offset:
            sql += f" OFFSET {offset}"
        
        return sql
    
    def _get_source_expression(self, uri: str) -> str:
        """Get DuckDB source expression for the given format."""
        if self._format == TableFormat.DELTA:
            return f"delta_scan('{uri}')"
        elif self._format == TableFormat.ICEBERG:
            if self._iceberg_catalog:
                return f"iceberg_scan('{uri}', catalog='{self._iceberg_catalog}')"
            return f"iceberg_scan('{uri}')"
        elif self._format == TableFormat.PARQUET:
            # Support glob patterns for multiple files
            if "*" in uri:
                # URI already contains a glob pattern, use it directly
                return f"read_parquet('{uri}')"
            elif uri.endswith("/"):
                # Directory, add glob pattern for parquet files
                return f"read_parquet('{uri}*.parquet')"
            return f"read_parquet('{uri}')"
        elif self._format == TableFormat.CSV:
            return f"read_csv_auto('{uri}')"
        elif self._format == TableFormat.JSON:
            return f"read_json_auto('{uri}')"
        else:
            return f"read_parquet('{uri}')"
    
    def _predicate_to_sql(self, pred: "Predicate") -> str:
        """Convert predicate to SQL WHERE clause part."""
        if pred.value is None:
            if pred.operator == "IS NULL":
                return f"{pred.column} IS NULL"
            return f"{pred.column} IS NOT NULL"
        
        if pred.operator == "IN":
            if isinstance(pred.value, (list, tuple)):
                values = ", ".join(
                    f"'{v}'" if isinstance(v, str) else str(v) 
                    for v in pred.value
                )
                return f"{pred.column} IN ({values})"
        
        value = f"'{pred.value}'" if isinstance(pred.value, str) else str(pred.value)
        return f"{pred.column} {pred.operator} {value}"
    
    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Get schema from cloud data (async)."""
        if anyio is None:
             raise ImportError("anyio is required for async operations")
        return await anyio.to_thread.run_sync(lambda: self.get_schema(table))
    
    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Get schema from cloud data."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        uri = self._resolve_uri(table)
        source = self._get_source_expression(uri)
        sql = f"DESCRIBE SELECT * FROM {source}"
        
        try:
            result = self._duckdb.execute(sql).fetchall()
            columns = [
                ColumnInfo(
                    name=row[0],
                    data_type=row[1],
                    nullable=row[2] == "YES" if len(row) > 2 else True,
                )
                for row in result
            ]
            
            self._cache_schema(table, columns)
            return columns
        except Exception as e:
            raise AdapterError(f"Failed to get schema from cloud storage: {e}")
    
    def list_tables(self) -> List[str]:
        """
        List available tables/files.
        
        For Parquet/CSV: Lists files in the path
        For Delta/Iceberg: Returns the table name
        """
        if self._format in (TableFormat.DELTA, TableFormat.ICEBERG):
            # For table formats, the URI itself is the table
            return [self._uri.split("/")[-1] or "table"]
        
        # For file formats, try to list files using glob
        try:
            source = self._get_source_expression(self._uri)
            result = self._duckdb.execute(f"SELECT DISTINCT filename FROM {source}").fetchall()
            return [Path(row[0]).stem for row in result]
        except Exception:
            # Fallback: just return the base name
            return [self._uri.split("/")[-1] or "data"]
    
    def get_table_metadata(self, table: str = None) -> Dict[str, Any]:
        """
        Get metadata about the table (especially useful for Delta/Iceberg).
        
        Returns format-specific metadata like:
        - Delta: version, partitioning, statistics
        - Iceberg: snapshot, partition spec, schema evolution history
        """
        uri = self._resolve_uri(table) if table else self._uri
        
        metadata = {
            "uri": uri,
            "format": self._format.value,
            "provider": self._provider.value,
        }
        
        if self._format == TableFormat.DELTA:
            try:
                # Get Delta table version and info
                result = self._duckdb.execute(
                    f"SELECT * FROM delta_scan('{uri}', version=0) LIMIT 0"
                )
                metadata["delta_info"] = "Delta table detected"
            except Exception as e:
                metadata["delta_error"] = str(e)
        elif self._format == TableFormat.ICEBERG:
            try:
                # Get Iceberg metadata
                result = self._duckdb.execute(
                    f"SELECT * FROM iceberg_metadata('{uri}')"
                ).fetchall()
                metadata["iceberg_metadata"] = result
            except Exception as e:
                metadata["iceberg_error"] = str(e)
        
        return metadata


# Convenience factory functions

def s3_adapter(
    bucket: str,
    prefix: str = "",
    format: str = "parquet",
    region: str = None,
    access_key: str = None,
    secret_key: str = None,
    **kwargs
) -> CloudStorageAdapter:
    """
    Create an S3 adapter with simplified parameters.
    
    Args:
        bucket: S3 bucket name
        prefix: Path prefix within bucket
        format: Data format (parquet, csv, json, delta, iceberg)
        region: AWS region
        access_key: AWS access key ID
        secret_key: AWS secret access key
        **kwargs: Additional parameters
        
    Returns:
        CloudStorageAdapter configured for S3
    """
    uri = f"s3://{bucket}/{prefix}".rstrip("/")
    
    credentials = CloudCredentials(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_region=region,
    ) if access_key or secret_key or region else None
    
    return CloudStorageAdapter(uri, format=format, credentials=credentials, **kwargs)


def gcs_adapter(
    bucket: str,
    prefix: str = "",
    format: str = "parquet",
    project_id: str = None,
    service_account_json: str = None,
    **kwargs
) -> CloudStorageAdapter:
    """
    Create a GCS adapter with simplified parameters.
    
    Args:
        bucket: GCS bucket name
        prefix: Path prefix within bucket
        format: Data format (parquet, csv, json, delta, iceberg)
        project_id: GCP project ID
        service_account_json: Path to service account JSON file
        **kwargs: Additional parameters
        
    Returns:
        CloudStorageAdapter configured for GCS
    """
    uri = f"gs://{bucket}/{prefix}".rstrip("/")
    
    credentials = CloudCredentials(
        gcs_project_id=project_id,
        gcs_service_account_json=service_account_json,
    ) if project_id or service_account_json else None
    
    return CloudStorageAdapter(uri, format=format, credentials=credentials, **kwargs)


def azure_adapter(
    container: str,
    storage_account: str,
    prefix: str = "",
    format: str = "parquet",
    storage_key: str = None,
    sas_token: str = None,
    **kwargs
) -> CloudStorageAdapter:
    """
    Create an Azure Blob adapter with simplified parameters.
    
    Args:
        container: Azure container name
        storage_account: Azure storage account name
        prefix: Path prefix within container
        format: Data format (parquet, csv, json, delta, iceberg)
        storage_key: Azure storage account key
        sas_token: SAS token for authentication
        **kwargs: Additional parameters
        
    Returns:
        CloudStorageAdapter configured for Azure
    """
    uri = f"azure://{container}@{storage_account}.blob.core.windows.net/{prefix}".rstrip("/")
    
    credentials = CloudCredentials(
        azure_storage_account=storage_account,
        azure_storage_key=storage_key,
        azure_sas_token=sas_token,
    ) if storage_account or storage_key or sas_token else None
    
    return CloudStorageAdapter(uri, format=format, credentials=credentials, **kwargs)


def delta_table(uri: str, **kwargs) -> CloudStorageAdapter:
    """
    Create a Delta Lake table adapter.
    
    Args:
        uri: URI to Delta table (s3://, gs://, azure://, or local path)
        **kwargs: Additional parameters
        
    Returns:
        CloudStorageAdapter configured for Delta Lake
    """
    return CloudStorageAdapter(uri, format="delta", **kwargs)


def iceberg_table(
    uri: str,
    catalog: str = None,
    **kwargs
) -> CloudStorageAdapter:
    """
    Create an Apache Iceberg table adapter.
    
    Args:
        uri: URI to Iceberg table metadata
        catalog: Catalog type (glue, hive, rest)
        **kwargs: Additional parameters
        
    Returns:
        CloudStorageAdapter configured for Iceberg
    """
    return CloudStorageAdapter(uri, format="iceberg", iceberg_catalog=catalog, **kwargs)
