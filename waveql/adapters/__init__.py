"""WaveQL Adapters Package"""

from waveql.adapters.base import BaseAdapter
from waveql.adapters.registry import register_adapter, get_adapter_class, get_adapter

# Cloud Storage Adapters
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

# Google Sheets Adapter
from waveql.adapters.google_sheets import (
    GoogleSheetsAdapter,
    GoogleSheetsCredentials,
)

# SaaS Adapters
from waveql.adapters.hubspot import HubSpotAdapter
from waveql.adapters.shopify import ShopifyAdapter
from waveql.adapters.zendesk import ZendeskAdapter
from waveql.adapters.stripe import StripeAdapter

__all__ = [
    # Base
    "BaseAdapter",
    "register_adapter",
    "get_adapter_class",
    "get_adapter",
    # Cloud Storage
    "CloudStorageAdapter",
    "CloudCredentials",
    "CloudProvider",
    "TableFormat",
    "s3_adapter",
    "gcs_adapter",
    "azure_adapter",
    "delta_table",
    "iceberg_table",
    # Google Sheets
    "GoogleSheetsAdapter",
    "GoogleSheetsCredentials",
    # SaaS
    "HubSpotAdapter",
    "ShopifyAdapter",
    "ZendeskAdapter",
    "StripeAdapter",
]
