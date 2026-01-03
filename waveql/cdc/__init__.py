"""
Change Data Capture (CDC) - Stream changes from data sources

This module provides real-time change streaming functionality,
allowing users to receive incremental updates from data sources
instead of polling full datasets.
"""

from waveql.cdc.models import Change, ChangeType, ChangeStream
from waveql.cdc.stream import CDCStream, CDCConfig
from waveql.cdc.providers import (
    BaseCDCProvider,
    ServiceNowCDCProvider,
    SalesforceCDCProvider,
    JiraCDCProvider,
)

__all__ = [
    "Change",
    "ChangeType",
    "ChangeStream",
    "CDCStream",
    "CDCConfig",
    "BaseCDCProvider",
    "ServiceNowCDCProvider",
    "SalesforceCDCProvider",
    "JiraCDCProvider",
]
