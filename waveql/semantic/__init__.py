"""
WaveQL Semantic Integrations

This module provides semantic layer capabilities:
- Virtual Views: Reusable SQL views over API data
- Saved Queries: Parameterized SQL templates
- dbt Integration: Read dbt manifest.json to expose models as tables
"""

from waveql.semantic.views import VirtualView, VirtualViewRegistry
from waveql.semantic.saved_queries import SavedQuery, SavedQueryRegistry
from waveql.semantic.dbt import DbtManifest, DbtModel

__all__ = [
    "VirtualView",
    "VirtualViewRegistry",
    "SavedQuery",
    "SavedQueryRegistry",
    "DbtManifest",
    "DbtModel",
]
