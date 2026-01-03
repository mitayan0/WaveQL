"""
Adapter Registry - Central registry for all adapters
"""

from typing import Dict, Type, Optional
from waveql.adapters.base import BaseAdapter

# Global adapter registry
_ADAPTER_REGISTRY: Dict[str, Type[BaseAdapter]] = {}


def register_adapter(name: str, adapter_class: Type[BaseAdapter]):
    """
    Register an adapter class.
    
    Args:
        name: Adapter name (used in connection strings)
        adapter_class: Adapter class to register
    """
    _ADAPTER_REGISTRY[name.lower()] = adapter_class


def get_adapter_class(name: str) -> Optional[Type[BaseAdapter]]:
    """
    Get an adapter class by name.
    
    Args:
        name: Adapter name
        
    Returns:
        Adapter class or None
    """
    return _ADAPTER_REGISTRY.get(name.lower())


def get_adapter(name: str) -> Optional[BaseAdapter]:
    """
    Get adapter class (alias for get_adapter_class for compatibility).
    """
    return get_adapter_class(name)


def list_adapters() -> list:
    """List all registered adapter names."""
    return list(_ADAPTER_REGISTRY.keys())


# Auto-register built-in adapters
def _register_builtin_adapters():
    """Register all built-in adapters."""
    try:
        from waveql.adapters.servicenow import ServiceNowAdapter
        register_adapter("servicenow", ServiceNowAdapter)
    except ImportError:
        pass
    
    try:
        from waveql.adapters.rest_adapter import RESTAdapter
        register_adapter("rest", RESTAdapter)
        register_adapter("http", RESTAdapter)
        register_adapter("https", RESTAdapter)
    except ImportError:
        pass
    
    try:
        from waveql.adapters.file_adapter import FileAdapter
        register_adapter("file", FileAdapter)
        register_adapter("csv", FileAdapter)
        register_adapter("parquet", FileAdapter)
    except ImportError:
        pass

    try:
        from waveql.adapters.salesforce import SalesforceAdapter
        register_adapter("salesforce", SalesforceAdapter)
        register_adapter("sf", SalesforceAdapter)
    except ImportError:
        pass

    try:
        from waveql.adapters.jira import JiraAdapter
        register_adapter("jira", JiraAdapter)
        register_adapter("atlassian", JiraAdapter)
    except ImportError:
        pass

    try:
        from waveql.adapters.sql import SQLAdapter
        register_adapter("sql", SQLAdapter)
        register_adapter("mysql", SQLAdapter)
        register_adapter("postgresql", SQLAdapter)
        register_adapter("postgres", SQLAdapter)
        register_adapter("mssql", SQLAdapter)
    except ImportError:
        pass


_register_builtin_adapters()

