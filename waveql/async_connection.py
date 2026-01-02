
from __future__ import annotations
from typing import Any, Dict, Optional, TYPE_CHECKING
from urllib.parse import urlparse, parse_qs
import duckdb

from waveql.exceptions import ConnectionError, AdapterError
from waveql.schema_cache import SchemaCache
from waveql.auth.manager import AuthManager
from waveql.async_cursor import AsyncWaveQLCursor

if TYPE_CHECKING:
    from waveql.adapters.base import BaseAdapter


class AsyncWaveQLConnection:
    """Async version of WaveQLConnection."""
    
    def __init__(
        self,
        connection_string: str = None,
        adapter: str = None,
        host: str = None,
        username: str = None,
        password: str = None,
        api_key: str = None,
        oauth_token: str = None,
        **kwargs
    ):
        if connection_string:
            from waveql.connection import WaveQLConnection
            # Reuse parsing logic from sync connection if possible or redefine
            parsed = self._parse_connection_string(connection_string)
            adapter = adapter or parsed.get("adapter")
            host = host or parsed.get("host")
            kwargs = {**parsed.get("params", {}), **kwargs}
        
        self._adapter_name = adapter
        self._host = host
        self._kwargs = kwargs
        self._closed = False
        self._duckdb = duckdb.connect(":memory:")
        self._schema_cache = SchemaCache()
        
        oauth_params = {k: v for k, v in kwargs.items() if k.startswith("oauth_") or k.startswith("auth_")}
        self._auth_manager = AuthManager(
            username=username, password=password, api_key=api_key, oauth_token=oauth_token, **oauth_params
        )
        self._adapters: Dict[str, BaseAdapter] = {}
        if adapter:
            self._init_default_adapter(adapter, host, **kwargs)

    def _parse_connection_string(self, conn_str: str) -> Dict[str, Any]:
        if conn_str.startswith("file://"):
            return {"adapter": "file", "host": conn_str[7:], "params": {}}
        parsed = urlparse(conn_str)
        params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}
        return {"adapter": parsed.scheme, "host": parsed.netloc or parsed.path, "params": params}

    def _init_default_adapter(self, adapter_name: str, host: str, **kwargs):
        from waveql.adapters import get_adapter_class
        adapter_class = get_adapter_class(adapter_name)
        if not adapter_class: raise AdapterError(f"Unknown adapter: {adapter_name}")
        adapter = adapter_class(host=host, auth_manager=self._auth_manager, schema_cache=self._schema_cache, **kwargs)
        self._adapters["default"] = adapter

    async def cursor(self) -> AsyncWaveQLCursor:
        if self._closed: raise ConnectionError("Connection is closed")
        return AsyncWaveQLCursor(self)

    def get_adapter(self, name: str = "default") -> Optional["BaseAdapter"]:
        return self._adapters.get(name)

    async def close(self):
        if not self._closed:
            self._duckdb.close()
            self._schema_cache.close()
            self._closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @property
    def duckdb(self):
        return self._duckdb
