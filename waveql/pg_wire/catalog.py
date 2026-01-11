"""
PostgreSQL Catalog Emulation (pg_catalog)

Emulates the pg_catalog schema tables that BI tools query to discover
database structure. This allows tools like Tableau, PowerBI, and DBeaver
to introspect WaveQL schemas as if they were PostgreSQL tables.

Key Tables Emulated:
- pg_catalog.pg_namespace (schemas)
- pg_catalog.pg_class (tables/views)
- pg_catalog.pg_attribute (columns)
- pg_catalog.pg_type (data types)
- pg_catalog.pg_database (databases)
"""

from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pyarrow as pa

from waveql.pg_wire.type_mapping import PG_TYPES, arrow_to_pg_oid

if TYPE_CHECKING:
    from waveql.connection import WaveQLConnection

logger = logging.getLogger(__name__)


class PGCatalogEmulator:
    """
    Emulates PostgreSQL system catalog tables.
    
    BI tools like Tableau and DBeaver query these tables to discover
    available schemas, tables, and columns. We intercept queries to
    pg_catalog.* and return appropriate data from WaveQL's schema cache.
    """
    
    # Namespace OIDs (schema OIDs)
    _PG_CATALOG_OID = 11
    _PUBLIC_OID = 2200
    _INFO_SCHEMA_OID = 12428
    
    def __init__(self, connection: "WaveQLConnection"):
        """
        Initialize catalog emulator.
        
        Args:
            connection: WaveQL connection to introspect
        """
        self._connection = connection
        self._next_oid = 16384  # Start after reserved OIDs
        
        # Cache OID assignments for consistency
        self._schema_oids: Dict[str, int] = {}
        self._table_oids: Dict[Tuple[str, str], int] = {}  # (schema, table) -> oid
    
    def _get_schema_oid(self, schema_name: str) -> int:
        """Get or assign OID for a schema."""
        if schema_name not in self._schema_oids:
            self._schema_oids[schema_name] = self._next_oid
            self._next_oid += 1
        return self._schema_oids[schema_name]
    
    def _get_table_oid(self, schema: str, table: str) -> int:
        """Get or assign OID for a table."""
        key = (schema, table)
        if key not in self._table_oids:
            self._table_oids[key] = self._next_oid
            self._next_oid += 1
        return self._table_oids[key]
    
    def handle_catalog_query(self, table_name: str, columns: List[str] = None) -> Optional[pa.Table]:
        """
        Handle a query against a pg_catalog table.
        
        Args:
            table_name: Catalog table name (e.g., "pg_namespace")
            columns: Requested columns (None = all)
            
        Returns:
            PyArrow table with results, or None if not a catalog table
        """
        handlers = {
            "pg_namespace": self._get_pg_namespace,
            "pg_class": self._get_pg_class,
            "pg_attribute": self._get_pg_attribute,
            "pg_type": self._get_pg_type,
            "pg_database": self._get_pg_database,
            "pg_tables": self._get_pg_tables,
            "pg_views": self._get_pg_views,
            "pg_indexes": self._get_pg_indexes,
            "pg_constraint": self._get_pg_constraint,
            "pg_description": self._get_pg_description,
            "pg_settings": self._get_pg_settings,
            "pg_stat_user_tables": self._get_pg_stat_user_tables,
            "pg_proc": self._get_pg_proc,
            "pg_am": self._get_pg_am,
            "pg_extension": self._get_pg_extension,
        }
        
        handler = handlers.get(table_name.lower())
        if handler:
            result = handler()
            # Filter columns if requested
            if columns and result is not None:
                available = set(result.column_names)
                requested = [c for c in columns if c in available]
                if requested:
                    result = result.select(requested)
            return result
        
        return None
    
    def _get_adapters(self) -> Dict[str, Any]:
        """Get all registered adapters from connection."""
        return getattr(self._connection, "_adapters", {})
    
    def _get_pg_namespace(self) -> pa.Table:
        """
        Generate pg_namespace (schemas).
        
        BI tools query this to list available schemas.
        """
        data = {
            "oid": [self._PG_CATALOG_OID, self._PUBLIC_OID],
            "nspname": ["pg_catalog", "public"],
            "nspowner": [10, 10],  # postgres user OID
            "nspacl": [None, None],
        }
        
        # Add adapter schemas
        for adapter_name in self._get_adapters():
            if adapter_name != "default":
                oid = self._get_schema_oid(adapter_name)
                data["oid"].append(oid)
                data["nspname"].append(adapter_name)
                data["nspowner"].append(10)
                data["nspacl"].append(None)
        
        return pa.Table.from_pydict(data)
    
    def _get_pg_class(self) -> pa.Table:
        """
        Generate pg_class (tables and views).
        
        relkind values:
        - 'r' = ordinary table
        - 'v' = view
        - 'i' = index
        - 'm' = materialized view
        """
        data = {
            "oid": [],
            "relname": [],
            "relnamespace": [],
            "reltype": [],
            "relowner": [],
            "relkind": [],
            "reltuples": [],
            "relhasindex": [],
            "relpersistence": [],
            "relispartition": [],
        }
        
        # Add tables from each adapter
        for adapter_name, adapter in self._get_adapters().items():
            schema_oid = self._get_schema_oid(adapter_name) if adapter_name != "default" else self._PUBLIC_OID
            
            # Try to get tables from schema cache or adapter
            try:
                tables = self._get_adapter_tables(adapter_name, adapter)
                for table_name in tables:
                    oid = self._get_table_oid(adapter_name, table_name)
                    data["oid"].append(oid)
                    data["relname"].append(table_name)
                    data["relnamespace"].append(schema_oid)
                    data["reltype"].append(0)
                    data["relowner"].append(10)
                    data["relkind"].append("r")  # ordinary table
                    data["reltuples"].append(-1.0)  # unknown
                    data["relhasindex"].append(False)
                    data["relpersistence"].append("p")  # permanent
                    data["relispartition"].append(False)
            except Exception as e:
                logger.debug(f"Failed to get tables for adapter {adapter_name}: {e}")
        
        # Add materialized views
        try:
            views = self._connection.list_materialized_views()
            for view in views:
                oid = self._get_table_oid("public", view["name"])
                data["oid"].append(oid)
                data["relname"].append(view["name"])
                data["relnamespace"].append(self._PUBLIC_OID)
                data["reltype"].append(0)
                data["relowner"].append(10)
                data["relkind"].append("m")  # materialized view
                data["reltuples"].append(float(view.get("row_count", -1)))
                data["relhasindex"].append(False)
                data["relpersistence"].append("p")
                data["relispartition"].append(False)
        except Exception:
            pass
        
        return pa.Table.from_pydict(data)
    
    def _get_adapter_tables(self, adapter_name: str, adapter) -> List[str]:
        """Get list of tables from an adapter."""
        # Try schema cache first
        if hasattr(self._connection, "_schema_cache") and self._connection._schema_cache:
            tables = self._connection._schema_cache.list_tables(adapter_name)
            if tables:
                return tables
        
        # Try adapter's list_tables method if available
        if hasattr(adapter, "list_tables"):
            try:
                return adapter.list_tables()
            except Exception:
                pass
        
        return []
    
    def _get_pg_attribute(self) -> pa.Table:
        """
        Generate pg_attribute (columns).
        
        BI tools query this to discover column metadata.
        """
        data = {
            "attrelid": [],
            "attname": [],
            "atttypid": [],
            "attstattarget": [],
            "attlen": [],
            "attnum": [],
            "attndims": [],
            "attcacheoff": [],
            "atttypmod": [],
            "attbyval": [],
            "attalign": [],
            "attstorage": [],
            "attnotnull": [],
            "atthasdef": [],
            "attisdropped": [],
            "attislocal": [],
            "attinhcount": [],
        }
        
        # Add columns from each adapter
        for adapter_name, adapter in self._get_adapters().items():
            try:
                tables = self._get_adapter_tables(adapter_name, adapter)
                for table_name in tables:
                    table_oid = self._get_table_oid(adapter_name, table_name)
                    columns = self._get_table_columns(adapter_name, table_name, adapter)
                    
                    for i, col in enumerate(columns, start=1):
                        type_oid = self._map_column_type_to_oid(col)
                        
                        data["attrelid"].append(table_oid)
                        data["attname"].append(col.name if hasattr(col, 'name') else str(col))
                        data["atttypid"].append(type_oid)
                        data["attstattarget"].append(-1)
                        data["attlen"].append(-1)
                        data["attnum"].append(i)
                        data["attndims"].append(0)
                        data["attcacheoff"].append(-1)
                        data["atttypmod"].append(-1)
                        data["attbyval"].append(False)
                        data["attalign"].append("i")
                        data["attstorage"].append("x")
                        data["attnotnull"].append(not getattr(col, 'nullable', True))
                        data["atthasdef"].append(False)
                        data["attisdropped"].append(False)
                        data["attislocal"].append(True)
                        data["attinhcount"].append(0)
            except Exception as e:
                logger.debug(f"Failed to get columns for adapter {adapter_name}: {e}")
        
        return pa.Table.from_pydict(data)
    
    def _get_table_columns(self, adapter_name: str, table_name: str, adapter) -> List:
        """Get columns for a table."""
        # Try schema cache first
        if hasattr(self._connection, "_schema_cache") and self._connection._schema_cache:
            schema = self._connection._schema_cache.get(adapter_name, table_name)
            if schema:
                return schema.columns
        
        # Try adapter's get_schema method
        if hasattr(adapter, "get_schema"):
            try:
                return adapter.get_schema(table_name)
            except Exception:
                pass
        
        return []
    
    def _map_column_type_to_oid(self, column) -> int:
        """Map a ColumnInfo to PostgreSQL type OID."""
        if hasattr(column, 'arrow_type') and column.arrow_type:
            return arrow_to_pg_oid(column.arrow_type)
        
        # Map string type names
        type_name = getattr(column, 'data_type', 'text').lower()
        
        type_mapping = {
            "string": PG_TYPES["text"].oid,
            "text": PG_TYPES["text"].oid,
            "varchar": PG_TYPES["varchar"].oid,
            "integer": PG_TYPES["int4"].oid,
            "int": PG_TYPES["int4"].oid,
            "int32": PG_TYPES["int4"].oid,
            "int64": PG_TYPES["int8"].oid,
            "bigint": PG_TYPES["int8"].oid,
            "smallint": PG_TYPES["int2"].oid,
            "int16": PG_TYPES["int2"].oid,
            "float": PG_TYPES["float8"].oid,
            "float64": PG_TYPES["float8"].oid,
            "double": PG_TYPES["float8"].oid,
            "float32": PG_TYPES["float4"].oid,
            "real": PG_TYPES["float4"].oid,
            "boolean": PG_TYPES["bool"].oid,
            "bool": PG_TYPES["bool"].oid,
            "date": PG_TYPES["date"].oid,
            "timestamp": PG_TYPES["timestamp"].oid,
            "datetime": PG_TYPES["timestamp"].oid,
            "time": PG_TYPES["time"].oid,
            "json": PG_TYPES["jsonb"].oid,
            "jsonb": PG_TYPES["jsonb"].oid,
            "binary": PG_TYPES["bytea"].oid,
            "bytes": PG_TYPES["bytea"].oid,
            "uuid": PG_TYPES["uuid"].oid,
            "decimal": PG_TYPES["numeric"].oid,
            "numeric": PG_TYPES["numeric"].oid,
        }
        
        return type_mapping.get(type_name, PG_TYPES["text"].oid)
    
    def _get_pg_type(self) -> pa.Table:
        """Generate pg_type (data types)."""
        data = {
            "oid": [],
            "typname": [],
            "typnamespace": [],
            "typowner": [],
            "typlen": [],
            "typbyval": [],
            "typtype": [],
            "typcategory": [],
            "typisdefined": [],
            "typrelid": [],
            "typelem": [],
            "typarray": [],
            "typinput": [],
            "typoutput": [],
        }
        
        for pg_type in PG_TYPES.values():
            data["oid"].append(pg_type.oid)
            data["typname"].append(pg_type.name)
            data["typnamespace"].append(self._PG_CATALOG_OID)
            data["typowner"].append(10)
            data["typlen"].append(pg_type.size)
            data["typbyval"].append(pg_type.size > 0 and pg_type.size <= 8)
            data["typtype"].append("b")  # base type
            data["typcategory"].append(pg_type.category)
            data["typisdefined"].append(True)
            data["typrelid"].append(0)
            data["typelem"].append(0)
            data["typarray"].append(pg_type.array_oid)
            data["typinput"].append(f"{pg_type.name}in")
            data["typoutput"].append(f"{pg_type.name}out")
        
        return pa.Table.from_pydict(data)
    
    def _get_pg_database(self) -> pa.Table:
        """Generate pg_database."""
        return pa.Table.from_pydict({
            "oid": [16384],
            "datname": ["waveql"],
            "datdba": [10],
            "encoding": [6],  # UTF8
            "datcollate": ["en_US.UTF-8"],
            "datctype": ["en_US.UTF-8"],
            "datistemplate": [False],
            "datallowconn": [True],
            "datconnlimit": [-1],
            "datlastsysoid": [16383],
            "datfrozenxid": [0],
            "datminmxid": [0],
        })
    
    def _get_pg_tables(self) -> pa.Table:
        """Generate pg_tables view."""
        data = {
            "schemaname": [],
            "tablename": [],
            "tableowner": [],
            "tablespace": [],
            "hasindexes": [],
            "hasrules": [],
            "hastriggers": [],
        }
        
        for adapter_name, adapter in self._get_adapters().items():
            schema = adapter_name if adapter_name != "default" else "public"
            try:
                tables = self._get_adapter_tables(adapter_name, adapter)
                for table_name in tables:
                    data["schemaname"].append(schema)
                    data["tablename"].append(table_name)
                    data["tableowner"].append("postgres")
                    data["tablespace"].append(None)
                    data["hasindexes"].append(False)
                    data["hasrules"].append(False)
                    data["hastriggers"].append(False)
            except Exception:
                pass
        
        return pa.Table.from_pydict(data)
    
    def _get_pg_views(self) -> pa.Table:
        """Generate pg_views."""
        data = {
            "schemaname": [],
            "viewname": [],
            "viewowner": [],
            "definition": [],
        }
        
        # Add materialized views
        try:
            views = self._connection.list_materialized_views()
            for view in views:
                data["schemaname"].append("public")
                data["viewname"].append(view["name"])
                data["viewowner"].append("postgres")
                data["definition"].append(view.get("query", ""))
        except Exception:
            pass
        
        return pa.Table.from_pydict(data)
    
    def _get_pg_indexes(self) -> pa.Table:
        """Generate pg_indexes (empty - no indexes in API sources)."""
        return pa.Table.from_pydict({
            "schemaname": [],
            "tablename": [],
            "indexname": [],
            "tablespace": [],
            "indexdef": [],
        })
    
    def _get_pg_constraint(self) -> pa.Table:
        """Generate pg_constraint (empty - no constraints in API sources)."""
        return pa.Table.from_pydict({
            "oid": [],
            "conname": [],
            "connamespace": [],
            "contype": [],
            "condeferrable": [],
            "condeferred": [],
            "convalidated": [],
            "conrelid": [],
            "contypid": [],
            "conindid": [],
            "confrelid": [],
            "confupdtype": [],
            "confdeltype": [],
            "confmatchtype": [],
            "conislocal": [],
            "coninhcount": [],
            "connoinherit": [],
            "conkey": [],
            "confkey": [],
        })
    
    def _get_pg_description(self) -> pa.Table:
        """Generate pg_description (empty - no descriptions by default)."""
        return pa.Table.from_pydict({
            "objoid": [],
            "classoid": [],
            "objsubid": [],
            "description": [],
        })
    
    def _get_pg_settings(self) -> pa.Table:
        """Generate pg_settings (configuration parameters)."""
        settings = [
            ("server_version", "15.0", "WaveQL PostgreSQL compatibility version"),
            ("server_encoding", "UTF8", "Server character set encoding"),
            ("client_encoding", "UTF8", "Client character set encoding"),
            ("DateStyle", "ISO, MDY", "Date and time formatting style"),
            ("TimeZone", "UTC", "Server time zone"),
            ("standard_conforming_strings", "on", "Cause strings to treat backslashes literally"),
            ("integer_datetimes", "on", "Datetimes are stored in 64-bit integers"),
            ("max_connections", "100", "Maximum number of connections"),
        ]
        
        return pa.Table.from_pydict({
            "name": [s[0] for s in settings],
            "setting": [s[1] for s in settings],
            "description": [s[2] for s in settings],
        })
    
    def _get_pg_stat_user_tables(self) -> pa.Table:
        """Generate pg_stat_user_tables (table statistics)."""
        data = {
            "relid": [],
            "schemaname": [],
            "relname": [],
            "seq_scan": [],
            "seq_tup_read": [],
            "idx_scan": [],
            "idx_tup_fetch": [],
            "n_tup_ins": [],
            "n_tup_upd": [],
            "n_tup_del": [],
            "n_live_tup": [],
            "n_dead_tup": [],
        }
        
        for adapter_name, adapter in self._get_adapters().items():
            schema = adapter_name if adapter_name != "default" else "public"
            try:
                tables = self._get_adapter_tables(adapter_name, adapter)
                for table_name in tables:
                    oid = self._get_table_oid(adapter_name, table_name)
                    data["relid"].append(oid)
                    data["schemaname"].append(schema)
                    data["relname"].append(table_name)
                    data["seq_scan"].append(0)
                    data["seq_tup_read"].append(0)
                    data["idx_scan"].append(None)
                    data["idx_tup_fetch"].append(None)
                    data["n_tup_ins"].append(0)
                    data["n_tup_upd"].append(0)
                    data["n_tup_del"].append(0)
                    data["n_live_tup"].append(0)
                    data["n_dead_tup"].append(0)
            except Exception:
                pass
        
        return pa.Table.from_pydict(data)
    
    def _get_pg_proc(self) -> pa.Table:
        """Generate pg_proc (functions - empty)."""
        return pa.Table.from_pydict({
            "oid": [],
            "proname": [],
            "pronamespace": [],
            "proowner": [],
            "prolang": [],
            "prorettype": [],
            "pronargs": [],
            "proargtypes": [],
        })
    
    def _get_pg_am(self) -> pa.Table:
        """Generate pg_am (access methods)."""
        return pa.Table.from_pydict({
            "oid": [2, 403],
            "amname": ["heap", "btree"],
            "amhandler": [0, 0],
            "amtype": ["t", "i"],
        })
    
    def _get_pg_extension(self) -> pa.Table:
        """Generate pg_extension (extensions - empty)."""
        return pa.Table.from_pydict({
            "oid": [],
            "extname": [],
            "extowner": [],
            "extnamespace": [],
            "extrelocatable": [],
            "extversion": [],
        })
    
    def is_catalog_query(self, sql: str) -> bool:
        """
        Check if a SQL query targets pg_catalog tables.
        
        Args:
            sql: SQL query string
            
        Returns:
            True if query targets catalog tables
        """
        sql_lower = sql.lower()
        catalog_patterns = [
            "pg_catalog.",
            "information_schema.",
            "from pg_",
            "from information_schema",
        ]
        return any(pattern in sql_lower for pattern in catalog_patterns)
    
    def get_version_info(self) -> Dict[str, str]:
        """Get PostgreSQL version information for compatibility."""
        return {
            "server_version": "15.0",
            "server_version_num": "150000",
            "server_encoding": "UTF8",
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "TimeZone": "UTC",
            "integer_datetimes": "on",
            "standard_conforming_strings": "on",
            "application_name": "WaveQL",
            "is_superuser": "on",
            "session_authorization": "postgres",
        }
