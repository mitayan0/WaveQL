
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy import types as sqltypes
import waveql

class WaveQLDialect(DefaultDialect):
    name = "waveql"
    driver = "waveql"
    
    # DB-API 2.0 globals
    returns_unicode_strings = True
    supports_native_boolean = True
    supports_statement_cache = True
    
    @classmethod
    def import_dbapi(cls):
        return waveql

    def create_connect_args(self, url):
        # url is a sqlalchemy.engine.url.URL object
        # Example: waveql://host:port/database?param=value
        
        opts = url.translate_connect_args(username='username', password='password', host='host')
        opts.update(url.query)
        
        # Determine the adapter from the host part if needed, or from a query param
        # Actually, WaveQL usually expects "adapter://host"
        # In SQLAlchemy, it's waveql+adapter://...
        
        adapter = url.drivername.split("+")[-1] if "+" in url.drivername else None
        
        # If the drivername is just 'waveql', we rely on the host or a param
        connect_args = {
            "adapter": adapter,
            "host": opts.pop("host", None),
            "username": opts.pop("username", None),
            "password": opts.pop("password", None),
            **opts
        }
        
        return ([], connect_args)

    def get_schema_names(self, connection, **kw):
        # In WaveQL, schemas are often adapter names
        return ["default"] + list(connection.connection._adapters.keys())

    def get_table_names(self, connection, schema=None, **kw):
        adapter = connection.connection.get_adapter(schema or "default")
        if adapter:
            return adapter.list_tables()
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        adapter = connection.connection.get_adapter(schema or "default")
        if not adapter:
            return []
            
        columns = adapter.get_schema(table_name)
        result = []
        for col in columns:
            result.append({
                "name": col.name,
                "type": self._map_type(col.data_type),
                "nullable": col.nullable,
                "default": None,
                "primary_key": col.primary_key
            })
        return result

    def _map_type(self, waveql_type):
        type_map = {
            "string": sqltypes.String,
            "integer": sqltypes.Integer,
            "float": sqltypes.Float,
            "boolean": sqltypes.Boolean,
            "decimal": sqltypes.Numeric,
            "date": sqltypes.Date,
            "datetime": sqltypes.DateTime,
        }
        return type_map.get(waveql_type, sqltypes.String)

    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name in self.get_table_names(connection, schema)

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        cols = self.get_columns(connection, table_name, schema)
        pk = [c["name"] for c in cols if c.get("primary_key")]
        return {"constrained_columns": pk, "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return {"text": None}
