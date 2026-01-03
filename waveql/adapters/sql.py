"""
SQL Adapter - Pass-through support for SQL databases via SQLAlchemy
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING
import sqlalchemy as sa
from sqlalchemy.engine import Engine, URL
import pandas as pd
import pyarrow as pa

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate


class SQLAdapter(BaseAdapter):
    """
    Generic SQL Adapter using SQLAlchemy.
    Supports MySQL, PostgreSQL, SQL Server, and others.
    """
    
    adapter_name = "sql"
    supports_predicate_pushdown = True
    supports_insert = True
    supports_update = True
    supports_delete = True
    
    def __init__(
        self,
        host: str,
        auth_manager=None,
        schema_cache=None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        # host argument is treated as the SQLAlchemy connection string
        self._connection_string = host
        self._engine: Optional[Engine] = None

    def _parse_table_path(self, table: str) -> Tuple[Optional[str], str]:
        """
        Extract schema and table name from a qualified table path.
        
        Args:
            table: Table name, optionally schema-qualified (e.g., 'schema.table' or '"schema"."table"')
            
        Returns:
            Tuple of (schema, table_name) where schema may be None
        """
        if "." in table:
            schema, table_name = table.rsplit(".", 1)
            return schema.strip('"'), table_name.strip('"')
        return None, table
        
    @property
    def engine(self) -> Engine:
        """Lazy-loaded SQLAlchemy engine."""
        if self._engine is None:
            self._engine = sa.create_engine(self._connection_string)
        return self._engine
        
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
        """Fetch data using SQLAlchemy."""
        metadata = sa.MetaData()
        
        try:
            # Parse schema-qualified table name
            schema, table_name = self._parse_table_path(table)
            
            sa_table = sa.Table(table_name, metadata, autoload_with=self.engine, schema=schema)
            
            # Columns
            if aggregates or group_by:
                selection = []
                if group_by:
                    for col in group_by:
                        selection.append(sa.column(col))
                
                if aggregates:
                    for agg in aggregates:
                        func_name = agg.func.lower()
                        col = sa.column(agg.column)
                        if func_name == "count":
                            selection.append(sa.func.count(col).label(agg.alias or f"count_{agg.column}"))
                        elif func_name == "sum":
                            selection.append(sa.func.sum(col).label(agg.alias or f"sum_{agg.column}"))
                        elif func_name == "min":
                            selection.append(sa.func.min(col).label(agg.alias or f"min_{agg.column}"))
                        elif func_name == "max":
                            selection.append(sa.func.max(col).label(agg.alias or f"max_{agg.column}"))
                        elif func_name == "avg":
                            selection.append(sa.func.avg(col).label(agg.alias or f"avg_{agg.column}"))
                
                query = sa.select(*selection).select_from(sa_table)
            elif columns and columns != ["*"]:
                cols = [sa_table.c[c] for c in columns if c in sa_table.c]
                query = sa.select(*cols)
            else:
                query = sa.select(sa_table)
            
            # Predicates
            if predicates:
                for pred in predicates:
                    clause = self._predicate_to_sa(pred, sa_table)
                    if clause is not None:
                        query = query.where(clause)
            
            # Group By
            if group_by:
                for col in group_by:
                    query = query.group_by(sa.column(col))

            # Order By
            if order_by:
                for col_name, direction in order_by:
                    col = sa.text(col_name) # Use text to handle generic names/aliases
                    if direction.upper() == "DESC":
                        query = query.order_by(col.desc())
                    else:
                        query = query.order_by(col.asc())
            
            # Limit/Offset
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)
            
            # Execute via Pandas for easy conversion to Arrow
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            # Convert to Arrow
            return pa.Table.from_pandas(df)
            
        except Exception as e:
            raise QueryError(f"SQL execution failed: {e}")

    def _predicate_to_sa(self, pred: "Predicate", table: sa.Table):
        """Convert predicate to SQLAlchemy expression."""
        if pred.column not in table.c:
            # Maybe it's a derived column or raw SQL, skip check?
            # For now return None or try strictly
            return None
            
        col = table.c[pred.column]
        op = pred.operator.upper()
        val = pred.value
        
        if op == "=":
            return col == val
        elif op == "!=":
            return col != val
        elif op == ">":
            return col > val
        elif op == "<":
            return col < val
        elif op == ">=":
            return col >= val
        elif op == "<=":
            return col <= val
        elif op == "LIKE":
            return col.like(val)
        elif op == "IN":
            return col.in_(val if isinstance(val, (list, tuple)) else [val])
        elif op == "IS NULL":
            return col.is_(None)
        elif op == "IS NOT NULL":
            return col.is_not(None)
            
        return None

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Discover schema via SQLAlchemy inspection."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
            
        inspector = sa.inspect(self.engine)
        
        schema, table_name = self._parse_table_path(table)

        columns_data = inspector.get_columns(table_name, schema=schema)
        
        columns = []
        for col in columns_data:
            # Map SQL types to generic
            # SQLAlchemy types: INTEGER, VARCHAR, etc.
            # We use string representation for simplicity or type checks
            dtype = "string"
            sa_type = col["type"]
            
            if isinstance(sa_type, sa.Integer):
                dtype = "integer"
            elif isinstance(sa_type, (sa.Float, sa.Numeric)):
                dtype = "float"
            elif isinstance(sa_type, sa.Boolean):
                dtype = "boolean"
            elif isinstance(sa_type, (sa.Date, sa.DateTime)):
                dtype = "timestamp"
            
            columns.append(ColumnInfo(
                name=col["name"],
                data_type=dtype,
                nullable=col.get("nullable", True)
            ))
            
        self._cache_schema(table, columns)
        return columns

    def insert(self, table: str, values: Dict[str, Any], parameters: Sequence = None) -> int:
        """Insert record."""
        metadata = sa.MetaData()
        schema, table_name = self._parse_table_path(table)
             
        sa_table = sa.Table(table_name, metadata, autoload_with=self.engine, schema=schema)
        
        stmt = sa_table.insert().values(**values)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Update records."""
        metadata = sa.MetaData()
        schema, table_name = self._parse_table_path(table)

        sa_table = sa.Table(table_name, metadata, autoload_with=self.engine, schema=schema)
        
        stmt = sa_table.update().values(**values)
        
        if predicates:
            for pred in predicates:
                clause = self._predicate_to_sa(pred, sa_table)
                if clause is not None:
                    stmt = stmt.where(clause)
                    
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def delete(
        self,
        table: str,
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """Delete records."""
        metadata = sa.MetaData()
        schema, table_name = self._parse_table_path(table)

        sa_table = sa.Table(table_name, metadata, autoload_with=self.engine, schema=schema)
        
        stmt = sa_table.delete()
        
        if predicates:
            for pred in predicates:
                clause = self._predicate_to_sa(pred, sa_table)
                if clause is not None:
                    stmt = stmt.where(clause)
                    
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount
