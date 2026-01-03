"""
File Adapter - CSV/Parquet support via DuckDB native capabilities

Uses DuckDB's built-in file reading for optimal performance.
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

import duckdb
import pyarrow as pa
import anyio

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError
from waveql.schema_cache import ColumnInfo
from waveql.query_planner import ParameterPlaceholder

if TYPE_CHECKING:
    from waveql.query_planner import Predicate


class FileAdapter(BaseAdapter):
    """
    File adapter for CSV, Parquet, and JSON files.
    
    Leverages DuckDB's native file reading for predicate pushdown
    and optimal performance.
    """
    
    adapter_name = "file"
    supports_predicate_pushdown = True
    supports_insert = True  # For CSV
    supports_update = False
    supports_delete = False
    
    def __init__(
        self,
        host: str,  # File path or directory
        auth_manager=None,
        schema_cache=None,
        file_type: str = None,  # csv, parquet, json (auto-detected if None)
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        self._path = Path(host)
        self._file_type = file_type or self._detect_file_type()
        self._duckdb = duckdb.connect(":memory:")
        self._config = kwargs
    
    
    def _detect_file_type(self) -> str:
        """Detect file type from extension."""
        suffix = self._path.suffix.lower()
        if suffix == ".parquet":
            return "parquet"
        elif suffix == ".json":
            return "json"
        elif suffix in [".xlsx", ".xls"]:
            return "excel"
        else:
            return "csv"
    
    async def fetch_async(self, *args, **kwargs) -> pa.Table:
        """Fetch data from file (async)."""
        return await anyio.to_thread.run_sync(self.fetch, *args, **kwargs)

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
        """Fetch data from file with DuckDB's native pushdown."""
        # Build file path
        file_path = self._resolve_path(table)
        
        # Build SQL query for DuckDB
        sql = self._build_query(file_path, columns, predicates, limit, offset, order_by, group_by, aggregates)
        
        try:
            # For Excel, we might need to load it into DuckDB first via Pandas if not using the extension
            if self._file_type == "excel":
                # Check if we are querying the file itself or a table we already registered
                # Optimization: Register the dataframe as a view in DuckDB first
                import pandas as pd
                df = pd.read_excel(file_path)
                # Register temporarily
                temp_name = f"excel_{abs(hash(file_path))}"
                self._duckdb.register(temp_name, df)
                
                # Rewrite SQL to query the temp table instead of read_... function
                # The _build_query for excel returns "SELECT * FROM 'filepath'" which is wrong for this approach
                # We need to construct the SQL here slightly differently or modifying _build_query
                # Let's rely on _build_query doing the "SELECT ... FROM view_name" logic if we handle it there.
                
                # Actually, simpler approach:
                # _build_query returns "SELECT ... FROM source". 
                # For Excel, source should be the registered view.
                
                # Execute
                result = self._duckdb.execute(sql.replace(f"'{file_path}'", temp_name))
                self._duckdb.unregister(temp_name)
                return result.fetch_arrow_table()
            else:
                result = self._duckdb.execute(sql)
                return result.fetch_arrow_table()
        except Exception as e:
            raise AdapterError(f"Failed to read file: {e}")
    
    def _resolve_path(self, table: str) -> str:
        """Resolve table name to file path."""
        # If path is a file, use it directly
        if self._path.is_file():
            return str(self._path)
        
        # If path is a directory, look for table as filename
        if self._path.is_dir():
            for ext in [".parquet", ".csv", ".json", ".xlsx", ".xls"]:
                file_path = self._path / f"{table}{ext}"
                if file_path.exists():
                    return str(file_path)
        
        # Try table as literal path
        if Path(table).exists():
            return table
        
        raise AdapterError(f"File not found: {table}")
    
    def _build_query(
        self,
        file_path: str,
        columns: List[str],
        predicates: List["Predicate"],
        limit: int,
        offset: int,
        order_by: List[tuple],
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> str:
        """Build DuckDB SQL query for file."""
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
             # Handle case where only aggregates (no group by) or only group by (distinct?)
             if not select_parts:
                 select_parts = ["*"]
             cols = ", ".join(select_parts)
        else:
            cols = ", ".join(columns) if columns and columns != ["*"] else "*"
        
        # File reader function
        if self._file_type == "parquet":
            source = f"read_parquet('{file_path}')"
        elif self._file_type == "json":
            source = f"read_json_auto('{file_path}')"
        elif self._file_type == "excel":
           # Placeholder, will be replaced in fetch
           source = f"'{file_path}'" 
        else:
            # CSV with auto-detection
            source = f"read_csv_auto('{file_path}')"
        
        sql = f"SELECT {cols} FROM {source}"
        
        # WHERE clause
        if predicates:
            where_parts = []
            for pred in predicates:
                value = f"'{pred.value}'" if isinstance(pred.value, str) else str(pred.value)
                if pred.value is None:
                    if pred.operator == "IS NULL":
                        where_parts.append(f"{pred.column} IS NULL")
                    else:
                        where_parts.append(f"{pred.column} IS NOT NULL")
                else:
                    where_parts.append(f"{pred.column} {pred.operator} {value}")
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
    
    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Get schema from file (async)."""
        return await anyio.to_thread.run_sync(self.get_schema, table)

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Get schema from file."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        file_path = self._resolve_path(table)
        
        if self._file_type == "excel":
           import pandas as pd
           df = pd.read_excel(file_path, nrows=1)
           # Register
           self._duckdb.register("temp_schema_excel", df)
           sql = "DESCRIBE SELECT * FROM temp_schema_excel"
        elif self._file_type == "parquet":
            sql = f"DESCRIBE SELECT * FROM read_parquet('{file_path}')"
        elif self._file_type == "json":
            sql = f"DESCRIBE SELECT * FROM read_json_auto('{file_path}')"
        else:
            sql = f"DESCRIBE SELECT * FROM read_csv_auto('{file_path}')"
        
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
            
            if self._file_type == "excel":
                self._duckdb.unregister("temp_schema_excel")
                
            self._cache_schema(table, columns)
            return columns
        except Exception as e:
            raise AdapterError(f"Failed to get schema: {e}")
    
    async def insert_async(self, *args, **kwargs) -> int:
        """Append to CSV file (async)."""
        return await anyio.to_thread.run_sync(self.insert, *args, **kwargs)

    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Append to CSV file."""
        if self._file_type != "csv":
            raise QueryError(f"INSERT only supported for CSV files")
        
        file_path = self._resolve_path(table)
        
        try:
            # Substitute parameters
            resolved_values = {}
            if parameters:
                # Handle single parameter set vs batch
                # If parameters is a list of parameters for this row
                # Check if parameters is 1D or 2D (batch)
                # But executemany calls execute_batch, checking base adapter
                # This insert is for single row mostly via execute
                current_params = parameters
                
            param_idx = 0
            for col, val in values.items():
                if hasattr(val, 'name') and val.name == 'ParameterPlaceholder' or val == '?':
                    # Check for ParameterPlaceholder object or string '?'
                     if parameters and param_idx < len(parameters):
                        resolved_values[col] = parameters[param_idx]
                        param_idx += 1
                     else:
                        raise QueryError(f"Missing parameter for column {col}")
                else:
                    resolved_values[col] = val
                    
            # Read existing, append, write back
            import csv
            
            # Get column order
            with open(file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or list(resolved_values.keys())
            
            # Append row
            with open(file_path, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(resolved_values)
            
            return 1
        except Exception as e:
            raise QueryError(f"INSERT failed: {e}")
    
    def list_tables(self) -> List[str]:
        """List files in directory."""
        if self._path.is_file():
            return [self._path.stem]
        
        if self._path.is_dir():
            tables = []
            for ext in ["*.parquet", "*.csv", "*.json"]:
                for file in self._path.glob(ext):
                    tables.append(file.stem)
            return tables
        
        return []
