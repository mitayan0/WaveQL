"""
Query Planner - SQL parsing and predicate extraction for pushdown
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple



class ParameterPlaceholder:
    """Represents a SQL parameter placeholder (?)"""
    def __eq__(self, other):
        return isinstance(other, ParameterPlaceholder)
    
    def __repr__(self):
        return "?"


@dataclass
class Predicate:
    """Represents a WHERE clause predicate."""
    column: str
    operator: str  # =, !=, <, >, <=, >=, LIKE, IN, IS NULL, IS NOT NULL
    value: Any
    
    def to_api_filter(self, dialect: str = "default") -> str:
        """Convert predicate to API-specific filter format."""
        if dialect == "servicenow":
            op_map = {"=": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<=", 
                      "LIKE": "LIKE", "IN": "IN"}
            return f"{self.column}{op_map.get(self.operator, '=')}{self.value}"
        return f"{self.column} {self.operator} {self.value}"


@dataclass
class Aggregate:
    """Represents an aggregation function."""
    func: str  # COUNT, SUM, AVG, MIN, MAX
    column: str
    alias: Optional[str] = None


@dataclass
class QueryInfo:
    """Parsed query information."""
    operation: str  # SELECT, INSERT, UPDATE, DELETE
    table: Optional[str] = None
    columns: List[str] = field(default_factory=lambda: ["*"])
    predicates: List[Predicate] = field(default_factory=list)
    values: Dict[str, Any] = field(default_factory=dict)
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: List[Tuple[str, str]] = field(default_factory=list)  # [(col, ASC/DESC), ...]
    joins: List[Dict] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    aggregates: List[Aggregate] = field(default_factory=list)
    raw_sql: str = ""


class QueryPlanner:
    """
    Parses SQL and extracts components for predicate pushdown.
    
    Supports:
    - SELECT with WHERE, LIMIT, OFFSET, ORDER BY, GROUP BY, Aggregates
    - INSERT with VALUES
    - UPDATE with SET and WHERE
    - DELETE with WHERE
    - JOINs (detected for virtual join handling)
    """
    
    # Regex patterns for SQL parsing
    SELECT_PATTERN = re.compile(
        r"SELECT\s+(.+?)\s+FROM\s+(\w+(?:\.\w+)?)"
        r"(?:\s+AS\s+(\w+))?"
        r"(?:\s+(.+))?",
        re.IGNORECASE | re.DOTALL
    )
    
    INSERT_PATTERN = re.compile(
        r"INSERT\s+INTO\s+(\w+(?:\.\w+)?)\s*"
        r"(?:\(([^)]+)\))?\s*"
        r"VALUES\s*\(([^)]+)\)",
        re.IGNORECASE
    )
    
    UPDATE_PATTERN = re.compile(
        r"UPDATE\s+(\w+(?:\.\w+)?)\s+"
        r"SET\s+(.+?)"
        r"(?:\s+WHERE\s+(.+))?$",
        re.IGNORECASE | re.DOTALL
    )
    
    DELETE_PATTERN = re.compile(
        r"DELETE\s+FROM\s+(\w+(?:\.\w+)?)"
        r"(?:\s+WHERE\s+(.+))?$",
        re.IGNORECASE
    )
    
    WHERE_PATTERN = re.compile(r"WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|$)", re.IGNORECASE)
    GROUP_PATTERN = re.compile(r"GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|$)", re.IGNORECASE)
    ORDER_PATTERN = re.compile(r"ORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s+OFFSET|$)", re.IGNORECASE)
    LIMIT_PATTERN = re.compile(r"LIMIT\s+(\d+)", re.IGNORECASE)
    OFFSET_PATTERN = re.compile(r"OFFSET\s+(\d+)", re.IGNORECASE)
    JOIN_PATTERN = re.compile(r"(LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+)?JOIN\s+(\w+(?:\.\w+)?)", re.IGNORECASE)
    AGG_PATTERN = re.compile(r"^\s*(COUNT|SUM|AVG|MIN|MAX)\s*\((.+?)\)(?:\s+AS\s+(\w+))?\s*$", re.IGNORECASE)
    
    def parse(self, sql: str) -> QueryInfo:
        """Parse SQL query and extract components."""
        sql = sql.strip()
        
        # Determine operation type
        upper_sql = sql.upper()
        
        if upper_sql.startswith("SELECT"):
            return self._parse_select(sql)
        elif upper_sql.startswith("INSERT"):
            return self._parse_insert(sql)
        elif upper_sql.startswith("UPDATE"):
            return self._parse_update(sql)
        elif upper_sql.startswith("DELETE"):
            return self._parse_delete(sql)
        else:
            # Return raw for DuckDB to handle (DDL, etc.)
            return QueryInfo(operation="RAW", raw_sql=sql)
    
    def _parse_select(self, sql: str) -> QueryInfo:
        """Parse SELECT query."""
        info = QueryInfo(operation="SELECT", raw_sql=sql)
        
        match = self.SELECT_PATTERN.match(sql)
        if match:
            columns_str, table, alias, rest = match.groups()
            
            # Parse columns and aggregates
            parsed_cols = []
            for col_str in columns_str.split(","):
                col_str = col_str.strip()
                agg_match = self.AGG_PATTERN.match(col_str)
                if agg_match:
                    func, col, agg_alias = agg_match.groups()
                    info.aggregates.append(Aggregate(func.upper(), col, agg_alias))
                    parsed_cols.append(agg_alias if agg_alias else f"{func}({col})")
                else:
                    parsed_cols.append(col_str)
            info.columns = parsed_cols

            info.table = table
            
            if rest:
                # Parse WHERE
                where_match = self.WHERE_PATTERN.search(rest)
                if where_match:
                    info.predicates = self._parse_predicates(where_match.group(1))
                
                # Parse GROUP BY
                group_match = self.GROUP_PATTERN.search(rest)
                if group_match:
                    info.group_by = [g.strip() for g in group_match.group(1).split(",")]
                
                # Parse ORDER BY
                order_match = self.ORDER_PATTERN.search(rest)
                if order_match:
                    info.order_by = self._parse_order_by(order_match.group(1))
                
                # Parse LIMIT
                limit_match = self.LIMIT_PATTERN.search(rest)
                if limit_match:
                    info.limit = int(limit_match.group(1))
                
                # Parse OFFSET
                offset_match = self.OFFSET_PATTERN.search(rest)
                if offset_match:
                    info.offset = int(offset_match.group(1))
                
                # Detect JOINs
                for join_match in self.JOIN_PATTERN.finditer(rest):
                    join_type, join_table = join_match.groups()
                    info.joins.append({
                        "type": (join_type or "INNER").strip().upper(),
                        "table": join_table
                    })
        
        return info
    
    def _parse_insert(self, sql: str) -> QueryInfo:
        """Parse INSERT query."""
        info = QueryInfo(operation="INSERT", raw_sql=sql)
        
        match = self.INSERT_PATTERN.match(sql)
        if match:
            table, columns_str, values_str = match.groups()
            info.table = table
            
            columns = [c.strip() for c in columns_str.split(",")] if columns_str else []
            values = [self._parse_value(v.strip()) for v in values_str.split(",")]
            
            if columns:
                info.values = dict(zip(columns, values))
            else:
                info.values = {"_values": values}
        
        return info
    
    def _parse_update(self, sql: str) -> QueryInfo:
        """Parse UPDATE query."""
        info = QueryInfo(operation="UPDATE", raw_sql=sql)
        
        match = self.UPDATE_PATTERN.match(sql)
        if match:
            table, set_clause, where_clause = match.groups()
            info.table = table
            
            # Parse SET clause
            for assignment in set_clause.split(","):
                if "=" in assignment:
                    col, val = assignment.split("=", 1)
                    info.values[col.strip()] = self._parse_value(val.strip())
            
            if where_clause:
                info.predicates = self._parse_predicates(where_clause)
        
        return info
    
    def _parse_delete(self, sql: str) -> QueryInfo:
        """Parse DELETE query."""
        info = QueryInfo(operation="DELETE", raw_sql=sql)
        
        match = self.DELETE_PATTERN.match(sql)
        if match:
            table, where_clause = match.groups()
            info.table = table
            
            if where_clause:
                info.predicates = self._parse_predicates(where_clause)
        
        return info
    
    def _parse_predicates(self, where_str: str) -> List[Predicate]:
        """Parse WHERE clause into predicates."""
        predicates = []
        
        # Split by AND (basic support)
        conditions = re.split(r"\s+AND\s+", where_str, flags=re.IGNORECASE)
        
        for cond in conditions:
            cond = cond.strip()
            
            # Handle different operators
            for op in [">=", "<=", "!=", "<>", "=", ">", "<", " LIKE ", " IN ", " IS NOT NULL", " IS NULL"]:
                op_upper = op.upper().strip()
                if op.upper() in cond.upper():
                    parts = re.split(re.escape(op), cond, maxsplit=1, flags=re.IGNORECASE)
                    if len(parts) == 2:
                        col = parts[0].strip()
                        val = self._parse_value(parts[1].strip())
                        predicates.append(Predicate(column=col, operator=op_upper, value=val))
                        break
                    elif "IS NULL" in op.upper():
                        col = parts[0].strip()
                        predicates.append(Predicate(column=col, operator=op_upper, value=None))
                        break
        
        return predicates
    
    def _parse_order_by(self, order_str: str) -> List[Tuple[str, str]]:
        """Parse ORDER BY clause."""
        result = []
        for part in order_str.split(","):
            part = part.strip()
            if " DESC" in part.upper():
                col = part.upper().replace(" DESC", "").strip()
                result.append((col, "DESC"))
            else:
                col = part.upper().replace(" ASC", "").strip()
                result.append((col, "ASC"))
        return result
    
    def _parse_value(self, val_str: str) -> Any:
        """Parse a value from SQL to Python type."""
        val_str = val_str.strip()
        
        # String literal
        if (val_str.startswith("'") and val_str.endswith("'")) or \
           (val_str.startswith('"') and val_str.endswith('"')):
            return val_str[1:-1]
        
        # NULL
        if val_str.upper() == "NULL":
            return None
        
        # Number
        try:
            if "." in val_str:
                return float(val_str)
            return int(val_str)
        except ValueError:
            pass
        
        # Boolean
        if val_str.upper() == "TRUE":
            return True
        if val_str.upper() == "FALSE":
            return False
        
        if val_str == "?":
            return ParameterPlaceholder()
        
        return val_str
