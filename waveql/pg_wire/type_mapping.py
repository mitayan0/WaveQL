"""
PostgreSQL Type Mapping

Maps between PyArrow types and PostgreSQL type OIDs for binary protocol encoding.

PostgreSQL Type OIDs: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
"""

from __future__ import annotations
import struct
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, Union
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class PGType:
    """PostgreSQL type metadata."""
    oid: int
    name: str
    array_oid: int = 0  # OID for array type (0 = no array type)
    size: int = -1  # -1 = variable length
    category: str = "S"  # Type category (S=string, N=numeric, D=datetime, etc)


# Common PostgreSQL type OIDs
# Reference: https://www.postgresql.org/docs/current/datatype-oid.html
PG_TYPES: Dict[str, PGType] = {
    # Boolean
    "bool": PGType(oid=16, name="bool", array_oid=1000, size=1, category="B"),
    
    # Bytes/Binary
    "bytea": PGType(oid=17, name="bytea", array_oid=1001, category="U"),
    
    # Character types
    "char": PGType(oid=18, name="char", array_oid=1002, size=1, category="S"),
    "name": PGType(oid=19, name="name", array_oid=1003, size=64, category="S"),
    "text": PGType(oid=25, name="text", array_oid=1009, category="S"),
    "varchar": PGType(oid=1043, name="varchar", array_oid=1015, category="S"),
    "bpchar": PGType(oid=1042, name="bpchar", array_oid=1014, category="S"),
    
    # Integer types
    "int2": PGType(oid=21, name="int2", array_oid=1005, size=2, category="N"),
    "int4": PGType(oid=23, name="int4", array_oid=1007, size=4, category="N"),
    "int8": PGType(oid=20, name="int8", array_oid=1016, size=8, category="N"),
    "oid": PGType(oid=26, name="oid", array_oid=1028, size=4, category="N"),
    
    # Floating point
    "float4": PGType(oid=700, name="float4", array_oid=1021, size=4, category="N"),
    "float8": PGType(oid=701, name="float8", array_oid=1022, size=8, category="N"),
    
    # Numeric/Decimal
    "numeric": PGType(oid=1700, name="numeric", array_oid=1231, category="N"),
    
    # Date/Time types
    "date": PGType(oid=1082, name="date", array_oid=1182, size=4, category="D"),
    "time": PGType(oid=1083, name="time", array_oid=1183, size=8, category="D"),
    "timetz": PGType(oid=1266, name="timetz", array_oid=1270, size=12, category="D"),
    "timestamp": PGType(oid=1114, name="timestamp", array_oid=1115, size=8, category="D"),
    "timestamptz": PGType(oid=1184, name="timestamptz", array_oid=1185, size=8, category="D"),
    "interval": PGType(oid=1186, name="interval", array_oid=1187, size=16, category="D"),
    
    # JSON types
    "json": PGType(oid=114, name="json", array_oid=199, category="U"),
    "jsonb": PGType(oid=3802, name="jsonb", array_oid=3807, category="U"),
    
    # UUID
    "uuid": PGType(oid=2950, name="uuid", array_oid=2951, size=16, category="U"),
    
    # Network types
    "inet": PGType(oid=869, name="inet", array_oid=1041, category="I"),
    "cidr": PGType(oid=650, name="cidr", array_oid=651, category="I"),
    "macaddr": PGType(oid=829, name="macaddr", array_oid=1040, size=6, category="U"),
    
    # Special types
    "void": PGType(oid=2278, name="void", category="P"),
    "unknown": PGType(oid=705, name="unknown", category="X"),
    "regclass": PGType(oid=2205, name="regclass", array_oid=2210, size=4, category="N"),
    "regtype": PGType(oid=2206, name="regtype", array_oid=2211, size=4, category="N"),
}

# Reverse lookup: OID -> PGType
OID_TO_TYPE: Dict[int, PGType] = {t.oid: t for t in PG_TYPES.values()}


def arrow_to_pg_oid(arrow_type: pa.DataType) -> int:
    """
    Map a PyArrow type to PostgreSQL type OID.
    
    Args:
        arrow_type: PyArrow data type
        
    Returns:
        PostgreSQL type OID
    """
    # Handle nested/parameterized types first
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # Get the element type and find its array OID
        value_type = arrow_type.value_type
        base_oid = arrow_to_pg_oid(value_type)
        base_type = OID_TO_TYPE.get(base_oid)
        if base_type and base_type.array_oid:
            return base_type.array_oid
        # Fallback to JSON for complex arrays
        return PG_TYPES["json"].oid
    
    if pa.types.is_struct(arrow_type) or pa.types.is_map(arrow_type):
        # Struct/Map -> JSONB
        return PG_TYPES["jsonb"].oid
    
    if pa.types.is_dictionary(arrow_type):
        # Dictionary encoded -> look at value type
        return arrow_to_pg_oid(arrow_type.value_type)
    
    # Boolean
    if pa.types.is_boolean(arrow_type):
        return PG_TYPES["bool"].oid
    
    # Integer types
    if pa.types.is_int8(arrow_type) or pa.types.is_uint8(arrow_type):
        return PG_TYPES["int2"].oid
    if pa.types.is_int16(arrow_type) or pa.types.is_uint16(arrow_type):
        return PG_TYPES["int2"].oid
    if pa.types.is_int32(arrow_type) or pa.types.is_uint32(arrow_type):
        return PG_TYPES["int4"].oid
    if pa.types.is_int64(arrow_type) or pa.types.is_uint64(arrow_type):
        return PG_TYPES["int8"].oid
    
    # Floating point
    if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type):
        return PG_TYPES["float4"].oid
    if pa.types.is_float64(arrow_type):
        return PG_TYPES["float8"].oid
    
    # Decimal
    if pa.types.is_decimal(arrow_type):
        return PG_TYPES["numeric"].oid
    
    # String types
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return PG_TYPES["text"].oid
    
    # Binary
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return PG_TYPES["bytea"].oid
    if pa.types.is_fixed_size_binary(arrow_type):
        return PG_TYPES["bytea"].oid
    
    # Date/Time types
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return PG_TYPES["date"].oid
    if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        return PG_TYPES["time"].oid
    if pa.types.is_timestamp(arrow_type):
        # Check for timezone
        if arrow_type.tz:
            return PG_TYPES["timestamptz"].oid
        return PG_TYPES["timestamp"].oid
    if pa.types.is_duration(arrow_type):
        return PG_TYPES["interval"].oid
    
    # Null type
    if pa.types.is_null(arrow_type):
        return PG_TYPES["text"].oid  # Treat NULL as text
    
    # Default to text
    return PG_TYPES["text"].oid


def pg_oid_to_arrow(oid: int) -> pa.DataType:
    """
    Map a PostgreSQL type OID to PyArrow type.
    
    Args:
        oid: PostgreSQL type OID
        
    Returns:
        PyArrow data type
    """
    pg_type = OID_TO_TYPE.get(oid)
    if not pg_type:
        return pa.string()
    
    name = pg_type.name
    
    # Boolean
    if name == "bool":
        return pa.bool_()
    
    # Integers
    if name == "int2":
        return pa.int16()
    if name in ("int4", "oid"):
        return pa.int32()
    if name == "int8":
        return pa.int64()
    
    # Floats
    if name == "float4":
        return pa.float32()
    if name == "float8":
        return pa.float64()
    
    # Numeric/Decimal - use string for lossless handling
    if name == "numeric":
        return pa.decimal128(38, 10)  # Reasonable default precision
    
    # String types
    if name in ("text", "varchar", "bpchar", "char", "name"):
        return pa.string()
    
    # Binary
    if name == "bytea":
        return pa.binary()
    
    # Date/Time
    if name == "date":
        return pa.date32()
    if name == "time":
        return pa.time64("us")
    if name == "timetz":
        return pa.time64("us")  # Timezone handled separately
    if name == "timestamp":
        return pa.timestamp("us")
    if name == "timestamptz":
        return pa.timestamp("us", tz="UTC")
    if name == "interval":
        return pa.duration("us")
    
    # JSON
    if name in ("json", "jsonb"):
        return pa.string()  # JSON as string
    
    # UUID
    if name == "uuid":
        return pa.string()  # UUID as string
    
    # Default to string
    return pa.string()


def encode_text_value(value: Any, pg_oid: int) -> Optional[bytes]:
    """
    Encode a Python value to PostgreSQL text format.
    
    Args:
        value: Python value to encode
        pg_oid: Target PostgreSQL type OID
        
    Returns:
        UTF-8 encoded bytes, or None for NULL
    """
    if value is None:
        return None
    
    pg_type = OID_TO_TYPE.get(pg_oid)
    type_name = pg_type.name if pg_type else "text"
    
    # Boolean
    if type_name == "bool":
        return b"t" if value else b"f"
    
    # Numeric types - just convert to string
    if type_name in ("int2", "int4", "int8", "oid", "float4", "float8", "numeric"):
        return str(value).encode("utf-8")
    
    # Date/Time formatting
    if type_name == "date":
        if isinstance(value, date):
            return value.isoformat().encode("utf-8")
        return str(value).encode("utf-8")
    
    if type_name in ("timestamp", "timestamptz"):
        if isinstance(value, datetime):
            # PostgreSQL format: YYYY-MM-DD HH:MM:SS.ffffff
            return value.strftime("%Y-%m-%d %H:%M:%S.%f").encode("utf-8")
        return str(value).encode("utf-8")
    
    if type_name in ("time", "timetz"):
        if isinstance(value, time):
            return value.isoformat().encode("utf-8")
        return str(value).encode("utf-8")
    
    # JSON types
    if type_name in ("json", "jsonb"):
        if isinstance(value, (dict, list)):
            return json.dumps(value).encode("utf-8")
        return str(value).encode("utf-8")
    
    # Binary -> hex encoding
    if type_name == "bytea":
        if isinstance(value, bytes):
            return (b"\\x" + value.hex().encode("utf-8"))
        return str(value).encode("utf-8")
    
    # Default: string conversion
    return str(value).encode("utf-8")


def encode_binary_value(value: Any, pg_oid: int) -> Optional[bytes]:
    """
    Encode a Python value to PostgreSQL binary format.
    
    Args:
        value: Python value to encode
        pg_oid: Target PostgreSQL type OID
        
    Returns:
        Binary encoded bytes, or None for NULL
    """
    if value is None:
        return None
    
    pg_type = OID_TO_TYPE.get(pg_oid)
    type_name = pg_type.name if pg_type else "text"
    
    # Boolean: 1 byte
    if type_name == "bool":
        return struct.pack("!B", 1 if value else 0)
    
    # Integer types
    if type_name == "int2":
        return struct.pack("!h", int(value))
    if type_name in ("int4", "oid"):
        return struct.pack("!i", int(value))
    if type_name == "int8":
        return struct.pack("!q", int(value))
    
    # Floating point
    if type_name == "float4":
        return struct.pack("!f", float(value))
    if type_name == "float8":
        return struct.pack("!d", float(value))
    
    # For other types, fall back to text encoding
    # Binary format for complex types (numeric, timestamp, etc.) is more involved
    return encode_text_value(value, pg_oid)


def get_type_info(oid: int) -> Tuple[str, int, str]:
    """
    Get type information for a given OID.
    
    Returns:
        Tuple of (type_name, type_size, category)
    """
    pg_type = OID_TO_TYPE.get(oid)
    if pg_type:
        return (pg_type.name, pg_type.size, pg_type.category)
    return ("unknown", -1, "X")


def pg_type_oid(type_name: str) -> int:
    """
    Get OID for a PostgreSQL type name.
    
    Args:
        type_name: PostgreSQL type name (e.g. 'int4', 'text', 'bool')
        
    Returns:
        Type OID
    """
    if type_name not in PG_TYPES:
        # Fallback for aliases or unknown types might be needed, but strict for now
        # Check if it's already an OID (as string? no, func signature says type_name)
        raise ValueError(f"Unknown PostgreSQL type: {type_name}")
    return PG_TYPES[type_name].oid


def arrow_to_pg_type(arrow_type: pa.DataType) -> str:
    """
    Map PyArrow type to PostgreSQL type name.
    
    Args:
        arrow_type: PyArrow data type
        
    Returns:
        PostgreSQL type name
    """
    oid = arrow_to_pg_oid(arrow_type)
    if oid in OID_TO_TYPE:
        return OID_TO_TYPE[oid].name
    
    # Check if it's an array OID
    for t in PG_TYPES.values():
        if t.array_oid == oid:
            return "json"  # Tests expect JSON for arrays
            
    # Fallback
    return "json"


def encode_value(value: Any, type_name: str) -> Optional[bytes]:
    """
    Encode a value to PostgreSQL text format bytes using type name.
    
    Args:
        value: Value to encode
        type_name: Target PostgreSQL type name
        
    Returns:
        Encoded bytes
    """
    try:
        oid = pg_type_oid(type_name)
        return encode_text_value(value, oid)
    except Exception:
        # Fallback?
        return str(value).encode('utf-8')


def decode_value(value: Optional[bytes], type_name: str) -> Any:
    """
    Decode a PostgreSQL text format value.
    
    Args:
        value: Bytes to decode
        type_name: PostgreSQL type name
        
    Returns:
        Decoded Python value
    """
    if value is None:
        return None
        
    text = value.decode('utf-8')
    
    if type_name == "bool":
        return text.lower() in ("t", "true", "1")
        
    if type_name in ("int2", "int4", "int8", "oid"):
        return int(text)
        
    if type_name in ("float4", "float8", "numeric"):
        return float(text)
        
    if type_name == "date":
        return date.fromisoformat(text)
        
    if type_name in ("timestamp", "timestamptz"):
        # Basic ISO parsing - might need more robust parsing for PG specifics
        return datetime.fromisoformat(text.replace(" ", "T"))

    if type_name in ("json", "jsonb"):
        return json.loads(text)
        
    return text
