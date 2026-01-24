"""
Tests for WaveQL pg_wire/type_mapping module.

This covers the 52% uncovered module waveql/pg_wire/type_mapping.py
"""

import pytest
import pyarrow as pa
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import struct

from waveql.pg_wire.type_mapping import (
    PGType,
    arrow_to_pg_oid,
    pg_oid_to_arrow,
    encode_text_value,
    encode_binary_value,
    get_type_info,
    pg_type_oid,
    arrow_to_pg_type,
    encode_value,
    decode_value,
    OID_TO_TYPE,
    PG_TYPES,
)


class TestPGType:
    """Tests for PGType dataclass."""
    
    def test_pg_type_basic(self):
        """Test PGType creation."""
        pg_type = PGType(
            oid=23,
            name="int4",
        )
        
        assert pg_type.oid == 23
        assert pg_type.name == "int4"
        assert pg_type.array_oid == 0  # Default
        assert pg_type.size == -1  # Default
        assert pg_type.category == "S"  # Default
    
    def test_pg_type_with_all_fields(self):
        """Test PGType with all fields."""
        pg_type = PGType(
            oid=25,
            name="text",
            array_oid=1009,
            size=-1,
            category="S",
        )
        
        assert pg_type.oid == 25
        assert pg_type.name == "text"
        assert pg_type.array_oid == 1009
        assert pg_type.category == "S"





class TestArrowToPgOid:
    """Tests for arrow_to_pg_oid function."""
    
    def test_int8(self):
        """Test mapping int8."""
        assert arrow_to_pg_oid(pa.int8()) in [21, 23]  # int2 or int4
    
    def test_int16(self):
        """Test mapping int16."""
        assert arrow_to_pg_oid(pa.int16()) == 21  # int2
    
    def test_int32(self):
        """Test mapping int32."""
        assert arrow_to_pg_oid(pa.int32()) == 23  # int4
    
    def test_int64(self):
        """Test mapping int64."""
        assert arrow_to_pg_oid(pa.int64()) == 20  # int8
    
    def test_float32(self):
        """Test mapping float32."""
        assert arrow_to_pg_oid(pa.float32()) == 700  # float4
    
    def test_float64(self):
        """Test mapping float64."""
        assert arrow_to_pg_oid(pa.float64()) == 701  # float8
    
    def test_string(self):
        """Test mapping string."""
        assert arrow_to_pg_oid(pa.string()) == 25  # text
    
    def test_large_string(self):
        """Test mapping large_string."""
        assert arrow_to_pg_oid(pa.large_string()) == 25  # text
    
    def test_bool(self):
        """Test mapping bool."""
        assert arrow_to_pg_oid(pa.bool_()) == 16  # bool
    
    def test_date32(self):
        """Test mapping date32."""
        assert arrow_to_pg_oid(pa.date32()) == 1082  # date
    
    def test_timestamp(self):
        """Test mapping timestamp."""
        assert arrow_to_pg_oid(pa.timestamp("us")) == 1114  # timestamp
    
    def test_timestamp_with_tz(self):
        """Test mapping timestamp with timezone."""
        oid = arrow_to_pg_oid(pa.timestamp("us", tz="UTC"))
        assert oid in [1114, 1184]  # timestamp or timestamptz
    
    def test_binary(self):
        """Test mapping binary."""
        assert arrow_to_pg_oid(pa.binary()) == 17  # bytea
    
    def test_null(self):
        """Test mapping null type."""
        oid = arrow_to_pg_oid(pa.null())
        assert isinstance(oid, int)
    
    def test_decimal(self):
        """Test mapping decimal."""
        oid = arrow_to_pg_oid(pa.decimal128(10, 2))
        assert oid == 1700  # numeric


class TestPgOidToArrow:
    """Tests for pg_oid_to_arrow function."""
    
    def test_int2(self):
        """Test mapping int2 OID to Arrow."""
        result = pg_oid_to_arrow(21)
        assert result == pa.int16()
    
    def test_int4(self):
        """Test mapping int4 OID to Arrow."""
        result = pg_oid_to_arrow(23)
        assert result == pa.int32()
    
    def test_int8(self):
        """Test mapping int8 OID to Arrow."""
        result = pg_oid_to_arrow(20)
        assert result == pa.int64()
    
    def test_float4(self):
        """Test mapping float4 OID to Arrow."""
        result = pg_oid_to_arrow(700)
        assert result == pa.float32()
    
    def test_float8(self):
        """Test mapping float8 OID to Arrow."""
        result = pg_oid_to_arrow(701)
        assert result == pa.float64()
    
    def test_text(self):
        """Test mapping text OID to Arrow."""
        result = pg_oid_to_arrow(25)
        assert result == pa.string()
    
    def test_bool(self):
        """Test mapping bool OID to Arrow."""
        result = pg_oid_to_arrow(16)
        assert result == pa.bool_()
    
    def test_date(self):
        """Test mapping date OID to Arrow."""
        result = pg_oid_to_arrow(1082)
        assert result == pa.date32()
    
    def test_timestamp(self):
        """Test mapping timestamp OID to Arrow."""
        result = pg_oid_to_arrow(1114)
        assert pa.types.is_timestamp(result)
    
    def test_unknown_oid(self):
        """Test mapping unknown OID."""
        result = pg_oid_to_arrow(99999)
        assert result == pa.string()  # Default to string


class TestEncodeTextValue:
    """Tests for encode_text_value function."""
    
    def test_encode_null(self):
        """Test encoding NULL."""
        result = encode_text_value(None, 25)
        assert result is None
    
    def test_encode_int(self):
        """Test encoding integer."""
        result = encode_text_value(42, 23)
        assert result == b"42"
    
    def test_encode_float(self):
        """Test encoding float."""
        result = encode_text_value(3.14, 701)
        assert b"3.14" in result
    
    def test_encode_string(self):
        """Test encoding string."""
        result = encode_text_value("hello", 25)
        assert result == b"hello"
    
    def test_encode_bool_true(self):
        """Test encoding boolean true."""
        result = encode_text_value(True, 16)
        assert result in [b"t", b"true", b"True", b"1"]
    
    def test_encode_bool_false(self):
        """Test encoding boolean false."""
        result = encode_text_value(False, 16)
        assert result in [b"f", b"false", b"False", b"0"]
    
    def test_encode_date(self):
        """Test encoding date."""
        d = date(2024, 1, 15)
        result = encode_text_value(d, 1082)
        assert b"2024" in result
        assert b"01" in result or b"1" in result
        assert b"15" in result
    
    def test_encode_datetime(self):
        """Test encoding datetime."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = encode_text_value(dt, 1114)
        assert b"2024" in result
    
    def test_encode_bytes(self):
        """Test encoding bytes."""
        result = encode_text_value(b"\x00\x01\x02", 17)
        assert result is not None


class TestEncodeBinaryValue:
    """Tests for encode_binary_value function."""
    
    def test_encode_null(self):
        """Test encoding NULL."""
        result = encode_binary_value(None, 25)
        assert result is None
    
    def test_encode_int4(self):
        """Test encoding int4 binary."""
        result = encode_binary_value(42, 23)
        assert result is not None
        assert len(result) == 4
    
    def test_encode_int8(self):
        """Test encoding int8 binary."""
        result = encode_binary_value(42, 20)
        assert result is not None
        assert len(result) == 8
    
    def test_encode_float4(self):
        """Test encoding float4 binary."""
        result = encode_binary_value(3.14, 700)
        assert result is not None
        assert len(result) == 4
    
    def test_encode_float8(self):
        """Test encoding float8 binary."""
        result = encode_binary_value(3.14, 701)
        assert result is not None
        assert len(result) == 8
    
    def test_encode_bool(self):
        """Test encoding bool binary."""
        result = encode_binary_value(True, 16)
        assert result is not None
        assert len(result) == 1


class TestGetTypeInfo:
    """Tests for get_type_info function."""
    
    def test_get_int4_info(self):
        """Test getting int4 type info."""
        name, size, category = get_type_info(23)
        
        assert name == "int4"
        assert size == 4
    
    def test_get_text_info(self):
        """Test getting text type info."""
        name, size, category = get_type_info(25)
        
        assert name == "text"
        assert size == -1  # Variable length
    
    def test_get_unknown_info(self):
        """Test getting unknown type info."""
        name, size, category = get_type_info(99999)
        
        # Should return some default
        assert isinstance(name, str)


class TestPgTypeOid:
    """Tests for pg_type_oid function."""
    
    def test_int4_oid(self):
        """Test getting int4 OID."""
        assert pg_type_oid("int4") == 23
    
    def test_int8_oid(self):
        """Test getting int8 OID."""
        assert pg_type_oid("int8") == 20
    
    def test_text_oid(self):
        """Test getting text OID."""
        assert pg_type_oid("text") == 25
    
    def test_bool_oid(self):
        """Test getting bool OID."""
        assert pg_type_oid("bool") == 16
    
    
    def test_unknown_type(self):
        """Test getting OID for unknown type."""
        with pytest.raises(ValueError):
            pg_type_oid("unknown_type")


class TestArrowToPgType:
    """Tests for arrow_to_pg_type function."""
    
    def test_int32_to_pg_type(self):
        """Test mapping int32 to pg type name."""
        result = arrow_to_pg_type(pa.int32())
        assert result == "int4"
    
    def test_int64_to_pg_type(self):
        """Test mapping int64 to pg type name."""
        result = arrow_to_pg_type(pa.int64())
        assert result == "int8"
    
    def test_string_to_pg_type(self):
        """Test mapping string to pg type name."""
        result = arrow_to_pg_type(pa.string())
        assert result == "text"
    
    def test_float64_to_pg_type(self):
        """Test mapping float64 to pg type name."""
        result = arrow_to_pg_type(pa.float64())
        assert result == "float8"
    
    def test_bool_to_pg_type(self):
        """Test mapping bool to pg type name."""
        result = arrow_to_pg_type(pa.bool_())
        assert result == "bool"


class TestEncodeValue:
    """Tests for encode_value function (with type name)."""
    
    def test_encode_int(self):
        """Test encoding int with type name."""
        result = encode_value(42, "int4")
        assert result == b"42"
    
    def test_encode_text(self):
        """Test encoding text with type name."""
        result = encode_value("hello", "text")
        assert result == b"hello"
    
    def test_encode_bool(self):
        """Test encoding bool with type name."""
        result = encode_value(True, "bool")
        assert result in [b"t", b"true", b"True", b"1"]


class TestDecodeValue:
    """Tests for decode_value function."""
    
    def test_decode_null(self):
        """Test decoding NULL."""
        result = decode_value(None, "int4")
        assert result is None
    
    def test_decode_int(self):
        """Test decoding integer."""
        result = decode_value(b"42", "int4")
        assert result == 42
    
    def test_decode_int8(self):
        """Test decoding bigint."""
        result = decode_value(b"9999999999", "int8")
        assert result == 9999999999
    
    def test_decode_float(self):
        """Test decoding float."""
        result = decode_value(b"3.14", "float8")
        assert abs(result - 3.14) < 0.001
    
    def test_decode_text(self):
        """Test decoding text."""
        result = decode_value(b"hello", "text")
        assert result == "hello"
    
    def test_decode_bool_t(self):
        """Test decoding bool 't'."""
        result = decode_value(b"t", "bool")
        assert result is True
    
    def test_decode_bool_f(self):
        """Test decoding bool 'f'."""
        result = decode_value(b"f", "bool")
        assert result is False
    
    def test_decode_bool_true(self):
        """Test decoding bool 'true'."""
        result = decode_value(b"true", "bool")
        assert result is True
    
    def test_decode_date(self):
        """Test decoding date."""
        result = decode_value(b"2024-01-15", "date")
        assert isinstance(result, date)
        assert result.year == 2024


class TestPGTypesMapping:
    """Tests for PG_TYPES mapping."""
    
    def test_common_types_exist(self):
        """Test common types exist in mapping."""
        # By OID -> Use OID_TO_TYPE
        assert 16 in OID_TO_TYPE  # bool
        assert 20 in OID_TO_TYPE  # int8
        assert 21 in OID_TO_TYPE  # int2
        assert 23 in OID_TO_TYPE  # int4
        assert 25 in OID_TO_TYPE  # text
        assert 700 in OID_TO_TYPE  # float4
        assert 701 in OID_TO_TYPE  # float8
    
    def test_type_properties(self):
        """Test PGType properties in mapping."""
        text_type = OID_TO_TYPE[25]
        
        assert text_type.oid == 25
        assert text_type.name == "text"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
