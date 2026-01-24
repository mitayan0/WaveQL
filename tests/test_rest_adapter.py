
import pytest
import requests
import pyarrow as pa
import json
import logging
from unittest.mock import MagicMock, patch
from waveql.adapters.rest_adapter import RESTAdapter
from waveql.exceptions import AdapterError, QueryError, RateLimitError
from waveql.query_planner import Predicate
from waveql.schema_cache import ColumnInfo, SchemaCache

@pytest.fixture
def mock_session():
    session = MagicMock()
    session.__enter__.return_value = session
    return session

@pytest.fixture
def schema_cache():
    return SchemaCache()

@pytest.fixture
def adapter(mock_session, schema_cache):
    a = RESTAdapter(
        host="api.example.com",
        endpoints={
            "users": {"path": "/v1/users", "supports_limit": True, "supports_offset": True}
        },
        schema_cache=schema_cache,
        use_connection_pool=False
    )
    a._local_session = mock_session
    return a

def test_rest_adapter_init():
    a = RESTAdapter(host="api.example.com")
    assert a._host == "https://api.example.com"
    a2 = RESTAdapter(host="http://plain.com")
    assert a2._host == "http://plain.com"

def test_get_endpoint_config(adapter):
    config = adapter._get_endpoint_config("users")
    assert config["path"] == "/v1/users"
    config2 = adapter._get_endpoint_config("unknown")
    assert config2["path"] == "/unknown"

def test_extract_records_full(adapter):
    assert adapter._extract_records({"id": 1}, {}) == [{"id": 1}]
    assert adapter._extract_records([{"id": 1}], {}) == [{"id": 1}]
    config = {"data_path": "nested.items"}
    data = {"nested": {"items": [{"id": 1}]}}
    assert adapter._extract_records(data, config) == [{"id": 1}]
    data2 = {"list": [{"a": 1}, {"a": 2}]}
    assert adapter._extract_records(data2, {"data_path": "list.1"}) == [{"a": 2}]
    assert adapter._extract_records(data, {"data_path": "bad.path"}) == []
    assert adapter._extract_records(None, {}) == []

def test_apply_filters_full(adapter):
    records = [
        {"id": 1, "name": "Alice", "age": 25, "score": 90.5},
        {"id": 2, "name": "Bob", "age": 30, "score": 85.0}
    ]
    # Operators (hits 332-344)
    assert len(adapter._apply_filters(records, [Predicate("age", "=", 25)])) == 1
    assert len(adapter._apply_filters(records, [Predicate("age", "!=", 25)])) == 1
    assert len(adapter._apply_filters(records, [Predicate("age", ">", 25)])) == 1
    assert len(adapter._apply_filters(records, [Predicate("age", ">=", 25)])) == 2
    assert len(adapter._apply_filters(records, [Predicate("age", "<", 30)])) == 1
    assert len(adapter._apply_filters(records, [Predicate("age", "<=", 30)])) == 2
    assert len(adapter._apply_filters(records, [Predicate("id", "IN", [1])])) == 1
    assert len(adapter._apply_filters(records, [Predicate("name", "LIKE", "Ali%")])) == 1
    
    # Coercion (hits 323, 325)
    assert len(adapter._apply_filters(records, [Predicate("score", "=", "90.5")])) == 1
    assert len(adapter._apply_filters(records, [Predicate("name", "=", 1)])) == 0
    # IN fallback (hits 355-357)
    assert len(adapter._apply_filters(records, [Predicate("id", "IN", ["1"])])) == 1

def test_fetch_scenarios_full(mock_session, adapter, caplog):
    caplog.set_level(logging.WARNING, logger="waveql.adapters.rest_adapter")
    
    # 1. JSON format & unhandled predicate (hits 93-104)
    adapter._endpoints.update({"t": {"path": "/t", "filter_format": "json", "supports_limit": True}})
    mock_session.get.return_value.status_code = 200
    mock_session.get.return_value.json.return_value = [{"id": 1, "name": "Alice"}]
    table = adapter.fetch("t", predicates=[Predicate("id", "=", 1), Predicate("name", "LIKE", "A%")])
    assert len(table) == 1
    
    # 2. Hits line 152: remaining <= 0
    mock_session.get.reset_mock()
    mock_session.get.return_value.json.return_value = [{"id": 1}]
    adapter.fetch("users", limit=1)
    assert mock_session.get.call_count == 1
    
    # 3. Hits line 173: break if not batch_records
    mock_session.get.return_value.json.return_value = []
    res = adapter.fetch("users")
    assert len(res) == 0
    
    # 4. Client side slicing (hits 212-220)
    adapter._endpoints["users"]["supports_limit"] = False
    adapter._endpoints["users"]["supports_offset"] = False
    mock_session.get.return_value.json.return_value = [{"id": i} for i in range(10)]
    table2 = adapter.fetch("users", limit=2, offset=5)
    assert len(table2) == 2
    assert int(table2.to_pylist()[0]["id"]) == 5

    # 5. Safety limit (hits 188)
    adapter._max_auto_fetch = 1
    mock_session.get.return_value.json.return_value = [{"id": i} for i in range(100)]
    with patch("waveql.adapters.rest_adapter.logger") as mock_logger:
        adapter.fetch("users")
        assert mock_logger.warning.called

def test_crud_ops_full(mock_session, adapter):
    mock_session.post.return_value.status_code = 201
    assert adapter.insert("users", {"id": 1}) == 1
    
    # Update needs an ID in predicates
    mock_session.patch.return_value.status_code = 200
    assert adapter.update("users", {"name": "A"}, [Predicate("id", "=", 1)]) == 1
    
    # Delete needs an ID in predicates
    mock_session.delete.return_value.status_code = 204
    assert adapter.delete("users", [Predicate("id", "=", 1)]) == 1
    
    # CRUD Errors (hits 481, 517-518, 538, 550)
    mock_session.post.side_effect = requests.RequestException("Boom")
    with pytest.raises(QueryError, match="INSERT failed"):
        adapter.insert("users", {"id": 1})
        
    with pytest.raises(QueryError, match="UPDATE requires"):
        adapter.update("users", {"a": 1}, [])
        
    with pytest.raises(QueryError, match="DELETE requires"):
        adapter.delete("users", [])
        
    mock_session.patch.side_effect = requests.RequestException("Boom")
    with pytest.raises(QueryError, match="UPDATE failed"):
        adapter.update("users", {"a": 1}, [Predicate("id", "=", 1)])
        
    mock_session.delete.side_effect = requests.RequestException("Boom")
    with pytest.raises(QueryError, match="DELETE failed"):
        adapter.delete("users", [Predicate("id", "=", 1)])

def test_schema_cache_full(mock_session, adapter):
    mock_session.get.return_value.json.return_value = [{"id": 1}]
    s1 = adapter.get_schema("users")
    assert mock_session.get.call_count == 1
    s2 = adapter.get_schema("users")
    assert mock_session.get.call_count == 1 # Hits 458
    
    # _get_or_discover_schema also hits cache at 377
    adapter.fetch("users")
    assert mock_session.get.call_count == 2 # one more for fetch itself

def test_arrow_utils_full(adapter):
    import pyarrow as pa
    assert adapter._arrow_type_to_string(pa.int64()) == "integer"
    assert adapter._arrow_type_to_string(pa.float64()) == "float"
    assert adapter._arrow_type_to_string(pa.bool_()) == "boolean"
    assert adapter._arrow_type_to_string(pa.struct([pa.field("f", pa.int32())])) == "struct"
    assert adapter._arrow_type_to_string(pa.list_(pa.int32())) == "list"
    assert adapter._arrow_type_to_string(pa.string()) == "string"
    
    # Hits 436 (skip missing column in selected_columns)
    schema = [ColumnInfo("id", "integer")]
    table = adapter._to_arrow([{"id": 1, "extra": 2}], schema, selected_columns=["id", "missing"])
    assert table.column_names == ["id"]

def test_fetch_edge_cases(mock_session, adapter):
    # Hits 173 (break on no records)
    mock_session.get.return_value.json.return_value = []
    assert len(adapter.fetch("users")) == 0
    
    # Hits 380 (return [] if not records in schema discovery)
    assert adapter._get_or_discover_schema("t", []) == []

@pytest.mark.asyncio
async def test_async_wrappers_full(adapter):
    with patch.object(adapter, "fetch", return_value=pa.table({"a": []})):
        await adapter.fetch_async("users")
    with patch.object(adapter, "get_schema", return_value=[]):
        await adapter.get_schema_async("users")
    with patch.object(adapter, "insert", return_value=1):
        await adapter.insert_async("users", {})
    with patch.object(adapter, "update", return_value=1):
        await adapter.update_async("users", {}, [Predicate("id", "=", 1)])
    with patch.object(adapter, "delete", return_value=1):
        await adapter.delete_async("users", [Predicate("id", "=", 1)])
