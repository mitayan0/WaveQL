
import pytest
import requests
import json
from unittest.mock import MagicMock, patch
from waveql.adapters.rest_adapter import RESTAdapter
from waveql.exceptions import AdapterError, QueryError
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
    # Mock the rate limiter to just execute the function
    # But for rate limit test we might want the real one or a controlled one
    return a

def test_build_params_json_format(adapter, mock_session):
    """Test JSON filter format parameter generation."""
    adapter._endpoints["users"]["filter_format"] = "json"
    adapter._endpoints["users"]["filter_param"] = "q"
    
    mock_session.get.return_value.json.return_value = []
    
    # Test = operator
    adapter.fetch("users", predicates=[Predicate("age", "=", 25)])
    call_args = mock_session.get.call_args
    assert call_args is not None
    params = call_args[1]["params"]
    assert "q" in params
    query = json.loads(params["q"])
    assert query == {"age": 25}
    
    # Test IN operator
    adapter.fetch("users", predicates=[Predicate("status", "IN", ["active", "pending"])])
    params = mock_session.get.call_args[1]["params"]
    query = json.loads(params["q"])
    assert query == {"status": {"$in": ["active", "pending"]}}

def test_build_params_query_format_types(adapter, mock_session):
    """Test standard query format with different operators."""
    adapter._endpoints["users"]["filter_format"] = "query"
    adapter._endpoints["users"]["supports_like"] = True
    
    mock_session.get.return_value.json.return_value = []
    
    # Test LIKE
    adapter.fetch("users", predicates=[Predicate("name", "LIKE", "Ali%")])
    params = mock_session.get.call_args[1]["params"]
    assert params["name_like"] == "Ali%"
    
    # Test IN (repeated params)
    adapter.fetch("users", predicates=[Predicate("id", "IN", [1, 2])])
    # note: param dict construction simple assignment might overwrite if not handled by requests
    # RESTAdapter implementation: params[pred.column] = pred.value
    # requests handles list as repeated params automatically.
    params = mock_session.get.call_args[1]["params"]
    assert params["id"] == [1, 2]

def test_rate_limit_retry(adapter, mock_session):
    """Test that 429 responses trigger retry logic."""
    # We need to unmock the rate limiter? RESTAdapter uses a RateLimiter instance.
    # But here 'adapter' fixture just creates it.
    # The default RESTAdapter uses waveql.utils.rate_limiter.RateLimiter which uses time.sleep.
    # We should mock time.sleep to avoid waiting.
    
    with patch("time.sleep") as mock_sleep:
        # First call 429, second call 200
        response_429 = MagicMock()
        response_429.status_code = 429
        response_429.headers = {"Retry-After": "1"}
        
        response_200 = MagicMock()
        response_200.status_code = 200
        response_200.json.return_value = [{"id": 1}]
        
        mock_session.get.side_effect = [response_429, response_200]
        
        results = adapter.fetch("users")
        assert len(results) == 1
        assert mock_session.get.call_count == 2
        mock_sleep.assert_called_with(1.0) # Retry-after is 1

def test_fetch_request_exception(adapter, mock_session):
    """Test request exception handling in fetch."""
    mock_session.get.side_effect = requests.RequestException("Connection failed")
    
    with pytest.raises(AdapterError, match="REST request failed"):
        adapter.fetch("users")

def test_apply_filters_type_errors(adapter):
    """Test robust error handling in client-side filtering."""
    # Create records with types that might cause comparison errors
    records = [
        {"val": None},
        {"val": "string"},
        {"val": 10},
    ]
    
    # GREATER THAN >
    # "string" > 5 raises TypeError in Python 3. None > 5 raises TypeError.
    # The code catches TypeError and treats as False (mismatch).
    
    # Predicate: val > 5
    filtered = adapter._apply_filters(records, [Predicate("val", ">", 5)])
    assert len(filtered) == 1
    assert filtered[0]["val"] == 10
    
    # LESS THAN <
    filtered = adapter._apply_filters(records, [Predicate("val", "<", 5)])
    assert len(filtered) == 0 # "string" < 5 is False (caught), None < 5 is False (caught)
    
    # IN fallback type matching
    records_mixed = [{"id": 1}, {"id": "2"}]
    # Predicate IN ["1", "2"] -> Strings
    # 1 (int) should match "1" via string fallback if direct match fails?
    # Logic in code: if value in p_values (1 in ["1", "2"] -> False)
    # else: str(value) in str_p_values ("1" in ["1", "2"] -> True)
    filtered = adapter._apply_filters(records_mixed, [Predicate("id", "IN", ["1", "2"])])
    assert len(filtered) == 2

def test_data_path_navigation_edge_cases(adapter):
    """Test complex data path navigation."""
    # data is list, key is not digit
    data = [{"id": 1}]
    config = {"data_path": "items.0"}
    
    # data_path splits to ["items", "0"]
    # data is list. key "items" IS NOT DIGIT -> returns []
    # But wait, code says: elif isinstance(data, list) and key.isdigit():
    # If not digit, it goes to `else: return []`.
    assert adapter._extract_records(data, config) == []
    
    # Correct deep nesting
    data_deep = {"response": {"payload": {"items": [{"id": 1}]}}}
    config_deep = {"data_path": "response.payload.items"}
    assert adapter._extract_records(data_deep, config_deep) == [{"id": 1}]

def test_to_arrow_projection(adapter):
    """Test column projection in to_arrow."""
    records = [{"id": 1, "name": "Alice", "secret": "hidden"}]
    schema = [
        ColumnInfo("id", "integer"),
        ColumnInfo("name", "string"),
        ColumnInfo("secret", "string")
    ]
    
    # Projection
    table = adapter._to_arrow(records, schema, selected_columns=["id", "name"])
    assert table.column_names == ["id", "name"]
    assert "secret" not in table.column_names
    
    # Wildcard
    table_all = adapter._to_arrow(records, schema, selected_columns=["*"])
    assert "secret" in table_all.column_names

def test_insert(adapter, mock_session):
    """Test insert method."""
    mock_session.post.return_value.status_code = 201
    mock_session.post.return_value.json.return_value = {"id": 101}
    
    values = {"name": "New User", "age": 30}
    adapter.insert("users", values)
    
    args = mock_session.post.call_args
    assert args[0][0] == "https://api.example.com/v1/users"
    assert args[1]["json"] == values
    
def test_update(adapter, mock_session):
    """Test update method."""
    mock_session.patch.return_value.status_code = 200
    mock_session.patch.return_value.json.return_value = {"id": 1, "name": "Updated"}
    
    values = {"name": "Updated"}
    predicates = [Predicate("id", "=", 1)]
    adapter.update("users", values, predicates)
    
    args = mock_session.patch.call_args
    assert args[0][0] == "https://api.example.com/v1/users/1"
    assert args[1]["json"] == values

def test_update_no_id(adapter):
    """Test update without ID raises error."""
    with pytest.raises(QueryError, match="UPDATE requires id in WHERE"):
        adapter.update("users", {"name": "Updated"}, [])

def test_delete(adapter, mock_session):
    """Test delete method."""
    mock_session.delete.return_value.status_code = 204
    
    predicates = [Predicate("id", "=", 1)]
    adapter.delete("users", predicates)
    
    args = mock_session.delete.call_args
    assert args[0][0] == "https://api.example.com/v1/users/1"

def test_delete_no_id(adapter):
    """Test delete without ID raises error."""
    with pytest.raises(QueryError, match="DELETE requires id in WHERE"):
        adapter.delete("users", [])

def test_fetch_group_by_raises(adapter):
    """Test that group_by raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        adapter.fetch("users", group_by=["age"])

def test_fetch_pagination_client_side(adapter, mock_session):
    """Test fetching with client-side limit/offset when pushdown is not possible."""
    # Configure endpoint to NOT support limit/offset
    adapter._endpoints["users"]["supports_limit"] = False
    adapter._endpoints["users"]["supports_offset"] = False
    
    # Mock data return - 2 pages
    page1 = [{"id": i} for i in range(1, 101)] # 100 items
    page2 = [{"id": i} for i in range(101, 151)] # 50 items
    
    mock_session.get.side_effect = [
        MagicMock(status_code=200, json=lambda: page1),
        MagicMock(status_code=200, json=lambda: page2),
        MagicMock(status_code=200, json=lambda: []) # End
    ]
    
    # Request fetch with limit 10, offset 5
    # Since no pushdown, it should fetch all and slice locally
    results = adapter.fetch("users", limit=10, offset=5)
    
    assert len(results) == 10
    assert results.to_pylist()[0]["id"] == 6 # 1-indexed range(1, 101) -> 0 is 1. Offset 5 means index 5 -> 6.

def test_arrow_type_conversion(adapter):
    import pyarrow as pa
    assert adapter._arrow_type_to_string(pa.bool_()) == "boolean"
    assert adapter._arrow_type_to_string(pa.int64()) == "integer"
    assert adapter._arrow_type_to_string(pa.float64()) == "float"
    assert adapter._arrow_type_to_string(pa.struct([('a', pa.int64())])) == "struct"
    assert adapter._arrow_type_to_string(pa.list_(pa.int64())) == "list"
    assert adapter._arrow_type_to_string(pa.string()) == "string"

@pytest.mark.asyncio
async def test_async_wrapper_methods(adapter):
    """Test async wrapper methods."""
    with patch.object(adapter, "fetch") as mock_fetch:
        mock_fetch.return_value = "fetched"
        res = await adapter.fetch_async("users")
        assert res == "fetched"
        mock_fetch.assert_called_once()
    
    with patch.object(adapter, "insert") as mock_insert:
        mock_insert.return_value = 1
        res = await adapter.insert_async("users", {})
        assert res == 1
        mock_insert.assert_called_once()
    
    with patch.object(adapter, "update") as mock_update:
        mock_update.return_value = 1
        res = await adapter.update_async("users", {})
        assert res == 1
        mock_update.assert_called_once()
    
    with patch.object(adapter, "delete") as mock_delete:
        mock_delete.return_value = 1
        res = await adapter.delete_async("users")
        assert res == 1
        mock_delete.assert_called_once()
        
    with patch.object(adapter, "get_schema") as mock_schema:
        mock_schema.return_value = []
        res = await adapter.get_schema_async("users")
        assert res == []
        mock_schema.assert_called_once()

def test_fetch_safety_limit(adapter, mock_session):
    """Test that warning is logged when safety limit is hit."""
    adapter._max_auto_fetch = 5
    
    # Return 100 items to match batch_size (100) and sustain the loop to hit safety limit check
    mock_session.get.return_value.json.return_value = [{"id": i} for i in range(100)]
    
    with patch("waveql.adapters.rest_adapter.logger") as mock_logger:
        adapter.fetch("users")
        
        # It fetches one batch of 100, then realizes 100 >= 5 and stops with warning
        mock_logger.warning.assert_called()
        assert "Auto-pagination hit safety limit" in mock_logger.warning.call_args[0][0]

def test_default_endpoint_config(adapter):
    """Test falling back to default endpoint config."""
    config = adapter._get_endpoint_config("unknown_table")
    assert config["path"] == "/unknown_table"

def test_client_side_filtering_extended(adapter):
    """Test more operators for client-side filtering."""
    records = [{"val": 10}, {"val": 20}, {"val": "hello"}]
    
    # !=
    res = adapter._apply_filters(records, [Predicate("val", "!=", 10)])
    assert len(res) == 2
    
    # >=
    res = adapter._apply_filters(records, [Predicate("val", ">=", 20)])
    assert len(res) == 1
    assert res[0]["val"] == 20
    
    # <=
    res = adapter._apply_filters(records, [Predicate("val", "<=", 10)])
    assert len(res) == 1
    assert res[0]["val"] == 10

    # Coercion float
    records_str = [{"val": "10.5"}]
    res = adapter._apply_filters(records_str, [Predicate("val", ">", 10.0)])
    assert len(res) == 1
    
    # Coercion int
    records_int_str = [{"val": "10"}]
    res = adapter._apply_filters(records_int_str, [Predicate("val", "=", 10)])
    assert len(res) == 1

def test_client_side_like(adapter):
    """Test client-side LIKE operator."""
    records = [{"val": "Hello World"}, {"val": "Foo Bar"}]
    # Case insensitive match
    res = adapter._apply_filters(records, [Predicate("val", "LIKE", "hell%")])
    assert len(res) == 1
    assert res[0]["val"] == "Hello World"
    # Underscore
    res = adapter._apply_filters(records, [Predicate("val", "LIKE", "F_o%")])
    assert len(res) == 1

def test_crud_exceptions(adapter, mock_session):
    """Test exceptions in write operations."""
    mock_session.post.side_effect = requests.RequestException("err")
    with pytest.raises(QueryError):
        adapter.insert("u", {})
        
    mock_session.patch.side_effect = requests.RequestException("err")
    with pytest.raises(QueryError):
        adapter.update("u", {"v": 1}, [Predicate("id", "=", 1)])
        
    mock_session.delete.side_effect = requests.RequestException("err")
    with pytest.raises(QueryError):
        adapter.delete("u", [Predicate("id", "=", 1)])

def test_extract_records_complex(adapter):
    """Test complex extraction logic."""
    # Single dict
    assert adapter._extract_records({"a": 1}, {}) == [{"a": 1}]
    
    # List index path
    data = [[{"a": 1}]]
    config = {"data_path": "0"}
    assert adapter._extract_records(data, config) == [{"a": 1}]
    
    # Invalid path
    config = {"data_path": "response.items"}
    assert adapter._extract_records({}, config) == []
    
    # Unknown type
    assert adapter._extract_records(123, {}) == []

def test_schema_caching_hit(adapter):
    """Test schema cache hit."""
    adapter._cache_schema("t", [ColumnInfo("c", "string")])
    # Should use cached version and not call fetch
    with patch.object(adapter, "fetch") as mock_fetch:
        s = adapter.get_schema("t")
        mock_fetch.assert_not_called()
        assert len(s) == 1
        assert s[0].name == "c"

def test_unhandled_filter_fetching(adapter, mock_session):
    """Test fetching where filtering must happen client-side."""
    # Setup endpoint that does not support filtering
    adapter._endpoints["nofilter"] = {"path": "/nofilter", "supports_filter": False}
    mock_session.get.return_value.json.return_value = [{"id": 1, "val": 10}, {"id": 2, "val": 20}]
    
    # Predicate should be applied client side
    res = adapter.fetch("nofilter", predicates=[Predicate("val", "=", 20)])
    assert len(res) == 1
    assert res.to_pylist()[0]["id"] == 2

def test_limit_pushdown_pagination(adapter, mock_session):
    """Test pagination splitting logic when limit is pushed down."""
    adapter._endpoints["users"]["supports_limit"] = True
    
    # Logic in code:
    # 1. limit=150.
    # 2. fetch iteration 1: remaining=150. batch_size=min(100, 150)=100.
    # 3. fetch iteration 2: remaining=50. batch_size=min(100, 50)=50.
    
    def side_effect(*args, **kwargs):
        p = kwargs["params"]
        l = int(p.get("limit", 100))
        return MagicMock(status_code=200, json=lambda: [{"id": i} for i in range(l)])
        
    mock_session.get.side_effect = side_effect
    
    results = adapter.fetch("users", limit=150)
    # Total 100 + 50 = 150
    assert len(results) == 150
    assert mock_session.get.call_count == 2
    
def test_offset_params(adapter, mock_session):
    """Test offset parameter generation."""
    adapter.fetch("users", offset=50)
    params = mock_session.get.call_args[1]["params"]
    assert params["offset"] == "50"

def test_arrow_type_fallback(adapter):
    """Test arrow type fallback."""
    import pyarrow as pa
    # Test unknown type
    assert adapter._arrow_type_to_string(pa.binary()) == "string"

def test_params_equals_operator(adapter, mock_session):
    """Explicitly test params construction for = operator."""
    mock_session.get.return_value.json.return_value = []
    adapter.fetch("users", predicates=[Predicate("x", "=", 1)])
    params = mock_session.get.call_args[1]["params"]
    assert params["x"] == 1

def test_unhandled_operator_fetch(adapter, mock_session):
    """Test unhandled operator > falls back to client side."""
    mock_session.get.return_value.json.return_value = [{"id": 1, "val": 5}, {"id": 2, "val": 15}]
    
    # Val > 10
    res = adapter.fetch("users", predicates=[Predicate("val", ">", 10)])
    
    assert len(res) == 1
    assert res.to_pylist()[0]["val"] == 15
    
    # Verify params didn't include the predicate (default query format only handles =, IN)
    params = mock_session.get.call_args[1]["params"]
    assert "val" not in params

def test_fetch_empty_with_schema(adapter, mock_session):
    """Test fetching empty results when schema is already known."""
    import pyarrow as pa
    adapter._cache_schema("emptytable", [ColumnInfo("col1", "integer", arrow_type=pa.int64())])
    
    mock_session.get.return_value.json.return_value = []
    
    res = adapter.fetch("emptytable")
    assert len(res) == 0
    assert res.schema.names == ["col1"]
    # Note: SchemaCache currently does not persist PyArrow type objects.
    # It falls back to default string type.
    assert res.schema.types[0] == pa.string()

def test_fetch_uses_cached_schema_logic(adapter, mock_session):
    """Test fetch hitting cached schema logic in _get_or_discover_schema."""
    adapter._cache_schema("cachedtable", [ColumnInfo("col1", "integer")])
    mock_session.get.return_value.json.return_value = [{"col1": 1}]
    
    res = adapter.fetch("cachedtable")
    assert len(res) == 1

def test_coercion_to_string_filter(adapter):
    """Test value coercion to string in client-side filter."""
    records = [{"id": 123}]
    # Predicate id="123" (string), record id=123 (int)
    # Should coerce record value to string and match
    res = adapter._apply_filters(records, [Predicate("id", "=", "123")])
    assert len(res) == 1
    
    # Verify mismatch is still respected
    res = adapter._apply_filters(records, [Predicate("id", "=", "124")])
    assert len(res) == 0

def test_limit_break_loop(adapter, mock_session):
    """Test breaking pagination loop when exact limit is reached."""
    # Mock return 10 items
    mock_session.get.return_value.json.return_value = [{"id": i} for i in range(10)]
    
    # Limit 10. Fetch 10. Next loop remaining=0 -> break.
    res = adapter.fetch("users", limit=10)
    assert len(res) == 10



def test_get_schema_discovery(adapter):
    """Test get_schema triggers fetch(limit=1) and discovery."""
    with patch.object(adapter, "fetch") as mock_fetch:
        table_mock = MagicMock()
        table_mock.to_pylist.return_value = [{"id": 1, "name": "Test"}]
        mock_fetch.return_value = table_mock
        
        columns = adapter.get_schema("users")
        
        mock_fetch.assert_called_with("users", limit=1)
        assert len(columns) == 2
        names = {c.name for c in columns}
        assert "id" in names
        assert "name" in names


