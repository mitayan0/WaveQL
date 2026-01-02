
import pytest
import responses
import pyarrow as pa
from waveql.adapters.rest_adapter import RESTAdapter
from waveql.query_planner import Predicate
from waveql.exceptions import QueryError

@pytest.fixture
def mock_rest_adapter():
    endpoints = {
        "users": {
            "path": "/users",
            "id_field": "id"
        },
        "posts": {
            "path": "/posts",
            "filter_format": "query"
        }
    }
    return RESTAdapter("https://api.example.com", endpoints=endpoints)

@responses.activate
def test_fetch_users(mock_rest_adapter):
    # Mock response
    users_data = [
        {"id": 1, "name": "Alice", "role": "admin"},
        {"id": 2, "name": "Bob", "role": "user"}
    ]
    responses.add(
        responses.GET,
        "https://api.example.com/users",
        json=users_data,
        status=200
    )
    
    # Exec fetch
    table = mock_rest_adapter.fetch("users")
    
    assert isinstance(table, pa.Table)
    assert len(table) == 2
    assert table.column("name")[0].as_py() == "Alice"
    assert table.column("role")[1].as_py() == "user"

@responses.activate
def test_fetch_users_with_filter(mock_rest_adapter):
    # Mock response with query params
    users_data = [{"id": 1, "name": "Alice", "role": "admin"}]
    
    # Expect query params in URL
    responses.add(
        responses.GET,
        "https://api.example.com/users?role=admin",
        json=users_data,
        status=200,
        match_querystring=True
    )
    
    predicates = [Predicate("role", "=", "admin")]
    table = mock_rest_adapter.fetch("users", predicates=predicates)
    
    assert len(table) == 1
    assert table.column("name")[0].as_py() == "Alice"

@responses.activate
def test_fetch_users_limit_offset(mock_rest_adapter):
    # The adapter seems to default to limit/offset params
    users_data = [{"id": 3, "name": "Charlie"}]
    
    responses.add(
        responses.GET,
        "https://api.example.com/users?limit=1&offset=2",
        json=users_data,
        status=200,
        match_querystring=True
    )
    
    table = mock_rest_adapter.fetch("users", limit=1, offset=2)
    assert len(table) == 1

@responses.activate
def test_insert_user(mock_rest_adapter):
    responses.add(
        responses.POST,
        "https://api.example.com/users",
        json={"id": 3, "name": "Charlie"},
        status=201
    )
    
    count = mock_rest_adapter.insert("users", values={"name": "Charlie"})
    assert count == 1
    assert len(responses.calls) == 1
    assert responses.calls[0].request.body == b'{"name": "Charlie"}'

@responses.activate
def test_update_user(mock_rest_adapter):
    responses.add(
        responses.PATCH,
        "https://api.example.com/users/1",
        json={"id": 1, "name": "Alice Updated"},
        status=200
    )
    
    predicates = [Predicate("id", "=", 1)]
    count = mock_rest_adapter.update("users", values={"name": "Alice Updated"}, predicates=predicates)
    assert count == 1

@responses.activate
def test_delete_user(mock_rest_adapter):
    responses.add(
        responses.DELETE,
        "https://api.example.com/users/1",
        status=204
    )
    
    predicates = [Predicate("id", "=", 1)]
    count = mock_rest_adapter.delete("users", predicates=predicates)
    assert count == 1

@responses.activate
def test_client_side_filtering(mock_rest_adapter):
    # If the API returns more data than requested (e.g. doesn't support a specific filter),
    # the adapter should filter it client-side.
    
    # We simulate this by returning ALL users even though we asked for role=admin
    # AND we assume the adapter relies on 'supports_filter' config being True/False
    # But currently the adapter blindly sends params. 
    # However, if we send a filter that the API ignores, we get all data.
    # The adapter implements _apply_filters ONLY IF config["supports_filter"] is False.
    
    # Let's override the config for this test
    mock_rest_adapter._endpoints["users"]["supports_filter"] = False
    
    users_data = [
        {"id": 1, "name": "Alice", "role": "admin"},
        {"id": 2, "name": "Bob", "role": "user"}
    ]
    
    responses.add(
        responses.GET,
        "https://api.example.com/users",
        json=users_data,
        status=200
    )
    
    predicates = [Predicate("role", "=", "admin")]
    table = mock_rest_adapter.fetch("users", predicates=predicates)
    
    # Should only have Alice
    assert len(table) == 1
    assert table.column("name")[0].as_py() == "Alice"
