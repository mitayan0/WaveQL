import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from waveql.adapters.servicenow import ServiceNowAdapter

@pytest.fixture
def adapter():
    return ServiceNowAdapter(host="https://dev12345.service-now.com", auth_manager=MagicMock())

def test_fetch_schema_with_hierarchy_success(adapter):
    """Test successful schema extraction including inherited fields via hierarchy."""
    
    # Mock hierarchy responses (sc_req_item -> task)
    def mock_fetch_page_side_effect(url, params, client):
        # 1. sys_db_object queries
        if "sys_db_object" in url:
            name = params["sysparm_query"].split("=")[1]
            if name == "sc_req_item":
                return [{"super_class.name": "task"}]
            elif name == "task":
                return [{"super_class.name": ""}] # No parent
            return []
            
        # 2. sys_dictionary query
        if "sys_dictionary" in url:
            # Check if query includes both tables
            assert "sc_req_item" in params["sysparm_query"]
            assert "task" in params["sysparm_query"]
            
            return [
                # Child field
                {"element": "cat_item", "internal_type": "reference", "mandatory": "true", "read_only": "false"},
                # Parent field (inherited)
                {"element": "number", "internal_type": "string", "mandatory": "false", "read_only": "true", "default_value": "javascript:getNextObjNumberPadded()"},
                # Parent PK
                {"element": "sys_id", "internal_type": "guid", "mandatory": "false", "read_only": "true", "primary": "true"},
            ]
        return []

    with patch.object(adapter, "_fetch_page", side_effect=mock_fetch_page_side_effect):
        columns = adapter._fetch_schema_from_metadata("sc_req_item")
        
        assert columns is not None
        assert len(columns) == 3
        
        # Check child field
        cat = next(c for c in columns if c.name == "cat_item")
        assert cat.nullable is False
        
        # Check inherited number field
        number = next(c for c in columns if c.name == "number")
        assert number.read_only is True
        assert number.auto_increment is True
        
        # Check inherited sys_id
        sys_id = next(c for c in columns if c.name == "sys_id")
        assert sys_id.primary_key is True

def test_fetch_schema_fallback_on_failure(adapter):
    """Test fallback when hierarchy fetch fails."""
    with patch.object(adapter, "_fetch_page", side_effect=Exception("API Error")):
        columns = adapter._fetch_schema_from_metadata("incident")
        assert columns is None

@pytest.mark.asyncio
async def test_fetch_schema_async_with_hierarchy(adapter):
    """Test async hierarchy fetching."""
    
    # Mocking async client responses is verbose, simplified here:
    mock_client = AsyncMock()
    
    # We need to mock multiple calls: 2 for hierarchy, 1 for dictionary
    async def side_effect(url, params=None, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        
        if "sys_db_object" in url:
            if "sc_req_item" in params.get("sysparm_query", ""):
                 resp.json.return_value = {"result": [{"super_class.name": "task"}]}
            else:
                 resp.json.return_value = {"result": []}
        
        elif "sys_dictionary" in url:
             resp.json.return_value = {"result": [
                 {"element": "number", "mandatory": "false", "read_only": "true", "default_value": "Next Obj Number"}
             ]}
             
        return resp

    mock_client.get.side_effect = side_effect
    
    with patch.object(adapter, "_get_async_client", return_value=mock_client):
        with patch.object(adapter, "_get_auth_headers_async", new_callable=AsyncMock) as mock_auth:
             mock_auth.return_value = {}
             
             columns = await adapter._fetch_schema_from_metadata_async("sc_req_item")
             
             assert columns is not None
             number = next(c for c in columns if c.name == "number")
             assert number.auto_increment is True
             assert number.read_only is True

@pytest.mark.asyncio
async def test_get_or_discover_schema_async_fallback(adapter):
    """Test async fallback to inference."""
    with patch.object(adapter, "_fetch_schema_from_metadata_async", return_value=None):
        records = [{"sys_id": "abc", "test": "val"}]
        result = await adapter._get_or_discover_schema_async("incident", records)
        
        assert len(result) == 2
        assert any(c.name == "test" for c in result)
