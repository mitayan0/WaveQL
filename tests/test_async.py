
import asyncio
import pytest
import respx
import httpx
from waveql import connect_async

MOCK_INCIDENTS = [
    {"sys_id": "1", "number": "INC001", "short_description": "Async Test 1"},
    {"sys_id": "2", "number": "INC002", "short_description": "Async Test 2"},
]

@pytest.mark.asyncio
async def test_async_fetch():
    async with respx.mock:
        # Mock ServiceNow Table API
        respx.get("https://test.service-now.com/api/now/table/incident").mock(
            return_value=httpx.Response(200, json={"result": MOCK_INCIDENTS})
        )
        
        # Connect asynchronously
        conn = await connect_async(
            adapter="servicenow",
            host="test.service-now.com",
            username="admin",
            password="password"
        )
        
        async with conn:
            cursor = await conn.cursor()
            
            # Execute query
            await cursor.execute("SELECT number, short_description FROM incident")
            
            # Fetch results
            results = cursor.fetchall()
            
            assert len(results) == 2
            assert results[0][0] == "INC001"
            assert results[1][0] == "INC002"
            
            # Test Arrow conversion
            table = cursor.to_arrow()
            assert table is not None
            assert len(table) == 2

@pytest.mark.asyncio
async def test_async_insert():
    async with respx.mock:
        respx.post("https://test.service-now.com/api/now/table/incident").mock(
            return_value=httpx.Response(201, json={"result": {"sys_id": "3"}})
        )
        
        conn = await connect_async(
            adapter="servicenow",
            host="test.service-now.com",
            username="admin",
            password="password"
        )
        
        async with conn:
            cursor = await conn.cursor()
            await cursor.execute("INSERT INTO incident (short_description) VALUES ('New Item')")
            assert cursor.rowcount == 1

if __name__ == "__main__":
    import anyio
    anyio.run(test_async_fetch)
    print("Async Fetch Test Passed!")
    anyio.run(test_async_insert)
    print("Async Insert Test Passed!")
