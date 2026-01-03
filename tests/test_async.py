
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


@pytest.mark.asyncio
async def test_async_cursor_repr():
    """Test AsyncWaveQLCursor __repr__."""
    async with respx.mock:
        respx.get("https://test.service-now.com/api/now/table/incident").mock(
            return_value=httpx.Response(200, json={"result": MOCK_INCIDENTS})
        )
        
        conn = await connect_async(
            adapter="servicenow",
            host="test.service-now.com",
            username="admin",
            password="password"
        )
        
        async with conn:
            cursor = await conn.cursor()
            repr_str = repr(cursor)
            assert "<AsyncWaveQLCursor" in repr_str
            assert "status=open" in repr_str
            
            await cursor.execute("SELECT * FROM incident")
            repr_str = repr(cursor)
            assert "rows=2" in repr_str
            
            await cursor.close()
            repr_str = repr(cursor)
            assert "status=closed" in repr_str


@pytest.mark.asyncio
async def test_async_cursor_fetchmany():
    """Test AsyncWaveQLCursor fetchmany()."""
    # Use more mock incidents for fetchmany testing
    more_incidents = [
        {"sys_id": str(i), "number": f"INC00{i}", "short_description": f"Test {i}"}
        for i in range(1, 6)
    ]
    
    async with respx.mock:
        respx.get("https://test.service-now.com/api/now/table/incident").mock(
            return_value=httpx.Response(200, json={"result": more_incidents})
        )
        
        conn = await connect_async(
            adapter="servicenow",
            host="test.service-now.com",
            username="admin",
            password="password"
        )
        
        async with conn:
            cursor = await conn.cursor()
            await cursor.execute("SELECT * FROM incident")
            
            rows = cursor.fetchmany(2)
            assert len(rows) == 2
            
            rows = cursor.fetchmany(2)
            assert len(rows) == 2
            
            rows = cursor.fetchmany(10)
            assert len(rows) == 1


@pytest.mark.asyncio
async def test_async_cursor_arraysize():
    """Test AsyncWaveQLCursor arraysize property."""
    async with respx.mock:
        respx.get("https://test.service-now.com/api/now/table/incident").mock(
            return_value=httpx.Response(200, json={"result": MOCK_INCIDENTS})
        )
        
        conn = await connect_async(
            adapter="servicenow",
            host="test.service-now.com",
            username="admin",
            password="password"
        )
        
        async with conn:
            cursor = await conn.cursor()
            
            assert cursor.arraysize == 100
            cursor.arraysize = 25
            assert cursor.arraysize == 25
            
            await cursor.execute("SELECT * FROM incident")
            rows = cursor.fetchmany()  # Uses arraysize (but only 2 results available)
            assert len(rows) == 2


@pytest.mark.asyncio
async def test_async_connection_repr():
    """Test AsyncWaveQLConnection __repr__."""
    async with respx.mock:
        conn = await connect_async(
            adapter="servicenow",
            host="test.service-now.com",
            username="admin",
            password="password"
        )
        
        repr_str = repr(conn)
        assert "<AsyncWaveQLConnection" in repr_str
        assert "status=open" in repr_str
        assert "servicenow" in repr_str
        
        await conn.close()
        repr_str = repr(conn)
        assert "status=closed" in repr_str


if __name__ == "__main__":
    import anyio
    anyio.run(test_async_fetch)
    print("Async Fetch Test Passed!")
    anyio.run(test_async_insert)
    print("Async Insert Test Passed!")

