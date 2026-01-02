
import sqlalchemy
from sqlalchemy import create_engine, inspect
import responses
import pytest
import pyarrow as pa
import pandas as pd

# Mock data
MOCK_INCIDENTS = [
    {"sys_id": "1", "number": "INC001", "short_description": "Dialect Test"},
]

@responses.activate
def test_sqlalchemy_dialect():
    # Setup mock for data
    responses.add(
        responses.GET,
        "https://test.service-now.com/api/now/table/incident",
        json={"result": MOCK_INCIDENTS},
        status=200,
    )
    
    # Setup mock for table listing
    responses.add(
        responses.GET,
        "https://test.service-now.com/api/now/table/sys_db_object",
        json={"result": [{"name": "incident", "label": "Incident"}]},
        status=200,
    )
    
    # Create SQLAlchemy engine
    # Format: waveql+<adapter>://<host>
    engine = create_engine("waveql+servicenow://test.service-now.com")
    
    print("\nTesting SQLAlchemy Introspection...")
    inspector = inspect(engine)
    
    # Test table listing
    tables = inspector.get_table_names()
    print(f"Tables found: {tables}")
    assert "incident" in tables
    
    # Test column listing
    columns = inspector.get_columns("incident")
    print(f"Columns in 'incident': {[c['name'] for c in columns]}")
    assert any(c['name'] == 'number' for c in columns)
    
    # Test query execution
    print("Executing SQL via SQLAlchemy...")
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT number, short_description FROM incident"))
        rows = result.fetchall()
        print(f"Result: {rows}")
        assert len(rows) == 1
        assert rows[0][0] == "INC001"

    # Test Pandas integration
    print("Testing Pandas read_sql...")
    df = pd.read_sql("SELECT * FROM incident", engine)
    print(f"Pandas DataFrame:\n{df}")
    assert len(df) == 1
    assert df.iloc[0]['number'] == "INC001"

if __name__ == "__main__":
    test_sqlalchemy_dialect()
