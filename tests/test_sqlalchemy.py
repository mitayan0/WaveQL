
import sqlalchemy
from sqlalchemy import create_engine, inspect
import respx
import httpx
import pandas as pd
import re

# Mock data
MOCK_INCIDENTS = [
    {"sys_id": "1", "number": "INC001", "short_description": "Dialect Test"},
]

@respx.mock
def test_sqlalchemy_dialect():
    # Setup mock for data
    respx.get(re.compile(r"https://test.service-now.com/api/now/table/incident.*")).mock(
        return_value=httpx.Response(200, json={"result": MOCK_INCIDENTS})
    )
    
    # Setup mock for table listing
    respx.get(re.compile(r"https://test.service-now.com/api/now/table/sys_db_object.*")).mock(
        return_value=httpx.Response(200, json={"result": [{"name": "incident", "label": "Incident"}]})
    )
    
    # Mock column listing (sys_dictionary)
    respx.get(re.compile(r"https://test.service-now.com/api/now/table/sys_dictionary.*")).mock(
        return_value=httpx.Response(200, json={"result": [
            {"element": "number", "column_label": "Number", "internal_type": {"value": "string"}},
            {"element": "short_description", "column_label": "Short Description", "internal_type": {"value": "string"}}
        ]})
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

    # Test inspection methods
    assert inspector.has_table("incident")
    # Adapter is registered as 'default' internally
    schemas = inspector.get_schema_names()
    assert "default" in schemas
    # Dedup check if dialect doesn't dedup
    assert len(set(schemas)) == 1 or "servicenow" not in schemas
    
    pk = inspector.get_pk_constraint("incident")
    # ServiceNow adapter now correctly identifies sys_id as the primary key
    assert pk["constrained_columns"] == ["sys_id"]
    
    assert inspector.get_foreign_keys("incident") == []
    assert inspector.get_indexes("incident") == []
    assert inspector.get_unique_constraints("incident") == []
    assert inspector.get_check_constraints("incident") == []
    assert inspector.get_table_comment("incident") == {"text": None}
    
    # Test query execution
    print("Executing SQL via SQLAlchemy...")
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT number FROM incident"))
        rows = result.fetchall()
        print(f"Result: {rows}")
        assert len(rows) == 1
        assert rows[0][0] == "INC001"

    # Test Pandas integration
    print("Testing Pandas read_sql...")
    df = pd.read_sql("SELECT number FROM incident", engine)
    print(f"Pandas DataFrame:\n{df}")
    assert len(df) == 1
    assert df.iloc[0]['number'] == "INC001"

def test_dialect_url_parsing():
    # Test without +adapter, with credentials in URL
    engine = create_engine("waveql://u:p@test.host?adapter=servicenow")
    
    # Verify connect args parsing (indirectly via dialect)
    dialect = engine.dialect
    args, kwargs = dialect.create_connect_args(engine.url)
    assert kwargs["host"] == "test.host"
    assert kwargs["adapter"] == "servicenow"
    assert kwargs["username"] == "u"
    assert kwargs["password"] == "p"

def test_dialect_invalid_schema():
    # Use a real engine but ask for a non-existent schema
    engine = create_engine("waveql+servicenow://test.host")
    inspector = inspect(engine)
    
    # These should return empty lists if schema doesn't exist (hits early returns in dialect)
    assert inspector.get_table_names(schema="ghost") == []
    assert inspector.get_columns("any", schema="ghost") == []

if __name__ == "__main__":
    test_sqlalchemy_dialect()
