import pytest
import sqlalchemy as sa
import pyarrow as pa
from waveql.adapters.sql import SQLAdapter
from waveql.query_planner import Predicate

@pytest.fixture
def sqlite_engine():
    """Create a temporary SQLite database with some data."""
    engine = sa.create_engine("sqlite:///")
    metadata = sa.MetaData()
    
    users = sa.Table(
        "users", metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("age", sa.Integer),
    )
    
    metadata.create_all(engine)
    
    with engine.connect() as conn:
        conn.execute(users.insert(), [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ])
        conn.commit()
        
    return engine

# NOTE: sql_adapter fixture was removed - use test_sql_adapter_sqlite_file instead
# which creates a file-based SQLite for stable testing.

def test_sql_adapter_sqlite_file(tmp_path):
    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"
    
    # 1. Setup DB
    engine = sa.create_engine(url)
    metadata = sa.MetaData()
    users = sa.Table(
        "users", metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("age", sa.Integer),
        sa.Column("active", sa.Boolean),
    )
    metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(users.insert(), [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
        ])
        conn.commit()
    
    # 2. Initialize Adapter
    adapter = SQLAdapter(host=url)
    
    # 3. Test Fetch All
    result = adapter.fetch("users")
    assert len(result) == 3
    assert result.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]
    
    # 4. Test Predicates
    pred = Predicate("age", ">", 28)
    result = adapter.fetch("users", predicates=[pred])
    assert len(result) == 2
    assert sorted(result.column("name").to_pylist()) == ["Alice", "Charlie"]
    
    # 5. Test Columns selection
    result = adapter.fetch("users", columns=["name"])
    assert result.column_names == ["name"]
    
    # 6. Test Group By & Aggregation
    # We need to construct Aggregate objects which might be simple mocks or importing class if available
    class MockAggregate:
        def __init__(self, func, column, alias=None):
            self.func = func
            self.column = column
            self.alias = alias
            
    # Count(*)
    aggs = [MockAggregate("COUNT", "id", alias="count")]
    result = adapter.fetch("users", aggregates=aggs)
    assert result.column("count")[0].as_py() == 3
    
    # Group By active
    aggs = [MockAggregate("COUNT", "id", alias="count")]
    result = adapter.fetch("users", group_by=["active"], aggregates=aggs)
    assert len(result) == 2
    
    # 7. Test Schema Discovery
    schema = adapter.get_schema("users")
    assert len(schema) == 4
    names = [c.name for c in schema]
    assert "name" in names
    assert "age" in names

    # 8. Test Insert
    adapter.insert("users", {"name": "David", "age": 40, "active": True})
    result = adapter.fetch("users")
    assert len(result) == 4
    
    # 9. Test Update
    adapter.update("users", {"age": 41}, predicates=[Predicate("name", "=","David")])
    result = adapter.fetch("users", predicates=[Predicate("name", "=","David")])
    assert result.column("age")[0].as_py() == 41

    # 10. Test Delete
    adapter.delete("users", predicates=[Predicate("name", "=","David")])
    result = adapter.fetch("users")
    assert len(result) == 3
