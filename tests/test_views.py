"""Tests for virtual views and registry."""

import pytest
import json
import yaml
from pathlib import Path
from waveql.semantic.views import VirtualView, VirtualViewRegistry

# --- VirtualView Tests ---

def test_view_create_validation():
    # Valid name
    view = VirtualView(name="valid_name", sql="SELECT 1")
    assert view.name == "valid_name"
    
    # Invalid name
    with pytest.raises(ValueError):
        VirtualView(name="invalid name", sql="SELECT 1")
    
    with pytest.raises(ValueError):
        VirtualView(name="123start", sql="SELECT 1") # Must start with letter/underscore if I recall regex correctly
    
    # Check regex: ^[a-zA-Z_][a-zA-Z0-9_]*$
    with pytest.raises(ValueError):
        VirtualView(name="1numeric", sql="SELECT 1")

def test_view_dependencies_extraction():
    # Simple dependency
    view = VirtualView(name="v1", sql="SELECT * FROM users")
    assert "users" in view.dependencies
    
    # Ignore keywords
    view = VirtualView(name="v2", sql="SELECT * FROM users JOIN orders ON u.id = o.user_id")
    assert "users" in view.dependencies
    assert "orders" in view.dependencies
    assert "user_id" not in view.dependencies
    
    # Explicit dependencies override
    view = VirtualView(name="v3", sql="SELECT 1", dependencies=["manual"])
    assert view.dependencies == ["manual"]

def test_view_qualified_name():
    view1 = VirtualView(name="v1", sql="SELECT 1")
    assert view1.qualified_name == "v1"
    
    view2 = VirtualView(name="v2", sql="SELECT 1", schema="analytics")
    assert view2.qualified_name == "analytics.v2"

def test_view_serialization():
    view = VirtualView(
        name="v1", 
        sql="SELECT 1", 
        description="desc", 
        schema="public", 
        tags=["t1"],
        metadata={"k": "v"}
    )
    
    data = view.to_dict()
    assert data["name"] == "v1"
    assert data["description"] == "desc"
    
    view2 = VirtualView.from_dict(data)
    assert view2.name == view.name
    assert view2.metadata == view.metadata

# --- Registry Tests ---

@pytest.fixture
def registry():
    return VirtualViewRegistry()

def test_registry_crud(registry):
    view = VirtualView(name="v1", sql="SELECT 1")
    
    # Register
    registry.register(view)
    assert "v1" in registry
    assert registry.get("v1") is view
    assert registry["v1"] is view
    assert len(registry) == 1
    
    # Iteration
    names = list(iter(registry))
    assert names == ["v1"]
    
    # Overwrite
    view2 = VirtualView(name="v1", sql="SELECT 2")
    registry.register(view2)
    assert registry.get("v1") is view2
    
    # Unregister
    assert registry.unregister("v1") is True
    assert "v1" not in registry
    assert registry.unregister("missing") is False

def test_registry_list_views(registry):
    v1 = VirtualView(name="v1", sql="S", schema="s1", tags=["t1"])
    v2 = VirtualView(name="v2", sql="S", schema="s1", tags=["t2"])
    v3 = VirtualView(name="v3", sql="S", schema="s2", tags=["t1"])
    
    registry.register(v1)
    registry.register(v2)
    registry.register(v3)
    
    # All
    assert len(registry.list_views()) == 3
    
    # Schema filter
    s1_views = registry.list_views(schema="s1")
    assert len(s1_views) == 2
    assert v1 in s1_views
    assert v2 in s1_views
    
    # Tag filter
    t1_views = registry.list_views(tag="t1")
    assert len(t1_views) == 2
    assert v1 in t1_views
    assert v3 in t1_views

def test_dependency_ordering(registry):
    # v1 -> v2 -> v3
    v1 = VirtualView(name="v1", sql="SELECT 1")
    v2 = VirtualView(name="v2", sql="SELECT * FROM v1")
    v3 = VirtualView(name="v3", sql="SELECT * FROM v2")
    
    registry.register(v3)
    registry.register(v1)
    registry.register(v2)
    
    ordered = registry.get_ordered_views()
    names = [v.name for v in ordered]
    assert names == ["v1", "v2", "v3"]
    
def test_circular_dependency(registry):
    # v1 -> v2 -> v1
    v1 = VirtualView(name="v1", sql="SELECT * FROM v2")
    v2 = VirtualView(name="v2", sql="SELECT * FROM v1")
    
    registry.register(v1)
    registry.register(v2)
    
    with pytest.raises(ValueError, match="Circular dependency"):
        registry.get_ordered_views()

def test_registry_serialization(registry, tmp_path):
    v1 = VirtualView(name="v1", sql="SELECT 1")
    registry.register(v1)
    
    # YAML
    yaml_str = registry.to_yaml()
    reg2 = VirtualViewRegistry.from_yaml(yaml_str)
    assert "v1" in reg2
    
    # JSON
    json_str = registry.to_json()
    reg3 = VirtualViewRegistry.from_json(json_str)
    assert "v1" in reg3
    
    # File Save/Load
    yaml_path = tmp_path / "views.yaml"
    registry.save(yaml_path, format="yaml")
    reg4 = VirtualViewRegistry.from_file(yaml_path)
    assert "v1" in reg4
    
    # Directory Load
    json_path = tmp_path / "s2_views.json"
    registry.save(json_path, format="json") # Save same registry to another file
    
    reg_combined = VirtualViewRegistry.from_directory(tmp_path)
    assert "v1" in reg_combined # Should load from files

def test_registry_edge_cases(tmp_path):
    registry = VirtualViewRegistry()
    
    # Save unsupported format
    with pytest.raises(ValueError, match="Unsupported format"):
        registry.save(tmp_path / "test.txt", format="txt")
        
    # from_file with unknown extension (hits the try-except logic)
    unknown_file = tmp_path / "test.weird"
    # To hit 297-298, it must FAIL from_yaml and then try from_json
    # YAML parser is very lenient, but "!!corrupt" might fail it for some reason?
    # Actually, from_yaml usually succeeds for a lot of strings.
    # Let's use a string that is DEFINITELY invalid YAML but valid JSON?
    # No, YAML is mostly a superset of JSON.
    # Let's use something that triggers YAMLError.
    unknown_file.write_text('{ "views": [{"name": "j1", "sql": "select 1"}] }', encoding="utf-8")
    # Actually from_yaml handles JSON fine.
    
    # What if it's INVALID YAML but valid JSON?
    # Tab characters are invalid in YAML for indentation.
    unknown_file.write_text('{\n\t"views": [\n\t\t{"name": "j1", "sql": "select 1"}\n\t]\n}', encoding="utf-8")
    
    reg2 = VirtualViewRegistry.from_file(unknown_file)
    assert len(reg2) == 1
    
    # from_directory with a corrupt file (hits the try-except in loop)
    corrupt_file = tmp_path / "corrupt.yaml"
    corrupt_file.write_text("!!corrupt content", encoding="utf-8")
    # This should log warning and continue
    reg3 = VirtualViewRegistry.from_directory(tmp_path)
    # reg3 should still have views from other files in tmp_path (like test.weird if it matched, but it doesn't)
    # wait, test.weird doesn't match glob.
    # We should have a valid yaml too.
    valid_file = tmp_path / "valid.yaml"
    valid_file.write_text("views:\n  - name: valid\n    sql: SELECT 1", encoding="utf-8")
    reg4 = VirtualViewRegistry.from_directory(tmp_path)
    assert "valid" in reg4

