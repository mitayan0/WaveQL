"""
Tests for WaveQL Semantic Layer

Tests for:
- Virtual Views
- Saved Queries
- dbt Integration
"""

import pytest
import json
import tempfile

from waveql.semantic.views import VirtualView, VirtualViewRegistry
from waveql.semantic.saved_queries import SavedQuery, SavedQueryRegistry, QueryParameter
from waveql.semantic.dbt import DbtManifest


class TestVirtualView:
    """Tests for VirtualView dataclass."""
    
    def test_create_simple_view(self):
        """Test creating a basic virtual view."""
        view = VirtualView(
            name="active_users",
            sql="SELECT * FROM users WHERE active = true"
        )
        
        assert view.name == "active_users"
        assert "active = true" in view.sql
        assert view.description == ""
    
    def test_view_with_metadata(self):
        """Test view with full metadata."""
        view = VirtualView(
            name="priority_incidents",
            sql="SELECT * FROM incident WHERE priority <= 2",
            description="High priority incidents",
            schema="analytics",
            tags=["sla", "monitoring"],
            metadata={"owner": "ops-team"}
        )
        
        assert view.schema == "analytics"
        assert view.qualified_name == "analytics.priority_incidents"
        assert "sla" in view.tags
        assert view.metadata["owner"] == "ops-team"
    
    def test_invalid_view_name(self):
        """Test that invalid view names are rejected."""
        with pytest.raises(ValueError, match="Invalid view name"):
            VirtualView(name="123invalid", sql="SELECT 1")
        
        with pytest.raises(ValueError, match="Invalid view name"):
            VirtualView(name="has-dash", sql="SELECT 1")
    
    def test_dependency_extraction(self):
        """Test automatic dependency extraction from SQL."""
        view = VirtualView(
            name="joined_data",
            sql="SELECT * FROM users u JOIN orders o ON u.id = o.user_id"
        )
        
        assert "users" in view.dependencies
        assert "orders" in view.dependencies
    
    def test_serialization(self):
        """Test view serialization to dict."""
        view = VirtualView(
            name="test_view",
            sql="SELECT 1",
            description="Test",
            tags=["test"]
        )
        
        data = view.to_dict()
        restored = VirtualView.from_dict(data)
        
        assert restored.name == view.name
        assert restored.sql == view.sql
        assert restored.tags == view.tags


class TestVirtualViewRegistry:
    """Tests for VirtualViewRegistry."""
    
    def test_register_and_get(self):
        """Test registering and retrieving views."""
        registry = VirtualViewRegistry()
        
        view = VirtualView(name="test", sql="SELECT 1")
        registry.register(view)
        
        assert "test" in registry
        assert registry.get("test") == view
        assert registry["test"] == view
    
    def test_dependency_ordering(self):
        """Test topological sort for dependencies."""
        registry = VirtualViewRegistry()
        
        # Register in wrong order
        registry.register(VirtualView(
            name="level2",
            sql="SELECT * FROM level1",
            dependencies=["level1"]
        ))
        registry.register(VirtualView(
            name="level1",
            sql="SELECT * FROM raw",
            dependencies=[]
        ))
        registry.register(VirtualView(
            name="level3",
            sql="SELECT * FROM level2",
            dependencies=["level2"]
        ))
        
        ordered = registry.get_ordered_views()
        names = [v.name for v in ordered]
        
        # level1 must come before level2, level2 before level3
        assert names.index("level1") < names.index("level2")
        assert names.index("level2") < names.index("level3")
    
    def test_circular_dependency_detection(self):
        """Test that circular dependencies raise an error."""
        registry = VirtualViewRegistry()
        
        registry.register(VirtualView(
            name="a",
            sql="SELECT * FROM b",
            dependencies=["b"]
        ))
        registry.register(VirtualView(
            name="b",
            sql="SELECT * FROM a",
            dependencies=["a"]
        ))
        
        with pytest.raises(ValueError, match="Circular dependency"):
            registry.get_ordered_views()
    
    def test_yaml_serialization(self):
        """Test YAML export and import."""
        registry = VirtualViewRegistry()
        registry.register(VirtualView(name="v1", sql="SELECT 1"))
        registry.register(VirtualView(name="v2", sql="SELECT 2"))
        
        yaml_content = registry.to_yaml()
        restored = VirtualViewRegistry.from_yaml(yaml_content)
        
        assert len(restored) == 2
        assert "v1" in restored
        assert "v2" in restored
    
    def test_file_load(self):
        """Test loading from file."""
        import os
        fd, temp_path = tempfile.mkstemp(suffix='.yaml')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("""
version: "1.0"
views:
  - name: test_view
    sql: "SELECT * FROM test"
    description: "Test view"
""")
            
            registry = VirtualViewRegistry.from_file(temp_path)
            assert "test_view" in registry
        finally:
            os.unlink(temp_path)


class TestSavedQuery:
    """Tests for SavedQuery."""
    
    def test_simple_query(self):
        """Test creating a simple parameterized query."""
        query = SavedQuery(
            name="get_user",
            sql="SELECT * FROM users WHERE id = :user_id",
            parameters={
                "user_id": {"type": "int", "description": "User ID"}
            }
        )
        
        rendered = query.render(user_id=42)
        assert "id = 42" in rendered
    
    def test_string_parameter(self):
        """Test string parameter escaping."""
        query = SavedQuery(
            name="search",
            sql="SELECT * FROM items WHERE name = :name",
            parameters={"name": {"type": "str"}}
        )
        
        rendered = query.render(name="O'Brien")
        assert "O''Brien" in rendered  # Escaped
    
    def test_default_value(self):
        """Test parameter with default value."""
        query = SavedQuery(
            name="incidents",
            sql="SELECT * FROM incident WHERE priority <= :max_priority",
            parameters={
                "max_priority": {"type": "int", "default": 2}
            }
        )
        
        # Using default
        rendered = query.render()
        assert "priority <= 2" in rendered
        
        # Override default
        rendered = query.render(max_priority=1)
        assert "priority <= 1" in rendered
    
    def test_list_parameter(self):
        """Test list parameter (IN clause)."""
        query = SavedQuery(
            name="multi_status",
            sql="SELECT * FROM tickets WHERE status IN :statuses",
            parameters={
                "statuses": {"type": "list"}
            }
        )
        
        rendered = query.render(statuses=["open", "pending"])
        assert "IN ('open', 'pending')" in rendered
    
    def test_required_parameter_validation(self):
        """Test that missing required parameters raise errors."""
        query = SavedQuery(
            name="test",
            sql="SELECT * FROM t WHERE x = :required_param",
            parameters={
                "required_param": {"type": "int", "required": True}
            }
        )
        
        with pytest.raises(ValueError, match="required"):
            query.render()
    
    def test_choices_validation(self):
        """Test parameter choices validation."""
        query = SavedQuery(
            name="by_status",
            sql="SELECT * FROM t WHERE status = :status",
            parameters={
                "status": {"type": "str", "choices": ["open", "closed"]}
            }
        )
        
        # Valid choice
        rendered = query.render(status="open")
        assert "status = 'open'" in rendered
        
        # Invalid choice
        with pytest.raises(ValueError, match="must be one of"):
            query.render(status="invalid")
    
    def test_documentation_generation(self):
        """Test parameter documentation generation."""
        query = SavedQuery(
            name="complex_query",
            sql="SELECT * FROM t WHERE a = :param1 AND b = :param2",
            description="A complex query",
            parameters={
                "param1": {"type": "int", "description": "First param"},
                "param2": {"type": "str", "default": "test", "description": "Second param"}
            }
        )
        
        docs = query.get_parameter_docs()
        assert "complex_query" in docs
        assert "First param" in docs
        assert "Second param" in docs


class TestSavedQueryRegistry:
    """Tests for SavedQueryRegistry."""
    
    def test_register_and_render(self):
        """Test registering and rendering queries."""
        registry = SavedQueryRegistry()
        
        query = SavedQuery(
            name="simple",
            sql="SELECT :value",
            parameters={"value": {"type": "int"}}
        )
        registry.register(query)
        
        rendered = registry.render("simple", value=42)
        assert "SELECT 42" in rendered
    
    def test_yaml_serialization(self):
        """Test YAML export and import."""
        registry = SavedQueryRegistry()
        registry.register(SavedQuery(
            name="q1",
            sql="SELECT :x",
            parameters={"x": {"type": "int"}}
        ))
        
        yaml_content = registry.to_yaml()
        restored = SavedQueryRegistry.from_yaml(yaml_content)
        
        assert "q1" in restored
        assert restored.render("q1", x=1) == "SELECT 1"


class TestDbtIntegration:
    """Tests for dbt manifest parsing."""
    
    @pytest.fixture
    def sample_manifest(self):
        """Create a sample dbt manifest."""
        return {
            "metadata": {
                "dbt_version": "1.7.0",
                "project_name": "test_project"
            },
            "nodes": {
                "model.test_project.stg_users": {
                    "name": "stg_users",
                    "resource_type": "model",
                    "schema": "staging",
                    "database": "warehouse",
                    "description": "Staged users",
                    "compiled_sql": "SELECT id, name FROM raw.users",
                    "columns": {
                        "id": {"description": "User ID"},
                        "name": {"description": "User name"}
                    },
                    "depends_on": {"nodes": []},
                    "tags": ["staging"],
                    "meta": {},
                    "config": {"materialized": "view"},
                    "package_name": "test_project"
                },
                "model.test_project.dim_users": {
                    "name": "dim_users",
                    "resource_type": "model",
                    "schema": "marts",
                    "database": "warehouse",
                    "description": "User dimension",
                    "compiled_sql": "SELECT * FROM staging.stg_users",
                    "columns": {},
                    "depends_on": {"nodes": ["model.test_project.stg_users"]},
                    "tags": ["marts"],
                    "meta": {},
                    "config": {"materialized": "table"},
                    "package_name": "test_project"
                }
            },
            "sources": {
                "source.test_project.raw.users": {
                    "name": "users",
                    "source_name": "raw",
                    "schema": "raw",
                    "description": "Raw users table",
                    "columns": {},
                    "meta": {}
                }
            }
        }
    
    def test_parse_manifest(self, sample_manifest):
        """Test parsing a dbt manifest."""
        manifest = DbtManifest(sample_manifest)
        
        assert manifest.project_name == "test_project"
        assert manifest.dbt_version == "1.7.0"
        assert len(manifest.models) == 2
        assert len(manifest.sources) == 1
    
    def test_get_model(self, sample_manifest):
        """Test getting a specific model."""
        manifest = DbtManifest(sample_manifest)
        
        model = manifest.get_model("stg_users")
        assert model is not None
        assert model.schema == "staging"
        assert model.materialized == "view"
        assert len(model.columns) == 2
    
    def test_model_to_virtual_view(self, sample_manifest):
        """Test converting a dbt model to VirtualView."""
        manifest = DbtManifest(sample_manifest)
        model = manifest.get_model("stg_users")
        
        view = model.to_virtual_view()
        
        assert view.name == "stg_users"
        assert "SELECT id, name" in view.sql
        assert view.description == "Staged users"
    
    def test_to_view_registry(self, sample_manifest):
        """Test converting manifest to VirtualViewRegistry."""
        manifest = DbtManifest(sample_manifest)
        registry = manifest.to_view_registry()
        
        assert len(registry) == 2
        assert "stg_users" in registry
        assert "dim_users" in registry
    
    def test_model_lineage(self, sample_manifest):
        """Test model lineage extraction."""
        manifest = DbtManifest(sample_manifest)
        
        lineage = manifest.get_model_lineage("dim_users")
        assert "stg_users" in lineage["upstream"]
        
        lineage = manifest.get_model_lineage("stg_users")
        assert "dim_users" in lineage["downstream"]
    
    def test_filter_by_tag(self, sample_manifest):
        """Test filtering models by tag."""
        manifest = DbtManifest(sample_manifest)
        
        staging = manifest.list_models(tag="staging")
        assert len(staging) == 1
        assert staging[0].name == "stg_users"
    
    def test_load_from_file(self, sample_manifest):
        """Test loading manifest from file."""
        import os
        fd, temp_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(sample_manifest, f)
            
            manifest = DbtManifest.from_file(temp_path)
            assert len(manifest.models) == 2
        finally:
            os.unlink(temp_path)


class TestQueryParameter:
    """Tests for QueryParameter validation."""
    
    def test_type_coercion(self):
        """Test type coercion for different types."""
        # int
        param = QueryParameter(name="x", type="int")
        assert param.validate("42") == 42
        
        # float
        param = QueryParameter(name="x", type="float")
        assert param.validate("3.14") == 3.14
        
        # bool
        param = QueryParameter(name="x", type="bool")
        assert param.validate("true") is True
        assert param.validate("false") is False
    
    def test_invalid_type_coercion(self):
        """Test error on invalid type coercion."""
        param = QueryParameter(name="x", type="int")
        
        with pytest.raises(ValueError, match="must be of type"):
            param.validate("not-a-number")
