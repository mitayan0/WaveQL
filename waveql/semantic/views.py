"""
Virtual Views for WaveQL

Virtual Views allow you to define reusable SQL views over API data.
Unlike materialized views, these are computed on-demand at query time.

Example:
    ```python
    from waveql.semantic import VirtualView, VirtualViewRegistry
    
    # Define a view
    high_priority = VirtualView(
        name="high_priority_incidents",
        sql="SELECT * FROM incident WHERE priority <= 2",
        description="All P1 and P2 incidents"
    )
    
    # Register it
    registry = VirtualViewRegistry()
    registry.register(high_priority)
    
    # Use it in queries
    conn.register_views(registry)
    cursor.execute("SELECT * FROM high_priority_incidents")
    ```
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import yaml

logger = logging.getLogger(__name__)


@dataclass
class VirtualView:
    """
    A reusable SQL view definition over API or database data.
    
    Virtual Views are logical abstractions that:
    - Are computed on-demand (not stored)
    - Can reference other views (with dependency tracking)
    - Support parameterization via Saved Queries
    
    Attributes:
        name: Unique identifier for this view (used as table name in SQL)
        sql: The SQL query that defines this view
        description: Human-readable description
        schema: Optional schema/namespace prefix (e.g., "analytics")
        dependencies: List of other view names this view depends on
        tags: Optional list of tags for organization
        metadata: Additional custom metadata
    """
    name: str
    sql: str
    description: str = ""
    schema: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Validate name (must be valid SQL identifier)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', self.name):
            raise ValueError(
                f"Invalid view name '{self.name}'. "
                "Must be a valid SQL identifier (letters, numbers, underscores)."
            )
        
        # Auto-detect dependencies from SQL
        if not self.dependencies:
            self.dependencies = self._extract_dependencies()
    
    def _extract_dependencies(self) -> List[str]:
        """Extract referenced table/view names from SQL (basic heuristic)."""
        # Simple regex to find FROM/JOIN clauses
        # This is a heuristic - won't catch all edge cases
        pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, self.sql, re.IGNORECASE)
        # Filter out SQL keywords
        keywords = {'select', 'where', 'and', 'or', 'on', 'as', 'group', 'order', 'limit'}
        return [m for m in matches if m.lower() not in keywords]
    
    @property
    def qualified_name(self) -> str:
        """Return schema-qualified name if schema is set."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize view to dictionary."""
        return {
            "name": self.name,
            "sql": self.sql,
            "description": self.description,
            "schema": self.schema,
            "dependencies": self.dependencies,
            "tags": self.tags,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VirtualView":
        """Create view from dictionary."""
        return cls(
            name=data["name"],
            sql=data["sql"],
            description=data.get("description", ""),
            schema=data.get("schema"),
            dependencies=data.get("dependencies", []),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )


class VirtualViewRegistry:
    """
    Registry for managing Virtual Views.
    
    Provides:
    - View registration and lookup
    - Dependency ordering for view creation
    - YAML/JSON serialization
    - Schema-based namespacing
    
    Example:
        ```python
        registry = VirtualViewRegistry()
        
        # Register views
        registry.register(VirtualView(
            name="active_incidents",
            sql="SELECT * FROM incident WHERE active = true"
        ))
        registry.register(VirtualView(
            name="critical_active",
            sql="SELECT * FROM active_incidents WHERE priority = 1",
            dependencies=["active_incidents"]
        ))
        
        # Get views in dependency order
        for view in registry.get_ordered_views():
            print(f"Creating: {view.name}")
        ```
    """
    
    def __init__(self):
        self._views: Dict[str, VirtualView] = {}
    
    def register(self, view: VirtualView) -> None:
        """Register a virtual view."""
        if view.name in self._views:
            logger.warning("Overwriting existing view: %s", view.name)
        self._views[view.name] = view
        logger.debug("Registered virtual view: %s", view.name)
    
    def unregister(self, name: str) -> bool:
        """Remove a view from the registry. Returns True if removed."""
        if name in self._views:
            del self._views[name]
            logger.debug("Unregistered virtual view: %s", name)
            return True
        return False
    
    def get(self, name: str) -> Optional[VirtualView]:
        """Get a view by name."""
        return self._views.get(name)
    
    def __getitem__(self, name: str) -> VirtualView:
        """Get a view by name (raises KeyError if not found)."""
        return self._views[name]
    
    def __contains__(self, name: str) -> bool:
        """Check if a view exists."""
        return name in self._views
    
    def __len__(self) -> int:
        """Return number of registered views."""
        return len(self._views)
    
    def __iter__(self):
        """Iterate over view names."""
        return iter(self._views)
    
    def list_views(self, schema: Optional[str] = None, tag: Optional[str] = None) -> List[VirtualView]:
        """
        List views, optionally filtered by schema or tag.
        
        Args:
            schema: Filter by schema name
            tag: Filter by tag
            
        Returns:
            List of matching views
        """
        views = list(self._views.values())
        
        if schema:
            views = [v for v in views if v.schema == schema]
        
        if tag:
            views = [v for v in views if tag in v.tags]
        
        return views
    
    def get_ordered_views(self) -> List[VirtualView]:
        """
        Return views in dependency order (topological sort).
        
        Views with no dependencies come first, then views that depend on them, etc.
        Raises ValueError if circular dependencies are detected.
        """
        # Build dependency graph
        in_degree = {name: 0 for name in self._views}
        dependents = {name: [] for name in self._views}
        
        for name, view in self._views.items():
            for dep in view.dependencies:
                if dep in self._views:
                    in_degree[name] += 1
                    dependents[dep].append(name)
        
        # Kahn's algorithm for topological sort
        queue = [name for name, degree in in_degree.items() if degree == 0]
        ordered = []
        
        while queue:
            current = queue.pop(0)
            ordered.append(self._views[current])
            
            for dependent in dependents[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        if len(ordered) != len(self._views):
            # Circular dependency detected
            remaining = set(self._views.keys()) - {v.name for v in ordered}
            raise ValueError(f"Circular dependency detected among views: {remaining}")
        
        return ordered
    
    def to_yaml(self) -> str:
        """Serialize registry to YAML."""
        data = {
            "version": "1.0",
            "views": [v.to_dict() for v in self._views.values()]
        }
        return yaml.dump(data, default_flow_style=False, sort_keys=False)
    
    def to_json(self) -> str:
        """Serialize registry to JSON."""
        data = {
            "version": "1.0",
            "views": [v.to_dict() for v in self._views.values()]
        }
        return json.dumps(data, indent=2)
    
    @classmethod
    def from_yaml(cls, yaml_content: str) -> "VirtualViewRegistry":
        """Load registry from YAML content."""
        data = yaml.safe_load(yaml_content)
        return cls._from_data(data)
    
    @classmethod
    def from_json(cls, json_content: str) -> "VirtualViewRegistry":
        """Load registry from JSON content."""
        data = json.loads(json_content)
        return cls._from_data(data)
    
    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "VirtualViewRegistry":
        """
        Load registry from a YAML or JSON file.
        
        File format is detected from extension (.yaml, .yml, .json).
        """
        path = Path(path)
        content = path.read_text(encoding="utf-8")
        
        if path.suffix.lower() in (".yaml", ".yml"):
            return cls.from_yaml(content)
        elif path.suffix.lower() == ".json":
            return cls.from_json(content)
        else:
            # Try YAML first, then JSON
            try:
                return cls.from_yaml(content)
            except yaml.YAMLError:
                return cls.from_json(content)
    
    @classmethod
    def from_directory(cls, directory: Union[str, Path]) -> "VirtualViewRegistry":
        """
        Load all view definitions from a directory.
        
        Scans for .yaml, .yml, and .json files containing view definitions.
        """
        directory = Path(directory)
        registry = cls()
        
        for pattern in ("*.yaml", "*.yml", "*.json"):
            for file_path in directory.glob(pattern):
                try:
                    file_registry = cls.from_file(file_path)
                    for view in file_registry._views.values():
                        registry.register(view)
                    logger.info("Loaded views from %s", file_path)
                except Exception as e:
                    logger.warning("Failed to load views from %s: %s", file_path, e)
        
        return registry
    
    @classmethod
    def _from_data(cls, data: Dict[str, Any]) -> "VirtualViewRegistry":
        """Create registry from parsed data."""
        registry = cls()
        
        views = data.get("views", [])
        for view_data in views:
            view = VirtualView.from_dict(view_data)
            registry.register(view)
        
        return registry
    
    def save(self, path: Union[str, Path], format: str = "yaml") -> None:
        """
        Save registry to a file.
        
        Args:
            path: File path to save to
            format: "yaml" or "json"
        """
        path = Path(path)
        
        if format == "yaml":
            content = self.to_yaml()
        elif format == "json":
            content = self.to_json()
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        path.write_text(content, encoding="utf-8")
        logger.info("Saved %d views to %s", len(self), path)
