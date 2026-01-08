"""
Saved Queries for WaveQL

Saved Queries are parameterized SQL templates that can be reused
with different parameter values. They support:
- Named parameters with type hints
- Default values
- Validation
- Documentation generation

Example:
    ```python
    from waveql.semantic import SavedQuery, SavedQueryRegistry
    
    # Define a parameterized query
    incidents_by_priority = SavedQuery(
        name="incidents_by_priority",
        sql="SELECT * FROM incident WHERE priority <= :max_priority AND state = :state",
        parameters={
            "max_priority": {"type": "int", "default": 2, "description": "Maximum priority (1-5)"},
            "state": {"type": "str", "description": "Incident state filter"}
        },
        description="Get incidents filtered by priority and state"
    )
    
    # Execute with parameters
    cursor.execute_saved(incidents_by_priority, max_priority=1, state="active")
    ```
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Literal

import yaml

logger = logging.getLogger(__name__)


# Supported parameter types
ParameterType = Literal["str", "int", "float", "bool", "list", "date", "datetime"]


@dataclass
class QueryParameter:
    """
    Definition of a query parameter.
    
    Attributes:
        name: Parameter name (used in SQL as :name)
        type: Data type for validation
        description: Human-readable description
        default: Default value if not provided
        required: Whether the parameter must be provided
        choices: Optional list of allowed values
    """
    name: str
    type: ParameterType = "str"
    description: str = ""
    default: Any = None
    required: bool = True
    choices: Optional[List[Any]] = None
    
    def validate(self, value: Any) -> Any:
        """
        Validate and coerce a value to the expected type.
        
        Args:
            value: The value to validate
            
        Returns:
            The validated/coerced value
            
        Raises:
            ValueError: If validation fails
        """
        if value is None:
            if self.required and self.default is None:
                raise ValueError(f"Parameter '{self.name}' is required")
            return self.default
        
        # Type coercion
        try:
            if self.type == "str":
                value = str(value)
            elif self.type == "int":
                value = int(value)
            elif self.type == "float":
                value = float(value)
            elif self.type == "bool":
                if isinstance(value, str):
                    value = value.lower() in ("true", "1", "yes")
                else:
                    value = bool(value)
            elif self.type == "list":
                if not isinstance(value, (list, tuple)):
                    value = [value]
            elif self.type in ("date", "datetime"):
                # Accept as-is for date types (let SQL handle it)
                value = str(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Parameter '{self.name}' must be of type {self.type}: {e}"
            )
        
        # Choices validation
        if self.choices and value not in self.choices:
            raise ValueError(
                f"Parameter '{self.name}' must be one of {self.choices}, got: {value}"
            )
        
        return value
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        result = {
            "name": self.name,
            "type": self.type,
        }
        if self.description:
            result["description"] = self.description
        if self.default is not None:
            result["default"] = self.default
        if not self.required:
            result["required"] = False
        if self.choices:
            result["choices"] = self.choices
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueryParameter":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            type=data.get("type", "str"),
            description=data.get("description", ""),
            default=data.get("default"),
            required=data.get("required", True),
            choices=data.get("choices"),
        )


@dataclass
class SavedQuery:
    """
    A parameterized SQL query template.
    
    Saved Queries provide:
    - Named parameters with :param_name syntax
    - Type validation and coercion
    - Default values
    - Self-documenting parameter definitions
    
    Attributes:
        name: Unique identifier for this query
        sql: SQL template with :parameter placeholders
        parameters: Dictionary of parameter definitions
        description: Human-readable description
        tags: Optional list of tags for organization
        metadata: Additional custom metadata
    """
    name: str
    sql: str
    parameters: Dict[str, Union[QueryParameter, Dict[str, Any]]] = field(default_factory=dict)
    description: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Validate name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', self.name):
            raise ValueError(
                f"Invalid query name '{self.name}'. "
                "Must be a valid identifier (letters, numbers, underscores)."
            )
        
        # Convert dict parameters to QueryParameter objects
        normalized = {}
        for key, value in self.parameters.items():
            if isinstance(value, QueryParameter):
                normalized[key] = value
            elif isinstance(value, dict):
                normalized[key] = QueryParameter(name=key, **value)
            else:
                raise ValueError(f"Invalid parameter definition for '{key}'")
        self.parameters = normalized
        
        # Validate that all parameters in SQL are defined
        sql_params = self._extract_parameters()
        for param in sql_params:
            if param not in self.parameters:
                # Auto-create parameter definition
                self.parameters[param] = QueryParameter(
                    name=param,
                    type="str",
                    required=True
                )
                logger.debug("Auto-created parameter definition for: %s", param)
    
    def _extract_parameters(self) -> List[str]:
        """Extract parameter names from SQL template."""
        # Match :param_name but not ::cast or string literals
        pattern = r'(?<![:\'])(?::([a-zA-Z_][a-zA-Z0-9_]*))'
        return list(set(re.findall(pattern, self.sql)))
    
    def render(self, **params) -> str:
        """
        Render the SQL template with provided parameters.
        
        Args:
            **params: Parameter values
            
        Returns:
            Rendered SQL string with parameters substituted
            
        Raises:
            ValueError: If required parameters are missing or validation fails
        """
        # Validate and collect parameter values
        validated = {}
        for name, param_def in self.parameters.items():
            value = params.get(name)
            validated[name] = param_def.validate(value)
        
        # Check for extra parameters
        extra = set(params.keys()) - set(self.parameters.keys())
        if extra:
            logger.warning("Ignoring unknown parameters: %s", extra)
        
        # Render SQL with parameter substitution
        rendered = self.sql
        for name, value in validated.items():
            # Convert value to SQL literal
            sql_value = self._to_sql_literal(value)
            rendered = re.sub(
                rf'(?<![:\']):{name}\b',
                sql_value,
                rendered
            )
        
        return rendered
    
    def _to_sql_literal(self, value: Any) -> str:
        """Convert a Python value to SQL literal."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, (list, tuple)):
            items = ", ".join(self._to_sql_literal(v) for v in value)
            return f"({items})"
        else:
            # String - escape single quotes
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
    
    def get_parameter_docs(self) -> str:
        """Generate documentation for parameters."""
        lines = [f"# {self.name}", ""]
        if self.description:
            lines.extend([self.description, ""])
        
        lines.append("## Parameters")
        lines.append("")
        
        for param in self.parameters.values():
            required = "*required*" if param.required else "*optional*"
            default = f", default: `{param.default}`" if param.default is not None else ""
            choices = f", choices: {param.choices}" if param.choices else ""
            
            lines.append(f"- **{param.name}** ({param.type}, {required}{default}{choices})")
            if param.description:
                lines.append(f"  {param.description}")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "name": self.name,
            "sql": self.sql,
            "parameters": {k: v.to_dict() for k, v in self.parameters.items()},
            "description": self.description,
            "tags": self.tags,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SavedQuery":
        """Create from dictionary."""
        params = {}
        for name, pdata in data.get("parameters", {}).items():
            if isinstance(pdata, dict):
                pdata["name"] = name
                params[name] = QueryParameter.from_dict(pdata)
            else:
                params[name] = pdata
        
        return cls(
            name=data["name"],
            sql=data["sql"],
            parameters=params,
            description=data.get("description", ""),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )


class SavedQueryRegistry:
    """
    Registry for managing Saved Queries.
    
    Provides:
    - Query registration and lookup
    - Execution with parameter binding
    - YAML/JSON serialization
    
    Example:
        ```python
        registry = SavedQueryRegistry()
        
        registry.register(SavedQuery(
            name="open_tickets",
            sql="SELECT * FROM tickets WHERE status = :status",
            parameters={"status": {"type": "str", "default": "open"}}
        ))
        
        # Get rendered SQL
        sql = registry.render("open_tickets", status="pending")
        ```
    """
    
    def __init__(self):
        self._queries: Dict[str, SavedQuery] = {}
    
    def register(self, query: SavedQuery) -> None:
        """Register a saved query."""
        if query.name in self._queries:
            logger.warning("Overwriting existing query: %s", query.name)
        self._queries[query.name] = query
        logger.debug("Registered saved query: %s", query.name)
    
    def unregister(self, name: str) -> bool:
        """Remove a query from the registry. Returns True if removed."""
        if name in self._queries:
            del self._queries[name]
            return True
        return False
    
    def get(self, name: str) -> Optional[SavedQuery]:
        """Get a query by name."""
        return self._queries.get(name)
    
    def __getitem__(self, name: str) -> SavedQuery:
        """Get a query by name (raises KeyError if not found)."""
        return self._queries[name]
    
    def __contains__(self, name: str) -> bool:
        """Check if a query exists."""
        return name in self._queries
    
    def __len__(self) -> int:
        """Return number of registered queries."""
        return len(self._queries)
    
    def __iter__(self):
        """Iterate over query names."""
        return iter(self._queries)
    
    def list_queries(self, tag: Optional[str] = None) -> List[SavedQuery]:
        """List queries, optionally filtered by tag."""
        queries = list(self._queries.values())
        if tag:
            queries = [q for q in queries if tag in q.tags]
        return queries
    
    def render(self, name: str, **params) -> str:
        """
        Render a saved query with parameters.
        
        Args:
            name: Query name
            **params: Parameter values
            
        Returns:
            Rendered SQL string
        """
        query = self._queries.get(name)
        if not query:
            raise KeyError(f"Query not found: {name}")
        return query.render(**params)
    
    def to_yaml(self) -> str:
        """Serialize registry to YAML."""
        data = {
            "version": "1.0",
            "queries": [q.to_dict() for q in self._queries.values()]
        }
        return yaml.dump(data, default_flow_style=False, sort_keys=False)
    
    def to_json(self) -> str:
        """Serialize registry to JSON."""
        data = {
            "version": "1.0",
            "queries": [q.to_dict() for q in self._queries.values()]
        }
        return json.dumps(data, indent=2)
    
    @classmethod
    def from_yaml(cls, yaml_content: str) -> "SavedQueryRegistry":
        """Load registry from YAML content."""
        data = yaml.safe_load(yaml_content)
        return cls._from_data(data)
    
    @classmethod
    def from_json(cls, json_content: str) -> "SavedQueryRegistry":
        """Load registry from JSON content."""
        data = json.loads(json_content)
        return cls._from_data(data)
    
    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "SavedQueryRegistry":
        """Load registry from a YAML or JSON file."""
        path = Path(path)
        content = path.read_text(encoding="utf-8")
        
        if path.suffix.lower() in (".yaml", ".yml"):
            return cls.from_yaml(content)
        elif path.suffix.lower() == ".json":
            return cls.from_json(content)
        else:
            try:
                return cls.from_yaml(content)
            except yaml.YAMLError:
                return cls.from_json(content)
    
    @classmethod
    def from_directory(cls, directory: Union[str, Path]) -> "SavedQueryRegistry":
        """Load all query definitions from a directory."""
        directory = Path(directory)
        registry = cls()
        
        for pattern in ("*.yaml", "*.yml", "*.json"):
            for file_path in directory.glob(pattern):
                try:
                    file_registry = cls.from_file(file_path)
                    for query in file_registry._queries.values():
                        registry.register(query)
                    logger.info("Loaded queries from %s", file_path)
                except Exception as e:
                    logger.warning("Failed to load queries from %s: %s", file_path, e)
        
        return registry
    
    @classmethod
    def _from_data(cls, data: Dict[str, Any]) -> "SavedQueryRegistry":
        """Create registry from parsed data."""
        registry = cls()
        
        queries = data.get("queries", [])
        for query_data in queries:
            query = SavedQuery.from_dict(query_data)
            registry.register(query)
        
        return registry
    
    def save(self, path: Union[str, Path], format: str = "yaml") -> None:
        """Save registry to a file."""
        path = Path(path)
        
        if format == "yaml":
            content = self.to_yaml()
        elif format == "json":
            content = self.to_json()
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        path.write_text(content, encoding="utf-8")
        logger.info("Saved %d queries to %s", len(self), path)
    
    def generate_docs(self) -> str:
        """Generate documentation for all queries."""
        lines = ["# Saved Queries Reference", ""]
        
        for query in sorted(self._queries.values(), key=lambda q: q.name):
            lines.append(query.get_parameter_docs())
            lines.append("")
            lines.append("---")
            lines.append("")
        
        return "\n".join(lines)
