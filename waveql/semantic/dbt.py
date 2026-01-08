"""
dbt Integration for WaveQL

Provides first-class support for reading dbt manifest.json to expose
dbt models as queryable tables in WaveQL.

This allows you to:
- Query dbt models directly using WaveQL
- Use dbt's compiled SQL as Virtual Views
- Access dbt metadata (descriptions, tags, columns)

Example:
    ```python
    from waveql.semantic import DbtManifest
    
    # Load dbt manifest
    manifest = DbtManifest.from_file("target/manifest.json")
    
    # List all models
    for model in manifest.models:
        print(f"{model.name}: {model.description}")
    
    # Register models as virtual views
    conn.register_dbt_models(manifest)
    
    # Query dbt models
    cursor.execute("SELECT * FROM stg_customers")
    ```
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from waveql.semantic.views import VirtualView, VirtualViewRegistry

logger = logging.getLogger(__name__)


@dataclass
class DbtColumn:
    """
    Represents a column from a dbt model.
    
    Attributes:
        name: Column name
        description: Column description from dbt
        data_type: Column data type (if specified)
        meta: Additional column metadata
        tags: Column-level tags
    """
    name: str
    description: str = ""
    data_type: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    @classmethod
    def from_manifest(cls, name: str, data: Dict[str, Any]) -> "DbtColumn":
        """Create from dbt manifest column data."""
        return cls(
            name=name,
            description=data.get("description", ""),
            data_type=data.get("data_type"),
            meta=data.get("meta", {}),
            tags=data.get("tags", []),
        )


@dataclass
class DbtModel:
    """
    Represents a dbt model with its metadata and compiled SQL.
    
    Attributes:
        name: Model name (e.g., "stg_customers")
        unique_id: Full dbt unique ID (e.g., "model.my_project.stg_customers")
        schema: Database schema
        database: Target database
        description: Model description from dbt
        compiled_sql: The compiled SQL query
        raw_sql: The raw SQL query (with Jinja)
        columns: List of column definitions
        depends_on: List of upstream model/source dependencies
        tags: Model-level tags
        meta: Additional model metadata
        materialized: Materialization type (table, view, incremental, ephemeral)
        package_name: dbt package name
    """
    name: str
    unique_id: str
    schema: str = ""
    database: str = ""
    description: str = ""
    compiled_sql: str = ""
    raw_sql: str = ""
    columns: List[DbtColumn] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    materialized: str = "view"
    package_name: str = ""
    
    @property
    def qualified_name(self) -> str:
        """Return schema-qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name
    
    @property
    def full_qualified_name(self) -> str:
        """Return fully qualified name including database."""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.name)
        return ".".join(parts)
    
    def to_virtual_view(self) -> VirtualView:
        """
        Convert this dbt model to a WaveQL Virtual View.
        
        Uses the compiled SQL as the view definition.
        """
        if not self.compiled_sql:
            raise ValueError(f"Model '{self.name}' has no compiled SQL")
        
        # Extract dependencies (other models this one depends on)
        dependencies = [
            dep.split(".")[-1]  # Get just the model name
            for dep in self.depends_on
            if dep.startswith("model.")
        ]
        
        return VirtualView(
            name=self.name,
            sql=self.compiled_sql,
            description=self.description,
            schema=self.schema or None,
            dependencies=dependencies,
            tags=self.tags,
            metadata={
                "dbt_unique_id": self.unique_id,
                "materialized": self.materialized,
                "package": self.package_name,
            }
        )
    
    @classmethod
    def from_manifest(cls, unique_id: str, data: Dict[str, Any]) -> "DbtModel":
        """Create from dbt manifest node data."""
        # Parse columns
        columns = []
        for col_name, col_data in data.get("columns", {}).items():
            columns.append(DbtColumn.from_manifest(col_name, col_data))
        
        # Parse dependencies
        depends_on = data.get("depends_on", {}).get("nodes", [])
        
        return cls(
            name=data.get("name", ""),
            unique_id=unique_id,
            schema=data.get("schema", ""),
            database=data.get("database", ""),
            description=data.get("description", ""),
            compiled_sql=data.get("compiled_sql", data.get("compiled_code", "")),
            raw_sql=data.get("raw_sql", data.get("raw_code", "")),
            columns=columns,
            depends_on=depends_on,
            tags=data.get("tags", []),
            meta=data.get("meta", {}),
            materialized=data.get("config", {}).get("materialized", "view"),
            package_name=data.get("package_name", ""),
        )


@dataclass
class DbtSource:
    """
    Represents a dbt source definition.
    
    Sources are the raw tables that dbt reads from.
    """
    name: str
    source_name: str
    unique_id: str
    schema: str = ""
    database: str = ""
    description: str = ""
    columns: List[DbtColumn] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def qualified_name(self) -> str:
        """Return schema-qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name
    
    @classmethod
    def from_manifest(cls, unique_id: str, data: Dict[str, Any]) -> "DbtSource":
        """Create from dbt manifest source data."""
        columns = []
        for col_name, col_data in data.get("columns", {}).items():
            columns.append(DbtColumn.from_manifest(col_name, col_data))
        
        return cls(
            name=data.get("name", ""),
            source_name=data.get("source_name", ""),
            unique_id=unique_id,
            schema=data.get("schema", ""),
            database=data.get("database", ""),
            description=data.get("description", ""),
            columns=columns,
            meta=data.get("meta", {}),
        )


class DbtManifest:
    """
    Parser and container for dbt manifest.json files.
    
    The manifest contains all the metadata about a dbt project including
    models, sources, seeds, and their compiled SQL.
    
    Example:
        ```python
        # Load manifest
        manifest = DbtManifest.from_file("target/manifest.json")
        
        # Access models
        for model in manifest.models:
            print(f"{model.name}: {model.materialized}")
        
        # Get specific model
        customers = manifest.get_model("stg_customers")
        print(customers.compiled_sql)
        
        # Convert to Virtual Views
        registry = manifest.to_view_registry()
        ```
    """
    
    def __init__(self, data: Dict[str, Any]):
        """
        Initialize from parsed manifest data.
        
        Args:
            data: Parsed manifest.json content
        """
        self._data = data
        self._models: Dict[str, DbtModel] = {}
        self._sources: Dict[str, DbtSource] = {}
        
        self._parse()
    
    def _parse(self) -> None:
        """Parse the manifest data into model and source objects."""
        nodes = self._data.get("nodes", {})
        sources = self._data.get("sources", {})
        
        # Parse models
        for unique_id, node_data in nodes.items():
            if node_data.get("resource_type") == "model":
                model = DbtModel.from_manifest(unique_id, node_data)
                self._models[model.name] = model
        
        # Parse sources
        for unique_id, source_data in sources.items():
            source = DbtSource.from_manifest(unique_id, source_data)
            key = f"{source.source_name}.{source.name}"
            self._sources[key] = source
        
        logger.info(
            "Parsed dbt manifest: %d models, %d sources",
            len(self._models), len(self._sources)
        )
    
    @property
    def models(self) -> List[DbtModel]:
        """Return all models."""
        return list(self._models.values())
    
    @property
    def sources(self) -> List[DbtSource]:
        """Return all sources."""
        return list(self._sources.values())
    
    @property
    def metadata(self) -> Dict[str, Any]:
        """Return manifest metadata."""
        return self._data.get("metadata", {})
    
    @property
    def dbt_version(self) -> str:
        """Return dbt version used to generate the manifest."""
        return self.metadata.get("dbt_version", "unknown")
    
    @property
    def project_name(self) -> str:
        """Return the dbt project name."""
        return self.metadata.get("project_name", "")
    
    def get_model(self, name: str) -> Optional[DbtModel]:
        """Get a model by name."""
        return self._models.get(name)
    
    def get_source(self, source_name: str, table_name: str) -> Optional[DbtSource]:
        """Get a source by source name and table name."""
        key = f"{source_name}.{table_name}"
        return self._sources.get(key)
    
    def list_models(
        self,
        tag: Optional[str] = None,
        materialized: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[DbtModel]:
        """
        List models with optional filters.
        
        Args:
            tag: Filter by tag
            materialized: Filter by materialization type
            schema: Filter by schema
        """
        models = list(self._models.values())
        
        if tag:
            models = [m for m in models if tag in m.tags]
        
        if materialized:
            models = [m for m in models if m.materialized == materialized]
        
        if schema:
            models = [m for m in models if m.schema == schema]
        
        return models
    
    def to_view_registry(
        self,
        include_ephemeral: bool = True,
        exclude_tags: Optional[List[str]] = None
    ) -> VirtualViewRegistry:
        """
        Convert all dbt models to a VirtualViewRegistry.
        
        Args:
            include_ephemeral: Include ephemeral models (compiled SQL only)
            exclude_tags: List of tags to exclude
            
        Returns:
            VirtualViewRegistry with all models as views
        """
        registry = VirtualViewRegistry()
        exclude_tags = exclude_tags or []
        
        for model in self._models.values():
            # Skip models without compiled SQL
            if not model.compiled_sql:
                logger.debug("Skipping model without compiled SQL: %s", model.name)
                continue
            
            # Skip ephemeral if not included
            if not include_ephemeral and model.materialized == "ephemeral":
                continue
            
            # Skip excluded tags
            if any(tag in model.tags for tag in exclude_tags):
                continue
            
            try:
                view = model.to_virtual_view()
                registry.register(view)
            except Exception as e:
                logger.warning("Failed to convert model %s: %s", model.name, e)
        
        logger.info("Created view registry with %d views from dbt manifest", len(registry))
        return registry
    
    def get_model_lineage(self, model_name: str) -> Dict[str, Any]:
        """
        Get the upstream and downstream lineage for a model.
        
        Returns:
            Dict with "upstream" and "downstream" lists of model names
        """
        model = self._models.get(model_name)
        if not model:
            return {"upstream": [], "downstream": []}
        
        # Upstream: models this one depends on
        upstream = [
            dep.split(".")[-1]
            for dep in model.depends_on
            if dep.startswith("model.")
        ]
        
        # Downstream: models that depend on this one
        downstream = []
        for other_model in self._models.values():
            if model.unique_id in other_model.depends_on:
                downstream.append(other_model.name)
        
        return {
            "upstream": upstream,
            "downstream": downstream
        }
    
    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "DbtManifest":
        """
        Load manifest from a file.
        
        Args:
            path: Path to manifest.json
            
        Returns:
            DbtManifest instance
        """
        path = Path(path)
        logger.info("Loading dbt manifest from: %s", path)
        
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        return cls(data)
    
    @classmethod
    def from_project(cls, project_path: Union[str, Path]) -> "DbtManifest":
        """
        Load manifest from a dbt project's target directory.
        
        Args:
            project_path: Path to dbt project root
            
        Returns:
            DbtManifest instance
        """
        project_path = Path(project_path)
        manifest_path = project_path / "target" / "manifest.json"
        
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"manifest.json not found at {manifest_path}. "
                "Run 'dbt compile' or 'dbt build' first."
            )
        
        return cls.from_file(manifest_path)
    
    def to_json(self) -> str:
        """Serialize models to JSON (for debugging/export)."""
        return json.dumps({
            "metadata": self.metadata,
            "models": [
                {
                    "name": m.name,
                    "unique_id": m.unique_id,
                    "schema": m.schema,
                    "materialized": m.materialized,
                    "description": m.description,
                    "dependencies": m.depends_on,
                    "tags": m.tags,
                }
                for m in self._models.values()
            ],
            "sources": [
                {
                    "name": s.name,
                    "source_name": s.source_name,
                    "schema": s.schema,
                    "description": s.description,
                }
                for s in self._sources.values()
            ]
        }, indent=2)
