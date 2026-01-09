"""
WaveQL Configuration - Centralized settings management.

All WaveQL SQLite databases and file storage locations are configured here.
This provides production-ready defaults with easy override options.

Configuration Priority (highest to lowest):
    1. Explicit parameters passed to WaveQLConnection
    2. Environment variables (WAVEQL_*)
    3. Config file (~/.waveql/config.yaml or WAVEQL_CONFIG_FILE)
    4. Built-in defaults

Environment Variables:
    WAVEQL_DATA_DIR          - Base directory for all WaveQL data
    WAVEQL_TRANSACTION_DB    - Path to transaction log database
    WAVEQL_REGISTRY_DB       - Path to materialized view registry
    WAVEQL_VIEWS_DIR         - Path to materialized view storage
    WAVEQL_CDC_STATE_DB      - Path to CDC state database
    WAVEQL_CREDENTIALS_FILE  - Path to cloud credentials YAML
    WAVEQL_CONFIG_FILE       - Path to config YAML file

Example Config File (~/.waveql/config.yaml):
    data_dir: /var/lib/waveql
    transaction_db: /var/lib/waveql/transactions.db
    registry_db: /var/lib/waveql/registry.db
    views_dir: /var/lib/waveql/views
    cdc_state_db: /var/lib/waveql/cdc_state.db
    credentials_file: /etc/waveql/credentials.yaml
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Default base directory
DEFAULT_DATA_DIR = Path.home() / ".waveql"


@dataclass
class WaveQLConfig:
    """
    Centralized configuration for WaveQL storage locations.
    
    All paths can be configured via:
    - Constructor parameters
    - Environment variables (WAVEQL_*)
    - Config file
    - Defaults (~/.waveql/)
    
    Attributes:
        data_dir: Base directory for all WaveQL data
        transaction_db: Path to transaction log SQLite database
        registry_db: Path to materialized view registry SQLite database
        views_dir: Path to materialized view Parquet storage directory
        cdc_state_db: Path to CDC state SQLite database
        credentials_file: Path to cloud credentials YAML file
        
    Example:
        # Use defaults
        config = WaveQLConfig.load()
        
        # Override via environment
        os.environ["WAVEQL_DATA_DIR"] = "/var/lib/waveql"
        config = WaveQLConfig.load()
        
        # Explicit configuration
        config = WaveQLConfig(data_dir="/app/data")
    """
    
    data_dir: Path = field(default_factory=lambda: DEFAULT_DATA_DIR)
    transaction_db: Optional[Path] = None
    registry_db: Optional[Path] = None
    views_dir: Optional[Path] = None
    cdc_state_db: Optional[Path] = None
    credentials_file: Optional[Path] = None
    
    def __post_init__(self):
        """Resolve all paths relative to data_dir if not explicitly set."""
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        
        # Resolve paths relative to data_dir
        if self.transaction_db is None:
            self.transaction_db = self.data_dir / "transactions.db"
        elif isinstance(self.transaction_db, str):
            self.transaction_db = Path(self.transaction_db)
        
        if self.registry_db is None:
            self.registry_db = self.data_dir / "registry.db"
        elif isinstance(self.registry_db, str):
            self.registry_db = Path(self.registry_db)
        
        if self.views_dir is None:
            self.views_dir = self.data_dir / "views"
        elif isinstance(self.views_dir, str):
            self.views_dir = Path(self.views_dir)
        
        if self.cdc_state_db is None:
            self.cdc_state_db = self.data_dir / "cdc_state.db"
        elif isinstance(self.cdc_state_db, str):
            self.cdc_state_db = Path(self.cdc_state_db)
        
        if self.credentials_file is None:
            self.credentials_file = self.data_dir / "credentials.yaml"
        elif isinstance(self.credentials_file, str):
            self.credentials_file = Path(self.credentials_file)
    
    @classmethod
    def load(
        cls,
        config_file: Optional[str] = None,
        **overrides,
    ) -> "WaveQLConfig":
        """
        Load configuration from environment, config file, and defaults.
        
        Args:
            config_file: Path to YAML config file (optional)
            **overrides: Explicit overrides for any setting
            
        Returns:
            WaveQLConfig instance
            
        Priority (highest to lowest):
            1. overrides parameter
            2. Environment variables
            3. Config file
            4. Defaults
        """
        config_data: Dict[str, Any] = {}
        
        # 1. Load from config file if exists
        config_path = config_file or os.environ.get("WAVEQL_CONFIG_FILE")
        if config_path is None:
            # Check default location
            default_config = DEFAULT_DATA_DIR / "config.yaml"
            if default_config.exists():
                config_path = str(default_config)
        
        if config_path and Path(config_path).exists():
            try:
                import yaml
                with open(config_path) as f:
                    file_config = yaml.safe_load(f) or {}
                config_data.update(file_config)
                logger.debug(f"Loaded config from {config_path}")
            except ImportError:
                logger.warning("PyYAML not installed, skipping config file")
            except Exception as e:
                logger.warning(f"Failed to load config file: {e}")
        
        # 2. Override with environment variables
        env_mapping = {
            "WAVEQL_DATA_DIR": "data_dir",
            "WAVEQL_TRANSACTION_DB": "transaction_db",
            "WAVEQL_REGISTRY_DB": "registry_db",
            "WAVEQL_VIEWS_DIR": "views_dir",
            "WAVEQL_CDC_STATE_DB": "cdc_state_db",
            "WAVEQL_CREDENTIALS_FILE": "credentials_file",
        }
        
        for env_var, config_key in env_mapping.items():
            value = os.environ.get(env_var)
            if value:
                config_data[config_key] = value
        
        # 3. Apply explicit overrides
        config_data.update({k: v for k, v in overrides.items() if v is not None})
        
        return cls(**config_data)
    
    def ensure_directories(self):
        """Create all necessary directories."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.views_dir.mkdir(parents=True, exist_ok=True)
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary with string paths."""
        return {
            "data_dir": str(self.data_dir),
            "transaction_db": str(self.transaction_db),
            "registry_db": str(self.registry_db),
            "views_dir": str(self.views_dir),
            "cdc_state_db": str(self.cdc_state_db),
            "credentials_file": str(self.credentials_file),
        }
    
    def __repr__(self) -> str:
        return f"WaveQLConfig(data_dir={self.data_dir})"


# Global configuration singleton
_global_config: Optional[WaveQLConfig] = None


def get_config() -> WaveQLConfig:
    """
    Get the global WaveQL configuration.
    
    Loads from environment/config file on first call.
    
    Returns:
        WaveQLConfig instance
    """
    global _global_config
    if _global_config is None:
        _global_config = WaveQLConfig.load()
    return _global_config


def set_config(config: WaveQLConfig):
    """
    Set the global WaveQL configuration.
    
    Call this at application startup to configure all WaveQL components.
    
    Args:
        config: WaveQLConfig instance
        
    Example:
        from waveql.config import WaveQLConfig, set_config
        
        # Configure for production
        config = WaveQLConfig(data_dir="/var/lib/waveql")
        set_config(config)
        
        # All subsequent WaveQL operations use this config
        conn = waveql.connect("servicenow://...")
    """
    global _global_config
    _global_config = config
    logger.info(f"WaveQL configured: {config.to_dict()}")


def reset_config():
    """Reset global configuration (mainly for testing)."""
    global _global_config
    _global_config = None
