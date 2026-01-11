"""
Tests for Config Module - Centralized configuration management

Tests cover:
- Default configuration values
- Configuration from environment variables
- Configuration from YAML files
- Configuration overrides
- Global config singleton
- Directory creation
- Serialization to dict
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch, mock_open

from waveql.config import (
    WaveQLConfig,
    get_config,
    set_config,
    reset_config,
    DEFAULT_DATA_DIR,
)


class TestWaveQLConfigDefaults:
    """Tests for default configuration values."""
    
    def test_default_data_dir(self):
        """Test default data directory is user home."""
        config = WaveQLConfig()
        
        assert config.data_dir == DEFAULT_DATA_DIR
        assert DEFAULT_DATA_DIR == Path.home() / ".waveql"
    
    def test_default_paths_relative_to_data_dir(self):
        """Test that all paths are relative to data_dir."""
        config = WaveQLConfig()
        
        assert config.transaction_db == config.data_dir / "transactions.db"
        assert config.registry_db == config.data_dir / "registry.db"
        assert config.views_dir == config.data_dir / "views"
        assert config.cdc_state_db == config.data_dir / "cdc_state.db"
        assert config.credentials_file == config.data_dir / "credentials.yaml"


class TestWaveQLConfigCustom:
    """Tests for custom configuration values."""
    
    def test_custom_data_dir(self):
        """Test custom data directory."""
        config = WaveQLConfig(data_dir="/var/lib/waveql")
        
        assert config.data_dir == Path("/var/lib/waveql")
    
    def test_custom_data_dir_updates_paths(self):
        """Test that custom data_dir updates all relative paths."""
        config = WaveQLConfig(data_dir="/app/data")
        
        assert config.transaction_db == Path("/app/data/transactions.db")
        assert config.registry_db == Path("/app/data/registry.db")
        assert config.views_dir == Path("/app/data/views")
    
    def test_explicit_path_overrides(self):
        """Test that explicit paths override defaults."""
        config = WaveQLConfig(
            data_dir="/app/data",
            transaction_db="/custom/transactions.db",
            registry_db="/custom/registry.db",
        )
        
        assert config.transaction_db == Path("/custom/transactions.db")
        assert config.registry_db == Path("/custom/registry.db")
        # Others should still be relative to data_dir
        assert config.views_dir == Path("/app/data/views")
    
    def test_string_paths_converted_to_path(self):
        """Test that string paths are converted to Path objects."""
        config = WaveQLConfig(
            data_dir="/app/data",
            transaction_db="/custom/tx.db",
        )
        
        assert isinstance(config.data_dir, Path)
        assert isinstance(config.transaction_db, Path)


class TestWaveQLConfigLoad:
    """Tests for loading configuration from various sources."""
    
    def setup_method(self):
        """Reset global config and environment before each test."""
        reset_config()
        # Clear any WAVEQL_ environment variables
        for key in list(os.environ.keys()):
            if key.startswith("WAVEQL_"):
                del os.environ[key]
    
    def teardown_method(self):
        """Clean up after each test."""
        reset_config()
        for key in list(os.environ.keys()):
            if key.startswith("WAVEQL_"):
                del os.environ[key]
    
    def test_load_defaults(self):
        """Test loading with no env vars or config file."""
        config = WaveQLConfig.load()
        
        assert config.data_dir == DEFAULT_DATA_DIR
    
    def test_load_from_environment(self):
        """Test loading from environment variables."""
        os.environ["WAVEQL_DATA_DIR"] = "/env/waveql"
        
        config = WaveQLConfig.load()
        
        assert config.data_dir == Path("/env/waveql")
    
    def test_load_individual_paths_from_env(self):
        """Test loading individual paths from environment."""
        os.environ["WAVEQL_TRANSACTION_DB"] = "/env/tx.db"
        os.environ["WAVEQL_REGISTRY_DB"] = "/env/registry.db"
        
        config = WaveQLConfig.load()
        
        assert config.transaction_db == Path("/env/tx.db")
        assert config.registry_db == Path("/env/registry.db")
    
    def test_load_with_overrides(self):
        """Test that overrides take precedence over everything."""
        os.environ["WAVEQL_DATA_DIR"] = "/env/waveql"
        
        config = WaveQLConfig.load(data_dir="/override/waveql")
        
        assert config.data_dir == Path("/override/waveql")
    
    def test_load_all_env_vars(self):
        """Test all environment variable mappings."""
        os.environ["WAVEQL_DATA_DIR"] = "/env/data"
        os.environ["WAVEQL_TRANSACTION_DB"] = "/env/tx.db"
        os.environ["WAVEQL_REGISTRY_DB"] = "/env/reg.db"
        os.environ["WAVEQL_VIEWS_DIR"] = "/env/views"
        os.environ["WAVEQL_CDC_STATE_DB"] = "/env/cdc.db"
        os.environ["WAVEQL_CREDENTIALS_FILE"] = "/env/creds.yaml"
        
        config = WaveQLConfig.load()
        
        assert config.data_dir == Path("/env/data")
        assert config.transaction_db == Path("/env/tx.db")
        assert config.registry_db == Path("/env/reg.db")
        assert config.views_dir == Path("/env/views")
        assert config.cdc_state_db == Path("/env/cdc.db")
        assert config.credentials_file == Path("/env/creds.yaml")


class TestWaveQLConfigYAML:
    """Tests for loading configuration from YAML files."""
    
    def setup_method(self):
        """Reset global config before each test."""
        reset_config()
    
    def teardown_method(self):
        """Clean up after each test."""
        reset_config()
        for key in list(os.environ.keys()):
            if key.startswith("WAVEQL_"):
                del os.environ[key]
    
    def test_load_from_yaml_file(self, tmp_path):
        """Test loading configuration from YAML file."""
        pytest.importorskip("yaml")
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
data_dir: /yaml/data
transaction_db: /yaml/tx.db
registry_db: /yaml/reg.db
""")
        
        config = WaveQLConfig.load(config_file=str(config_file))
        
        assert config.data_dir == Path("/yaml/data")
        assert config.transaction_db == Path("/yaml/tx.db")
        assert config.registry_db == Path("/yaml/reg.db")
    
    def test_yaml_override_by_env(self, tmp_path):
        """Test that environment overrides YAML."""
        pytest.importorskip("yaml")
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
data_dir: /yaml/data
""")
        
        os.environ["WAVEQL_DATA_DIR"] = "/env/data"
        
        config = WaveQLConfig.load(config_file=str(config_file))
        
        # Environment should win over YAML
        assert config.data_dir == Path("/env/data")
    
    def test_nonexistent_yaml_file_ignored(self):
        """Test that non-existent config file is ignored."""
        config = WaveQLConfig.load(config_file="/nonexistent/config.yaml")
        
        # Should still work with defaults
        assert config.data_dir == DEFAULT_DATA_DIR


class TestGlobalConfigSingleton:
    """Tests for global config singleton functions."""
    
    def setup_method(self):
        """Reset global config before each test."""
        reset_config()
    
    def teardown_method(self):
        """Clean up after each test."""
        reset_config()
        for key in list(os.environ.keys()):
            if key.startswith("WAVEQL_"):
                del os.environ[key]
    
    def test_get_config_creates_on_first_call(self):
        """Test that get_config creates config on first call."""
        config = get_config()
        
        assert config is not None
        assert isinstance(config, WaveQLConfig)
    
    def test_get_config_returns_same_instance(self):
        """Test that get_config returns the same instance."""
        config1 = get_config()
        config2 = get_config()
        
        assert config1 is config2
    
    def test_set_config(self):
        """Test setting global config."""
        custom_config = WaveQLConfig(data_dir="/custom/data")
        
        set_config(custom_config)
        
        assert get_config() is custom_config
    
    def test_reset_config(self):
        """Test resetting global config."""
        custom_config = WaveQLConfig(data_dir="/custom/data")
        set_config(custom_config)
        
        reset_config()
        
        # Next get_config should create new instance
        new_config = get_config()
        assert new_config is not custom_config


class TestEnsureDirectories:
    """Tests for directory creation."""
    
    def test_ensure_directories_creates_dirs(self, tmp_path):
        """Test that ensure_directories creates required directories."""
        data_dir = tmp_path / "waveql"
        
        config = WaveQLConfig(data_dir=str(data_dir))
        config.ensure_directories()
        
        assert data_dir.exists()
        assert (data_dir / "views").exists()
    
    def test_ensure_directories_idempotent(self, tmp_path):
        """Test that ensure_directories can be called multiple times."""
        data_dir = tmp_path / "waveql"
        
        config = WaveQLConfig(data_dir=str(data_dir))
        config.ensure_directories()
        config.ensure_directories()  # Should not raise
        
        assert data_dir.exists()


class TestToDict:
    """Tests for configuration serialization."""
    
    def test_to_dict(self):
        """Test converting configuration to dictionary."""
        config = WaveQLConfig(data_dir="/app/data")
        
        d = config.to_dict()
        
        assert d["data_dir"] == "/app/data"
        assert d["transaction_db"] == "/app/data/transactions.db"
        assert d["registry_db"] == "/app/data/registry.db"
        assert d["views_dir"] == "/app/data/views"
        assert d["cdc_state_db"] == "/app/data/cdc_state.db"
        assert d["credentials_file"] == "/app/data/credentials.yaml"
    
    def test_to_dict_all_strings(self):
        """Test that to_dict returns all string values."""
        config = WaveQLConfig()
        
        d = config.to_dict()
        
        for key, value in d.items():
            assert isinstance(value, str), f"{key} should be string, got {type(value)}"


class TestRepr:
    """Tests for string representation."""
    
    def test_repr(self):
        """Test config repr."""
        config = WaveQLConfig(data_dir="/app/data")
        
        repr_str = repr(config)
        
        assert "WaveQLConfig" in repr_str
        assert "/app/data" in repr_str
