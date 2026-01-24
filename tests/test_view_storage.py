"""
Tests for WaveQL materialized_view/storage module.

This covers the 65% uncovered module waveql/materialized_view/storage.py
"""

import pytest
import tempfile
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock

import pyarrow as pa

from waveql.materialized_view.storage import ViewStorage
from waveql.materialized_view.models import ViewStats


class TestViewStorageInit:
    """Tests for ViewStorage initialization."""
    
    def test_init_with_path(self, tmp_path):
        """Test initialization with explicit path."""
        storage = ViewStorage(base_path=tmp_path)
        assert storage.base_path == tmp_path
        assert storage.base_path.exists()
    
    def test_init_creates_directory(self, tmp_path):
        """Test that init creates the base directory."""
        new_path = tmp_path / "views" / "data"
        storage = ViewStorage(base_path=new_path)
        assert new_path.exists()
    
    def test_init_default_path(self):
        """Test initialization with default path from config."""
        with patch("waveql.config.get_config") as mock_config:
            mock_cfg = MagicMock()
            mock_cfg.views_dir = Path(tempfile.mkdtemp()) / "views"
            mock_config.return_value = mock_cfg
            
            # Need to reimport or instantiate after patching
            storage = ViewStorage()
            # Just verify it doesn't crash and has some base_path
            assert storage.base_path is not None


class TestViewStoragePaths:
    """Tests for path helper methods."""
    
    def test_get_view_dir(self, tmp_path):
        """Test getting view directory."""
        storage = ViewStorage(base_path=tmp_path)
        view_dir = storage.get_view_dir("my_view")
        assert view_dir == tmp_path / "my_view"
    
    def test_get_data_path(self, tmp_path):
        """Test getting data path."""
        storage = ViewStorage(base_path=tmp_path)
        data_path = storage.get_data_path("my_view")
        assert data_path == tmp_path / "my_view" / "data.parquet"
    
    def test_get_metadata_path(self, tmp_path):
        """Test getting metadata path."""
        storage = ViewStorage(base_path=tmp_path)
        meta_path = storage.get_metadata_path("my_view")
        assert meta_path == tmp_path / "my_view" / "metadata.json"


class TestViewStorageWrite:
    """Tests for write operations."""
    
    def test_write_basic(self, tmp_path):
        """Test basic write operation."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        stats = storage.write("test_view", data)
        
        assert stats.row_count == 3
        assert stats.size_bytes > 0
        assert storage.exists("test_view")
    
    def test_write_with_metadata(self, tmp_path):
        """Test write with metadata."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1, 2]})
        metadata = {"source": "test", "timestamp": datetime.now()}
        
        storage.write("test_view", data, metadata=metadata)
        
        meta_path = storage.get_metadata_path("test_view")
        assert meta_path.exists()
        
        with open(meta_path) as f:
            saved_meta = json.load(f)
        assert saved_meta["source"] == "test"
    
    def test_write_empty_table(self, tmp_path):
        """Test writing empty table."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": []})
        stats = storage.write("empty_view", data)
        
        assert stats.row_count == 0
        assert storage.exists("empty_view")
    
    def test_write_overwrites(self, tmp_path):
        """Test that write overwrites existing data."""
        storage = ViewStorage(base_path=tmp_path)
        
        # First write
        data1 = pa.table({"id": [1, 2]})
        storage.write("test_view", data1)
        
        # Second write
        data2 = pa.table({"id": [3, 4, 5, 6]})
        stats = storage.write("test_view", data2)
        
        assert stats.row_count == 4
        
        # Verify data was overwritten
        result = storage.read("test_view")
        assert len(result) == 4


class TestViewStorageRead:
    """Tests for read operations."""
    
    def test_read_existing(self, tmp_path):
        """Test reading existing view."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1, 2, 3]})
        storage.write("test_view", data)
        
        result = storage.read("test_view")
        
        assert result is not None
        assert len(result) == 3
    
    def test_read_nonexistent(self, tmp_path):
        """Test reading non-existent view."""
        storage = ViewStorage(base_path=tmp_path)
        
        result = storage.read("nonexistent")
        
        assert result is None


class TestViewStorageAppend:
    """Tests for append operations."""
    
    def test_append_to_existing(self, tmp_path):
        """Test appending to existing view."""
        storage = ViewStorage(base_path=tmp_path)
        
        # Initial write
        data1 = pa.table({"id": [1, 2]})
        storage.write("test_view", data1)
        
        # Append
        data2 = pa.table({"id": [3, 4]})
        stats = storage.append("test_view", data2)
        
        assert stats.row_count == 4
        
        result = storage.read("test_view")
        assert len(result) == 4
    
    def test_append_to_nonexistent(self, tmp_path):
        """Test appending to non-existent view (creates new)."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1, 2]})
        stats = storage.append("new_view", data)
        
        assert stats.row_count == 2
        assert storage.exists("new_view")


class TestViewStorageUpsert:
    """Tests for upsert operations."""
    
    def test_upsert_new_view(self, tmp_path):
        """Test upsert on non-existent view (creates new)."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1, 2], "name": ["a", "b"]})
        stats = storage.upsert("test_view", data, key_column="id")
        
        assert stats.row_count == 2
    
    def test_upsert_update_existing(self, tmp_path):
        """Test upsert updates existing rows."""
        storage = ViewStorage(base_path=tmp_path)
        
        # Initial data
        data1 = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        storage.write("test_view", data1)
        
        # Upsert with updates
        data2 = pa.table({"id": [2, 3], "name": ["Bobby", "Charlie"]})
        stats = storage.upsert("test_view", data2, key_column="id")
        
        # Should have 3 rows: id=1 (original), id=2 (updated), id=3 (new)
        result = storage.read("test_view")
        assert len(result) == 3


class TestViewStorageExists:
    """Tests for exists method."""
    
    def test_exists_true(self, tmp_path):
        """Test exists returns True for existing view."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1]})
        storage.write("test_view", data)
        
        assert storage.exists("test_view") is True
    
    def test_exists_false(self, tmp_path):
        """Test exists returns False for non-existent view."""
        storage = ViewStorage(base_path=tmp_path)
        
        assert storage.exists("nonexistent") is False


class TestViewStorageGetStats:
    """Tests for get_stats method."""
    
    def test_get_stats_existing(self, tmp_path):
        """Test getting stats for existing view."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1, 2, 3]})
        storage.write("test_view", data)
        
        stats = storage.get_stats("test_view")
        
        assert stats is not None
        assert stats.row_count == 3
        assert stats.size_bytes > 0
    
    def test_get_stats_nonexistent(self, tmp_path):
        """Test getting stats for non-existent view."""
        storage = ViewStorage(base_path=tmp_path)
        
        stats = storage.get_stats("nonexistent")
        
        assert stats is None


class TestViewStorageDelete:
    """Tests for delete operations."""
    
    def test_delete_existing(self, tmp_path):
        """Test deleting existing view."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1]})
        storage.write("test_view", data)
        
        # Also write metadata to test full cleanup
        storage.write("test_view", data, metadata={"test": True})
        
        result = storage.delete("test_view")
        
        assert result is True
        assert not storage.exists("test_view")
    
    def test_delete_nonexistent(self, tmp_path):
        """Test deleting non-existent view."""
        storage = ViewStorage(base_path=tmp_path)
        
        result = storage.delete("nonexistent")
        
        assert result is False


class TestViewStorageListViews:
    """Tests for list_views method."""
    
    def test_list_views_empty(self, tmp_path):
        """Test listing views when none exist."""
        storage = ViewStorage(base_path=tmp_path)
        
        views = storage.list_views()
        
        assert views == []
    
    def test_list_views_multiple(self, tmp_path):
        """Test listing multiple views."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1]})
        storage.write("view_a", data)
        storage.write("view_b", data)
        storage.write("view_c", data)
        
        views = storage.list_views()
        
        assert len(views) == 3
        assert "view_a" in views
        assert "view_b" in views
        assert "view_c" in views
    
    def test_list_views_sorted(self, tmp_path):
        """Test that views are returned sorted."""
        storage = ViewStorage(base_path=tmp_path)
        
        data = pa.table({"id": [1]})
        storage.write("z_view", data)
        storage.write("a_view", data)
        
        views = storage.list_views()
        
        assert views == ["a_view", "z_view"]
    
    def test_list_views_excludes_empty_dirs(self, tmp_path):
        """Test that directories without data.parquet are excluded."""
        storage = ViewStorage(base_path=tmp_path)
        
        # Create a valid view
        data = pa.table({"id": [1]})
        storage.write("valid_view", data)
        
        # Create an empty directory
        (tmp_path / "empty_dir").mkdir()
        
        views = storage.list_views()
        
        assert views == ["valid_view"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
