"""
View Storage - Parquet-based storage for materialized view data
"""

from __future__ import annotations
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from waveql.materialized_view.models import ViewStats

logger = logging.getLogger(__name__)


class ViewStorage:
    """
    Handles Parquet read/write operations for materialized views.
    
    Storage layout:
        base_path/
        ├── view_name/
        │   ├── data.parquet
        │   └── metadata.json
    """
    
    def __init__(self, base_path: Path = None):
        """
        Initialize storage.
        
        Args:
            base_path: Base directory for view storage. Defaults to ~/.waveql/views/
        """
        if base_path is None:
            base_path = Path.home() / ".waveql" / "views"
        
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def get_view_dir(self, name: str) -> Path:
        """Get the directory for a specific view."""
        return self.base_path / name
    
    def get_data_path(self, name: str) -> Path:
        """Get the Parquet file path for a view."""
        return self.get_view_dir(name) / "data.parquet"
    
    def get_metadata_path(self, name: str) -> Path:
        """Get the metadata file path for a view."""
        return self.get_view_dir(name) / "metadata.json"
    
    def write(self, name: str, data: pa.Table, metadata: dict = None) -> ViewStats:
        """
        Write data to a materialized view (full replacement).
        
        Args:
            name: View name
            data: PyArrow Table to write
            metadata: Optional metadata to store
            
        Returns:
            ViewStats with row count and size
        """
        view_dir = self.get_view_dir(name)
        view_dir.mkdir(parents=True, exist_ok=True)
        
        data_path = self.get_data_path(name)
        
        # Write Parquet file with compression
        pq.write_table(
            data,
            data_path,
            compression="snappy",
            use_dictionary=True,
        )
        
        # Get file stats
        size_bytes = data_path.stat().st_size
        row_count = len(data)
        
        # Write metadata
        if metadata:
            metadata_path = self.get_metadata_path(name)
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)
        
        stats = ViewStats(
            row_count=row_count,
            size_bytes=size_bytes,
            last_refresh=datetime.now(),
        )
        
        logger.info(
            "Wrote materialized view '%s': %d rows, %.2f MB",
            name, row_count, size_bytes / (1024 * 1024)
        )
        
        return stats
    
    def append(self, name: str, new_data: pa.Table) -> ViewStats:
        """
        Append data to an existing materialized view.
        
        Args:
            name: View name
            new_data: PyArrow Table with new rows to append
            
        Returns:
            Updated ViewStats
        """
        data_path = self.get_data_path(name)
        
        if not data_path.exists():
            # No existing data, just write
            return self.write(name, new_data)
        
        # Read existing data
        existing = self.read(name)
        
        # Concatenate
        combined = pa.concat_tables([existing, new_data])
        
        # Write back
        return self.write(name, combined)
    
    def upsert(self, name: str, new_data: pa.Table, key_column: str) -> ViewStats:
        """
        Upsert data into an existing materialized view.
        
        Updates existing rows (by key) and inserts new rows.
        
        Args:
            name: View name
            new_data: PyArrow Table with data to upsert
            key_column: Column to use as unique key
            
        Returns:
            Updated ViewStats
        """
        data_path = self.get_data_path(name)
        
        if not data_path.exists():
            return self.write(name, new_data)
        
        existing = self.read(name)
        
        # Convert to pandas for easier upsert logic
        import pandas as pd
        
        existing_df = existing.to_pandas()
        new_df = new_data.to_pandas()
        
        # Set index for upsert
        existing_df.set_index(key_column, inplace=True)
        new_df.set_index(key_column, inplace=True)
        
        # Update existing + add new
        existing_df.update(new_df)
        
        # Add rows that don't exist in existing
        new_keys = new_df.index.difference(existing_df.index)
        if len(new_keys) > 0:
            existing_df = pd.concat([existing_df, new_df.loc[new_keys]])
        
        existing_df.reset_index(inplace=True)
        
        # Convert back to Arrow and write
        combined = pa.Table.from_pandas(existing_df)
        return self.write(name, combined)
    
    def read(self, name: str) -> Optional[pa.Table]:
        """
        Read data from a materialized view.
        
        Args:
            name: View name
            
        Returns:
            PyArrow Table or None if not found
        """
        data_path = self.get_data_path(name)
        
        if not data_path.exists():
            logger.warning("Materialized view '%s' not found at %s", name, data_path)
            return None
        
        return pq.read_table(data_path)
    
    def exists(self, name: str) -> bool:
        """Check if a view's data exists."""
        return self.get_data_path(name).exists()
    
    def get_stats(self, name: str) -> Optional[ViewStats]:
        """
        Get statistics for a view's stored data.
        
        Args:
            name: View name
            
        Returns:
            ViewStats or None if not found
        """
        data_path = self.get_data_path(name)
        
        if not data_path.exists():
            return None
        
        # Get file stats
        size_bytes = data_path.stat().st_size
        
        # Read row count from Parquet metadata (fast, doesn't load data)
        parquet_file = pq.ParquetFile(data_path)
        row_count = parquet_file.metadata.num_rows
        
        return ViewStats(
            row_count=row_count,
            size_bytes=size_bytes,
        )
    
    def delete(self, name: str) -> bool:
        """
        Delete a materialized view's data.
        
        Args:
            name: View name
            
        Returns:
            True if deleted, False if not found
        """
        view_dir = self.get_view_dir(name)
        
        if not view_dir.exists():
            return False
        
        # Delete all files in the view directory
        for file in view_dir.iterdir():
            file.unlink()
        
        # Delete the directory
        view_dir.rmdir()
        
        logger.info("Deleted materialized view data: %s", name)
        return True
    
    def list_views(self) -> list:
        """List all view names that have stored data."""
        if not self.base_path.exists():
            return []
        
        views = []
        for item in self.base_path.iterdir():
            if item.is_dir() and (item / "data.parquet").exists():
                views.append(item.name)
        
        return sorted(views)
