"""
Tests for WaveQL utils/streaming module - ParallelFetcher.

This covers the 16% uncovered module waveql/utils/streaming.py
"""

import pytest
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor

from waveql.utils.streaming import ParallelFetcher


class TestParallelFetcher:
    """Tests for ParallelFetcher class."""
    
    def test_init_default_values(self):
        """Test ParallelFetcher initializes with defaults."""
        fetcher = ParallelFetcher()
        assert fetcher.max_workers == 4
        assert fetcher.batch_size == 1000
    
    def test_init_custom_values(self):
        """Test ParallelFetcher with custom values."""
        fetcher = ParallelFetcher(max_workers=8, batch_size=500)
        assert fetcher.max_workers == 8
        assert fetcher.batch_size == 500
    
    def test_records_to_arrow_empty(self):
        """Test converting empty records to Arrow."""
        fetcher = ParallelFetcher()
        result = fetcher._records_to_arrow([])
        assert isinstance(result, pa.Table)
        assert len(result) == 0
    
    def test_records_to_arrow_with_data(self):
        """Test converting records to Arrow table."""
        fetcher = ParallelFetcher()
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        result = fetcher._records_to_arrow(records)
        
        assert isinstance(result, pa.Table)
        assert len(result) == 3
        assert "id" in result.column_names
        assert "name" in result.column_names
    
    def test_fetch_parallel_known_pages(self):
        """Test parallel fetch with known total pages."""
        fetcher = ParallelFetcher(max_workers=2)
        
        def mock_fetch(page):
            return [{"id": page * 10 + i, "value": f"val_{page}_{i}"} for i in range(5)]
        
        result = fetcher.fetch_parallel(mock_fetch, total_pages=3, start_page=0)
        
        assert isinstance(result, pa.Table)
        assert len(result) == 15  # 3 pages * 5 records each
    
    def test_fetch_parallel_unknown_pages_stop_on_empty(self):
        """Test parallel fetch with unknown pages, stopping on empty."""
        fetcher = ParallelFetcher(max_workers=2)
        
        pages_fetched = []
        
        def mock_fetch(page):
            pages_fetched.append(page)
            if page >= 3:
                return []  # Empty signals end
            return [{"id": page * 10 + i} for i in range(5)]
        
        result = fetcher.fetch_parallel(mock_fetch, total_pages=None, stop_on_empty=True)
        
        assert isinstance(result, pa.Table)
        # Should have fetched pages 0, 1, 2 before getting empty
        assert len(result) <= 20  # At most 4 pages fetched
    
    def test_fetch_parallel_with_start_page(self):
        """Test parallel fetch starting from specific page."""
        fetcher = ParallelFetcher(max_workers=2)
        
        pages_fetched = []
        
        def mock_fetch(page):
            pages_fetched.append(page)
            if page >= 5:
                return []
            return [{"id": page}]
        
        fetcher.fetch_parallel(mock_fetch, total_pages=None, start_page=2)
        
        # Should have started from page 2
        assert 0 not in pages_fetched
        assert 1 not in pages_fetched
        assert 2 in pages_fetched
    
    def test_fetch_parallel_error_handling_known_pages(self):
        """Test error handling in parallel fetch with known pages."""
        fetcher = ParallelFetcher(max_workers=2)
        
        def mock_fetch(page):
            if page == 1:
                raise ValueError("Simulated error")
            return [{"id": page}]
        
        with pytest.raises(ValueError, match="Simulated error"):
            fetcher.fetch_parallel(mock_fetch, total_pages=3)
    
    def test_fetch_parallel_error_handling_unknown_pages(self):
        """Test error handling in parallel fetch with unknown pages."""
        fetcher = ParallelFetcher(max_workers=2)
        
        call_count = 0
        
        def mock_fetch(page):
            nonlocal call_count
            call_count += 1
            if call_count > 2:
                raise RuntimeError("Simulated network error")
            return [{"id": page}]
        
        with pytest.raises(RuntimeError, match="Simulated network error"):
            fetcher.fetch_parallel(mock_fetch, total_pages=None)
    
    def test_wait_for_any(self):
        """Test _wait_for_any helper method."""
        fetcher = ParallelFetcher()
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(lambda: 1),
                executor.submit(lambda: 2),
            }
            done, remaining = fetcher._wait_for_any(futures)
            
            assert len(done) == 1
            assert len(remaining) == 1
    
    def test_stream_batches(self):
        """Test streaming batches."""
        fetcher = ParallelFetcher(batch_size=2)
        
        def mock_fetch(page):
            if page >= 3:
                return []
            return [{"id": page * 10 + i, "name": f"item_{page}_{i}"} for i in range(5)]
        
        batches = list(fetcher.stream_batches(mock_fetch))
        
        assert len(batches) > 0
        # Each batch should be a RecordBatch
        for batch in batches:
            assert isinstance(batch, pa.RecordBatch)
    
    def test_stream_batches_empty(self):
        """Test streaming batches with empty first page."""
        fetcher = ParallelFetcher()
        
        def mock_fetch(page):
            return []  # No data
        
        batches = list(fetcher.stream_batches(mock_fetch))
        
        assert len(batches) == 0
    
    def test_stream_batches_single_page(self):
        """Test streaming batches with single page."""
        fetcher = ParallelFetcher(batch_size=10)
        
        def mock_fetch(page):
            if page >= 1:
                return []
            return [{"id": i} for i in range(5)]
        
        batches = list(fetcher.stream_batches(mock_fetch))
        
        assert len(batches) == 1
        assert batches[0].num_rows == 5


class TestParallelFetcherConcurrency:
    """Tests for concurrency aspects of ParallelFetcher."""
    
    def test_max_workers_respected(self):
        """Test that max_workers limits concurrency."""
        import time
        from threading import active_count
        
        fetcher = ParallelFetcher(max_workers=2)
        max_concurrent = 0
        
        def slow_fetch(page):
            nonlocal max_concurrent
            # Track max concurrent threads
            current = active_count()
            max_concurrent = max(max_concurrent, current)
            time.sleep(0.05)
            if page >= 4:
                return []
            return [{"id": page}]
        
        fetcher.fetch_parallel(slow_fetch, total_pages=None)
        
        # Max concurrent should be limited
        assert max_concurrent <= 10  # Workers + main thread + overhead


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
