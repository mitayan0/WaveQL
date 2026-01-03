"""
Parallel Streaming - High-throughput parallel data fetching with Arrow batches
"""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterator, List, Any, Dict
import pyarrow as pa


class ParallelFetcher:
    """
    Parallel data fetcher for high-throughput API data retrieval.
    
    Features:
    - Concurrent page fetching
    - Arrow batch streaming
    - Memory-efficient iteration
    """
    
    def __init__(
        self,
        max_workers: int = 4,
        batch_size: int = 1000,
    ):
        self.max_workers = max_workers
        self.batch_size = batch_size
    
    def fetch_parallel(
        self,
        fetch_func: Callable[[int], List[Dict]],
        total_pages: int = None,
        stop_on_empty: bool = True,
        start_page: int = 0,
    ) -> pa.Table:
        """
        Fetch data in parallel from multiple pages.
        
        Args:
            fetch_func: Function that takes page number and returns list of records
            total_pages: Total number of pages (None for auto-detect)
            stop_on_empty: Stop when empty page is returned
            start_page: Page number to start fetching from
            
        Returns:
            Combined Arrow table
        """
        all_records = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            if total_pages:
                # Known number of pages
                futures = {
                    executor.submit(fetch_func, page): page
                    for page in range(start_page, total_pages)
                }
                
                for future in as_completed(futures):
                    page_num = futures[future]
                    try:
                        records = future.result()
                        all_records.extend(records)
                    except Exception as e:
                        # Log and re-raise to indicate failure
                        logger.error(f"Error fetching page {page_num}: {e}")
                        raise
            else:
                # Unknown total - fetch until empty
                page = start_page
                active_futures = set()
                
                # Start initial batch of requests
                for i in range(self.max_workers):
                    active_futures.add(executor.submit(fetch_func, page + i))
                page += self.max_workers
                
                while active_futures:
                    # Wait for any to complete
                    done, active_futures = self._wait_for_any(active_futures)
                    
                    for future in done:
                        try:
                            records = future.result()
                            if records:
                                all_records.extend(records)
                                # Submit next page
                                active_futures.add(executor.submit(fetch_func, page))
                                page += 1
                            elif stop_on_empty:
                                # Cancel remaining and exit
                                for f in active_futures:
                                    f.cancel()
                                active_futures = set()
                                break
                        except Exception as e:
                            # Propagate exception
                            for f in active_futures:
                                f.cancel()
                            logger.error(f"Error in parallel fetch at page {page}: {e}")
                            raise
        
        return self._records_to_arrow(all_records)
    
    def _wait_for_any(self, futures: set):
        """Wait for any future to complete."""
        done = set()
        remaining = set()
        
        for future in as_completed(futures):
            done.add(future)
            break  # Only wait for first one
        
        for future in futures:
            if future not in done:
                remaining.add(future)
        
        return done, remaining
    
    def stream_batches(
        self,
        fetch_func: Callable[[int], List[Dict]],
    ) -> Iterator[pa.RecordBatch]:
        """
        Stream data as Arrow RecordBatches for memory-efficient processing.
        
        Args:
            fetch_func: Function that takes page number and returns records
            
        Yields:
            Arrow RecordBatch for each page
        """
        page = 0
        
        while True:
            records = fetch_func(page)
            if not records:
                break
            
            table = self._records_to_arrow(records)
            for batch in table.to_batches(max_chunksize=self.batch_size):
                yield batch
            
            page += 1
    
    def _records_to_arrow(self, records: List[Dict]) -> pa.Table:
        """Convert records to Arrow table."""
        if not records:
            return pa.table({})
        
        # Infer schema from first record
        columns = {}
        for key in records[0].keys():
            values = [r.get(key) for r in records]
            columns[key] = pa.array(values)
        
        return pa.table(columns)
