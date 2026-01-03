"""
Rate Limiter - Handles API rate limiting with exponential backoff
"""

from __future__ import annotations
import logging
import random
import time
from typing import Any, Awaitable, Callable

import anyio
import httpx

from waveql.exceptions import RateLimitError

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter with exponential backoff for API requests.
    
    Features:
    - Configurable retry attempts
    - Exponential backoff with jitter
    - Respects Retry-After headers
    """
    
    def __init__(
        self,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
    
    def execute_with_retry(
        self,
        func: Callable[..., Any],
        *args,
        retry_on: tuple = (429, 503),
        **kwargs
    ) -> Any:
        """
        Execute function with retry on rate limit errors.
        
        Args:
            func: Function to execute
            retry_on: HTTP status codes to retry on
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Result from function
            
        Raises:
            Last exception if all retries fail
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                # Check if we should retry
                should_retry = False
                retry_after = None
                
                # Check for RateLimitError
                if isinstance(e, RateLimitError):
                    should_retry = True
                    retry_after = e.retry_after
                
                # Check for requests HTTPError
                elif hasattr(e, "response") and hasattr(e.response, "status_code"):
                    if e.response.status_code in retry_on:
                        should_retry = True
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                retry_after = int(retry_after)
                            except ValueError:
                                retry_after = None
                
                if not should_retry or attempt == self.max_retries:
                    raise
                
                # Calculate delay
                if retry_after:
                    delay = float(retry_after)
                else:
                    delay = self._calculate_delay(attempt)
                
                logger.debug("Retry attempt %d/%d after %.2fs delay", attempt + 1, self.max_retries, delay)
                time.sleep(delay)
        
        raise last_exception
    
    async def execute_with_retry_async(
        self,
        func: Callable[..., Awaitable[Any]],
        *args,
        retry_on: tuple = (429, 503),
        **kwargs
    ) -> Any:
        """Execute async function with retry on rate limit errors."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                should_retry = False
                retry_after = None
                
                if isinstance(e, RateLimitError):
                    should_retry = True
                    retry_after = e.retry_after
                
                elif hasattr(e, "response") and hasattr(e.response, "status_code"):
                    if e.response.status_code in retry_on:
                        should_retry = True
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                retry_after = int(retry_after)
                            except ValueError:
                                retry_after = None

                # httpx support
                if isinstance(e, httpx.HTTPStatusError):
                    if e.response.status_code in retry_on:
                        should_retry = True
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                retry_after = int(retry_after)
                            except ValueError:
                                retry_after = None
                
                if not should_retry or attempt == self.max_retries:
                    raise
                
                if retry_after:
                    delay = float(retry_after)
                else:
                    delay = self._calculate_delay(attempt)
                
                logger.debug("Async retry attempt %d/%d after %.2fs delay", attempt + 1, self.max_retries, delay)
                await anyio.sleep(delay)
        
        raise last_exception

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and jitter."""
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)
        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        return delay + jitter
