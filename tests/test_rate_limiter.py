"""
Tests for Rate Limiter - Exponential backoff and retry logic

Tests cover:
- Exponential backoff calculation with jitter
- Retry-After header handling
- Sync and async retry behavior
- Different error types (429, 503, RateLimitError)
- Max retries enforcement
"""

import pytest
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import httpx

from waveql.utils.rate_limiter import RateLimiter
from waveql.exceptions import RateLimitError


class TestRateLimiterInit:
    """Tests for RateLimiter initialization."""
    
    def test_default_values(self):
        """Test default initialization values."""
        limiter = RateLimiter()
        
        assert limiter.max_retries == 5
        assert limiter.base_delay == 1.0
        assert limiter.max_delay == 60.0
        assert limiter.exponential_base == 2.0
    
    def test_custom_values(self):
        """Test custom initialization values."""
        limiter = RateLimiter(
            max_retries=10,
            base_delay=0.5,
            max_delay=120.0,
            exponential_base=3.0,
        )
        
        assert limiter.max_retries == 10
        assert limiter.base_delay == 0.5
        assert limiter.max_delay == 120.0
        assert limiter.exponential_base == 3.0


class TestDelayCalculation:
    """Tests for exponential backoff delay calculation."""
    
    def test_first_attempt_delay(self):
        """Test delay for first retry attempt (attempt 0)."""
        limiter = RateLimiter(base_delay=1.0, max_delay=60.0, exponential_base=2.0)
        
        # Calculate multiple times to account for jitter
        delays = [limiter._calculate_delay(0) for _ in range(10)]
        
        # Base delay is 1.0 * (2.0 ** 0) = 1.0
        # With ±25% jitter, range is 0.75 to 1.25
        for delay in delays:
            assert 0.75 <= delay <= 1.25
    
    def test_exponential_growth(self):
        """Test that delay grows exponentially."""
        limiter = RateLimiter(base_delay=1.0, max_delay=60.0, exponential_base=2.0)
        
        # Expected delays without jitter: 1, 2, 4, 8, 16, 32
        expected_base = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0]
        
        for attempt, expected in enumerate(expected_base):
            delay = limiter._calculate_delay(attempt)
            # Allow ±25% jitter
            min_delay = expected * 0.75
            max_delay = expected * 1.25
            assert min_delay <= delay <= max_delay, f"Attempt {attempt}: {delay} not in [{min_delay}, {max_delay}]"
    
    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay."""
        limiter = RateLimiter(base_delay=1.0, max_delay=10.0, exponential_base=2.0)
        
        # At attempt 5, base delay would be 32, but capped at 10
        delay = limiter._calculate_delay(5)
        
        # With ±25% jitter on max_delay (10), range is 7.5 to 12.5
        assert delay <= 12.5, f"Delay {delay} exceeds max_delay with jitter"
    
    def test_jitter_randomness(self):
        """Test that jitter provides variation."""
        limiter = RateLimiter(base_delay=1.0, max_delay=60.0, exponential_base=2.0)
        
        # Calculate many delays for same attempt
        delays = [limiter._calculate_delay(2) for _ in range(50)]
        
        # Should have some variation due to jitter
        assert len(set(delays)) > 1, "Jitter should produce different delays"


class TestExecuteWithRetry:
    """Tests for synchronous retry execution."""
    
    def test_success_on_first_try(self):
        """Test successful execution without need for retries."""
        limiter = RateLimiter()
        func = Mock(return_value="success")
        
        result = limiter.execute_with_retry(func)
        
        assert result == "success"
        assert func.call_count == 1
    
    def test_success_after_retries(self):
        """Test successful execution after some retries."""
        limiter = RateLimiter(base_delay=0.01, max_retries=3)
        
        # Fail twice, then succeed
        call_count = {"count": 0}
        def flaky_func():
            call_count["count"] += 1
            if call_count["count"] < 3:
                error = Mock()
                error.response = Mock()
                error.response.status_code = 429
                error.response.headers = {}
                raise type("HTTPError", (Exception,), {"response": error.response})()
            return "success"
        
        result = limiter.execute_with_retry(flaky_func)
        
        assert result == "success"
        assert call_count["count"] == 3
    
    def test_max_retries_exceeded(self):
        """Test that exception is raised after max retries."""
        limiter = RateLimiter(base_delay=0.01, max_retries=2)
        
        def always_fail():
            error = Mock()
            error.response = Mock()
            error.response.status_code = 429
            error.response.headers = {}
            raise type("HTTPError", (Exception,), {"response": error.response})()
        
        with pytest.raises(Exception):
            limiter.execute_with_retry(always_fail)
    
    def test_rate_limit_error_retry(self):
        """Test retry on RateLimitError exception."""
        limiter = RateLimiter(base_delay=0.01, max_retries=3)
        
        call_count = {"count": 0}
        def rate_limited_func():
            call_count["count"] += 1
            if call_count["count"] < 2:
                raise RateLimitError("Rate limited", retry_after=1)
            return "success"
        
        result = limiter.execute_with_retry(rate_limited_func)
        
        assert result == "success"
        assert call_count["count"] == 2
    
    def test_retry_after_header_respected(self):
        """Test that Retry-After header value is used for delay."""
        limiter = RateLimiter(base_delay=10.0, max_retries=2)  # High base delay
        
        call_count = {"count": 0}
        start_time = time.time()
        
        def rate_limited_with_header():
            call_count["count"] += 1
            if call_count["count"] < 2:
                error = Mock()
                error.response = Mock()
                error.response.status_code = 429
                error.response.headers = {"Retry-After": "0"}  # 0 seconds
                raise type("HTTPError", (Exception,), {"response": error.response})()
            return "success"
        
        result = limiter.execute_with_retry(rate_limited_with_header)
        elapsed = time.time() - start_time
        
        assert result == "success"
        # Should use Retry-After (0s) instead of base_delay (10s)
        assert elapsed < 5.0, f"Should have used Retry-After header, took {elapsed}s"
    
    def test_non_retryable_error_raised_immediately(self):
        """Test that non-rate-limit errors are raised immediately."""
        limiter = RateLimiter(max_retries=5)
        
        call_count = {"count": 0}
        def auth_error_func():
            call_count["count"] += 1
            raise ValueError("Not a rate limit error")
        
        with pytest.raises(ValueError):
            limiter.execute_with_retry(auth_error_func)
        
        # Should only be called once (no retries)
        assert call_count["count"] == 1
    
    def test_503_service_unavailable_retry(self):
        """Test retry on 503 Service Unavailable."""
        limiter = RateLimiter(base_delay=0.01, max_retries=3)
        
        call_count = {"count": 0}
        def unavailable_func():
            call_count["count"] += 1
            if call_count["count"] < 2:
                error = Mock()
                error.response = Mock()
                error.response.status_code = 503
                error.response.headers = {}
                raise type("HTTPError", (Exception,), {"response": error.response})()
            return "success"
        
        result = limiter.execute_with_retry(unavailable_func)
        
        assert result == "success"
        assert call_count["count"] == 2
    
    def test_custom_retry_on_codes(self):
        """Test that custom retry_on status codes are respected."""
        limiter = RateLimiter(base_delay=0.01, max_retries=3)
        
        call_count = {"count": 0}
        def conflict_func():
            call_count["count"] += 1
            if call_count["count"] < 2:
                error = Mock()
                error.response = Mock()
                error.response.status_code = 409  # Conflict
                error.response.headers = {}
                raise type("HTTPError", (Exception,), {"response": error.response})()
            return "success"
        
        # Should NOT retry on 409 by default
        with pytest.raises(Exception):
            limiter.execute_with_retry(conflict_func)
        
        # Reset and try with 409 in retry_on
        call_count["count"] = 0
        result = limiter.execute_with_retry(conflict_func, retry_on=(429, 503, 409))
        
        assert result == "success"


class TestExecuteWithRetryAsync:
    """Tests for asynchronous retry execution."""
    
    @pytest.mark.asyncio
    async def test_async_success_on_first_try(self):
        """Test successful async execution without retries."""
        limiter = RateLimiter()
        func = AsyncMock(return_value="success")
        
        result = await limiter.execute_with_retry_async(func)
        
        assert result == "success"
        assert func.call_count == 1
    
    @pytest.mark.asyncio
    async def test_async_success_after_retries(self):
        """Test successful async execution after retries."""
        limiter = RateLimiter(base_delay=0.01, max_retries=3)
        
        call_count = {"count": 0}
        async def flaky_async():
            call_count["count"] += 1
            if call_count["count"] < 3:
                raise RateLimitError("Rate limited")
            return "success"
        
        result = await limiter.execute_with_retry_async(flaky_async)
        
        assert result == "success"
        assert call_count["count"] == 3
    
    @pytest.mark.asyncio
    async def test_async_max_retries_exceeded(self):
        """Test that exception is raised after max retries in async."""
        limiter = RateLimiter(base_delay=0.01, max_retries=2)
        
        async def always_fail():
            raise RateLimitError("Rate limited")
        
        with pytest.raises(RateLimitError):
            await limiter.execute_with_retry_async(always_fail)
    
    @pytest.mark.asyncio
    async def test_async_httpx_error_retry(self):
        """Test retry on httpx.HTTPStatusError (429)."""
        limiter = RateLimiter(base_delay=0.01, max_retries=3)
        
        call_count = {"count": 0}
        async def httpx_rate_limited():
            call_count["count"] += 1
            if call_count["count"] < 2:
                # Create an httpx HTTPStatusError
                request = httpx.Request("GET", "https://example.com")
                response = httpx.Response(429, headers={"Retry-After": "0"}, request=request)
                raise httpx.HTTPStatusError("Rate limited", request=request, response=response)
            return "success"
        
        result = await limiter.execute_with_retry_async(httpx_rate_limited)
        
        assert result == "success"
        assert call_count["count"] == 2


class TestRateLimitErrorIntegration:
    """Tests for integration with RateLimitError exception."""
    
    def test_retry_after_from_rate_limit_error(self):
        """Test that retry_after from RateLimitError is used."""
        limiter = RateLimiter(base_delay=10.0, max_retries=2)
        
        call_count = {"count": 0}
        
        def rate_limited():
            call_count["count"] += 1
            if call_count["count"] < 2:
                raise RateLimitError("Rate limited", retry_after=0)  # 0 seconds
            return "success"
        
        start_time = time.time()
        result = limiter.execute_with_retry(rate_limited)
        elapsed = time.time() - start_time
        
        assert result == "success"
        # Should use retry_after (0s) instead of base_delay (10s)
        assert elapsed < 5.0
    
    def test_rate_limit_error_without_retry_after(self):
        """Test RateLimitError without retry_after uses exponential backoff."""
        limiter = RateLimiter(base_delay=0.05, max_retries=2, exponential_base=2.0)
        
        call_count = {"count": 0}
        
        def rate_limited():
            call_count["count"] += 1
            if call_count["count"] < 2:
                raise RateLimitError("Rate limited")  # No retry_after
            return "success"
        
        result = limiter.execute_with_retry(rate_limited)
        
        assert result == "success"
