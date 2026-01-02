"""WaveQL Exceptions"""


class WaveQLError(Exception):
    """Base exception for WaveQL"""
    pass


class ConnectionError(WaveQLError):
    """Failed to establish connection"""
    pass


class AuthenticationError(WaveQLError):
    """Authentication failed"""
    pass


class QueryError(WaveQLError):
    """Query execution failed"""
    pass


class AdapterError(WaveQLError):
    """Adapter-related error"""
    pass


class SchemaError(WaveQLError):
    """Schema discovery or validation error"""
    pass


class RateLimitError(WaveQLError):
    """API rate limit exceeded"""
    def __init__(self, message: str, retry_after: int = None):
        super().__init__(message)
        self.retry_after = retry_after


class PredicatePushdownError(WaveQLError):
    """Failed to push predicate to API"""
    pass
