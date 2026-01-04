"""
WaveQL Exceptions - Rich, developer-friendly error classes.

All exceptions include:
- Descriptive error messages
- Error codes for programmatic handling
- Suggestions for fixing the issue
- Context information (adapter, URL, table, etc.)
"""

from typing import Optional, Dict, Any


class WaveQLError(Exception):
    """
    Base exception for WaveQL with rich error context.
    
    Attributes:
        message: Human-readable error description
        error_code: Machine-readable error identifier (e.g., "E001")
        suggestion: Helpful suggestion for resolving the error
        context: Additional context (adapter, url, table, etc.)
    """
    
    error_code: str = "E000"
    
    def __init__(
        self,
        message: str,
        suggestion: str = None,
        context: Dict[str, Any] = None,
        **kwargs
    ):
        self.message = message
        self.suggestion = suggestion
        self.context = context or {}
        
        # Build rich error message
        full_message = self._build_message()
        super().__init__(full_message)
    
    def _build_message(self) -> str:
        """Build a rich, formatted error message."""
        parts = [f"[{self.error_code}] {self.message}"]
        
        # Add context if present
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"  Context: {context_str}")
        
        # Add suggestion if present
        if self.suggestion:
            parts.append(f"  Suggestion: {self.suggestion}")
        
        return "\n".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to a dictionary for JSON serialization."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "suggestion": self.suggestion,
            "context": self.context,
        }


class ConnectionError(WaveQLError):
    """Failed to establish connection to the data source."""
    
    error_code = "E001"
    
    def __init__(self, message: str, host: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            f"Check that the host '{host}' is reachable and the URL is correct."
            if host else "Verify your connection settings."
        )
        context = kwargs.pop('context', {})
        if host:
            context['host'] = host
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class AuthenticationError(WaveQLError):
    """Authentication failed - invalid or expired credentials."""
    
    error_code = "E002"
    
    def __init__(self, message: str, adapter: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            "Check your credentials (username/password, API key, or OAuth tokens). "
            "Ensure they are not expired and have the required permissions."
        )
        context = kwargs.pop('context', {})
        if adapter:
            context['adapter'] = adapter
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class QueryError(WaveQLError):
    """Query execution failed - syntax error or unsupported operation."""
    
    error_code = "E003"
    
    def __init__(self, message: str, sql: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None)
        context = kwargs.pop('context', {})
        if sql:
            # Truncate long SQL for display
            context['sql'] = sql[:200] + "..." if len(sql) > 200 else sql
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class AdapterError(WaveQLError):
    """
    Adapter-related error - API request failed.
    
    Provides detailed information about what went wrong with the
    external API call.
    """
    
    error_code = "E004"
    
    def __init__(
        self,
        message: str,
        adapter: str = None,
        url: str = None,
        status_code: int = None,
        **kwargs
    ):
        context = kwargs.pop('context', {})
        if adapter:
            context['adapter'] = adapter
        if url:
            context['url'] = url
        if status_code:
            context['status_code'] = status_code
            
        # Generate suggestions based on status code
        suggestion = kwargs.pop('suggestion', None)
        if not suggestion and status_code:
            suggestion = self._suggest_for_status_code(status_code, adapter)
        
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)
    
    @staticmethod
    def _suggest_for_status_code(status_code: int, adapter: str = None) -> str:
        """Generate helpful suggestions based on HTTP status code."""
        adapter_name = adapter or "the API"
        
        suggestions = {
            400: f"Check your query syntax. The request to {adapter_name} was malformed.",
            401: f"Authentication required. Provide valid credentials for {adapter_name}.",
            403: f"Access denied. Your credentials don't have permission for this resource in {adapter_name}.",
            404: f"Resource not found. Verify the table/endpoint exists in {adapter_name}.",
            408: f"Request timed out. Try reducing the query scope or increasing the timeout.",
            429: f"Rate limit exceeded. Wait before retrying or reduce request frequency.",
            500: f"Internal server error in {adapter_name}. This is a temporary issue - retry later.",
            502: f"Bad gateway. {adapter_name} may be temporarily unavailable.",
            503: f"Service unavailable. {adapter_name} is down for maintenance.",
            504: f"Gateway timeout. {adapter_name} is slow to respond - try again later.",
        }
        
        return suggestions.get(status_code, f"HTTP {status_code} error from {adapter_name}.")


class SchemaError(WaveQLError):
    """Schema discovery or validation error."""
    
    error_code = "E005"
    
    def __init__(self, message: str, table: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            f"Try clearing the schema cache and re-querying table '{table}'."
            if table else "Clear the schema cache and retry."
        )
        context = kwargs.pop('context', {})
        if table:
            context['table'] = table
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class RateLimitError(WaveQLError):
    """
    API rate limit exceeded.
    
    Includes retry_after information for automatic backoff.
    """
    
    error_code = "E006"
    
    def __init__(self, message: str, retry_after: int = None, adapter: str = None, **kwargs):
        self.retry_after = retry_after
        
        suggestion = kwargs.pop('suggestion', None)
        if not suggestion:
            if retry_after:
                suggestion = f"Wait {retry_after} seconds before retrying."
            else:
                suggestion = "Reduce request frequency or implement exponential backoff."
        
        context = kwargs.pop('context', {})
        if retry_after:
            context['retry_after_seconds'] = retry_after
        if adapter:
            context['adapter'] = adapter
            
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class PredicatePushdownError(WaveQLError):
    """Failed to push predicate to the API - unsupported filter."""
    
    error_code = "E007"
    
    def __init__(self, message: str, predicate: str = None, adapter: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            f"The predicate '{predicate}' is not supported by {adapter}. "
            "The filter will be applied client-side instead."
            if predicate and adapter else
            "This filter type is not supported for pushdown. It will be applied client-side."
        )
        context = kwargs.pop('context', {})
        if predicate:
            context['predicate'] = predicate
        if adapter:
            context['adapter'] = adapter
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class ConfigurationError(WaveQLError):
    """Invalid or missing configuration."""
    
    error_code = "E008"
    
    def __init__(self, message: str, setting: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            f"Check the configuration setting '{setting}' in your WaveQL setup."
            if setting else "Review your WaveQL configuration."
        )
        context = kwargs.pop('context', {})
        if setting:
            context['setting'] = setting
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class TimeoutError(WaveQLError):
    """Operation timed out."""
    
    error_code = "E009"
    
    def __init__(self, message: str, timeout_seconds: int = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            f"The operation exceeded the {timeout_seconds}s timeout. "
            "Try increasing the timeout or reducing the query scope."
            if timeout_seconds else
            "Try increasing the timeout value or simplifying the query."
        )
        context = kwargs.pop('context', {})
        if timeout_seconds:
            context['timeout_seconds'] = timeout_seconds
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


class SchemaEvolutionError(WaveQLError):
    """Schema has changed in an incompatible way."""
    
    error_code = "E010"
    
    def __init__(self, message: str, changes: list = None, table: str = None, **kwargs):
        suggestion = kwargs.pop('suggestion', None) or (
            "Clear the schema cache to pick up the new schema, or update your query "
            "to handle the schema changes."
        )
        context = kwargs.pop('context', {})
        if table:
            context['table'] = table
        if changes:
            context['changes'] = str(changes)
        super().__init__(message, suggestion=suggestion, context=context, **kwargs)


# Export all exceptions
__all__ = [
    "WaveQLError",
    "ConnectionError",
    "AuthenticationError",
    "QueryError",
    "AdapterError",
    "SchemaError",
    "RateLimitError",
    "PredicatePushdownError",
    "ConfigurationError",
    "TimeoutError",
    "SchemaEvolutionError",
]
