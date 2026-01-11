"""
Tests for Exceptions Module - Rich, developer-friendly error classes

Tests cover:
- All exception types instantiation
- Error code assignment
- Message formatting with context and suggestions
- Serialization to dict
- Status code suggestions for AdapterError
- RateLimitError retry_after handling
- ContractViolationError violations tracking
"""

import pytest

from waveql.exceptions import (
    WaveQLError,
    ConnectionError,
    AuthenticationError,
    QueryError,
    AdapterError,
    SchemaError,
    RateLimitError,
    PredicatePushdownError,
    ConfigurationError,
    TimeoutError,
    SchemaEvolutionError,
    ContractViolationError,
)


class TestWaveQLError:
    """Tests for base WaveQLError."""
    
    def test_basic_instantiation(self):
        """Test creating a basic error."""
        error = WaveQLError("Something went wrong")
        
        assert "Something went wrong" in str(error)
        assert error.message == "Something went wrong"
    
    def test_error_code(self):
        """Test default error code."""
        error = WaveQLError("Test error")
        
        assert error.error_code == "E000"
        assert "[E000]" in str(error)
    
    def test_with_suggestion(self):
        """Test error with suggestion."""
        error = WaveQLError(
            "Something went wrong",
            suggestion="Try restarting the connection"
        )
        
        assert "Try restarting" in str(error)
        assert error.suggestion == "Try restarting the connection"
    
    def test_with_context(self):
        """Test error with context."""
        error = WaveQLError(
            "API request failed",
            context={"adapter": "servicenow", "table": "incident"}
        )
        
        assert "adapter=servicenow" in str(error)
        assert "table=incident" in str(error)
    
    def test_to_dict(self):
        """Test serialization to dictionary."""
        error = WaveQLError(
            "Test error",
            suggestion="Fix it",
            context={"key": "value"}
        )
        
        d = error.to_dict()
        
        assert d["error_code"] == "E000"
        assert d["message"] == "Test error"
        assert d["suggestion"] == "Fix it"
        assert d["context"] == {"key": "value"}


class TestConnectionError:
    """Tests for ConnectionError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = ConnectionError("Connection failed")
        
        assert error.error_code == "E001"
    
    def test_with_host(self):
        """Test with host parameter."""
        error = ConnectionError("Connection failed", host="api.example.com")
        
        assert "api.example.com" in str(error)
        assert error.context["host"] == "api.example.com"
    
    def test_default_suggestion(self):
        """Test default suggestion includes host."""
        error = ConnectionError("Connection failed", host="api.example.com")
        
        assert "api.example.com" in error.suggestion


class TestAuthenticationError:
    """Tests for AuthenticationError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = AuthenticationError("Invalid credentials")
        
        assert error.error_code == "E002"
    
    def test_with_adapter(self):
        """Test with adapter parameter."""
        error = AuthenticationError("Invalid credentials", adapter="servicenow")
        
        assert error.context["adapter"] == "servicenow"
    
    def test_default_suggestion(self):
        """Test default suggestion mentions credentials."""
        error = AuthenticationError("Invalid credentials")
        
        assert "credentials" in error.suggestion.lower()


class TestQueryError:
    """Tests for QueryError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = QueryError("Syntax error")
        
        assert error.error_code == "E003"
    
    def test_with_sql(self):
        """Test with SQL parameter."""
        error = QueryError("Syntax error", sql="SELECT * FROM users")
        
        assert "SELECT * FROM users" in error.context["sql"]
    
    def test_sql_truncation(self):
        """Test that long SQL is truncated."""
        long_sql = "SELECT " + "a, " * 200 + "b FROM users"
        error = QueryError("Syntax error", sql=long_sql)
        
        assert len(error.context["sql"]) <= 203  # 200 chars + "..."


class TestAdapterError:
    """Tests for AdapterError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = AdapterError("API request failed")
        
        assert error.error_code == "E004"
    
    def test_with_all_params(self):
        """Test with all parameters."""
        error = AdapterError(
            "API request failed",
            adapter="servicenow",
            url="https://api.example.com/table/incident",
            status_code=404
        )
        
        assert error.context["adapter"] == "servicenow"
        assert error.context["url"] == "https://api.example.com/table/incident"
        assert error.context["status_code"] == 404
    
    def test_suggestion_for_400(self):
        """Test suggestion for 400 Bad Request."""
        error = AdapterError("Request failed", status_code=400)
        
        assert "malformed" in error.suggestion.lower() or "syntax" in error.suggestion.lower()
    
    def test_suggestion_for_401(self):
        """Test suggestion for 401 Unauthorized."""
        error = AdapterError("Request failed", status_code=401)
        
        assert "authentication" in error.suggestion.lower()
    
    def test_suggestion_for_403(self):
        """Test suggestion for 403 Forbidden."""
        error = AdapterError("Request failed", status_code=403)
        
        assert "permission" in error.suggestion.lower() or "access" in error.suggestion.lower()
    
    def test_suggestion_for_404(self):
        """Test suggestion for 404 Not Found."""
        error = AdapterError("Request failed", status_code=404)
        
        assert "not found" in error.suggestion.lower() or "exists" in error.suggestion.lower()
    
    def test_suggestion_for_429(self):
        """Test suggestion for 429 Too Many Requests."""
        error = AdapterError("Request failed", status_code=429)
        
        assert "rate limit" in error.suggestion.lower()
    
    def test_suggestion_for_500(self):
        """Test suggestion for 500 Internal Server Error."""
        error = AdapterError("Request failed", status_code=500)
        
        assert "retry" in error.suggestion.lower() or "temporary" in error.suggestion.lower()
    
    def test_suggestion_for_503(self):
        """Test suggestion for 503 Service Unavailable."""
        error = AdapterError("Request failed", status_code=503)
        
        assert "unavailable" in error.suggestion.lower() or "maintenance" in error.suggestion.lower()


class TestSchemaError:
    """Tests for SchemaError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = SchemaError("Schema discovery failed")
        
        assert error.error_code == "E005"
    
    def test_with_table(self):
        """Test with table parameter."""
        error = SchemaError("Schema discovery failed", table="incident")
        
        assert error.context["table"] == "incident"
        assert "incident" in error.suggestion


class TestRateLimitError:
    """Tests for RateLimitError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = RateLimitError("Rate limited")
        
        assert error.error_code == "E006"
    
    def test_retry_after(self):
        """Test retry_after attribute."""
        error = RateLimitError("Rate limited", retry_after=30)
        
        assert error.retry_after == 30
        assert error.context["retry_after_seconds"] == 30
    
    def test_suggestion_with_retry_after(self):
        """Test suggestion includes retry time."""
        error = RateLimitError("Rate limited", retry_after=30)
        
        assert "30" in error.suggestion
    
    def test_suggestion_without_retry_after(self):
        """Test default suggestion when no retry_after."""
        error = RateLimitError("Rate limited")
        
        assert "backoff" in error.suggestion.lower() or "frequency" in error.suggestion.lower()
    
    def test_with_adapter(self):
        """Test with adapter parameter."""
        error = RateLimitError("Rate limited", adapter="servicenow")
        
        assert error.context["adapter"] == "servicenow"


class TestPredicatePushdownError:
    """Tests for PredicatePushdownError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = PredicatePushdownError("Pushdown failed")
        
        assert error.error_code == "E007"
    
    def test_with_predicate_and_adapter(self):
        """Test with predicate and adapter parameters."""
        error = PredicatePushdownError(
            "Cannot push LIKE predicate",
            predicate="name LIKE '%test%'",
            adapter="servicenow"
        )
        
        assert error.context["predicate"] == "name LIKE '%test%'"
        assert error.context["adapter"] == "servicenow"
        assert "client-side" in error.suggestion.lower()


class TestConfigurationError:
    """Tests for ConfigurationError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = ConfigurationError("Invalid config")
        
        assert error.error_code == "E008"
    
    def test_with_setting(self):
        """Test with setting parameter."""
        error = ConfigurationError("Missing setting", setting="api_key")
        
        assert error.context["setting"] == "api_key"
        assert "api_key" in error.suggestion


class TestTimeoutError:
    """Tests for TimeoutError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = TimeoutError("Operation timed out")
        
        assert error.error_code == "E009"
    
    def test_with_timeout_seconds(self):
        """Test with timeout_seconds parameter."""
        error = TimeoutError("Operation timed out", timeout_seconds=30)
        
        assert error.context["timeout_seconds"] == 30
        assert "30" in error.suggestion


class TestSchemaEvolutionError:
    """Tests for SchemaEvolutionError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = SchemaEvolutionError("Schema changed")
        
        assert error.error_code == "E010"
    
    def test_with_changes_and_table(self):
        """Test with changes and table parameters."""
        changes = [
            {"type": "column_removed", "column": "old_column"},
            {"type": "type_changed", "column": "status", "old": "int", "new": "string"},
        ]
        error = SchemaEvolutionError("Schema changed", changes=changes, table="incident")
        
        assert error.context["table"] == "incident"
        assert "changes" in error.context


class TestContractViolationError:
    """Tests for ContractViolationError."""
    
    def test_error_code(self):
        """Test correct error code."""
        error = ContractViolationError("Contract violated")
        
        assert error.error_code == "E011"
    
    def test_with_violations(self):
        """Test with violations list."""
        violations = [
            {"column": "id", "expected": "integer", "actual": "string"},
            {"column": "status", "error": "missing_column"},
        ]
        error = ContractViolationError("Contract violated", violations=violations)
        
        assert error.violations == violations
        assert error.context["violation_count"] == 2
    
    def test_with_table_and_adapter(self):
        """Test with table and adapter parameters."""
        error = ContractViolationError(
            "Contract violated",
            table="incident",
            adapter="servicenow"
        )
        
        assert error.context["table"] == "incident"
        assert error.context["adapter"] == "servicenow"
    
    def test_empty_violations(self):
        """Test with no violations."""
        error = ContractViolationError("Contract violated")
        
        assert error.violations == []


class TestExceptionInheritance:
    """Tests for exception inheritance hierarchy."""
    
    def test_all_inherit_from_waveql_error(self):
        """Test that all exceptions inherit from WaveQLError."""
        exceptions = [
            ConnectionError,
            AuthenticationError,
            QueryError,
            AdapterError,
            SchemaError,
            RateLimitError,
            PredicatePushdownError,
            ConfigurationError,
            TimeoutError,
            SchemaEvolutionError,
            ContractViolationError,
        ]
        
        for exc_class in exceptions:
            assert issubclass(exc_class, WaveQLError)
    
    def test_all_inherit_from_exception(self):
        """Test that all exceptions can be caught as Exception."""
        exceptions = [
            WaveQLError("test"),
            ConnectionError("test"),
            AuthenticationError("test"),
            QueryError("test"),
            AdapterError("test"),
            SchemaError("test"),
            RateLimitError("test"),
            PredicatePushdownError("test"),
            ConfigurationError("test"),
            TimeoutError("test"),
            SchemaEvolutionError("test"),
            ContractViolationError("test"),
        ]
        
        for exc in exceptions:
            assert isinstance(exc, Exception)


class TestErrorCodeUniqueness:
    """Tests to ensure error codes are unique."""
    
    def test_unique_error_codes(self):
        """Test that all error codes are unique."""
        error_codes = {
            WaveQLError.error_code,
            ConnectionError.error_code,
            AuthenticationError.error_code,
            QueryError.error_code,
            AdapterError.error_code,
            SchemaError.error_code,
            RateLimitError.error_code,
            PredicatePushdownError.error_code,
            ConfigurationError.error_code,
            TimeoutError.error_code,
            SchemaEvolutionError.error_code,
            ContractViolationError.error_code,
        }
        
        # Should have 12 unique codes
        assert len(error_codes) == 12
