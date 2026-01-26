
import pytest

# Note: DuckDB mocking has been removed. The previous mock was causing test failures
# because it replaced real DuckDB calls with MagicMock objects, making tests that
# depend on actual DuckDB operations fail. DuckDB is properly installed and working.

