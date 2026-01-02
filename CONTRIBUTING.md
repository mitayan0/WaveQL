# Contributing to WaveQL

Thank you for your interest in contributing to WaveQL! This document provides guidelines and information for contributors.

## Getting Started

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/waveql.git
   cd waveql
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -e ".[dev]"
   # Or manually:
   pip install -e .
   pip install pytest pytest-asyncio responses respx black isort mypy
   ```

4. **Run tests**
   ```bash
   pytest tests/ -v
   ```

## How to Contribute

### Reporting Bugs

- Check if the bug has already been reported in [Issues](https://github.com/yourusername/waveql/issues)
- If not, create a new issue with:
  - Clear title and description
  - Steps to reproduce
  - Expected vs actual behavior
  - Python version and OS

### Suggesting Features

- Open an issue with the `enhancement` label
- Describe the use case and expected behavior
- Discuss before implementing large features

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Write tests** for new functionality
3. **Follow code style** (see below)
4. **Update documentation** if needed
5. **Submit the PR** with a clear description

## Code Style

### Python Style

- Follow [PEP 8](https://pep8.org/)
- Use `black` for formatting: `black waveql/`
- Use `isort` for imports: `isort waveql/`
- Maximum line length: 100 characters

### Type Hints

- Use type hints for function signatures
- Run `mypy waveql/` to check types

### Docstrings

Use Google-style docstrings:

```python
def fetch(self, table: str, columns: List[str] = None) -> pa.Table:
    """
    Fetch data from the source.
    
    Args:
        table: Table name to query
        columns: Optional list of columns to select
        
    Returns:
        Arrow table with the results
        
    Raises:
        AdapterError: If the request fails
    """
```

## Adding a New Adapter

1. **Create the adapter file**: `waveql/adapters/your_adapter.py`

2. **Inherit from BaseAdapter**:
   ```python
   from waveql.adapters.base import BaseAdapter
   
   class YourAdapter(BaseAdapter):
       adapter_name = "your_adapter"
       
       def fetch(self, table, columns=None, predicates=None, ...):
           # Implementation
           pass
       
       def get_schema(self, table):
           # Implementation
           pass
   ```

3. **Register the adapter** in `waveql/adapters/registry.py`:
   ```python
   try:
       from waveql.adapters.your_adapter import YourAdapter
       register_adapter("your_adapter", YourAdapter)
   except ImportError:
       pass
   ```

4. **Add tests** in `tests/test_your_adapter.py`

5. **Update documentation**

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_servicenow_adapter.py -v

# Run with coverage
pytest tests/ --cov=waveql --cov-report=html
```

### Writing Tests

- Use `responses` library for mocking HTTP requests
- Test both success and error cases
- Test predicate pushdown conversion

```python
import responses
import pytest

@responses.activate
def test_fetch_data(adapter):
    responses.add(
        responses.GET,
        "https://api.example.com/data",
        json={"results": [{"id": 1}]},
        status=200,
    )
    
    result = adapter.fetch("data")
    assert len(result) == 1
```

## Project Structure

```
waveql/
├── __init__.py           # Package exports
├── connection.py         # WaveQLConnection
├── cursor.py             # WaveQLCursor
├── async_cursor.py       # AsyncCursor
├── query_planner.py      # SQL parsing
├── schema_cache.py       # Schema caching
├── exceptions.py         # Custom exceptions
├── adapters/
│   ├── base.py           # BaseAdapter class
│   ├── servicenow.py
│   ├── salesforce.py
│   ├── jira.py
│   ├── rest_adapter.py
│   ├── file_adapter.py
│   └── registry.py
├── auth/
│   └── manager.py        # AuthManager
├── utils/
│   ├── connection_pool.py
│   ├── rate_limiter.py
│   └── streaming.py
└── sqlalchemy/
    └── dialect.py
```

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create a git tag: `git tag v1.0.0`
4. Push: `git push origin main --tags`
5. Build: `python -m build`
6. Upload: `twine upload dist/*`

## Code of Conduct

Be respectful, inclusive, and constructive. We're all here to build something great together.

## Questions?

- Open an issue with the `question` label
- Join our discussions

Thank you for contributing! 🎉
