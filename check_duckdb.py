import duckdb
print(f"DuckDB version: {duckdb.__version__}")
try:
    import _duckdb
    print(f"_duckdb file: {_duckdb.__file__}")
except ImportError as e:
    print(f"Cannot import _duckdb: {e}")
