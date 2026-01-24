
import sys
import os

try:
    with open("debug_python.txt", "w") as f:
        f.write(f"Executable: {sys.executable}\n")
        f.write(f"CWD: {os.getcwd()}\n")
        
        # Check if conftest mocks are active (they won't be, because I'm running a script, not pytest)
        # But I can check if I can import duckdb normally
        try:
            import duckdb
            f.write("duckdb imported successfully\n")
        except Exception as e:
            f.write(f"duckdb import failed: {e}\n")
            
except Exception as e:
    print(f"Failed to write file: {e}")
