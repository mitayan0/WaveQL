
import subprocess
import sys

def run_tests():
    cmd = [
        sys.executable, "-m", "pytest", 
        "tests/test_rest_adapter_extended.py", 
        "--cov=waveql.adapters.rest_adapter", 
        "--cov-report=term-missing"
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        with open("test_output.txt", "w") as f:
            f.write(result.stdout)
            f.write("\nSTDERR:\n")
            f.write(result.stderr)
        print("Tests finished")
    except subprocess.TimeoutExpired:
        print("Tests timed out")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_tests()
