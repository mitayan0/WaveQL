
import subprocess
import sys

def run_tests():
    cmd = [
        sys.executable, "-m", "pytest", 
        "tests/test_rest_adapter_extended.py", 
        "--cov=waveql.adapters.rest_adapter", 
        "--cov-report=term-missing"
    ]
    with open("output.log", "w") as f:
        try:
            print("Running tests...")
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, text=True, timeout=60)
            print(f"Tests finished with return code {result.returncode}")
        except subprocess.TimeoutExpired:
            f.write("\nTIMEOUT\n")
            print("Timeout")
        except Exception as e:
            f.write(f"\nERROR: {e}\n")
            print(f"Error: {e}")

if __name__ == "__main__":
    run_tests()
