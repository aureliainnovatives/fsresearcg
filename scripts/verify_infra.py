
import requests
import sys

def check_service(name, url):
    print(f"Checking {name} at {url} ...", end=" ")
    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            print("OK")
            return True
        else:
            print(f"FAILED (Status {resp.status_code})")
            return False
    except Exception as e:
        print(f"FAILED (Error: {e})")
        return False

def run_checks():
    success = True
    
    # Check API Gateway
    if not check_service("API Gateway", "http://localhost:8000/health"):
        success = False
        
    # Check MinIO (Console)
    if not check_service("MinIO Console", "http://localhost:9001"):
        success = False
        
    # Check Trino
    if not check_service("Trino", "http://localhost:8081/ui/login.html"):
        # Also try info endpoint
        if not check_service("Trino Info", "http://localhost:8081/v1/info"):
             success = False
    
    # Check Spark Master UI - Skipped (Running as driver-only container)
    # if not check_service("Spark Master UI", "http://localhost:8080"):
    #    success = False
    
    # Check Spark History UI
    check_service("Spark History Server", "http://localhost:18080")

    if success:
        print("\nAll infrastructure components are reachable.")
    else:
        print("\nSome infrastructure components are NOT reachable.")
        sys.exit(1)

if __name__ == "__main__":
    run_checks()
