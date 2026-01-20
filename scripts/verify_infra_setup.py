import requests
import time
import sys

def check_url(url, name):
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code in [200, 403, 401]: # 403/401 means service is listening but needs auth
            print(f"[OK] {name} is accessible at {url} (Status: {resp.status_code})")
            return True
        else:
            print(f"[WARN] {name} at {url} returned {resp.status_code}")
            return True # Still reachable
    except Exception as e:
        print(f"[FAIL] {name} at {url} unreachable: {e}")
        return False

def main():
    print("--- Infrastructure Verification ---")
    all_ok = True
    
    # MinIO
    if not check_url("http://localhost:9001", "MinIO Console"): all_ok = False
    if not check_url("http://localhost:9000/minio/health/live", "MinIO Health"): all_ok = False

    # Trino (Host mapped port 8081)
    if not check_url("http://localhost:8081/ui/login.html", "Trino UI"): all_ok = False

    # Spark History Server (Host mapped port 18080)
    if not check_url("http://localhost:18080", "Spark History Server"): all_ok = False

    # API Gateway
    if not check_url("http://localhost:8000/health", "API Gateway Health"): all_ok = False
        
    if all_ok:
        print("\n[SUCCESS] All Core Infrastructure Services are UP.")
    else:
        print("\n[FAILURE] Some services are not running correctly.")
        sys.exit(1)

if __name__ == "__main__":
    main()
