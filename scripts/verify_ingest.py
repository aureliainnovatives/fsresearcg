
import requests
import time
import os

API_URL = "http://localhost:8000"
DATASET_NAME = "test_ingest_ds"
FILE_CONTENT = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300"

def log(msg):
    print(msg)
    with open("verification.log", "a") as f:
        f.write(msg + "\n")

def run_verification():
    log(f"--- Starting Verification for Dataset: {DATASET_NAME} ---")

    # 1. Presign Upload
    log("1. Requesting Presigned URL...")
    resp = requests.post(f"{API_URL}/v1/datasets/{DATASET_NAME}/presign-upload", json={"expires": 3600})
    if resp.status_code != 200:
        log(f"FAILED: Presign failed {resp.text}")
        return
    data = resp.json()
    upload_url = data['url'].replace("http://minio:9000", "http://localhost:9000")
    key = data['key']
    log(f"   Success: Key={key}, URL={upload_url}")

    # 2. Upload File
    log("2. Uploading CSV content to MinIO...")
    upload_resp = requests.put(upload_url, data=FILE_CONTENT)
    if upload_resp.status_code != 200:
        log(f"FAILED: Upload failed {upload_resp.text}")
        return
    log("   Success: File uploaded.")

    # 3. Trigger Ingest
    log("3. Triggering Ingest...")
    ingest_payload = {
        "dataset": DATASET_NAME,
        "uri": key,  # API expects simple key or s3a://... our code handles both now
        "format": "csv",
        "mode": "overwrite"
    }
    ingest_resp = requests.post(f"{API_URL}/v1/ingest/file", json=ingest_payload)
    if ingest_resp.status_code != 200:
        log(f"FAILED: Ingest call failed {ingest_resp.text}")
        return
    log(f"   Success: {ingest_resp.json()}")

    # 4. Query Data (Optional, if SQL endpoint works)
    log("4. Verifying Data via SQL...")
    # Give it a moment? Spark is sync, so it should be there.
    sql_payload = {
        "sql": f"SELECT * FROM iceberg.default.{DATASET_NAME}"
    }
    sql_resp = requests.post(f"{API_URL}/v1/sql", json=sql_payload)
    if sql_resp.status_code != 200:
        log(f"WARNING: SQL Query failed {sql_resp.text}")
    else:
        log(f"   Success: Data = {sql_resp.json()}")

if __name__ == "__main__":
    try:
        run_verification()
    except Exception as e:
        print(f"ERROR: {e}")
