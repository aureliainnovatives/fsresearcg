
import os, time
import boto3
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
WAREHOUSE_BUCKET = os.getenv("S3_BUCKET_WAREHOUSE", "warehouse")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=S3_REGION,
)

router = APIRouter()

class PresignRequest(BaseModel):
    bucket: str | None = None
    key_prefix: str = "uploads/"
    expires: int = 3600

@router.post("/datasets/{name}/presign-upload")
def presign_upload(name: str, req: PresignRequest):
    bucket = req.bucket or WAREHOUSE_BUCKET
    key = f"{req.key_prefix}{name}-{int(time.time())}.csv"
    try:
        url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=req.expires,
        )
        return {"bucket": bucket, "key": key, "url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class IngestRequest(BaseModel):
    dataset: str
    uri: str
    format: str = "csv"
    mode: str = "append"

@router.post("/ingest/file")
def ingest_file(req: IngestRequest):
    import subprocess
    # Construct spark-submit command
    # We run 'docker exec spark ...' from within the API container (which has docker client installed)
    # The spark container name is 'spark' (from docker-compose)
    
    cmd = [
        "/usr/bin/docker", "exec", "spark",
        "/opt/spark/bin/spark-submit",
        "--jars", "/app/aws-java-sdk-bundle-1.12.540.jar",
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4",
        "--exclude-packages", "com.amazonaws:aws-java-sdk-bundle",
        "/app/jobs/ingest.py",
        "--instance-id", "default",  # Using 'default' as generic instance/db for now
        "--table", req.dataset,
        "--object-key", req.uri.replace("s3://warehouse/iceberg/default/", ""), # Extract key if full URI provided, or assume key
        "--format", req.format,
        "--mode", req.mode
    ]
    
    # Handle URI: if user passes full s3a://... or just the key
    # defined in ingest.py: src_uri = f"s3a://warehouse/iceberg/{args.instance_id}/{args.object_key}"
    # So we need to pass just the OBJECT KEY (e.g. "uploads/file.csv")
    # Helper to strip prefix if present
    clean_key = req.uri
    prefix = "s3://warehouse/iceberg/default/"
    if clean_key.startswith(prefix):
        clean_key = clean_key[len(prefix):]
    elif clean_key.startswith("s3a://"):
        # Legacy/alternate support
        clean_key = clean_key.replace("s3a://warehouse/iceberg/default/", "")
    elif clean_key.startswith("s3a://"):
        # Fallback if diff path
        pass 
        
    cmd[cmd.index("--object-key") + 1] = clean_key

    try:
        # Run synchronous for M1 (can be async task later)
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            raise HTTPException(status_code=500, detail=f"Ingest failed (RC={result.returncode}): STDOUT={result.stdout} STDERR={result.stderr}")
        
        return {"status": "success", "dataset": req.dataset, "output": result.stdout}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
