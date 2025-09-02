
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
