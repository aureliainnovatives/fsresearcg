# /app/jobs/ingest.py
import os, argparse
from datetime import datetime
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
WAREHOUSE_ROOT = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")

parser = argparse.ArgumentParser()
parser.add_argument("--instance-id", required=True)
parser.add_argument("--table", required=True)
parser.add_argument("--object-key", required=True)   # e.g., uploads/123.csv
parser.add_argument("--format", default="csv", choices=["csv","parquet"])
parser.add_argument("--mode", default="overwrite", choices=["overwrite","append"])
parser.add_argument("--header", default="true")
parser.add_argument("--infer-schema", default="true")
args = parser.parse_args()

warehouse = f"{WAREHOUSE_ROOT}/{args.instance_id}"
catalog_name = "iceberg"
db = args.instance_id
full_table = f"{catalog_name}.{db}.{args.table}"

spark = (
    SparkSession.builder.appName(f"ingest-{args.instance_id}-{args.table}")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_name}.type", "hive")
    .config(f"spark.sql.catalog.{catalog_name}.uri", "thrift://hive-metastore:9083")
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{db}")

WAREHOUSE_BUCKET = os.getenv("S3_BUCKET_WAREHOUSE", "warehouse")
src_uri = f"s3a://{WAREHOUSE_BUCKET}/{args.object_key}"
reader = spark.read
if args.format == "csv":
    reader = (reader
        .option("header", args.header)
        .option("inferSchema", args.infer_schema))
df = reader.format(args.format).load(src_uri)

(df.write
   .format("iceberg")
   .mode(args.mode)
   .saveAsTable(full_table))

print(f"[OK] Ingested {src_uri} -> {full_table} ({args.mode}) at {datetime.utcnow().isoformat()}Z")
spark.stop()
