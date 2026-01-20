from pyspark.sql import SparkSession
import os

print("Starting S3A Write Test...")

spark = SparkSession.builder \
    .appName("S3 Write Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

data = [("test_row", 1)]
df = spark.createDataFrame(data, ["col1", "col2"])

# Write simple paquet file
path = "s3a://warehouse/debug_test"
print(f"Writing dataframe to {path}...")
try:
    df.write.mode("overwrite").parquet(path)
    print("Write SUCCESS.")
except Exception as e:
    print(f"Write FAILED: {e}")

spark.stop()
