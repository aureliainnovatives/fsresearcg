# /app/jobs/crud_demo.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
WAREHOUSE_ROOT = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/iceberg")

def get_spark_session(app_name):
    return (SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE_ROOT)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4")
        .getOrCreate())

def run_crud_demo():
    spark = get_spark_session("crud-demo")
    spark.sparkContext.setLogLevel("WARN")
    
    table_name = "iceberg.default.customer_data"

    print("--- 1. Creating Table and Inserting Data ---")
    data = [(1, "Alice", "active"), (2, "Bob", "inactive"), (3, "Charlie", "active")]
    df = spark.createDataFrame(data, ["id", "name", "status"])

    # clean up previous run
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    df.writeTo(table_name).createOrReplace()
    spark.table(table_name).show()
    
    print("--- 2. Performing Upsert (MERGE INTO) ---")
    # Alice becomes inactive, Bob gets updated name, new user David
    upsert_data = [(1, "Alice", "inactive"), (2, "Bob Updated", "inactive"), (4, "David", "active")]
    upsert_df = spark.createDataFrame(upsert_data, ["id", "name", "status"])
    upsert_df.createOrReplaceTempView("updates")
    
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING updates u
        ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET t.name = u.name, t.status = u.status
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.table(table_name).sort("id").show()

    print("--- 3. Time Travel (History) ---")
    history_df = spark.sql(f"SELECT * FROM {table_name}.history")
    history_df.show(truncate=False)
    
    # Get the first snapshot ID
    first_snapshot_id = history_df.sort("made_current_at").first()["snapshot_id"]
    print(f"Reading from Snapshot ID: {first_snapshot_id}")
    
    spark.read \
        .option("snapshot-id", first_snapshot_id) \
        .table(table_name) \
        .show()

    spark.stop()

if __name__ == "__main__":
    run_crud_demo()
