
import os
from pyspark.sql import SparkSession
from datetime import date

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")

ICEBERG_CATALOG_NAME = "iceberg"   # matches Trino catalog name
DB = "demo"
TABLE = "customers"
CSV_PATH = "/app/jobs/data/customers.csv"

spark = (
    SparkSession.builder.appName("mini-lakehouse")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type", "hadoop")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", WAREHOUSE)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.{DB}")

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(CSV_PATH))

full_table = f"{ICEBERG_CATALOG_NAME}.{DB}.{TABLE}"
spark.sql(f"DROP TABLE IF EXISTS {full_table}")
(df.write
   .format("iceberg")
   .mode("overwrite")
   .saveAsTable(full_table))

print("== After initial load ==")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_table}").show()

new_rows = spark.createDataFrame(
    [
        (5, "Nina", "Kulkarni", "UK", date(2025, 2, 20), 230.75),
        (6, "Vikram", "Iyer", "IN", date(2025, 3, 15), 999.00),
    ],
    df.schema
)

new_rows.write.format("iceberg").mode("append").saveAsTable(full_table)

print("== After append ==")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_table}").show()

print("== Snapshots (history) ==")
spark.sql(f"SELECT committed_at, snapshot_id, operation FROM {full_table}.snapshots").show(truncate=False)

snapshots = spark.sql(f"SELECT snapshot_id, committed_at FROM {full_table}.snapshots ORDER BY committed_at").collect()
first_snapshot_id = snapshots[0]["snapshot_id"]

print("== Time-travel to first snapshot (pre-append) ==")
df_old = (
    spark.read
         .format("iceberg")
         .option("snapshot-id", int(first_snapshot_id))  # int or str both fine
         .load(f"{ICEBERG_CATALOG_NAME}.{DB}.{TABLE}")   # <-- keep dot notation
)
df_old.show(truncate=False)
print(f"Old row count: {df_old.count()}")


# optional SQL time travel
snap_id = int(first_snapshot_id)
old_count = spark.sql(
    f"SELECT COUNT(*) AS c FROM {full_table} VERSION AS OF {snap_id}"
).collect()[0]["c"]
print(f"Old row count (SQL VERSION AS OF): {old_count}")

spark.stop()
