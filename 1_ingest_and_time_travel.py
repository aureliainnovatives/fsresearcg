import os
from pyspark.sql import SparkSession

# ---- Config ----
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
WAREHOUSE = "s3a://lakehouse/warehouse"   # bucket must exist

# Choose Spark version to match the Iceberg runtime jar you’ll pass via --packages
ICEBERG_CATALOG_NAME = "local"            # our Spark/Iceberg catalog name
DB = "demo"
TABLE = "customers"
CSV_PATH = "data/customers.csv"

# ---- Spark session with S3A + Iceberg (Hadoop catalog) ----
spark = (
    SparkSession.builder.appName("mini-lakehouse")
    # S3A to talk to MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg catalog (HadoopCatalog → metadata stored under the warehouse path)
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type", "hadoop")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", WAREHOUSE)
    .getOrCreate()
)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.{DB}")

# 1) Load CSV → DataFrame (infer schema for demo)
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(CSV_PATH))

# 2) Write to an Iceberg table (create fresh)
full_table = f"{ICEBERG_CATALOG_NAME}.{DB}.{TABLE}"
spark.sql(f"DROP TABLE IF EXISTS {full_table}")
(df.write
   .format("iceberg")
   .mode("overwrite")
   .saveAsTable(full_table))

print("\n== After initial load ==")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_table}").show(truncate=False)

# 3) Update: add a few more rows (simulate a new batch)
'''new_rows = spark.createDataFrame(
    [
        (5, "Nina", "Kulkarni", "UK", "2025-02-20", 230.75),
        (6, "Vikram", "Iyer", "IN", "2025-03-15", 999.00),
    ],
    df.schema
)'''
from datetime import datetime

new_rows = spark.createDataFrame(
    [
        (5, "Nina", "Kulkarni", "UK", datetime.strptime("2025-02-20", "%Y-%m-%d").date(), 230.75),
        (6, "Vikram", "Iyer", "IN", datetime.strptime("2025-03-15", "%Y-%m-%d").date(), 999.00),
    ],
    df.schema
)


new_rows.write.format("iceberg").mode("append").saveAsTable(full_table)

print("\n== After append ==")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_table}").show(truncate=False)

# 4) List snapshots (history) to get snapshot_ids
print("\n== Snapshots (history) ==")
spark.sql(f"SELECT committed_at, snapshot_id, operation FROM {full_table}.snapshots").show(truncate=False)

# Grab the earliest snapshot_id (before append) for time-travel
snapshots = spark.sql(f"SELECT snapshot_id, committed_at FROM {full_table}.snapshots ORDER BY committed_at").collect()
first_snapshot_id = snapshots[0]["snapshot_id"]

# 5) Time-travel read: load the table as of first snapshot (pre-append)
print("\n== Time-travel to first snapshot (pre-append) ==")
'''df_old = (spark.read
          .format("iceberg")
          .option("snapshot-id", str(first_snapshot_id))
          .load(full_table.replace(".", "/")))  # load() uses 'catalog/db/table' path
'''
df_old = (
    spark.read
    .format("iceberg")
    .option("snapshot-id", str(first_snapshot_id))
    .table(full_table)  # ✅ use Spark table reference instead
)


df_old.show(truncate=False)
print(f"Old row count: {df_old.count()}")

spark.stop()