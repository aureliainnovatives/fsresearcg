#!/bin/bash
# scripts/fix_hms.sh
# Automates fixing the Hive Metastore missing S3 driver issue

echo "--- Fixing Hive Metastore Dependencies ---"

# Ensure containers are running
if ! docker compose -f infra/docker-compose.yml ps | grep -q "spark"; then
    echo "Spark container not running. Starting..."
    docker compose -f infra/docker-compose.yml up -d spark
    sleep 5
fi

# 1. Copy Jars from Spark (which has them) to Hive (which needs them)
echo "Copying hadoop-aws-3.3.4.jar..."
docker compose -f infra/docker-compose.yml cp \
    spark:/root/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar \
    hive-metastore:/opt/hive/lib/hadoop-aws-3.3.4.jar

echo "Copying aws-java-sdk-bundle-1.12.540.jar..."
docker compose -f infra/docker-compose.yml cp \
    spark:/root/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.540.jar \
    hive-metastore:/opt/hive/lib/aws-java-sdk-bundle-1.12.540.jar

# 2. Restart Hive Metastore to load new jars
echo "Restarting Hive Metastore..."
docker compose -f infra/docker-compose.yml restart hive-metastore

echo "--- Done! Hive Metastore should now support S3A ---"
