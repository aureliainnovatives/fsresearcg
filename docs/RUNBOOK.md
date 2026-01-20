
# RUNBOOK — M1 (Local)

## Start core services
```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```

Services:
- MinIO: http://localhost:9001  (login from .env)
- Trino: http://localhost:8080  (no auth by default)
- API:   http://localhost:8000/health

## One-time MinIO setup
- Login → create buckets: `warehouse`, `ml-artifacts`

## Smoke test API
```bash
curl http://localhost:8000/health
curl http://localhost:8000/v1/version
```

## Run sample Spark job (ingest + snapshots demo)
Option A — from host (requires local spark & java):
```bash
spark-submit   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540   jobs/1_ingest_and_time_travel.py
```

Option B — inside Spark container:
```bash
# exec into container
docker compose -f infra/docker-compose.yml exec spark bash

# inside container
spark-submit   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540   /app/jobs/1_ingest_and_time_travel.py
```

If you want Spark UI:
- Run container with `-p 4040:4040` (already set in compose).
- Open http://localhost:4040 while the job runs.

## Query with Trino
- Open http://localhost:8080
- Catalog: `iceberg` → Schema: `demo`
- Example:
```sql
SELECT * FROM iceberg.demo.customers LIMIT 10;
SELECT * FROM iceberg.demo.customers$snapshots;
```

## Common issues
- **403/Access denied (Trino→MinIO):** ensure buckets exist and credentials match `.env`.
- **Spark can't reach MinIO:** check endpoint `http://minio:9000` when running inside container; use `http://localhost:9000` from host.
- **No Trino tables:** ensure you wrote an Iceberg table to `s3a://warehouse/iceberg/...` (see Spark job).

