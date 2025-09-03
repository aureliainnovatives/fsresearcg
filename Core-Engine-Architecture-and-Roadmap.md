# Core Engine — Architecture & Roadmap (M1 = MVP)

> **Goal:** ship a lean, Dockerized **data+model OS core** that ingests data, stores it once, queries fast, **modifies fast-enough**, and **exports for model training** — without boxing us into a rewrite when we add streaming, hot/cold paths, or branching later.

---

## 1) Purpose & Scope

- **In-scope (M1/MVP):**  
  - File-based ingestion (CSV/JSON/Parquet) via presigned upload  
  - ACID tables with time-travel (Iceberg) stored on S3-style object storage (MinIO)  
  - Fast interactive SQL (Trino) + programmatic queries via API proxy  
  - Low-latency **micro-batch** upserts/deletes (Spark MERGE)  
  - Export curated snapshots to sharded Parquet (or basic WebDataset) for training  
  - Everything **Dockerized**, driven by a **FastAPI** control plane

- **Out-of-scope (for M1; planned later):** Kafka/streaming, Hudi/Paimon “hot path”, Nessie branching, Ray exporter, governance (Ranger/Atlas), advanced caching (Alluxio), model serving (vLLM/KServe).

---

## 2) Product North Star (Step-1 promises)

1) **Ingest anything** (initially files; later DBs & streams) with contracts.  
2) **Store once**, expose as **ACID tables** with time travel.  
3) **Query fast** (sub-second to seconds) with proper partitioning.  
4) **Modify fast-enough** via micro-batched upserts/deletes.  
5) **Export to training** as shard sets + manifest for zero-copy pipelines.  

---

## 3) Architecture (M1/MVP)

### Components (minimal, all Docker)
- **MinIO** — S3-compatible object store (our “filesystem”).  
- **Apache Iceberg** — table/transaction layer on top of Parquet.  
- **Apache Spark** — batch + micro-batch ETL, MERGE upserts/deletes into Iceberg.  
- **Trino** — low-latency distributed SQL over Iceberg.  
- **FastAPI** — control plane (presigned upload, ingest, modify, query proxy, export).

### Core dataflows

**A) File ingest → Iceberg table → Query**  
1. FastAPI: `POST /v1/datasets/{name}/presign-upload` → client uploads to MinIO.  
2. FastAPI: `POST /v1/ingest/file {dataset, uri, format, mode}`  
3. Spark reads file, light validation, writes/append to **Iceberg**.  
4. Trino queries the table; FastAPI can proxy SQL.

**B) Modify (upsert/delete)**  
1. FastAPI: `POST /v1/datasets/{name}/upsert|delete` (payload rows or filter).  
2. Requests buffered (N rows / N seconds) → Spark **MERGE INTO** Iceberg.  
3. Return commit ID/snapshot for audit & time travel.

**C) Export to training**  
1. FastAPI: `POST /v1/exports {dataset, columns, filters, snapshot/timestamp, shard_mb}`  
2. Spark reads Iceberg snapshot → writes **sharded Parquet** (or basic `.tar` WebDataset) to `s3://ml-artifacts/...`  
3. Emit `manifest.json` (URIs, schema, stats) → return URI.

---

## 4) Key Decisions (and why)

- **Iceberg-only (for M1):** one table format for both reads & writes → simplest mental model; no rewrite later.  
- **Micro-batch MERGE for “fast modify”:** 2–10s commit latency is sufficient for Step-1; we can introduce Hudi/Paimon later **behind the same API** if sub-second is required.  
- **Trino for fast SQL:** interactive performance + federation potential later.  
- **No Kafka/Schema Registry yet:** fewer moving parts; add only when streaming is justified.  
- **Manifest-based exports:** trainers read a manifest (URIs + schema) → we can swap exporters later without client changes.  
- **Strict API boundaries:** StorageAdapter, TableService, QueryService, ExportService ensure internals can evolve without breaking clients.

---

## 5) Non-Functional Goals (initial targets)

- **Ingest throughput (CSV→Iceberg):** 50–200 MB/s on a modest box; higher on servers.  
- **Query (Trino on partitioned data):** sub-second on selective filters; a few seconds on GB-scale scans.  
- **Modify (micro-batch MERGE):** 2–10s end-to-end commit cadence.  
- **Export:** 100–300 MB/s writing sharded Parquet; shard size ~256–512 MB.

---

## 6) Public API (M1 surface)

- `POST /v1/datasets/{name}/presign-upload` → `{url, fields, bucket, key}`  
- `POST /v1/ingest/file` → `{dataset, uri, format: csv|json|parquet, mode: append|overwrite, options?}`  
- `POST /v1/datasets/{name}/upsert` → `{primaryKey, rows[]}` (buffered → MERGE)  
- `POST /v1/datasets/{name}/delete` → `{filter}`  
- `POST /v1/sql` → `{sql}` (proxy to Trino; pagination, max rows)  
- `GET /v1/datasets/{name}/snapshots` → list snapshot IDs & timestamps  
- `POST /v1/datasets/{name}/query-at` → `{snapshot_id|as_of_timestamp, sqlFragment}`  
- `POST /v1/exports` → `{dataset, columns, filters, snapshot|timestamp, format: parquet|webdataset, shard_mb}` → returns `{manifest_uri}`

> **Auth:** API key/JWT (env-configurable), IP allowlist optional in M1.

---

## 7) Dockerization & Layout

**Services (Compose)**  
- `minio` (9000 S3 / 9001 console)  
- `trino` (8080) with Iceberg connector  
- `spark` (driver only is fine for M1)  
- `api-gateway` (FastAPI)  

**Network & volumes**  
- Single Docker network `core-net`; named volumes for MinIO data; bind-mount for configs.  

**Environment (example)**
```ini
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
S3_ENDPOINT=http://minio:9000
S3_BUCKET_WAREHOUSE=warehouse
ICEBERG_WAREHOUSE=s3a://warehouse/iceberg
TRINO_URL=http://trino:8080
API_AUTH_TOKEN=change-me
```

**Repo skeleton**
```
/core-engine
  /api/                # FastAPI app (routers/services/schemas)
  /jobs/               # Spark jobs (ingest, merge, export)
  /infra/              # docker-compose.yml, trino catalog configs
  /docs/               # this doc + runbooks
```

---

## 8) Data Layout & Naming

- **MinIO buckets:**  
  - `warehouse` (Iceberg data + metadata)  
  - `ml-artifacts` (exports, manifests)

- **Namespaces:** `iceberg.db_name.table_name`  
- **Partitioning (default):**  
  - Date/time columns → `day(ts)` or `month(ts)`  
  - High-cardinality keys → avoid direct partitioning; rely on Iceberg clustering + file size targets  
- **Target file size:** 256–512 MB (Parquet)

---

## 9) Operations (M1)

- **Compaction & stats (cron or on-demand):**  
  - Optimize small files, refresh stats/manifests  
  - Configurable cadence per table (e.g., hourly/daily)
- **Observability:**  
  - Structured logs (API + Spark)  
  - Trino UI enabled; Spark UI on demand (port-map when needed)  
- **Security:**  
  - API token auth; MinIO creds by env; no PII in logs

---

## 10) Demo Script (M1)

1. Create dataset & **presign upload** → push a heavy CSV.  
2. **Ingest** via API → Spark writes **Iceberg** table (show MinIO objects).  
3. **Query** with `/sql` (Trino) → filters/aggregations; show seconds/sub-second latency.  
4. **Modify** with `/upsert` (send a few rows) → micro-batch MERGE → show new snapshot.  
5. **Time-travel** via `/query-at` using the prior snapshot → verify old view.  
6. **Export** to training → return **manifest URI** → show sharded Parquet in `ml-artifacts/`.

---

## 11) Risks & Mitigations

- **Small file explosion** → scheduled compaction; enforce target file size.  
- **Schema drift** → start with light validation; add schema registry later.  
- **Concurrency on MERGE** → serialize per table or use optimistic retries (M1).  
- **Throughput plateaus** → tune Spark parallelism, column pruning, partition design.

---

## 12) Roadmap (add layers without rewrites)

- **S-1 Streaming (optional):** Spark Structured Streaming → Iceberg; later switch to Redpanda/Flink if needed (API unchanged).  
- **S-2 Branching:** add **Nessie** for data branches/tags (`/branch`, `/merge`).  
- **S-3 Hot path:** introduce **Hudi/Paimon** behind `/upsert` for sub-second writes; publish to Iceberg on cadence.  
- **S-4 Contracts & DQ:** Schema Registry + Great Expectations; block bad data at the door.  
- **S-5 Training exporter v2:** swap Spark exporter with **Ray Data**; same `/export` API.  
- **S-6 Perf optimizer:** auto-recommend partitioning, clustering, compaction (your proprietary “Adaptive Layout Optimizer”).  
- **S-7 Caching near compute:** Alluxio in front of MinIO for GPU-adjacent workloads.  
- **S-8 Governance/lineage:** Ranger, Atlas/OpenLineage.  
- **S-9 Serving (stretch):** vLLM + KServe; vector DB (Qdrant/Milvus).

---

## 13) No-Rewrite Guardrails

- **Stable external API** (section 6) — never break it.  
- **Service boundaries**:  
  - `StorageAdapter` (S3/MinIO ops)  
  - `TableService` (Iceberg now; can multiplex Hudi later)  
  - `QueryService` (Trino proxy)  
  - `ExportService` (Spark→Ray swap)  
- **Manifest-based exports**, **config-driven table policies**, **idempotent job submission**.

---

## 14) Acceptance Criteria (M1 = MVP)

- Can **upload** a file, **ingest** to Iceberg, **query** via API, **upsert/delete** with commit IDs, **time-travel** to an earlier snapshot, and **export** a filtered snapshot to sharded Parquet with a **manifest** — all via **Dockerized services** and documented env vars.

---

## 15) Glossary (quick)

- **Object store:** S3-style storage (MinIO) for durability & scale.  
- **Parquet:** columnar file format (fast scans, compression).  
- **Iceberg:** ACID table layer over files; snapshots/time-travel/partition evolution.  
- **Spark:** compute engine for ETL and MERGE upserts.  
- **Trino:** fast, distributed SQL engine (BI-style queries).  
- **Hot/Cold:** write-optimized vs read-optimized layouts (future pattern, not required in M1).

---

### Notes for future dev
- Prefer **incremental filenames** for code/scripts (e.g., `1-ingest.py`, `2-merge.py`, `3-export.py`).  
- Keep infra configs in `/infra/` with clear comments and env samples.  
- Add a **RUNBOOK.md** beside this doc with exact `docker compose` commands when we cut M1.

*End of document.*
