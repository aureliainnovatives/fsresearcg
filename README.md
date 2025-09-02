
# Core Engine (M1 / MVP)

This repo contains the **minimal, dockerized core** for the data+model OS:
- Ingest files → Iceberg (on MinIO)
- Query via Trino (SQL)
- Modify via micro-batch MERGE (Spark)
- Export snapshots for training
- Controlled by a FastAPI gateway

> Start simple. Add streaming, hot/cold paths, and branching later without breaking the public API.

## Quick start
```bash
# 0) Clone and switch to your v1 branch
git checkout v1

# 1) Copy env sample
cp .env.example .env

# 2) Start core services
docker compose -f infra/docker-compose.yml up -d

# 3) Open MinIO console
# http://localhost:9001  (login from .env)
# Create buckets: warehouse, ml-artifacts

# 4) Check API
curl http://localhost:8000/health
curl http://localhost:8000/v1/version

# 5) Run a sample Spark job (from host or inside spark container)
# (See docs/RUNBOOK.md)
```

## Docs
- `docs/ARCHITECTURE.md` — high-level architecture & roadmap
- `docs/RUNBOOK.md` — day-0 commands and common tasks
- `docs/ADR/ADR-001-iceberg-only.md` — decision record: Iceberg-only for M1

## Services
- **MinIO** — S3-compatible storage
- **Trino** — fast SQL over Iceberg (hadoop catalog)
- **Spark** — batch/micro-batch ETL (MERGE)
- **FastAPI** — control plane (presign upload, ingest, modify, query, export)

## Public API (M1 surface)
See `docs/ARCHITECTURE.md` (Section 6).
