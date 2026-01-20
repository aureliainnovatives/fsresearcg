
# ADR-001: Iceberg-only for M1 (no hot/cold, no streaming)

- Status: Accepted
- Date: 2025-09-02

## Context
We need a minimal, teachable core that supports ingest, query, modify, and export without excess infra.

## Decision
Use **Apache Iceberg** as the sole table format for both reads and writes. Use **Spark MERGE** for micro-batch updates. Defer streaming, hot/cold paths, and Nessie branches.

## Consequences
- Simpler stack, faster demo and onboarding.
- Commit latency for updates is seconds, not sub-second (acceptable for M1).
- We can introduce Hudi/Paimon, Nessie, and streaming later behind stable APIs with no client breaking changes.
