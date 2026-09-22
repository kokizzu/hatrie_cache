#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M222_REPLICATED_COMPUTE_WORKERS.md \
  hat/hatSql/m222_materialized_compute_replica.go \
  hat/hatSql/m222_materialized_compute_replica_test.go \
  hat/hatSql/m222_materialized_compute_replica_benchmark_test.go \
  scripts/format-m222-materialized-compute-replicas.sh \
  scripts/test-m222-materialized-compute-replicas.sh \
  scripts/benchmark-m222-materialized-compute-replicas.sh \
  scripts/race-m222-materialized-compute-replicas.sh \
  scripts/vet-m222-materialized-compute-replicas.sh \
  scripts/test-m222-related-materialized.sh \
  scripts/stage-m222-materialized-compute-replicas.sh \
  scripts/commit-m222-materialized-compute-replicas.sh \
  scripts/push-m222-materialized-compute-replicas.sh \
  scripts/status-m222-materialized-compute-replicas.sh
