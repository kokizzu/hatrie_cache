#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M222_REPLICATED_INDEX_WORKERS.md \
  Makefile \
  README.md \
  hat/hatSql/index_rebuild_replica.go \
  hat/hatSql/m222_index_rebuild_replica_benchmark_test.go \
  hat/hatSql/m222_index_rebuild_replica_test.go \
  hat/hatSql/materialized.go \
  scripts/m222-benchmark.sh \
  scripts/m222-commit.sh \
  scripts/m222-format.sh \
  scripts/m222-push.sh \
  scripts/m222-race.sh \
  scripts/m222-stage.sh \
  scripts/m222-test.sh \
  scripts/m222-vet.sh
git diff --cached --check
git diff --cached --stat
