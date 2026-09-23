#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M220_SAFE_INDEX_REMOVAL.md \
  Makefile \
  README.md \
  hat/hatSql/materialized.go \
  hat/hatSql/m220_materialized_view_index_lifecycle_benchmark_test.go \
  hat/hatSql/m220_materialized_view_index_lifecycle_internal_test.go \
  hat/hatSql/m220_materialized_view_index_lifecycle_test.go \
  scripts/m220-benchmark.sh \
  scripts/m220-format.sh \
  scripts/m220-race.sh \
  scripts/m220-stage.sh \
  scripts/m220-test.sh \
  scripts/m220-vet.sh \
  scripts/m220-commit.sh \
  scripts/m220-push.sh

git status --short
