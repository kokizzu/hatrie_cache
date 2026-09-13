#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ050_PLAN_SNAPSHOTS.md \
  Makefile \
  README.md \
  hat/hatSql/keyset.go \
  hat/hatSql/materialized.go \
  hat/hatSql/model.go \
  hat/hatSql/mz050_plan_snapshot.go \
  hat/hatSql/mz050_plan_snapshot_benchmark_test.go \
  hat/hatSql/mz050_plan_snapshot_test.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go \
  hat/hatSql/sql_result_cache.go \
  scripts/benchmark-mz050.sh \
  scripts/commit-mz050.sh \
  scripts/format-mz050.sh \
  scripts/push-mz050.sh \
  scripts/race-mz050.sh \
  scripts/test-mz050.sh \
  scripts/vet-mz050.sh
git commit -m "feat: add SQL plan snapshots"
