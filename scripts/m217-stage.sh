#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M217_MATERIALIZED_POINT_LOOKUPS.md \
  Makefile \
  README.md \
  hat/hatSql/materialized.go \
  hat/hatSql/m217_materialized_view_point_lookup_benchmark_test.go \
  hat/hatSql/m217_materialized_view_point_lookup_test.go \
  scripts/m217-benchmark.sh \
  scripts/m217-commit.sh \
  scripts/m217-format.sh \
  scripts/m217-push.sh \
  scripts/m217-race.sh \
  scripts/m217-stage.sh \
  scripts/m217-test.sh \
  scripts/m217-vet.sh
git diff --cached --check
git diff --cached --stat
