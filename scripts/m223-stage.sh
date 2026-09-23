#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M223_MATERIALIZED_VIEW_HYDRATION.md \
  hat/hatSql/materialized.go \
  hat/hatSql/m223_materialized_view_hydration_test.go \
  hat/hatSql/m223_materialized_view_hydration_benchmark_test.go \
  scripts/m223-test.sh \
  scripts/m223-format.sh \
  scripts/m223-benchmark.sh \
  scripts/m223-race.sh \
  scripts/m223-vet.sh \
  scripts/m223-stage.sh \
  scripts/m223-commit.sh \
  scripts/m223-push.sh
git status --short
