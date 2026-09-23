#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M224_HYDRATION_PROGRESS.md \
  Makefile \
  README.md \
  hat/hatSql/materialized.go \
  hat/hatSql/m224_materialized_view_hydration_benchmark_test.go \
  hat/hatSql/m224_materialized_view_hydration_progress_test.go \
  scripts/m224-benchmark.sh \
  scripts/m224-commit.sh \
  scripts/m224-format.sh \
  scripts/m224-push.sh \
  scripts/m224-race.sh \
  scripts/m224-stage.sh \
  scripts/m224-test.sh \
  scripts/m224-vet.sh
git diff --cached --check
git diff --cached --stat
