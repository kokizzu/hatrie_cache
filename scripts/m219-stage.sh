#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M219_BACKGROUND_INDEX_BUILD.md \
  Makefile \
  README.md \
  hat/hatSql/index_rebuild_queue.go \
  hat/hatSql/materialized.go \
  hat/hatSql/m219_materialized_view_point_build_benchmark_test.go \
  hat/hatSql/m219_materialized_view_point_build_test.go \
  scripts/m219-benchmark.sh \
  scripts/m219-commit.sh \
  scripts/m219-format.sh \
  scripts/m219-push.sh \
  scripts/m219-race.sh \
  scripts/m219-stage.sh \
  scripts/m219-test.sh \
  scripts/m219-vet.sh

git status --short
