#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  TT021_MATERIALIZED_SPATIAL_INDEX.md \
  hat/hatSchema/tt021_materialized_spatial_index_benchmark_test.go \
  hat/hatSchema/tt021_spatial_index_persistence.go \
  hat/hatSchema/tt021_spatial_index_persistence_test.go \
  scripts/commit-tt021-spatial-persistence.sh \
  scripts/format-tt021-spatial-persistence.sh \
  scripts/push-tt021-spatial-persistence.sh \
  scripts/stage-tt021-spatial-persistence.sh \
  scripts/test-tt021-spatial-persistence.sh \
  scripts/verify-tt021-spatial-persistence.sh

git diff --cached --check
git diff --cached --stat
git status --short
