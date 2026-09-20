#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU20_ARRAY_JSON_LATE_MATERIALIZATION.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  hat/hatSql/ch031_automatic_json_subcolumn.go \
  hat/hatSql/columnar_json_subcolumn.go \
  hat/hatSql/columnar_json_subcolumn_scan.go \
  hat/hatSql/json_path.go \
  hat/hatSql/chu20_array_json_late_materialization_benchmark_test.go \
  hat/hatSql/chu20_array_json_late_materialization_test.go \
  scripts/benchmark-chu20-baseline.sh \
  scripts/benchmark-chu20.sh \
  scripts/commit-chu20.sh \
  scripts/format-chu20.sh \
  scripts/push-chu20.sh \
  scripts/race-chu20.sh \
  scripts/review-chu20.sh \
  scripts/stage-chu20.sh \
  scripts/test-chu20-package.sh \
  scripts/test-chu20.sh \
  scripts/vet-chu20.sh
