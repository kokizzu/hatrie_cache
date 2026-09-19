#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  BENCHMARK.md \
  CHU35_OPTIMIZE_CONTROL.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatStorage/compaction_control.go \
  hat/hatStorage/chu35_optimize_control_benchmark_test.go \
  hat/hatStorage/chu35_optimize_control_test.go \
  scripts/benchmark-chu35-baseline.sh \
  scripts/benchmark-chu35.sh \
  scripts/commit-chu35.sh \
  scripts/format-chu35.sh \
  scripts/push-chu35.sh \
  scripts/race-chu35.sh \
  scripts/test-chu35-package.sh \
  scripts/test-chu35.sh \
  scripts/verify-chu35.sh \
  scripts/vet-chu35.sh
