#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  M247_FRONTIER_EXPIRY_ERRORS.md \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/frontier_retention_expiry.go \
  hat/hatPipeline/m247_frontier_expiry_error_test.go \
  hat/hatPipeline/m247_frontier_expiry_error_benchmark_test.go \
  scripts/test-m247.sh \
  scripts/format-m247.sh \
  scripts/benchmark-m247.sh \
  scripts/race-m247.sh \
  scripts/vet-m247.sh \
  scripts/test-m247-package.sh \
  scripts/verify-docs-m247.sh \
  scripts/stage-m247.sh \
  scripts/commit-m247.sh \
  scripts/push-m247.sh
