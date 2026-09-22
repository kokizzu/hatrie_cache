#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  M246_FRONTIER_RETENTION_POLICY.md \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/frontier_retention_policy.go \
  hat/hatPipeline/m246_frontier_retention_policy_test.go \
  hat/hatPipeline/m246_frontier_retention_policy_benchmark_test.go \
  scripts/test-m246.sh \
  scripts/format-m246.sh \
  scripts/benchmark-m246.sh \
  scripts/race-m246.sh \
  scripts/vet-m246.sh \
  scripts/test-m246-package.sh \
  scripts/verify-docs-m246.sh \
  scripts/stage-m246.sh \
  scripts/commit-m246.sh \
  scripts/push-m246.sh
