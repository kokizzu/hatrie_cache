#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md \
  M250_TEMPORAL_JOIN_FRONTIER_ALIGNMENT.md \
  hat/hatSql/temporal_join_frontier_alignment.go \
  hat/hatSql/m250_temporal_join_alignment_test.go \
  hat/hatSql/m250_temporal_join_alignment_benchmark_test.go \
  scripts/test-m250.sh \
  scripts/benchmark-m250.sh \
  scripts/format-m250.sh \
  scripts/race-m250.sh \
  scripts/vet-m250.sh \
  scripts/test-m250-package.sh \
  scripts/verify-docs-m250.sh \
  scripts/stage-m250.sh \
  scripts/commit-m250.sh \
  scripts/push-m250.sh
git diff --cached --stat
git diff --cached --check
git status --short
