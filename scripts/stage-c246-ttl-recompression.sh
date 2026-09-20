#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C246_TTL_RECOMPRESSION.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatDataStructure/ttl_recompression.go \
  hat/hatDataStructure/ttl_recompression_test.go \
  hat/hatDataStructure/ttl_recompression_benchmark_test.go \
  scripts/inspect-c246-scope.sh \
  scripts/test-c246-ttl-recompression.sh \
  scripts/format-c246-ttl-recompression.sh \
  scripts/race-c246-ttl-recompression.sh \
  scripts/vet-c246-ttl-recompression.sh \
  scripts/benchmark-c246-ttl-recompression.sh \
  scripts/review-c246-ttl-recompression.sh \
  scripts/stage-c246-ttl-recompression.sh \
  scripts/commit-c246-ttl-recompression.sh \
  scripts/push-c246-ttl-recompression.sh
git diff --cached --check
git diff --cached --stat
