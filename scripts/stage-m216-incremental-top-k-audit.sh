#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M216_INCREMENTAL_TOP_K_AUDIT.md \
  scripts/benchmark-m216-incremental-top-k-audit.sh \
  scripts/commit-m216-incremental-top-k-audit.sh \
  scripts/push-m216-incremental-top-k-audit.sh \
  scripts/stage-m216-incremental-top-k-audit.sh \
  scripts/test-m216-incremental-top-k-audit.sh
